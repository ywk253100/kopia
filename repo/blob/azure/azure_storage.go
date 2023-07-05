// Package azure implements Azure Blob Storage.
package azure

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/timestampmeta"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	azStorageType = "azureBlob"

	timeMapKey = "Kopiamtime" // this must be capital letter followed by lowercase, to comply with AZ tags naming convention.
)

type AzStorage struct {
	Options
	blob.UnsupportedBlobRetention

	Service   *azblob.Client
	Container string
}

func (az *AzStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	return blob.Capacity{}, blob.ErrNotAVolume
}

func (az *AzStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	if offset < 0 {
		return errors.Wrap(blob.ErrInvalidRange, "invalid offset")
	}

	opt := &azblob.DownloadStreamOptions{}

	if length > 0 {
		opt.Range.Offset = offset
		opt.Range.Count = length
	}

	if length == 0 {
		l1 := int64(1)
		opt.Range.Offset = offset
		opt.Range.Count = l1
	}

	resp, err := az.Service.DownloadStream(ctx, az.Container, az.getObjectNameString(b), opt)
	if err != nil {
		return translateError(err)
	}

	body := resp.Body
	defer body.Close() //nolint:errcheck

	if length == 0 {
		return nil
	}

	if err := iocopy.JustCopy(output, body); err != nil {
		return translateError(err)
	}

	//nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (az *AzStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	bc := az.Service.ServiceClient().NewContainerClient(az.Container).NewBlobClient(az.getObjectNameString(b))

	fi, err := bc.GetProperties(ctx, nil)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(translateError(err), "Attributes")
	}

	bm := blob.Metadata{
		BlobID:    b,
		Length:    *fi.ContentLength,
		Timestamp: *fi.LastModified,
	}

	if fi.Metadata[timeMapKey] != nil {
		if t, ok := timestampmeta.FromValue(*fi.Metadata[timeMapKey]); ok {
			bm.Timestamp = t
		}
	}

	return bm, nil
}

func translateError(err error) error {
	if err == nil {
		return nil
	}

	var re *azcore.ResponseError

	if errors.As(err, &re) {
		switch re.ErrorCode {
		case string(bloberror.BlobNotFound):
			return blob.ErrBlobNotFound
		case string(bloberror.InvalidRange):
			return blob.ErrInvalidRange
		}
	}

	return err
}

func (az *AzStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	switch {
	case opts.HasRetentionOptions():
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "blob-retention")
	case opts.DoNotRecreate:
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "do-not-recreate")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tsMetadata := timestampmeta.ToMap(opts.SetModTime, timeMapKey)

	metadata := make(map[string]*string, len(tsMetadata))

	for k, v := range tsMetadata {
		metadata[k] = to.Ptr(v)
	}

	uso := &azblob.UploadStreamOptions{
		Metadata: metadata,
	}

	resp, err := az.Service.UploadStream(ctx, az.Container, az.getObjectNameString(b), data.Reader(), uso)
	if err != nil {
		return translateError(err)
	}

	if opts.GetModTime != nil {
		*opts.GetModTime = *resp.LastModified
	}

	return nil
}

// DeleteBlob deletes azure blob from container with given ID.
func (az *AzStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	_, err := az.Service.DeleteBlob(ctx, az.Container, az.getObjectNameString(b), nil)
	err = translateError(err)

	// don't return error if blob is already deleted
	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return err
}

func (az *AzStorage) getObjectNameString(b blob.ID) string {
	return az.Prefix + string(b)
}

// ListBlobs list azure blobs with given prefix.
func (az *AzStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	prefixStr := az.Prefix + string(prefix)

	pager := az.Service.NewListBlobsFlatPager(az.Container, &azblob.ListBlobsFlatOptions{
		Prefix: &prefixStr,
		Include: azblob.ListBlobsInclude{
			Metadata: true,
		},
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return translateError(err)
		}

		for _, it := range page.Segment.BlobItems {
			n := *it.Name

			bm := blob.Metadata{
				BlobID: blob.ID(n[len(az.Prefix):]),
				Length: *it.Properties.ContentLength,
			}

			// see if we have 'Kopiamtime' metadata, if so - trust it.
			if t, ok := timestampmeta.FromValue(stringDefault(it.Metadata["kopiamtime"], "")); ok {
				bm.Timestamp = t
			} else {
				bm.Timestamp = *it.Properties.LastModified
			}

			if err := callback(bm); err != nil {
				return err
			}
		}
	}

	return nil
}

func stringDefault(s *string, def string) string {
	if s == nil {
		return def
	}

	return *s
}

func (az *AzStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   azStorageType,
		Config: &az.Options,
	}
}

func (az *AzStorage) DisplayName() string {
	return fmt.Sprintf("Azure: %v", az.Options.Container)
}

func (az *AzStorage) Close(ctx context.Context) error {
	return nil
}

func (az *AzStorage) FlushCaches(ctx context.Context) error {
	return nil
}

// New creates new Azure Blob Storage-backed storage with specified options
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	_ = isCreate

	if opt.Container == "" {
		return nil, errors.New("container name must be specified")
	}

	var (
		service    *azblob.Client
		serviceErr error
	)

	storageDomain := opt.StorageDomain
	if storageDomain == "" {
		storageDomain = "blob.core.windows.net"
	}

	storageHostname := fmt.Sprintf("%v.%v", opt.StorageAccount, storageDomain)

	switch {
	// shared access signature
	case opt.SASToken != "":
		service, serviceErr = azblob.NewClientWithNoCredential(
			fmt.Sprintf("https://%s?%s", storageHostname, opt.SASToken), nil)

	// storage account access key
	case opt.StorageKey != "":
		// create a credentials object.
		cred, err := azblob.NewSharedKeyCredential(opt.StorageAccount, opt.StorageKey)
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize credentials")
		}

		service, serviceErr = azblob.NewClientWithSharedKeyCredential(
			fmt.Sprintf("https://%s/", storageHostname), cred, nil,
		)

	// Azure AD
	default:
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize Azure default credential")
		}
		service, serviceErr = azblob.NewClient(fmt.Sprintf("https://%s/", storageHostname), cred, nil)
	}

	if serviceErr != nil {
		return nil, errors.Wrap(serviceErr, "opening azure service")
	}

	raw := &AzStorage{
		Options:   *opt,
		Container: opt.Container,
		Service:   service,
	}

	az := retrying.NewWrapper(raw)

	// verify Azure connection is functional by listing blobs in a bucket, which will fail if the container
	// does not exist. We list with a prefix that will not exist, to avoid iterating through any objects.
	nonExistentPrefix := fmt.Sprintf("kopia-azure-storage-initializing-%v", clock.Now().UnixNano())
	if err := raw.ListBlobs(ctx, blob.ID(nonExistentPrefix), func(md blob.Metadata) error {
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "unable to list from the bucket")
	}

	return az, nil
}

func init() {
	blob.AddSupportedStorage(azStorageType, Options{}, New)
}
