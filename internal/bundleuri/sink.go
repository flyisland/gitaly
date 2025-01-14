package bundleuri

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gocloud.dev/blob"

	_ "gocloud.dev/blob/azureblob" // register Azure driver
	_ "gocloud.dev/blob/fileblob"  // register file driver
	_ "gocloud.dev/blob/gcsblob"   // register Google Cloud driver
	_ "gocloud.dev/blob/memblob"   // register in-memory driver
	_ "gocloud.dev/blob/s3blob"    // register Amazon S3 driver
)

const (
	defaultBundle = "default"
	defaultExpiry = 10 * time.Minute
)

// Sink is a wrapper around the storage bucket used for accessing/writing
// bundleuri bundles.
type Sink struct {
	bucket *blob.Bucket
}

// NewSink creates a Sink from the given parameters.
func NewSink(ctx context.Context, uri string) (*Sink, error) {
	bucket, err := blob.OpenBucket(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("open bucket: %w", err)
	}
	return &Sink{
		bucket: bucket,
	}, nil
}

// getWriter creates a writer to store data into a relative path on the
// configured bucket.
// It is the callers responsibility to Close the reader after usage.
func (s *Sink) getWriter(ctx context.Context, relativePath string) (io.WriteCloser, error) {
	writer, err := s.bucket.NewWriter(ctx, relativePath, &blob.WriterOptions{
		// 'no-store' - we don't want the bundle to be cached as the content could be changed,
		// so we always want a fresh and up to date data
		// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#cacheability
		// 'no-transform' - disallows intermediates to modify data
		// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#other
		CacheControl: "no-store, no-transform",
		ContentType:  "application/octet-stream",
	})
	if err != nil {
		return nil, fmt.Errorf("new writer for %q: %w", relativePath, err)
	}
	return writer, nil
}

func (s *Sink) signedURL(ctx context.Context, relativePath string) (string, error) {
	if exists, err := s.bucket.Exists(ctx, relativePath); !exists {
		if err == nil {
			return "", structerr.NewNotFound("no bundle available")
		}
		return "", structerr.NewNotFound("no bundle available: %w", err)
	}

	uri, err := s.bucket.SignedURL(ctx, relativePath, &blob.SignedURLOptions{
		Expiry: defaultExpiry,
	})
	if err != nil {
		err = errors.Unwrap(err) // unwrap the filename from the error message
		return "", fmt.Errorf("signed URL: %s", err.Error())
	}

	return uri, nil
}
