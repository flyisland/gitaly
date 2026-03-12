package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
)

// ResolveSink returns a sink implementation based on the provided uri.
// The storage engine is chosen based on the provided uri.
// It is the caller's responsibility to provide all required environment
// variables in order to get properly initialized storage engine driver.
func ResolveSink(ctx context.Context, uri string, opts ...SinkOption) (*Sink, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	scheme := parsed.Scheme
	if i := strings.LastIndex(scheme, "+"); i > 0 {
		// the url may include additional configuration options like service name
		// we don't include it into the scheme definition as it will push us to create
		// a full set of variations. Instead we trim it up to the service option only.
		scheme = scheme[i+1:]
	}
	var sink *Sink
	switch scheme {
	case s3blob.Scheme, azureblob.Scheme, gcsblob.Scheme, memblob.Scheme:
		sink, err = newSink(ctx, uri)
	case fileblob.Scheme, "":
		// fileblob.OpenBucket requires a bare path without 'file://'.
		sink, err = newFileblobSink(parsed.Path)
	default:
		return nil, fmt.Errorf("unsupported sink URI scheme: %q", scheme)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create object storage sink: %w", err)
	}

	for _, opt := range opts {
		opt(sink)
	}

	return sink, nil
}

// WithBufferSize sets the buffer size for the sink.
func WithBufferSize(size int) SinkOption {
	return func(s *Sink) {
		s.bufferSize = size
	}
}

// WithEnableChecksumAlgorithm enables checksum algorithm injection for S3-compatible
// providers.
func WithEnableChecksumAlgorithm(enabled bool) SinkOption {
	return func(s *Sink) {
		s.enableChecksumAlgo = enabled
	}
}

// Sink uses a storage engine that can be defined by the construction url on creation.
type Sink struct {
	bucket             *blob.Bucket
	bufferSize         int
	enableChecksumAlgo bool
}

// SinkOption is a function that configures a Sink.
type SinkOption func(*Sink)

// newSink returns initialized instance of Sink instance.
func newSink(ctx context.Context, url string) (*Sink, error) {
	bucket, err := blob.OpenBucket(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("sink: open bucket: %w", err)
	}

	return &Sink{bucket: bucket}, nil
}

// newFileblobSink returns initialized instance of Sink instance using the
// fileblob backend.
func newFileblobSink(path string) (*Sink, error) {
	// fileblob's CreateDir creates directories with permissions 0777, so
	// create this directory ourselves:
	// https://github.com/google/go-cloud/issues/3423
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("stat sink path: %w", err)
		}
		if err := os.MkdirAll(path, mode.Directory); err != nil {
			return nil, fmt.Errorf("creating sink directory: %w", err)
		}
	}

	bucket, err := fileblob.OpenBucket(path, &fileblob.Options{NoTempDir: true, Metadata: fileblob.MetadataDontWrite})
	if err != nil {
		return nil, fmt.Errorf("sink: open bucket: %w", err)
	}

	return &Sink{bucket: bucket}, nil
}

func injectChecksumAlgorithm(asFunc func(interface{}) bool) error {
	// Only works for aws sdk v2
	// For awssdk=v1 we deliberately do nothing here:
	// v1's s3manager.Uploader cannot reliably send checksum headers
	// and setting UploadInput.ChecksumAlgorithm would cause an error.
	var putInput *s3.PutObjectInput
	if asFunc(&putInput) {
		putInput.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
	}
	return nil
}

// Close releases resources associated with the bucket communication.
func (s Sink) Close() error {
	if err := s.bucket.Close(); err != nil {
		return fmt.Errorf("sink: close bucket: %w", err)
	}
	return nil
}

// GetWriter stores the written data into a relativePath path on the configured
// bucket. It is the callers responsibility to Close the reader after usage.
func (s Sink) GetWriter(ctx context.Context, relativePath string) (io.WriteCloser, error) {
	opts := &blob.WriterOptions{
		BufferSize: s.bufferSize,
		// 'no-store' - we don't want the backup to be cached as the content could be changed,
		// so we always want a fresh and up to date data
		// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#cacheability
		// 'no-transform' - disallows intermediates to modify data
		// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#other
		CacheControl: "no-store, no-transform",
		ContentType:  "application/octet-stream",
	}

	if s.enableChecksumAlgo {
		opts.BeforeWrite = injectChecksumAlgorithm
	}

	writer, err := s.bucket.NewWriter(ctx, relativePath, opts)
	if err != nil {
		return nil, fmt.Errorf("sink: new writer for %q: %w", relativePath, err)
	}
	return writer, nil
}

// GetReader returns a reader to consume the data from the configured bucket.
// It is the caller's responsibility to Close the reader after usage.
func (s Sink) GetReader(ctx context.Context, relativePath string) (io.ReadCloser, error) {
	reader, err := s.bucket.NewReader(ctx, relativePath, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			err = ErrDoesntExist
		}
		return nil, fmt.Errorf("sink: new reader for %q: %w", relativePath, err)
	}
	return reader, nil
}

// SignedURL returns a URL that can be used to GET the blob for the duration
// specified in expiry.
func (s Sink) SignedURL(ctx context.Context, relativePath string, expiry time.Duration) (string, error) {
	opt := &blob.SignedURLOptions{
		Expiry: expiry,
	}

	signed, err := s.bucket.SignedURL(ctx, relativePath, opt)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			err = ErrDoesntExist
		}
		return "", fmt.Errorf("sink: signed URL for %q: %w", relativePath, err)
	}

	return signed, nil
}

// Exists is a wrapper around the underlying bucket and returns true if a blob exists at key,
// false if it does not exist, or an error.
func (s Sink) Exists(ctx context.Context, relativePath string) (bool, error) {
	return s.bucket.Exists(ctx, relativePath)
}

// List all objects that have the specified prefix.
func (s Sink) List(prefix string) *ListIterator {
	it := s.bucket.List(&blob.ListOptions{Prefix: prefix})
	return &ListIterator{it: it}
}

// ListIterator allows iterating over objects stored in the Sink.
type ListIterator struct {
	it  *blob.ListIterator
	obj *blob.ListObject
	err error
}

// Next retrieves the next result from the iterator. It returns true if there
// are more objects to retrieve and false if an error occurred or there are no
// more objects. It is the callers responsibility to check for iteration errors
// by calling Err.
func (li *ListIterator) Next(ctx context.Context) bool {
	if li.err != nil {
		return false
	}

	li.obj, li.err = li.it.Next(ctx)
	if li.err != nil {
		return false
	}

	for li.obj.IsDir {
		li.obj, li.err = li.it.Next(ctx)
		if li.err != nil {
			return false
		}
	}

	return true
}

// Err returns the iteration error if there is one.
func (li *ListIterator) Err() error {
	if errors.Is(li.err, io.EOF) {
		return nil
	}
	return li.err
}

// Path is the path of the current object.
func (li *ListIterator) Path() string {
	if li.obj == nil {
		return ""
	}
	return li.obj.Key
}
