package offloading

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/backoff"
	"gocloud.dev/blob"
	"golang.org/x/sync/errgroup"

	_ "gocloud.dev/blob/azureblob" // register Azure driver
	_ "gocloud.dev/blob/fileblob"  // register file driver
	_ "gocloud.dev/blob/gcsblob"   // register Google Cloud driver
	_ "gocloud.dev/blob/memblob"   // register in-memory driver
	_ "gocloud.dev/blob/s3blob"
)

var (
	// deletionGoroutineLimit is the upper bound parallel number of goroutines to delete objects
	deletionGoroutineLimit = 5

	errMissingBucket = errors.New("missing bucket")
)

// Bucket is an interface to abstract the behavior of the gocloud.dev/blob Bucket type.
// This abstraction is especially useful when adding a customized bucket to intercept traffic
// or to modify the functionality for specific use cases.
type Bucket interface {
	Download(ctx context.Context, key string, w io.Writer, opts *blob.ReaderOptions) error
	Upload(ctx context.Context, key string, r io.Reader, opts *blob.WriterOptions) error
	List(opts *blob.ListOptions) *blob.ListIterator
	Delete(ctx context.Context, key string) (err error)
	Attributes(ctx context.Context, key string) (*blob.Attributes, error)
	Close() error
}

// Iterator is an interface to abstract the behavior of the gocloud.dev/blob ListIterator type.
// This abstraction is especially useful when adding a customized bucket to intercept traffic
// or to modify the functionality for specific use cases.
type Iterator interface {
	Next(ctx context.Context) (*blob.ListObject, error)
}

// Sink is a wrapper around the storage bucket, providing an interface for
// operations on offloaded objects.
type Sink struct {
	overallTimeout  time.Duration
	bucket          Bucket
	backoffStrategy backoff.Strategy

	maxRetry     uint
	noRetry      bool
	retryTimeout time.Duration
}

// NewSink creates a Sink from the given options. If some options are not specified,
// the function will use the default values for them.
func NewSink(bucket Bucket, options ...SinkOption) (*Sink, error) {
	if bucket == nil {
		return nil, errMissingBucket
	}

	var cfg sinkCfg
	for _, apply := range options {
		apply(&cfg)
	}
	sink := &Sink{
		overallTimeout:  cfg.overallTimeout,
		bucket:          bucket,
		backoffStrategy: cfg.backoffStrategy,
		maxRetry:        cfg.maxRetry,
		noRetry:         cfg.noRetry,
		retryTimeout:    cfg.retryTimeout,
	}

	// fills in default values for missing options.
	sink.setDefaults()

	return sink, nil
}

// Upload uploads a file located at fullFilePath to the bucket under the specified prefix.
// The fullFilePath include the file name, e.g. /tmp/foo.txt.
func (r *Sink) Upload(ctx context.Context, fullFilePath string, prefix string) (returnErr error) {
	ctx, cancel := context.WithTimeout(ctx, r.overallTimeout)
	defer cancel()

	file, err := os.Open(fullFilePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("close file: %w", err))
		}
	}()

	objectKey := fmt.Sprintf("%s/%s", prefix, filepath.Base(fullFilePath))
	if err := r.withRetry(ctx, func(operationCtx context.Context) error {
		return r.bucket.Upload(operationCtx, objectKey, file, &blob.WriterOptions{
			// 'no-store' - we don't want the offloaded blobs to be cached as the content could be changed,
			// so we always want a fresh and up-to-date data
			// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#cacheability
			// 'no-transform' - disallows intermediates to modify data
			// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#other
			CacheControl: "no-store, no-transform",
			ContentType:  "application/octet-stream",
		})
	}); err != nil {
		return fmt.Errorf("upload object %q: %w", objectKey, err)
	}
	return nil
}

// Download retrieves a file from the bucket and saves it to the specified location on the local file system.
// The objectKey is the key of the object in the bucket, which includes the prefix and
// object name (e.g., "prefix/my_object.idx"); fullFilePath is full path on the local file system where the
// object will be saved including the file name (e.g., "/tmp/foo.txt").
func (r *Sink) Download(ctx context.Context, objectKey string, fullFilePath string) (returnErr error) {
	ctx, cancel := context.WithTimeout(ctx, r.overallTimeout)
	defer cancel()

	file, err := os.Create(fullFilePath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer func() {
		err := file.Close()
		if returnErr == nil {
			// Downloading is successful, check of we can close the file.
			if err != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("close file: %w", err))
			}
		} else {
			// Downloading has error, delete the file anyway.
			if err := os.Remove(fullFilePath); err != nil {
				returnErr = errors.Join(returnErr,
					fmt.Errorf("remove file when downloading failed: %w", err))
			}
		}
	}()

	if err := r.withRetry(ctx, func(operationCtx context.Context) error {
		return r.bucket.Download(operationCtx, objectKey, file, nil)
	}); err != nil {
		return fmt.Errorf("download object: %w", err)
	}
	return nil
}

// List lists all objects in the bucket that have the specified prefix.
func (r *Sink) List(ctx context.Context, prefix string) (res []string, err error) {
	return listHandler(ctx, r, prefix, r.bucket.List)
}

type listFunc[T Iterator] func(opts *blob.ListOptions) T

// listHandler is responsible for loading the listFunc and executing it to perform the list operation.
//
// Generics are used here because the List signature in the bucket interface uses ListIterator,
// which is a concrete type rather than an interface. If we need to intercept or modify ListIterator 's behavior,
// such as adding delays or intentionally returning errors, generics provide the necessary flexibility to achieve this.
func listHandler[T Iterator](ctx context.Context, r *Sink, prefix string, listFunc listFunc[T]) (res []string, err error) {
	prefix = filepath.Clean(prefix)

	// listExecutor is where we call the listFunc perform the list operation.
	// We can put listExecutor in later retry loop.
	listExecutor := func(operationCtx context.Context) (res []string, err error) {
		var listErr error
		var attrs *blob.ListObject
		it := listFunc(&blob.ListOptions{
			Prefix:    prefix + "/",
			Delimiter: "/",
		})
		objects := make([]string, 0)
		for {
			attrs, listErr = it.Next(operationCtx)

			if listErr != nil {
				if errors.Is(listErr, io.EOF) {
					return objects, nil
				}
				return []string{}, fmt.Errorf("list object: %w", listErr)
			}

			// exclude the bucketPrefix "folder" itself
			if attrs != nil && attrs.Key != prefix+"/" {
				objects = append(objects, filepath.Base(attrs.Key))
			}
		}
	}

	ctx, cancel := context.WithTimeout(ctx, r.overallTimeout)
	defer cancel()

	if err := r.withRetry(ctx, func(operationCtx context.Context) error {
		res, err = listExecutor(operationCtx)
		return err
	}); err != nil {
		return []string{}, fmt.Errorf("list object %w", err)
	}
	return res, nil
}

// DeleteObjects attempts to delete the specified objects within the given prefix.
// The result is a map of objectKey to error. Successfully deleted objects will not appear in the map.
func (r *Sink) DeleteObjects(ctx context.Context, prefix string, objectNames []string) map[string]error {
	res := make(map[string]error)
	if len(objectNames) == 0 {
		return res
	}

	ctx, cancel := context.WithTimeout(ctx, r.overallTimeout)
	defer cancel()

	type deleteResult struct {
		object string
		err    error
	}
	resCh := make(chan deleteResult)

	group := errgroup.Group{}
	group.SetLimit(deletionGoroutineLimit)
	for _, object := range objectNames {
		group.Go(func() error {
			// var err error
			obj := fmt.Sprintf("%s/%s", prefix, filepath.Base(object))
			if err := r.withRetry(ctx, func(operationCtx context.Context) error {
				return r.bucket.Delete(operationCtx, obj)
			}); err != nil {
				resCh <- deleteResult{object: obj, err: err}
			}
			return nil
		})
	}

	go func() {
		// ignore error here since we use resCh to deal with error returned from the operation
		// no error is returned from the function called by group.Go()
		_ = group.Wait()
		close(resCh)
	}()

	for delRes := range resCh {
		res[delRes.object] = delRes.err
	}
	return res
}

// setDefaults fills in default values for missing options.
func (r *Sink) setDefaults() {
	// Retry is wanted but it is not configured.
	if !r.noRetry && r.maxRetry == 0 {
		r.maxRetry = defaultMaxRetry
	}
	if r.overallTimeout == 0 {
		r.overallTimeout = defaultOverallTimeout
	}
	if r.backoffStrategy == nil {
		r.backoffStrategy = defaultBackoffStrategy
	}
	if r.retryTimeout == 0 {
		r.retryTimeout = defaultRetryTimeout
	}
}

// withRetry retries the given operation until it succeeds or the maximum number of retries is reached.
func (r *Sink) withRetry(ctx context.Context, op func(context.Context) error) error {
	var err error
	for retry := uint(0); retry <= r.maxRetry; {
		err = func() error {
			operationCtx, operationCancel := context.WithTimeout(ctx, r.retryTimeout)
			defer operationCancel()
			return op(operationCtx)
		}()
		if err == nil || r.noRetry {
			break
		}
		timer := time.NewTimer(r.backoffStrategy.Backoff(retry))
		select {
		case <-ctx.Done():
			timer.Stop()
			err = fmt.Errorf("backoffStrategy operation %w", err)
			return err
		case <-timer.C:
			// Refresh timer expires, issue another try.
			retry++
		}
	}
	return err
}
