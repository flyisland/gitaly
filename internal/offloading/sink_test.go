package offloading

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backoff"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

var backoffStrategyInTest = constantBackoff{}

func TestNewSink(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	bucket := testhelper.TempDir(t)
	localBucketURI := fmt.Sprintf("file://%s", bucket)
	localBucket, err := blob.OpenBucket(ctx, localBucketURI)
	require.NoError(t, err)

	for _, tc := range []struct {
		desc   string
		bucket Bucket

		// allDefault, if true, all options are the default values.
		allDefault      bool
		overallTimeout  time.Duration
		maxRetry        uint
		retryTimeout    time.Duration
		backoffStrategy backoff.Strategy
		expectedError   error
	}{
		{
			desc:          "create a new sink with all defaults",
			bucket:        localBucket,
			allDefault:    true,
			expectedError: nil,
		},
		{
			desc:            "create a new sink with options",
			bucket:          localBucket,
			overallTimeout:  15 * time.Second,
			maxRetry:        0,
			retryTimeout:    25 * time.Second,
			backoffStrategy: defaultBackoffStrategy,
			expectedError:   nil,
		},
		{
			// This is not an error case, no max retry means maxRetry == 0 and the sink
			// will just proceed with no retry at all
			desc:            "missing max retry",
			bucket:          localBucket,
			overallTimeout:  15 * time.Second,
			retryTimeout:    25 * time.Second,
			backoffStrategy: defaultBackoffStrategy,
			expectedError:   nil,
		},
		{
			desc:            "missing bucket",
			bucket:          nil,
			overallTimeout:  15 * time.Second,
			maxRetry:        0,
			retryTimeout:    25 * time.Second,
			backoffStrategy: defaultBackoffStrategy,
			expectedError:   errMissingBucket,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			if tc.allDefault {
				_, err := NewSink(tc.bucket)
				require.NoError(t, err)
				return
			}
			sink, err := NewSink(tc.bucket, WithOverallTimout(tc.overallTimeout),
				WithMaxRetry(tc.maxRetry),
				WithRetryTimeout(tc.retryTimeout),
				WithBackoffStrategy(tc.backoffStrategy))
			if tc.expectedError != nil {
				require.Nil(t, sink)
				require.ErrorIs(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSink_Upload(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc   string
		prefix string

		// filesToUpload is a map of files to be uploaded.
		// The key is the file name and value is the file content.
		// If content is empty, it means the file does not exist on local filesystem.
		filesToUpload map[string]string

		expectedSuccessful map[string]string
		expectedErrored    map[string]error
	}{
		{
			desc:   "Upload objects",
			prefix: "jerry",
			filesToUpload: map[string]string{
				"C-131": "I am Mr. Frundles",
				"C-137": "Why are you here?",
			},
			expectedSuccessful: map[string]string{
				"C-131": "I am Mr. Frundles",
				"C-137": "Why are you here?",
			},
		},
		{
			desc:   "Upload objects but one is missing on local",
			prefix: "jerry",
			filesToUpload: map[string]string{
				"C-131": "I am Mr. Frundles",
				"C-137": "Why are you here?",
				"5126":  "", // missing on local
			},
			expectedSuccessful: map[string]string{
				"C-131": "I am Mr. Frundles",
				"C-137": "Why are you here?",
			},
			expectedErrored: map[string]error{
				"5126": os.ErrNotExist,
			},
		},
		{
			desc:   "prefix can be empty",
			prefix: "",
			filesToUpload: map[string]string{
				"C-131": "I am Mr. Frundles",
				"C-137": "Why are you here?",
				"5126":  "", // missing on local
			},
			expectedSuccessful: map[string]string{
				"C-131": "I am Mr. Frundles",
				"C-137": "Why are you here?",
			},
			expectedErrored: map[string]error{
				"5126": os.ErrNotExist,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			sink := setupEmptyLocalBucket(t)
			defer closeBucket(t, sink)
			localDir := testhelper.TempDir(t)

			for fileName, content := range tc.filesToUpload {
				if content != "" {
					err := os.WriteFile(filepath.Join(localDir, fileName), []byte(content), mode.File)
					require.NoError(t, err)
				}

				objectName := fileName
				err := sink.Upload(ctx, filepath.Join(localDir, objectName), tc.prefix)
				if err == nil {
					objKey := tc.prefix + "/" + objectName
					attr, err := sink.bucket.Attributes(ctx, objKey)
					require.NoError(t, err)
					require.Equal(t, attr.CacheControl, "no-store, no-transform")
					require.Equal(t, attr.ContentType, "application/octet-stream")

					var builder strings.Builder
					err = sink.bucket.Download(ctx, objKey, &builder, nil)
					require.NoError(t, err)
					require.Equal(t, tc.expectedSuccessful[objectName], builder.String())
				} else {
					require.ErrorIs(t, err, tc.expectedErrored[objectName])
				}
			}
		})
	}

	t.Run("upload duplicated key whose writer finished later wins", func(t *testing.T) {
		ctx := testhelper.Context(t)
		localDirLoser := testhelper.TempDir(t)
		err := os.WriteFile(filepath.Join(localDirLoser, "i_am_key"), []byte("hello"), mode.File)
		require.NoError(t, err)
		localDirWinner := testhelper.TempDir(t)
		err = os.WriteFile(filepath.Join(localDirWinner, "i_am_key"), []byte("world"), mode.File)
		require.NoError(t, err)

		sink := setupEmptyLocalBucket(t)

		err = sink.Upload(ctx, filepath.Join(localDirLoser, "i_am_key"), "some/prefix")
		require.NoError(t, err)
		err = sink.Upload(ctx, filepath.Join(localDirWinner, "i_am_key"), "some/prefix")
		require.NoError(t, err)

		var builder strings.Builder
		err = sink.bucket.Download(ctx, "some/prefix/i_am_key", &builder, nil)
		require.NoError(t, err)
		require.Equal(t, "world", builder.String())
	})

	t.Run("local file path cannot be empty", func(t *testing.T) {
		ctx := testhelper.Context(t)
		sink := setupEmptyLocalBucket(t)
		defer closeBucket(t, sink)

		err := sink.Upload(ctx, "", "some/prefix")
		require.Error(t, err)
	})
}

func TestSink_Upload_Timeout_Cancellation_And_Retry(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc           string
		maxRetry       uint
		overallTimeout time.Duration

		// operationTimeOut is the timer for each operation e.g. upload, download
		operationTimeOut time.Duration
		objectName       string
		simulationData   []simulation
		expectedError    error
	}{
		{
			desc:             "upload failed with overall timer overallTimeout no backoffStrategy",
			maxRetry:         0,
			overallTimeout:   200 * time.Millisecond,
			operationTimeOut: 5 * time.Second,
			objectName:       "overall_timeout_obj_key",
			simulationData: []simulation{
				{Delay: 1 * time.Second, Err: nil},
			},
			expectedError: errSimulationCanceled,
		},
		{
			desc:             "upload with first two attempts timing out and last backoffStrategy succeeding",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 400 * time.Millisecond,
			objectName:       "success_with_retry",
			simulationData: []simulation{
				{5 * time.Second, nil},      // this will timeout
				{1 * time.Millisecond, nil}, // this should succeed
			},
			expectedError: nil,
		},
		{
			desc:             "upload failed with operation timer overallTimeout and with backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 100 * time.Millisecond,
			objectName:       "failed_even_retry",
			simulationData: []simulation{
				{1 * time.Second, nil},
				{1 * time.Second, nil},
			},
			expectedError: errSimulationCanceled,
		},
		{
			desc:             "upload with first two attempts canceled and last backoffStrategy succeeding",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 10 * time.Second,
			objectName:       "success_with_cancel_and_retry",
			simulationData: []simulation{
				{100 * time.Millisecond, errSimulationCanceled},
				{100 * time.Millisecond, nil},
			},
			expectedError: nil,
		},
		{
			desc:             "upload failed with simulated cancel and backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 10 * time.Second,
			objectName:       "success_with_cancel_and_retry",
			simulationData: []simulation{
				{100 * time.Millisecond, errSimulationCanceled},
				{100 * time.Millisecond, errSimulationCanceled},
			},
			expectedError: errSimulationCanceled,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)

			prefix := "some/prefix"
			localBucket := testhelper.TempDir(t)
			localBucketURI := fmt.Sprintf("file://%s", localBucket)
			bucket, err := blob.OpenBucket(ctx, localBucketURI)
			require.NoError(t, err)

			objectKey := fmt.Sprintf("%s/%s", prefix, tc.objectName)
			simulationData := map[string][]simulation{
				objectKey: tc.simulationData,
			}
			simulatedBucket, err := newSimulationBucket(bucket, simulationData)
			require.NoError(t, err)

			sink, err := NewSink(simulatedBucket,
				WithOverallTimout(tc.overallTimeout),
				WithMaxRetry(tc.maxRetry),
				WithRetryTimeout(tc.operationTimeOut),
				WithBackoffStrategy(&backoffStrategyInTest))
			require.NoError(t, err)
			defer closeBucket(t, sink)

			localDir := testhelper.TempDir(t)
			err = os.WriteFile(filepath.Join(localDir, tc.objectName), []byte("Go long!"), mode.File)
			require.NoError(t, err)

			err = sink.Upload(ctx, filepath.Join(localDir, tc.objectName), prefix)

			if tc.expectedError != nil {
				require.ErrorIs(t, err, errSimulationCanceled)
				// Add delay here to ensure any pending operations complete
				time.Sleep(50 * time.Millisecond)
				listRes, err := sink.List(ctx, prefix)
				require.NoError(t, err)
				require.Empty(t, listRes)
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("upload failed with overall cancel triggered no backoffStrategy", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)

		prefix := "some/prefix"
		objectName := "overall_cancel_called"
		localBucket := testhelper.TempDir(t)
		localBucketURI := fmt.Sprintf("file://%s", localBucket)
		bucket, err := blob.OpenBucket(ctx, localBucketURI)
		require.NoError(t, err)
		objectKey := fmt.Sprintf("%s/%s", prefix, objectName)
		simulation := map[string][]simulation{
			objectKey: {
				{1 * time.Second, nil},
			},
		}
		simulatedBucket, err := newSimulationBucket(bucket, simulation)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(ctx)

		// make the overall timer and operation timer are long enough
		sink, err := NewSink(simulatedBucket, WithOverallTimout(100*time.Second), WithNoRetry(),
			WithRetryTimeout(10*time.Second), WithBackoffStrategy(&backoffStrategyInTest))
		require.NoError(t, err)
		defer closeBucket(t, sink)

		localDir := testhelper.TempDir(t)
		err = os.WriteFile(filepath.Join(localDir, objectName), []byte("Go long!"), mode.File)
		require.NoError(t, err)

		errCh := make(chan error)
		go func() {
			errCh <- sink.Upload(ctx, filepath.Join(localDir, objectName), prefix)
		}()

		// Add a small delay before cancellation to ensure operation has started
		time.Sleep(50 * time.Millisecond)

		// Trigger the cancellation of the context to stop the ongoing operation.
		cancel()

		// Add a timeout to prevent test from hanging
		select {
		case err = <-errCh:
			require.ErrorIs(t, err, errSimulationCanceled)
		case <-time.After(5 * time.Second):
			t.Fatal("Test timed out waiting for cancellation")
		}
	})
}

func TestSink_Download(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	for _, tc := range []struct {
		desc             string
		queryPrefix      string
		queryObjectNames []string
		expectedContent  map[string]string
		expectedError    map[string]gcerrors.ErrorCode
	}{
		{
			desc:             "download all objects",
			queryPrefix:      "jerry",
			queryObjectNames: []string{"C-131", "C-137"},
			expectedContent: map[string]string{
				"C-131": "I am Mr. Frundles",
				"C-137": "Why are you here?",
			},
		},
		{
			desc:             "Download nonexistent objects",
			queryPrefix:      "jerry",
			queryObjectNames: []string{"5126"},
			expectedContent:  map[string]string{},
			expectedError: map[string]gcerrors.ErrorCode{
				"5126": gcerrors.NotFound,
			},
		},
		{
			desc:             "Download nonexistent folder",
			queryPrefix:      "nonexistent",
			queryObjectNames: []string{"C-131"},
			expectedContent:  map[string]string{},
			expectedError: map[string]gcerrors.ErrorCode{
				"C-131": gcerrors.NotFound,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			var sink *Sink

			prefix := "jerry"
			objects := []fileBucketData{
				{
					ObjectName: "C-131",
					Content:    "I am Mr. Frundles",
					WriterOpt: blob.WriterOptions{
						ContentType:  "application/octet-stream",
						CacheControl: "no-store, no-transform",
					},
				},
				{
					ObjectName: "C-137",
					Content:    "Why are you here?",
					WriterOpt: blob.WriterOptions{
						ContentType:  "text/html; charset=utf-8",
						CacheControl: "no-store, no-transform",
					},
				},
			}
			sink, _ = setupLocalBucketWithData(t, prefix, objects)
			defer closeBucket(t, sink)
			localDir := testhelper.TempDir(t)

			for _, obj := range tc.queryObjectNames {
				objectKey := fmt.Sprintf("%s/%s", tc.queryPrefix, obj)
				localFullPath := filepath.Join(localDir, obj)
				err := sink.Download(ctx, objectKey, localFullPath)
				if err == nil {
					buf, err := os.ReadFile(localFullPath)
					require.NoError(t, err)
					require.Equal(t, tc.expectedContent[obj], string(buf))
				} else {
					require.Equal(t, tc.expectedError[obj], gcerrors.Code(err))
					require.NoFileExists(t, localFullPath)
				}
			}
		})
	}
}

func TestSink_Download_Timeout_Cancellation_And_Retry(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc           string
		maxRetry       uint
		overallTimeout time.Duration

		// operationTimeOut is the timer for each operation e.g. upload, download
		operationTimeOut time.Duration
		objectName       string
		objectContent    string
		simulationData   []simulation
		expectedError    error
	}{
		{
			desc:             "download failed with overall timer overallTimeout no backoffStrategy",
			maxRetry:         0,
			overallTimeout:   100 * time.Millisecond,
			operationTimeOut: 5 * time.Second,
			objectName:       "overall_timeout_obj_key",
			objectContent:    "it's about time",
			simulationData: []simulation{
				{Delay: 10 * time.Second, Err: nil},
			},
			expectedError: errSimulationCanceled,
		},
		{
			desc:             "down success with some timeouts and backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 100 * time.Millisecond,
			objectName:       "success_with_retry",
			objectContent:    "with some network jitter, we made it",
			simulationData: []simulation{
				{300 * time.Millisecond, nil}, // this will overallTimeout
				{10 * time.Millisecond, nil},  // this should succeed
			},
			expectedError: nil,
		},
		{
			desc:             "download failed with operation overallTimeout and with backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 50 * time.Millisecond,
			objectName:       "failed_even_retry",
			objectContent:    "we had a terrible network",
			simulationData: []simulation{
				{1 * time.Second, nil},
				{1 * time.Second, nil},
			},
			expectedError: errSimulationCanceled,
		},
		{
			desc:             "down success with simulated cancel and backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 10 * time.Second,
			objectName:       "success_with_cancel_and_retry",
			simulationData: []simulation{
				{100 * time.Millisecond, errSimulationCanceled},
				{100 * time.Millisecond, nil},
			},
			expectedError: nil,
		},
		{
			desc:             "download failed with simulated cancel and backoffStrategy",
			maxRetry:         2,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 10 * time.Second,
			objectName:       "success_with_cancel_and_retry",
			simulationData: []simulation{
				{100 * time.Millisecond, errSimulationCanceled},
				{100 * time.Millisecond, errSimulationCanceled},
				{100 * time.Millisecond, errSimulationCanceled},
			},
			expectedError: errSimulationCanceled,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			prefix := "some/prefix"
			objects := []fileBucketData{
				{
					ObjectName: tc.objectName,
					Content:    tc.objectContent,
					WriterOpt: blob.WriterOptions{
						ContentType:  "application/octet-stream",
						CacheControl: "no-store, no-transform",
					},
				},
			}
			_, bucket := setupLocalBucketWithData(t, prefix, objects)

			objectKey := fmt.Sprintf("%s/%s", prefix, tc.objectName)
			simulationData := map[string][]simulation{
				objectKey: tc.simulationData,
			}
			simulatedBucket, err := newSimulationBucket(bucket, simulationData)
			require.NoError(t, err)
			sink, err := NewSink(simulatedBucket,
				WithOverallTimout(tc.overallTimeout),
				WithMaxRetry(tc.maxRetry),
				WithRetryTimeout(tc.operationTimeOut),
				WithBackoffStrategy(&backoffStrategyInTest),
			)
			require.NoError(t, err)
			defer closeBucket(t, sink)

			localDir := testhelper.TempDir(t)
			localFullPath := filepath.Join(localDir, tc.objectName)
			err = sink.Download(ctx, objectKey, localFullPath)

			if tc.expectedError != nil {
				require.ErrorIs(t, err, tc.expectedError)
				require.NoFileExists(t, localFullPath)
			} else {
				buf, err := os.ReadFile(localFullPath)
				require.NoError(t, err)
				require.Equal(t, tc.objectContent, string(buf))
			}
		})
	}

	t.Run("download failed with overall cancel triggered no backoffStrategy", func(t *testing.T) {
		prefix := "some/prefix"
		objectName := "overall_cancel_called"
		objectContent := "I can't wait forever"
		objects := []fileBucketData{
			{
				ObjectName: objectName,
				Content:    objectContent,
				WriterOpt: blob.WriterOptions{
					ContentType:  "application/octet-stream",
					CacheControl: "no-store, no-transform",
				},
			},
		}
		_, bucket := setupLocalBucketWithData(t, prefix, objects)

		objectKey := fmt.Sprintf("%s/%s", prefix, objectName)
		simulationData := map[string][]simulation{
			objectKey: {
				{1 * time.Second, nil},
			},
		}
		simulatedBucket, err := newSimulationBucket(bucket, simulationData)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(ctx)
		sink, err := NewSink(simulatedBucket, WithOverallTimout(defaultOverallTimeout),
			WithNoRetry(), WithRetryTimeout(defaultRetryTimeout),
			WithBackoffStrategy(&backoffStrategyInTest))
		require.NoError(t, err)
		defer closeBucket(t, sink)

		localDir := testhelper.TempDir(t)
		localFullPath := filepath.Join(localDir, objectName)
		errCh := make(chan error)
		go func() {
			errCh <- sink.Download(ctx, objectKey, localFullPath)
		}()

		// Trigger the cancellation of the context to stop the ongoing operation.
		cancel()
		err = <-errCh
		require.ErrorIs(t, err, errSimulationCanceled)
	})
}

func TestSink_Delete(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc            string
		queryingPrefix  string
		createBucket    bool
		bucket          string
		prefix          string
		objects         []fileBucketData
		objectsToDelete []string
		objectsLeft     []string
		expectedRes     map[string]gcerrors.ErrorCode
	}{
		{
			desc:           "delete all objects",
			queryingPrefix: "jerry",
			prefix:         "jerry",
			objects: []fileBucketData{
				{
					ObjectName: "C-131",
					Content:    "I am Mr. Frundles",
					WriterOpt: blob.WriterOptions{
						ContentType:  "application/octet-stream",
						CacheControl: "no-store, no-transform",
					},
				},
				{
					ObjectName: "C-137",
					Content:    "Why are you here?",
					WriterOpt: blob.WriterOptions{
						ContentType:  "text/html; charset=utf-8",
						CacheControl: "no-store, no-transform",
					},
				},
			},
			objectsToDelete: []string{"C-131", "C-137"},
			expectedRes:     map[string]gcerrors.ErrorCode{},
		},
		{
			desc:           "delete some objects",
			queryingPrefix: "jerry",
			prefix:         "jerry",
			objects: []fileBucketData{
				{
					ObjectName: "C-131",
					Content:    "I am Mr. Frundles",
					WriterOpt: blob.WriterOptions{
						ContentType:  "application/octet-stream",
						CacheControl: "no-store, no-transform",
					},
				},
				{
					ObjectName: "C-137",
					Content:    "Why are you here?",
					WriterOpt: blob.WriterOptions{
						ContentType:  "text/html; charset=utf-8",
						CacheControl: "no-store, no-transform",
					},
				},
			},
			objectsToDelete: []string{"C-131"},
			objectsLeft:     []string{"C-137"},
			expectedRes:     map[string]gcerrors.ErrorCode{},
		},
		{
			desc:           "delete more objects than you have",
			queryingPrefix: "jerry",
			prefix:         "jerry",
			objects: []fileBucketData{
				{
					ObjectName: "C-131",
					Content:    "I am Mr. Frundles",
					WriterOpt: blob.WriterOptions{
						ContentType:  "application/octet-stream",
						CacheControl: "no-store, no-transform",
					},
				},
				{
					ObjectName: "C-137",
					Content:    "Why are you here?",
					WriterOpt: blob.WriterOptions{
						ContentType:  "text/html; charset=utf-8",
						CacheControl: "no-store, no-transform",
					},
				},
			},
			objectsToDelete: []string{"C-131", "C-137", "5126"},
			objectsLeft:     []string{},
			expectedRes: map[string]gcerrors.ErrorCode{
				"jerry/5126": gcerrors.NotFound,
			},
		},
		{
			desc:           "delete nonexistent prefix",
			queryingPrefix: "nonexistent",
			prefix:         "jerry",
			objects: []fileBucketData{
				{
					ObjectName: "C-131",
					Content:    "I am Mr. Frundles",
					WriterOpt: blob.WriterOptions{
						ContentType:  "application/octet-stream",
						CacheControl: "no-store, no-transform",
					},
				},
			},
			objectsToDelete: []string{"C-131"},
			objectsLeft:     []string{"C-131"},
			expectedRes: map[string]gcerrors.ErrorCode{
				"nonexistent/C-131": gcerrors.NotFound,
			},
		},
		{
			desc:            "empty bucket",
			queryingPrefix:  "i-am-empty",
			prefix:          "i-am-empty",
			objects:         []fileBucketData{},
			objectsToDelete: []string{"C-131"},
			objectsLeft:     []string{},
			expectedRes: map[string]gcerrors.ErrorCode{
				"i-am-empty/C-131": gcerrors.NotFound,
			},
		},
		{
			desc:           "delete parameter is empty",
			queryingPrefix: "jerry",
			prefix:         "jerry",
			objects: []fileBucketData{
				{
					ObjectName: "C-131",
					Content:    "I am Mr. Frundles",
					WriterOpt: blob.WriterOptions{
						ContentType:  "application/octet-stream",
						CacheControl: "no-store, no-transform",
					},
				},
				{
					ObjectName: "C-137",
					Content:    "Why are you here?",
					WriterOpt: blob.WriterOptions{
						ContentType:  "text/html; charset=utf-8",
						CacheControl: "no-store, no-transform",
					},
				},
			},
			objectsToDelete: []string{},
			expectedRes:     map[string]gcerrors.ErrorCode{},
			objectsLeft:     []string{"C-131", "C-137"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			var sink *Sink
			sink, _ = setupLocalBucketWithData(t, tc.prefix, tc.objects)
			defer closeBucket(t, sink)

			actualResWithErrorCode := make(map[string]gcerrors.ErrorCode)
			res := sink.DeleteObjects(ctx, tc.queryingPrefix, tc.objectsToDelete)
			for k, v := range res {
				actualResWithErrorCode[k] = gcerrors.Code(v)
			}
			require.Equal(t, tc.expectedRes, actualResWithErrorCode)

			actualObjectsLeft, err := sink.List(ctx, tc.prefix)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.objectsLeft, actualObjectsLeft)
		})
	}
}

func TestSink_Delete_Timeout_Cancellation_And_Retry(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc           string
		maxRetry       uint
		overallTimeout time.Duration

		// operationTimeOut is the timer for each operation e.g. upload, download
		operationTimeOut time.Duration
		objectName       string
		simulationData   []simulation
		expectedError    error
	}{
		{
			desc:             "delete failed with overall timer overallTimeout no backoffStrategy",
			maxRetry:         0,
			overallTimeout:   100 * time.Millisecond,
			operationTimeOut: 5 * time.Second,
			objectName:       "overall_timeout_obj_key",
			simulationData: []simulation{
				{Delay: 10 * time.Second, Err: nil},
			},
			expectedError: errSimulationCanceled,
		},
		{
			desc:             "delete success with some timeouts and backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 100 * time.Millisecond,
			objectName:       "success_with_retry",
			simulationData: []simulation{
				{300 * time.Millisecond, nil}, // this will overallTimeout
				{10 * time.Millisecond, nil},  // this should succeed
			},
			expectedError: nil,
		},
		{
			desc:             "delete failed with operation timout and with backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 50 * time.Millisecond,
			objectName:       "failed_even_retry",
			simulationData: []simulation{
				{1 * time.Second, nil},
				{1 * time.Second, nil},
			},
			expectedError: errSimulationCanceled,
		},
		{
			desc:             "delete success with simulated cancel and backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 10 * time.Second,
			objectName:       "success_with_cancel_and_retry",
			simulationData: []simulation{
				{100 * time.Millisecond, errSimulationCanceled},
				{100 * time.Millisecond, nil},
			},
			expectedError: nil,
		},
		{
			desc:             "delete failed with simulated cancel and backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 10 * time.Second,
			objectName:       "success_with_cancel_and_retry",
			simulationData: []simulation{
				{100 * time.Millisecond, errSimulationCanceled},
				{100 * time.Millisecond, errSimulationCanceled},
			},
			expectedError: errSimulationCanceled,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			prefix := "some/prefix"
			objects := []fileBucketData{
				{
					ObjectName: tc.objectName,
					Content:    "I am Mr. Frundles",
					WriterOpt: blob.WriterOptions{
						ContentType:  "application/octet-stream",
						CacheControl: "no-store, no-transform",
					},
				},
			}
			_, bucket := setupLocalBucketWithData(t, prefix, objects)

			objectKey := fmt.Sprintf("%s/%s", prefix, tc.objectName)
			simulation := map[string][]simulation{
				objectKey: tc.simulationData,
			}
			simulatedBucket, err := newSimulationBucket(bucket, simulation)
			require.NoError(t, err)
			sink, err := NewSink(simulatedBucket,
				WithOverallTimout(tc.overallTimeout),
				WithMaxRetry(tc.maxRetry),
				WithRetryTimeout(tc.operationTimeOut),
				WithBackoffStrategy(&backoffStrategyInTest),
			)
			require.NoError(t, err)
			defer closeBucket(t, sink)

			res := sink.DeleteObjects(ctx, prefix, []string{objectKey})

			if tc.expectedError != nil {
				require.ErrorIs(t, res[objectKey], tc.expectedError)
			} else {
				require.NoError(t, res[objectKey])
				actualObjectsLeft, err := sink.List(ctx, prefix)
				require.NoError(t, err)
				require.Empty(t, actualObjectsLeft)
			}
		})
	}

	t.Run("download failed with overall cancel triggered no backoffStrategy", func(t *testing.T) {
		prefix := "some/prefix"
		objectName := "overall_cancel_called"
		objectContent := "I can't wait forever"
		objects := []fileBucketData{
			{
				ObjectName: objectName,
				Content:    objectContent,
				WriterOpt: blob.WriterOptions{
					ContentType:  "application/octet-stream",
					CacheControl: "no-store, no-transform",
				},
			},
		}
		_, bucket := setupLocalBucketWithData(t, prefix, objects)

		objectKey := fmt.Sprintf("%s/%s", prefix, objectName)
		simulation := map[string][]simulation{
			objectKey: {
				{1 * time.Second, nil},
			},
		}
		simulatedBucket, err := newSimulationBucket(bucket, simulation)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(ctx)
		sink, err := NewSink(simulatedBucket, WithOverallTimout(defaultOverallTimeout),
			WithNoRetry(), WithRetryTimeout(defaultRetryTimeout),
			WithBackoffStrategy(&backoffStrategyInTest))
		require.NoError(t, err)
		defer closeBucket(t, sink)

		errCh := make(chan map[string]error)
		go func() {
			errCh <- sink.DeleteObjects(ctx, prefix, []string{objectKey})
		}()

		// Trigger the cancellation of the context to stop the ongoing operation.
		cancel()
		res := <-errCh
		require.ErrorIs(t, res[objectKey], errSimulationCanceled)
	})
}

// Test List
func TestSink_List(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc           string
		queryingPrefix string
		createBucket   bool
		bucket         string
		prefix         string
		objects        []fileBucketData
		expectedRes    []string
		expectedErr    error
	}{
		{
			desc:           "bucket with objects",
			queryingPrefix: "jerry",
			prefix:         "jerry",
			objects: []fileBucketData{
				{
					ObjectName: "C-131",
					Content:    "I am Mr. Frundles",
					WriterOpt: blob.WriterOptions{
						ContentType:  "application/octet-stream",
						CacheControl: "no-store, no-transform",
					},
				},
				{
					ObjectName: "C-137",
					Content:    "Why are you here?",
					WriterOpt: blob.WriterOptions{
						ContentType:  "text/html; charset=utf-8",
						CacheControl: "no-store, no-transform",
					},
				},
			},
			expectedRes: []string{"C-131", "C-137"},
			expectedErr: nil,
		},
		{
			// The "/" will not affect the result.
			desc:           "prefix with / ending",
			queryingPrefix: "jerry/",
			prefix:         "jerry",
			objects: []fileBucketData{
				{
					ObjectName: "C-131",
					Content:    "I am Mr. Frundles",
					WriterOpt: blob.WriterOptions{
						ContentType:  "application/octet-stream",
						CacheControl: "no-store, no-transform",
					},
				},
				{
					ObjectName: "C-137",
					Content:    "Why are you here?",
					WriterOpt: blob.WriterOptions{
						ContentType:  "text/html; charset=utf-8",
						CacheControl: "no-store, no-transform",
					},
				},
			},
			expectedRes: []string{"C-131", "C-137"},
			expectedErr: nil,
		},
		{
			desc:           "nonexistent prefix in a bucket with objects",
			queryingPrefix: "nonexistent",
			prefix:         "jerry",
			objects: []fileBucketData{
				{
					ObjectName: "C-131",
					Content:    "I am Mr. Frundles",
					WriterOpt: blob.WriterOptions{
						ContentType:  "application/octet-stream",
						CacheControl: "no-store, no-transform",
					},
				},
			},
			expectedRes: []string{},
			expectedErr: nil,
		},
		{
			desc:           "empty bucket",
			queryingPrefix: "i-am-empty",
			prefix:         "i-am-empty",
			objects:        []fileBucketData{},
			expectedRes:    []string{},
			expectedErr:    nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			var sink *Sink
			sink, _ = setupLocalBucketWithData(t, tc.prefix, tc.objects)
			defer closeBucket(t, sink)

			res, err := sink.List(ctx, tc.queryingPrefix)
			require.NoError(t, err)

			require.Equal(t, tc.expectedRes, res)
		})
	}
}

func TestSink_List_Timeout_Cancellation_And_Retry(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc           string
		maxRetry       uint
		overallTimeout time.Duration

		// operationTimeOut is the timer for each operation e.g. upload, download
		operationTimeOut time.Duration
		objectName       string
		simulationData   []simulation
		expectedError    error
	}{
		{
			desc:             "list failed with overall timer overallTimeout no backoffStrategy",
			maxRetry:         0,
			overallTimeout:   100 * time.Millisecond,
			operationTimeOut: 5 * time.Second,
			objectName:       "overall_timeout_obj_key",
			simulationData: []simulation{
				{Delay: 10 * time.Second, Err: nil},
			},
			expectedError: errSimulationCanceled,
		},
		{
			desc:             "list success with some timeouts and backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 100 * time.Millisecond,
			objectName:       "success_with_retry",
			simulationData: []simulation{
				{1 * time.Second, nil},      // this will overallTimeout
				{1 * time.Millisecond, nil}, // this should succeed
			},
			expectedError: nil,
		},
		{
			desc:             "list failed with operation overallTimeout and with backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 50 * time.Millisecond,
			objectName:       "failed_even_retry",
			simulationData: []simulation{
				{1 * time.Second, nil},
				{1 * time.Second, nil},
			},
			expectedError: errSimulationCanceled,
		},
		{
			desc:             "list success with simulated cancel and backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 10 * time.Second,
			objectName:       "success_with_cancel_and_retry",
			simulationData: []simulation{
				{100 * time.Millisecond, errSimulationCanceled},
				{100 * time.Millisecond, nil},
			},
			expectedError: nil,
		},
		{
			desc:             "delete failed with simulated cancel and backoffStrategy",
			maxRetry:         1,
			overallTimeout:   60 * time.Second,
			operationTimeOut: 10 * time.Second,
			objectName:       "success_with_cancel_and_retry",
			simulationData: []simulation{
				{100 * time.Millisecond, errSimulationCanceled},
				{100 * time.Millisecond, errSimulationCanceled},
			},
			expectedError: errSimulationCanceled,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			prefix := "some/prefix"
			objects := []fileBucketData{
				{
					ObjectName: tc.objectName,
					Content:    "I am Mr. Frundles",
					WriterOpt: blob.WriterOptions{
						ContentType:  "application/octet-stream",
						CacheControl: "no-store, no-transform",
					},
				},
			}
			_, bucket := setupLocalBucketWithData(t, prefix, objects)

			objectKey := fmt.Sprintf("%s/%s", prefix, tc.objectName)
			simulationData := map[string][]simulation{
				objectKey: tc.simulationData,
			}
			simulatedBucket, err := newSimulationBucket(bucket, simulationData)
			require.NoError(t, err)

			simulatedBucketPtr, ok := simulatedBucket.(*simulationBucket)
			require.True(t, ok)

			sink, err := NewSink(simulatedBucket, WithOverallTimout(tc.overallTimeout),
				WithMaxRetry(tc.maxRetry), WithRetryTimeout(tc.operationTimeOut),
				WithBackoffStrategy(&backoffStrategyInTest))

			require.NoError(t, err)
			defer closeBucket(t, sink)

			// Notice that we are testing against listHandler instead of the actual List function here.
			// The reason is that listHandler can load the intercepted List function, which uses the
			// simulation data defined in interceptedList. This approach is valid because listHandler
			// serves as the underlying implementation of the List function.
			res, err := listHandler(ctx, sink, prefix, simulatedBucketPtr.interceptedList)
			if tc.expectedError != nil {
				require.ErrorIs(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
				require.ElementsMatch(t, res, []string{tc.objectName})
			}
		})
	}

	t.Run("list failed with overall cancel triggered no backoffStrategy", func(t *testing.T) {
		prefix := "some/prefix"
		objectName := "overall_cancel_called"
		objectContent := "I can't wait forever"
		objects := []fileBucketData{
			{
				ObjectName: objectName,
				Content:    objectContent,
				WriterOpt: blob.WriterOptions{
					ContentType:  "application/octet-stream",
					CacheControl: "no-store, no-transform",
				},
			},
		}
		_, bucket := setupLocalBucketWithData(t, prefix, objects)

		objectKey := fmt.Sprintf("%s/%s", prefix, objectName)
		simulation := map[string][]simulation{
			objectKey: {
				{1 * time.Second, nil},
			},
		}
		simulatedBucket, err := newSimulationBucket(bucket, simulation)
		require.NoError(t, err)

		simulatedBucketPtr, ok := simulatedBucket.(*simulationBucket)
		require.True(t, ok)

		ctx, cancel := context.WithCancel(ctx)
		sink, err := NewSink(simulatedBucket)

		require.NoError(t, err)
		defer closeBucket(t, sink)

		errCh := make(chan error)
		go func() {
			// Notice that we are testing against listHandler instead of the actual List function here.
			// The reason is that listHandler can load the intercepted List function, which uses the
			// simulation data defined in interceptedList. This approach is valid because listHandler
			// serves as the underlying implementation of the List function.
			_, err := listHandler(ctx, sink, prefix, simulatedBucketPtr.interceptedList)
			errCh <- err
		}()

		// Trigger the cancellation of the context to stop the ongoing operation.
		cancel()
		res := <-errCh
		require.ErrorIs(t, res, errSimulationCanceled)
	})
}

type fileBucketData struct {
	ObjectName string
	Content    string
	WriterOpt  blob.WriterOptions
}

// setupEmptyLocalBucket initializes an empty Bucket backed by the local file system.
func setupEmptyLocalBucket(t *testing.T) *Sink {
	ctx := testhelper.Context(t)
	localBucket := testhelper.TempDir(t)
	localBucketURI := fmt.Sprintf("file://%s", localBucket)
	bucket, err := blob.OpenBucket(ctx, localBucketURI)
	require.NoError(t, err)
	// sink, err := NewSinkWithDefaults(bucket)
	sink, err := NewSink(bucket, WithOverallTimout(defaultOverallTimeout),
		WithMaxRetry(defaultMaxRetry), WithRetryTimeout(defaultRetryTimeout),
		WithBackoffStrategy(&constantBackoff{}))
	require.NoError(t, err)
	return sink
}

// setupLocalBucketWithData initializes a Bucket backed by the local file system with data.
func setupLocalBucketWithData(t *testing.T, prefix string, objectsToUpload []fileBucketData) (*Sink, *blob.Bucket) {
	ctx := testhelper.Context(t)
	bucket := testhelper.TempDir(t)
	localBucketURI := fmt.Sprintf("file://%s", bucket)
	localBucket, err := blob.OpenBucket(ctx, localBucketURI)
	require.NoError(t, err)
	// sink, err := NewSinkWithDefaults(localBucket)
	sink, err := NewSink(localBucket, WithOverallTimout(defaultOverallTimeout),
		WithMaxRetry(defaultMaxRetry), WithRetryTimeout(defaultRetryTimeout),
		WithBackoffStrategy(&constantBackoff{}))
	require.NoError(t, err)

	for _, obj := range objectsToUpload {
		objectKey := filepath.Join(prefix, obj.ObjectName)
		content := strings.NewReader(obj.Content)
		require.NoError(t, err)
		err = sink.bucket.Upload(ctx, objectKey, content, &obj.WriterOpt)
		require.NoError(t, err)
	}

	return sink, localBucket
}

func closeBucket(t *testing.T, sink *Sink) {
	err := sink.bucket.Close()
	require.NoError(t, err)
}

// constantBackoff always returns constant backoff time intervals regardless of the backoffStrategy attempts.
type constantBackoff struct{}

func (c *constantBackoff) Backoff(uint) time.Duration {
	return time.Duration(100) * time.Millisecond
}
