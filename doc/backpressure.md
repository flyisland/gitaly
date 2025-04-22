# Request limiting in Gitaly

In the GitLab ecosystem, Gitaly is the service that is at the bottom of the
stack for Git data access. This means that when there is a surge of
requests to retrieve or change a piece of Git data, the I/O happens in Gitaly.
This can lead to Gitaly being overwhelmed due to system resource exhaustion
because all Git access goes through Gitaly.

If there is a surge of traffic beyond what Gitaly can handle, Gitaly should
be able to push back on the client calling. Gitaly shouldn't subserviently agree
to process more than it can handle.

We employ concurrency limiting as our primary backpressure mechanism in Gitaly.

## Concurrency queue

Limit the number of concurrent RPCs that are in flight on each Gitaly node for each
repository per RPC using `[[concurrency]]` configuration:

```toml
[[concurrency]]
rpc = "/gitaly.SmartHTTPService/PostUploadPackWithSidechannel"
max_per_repo = 1
```

For example:

- One clone request comes in for repository "A" (a largish repository).
- While this RPC is executing, another request comes in for repository "A". Because
  `max_per_repo` is 1 in this case, the second request blocks until the first request
  is finished.

An in-memory queue of requests can build up in Gitaly that are waiting their turn. Because
this is a potential vector for a memory leak, two other values in the `[[concurrency]]`
configuration can prevent an unbounded in-memory queue of requests:

- `max_queue_wait` is the maximum amount of time a request can wait in the
  concurrency queue. When a request waits longer than this time, it returns
  an error to the client.
- `max_queue_size` is the maximum size the concurrency queue can grow for a
  given RPC. If a concurrency queue is at its maximum, subsequent requests
  return with an error. The queue size is per repository.

For example:

```toml
[[concurrency]]
rpc = "/gitaly.SmartHTTPService/PostUploadPackWithSidechannel"
max_per_repo = 1
max_queue_wait = "1m"
max_queue_size = 5
```

## Note on Rate Limiting

Rate limiting has been removed from Gitaly. For more information about why and the alternatives, please see [issue #5011](https://gitlab.com/gitlab-org/gitaly/-/issues/5011).

## Errors

With concurrency limiting, Gitaly responds with a structured
gRPC `gitalypb.LimitError` error with:

- A `Message` field that describes the error.
- A `BackoffDuration` field that provides the client with a time when it is safe to retry.
  If 0, it means it should never retry.

Gitaly clients (`gitlab-shell`, `workhorse`, Rails) must parse this error and
return sensible error messages to the end user. For example:

- Something trying to clone using HTTP or SSH.
- The GitLab application.
- Something calling the API.

## Metrics

Metrics are available that provide visibility into how these limits are being applied.
See the [GitLab Documentation](https://docs.gitlab.com/administration/gitaly/monitoring/) for details.
