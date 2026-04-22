# Bundle URI

Git supports bundling a repository into a [bundle](https://git-scm.com/docs/git-bundle).
A [bundle](https://git-scm.com/docs/gitformat-bundle) is essentially a file that stores a packfile along with some extra
metadata, including a set of refs and a (possibly empty) set of necessary commits.

The [bundle URI](https://git-scm.com/docs/bundle-uri) feature is a mechanism by which Git takes advantage of those bundles to reduce the load on the Git
server during a `git clone` command: assuming a bundle already exists for a repository, the Git server can return the URI
of the bundle to the client prior to the [negotiation](https://git-scm.com/docs/pack-protocol/2.2.3#_packfile_negotiation)
process by which a client asks the server for the objects it needs. Thus, the client can download the bundle, process it,
and request from the Git server only the objects it needs that were not in the bundle, which reduces the amount of work
the Git server has to do to generate packfiles for this client. This feature can drastically reduce the load on a Git
server, especially when frequent clones are made for a large repository.

## How Gitaly uses bundle URI

Gitaly supports bundle URI for `git clone` to reduce the load on the server.

It does so in two different ways.

### Manual bundle

Gitaly has a `bundle-uri` command that can be used to create a bundle for a repository. For more information on the
command, see [the Gitaly user documentation](https://docs.gitlab.com/administration/gitaly/bundle_uris).
Once a bundle has been manually generated for a given repository, any subsequent `git clone` command for this repository
will return the bundle URI provided that the [feature flags](#2-enable-feature-flags) are turned on.

### Automatic bundle

Gitaly can also [automatically generate bundles](#automatic-bundle-generation) based on heuristics. Because
the idea behind `bundle-uri` is to speed up clones by unloading the Gitaly server from the task of generating the same
packfile multiple times, Gitaly can detect periods of time when clones are frequent (such as a running CI/CD pipelines)
for a given repository and automatically generate a bundle to allow further clones to use the bundle.

> [!NOTE]
> Gitaly generates a bundle only for the default branch (`HEAD`).

## Enabling bundle URI

Enabling bundle URI in Gitaly is a two-step process.

### 1. Configure the bundle bucket

You must configure a bucket to store the bundles. Here is a snippet of the configuration to use:

```toml
# Bundle-URI
[bundle_uri]
# The destination object-storage URL.
go_cloud_url = "gs://my-bundle-uri-bucket"
```

The value of `bundle_uri.go_cloud_url` must be a cloud bucket URL. We [use the `gocloud.dev` library](https://gocloud.dev/howto/blob/#using)
under the hood.

> [!important]
> If the bucket is not set (or set to an empty string), bundle URI is disabled.

### 2. Enable feature flags

You must enable two feature flags to enable `bundle URI`:

- `bundle_generation`: enables [auto-generation](#automatic-bundle) of bundles.
- `bundle_uri`: makes Gitaly advertise bundle URIs when they are available (either manually created or auto-generated) and allows the user to
  [manually](#manual-bundle) generate bundles.

## Automatic bundle generation

Gitaly can generate bundles automatically using heuristics. Gitaly makes that decision through an abstraction called a
[`Strategy`](https://gitlab.com/gitlab-org/gitaly/-/blob/master/internal/bundleuri/strategy.go?ref_type=heads), which is currently implemented in
[`OccurenceStrategy`](https://gitlab.com/gitlab-org/gitaly/-/blob/master/internal/bundleuri/strategy_occurences.go?ref_type=heads).

This strategy has an `Evaluate` method that can be called to determine if a bundle should be generated for a given
repository. To perform that evaluation, the strategy keeps track of the number of times (occurrences) the `Evaluate`
method was called for each repository. If the number of occurrences reaches a certain `threshold` in a given time
`interval`, a bundle is generated. The strategy also keeps track of the last time Gitaly generated a bundle for a repository.
When a new bundle should be generated, based on the occurrences, the strategy looks at the last time a bundle was
generated for the given repository. It will only generate a new bundle if the existing bundle is older than some
`maxBundleAge` configuration.

This strategy assumes that if clones are frequent for a given repository in a short amount of time,
it must be that some process such as a CI/CD pipeline is running for this repository. Thus, generating a bundle during
that time will allow future CI/CD jobs in the pipeline to use the bundle, thus reducing the load on the server. The
bundle will also be available for the next pipelines until it is [cleaned up from the cloud storage](#cleaning-up-old-bundles-from-the-storage).

## Cleaning up old bundles from the storage

Currently, Gitaly does not handle cleaning up old bundles from the cloud storage. This responsibility is delegated
to the cloud storage solution. The bucket used to store the bundles should be configured with object lifecycle management
policies that allows for the cleaning up of old resources. On GitLab.com, bundles are deleted after
24 hours from their creation timestamp.

For GCP, you can take a look at the [object lifecycle management documentation](https://cloud.google.com/storage/docs/lifecycle).
