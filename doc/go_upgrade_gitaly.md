# Upgrading Go used by Gitaly

Before following the process for upgrading the Go version used by Gitaly, for a general understanding of managing Go versions in
GitLab projects, see [Managing Go versions](https://docs.gitlab.com/development/go_guide/go_upgrade).

## Prerequisites

Before upgrading the version of Go that is used by Gitaly to a specific version, the version must be already supported in these projects:

- [`gitlab-build-images`](https://gitlab.com/gitlab-org/gitlab-build-images): The build infrastructure must support the target Go version.
  For an example, see [`gitlab-build-images` merge request 910](https://gitlab.com/gitlab-org/gitlab-build-images/-/merge_requests/910).
- [`CNG`](https://gitlab.com/gitlab-org/build/CNG): Cloud Native GitLab container images (CNG) must provide the required Go version runtime
  environment before Gitaly containers can use it. For an example, see
  [`CNG` merge request 2360](https://gitlab.com/gitlab-org/build/CNG/-/merge_requests/2360).

## Upgrade Go for Gitaly

To upgrade the version of Go used by Gitaly:

1. Set the target version in the following files:
   - `.gitlab-ci.yml`: CI/CD pipeline Go version variables.
   - `.tool-versions`: Go version for `asdf` and `mise` version manager.
   - `README.md`: Documentation stating minimum Go requirement.
   - `go.mod`: Main project Go module file.
   - All `tools/**/go.mod` files because they're separate Go modules with their own version settings.
1. Raise a merge request with the changes.
