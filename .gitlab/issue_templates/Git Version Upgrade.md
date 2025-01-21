/title Rollout Git version vX.XX.X

## Changelog

<!--
Add the changelog related to the new version and how this impacts us. It is especially important to highlight changes that increase the risk for this particular upgrade.
Would be really nice to point out contributions made by the Gitaly team, if any.
-->

## Steps

### Introducing a new Git version

- [ ] Add the new bundled Git version in the [Makefile](/Makefile) ([Reference](https://gitlab.com/gitlab-org/gitaly/-/merge_requests/7184)).
  - [ ] Update the `build-bundled-git` and `install-bundled-git` targets as necessary.
  - [ ] For a minor version bumps and above, add a new target under the `# These targets build specific releases of Git.` section corresponding to the new version.
- [ ] Introduce the new bundled Git execution environment in the [Git package](/internal/git/execution_environment.go) behind a feature flag.
  - [ ] Create an issue for the rollout of the feature flag ([Reference](https://gitlab.com/gitlab-org/gitaly/-/issues/5030)).
  - [ ] Optional: Create a [change request](https://about.gitlab.com/handbook/engineering/infrastructure/change-management/#change-request-workflows) in case the new Git version contains changes that may cause issues.
  - [ ] Roll out the feature flag (Announce to #g_gitaly and #g_git).
  - [ ] Default enable the feature flag once the upgrade is deemed stable. You can do this in same release that added the feature flag initially.
  - [ ] Remove the feature flag after there has been at least one release with the feature flag default enabled. This is not a requirement since we bundle Git with Gitaly, but more of a safety precaution taken to ensure there is sufficient time for rollbacks/reverts.

### Removing an old Git version

- [ ] Remove the old bundled Git version from the [Makefile](/Makefile)
  - [ ] Update the `build-bundled-git` and `install-bundled-git` targets as necessary.
  - [ ] Remove any unused targets under the `# These targets build specific releases of Git.` section corresponding to the old version.
- [ ] Remove the old bundled Git execution environment from the [Git package](/internal/git/execution_environment.go).

### Upgrading the minimum required Git version

Optional: This is only needed when we want to start using features that have been introduced with the new Git version.

- [ ] Update the minimum required Git version in the [Git package](/internal/git/version.go). ([Reference](https://gitlab.com/gitlab-org/gitaly/-/merge_requests/5705))
- [ ] Update the minimum required Git version in the [README.md](/README.md).
- [ ] Update the GitLab release notes to reflect the new minimum required Git version. ([Reference](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/107565).

/label ~"Category:Gitaly" ~"group::gitaly" ~"group::gitaly::git" ~"section::enablement" ~"devops::systems" ~"type::maintenance" ~"maintenance::dependency"
/label ~"workflow::ready for development"
