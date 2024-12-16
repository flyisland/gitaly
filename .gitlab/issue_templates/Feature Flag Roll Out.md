/title [Feature flag] Enable description of feature

<!--

IMPORTANT: when Gitaly is started in a Rails test, all feature flags are enabled:
https://gitlab.com/gitlab-org/gitlab/blob/28bdb35c83fbfcfca09a9d237bfecb71e55b8f9e/spec/support/helpers/gitaly_setup.rb#L69

Any flags which change user-facing behaviour have a chance of causing test failures, which
in turn will block Gitaly deployment MRs. In these situations, you can do the following:

1. Get the Gitaly change reviewed and merged. Validate the Gitaly pipeline is passing on the default branch.
1. Open a Rails MR that simultaneously updates the commit SHA in
   GITALY_SERVER_VERSION (https://gitlab.com/gitlab-org/gitlab/-/blob/master/GITALY_SERVER_VERSION) and
   contains the updated assertions. This will allow tests to execute against a version of Gitaly that
   contains your changes, while updating the version at the same time.
1. Get the Rails MR reviewed and merged.

-->

## What

Enable the `:feature_name` feature flag ...

## Owners

- Team: Gitaly
- Most appropriate slack channel to reach out to: `#g_gitaly`
- Best individual to reach out to: NAME

## Expectations

### What release does this feature occur in first?

### What are we expecting to happen?

### What might happen if this goes wrong?

### What can we monitor to detect problems with this?

<!--

Which dashboards from https://dashboards.gitlab.net are most relevant?
Usually you'd just like a link to the method you're changing in the
dashboard at:

https://dashboards.gitlab.net/d/000000199/gitaly-feature-status

I.e.

1. Open that URL
2. Change "method" to your feature, e.g. UserDeleteTag
3. Copy/paste the URL & change gprd to gstd to monitor staging as well as prod

-->

## Roll Out Steps

- [ ] Enable on staging
    - [ ] Is the required code deployed on staging? ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#is-the-required-code-deployed))
    - [ ] Enable on staging ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#enable-on-staging))
    - [ ] Add ~"featureflag::staging" to this issue ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#feature-flag-labels))
    - [ ] Test on staging ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#test-on-staging))
    - [ ] Verify the feature flag was used by checking Prometheus metric [`gitaly_feature_flag_checks_total`](https://prometheus.gstg.gitlab.net/graph?g0.expr=sum%20by%20(flag)%20(rate(gitaly_feature_flag_checks_total%5B5m%5D))&g0.tab=1&g0.stacked=0&g0.range_input=1h)
- [ ] Enable on production
    - [ ] Is the required code deployed on production? ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#is-the-required-code-deployed))
    - [ ] Progressively enable in production ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#enable-in-production))
    - [ ] Add ~"featureflag::production" to this issue
    - [ ] Verify the feature flag was used by checking Prometheus metric [`gitaly_feature_flag_checks_total`](https://prometheus.gprd.gitlab.net/graph?g0.expr=sum%20by%20(flag)%20(rate(gitaly_feature_flag_checks_total%5B5m%5D))&g0.tab=1&g0.stacked=0&g0.range_input=1h)
- [ ] Create subsequent issues
  - [ ] To default enable the feature flag (optional, only required if backwards-compatibility concerns exist)
    - [ ] [Create issue](https://gitlab.com/gitlab-org/gitaly/-/issues/new?issuable_template=Feature%20Flag%20Default%20Enable) using the `Feature Flag Default Enable` template.
    - [ ] Set milestone to current+1 release
  - [ ] To Remove feature flag
    - [ ] [Create issue](https://gitlab.com/gitlab-org/gitaly/-/issues/new?issuable_template=Feature%20Flag%20Removal) using the `Feature Flag Removal` template.
    - [ ] Set milestone to current+1 (+2 if we created an issue to default enable the flag).

Please refer to the [documentation of feature flags](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#feature-flags) for further information.

/label ~"devops::systems" ~"group::gitaly" ~"feature flag" ~"feature::maintainance" ~"Category:Gitaly" ~"section::enablement" ~"featureflag::disabled"
