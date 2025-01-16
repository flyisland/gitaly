# Gitaly Team production training

This is a collection of items that should prepare new team members to be effective in understanding production issues and thus join the [on-call rotation](https://about.gitlab.com/handbook/engineering/infrastructure-platforms/data-access/gitaly#gitaly-oncall-rotation).

While this can be started at any time, team members should complete their onboarding first, and have some experience in the codebase before completing this process.

**This is not a test, it's an interactive learning guide.** It's quite normal and expected to ask for help and to discuss different approaches.

## Setup

- [ ] Set the title to `Gitaly Team production training: <your name here>`

## Links

Skim/read through these, and use them as references.

- [Debugging Gitaly](https://handbook.gitlab.com/handbook/engineering/infrastructure-platforms/data-access/gitaly/debug/)
- [Managing monorepos](https://docs.gitlab.com/ee/user/project/repository/monorepos/)
- [GCP project layout for Gitaly VMs](https://gitlab.com/gitlab-com/gl-infra/readiness/-/blob/master/library/gitaly-multi-project/README.md)

Please help correct, clarify, or otherwise improve any documentation you find lacking (including this template)!

## Questions

Please edit this section like a workbook, adding not just the answer but also how you got there.

- [ ] Find a [senior team member](https://handbook.gitlab.com/handbook/engineering/infrastructure-platforms/data-access/gitaly/) to review and discuss this work, and assign them here.

### Statistics

- [ ] What was Gitaly's SLO availability last month?
- [ ] Which repository had the most errors in `gprd` last week?
- [ ] What was the top error they had? If it's a real bug, please file an issue. :slight_smile:
- [ ] What was the p95 latency of the `SSHUploadPackWithSidechannel` RPC in `gstg` last week?
- [ ] Which Gitaly node had the most performance issues last week?
- [ ] Which RPC handlers spent the most CPU time in the last week? Is there an overarching theme amongst them? Hint: we export profiling metrics to GCP.

### Feature flags

- [ ] When was the last feature flag enabled on `gprd`?
- [ ] ...How would you roll it back?
- [ ] Are there any feature flags currently rolling out? What stage are they in?

### Releases

- [ ] There's a bug in `git` and it needs to be rolled back. Describe the process; think about what stage of being rolled out the broken `git` is in. [HINT](https://gitlab.com/gitlab-org/gitaly/-/blob/master/.gitlab/issue_templates/Git%20Version%20Upgrade.md)
- [ ] What version of Gitaly is running currently in `gprd`?
- [ ] Pick a MR from last month -- did it make it into the last release?

### Git Operations

- [ ] Scenario: clones are "slow". [HINT](https://log.gprd.gitlab.net/app/r/s/zoX53)
  - [ ] How much memory was consumed for clones (http or ssh) for the gitlab-org/gitlab repository in the past hour?
  - [ ] How much CPU was consumed for all clones (http or ssh) for the gitlab-org/gitlab repository in the past hour?
  - [ ] How slow are the slowest clones taking on the gitlab-org/gitlab repository the past day?
- [ ] Were there any git operations rejected by Gitaly due to `GitLab is currently unable to handle this request due to load`?
  - [ ] If yes, what was the main reason?

### Tracking calls throughout the GitLab ecosystem

- [ ] Pick a [repository files API
  call](https://docs.gitlab.com/ee/api/repository_files.html#get-file-from-repository) that happened in the past day.  Which Gitaly calls were invoked downstream?
- [ ] Pick a `GetBlobs` RPC call in the past day. Who were the upstream callers?
- [ ] Trace the entire flow when a user clones a repo, over SSH, over HTTP, and through the web editor. What components were involved? On which node is the repo data located?

## Production

- [ ] Are there any production outages going on right now (Gitaly or not)? If not, what was the last one?

## Follow-up activities

- [ ] [Set up a local ELK and import log files into it](../../doc/setup_local_elk_for_downloaded_logs.md)
- [ ] Read through some recently closed customer issues and the investigation. Follow the reasoning and understand the fix. [Gitaly Customer Issues](https://gitlab.com/groups/gitlab-org/data-access/gitaly/-/epics/2).
- [ ] Join an ongoing investigation, or pick up a new incoming issue. (Add the current milestone and ~"workflow::in dev" while assignign the issue to yourself.) Ask for help and guidance shamelessly. :slight_smile:
- [ ] Monitor `#g_gitaly` for incoming questions, direct them to our [intake flow](https://handbook.gitlab.com/handbook/engineering/infrastructure-platforms/data-access/gitaly#customer-issues)

## Finally

- [ ] Add yourself to the [oncall rotation](https://ops.gitlab.net/gitlab-com/gl-infra/config-mgmt/-/blob/main/environments/pagerduty/gitaly_locals.tf?ref_type=heads) by raising a MR. Set manager and reviewer buddy as reviewers.

/confidential
/assign me
/cc @jcaigitlab
