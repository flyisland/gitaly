# Accessing Production Through Teleport

We use [Teleport](https://goteleport.com/) to gain SSH access into production
environments. The Gitaly team requires such access to debug production
incidents.

## Preparation

1. [Install](https://goteleport.com/docs/installation) tsh.
1. Reach out to the EM to make sure you are added to the Gitaly production okta
   group.

## Access

To log onto a production Gitaly node, first log in to the Teleport production
instance.

```shell
> tsh login --proxy=production.teleport.gitlab.net
```

This will bring up an Okta login screen through which you will need to
authenticate using your credentials.

Find the ID of a Gitaly node you want to SSH into.

```shell
> tsh ls -v
```

This will yield entries that include the name of the node, the UUID, and some
other metadata.

```shell
gitaly-09-stor-gprd                          27984ac4-cba0-40b3-b9f9-3ee661eb4505 ⟵ Tunnel   arch=x86_64,environment=gprd,...
```

Login to a server over SSH using the UUID in the previous step.

```shell
> tsh ssh <username>@27984ac4-cba0-40b3-b9f9-3ee661eb4505
```
