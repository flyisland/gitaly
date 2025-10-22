# Cgroups in Gitaly

## High Level

Gitaly can be configured to run its Git processes inside of cgroups, which prevent
shelled-out processes from hogging too much CPU and memory. See [these docs](https://man7.org/linux/man-pages/man7/cgroups.7.html) for a full specification on cgroups.

## Configuration

### Top Level

Here is the top level `[cgroups]` configuration:

```toml
[cgroups]
mountpoint = "/sys/fs/cgroup"
hierarchy_root = "gitaly"
memory_bytes = 64424509440 # 60gb
cpu_shares = 1024
cpu_quota_us = 400000
```

**mountpoint** is the top level directory where cgroups will be created.
**hierarchy_root** is the parent cgroup under which Gitaly creates cgroups.
**memory_bytes** limits all processes created by Gitaly to a memory limit,
collectively.
**cpu_shares** limits all processes created by Gitaly when there are multiple
processes competing for CPU.
**cpu_quota_us** hard limit for all processes created by Gitaly.

### Repository Groups

Cgroups that have a repository-level isolation can also be defined:

```toml
[cgroups.repositories]
count = 1000
memory_bytes = 12884901888 # 12gb
cpu_shares = 512
cpu_quota_us = 200000
```

**count** is the number of cgroups to create.
**memory_bytes** limits [memory](#memory-limits) for processes in one cgroup.
This number cannot exceed the top level memory limit.
**cpu_shares** limits [CPU shares](#cpu-shares) for processes in one cgroup when
there are multiple processes competing for CPU. This number cannot exceed the
top level CPU limit.
**cpu_quota_us** hard limit [CPU quota](#cpu-quotas) for all processes created by
Gitaly. This number cannot exceed the top level CPU quota.

These cgroups will be created when Gitaly starts up. A circular hashing algorithm
is used to assign repositories to cgroups. So when we reach the max number of
cgroups we set in `[cgroups.repositories]`, requests from subsequent repositories
will be assigned to an existing cgroup.

## Memory Limits

Each cgroup has a memory limit which in this example config, is 12gb. All
processes that are part of a cgroup are limited to 12gb of memory collectively.
This means that if there are 5 processes that collectively use up 10gb, and a
6th process is added to the cgroup and its memory slowly climbs beyond 2gb, the
OOM killer will target the process with the highest memory usage to be killed.

This is an oversimplification of how the OOM killer works. A more detailed
picture can be found at:

- [This Crunchy Data blog post](https://blog.crunchydata.com/blog/deep-postgresql-thoughts-the-linux-assassin#:~:text=CGroup%20Level%20OOM%20Killer%20Mechanics&text=First%20of%20all%2C%20the%20OOM,%2Fcgroup%2Fmemory%2Fmemory.)
- [This Linux Weekly News resource](https://lwn.net/Kernel/Index/#OOM_killer).

This provides baseline protection from processes running haywire, such as a
memory leak. However, this only provides limited protection because there are
some legitimate Git processes that can take a huge amount of memory such as
`git-repack(1)`, or `git-upload-pack(1)`. For a large repository, these
operations can easily take 12gb of ram as seen in production systems.

Hence, we also need finer grained controls to allow certain expensive Git
operations to have their own cgroups.

## CPU Shares

Each cgroup has a configured share of CPU resources. This setting is used when
there are multiple processes competing for CPU, otherwise the cgroup has access
to all CPU resources. For more information, see
[`cpu.shares`](https://www.redhat.com/sysadmin/cgroups-part-two).

All the CPU resources of a machine are equal to 1024 shares. Lower values are
a fraction of the total CPU resources.

## CPU Quotas

CPU quota sets
[`cfs_quota_us`](https://docs.kernel.org/scheduler/sched-bwc.html?highlight=cfs_quota_us#management),
which is a hard limit of how much CPU a process can use.

The quota specifies a maximum amount of CPU time usable per period of wall
clock time. The quota refills at the start of each period. Gitaly explicitly
sets the refill period to match the [kernel default](https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git/tree/kernel/sched/fair.c?h=v5.15.92#n4807)
of 100 ms (`cfs_period_us=100000`). So setting the quota (`cfs_quota_us`) to
the same value `100000` (100 ms) is equivalent to budgeting 1 CPU core for the
cgroup to use.

This means a 4-core machine has a maximum `cfs_quota_us` of `400000`. If we
want to use 2 of those cores for Gitaly spawned `git` processes, we would set
the value of `cfs_quota_us` to `200000`.

## Cgroup Hierarchy

```plaintext
/sys/fs/cgroup
|
|--memory
|    |--gitaly
|         |--gitaly-<pid>
|               |--memory.limit_in_bytes
|               |--repos-0
|               |     |--memory.limit_in_bytes
|               |--repos-1
|               |     |--memory.limit_in_bytes
|               |--repos-2
|               |     |--memory.limit_in_bytes
|               |--repos-3
|               |     |--memory.limit_in_bytes
|               |--repos-4
|               |     |--memory.limit_in_bytes
|               |--repos-5
|               |     |--memory.limit_in_bytes
|               |--repos-6
|               |     |--memory.limit_in_bytes
|               |--repos-7
|               |     |--memory.limit_in_bytes
|               |--repos-8
|               |     |--memory.limit_in_bytes
|               |--repos-9
|               |     |--memory.limit_in_bytes
|               |--repos-10
|                     |--memory.limit_in_bytes
|
|-cpu
|  |--gitaly
|        |--gitaly-<pid>
|              |--cpu.shares
|              |--repos-0
|              |     |--cpu.shares
|              |     |--cpu.cfs_period_us
|              |     |--cpu.cfs_quota_us
|              |--repos-1
|              |     |--cpu.shares
|              |     |--cpu.cfs_period_us
|              |     |--cpu.cfs_quota_us
|              |--repos-2
|              |     |--cpu.shares
|              |     |--cpu.cfs_period_us
|              |     |--cpu.cfs_quota_us
|              |--repos-3
|              |     |--cpu.shares
|              |     |--cpu.cfs_period_us
|              |     |--cpu.cfs_quota_us
|              |--repos-4
|              |     |--cpu.shares
|              |     |--cpu.cfs_period_us
|              |     |--cpu.cfs_quota_us
|              |--repos-5
|              |     |--cpu.shares
|              |     |--cpu.cfs_period_us
|              |     |--cpu.cfs_quota_us
|              |--repos-6
|              |     |--cpu.shares
|              |     |--cpu.cfs_period_us
|              |     |--cpu.cfs_quota_us
|              |--repos-7
|              |     |--cpu.shares
|              |     |--cpu.cfs_period_us
|              |     |--cpu.cfs_quota_us
|              |--repos-8
|              |     |--cpu.shares
|              |     |--cpu.cfs_period_us
|              |     |--cpu.cfs_quota_us
|              |--repos-9
|              |     |--cpu.shares
|              |     |--cpu.cfs_period_us
|              |     |--cpu.cfs_quota_us
|              |--repos-10
|                    |--cpu.shares
|                    |--cpu.cfs_period_us
|                    |--cpu.cfs_quota_us
```
