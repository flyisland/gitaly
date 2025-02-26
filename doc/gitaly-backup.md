# `gitaly-backup`

## Overview

The `gitaly-backup tool` establishes a gRPC connection with a Gitaly instance to facilitate repository backups by requesting
backup data and transferring it to a specified storage sink, either locally or in a remote cloud storage service (such as AWS S3,
Google Cloud Storage, or Azure).

Gitaly supports two backup approaches:

1. The `gitaly-backup` CLI initiates Remote Procedure Calls (RPCs) to the Gitaly server requesting backup data for one or more repositories.
The Gitaly server generates the backup data and streams it back to `gitaly-backup` over the RPC connection. `gitaly-backup` then
transfers the received data to the specified storage destination, as defined by the `-path` argument.

   ```plaintext
    +------------------+      Repository  data        +-----------------+
    | gitlab-backup-cli|      backup job request   >  |      Gitaly     |
    +------------------+                              +-----------------+
           ^                                                 |
           |                                                 |
           |  Creates backup and                             |
           |  gRPC Streamed Data to                          |
           | <---------------------------------------------- +
           |
           |
           |                                  +------------------------+
           +-> write repository data write to |     Backup Storage     |
                                              | (Local / S3 / GCS etc.)|
                                              +------------------------+
    ```

    For a detailed overview, see the [GitLab backup sequence diagram.](https://docs.gitlab.com/administration/backup_restore/backup_archive_process/#back-up-git-repositories)

    **Note**:
    In setups where Gitaly and `gitaly-backup` run on the same host (such as smaller reference architectures with fewer than 1,000 seats),
    this approach can reduce network load on Gitaly by offloading remote network egress overhead to the `gitaly-backup` CLI. Since Gitaly streams backup data locally via gRPC,
    it minimizes network congestion and improves efficiency.

1. Direct Gitaly Server-Side Backup – This approach prioritizes direct transfer from Gitaly server to the destination sink.
   CLI initiates Remote Procedure Calls (RPCs) to the Gitaly server requesting backup data for one or more repositories.
   The Gitaly server generates the backup data and streams this directly to the storage backend configured in the Gitaly configuration file.
   in the Gitaly [`config.toml`](/gitlab-org/gitaly/-/blob/master/config.toml.example)

   ```plaintext
    +------------------+         1. Repository         +------------------+
    | gitlab-backup-cli|  >     data backup         >  |  Gitaly          |
    +------------------+          request              +------------------+
                                                            |
                                                            | 2. Creates backup and
                                                            |    directly writes to
                                                            v
                                                +------------------------+
                                                |     Backup Storage     |
                                                | (Local / S3 / GCS etc.)|
                                                +------------------------+
    ```

    For a detailed overview, see the [Server-side backup sequence diagram](https://docs.gitlab.com/administration/backup_restore/backup_archive_process/#server-side-backups)

    **Note**:
    Sever-side backups reduce network and disk pressure and could offer higher speed in running backups.

## How to set up for backup

1. For each project to backup, find the Gitaly storage name and relative or disk path using either:
   - The [Admin area](https://docs.gitlab.com/ee/administration/repository_storage_paths.html#from-project-name-to-hashed-path).
   - The [repository storage API](https://docs.gitlab.com/ee/api/projects.html#get-the-path-to-repository-storage).
1. Generate the `GITALY_SERVERS` environment variable. This variable specifies
   the address and authentication details of each storage to restore to. The variable takes a base64-encoded JSON object.

   For example:

   ```shell
   export GITALY_SERVERS=`echo '{"default":{"address":"unix:/var/opt/gitlab/gitaly/gitaly.socket","token":""}}' | base64 --wrap=0`
   ```

1. Generate the backup job file. The job file consists of a series of JSON objects separated by a new line (`\n`).

   | Attribute           | Type     | Required | Description |
   |:--------------------|:---------|:---------|:------------|
   |  `address`          |  string  |  no      |  Address of the Gitaly or Gitaly Cluster server. Overrides the address specified in `GITALY_SERVERS`. |
   |  `token`            |  string  |  no      |  Authentication token for the Gitaly server. Overrides the token specified in `GITALY_SERVERS`. |
   |  `storage_name`     |  string  |  yes     |  Name of the storage where the repository is stored. |
   |  `relative_path`    |  string  |  yes     |  Relative path of the repository. |
   |  `gl_project_path`  |  string  |  no      |  Name of the project. Used for logging. |

   For example, `backup_job.json`:

   ```json
   {
     "storage_name":"default",
     "relative_path":"@hashed/f5/ca/f5ca38f748a1d6eaf726b8a42fb575c3c71f1864a8143301782de13da2d9202b.git",
     "gl_project_path":"diaspora/diaspora-client"
   }
   {
     "storage_name":"default",
     "relative_path":"@hashed/6b/86/6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b.git",
     "gl_project_path":"brightbox/puppet"
   }
   ```

## Gitaly-Backup Client-Orchestrated backup

1. Pipe the backup job file to `gitaly-backup create`.
   Set up the environment as show in [Set up environment step](#how-to-set-up-for-backup)

    ```shell
    /opt/gitlab/embedded/bin/gitaly-backup create -path $BACKUP_DESTINATION_PATH < backup_job.json
    ```

    | Argument              | Type      | Required | Description |
    |:----------------------|:----------|:---------|:------------|
    |  `-path`              |  string   |  yes     |  Directory where the backup files will be created. |
    |  `-parallel`          |  integer  |  no      |  Maximum number of parallel backups. |
    |  `-parallel-storage`  |  integer  |  no      |  Maximum number of parallel backups per storage. |
    |  `-id`                |  string   |  no      |  Used to determine a unique path for the backup when a full backup is created. |
    |  `-layout`            |  string   |  no      |  How backup files are located. One of `manifest` (default), `pointer`, or `legacy`. |
    |  `-incremental`       |  bool     |  no      |  Indicates whether to create an incremental backup. |
    |  `-server-side`       |  bool     |  no      |  Indicates whether to use server-side backups. |

## Gitaly-Backup Client-Orchestrated Restore

Set up the environment as show in [Set up environment step](#how-to-set-up-for-backup)

1. Pipe the restore job file to `gitaly-backup restore`.

    ```shell
     /opt/gitlab/embedded/bin/gitaly-backup restore -path $BACKUP_SOURCE_PATH < restore_job.json
    ```

     | Argument                    | Type                   | Required | Description |
    |:----------------------------|:-----------------------|:---------|:------------|
     |  `-path`                    |  string                |  yes     |  Directory where the backup files are stored. |
     |  `-parallel`                |  integer               |  no      |  Maximum number of parallel restores. |
     |  `-parallel-storage`        |  integer               |  no      |  Maximum number of parallel restores per storage. |
     |  `-id`                      |  string                |  no      |  ID of full backup to restore. If not specified, the latest backup is restored (default). |
     |  `-layout`                  |  string                |  no      |  How backup files are located. One of `manifest` (default), `pointer`, or `legacy`. |
     |  `-remove-all-repositories` |  comma-separated list  |  no      |  List of storage names to have all repositories removed from before restoring. You must specify `GITALY_SERVERS` for the listed storage names. |
     |  `-server-side`             |  bool                  |  no      |  Indicates whether to use server-side backups. |

## Direct Gitaly Server-side backup

Server-side create stream backup data from the object storage sink defined in the Gitaly config.
To use server-side create:

1. Set up the environment and create backup job file as show in [Set up environment step](#how-to-set-up-for-backup).
1. Add backup parameters (`enabled` and `go_cloud_url`) to the Gitaly `config.toml` file.

    ```toml
    [backup]
    go_cloud_url = "s3://gitaly-backup?region=us-east-1"
    ```

    See [object storage](#object-storage) for information on how to configure `go_cloud_url` storage.

1. Ensure the Gitaly user or IAM credentials has access to the object storage i.e:
   - in aws ensure the role has a policy similar to :

     ```json
     {
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Action": [
             "s3:PutObject",
             "s3:GetObject",
             "s3:ListBucket"
           ],
           "Resource": [
             "arn:aws:s3:::gitaly-backup",
             "arn:aws:s3:::gitaly-backup/*"
           ]
         }
       ]
     }
     ```

   - In gcp the Gitaly user/service account

     ````json
     {
       "bindings": [
         {
           "role": [
             "roles/storage.objectGet",
             "roles/storage.objectList",
             "roles/storage.objectPut"
           ],
           "members": [
             "serviceAccount:<your-service-account>@<your-project>.iam.gserviceaccount.com"
           ]
         }
       ]
     }
     ````

1. Restart your Gitaly server.
1. Pipe the backup job file to `gitaly-backup create`.

    ```shell
     /opt/gitlab/embedded/bin/gitaly-backup create -server-side < backup_job.json
    ```

   | Argument                    | Type                   | Required | Description |
   |:----------------------------|:-----------------------|:---------|:------------|
   |  `-parallel`                |  integer               | no       |  Maximum number of parallel restores. |
   |  `-parallel-storage`        |  integer               | no       |  Maximum number of parallel restores per storage. |
   |  `-id`                      |  string                | no       |  ID of full backup to restore. If not specified, the latest backup is restored (default). |
   |  `-remove-all-repositories` |  comma-separated list  | no       |  List of storage names to have all repositories removed from before restoring. You must specify `GITALY_SERVERS` for the listed storage names. |
   |  `-server-side`             |  bool                  | yes      |  Indicates whether to use server-side backups. |

   Important: Do not set `-path` and `-layout` flags cannot be used in server-side mode as Gitaly uses the `go_cloud_url` set in the Gitaly config file.

## Direct Gitaly Server-side restore

Server-side restore stream backup data from the object storage sink defined in the Gitaly config.
To use server-side restore:

1. Set up the environment and create backup job file as show in [Set up environment step](#how-to-set-up-for-backup)
1. Add backup parameters (`enabled` and `go_cloud_url`) to the Gitaly `config.toml` file.

    ```toml
    [backup]
    go_cloud_url = "s3://gitaly-backup?region=us-east-1"
    ```

   See [object storage](#object-storage) for information on how to configure `go_cloud_url` storage.

1. Ensure the Gitaly user or IAM credentials has access to the object storage i.e:

   - in aws ensure the role has a policy similar to :

     ```json
     {
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Action": [
             "s3:PutObject",
             "s3:GetObject",
             "s3:ListBucket"
           ],
           "Resource": [
             "arn:aws:s3:::gitaly-backup",
             "arn:aws:s3:::gitaly-backup/*"
           ]
         }
       ]
     }
     ```

   - In gcp the Gitaly user/service account

     ````json
     {
       "bindings": [
         {
           "role": [
             "roles/storage.objectGet",
             "roles/storage.objectList",
             "roles/storage.objectPut"
           ],
           "members": [
             "serviceAccount:<your-service-account>@<your-project>.iam.gserviceaccount.com"
           ]
         }
       ]
     }
     ````

1. Restart your Gitaly server.
1. Pipe the backup job file to `gitaly-backup restore`.

    ```shell
     /opt/gitlab/embedded/bin/gitaly-backup restore -server-side < backup_job.json
    ```

   | Argument                    | Type                   | Required | Description |
    |:----------------------------|:-----------------------|:---------|:------------|
   |  `-parallel`                |  integer               | no       |  Maximum number of parallel restores. |
   |  `-parallel-storage`        |  integer               | no       |  Maximum number of parallel restores per storage. |
   |  `-id`                      |  string                | no       |  ID of full backup to restore. If not specified, the latest backup is restored (default). |
   |  `-remove-all-repositories` |  comma-separated list  | no       |  List of storage names to have all repositories removed from before restoring. You must specify `GITALY_SERVERS` for the listed storage names. |
   |  `-server-side`             |  bool                  | yes      |  Indicates whether to use server-side backups. |

   Important: Do not set `-path` and `-layout` flags cannot be used in server-side mode as Gitaly uses the `go_cloud_url` set in the Gitaly config file.

## Concepts and Flags

### Path

Path determines where on the local filesystem or in object storage backup files
are created on or restored from. The path is set using the `-path` flag.

### Local Filesystem

If `-path` specifies a local filesystem, it is the root of where all backup
files are created.

### Object Storage

`gitaly-backup` supports streaming backup files directly to object storage
using the [`gocloud.dev/blob`](https://pkg.go.dev/gocloud.dev/blob) library.
`-path` or `go_cloud_url` can be used with:

- [Amazon S3](https://pkg.go.dev/gocloud.dev/blob/s3blob). For example `go_cloud_url = "s3://my-bucket?region=us-west-1"`.
- [Azure Blob Storage](https://pkg.go.dev/gocloud.dev/blob/azureblob). For example `go_cloud_url = "azblob://my-container"`.
- [Google Cloud Storage](https://pkg.go.dev/gocloud.dev/blob/gcsblob). For example `go_cloud_url = "gs//my-bucket"`.
- [memory block storage backend](https://pkg.go.dev/gocloud.dev/blob/memblob). For example `go_cloud_url = "mem://my-backup"`.
- [Filesystem storage backend](https://pkg.go.dev/gocloud.dev@v0.40.0/blob/fileblob) for example `go_cloud_url = "/tmp//my-backup"`.

## Layouts

The way backup files are arranged on the filesystem or on object storage is
determined by the layout. The layout is set using the `-layout` flag.

### Legacy layout

This layout is designed to be identical to historic `backup.rake` repository
backups. Repository data is stored in bundle files in a pre-determined
directory structure based on each repository's relative path. This directory
structure is then archived into a tar file by `backup.rake`. Each time a backup
is created, this entire directory structure is recreated.

For example, a repository with the relative path of
`@hashed/4e/c9/4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a.git`
creates the following structure:

```text
$BACKUP_DESTINATION_PATH/
  @hashed/
    4e/
      c9/
        4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a.bundle
```

#### Generating full backups

A bundle with all references is created via the RPC `CreateBundle`. It
effectively executes the following:

```shell
git bundle create repo.bundle --all
```

#### Generating incremental backups

This layout does not support incremental backups.

### Pointer layout

This layout is designed to support incremental backups. Each repository backup
cannot overwrite a previous backup because this would leave dangling incremental
backups. To prevent dangling incremental backups, every new full backup is put into a new directory.
The two files called `LATEST` point to:

- The latest full backup.
- The latest increment of that full backup.

These pointer files enable looking up
backups from object storage without needing directory traversal (directory
traversal typically requires additional permissions). In addition to the bundle
files, each backup writes a full list of refs and their target object IDs.

When the pointer files are not found, the pointer layout will fall back to
using the legacy layout.

For example, a repository with the relative path of
`@hashed/4e/c9/4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a.git`
and a backup ID of `20210930065413` will create the following structure:

```text
$BACKUP_DESTINATION_PATH/
  @hashed/
    4e/
      c9/
        4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a/
          LATEST
          20210930065413/
            001.bundle
            001.refs
            LATEST
```

#### Generating full backups

1. A full list of references is retrieved via the RPC `ListRefs`. This list is written to `001.refs` in the same format as [`git-show-ref`](https://git-scm.com/docs/git-show-ref#_output).
1. A bundle is generated using the retrieved reference names. Effectively, by running:

   ```shell
   awk '{print $2}' 001.refs | git bundle create repo.bundle --stdin
   ```

1. The backup and increment pointers are written.

#### Generating incremental backups

1. The next increment is calculated by finding the increment `LATEST` file and
   adding 1. For example, `001` + `1` = `002`.
1. A full list of references is retrieved using the `ListRefs` RPC. This list is
   written to the calculated next increment (for example, `002.refs`) in the same
   format as [`git-show-ref`](https://git-scm.com/docs/git-show-ref#_output).
1. The full list of the previous increments references is retrieved by reading
   the file. For example, `001.refs`.
1. A bundle is generated using the negated list of reference targets of the
   previous increment and the new list of retrieved reference names
   by effectively running:

   ```shell
   { awk '{print "^" $1}' 001.refs; awk '{print $2}' 002.refs; } | git bundle create repo.bundle --stdin
   ```

   Negating the object IDs from the previous increment ensures that we stop
   traversing commits when we reach the HEAD of the branch at the time of the
   last incremental backup.

### Manifest layout

This layout uses manifest files to describe where each file exists in a backup
to restore the files from the backup:

- The latest backup has two manifests, one named `+latest.toml` and another named after
  its backup ID. `+latest.toml` is overwritten by newer backups.
- Every other backup has one manifest named after the backup ID.

For example, the following structure is created for a repository with the:

- Storage name of `default`.
- Relative path of `@hashed/4e/c9/4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a.git`.
- Backup ID of `20210930065413`.

```plaintext
$BACKUP_DESTINATION_PATH/
  manifests/
    default/
      @hashed/
        4e/
          c9/
            4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a.git/
              20210930065413.toml
              +latest.toml
  default/
    @hashed/
      4e/
        c9/
          4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a.git/
            20210930065413/
              001.bundle
              001.refs
```

If an incremental backup was then created with a backup ID of `20211001065419`,
`gitaly-backup` would create the following structure:

```plaintext
$BACKUP_DESTINATION_PATH/
  manifests/
    default/
      @hashed/
        4e/
          c9/
            4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a.git/
              20210930065413.toml
              20211001065419.toml
              +latest.toml
  default/
    @hashed/
      4e/
        c9/
          4ec9599fc203d176a301536c2e091a19bc852759b255bd6818810a42c5fed14a.git/
            20210930065413/
              001.bundle
              001.refs
            20211001065419/
              002.bundle
              002.refs
```

In this case, the manifest file `20211001065419.toml` would reference all the
bundle files required to restore the repository.
