# Beginner's guide to Gitaly contributions

## Setup

### GitLab

Before you can develop on Gitaly, it's required to have the
[GitLab Development Kit][gdk] properly installed. After installing GitLab, verify
it to be working by starting the required servers and visiting GitLab at
`http://localhost:3000`.

### Gitaly Proto

Data is shared between GitLab Rails and Gitaly using the [Google Protocol Buffers](https://developers.google.com/protocol-buffers) to provide a shared format for serializing the exchanged data. They are also referred to as _protobuf_.

Protocol buffers define which requests can be made and what data the requester must provide with the request. The response to each request is likewise defined using the Protocol buffers format.

The protocol definitions can be found in `proto/*.proto`.

### Gitaly

Gitaly provides high-level RPC access to Git repositories. It controls access to the `git` binary and is used by GitLab to read and write Git data. Gitaly is present in every GitLab installation and coordinates Git repository storage and retrieval.

Within the GDK, you can find a clone of the Gitaly repository in
`/path/to/gdk/gitaly`. You can check out your working branch here, but
please be aware that `gdk update` will reset it to the tag specified by
`/path/to/gdk/gitlab/GITALY_SERVER_VERSION`.

This can be ineffective if you do a lot of Gitaly development, so if you
want to stop `gdk update` from overwriting your Gitaly checkout, add
the following to `/path/to/gdk/gdk.yml`:

```yaml
gitaly:
  auto_update: false
```

## Development

### General advice

#### Using the Makefile

Gitaly uses [Make](https://en.wikipedia.org/wiki/Make_(software)) to manage its build process, and all targets are
defined in our top-level [`Makefile`](../Makefile). By default, running `make` builds the `all` target, which installs
Gitaly into the `./_build/bin` directory so that it's picked up by GDK. The following is a list of the most frequently
used targets:

- `build`: Build Gitaly, but do not install it.
- `install`: Build and install Gitaly. The destination directory can be modified by modifying a set of variables, most
  importantly `PREFIX`.
- `test`: Execute tests.
- `clean`: Remove all generated build artifacts.
- `help`: Print a list of available Makefile targets and variables.

You can modify some parts of the build process by setting up various variables. For example, by:

- Executing `make V=1`, you can do a verbose build
- Overriding the `PROTOC_VERSION` and `PROTOC_HASH`, a different protobuf compiler version is used for generating code.

If you wish to persist your configuration, create a `config.mak` file next to the Makefile and put all variables you
wish to override in there.

##### Lint code and documentation

The `Makefile` defines these targets for linting:

- `lint`: Run [`golangci-lint`](https://golangci-lint.run) but don't correct any problems found.
- `lint-fix`: Run `golangci-lint` and fix any problems found.
- `lint-docs`: Run [`markdownlint-cli2`](https://github.com/DavidAnson/markdownlint-cli2) in the project but don't
  correct any problems found.
- `lint-docs-fix`: Run `markdownlint-cli2` in the project and fix any problems found.

To enable linting in your code editor:

1. Run `make lint` at least once. That builds a version of `golangci-lint` for you.
1. Point your code editor or code editor's plugin to the binary at `_build/tools/golangci-lint`.

#### Experimenting with editing code

If you're used to Ruby on Rails development you may be used to a "edit
code and reload" cycle where you keep editing and reloading until you
have your change working. This is usually not the best workflow for Gitaly
development.

At some point you will know which Gitaly RPC you need to work on. This
is where you probably want to stop using `localhost:3000` and zoom in on
the RPC instead.

##### A suggested workflow

To experiment with changing an RPC you should use the Gitaly service
tests. The RPC you want to work on will have tests somewhere in
`internal/gitaly/service/...`.

Before you edit any code, make sure the tests pass when you run them:

```shell
TEST_PACKAGES=./internal/gitaly/service/foobar TEST_OPTIONS="-count=1 -run=MyRPC" make test
```

In this command, `MyRPC` is a regex that will match functions like
`TestMyRPCSuccess` and `TestMyRPCValidationFailure`.

Once you have found your tests and your test command, you can start tweaking the implementation or adding test cases and re-running the tests.

This approach is many times faster than "edit Gitaly, reinstall Gitaly into GDK,
restart, reload `localhost:3000`".

To see the changes, run the following commands in
your GDK directory:

```shell
make gitaly-setup
gdk restart gitaly
```

### Development Process

The general approach is:

1. Add a request/response combination to [Gitaly Proto][gitaly-proto], or edit
  an existing one
1. Change [Gitaly][gitaly] accordingly
1. Use the endpoint in other GitLab components (CE/EE, GitLab Workhorse, etc.)

#### Configuration changes

When modifying the Gitaly or Praefect configuration, the changes should be propagated to other GitLab projects that rely on them:

1. [`omnibus-gitlab`](https://gitlab.com/gitlab-org/omnibus-gitlab) contains template files that are used to generate Gitaly's and Praefect's configuration.
1. [CNG](https://gitlab.com/gitlab-org/build/CNG) contains configuration required to run Gitaly in a container.

#### Gitaly Proto

The [Protocol buffer documentation][proto-docs] combined with the `*.proto` files in the `proto/` directory should be enough to get you started. A service needs to be picked that can receive the procedure call. A general rule of thumb is that the service is named either after the Git CLI command, or after the Git object type.

If either your request or response data can exceed 100KB you need to use the `stream` keyword. To generate the server and client code, run `make proto`.

#### Gitaly

If proto is updated, run `make`. This should compile successfully.

## Testing

Gitaly's tests are mostly in Go and we apply the following guidelines:

- Each RPC must have end-to-end tests at the service level.
- (Optional) Add unit tests for functions that need more coverage.

To run the full test suite, use `make test`.
You'll need some [test repositories](test_repos.md), you can set these up with `make prepare-tests`.

### Integration Tests

A typical set of Go tests for an RPC consists of two or three test
functions:

- a success test
- a failure test (usually a table driven test using t.Run)
- sometimes a validation test.

Our Go RPC tests use in-process test servers that only implement the service the current RPC belongs to.

For example, if you are working on an RPC in the 'RepositoryService', your tests would go in `internal/gitaly/service/repository/your_rpc_test.go`.

### Running a specific test

When you are trying to fix a specific test failure it is inefficient
to run `make test` all the time. To run just one test you need to know
the package it lives in (e.g. `internal/gitaly/service/repository`) and the
test name (e.g. `TestRepositoryExists`).

To run the test you need a terminal window with working directory
`/path/to/gdk/gitaly`. To run just the one test you're interested in:

```shell
TEST_PACKAGES=./internal/gitaly/service/repository TEST_OPTIONS="-count=1 -run=TestRepositoryExists" make test-go
```

When writing tests, prefer using [testify]'s [require], and [assert] as
methods to set expectations over functions like `t.Fatal()` and others directly
called on `testing.T`.

[testify]: https://github.com/stretchr/testify
[require]: https://github.com/stretchr/testify/tree/master/require
[assert]: https://github.com/stretchr/testify/tree/master/assert

### Using Delve to debug a test

The process to debug a test in your terminal using
[Delve](https://github.com/go-delve/delve) is almost the same as
[running a single test](#running-a-specific-test), just change the
target to `debug-test-go`:

```shell
TEST_PACKAGES=./internal/gitaly/service/repository TEST_OPTIONS="-count=1 -run=TestRepositoryExists" make debug-test-go
```

### Praefect tests

Because Praefect lives in the same repository, we need to provide database connection
information to run tests for it successfully. For more information, see
[glsql](../internal/praefect/datastore/glsql/doc.go) package documentation.

When using [GDK](https://gitlab.com/gitlab-org/gitlab-development-kit/),
the easiest way to run a PostgreSQL database is by running:

```shell
gdk start db
```

Otherwise, you can set up a PostgreSQL database instance as a Docker container:

```shell
docker rm -f $(docker ps -q --all -f name=praefect-pg) > /dev/null 2>1; \
docker run --name praefect-pg -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -d postgres:12.6
```

and run the tests by setting the appropriate `env` variables:

```shell
PGHOST=localhost PGPORT=5432 PGUSER=postgres make test-with-praefect
```

### Useful snippets for creating a test

#### testhelper package

The `testhelper` package provides functions to create configurations to run Gitaly and helpers to run a Gitaly gRPC server:

- [Create test configuration](https://gitlab.com/gitlab-org/gitaly/-/blob/aa098de7b267e3d6cb8a05e7862a1ad34f8f2ab5/internal/gitaly/service/ref/testhelper_test.go#L43)
- [Run Gitaly](https://gitlab.com/gitlab-org/gitaly/-/blob/aa098de7b267e3d6cb8a05e7862a1ad34f8f2ab5/internal/gitaly/service/ref/testhelper_test.go#L57)
- [Clone test repository](https://gitlab.com/gitlab-org/gitaly/-/blob/aa098de7b267e3d6cb8a05e7862a1ad34f8f2ab5/internal/gitaly/service/ref/find_all_tags_test.go#L30)

### Invoking gRPC endpoints locally

Gitaly listens on a UNIX socket when operated locally using GDK. You can use tools like
[`grpcurl`](https://github.com/fullstorydev/grpcurl) and [`grpcgui`](https://github.com/fullstorydev/grpcui)
to invoke the gRPC endpoints of Gitaly locally:

1. Generate a Protoset file at `_build/protoset/gitaly.protoset`:

   ```shell
   make build-protoset
   ```

1. Identify the address of the socket:
   - If using Praefect, the address is the value of the `socket_path` field in one of the `gitaly/gitaly-*.praefect.toml` files.
   - Otherwise, the address is the value of the `socket_path` field in the `gitaly/gitaly.config.toml` file.

You can now invoke RPCs with `grpcurl` or open a web GUI with `grpcui`:

- Invoke RPCs with `grpcurl`:

  ```shell
  grpcurl -plaintext -unix -protoset <protoset path> <socket address> <command>
  ```

- Open a web GUI by using `grpcui`:

  ```shell
  grpcui -plaintext -unix -protoset <protoset path> <socket address>
  ```

> [!note]
> The latest versions of `grpcui` and `grpcurl` have issues when connecting to Unix sockets. Either:
>
> - [Workaround the issues](https://github.com/fullstorydev/grpcui/issues/375).
> - Use an older version.

## Rails tests

To use your custom Gitaly when running Rails tests in GDK, go to the
`gitlab` directory in your GDK and follow the instructions at
[Running tests with a locally modified version of Gitaly][custom-gitaly].

[custom-gitaly]: https://docs.gitlab.com/development/gitaly/#running-tests-with-a-locally-modified-version-of-gitaly
[gdk]: https://gitlab.com/gitlab-org/gitlab-development-kit/#getting-started
[gitaly]: https://gitlab.com/gitlab-org/gitaly
[gitaly-proto]: https://gitlab.com/gitlab-org/gitaly/tree/master/proto
[proto-docs]: https://developers.google.com/protocol-buffers/docs/overview
