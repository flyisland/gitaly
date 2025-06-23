# Configuring Praefect

This document describes how to configure the Praefect server.

Praefect is configured via a [TOML](https://github.com/toml-lang/toml)
configuration file. The TOML file contents and location depends on how you
installed GitLab. See: <https://docs.gitlab.com/administration/gitaly/>

The configuration file is passed as an argument to the `praefect`
executable. This is usually done by either `omnibus-gitlab` or your init
script.

```shell
praefect -config /path/to/config.toml
```

## Format

```toml
listen_addr = "127.0.0.1:2305"
socket_path = "/path/to/praefect.socket"
tls_listen_addr = "127.0.0.1:2306"

[tls]
certificate_path = '/home/git/cert.cert'
key_path = '/home/git/key.pem'

[logging]
format = "json"
level = "info"

[[virtual_storage]]
name = 'praefect'

[[virtual_storage.node]]
  storage = "gitaly-0"
  address = "tcp://gitaly-0.internal"
  token = 'secret_token'
```

An example [config TOML](../../config.praefect.toml.example) is stored in this repository.

## Timeout recommendation

Rather than the PostgreSQL `statement_timeout` setting, Praefect relies on application-level timeouts and
uses Go contexts for most database operations. For example:

- In `internal/cli/praefect/subcmd.go`, defaultDialTimeout is set to `10 * time.Second`.
- In various places like `internal/cli/praefect/serve.go`, contexts are created with timeouts such as
  `context.WithTimeout(ctx, 30*time.Second)`.

If you want to set `statement_timeout` for Praefect-related sessions or transactions, set a value equal to or slightly
less than the application’s context timeouts (for example, 30 seconds). This setting:

- Ensures that PostgreSQL queries respect the same timeout boundaries as the application.
- Avoids leaving long-running queries hanging after the context is canceled.
