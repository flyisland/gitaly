package praefect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore/glsql"
	"google.golang.org/grpc"
)

const (
	defaultDialTimeout        = 10 * time.Second
	paramVirtualStorage       = "virtual-storage"
	paramRelativePath         = "relative-path"
	paramReplicaPath          = "replica-path"
	paramAuthoritativeStorage = "authoritative-storage"
)

func getNodeAddress(cfg config.Config) (string, error) {
	switch {
	case cfg.SocketPath != "":
		return "unix:" + cfg.SocketPath, nil
	case cfg.ListenAddr != "":
		return "tcp://" + cfg.ListenAddr, nil
	case cfg.TLSListenAddr != "":
		return "tls://" + cfg.TLSListenAddr, nil
	default:
		return "", errors.New("no Praefect address configured")
	}
}

func openDB(conf config.DB, errOut io.Writer) (*sql.DB, func(), error) {
	ctx := context.Background()

	openDBCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := glsql.OpenDB(openDBCtx, conf)
	if err != nil {
		return nil, nil, fmt.Errorf("sql open: %w", err)
	}

	clean := func() {
		if err := db.Close(); err != nil {
			fmt.Fprintf(errOut, "sql close: %v\n", err)
		}
	}

	return db, clean, nil
}

func subCmdDial(ctx context.Context, addr, token string, timeout time.Duration, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	opts = append(opts,
		client.UnaryInterceptor(),
		client.StreamInterceptor(),
	)

	if len(token) > 0 {
		opts = append(opts,
			grpc.WithPerRPCCredentials(
				gitalyauth.RPCCredentialsV2(token),
			),
		)
	}

	return client.New(ctx, addr, client.WithGrpcOptions(opts))
}

type requiredParameterError string

func (p requiredParameterError) Error() string {
	return fmt.Sprintf("%q is a required parameter", string(p))
}

type unexpectedPositionalArgsError struct{ Command string }

func (err unexpectedPositionalArgsError) Error() string {
	return fmt.Sprintf("%s doesn't accept positional arguments", err.Command)
}
