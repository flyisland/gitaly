package praefect

import (
	"context"
	"fmt"
	"strings"

	"github.com/urfave/cli/v3"
	glcli "gitlab.com/gitlab-org/gitaly/v18/internal/cli"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

const (
	setReplicationFactorCmdName = "set-replication-factor"
	paramReplicationFactor      = "replication-factor"
)

func newSetReplicationFactorCommand() *cli.Command {
	return &cli.Command{
		Name:  setReplicationFactorCmdName,
		Usage: "set a replication factor for a repository",
		Description: `Set a new replication factor for a repository.

By default, repositories are replicated to all physical storages managed by Praefect. Use the set-replication-factor
subcommand to change this behavior. You should rarely set replication factors above 3.

When a new replication factor is specified, the subcommand:

- Assigns physical storages to or unassigns physical storages from the repository to meet the new replication factor.
  The assigned physical storages are displayed on stdout.
- Returns an error if the new replication factor is either:
  - More than the number of physical storages in the virtual storage.
  - Less than one.

The authoritative physical storage is never unassigned because it:

- Accepts writes.
- Is the first storage that is assigned when setting a replication factor for a repository.

Example: praefect --config praefect.config.toml set-replication-factor --virtual-storage default --relative-path <relative_path_on_the_virtual_storage> --replication-factor 3`,
		Action: setReplicationFactorAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     paramVirtualStorage,
				Usage:    "name of the repository's virtual storage",
				Required: true,
			},
			&cli.StringFlag{
				Name:     paramRelativePath,
				Usage:    "relative path on the virtual storage of the repository to set the replication factor for",
				Required: true,
			},
			&cli.UintFlag{
				Name:     paramReplicationFactor,
				Usage:    "replication factor to set",
				Required: true,
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Args().Present() {
				_ = cli.ShowSubcommandHelp(cmd)
				return nil, cli.Exit(unexpectedPositionalArgsError{Command: cmd.Name}, 1)
			}
			return ctx, nil
		},
	}
}

func setReplicationFactorAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	conf, err := readConfig(cmd.String(configFlagName))
	if err != nil {
		return err
	}

	virtualStorage := cmd.String(paramVirtualStorage)
	relativePath := cmd.String(paramRelativePath)
	replicationFactor := cmd.Uint(paramReplicationFactor)

	nodeAddr, err := getNodeAddress(conf)
	if err != nil {
		return err
	}

	conn, err := glcli.Dial(ctx, nodeAddr, conf.Auth.Token, defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("error dialing: %w", err)
	}
	defer conn.Close()

	client := gitalypb.NewPraefectInfoServiceClient(conn)
	resp, err := client.SetReplicationFactor(ctx, &gitalypb.SetReplicationFactorRequest{
		VirtualStorage:    virtualStorage,
		RelativePath:      relativePath,
		ReplicationFactor: int32(replicationFactor),
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(cmd.Writer, "current assignments: %v\n", strings.Join(resp.GetStorages(), ", "))

	return nil
}
