package praefect

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
	glcli "gitlab.com/gitlab-org/gitaly/v18/internal/cli"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func newAcceptDatalossCommand() *cli.Command {
	return &cli.Command{
		Name:  "accept-dataloss",
		Usage: "accept potential data loss in a repository",
		Description: `Set a new authoritative replica of a repository and enable the repository for writing again.

The replica of the repository on the specified physical storage is set as authoritative. Replications to other physical
storages that contain replicas of the repository are scheduled to make them consistent with the replica on the new
authoritative physical storage.

Example: praefect --config praefect.config.toml accept-dataloss --virtual-storage default --repository <relative_path_on_the_virtual_storage> --authoritative-storage <physical_storage_1>`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     paramVirtualStorage,
				Usage:    "name of the repository's virtual storage",
				Required: true,
			},
			&cli.StringFlag{
				Name:     paramRelativePath,
				Usage:    "relative path on the virtual storage of the repository to accept data loss for",
				Required: true,
			},
			&cli.StringFlag{
				Name:     paramAuthoritativeStorage,
				Usage:    "physical storage containing the replica of the repository to set as authoritative",
				Required: true,
			},
		},
		Action: acceptDatalossAction,
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Args().Present() {
				return nil, unexpectedPositionalArgsError{Command: cmd.Name}
			}
			return ctx, nil
		},
	}
}

func acceptDatalossAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	conf, err := readConfig(cmd.String(configFlagName))
	if err != nil {
		return err
	}

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
	if _, err := client.SetAuthoritativeStorage(ctx, &gitalypb.SetAuthoritativeStorageRequest{
		VirtualStorage:       cmd.String(paramVirtualStorage),
		RelativePath:         cmd.String(paramRelativePath),
		AuthoritativeStorage: cmd.String(paramAuthoritativeStorage),
	}); err != nil {
		return cli.Exit(fmt.Errorf("set authoritative storage: %w", err), 1)
	}

	return nil
}
