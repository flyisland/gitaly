package praefect

import (
	"context"
	"errors"
	"fmt"

	"github.com/urfave/cli/v3"
	glcli "gitlab.com/gitlab-org/gitaly/v18/internal/cli"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

const verifyCmdName = "verify"

func newVerifyCommand() *cli.Command {
	return &cli.Command{
		Name:  verifyCmdName,
		Usage: "mark repository replicas as unverified",
		Description: `Mark as unverified replicas of repositories to prioritize reverification.

The subcommand sets different replicas as unverified depending on the supplied flags:

- When a repository ID is specified, all the replicas of the repository on physical storage are marked as unverified.
- When a virtual storage name is specified, all the replicas of all repositories on all physical storages associated with
  the virtual storage as marked as unverified.
- When a virtual storage name and physical storage name are specified, all the replicas of all repositories on the
  specified physical storage associated with the specified virtual storage are marked as unverified.

Reverification runs asynchronously in the background.

Examples:

- praefect --config praefect.config.toml verify --repository-id 1
- praefect --config praefect.config.toml verify --virtual-storage default
- praefect --config praefect.config.toml verify --virtual-storage default --storage <physical_storage_1>`,
		Action: verifyAction,
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:  "repository-id",
				Usage: "repository ID of the repository to mark as unverified",
			},
			&cli.StringFlag{
				Name:  paramVirtualStorage,
				Usage: "name of the virtual storage with replicas to mark as unverified",
			},
			&cli.StringFlag{
				Name:  "storage",
				Usage: "name of the the physical storage associated with the virtual storage with replicas to mark as unverified",
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

func verifyAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	conf, err := readConfig(cmd.String(configFlagName))
	if err != nil {
		return err
	}

	repositoryID := cmd.Int("repository-id")
	virtualStorage := cmd.String(paramVirtualStorage)
	storage := cmd.String("storage")

	var request gitalypb.MarkUnverifiedRequest
	switch {
	case repositoryID != 0:
		if virtualStorage != "" || storage != "" {
			return errors.New("virtual storage and storage can't be provided with a repository ID")
		}

		request.Selector = &gitalypb.MarkUnverifiedRequest_RepositoryId{RepositoryId: int64(repositoryID)}
	case storage != "":
		if virtualStorage == "" {
			return errors.New("virtual storage must be passed with storage")
		}

		request.Selector = &gitalypb.MarkUnverifiedRequest_Storage_{
			Storage: &gitalypb.MarkUnverifiedRequest_Storage{
				VirtualStorage: virtualStorage,
				Storage:        storage,
			},
		}
	case virtualStorage != "":
		request.Selector = &gitalypb.MarkUnverifiedRequest_VirtualStorage{VirtualStorage: virtualStorage}
	default:
		return errors.New("(repository id), (virtual storage) or (virtual storage, storage) required")
	}

	nodeAddr, err := getNodeAddress(conf)
	if err != nil {
		return fmt.Errorf("get node address: %w", err)
	}

	conn, err := glcli.Dial(ctx, nodeAddr, conf.Auth.GetToken(), defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	response, err := gitalypb.NewPraefectInfoServiceClient(conn).MarkUnverified(ctx, &request)
	if err != nil {
		return fmt.Errorf("verify replicas: %w", err)
	}

	fmt.Fprintf(cmd.Writer, "%d replicas marked unverified\n", response.GetReplicasMarked())

	return nil
}
