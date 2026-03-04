package gitaly

import (
	"context"
	"fmt"
	"time"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func newBundleURICommand() *cli.Command {
	return &cli.Command{
		Name:  "bundle-uri",
		Usage: "Generate bundle URI bundle",
		UsageText: `gitaly bundle-uri --storage=<storage-name> --repository=<relative-path> --config=<gitaly_config_file>

Example: gitaly bundle-uri --storage=default --repository=ab/cd/ef012345678901234567890 --config=config.toml`,
		Description: "Generate a bundle for bundle-URI for the given repository.",
		Action:      bundleURIAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  flagStorage,
				Usage: "storage containing the repository",
			},
			&cli.StringFlag{
				Name:     flagRepository,
				Usage:    "repository to generate bundle-URI for",
				Required: true,
			},
			gitalyConfigFlag(),
		},
	}
}

func bundleURIAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	cfg, err := loadConfig(cmd.String(flagConfig))
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	storage := cmd.String(flagStorage)
	if storage == "" {
		if len(cfg.Storages) != 1 {
			return fmt.Errorf("multiple storages configured: use --storage to target storage explicitly")
		}

		storage = cfg.Storages[0].Name
	}

	address, err := cfg.GetAddressWithScheme()
	if err != nil {
		return fmt.Errorf("get Gitaly address: %w", err)
	}

	conn, err := dial(ctx, address, cfg.Auth.GetToken(), 10*time.Second)
	if err != nil {
		return fmt.Errorf("connect to Gitaly: %w", err)
	}
	defer conn.Close()

	req := gitalypb.GenerateBundleURIRequest{
		Repository: &gitalypb.Repository{
			StorageName:  storage,
			RelativePath: cmd.String(flagRepository),
		},
	}

	repoClient := gitalypb.NewRepositoryServiceClient(conn)
	_, err = repoClient.GenerateBundleURI(ctx, &req)

	return err
}
