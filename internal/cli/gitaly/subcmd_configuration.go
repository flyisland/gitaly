package gitaly

import (
	"context"

	"github.com/urfave/cli/v3"
	gitalycmd "gitlab.com/gitlab-org/gitaly/v16/cmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

const validationErrorCode = 2

func newConfigurationCommand() *cli.Command {
	return &cli.Command{
		Name:        "configuration",
		Usage:       "run configuration-related commands",
		Description: "Run commands related to Gitaly configuration.",
		Commands: []*cli.Command{
			{
				Name:  "validate",
				Usage: "validate Gitaly configuration",
				UsageText: `gitaly configuration validate < <gitaly_config_file>

Example: gitaly configuration validate < gitaly.config.toml`,
				Description: `Check that input provided on stdin is valid Gitaly configuration.
Use validate before starting Gitaly.

Prints all configuration problems to stdout in JSON format. The output's structure includes:

- A key, which is the path to the configuration field where the problem is detected.
- A message, with an explanation of the problem.`,
				Action: validateConfigurationAction,
			},
		},
	}
}

func validateConfigurationAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	cfg, err := config.Load(cmd.Reader)
	if err != nil {
		if gitalycmd.WriteTomlReadError(err, cmd.Writer, cmd.ErrWriter) {
			return cli.Exit("", validationErrorCode)
		}

		return cli.Exit("", 1)
	}

	if !gitalycmd.Validate(&cfg, cmd.Writer, cmd.ErrWriter) {
		return cli.Exit("", validationErrorCode)
	}

	return nil
}
