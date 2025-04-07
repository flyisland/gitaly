package praefect

import (
	"github.com/urfave/cli/v3"
)

const configurationCmdName = "configuration"

func newConfigurationCommand() *cli.Command {
	return &cli.Command{
		Name:  configurationCmdName,
		Usage: "manage configuration",
		Description: `Manage Praefect configuration.

Provides the following subcommand:

- validate`,
		HideHelpCommand: true,
		Commands: []*cli.Command{
			newConfigurationValidateCommand(),
		},
	}
}
