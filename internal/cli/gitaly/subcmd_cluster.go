package gitaly

import (
	"github.com/urfave/cli/v3"
)

func newClusterCommand() *cli.Command {
	return &cli.Command{
		Name:      "cluster",
		Usage:     "manage Gitaly cluster",
		UsageText: "gitaly cluster command [command options]",
		Description: `The cluster command provides subcommands for managing and inspecting Gitaly clusters.

Use 'gitaly cluster info' to display cluster statistics and overview.
Use 'gitaly cluster get-partition' to display detailed partition information.`,
		Commands: []*cli.Command{
			newClusterInfoCommand(),
			newClusterGetPartitionCommand(),
		},
	}
}
