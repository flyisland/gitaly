// Command praefect provides a reverse-proxy server with high-availability
// specific features for Gitaly.
//
// Additionally, praefect has subcommands for common tasks:
//
// The subcommand "sql-migrate-status" will show which SQL migrations have
// been applied and which ones have not:
//
//	praefect -config PATH_TO_CONFIG sql-migrate-status
//

package praefect

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"slices"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/version"
)

func init() {
	// Override the version printer so the output format matches what Praefect
	// used before the introduction of the CLI toolkit.
	cli.VersionPrinter = func(cmd *cli.Command) {
		fmt.Fprintln(cmd.Writer, version.GetVersionString("Praefect"))
	}
}

const (
	progname = "praefect"

	configFlagName = "config"
)

// NewApp returns a new praefect app.
func NewApp() *cli.Command {
	interrupt := make(chan os.Signal, 1)

	return &cli.Command{
		Name:    progname,
		Usage:   "a gitaly proxy",
		Version: version.GetVersionString("Praefect"),
		// serveAction is also here in the root to keep the CLI backwards compatible with
		// the previous way to launch Praefect with just `praefect -config FILE`.
		// We may want to deprecate this eventually.
		//
		// The 'DefaultCommand: "serve"' setting can't be used here because it won't be
		// possible to invoke sub-command not yet registered.
		Action: serveAction,
		Commands: []*cli.Command{
			newServeCommand(),
			newConfigurationCommand(),
			newAcceptDatalossCommand(),
			newCheckCommand(service.Checks()),
			newDatalossCommand(),
			newDialNodesCommand(),
			newListStoragesCommand(),
			newListUntrackedRepositoriesCommand(),
			newTrackRepositoryCommand(),
			newTrackRepositoriesCommand(),
			newVerifyCommand(),
			newMetadataCommand(),
			newSQLPingCommand(),
			newSQLMigrateCommand(),
			newSQLMigrateDownCommand(),
			newSQLMigrateStatusCommand(),
			newRemoveRepositoryCommand(),
			newSetReplicationFactorCommand(),
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				// We can't mark it required, because it is not for all sub-commands.
				// We need it as it is used by majority of the sub-commands and
				// because of the existing format of commands invocation.
				Name:  configFlagName,
				Usage: "load configuration from `FILE`",
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			// Praefect service manages os.Interrupt on its own, by making a "table-flip".
			// That is why the signal listening is omitted if there are no arguments passed
			// (old-fashioned method of starting Praefect service) or 'serve' sub-command
			// is invoked. Other sub-commands require signal to be properly handled.
			args := cmd.Args().Slice()
			if len(args) == 0 || slices.Contains(args, "serve") {
				return ctx, nil
			}

			signal.Notify(interrupt, os.Interrupt)
			go func() {
				if _, ok := <-interrupt; ok {
					os.Exit(130) // indicates program was interrupted
				}
			}()

			return ctx, nil
		},
		After: func(context.Context, *cli.Command) error {
			close(interrupt)
			return nil
		},
	}
}

// mustProvideConfigFlag extracts value of the 'config' flag and returns it.
// If flag is not set the help for the command will be printed and terminated with exit code 2.
func mustProvideConfigFlag(ctx context.Context, cmd *cli.Command, command string) string {
	pathToConfigFile := cmd.String(configFlagName)
	if pathToConfigFile == "" {
		// We can't make 'config' flag required for all commands, but we still want the
		// same output to be printed if it is not provided.
		// It should be removed after migration to the `praefect CMD -config FILE`
		// where we can mark it as required for each sub-command.
		_ = cli.ShowCommandHelp(ctx, cmd, command)
		log.Printf("Required flag %q not set\n", configFlagName)
		os.Exit(2)
	}

	return pathToConfigFile
}
