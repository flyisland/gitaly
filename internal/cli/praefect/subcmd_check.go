package praefect

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/service"
)

func newCheckCommand(checkFuncs []service.CheckFunc) *cli.Command {
	return &cli.Command{
		Name:  "check",
		Usage: "run startup checks",
		Description: `Run Praefect startup checks.

Example: praefect --config praefect.config.toml check`,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "q",
				Usage: "do not print verbose output for each check",
			},
		},
		Action: checkAction(checkFuncs),
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Args().Present() {
				_ = cli.ShowSubcommandHelp(cmd)
				return nil, cli.Exit(unexpectedPositionalArgsError{Command: cmd.Name}, 1)
			}
			return ctx, nil
		},
	}
}

var errFatalChecksFailed = errors.New("checks failed")

func checkAction(checkFuncs []service.CheckFunc) cli.ActionFunc {
	return func(ctx context.Context, cmd *cli.Command) error {
		log.ConfigureCommand()

		conf, err := readConfig(cmd.String(configFlagName))
		if err != nil {
			return err
		}

		quiet := cmd.Bool("q")
		var allChecks []*service.Check
		for _, checkFunc := range checkFuncs {
			allChecks = append(allChecks, checkFunc(conf, cmd.Writer, quiet))
		}

		passed := true
		var failedChecks int
		for _, check := range allChecks {
			func() {
				timeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				printCheckDetails(quiet, cmd.Writer, check)

				if err := check.Run(timeCtx); err != nil {
					failedChecks++
					if check.Severity == service.Fatal {
						passed = false
					}
					fmt.Fprintf(cmd.Writer, "Failed (%s) error: %s\n", check.Severity, err.Error())
				} else {
					fmt.Fprintf(cmd.Writer, "Passed\n")
				}
			}()
		}

		fmt.Fprintf(cmd.Writer, "\n")

		if !passed {
			fmt.Fprintf(cmd.Writer, "%d check(s) failed, at least one was fatal.\n", failedChecks)
			return errFatalChecksFailed
		}

		if failedChecks > 0 {
			fmt.Fprintf(cmd.Writer, "%d check(s) failed, but none are fatal.\n", failedChecks)
		} else {
			fmt.Fprintf(cmd.Writer, "All checks passed.\n")
		}

		return nil
	}
}

func printCheckDetails(quiet bool, w io.Writer, check *service.Check) {
	if quiet {
		fmt.Fprintf(w, "Checking %s...", check.Name)
		return
	}

	fmt.Fprintf(w, "Checking %s - %s [%s]\n", check.Name, check.Description, check.Severity)
}
