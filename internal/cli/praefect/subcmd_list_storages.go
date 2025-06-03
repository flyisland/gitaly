package praefect

import (
	"context"
	"fmt"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

func newListStoragesCommand() *cli.Command {
	return &cli.Command{
		Name:  "list-storages",
		Usage: "list physical storages for virtual storages",
		Description: `List the physical storages connected to virtual storages.

Returns a table with the following columns:

- VIRTUAL_STORAGE: Name of the virtual storage Praefect provides to clients.
- NODE: Name the physical storage connected to the virtual storage.
- ADDRESS: Address of the Gitaly node that manages the physical storage for the virtual storage.

If the virtual-storage flag:

- Is specified, lists only physical storages for the specified virtual storage.
- Is not specified, lists physical storages for all virtual storages.

Example: praefect --config praefect.config.toml list-storages --virtual-storage default`,
		Action: listStoragesAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  paramVirtualStorage,
				Usage: "name of the virtual storage to list physical storages for",
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

func listStoragesAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	conf, err := readConfig(cmd.String(configFlagName))
	if err != nil {
		return err
	}

	table := tablewriter.NewTable(cmd.Writer,
		tablewriter.WithRenderer(renderer.NewBlueprint(tw.Rendition{
			Borders: tw.BorderNone,
			Settings: tw.Settings{
				Lines: tw.Lines{
					ShowHeaderLine: tw.Off,
				},
				Separators: tw.Separators{
					BetweenRows:    tw.Off,
					BetweenColumns: tw.Off,
				},
			},
		})),
	)

	table.Configure(func(cfg *tablewriter.Config) {
		cfg.Header.Formatting.AutoFormat = tw.Off
		cfg.Header.Alignment.Global = tw.AlignLeft
		cfg.Row.Alignment.Global = tw.AlignLeft
	})

	table.Header([]string{"VIRTUAL_STORAGE", "NODE", "ADDRESS"})

	if pickedVirtualStorage := cmd.String(paramVirtualStorage); pickedVirtualStorage != "" {
		for _, virtualStorage := range conf.VirtualStorages {
			if virtualStorage.Name != pickedVirtualStorage {
				continue
			}

			for _, node := range virtualStorage.Nodes {
				if err := table.Append([]string{virtualStorage.Name, node.Storage, node.Address}); err != nil {
					return fmt.Errorf("append: %w", err)
				}
			}

			return table.Render()
		}

		fmt.Fprintf(cmd.Writer, "No virtual storages named %s.\n", pickedVirtualStorage)

		return nil
	}

	for _, virtualStorage := range conf.VirtualStorages {
		for _, node := range virtualStorage.Nodes {
			if err := table.Append([]string{virtualStorage.Name, node.Storage, node.Address}); err != nil {
				return fmt.Errorf("append: %w", err)
			}
		}
	}

	return table.Render()
}
