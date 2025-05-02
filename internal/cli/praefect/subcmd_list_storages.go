package praefect

import (
	"context"
	"fmt"

	"github.com/olekukonko/tablewriter"
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

	table := tablewriter.NewWriter(cmd.Writer)
	table.SetHeader([]string{"VIRTUAL_STORAGE", "NODE", "ADDRESS"})
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAutoFormatHeaders(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetBorder(false)
	table.SetTablePadding("\t") // pad with tabs
	table.SetNoWhiteSpace(true)

	if pickedVirtualStorage := cmd.String(paramVirtualStorage); pickedVirtualStorage != "" {
		for _, virtualStorage := range conf.VirtualStorages {
			if virtualStorage.Name != pickedVirtualStorage {
				continue
			}

			for _, node := range virtualStorage.Nodes {
				table.Append([]string{virtualStorage.Name, node.Storage, node.Address})
			}

			table.Render()

			return nil
		}

		fmt.Fprintf(cmd.Writer, "No virtual storages named %s.\n", pickedVirtualStorage)

		return nil
	}

	for _, virtualStorage := range conf.VirtualStorages {
		for _, node := range virtualStorage.Nodes {
			table.Append([]string{virtualStorage.Name, node.Storage, node.Address})
		}
	}

	table.Render()

	return nil
}
