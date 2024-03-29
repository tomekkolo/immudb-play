package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tomekkolo/immudb-play/pkg/repository/immudb"
)

var auditSQLCmd = &cobra.Command{
	Use:     "sql <collection> <<temporal query range and condition>>",
	Short:   "Audit your sql collection with temporal queries",
	Example: "immudb-audit audit sql samplecollection \"SINCE '2022-01-06 11:38' UNTIL '2022-01-06 12:00' WHERE id=1\"",
	Args:    cobra.MinimumNArgs(1),
	RunE:    auditSQL,
}

func init() {
	auditCmd.AddCommand(auditSQLCmd)
}

func auditSQL(cmd *cobra.Command, args []string) error {
	err := runParentCmdE(cmd, args)
	if err != nil {
		return err
	}

	jr, err := immudb.NewJsonSQLRepository(immuCli, args[0])
	if err != nil {
		return fmt.Errorf("could not create json sql repository, %w", err)
	}

	query := ""
	if len(args) == 2 {
		query = args[1]
	}

	history, err := jr.History(query)
	if err != nil {
		return fmt.Errorf("could not get audit, %w", err)
	}

	for _, h := range history {
		fmt.Println(string(h))
	}
	return nil
}
