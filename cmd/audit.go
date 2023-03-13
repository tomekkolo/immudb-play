package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var auditCmd = &cobra.Command{
	Use:   "audit <collection> <primary key>",
	Short: "Audit data from immudb",
	RunE:  audit,
	Args:  cobra.ExactArgs(2),
}

func init() {
	rootCmd.AddCommand(auditCmd)
}

func audit(cmd *cobra.Command, args []string) error {
	err := runParentCmdE(cmd, args)
	if err != nil {
		return err
	}

	err = configure(args[0])
	if err != nil {
		return err
	}

	history, err := jsonRepository.History(args[1])
	if err != nil {
		return fmt.Errorf("could not read, %w", err)
	}

	for _, h := range history {
		fmt.Printf("{\"tx_id\": %d, \"revision\": %d, \"entry\": %s}\n", h.TxID, h.Revision, string(h.Entry))
	}
	return nil
}
