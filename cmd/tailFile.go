package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tomekkolo/immudb-play/pkg/service"
	"github.com/tomekkolo/immudb-play/pkg/source"
)

var tailFileCmd = &cobra.Command{
	Use:   "file <collection> <file>",
	Short: "Tail from file and store audit data in immudb",
	RunE:  tailFile,
	Args:  cobra.ExactArgs(2),
}

func tailFile(cmd *cobra.Command, args []string) error {
	err := runParentCmdE(cmd, args)
	if err != nil {
		return err
	}

	err = configure(args[0])
	if err != nil {
		return err
	}

	fileTail, err := source.NewFileTail(args[1], flagFollow)
	if err != nil {
		return fmt.Errorf("invalid source: %w", err)
	}

	s := service.NewAuditService(fileTail, lp, jsonRepository)
	return s.Run()
}

func init() {
	tailCmd.AddCommand(tailFileCmd)
}
