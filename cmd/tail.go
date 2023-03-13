package cmd

import (
	"github.com/spf13/cobra"
)

var flagFollow bool

var tailCmd = &cobra.Command{
	Use:   "tail",
	Short: "Tail your source and store audit data in immudb",
	RunE:  tail,
}

func init() {
	rootCmd.AddCommand(tailCmd)
	tailCmd.PersistentFlags().BoolVar(&flagFollow, "follow", false, "If True, follow data stream")
}

func tail(cmd *cobra.Command, args []string) error {
	if cmd.CalledAs() == "tail" {
		return cmd.Help()
	}

	err := runParentCmdE(cmd, args)
	if err != nil {
		return err
	}

	return nil
}
