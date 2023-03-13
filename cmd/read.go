package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var readCmd = &cobra.Command{
	Use:   "read <collection> <index>",
	Short: "Read audit data from immudb",
	RunE:  read,
	Args:  cobra.ExactArgs(2),
}

func init() {
	rootCmd.AddCommand(readCmd)
	readCmd.Flags().String("query", "", "query expression")
}

func read(cmd *cobra.Command, args []string) error {
	err := runParentCmdE(cmd, args)
	if err != nil {
		return err
	}

	err = configure(args[0])
	if err != nil {
		return err
	}

	query, _ := cmd.Flags().GetString("query")

	jsons, err := jsonRepository.Read(args[1], query)
	if err != nil {
		return fmt.Errorf("could not read, %w", err)
	}

	for _, j := range jsons {
		fmt.Println(string(j))
	}
	return nil
}
