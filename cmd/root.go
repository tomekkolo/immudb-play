package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "immudb-audit",
	Short: "Store and audit your data",
}

func init() {
	rootCmd.PersistentFlags().String("collection", "default", "name of a collection")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
