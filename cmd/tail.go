package cmd

import "github.com/spf13/cobra"

var tailCmd = &cobra.Command{
	Use:   "tail",
	Short: "Tail your source and store audit data in immudb",
}

func init() {
	rootCmd.AddCommand(tailCmd)
	tailCmd.PersistentFlags().Bool("follow", false, "if True, follow data stream")
	rootCmd.PersistentFlags().StringSlice("indexes", nil, "list of fields to create indexes. First entry is primary key")
	rootCmd.PersistentFlags().String("parser", "", "line parser to be used. When not specified, lines will be considered as jsons. Also available 'pgaudit'")
	//rootCmd.MarkFlagRequired("indexes")
}
