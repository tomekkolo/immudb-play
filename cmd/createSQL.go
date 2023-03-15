package cmd

import (
	"errors"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/tomekkolo/immudb-play/pkg/repository/immudb"
)

var createSQLCmd = &cobra.Command{
	Use:   "sql <collection>",
	Short: "Create collection in immudb with SQL",
	RunE:  createSQL,
	Args:  cobra.ExactArgs(1),
}

func init() {
	createCmd.AddCommand(createSQLCmd)
	createSQLCmd.Flags().StringSlice("primary-key", nil, "List of columns to be used as primary key")
	createSQLCmd.Flags().StringSlice("columns", nil, "List of fields to be used as columns")
}

func createSQL(cmd *cobra.Command, args []string) error {
	err := runParentCmdE(cmd, args)
	if err != nil {
		return err
	}

	primaryKey, _ := cmd.Flags().GetStringSlice("primary-key")
	flagColumns, _ := cmd.Flags().GetStringSlice("columns")
	if flagParser == "pgaudit" {
		flagColumns = []string{"statement_id=INTEGER", "log_timestamp=TIMESTAMP", "\"timestamp\"=TIMESTAMP", "audit_type=VARCHAR[256]", "class=VARCHAR[256]", "command=VARCHAR[256]"}
		primaryKey = []string{"statement_id"}
		log.WithField("columns", flagColumns).WithField("primary_key", primaryKey).Info("Using default indexes for pgaudit parser")
	} else if flagParser == "wrap" {
		flagColumns = []string{"uid=VARCHAR", "timestamp=TIMESTAMP"}
		primaryKey = []string{"uid"}
		log.WithField("columns", flagColumns).WithField("primary_key", primaryKey).Info("Using default indexes for wrap parser")
	}

	if len(flagColumns) == 0 || len(primaryKey) == 0 {
		return errors.New("at least one column and primary key needs to be specified")
	}

	cfgs := immudb.NewConfigs(immuCli)
	err = cfgs.Write(args[0], immudb.Config{Parser: flagParser, Type: "sql"})
	if err != nil {
		return fmt.Errorf("collection does not exist, please create one first")
	}

	immudb.SetupJsonSQLRepository(immuCli, args[0], strings.Join(primaryKey, ","), flagColumns)

	return nil
}
