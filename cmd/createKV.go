package cmd

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/tomekkolo/immudb-play/pkg/repository/immudb"
)

var createKVCmd = &cobra.Command{
	Use:   "kv <collection>",
	Short: "create kv collection in immudb",
	RunE:  createKV,
	Args:  cobra.ExactArgs(1),
}

func init() {
	createCmd.AddCommand(createKVCmd)
	createKVCmd.Flags().StringSlice("indexes", nil, "List of JSON fields to create indexes for. First entry is primary key")
}

func createKV(cmd *cobra.Command, args []string) error {
	err := runParentCmdE(cmd, args)
	if err != nil {
		return err
	}

	flagIndexes, _ := cmd.Flags().GetStringSlice("indexes")
	if flagParser == "pgaudit" {
		flagIndexes = []string{"statement_id", "timestamp", "audit_type", "class", "command"}
		log.WithField("indexes", flagIndexes).Info("Using default indexes for pgaudit parser")
	} else if flagParser == "wrap" {
		flagIndexes = []string{"uid", "timestamp"}
		log.WithField("indexes", flagIndexes).Info("Using default indexes for wrap parser")
	}

	if len(flagIndexes) == 0 {
		return errors.New("at least primary key needs to be specified")
	}

	jsonKVRepository, err := immudb.NewJsonKVRepository(immuCli, args[0])
	if err != nil {
		return fmt.Errorf("could not create json repository, %w", err)
	}

	err = jsonKVRepository.Create(flagIndexes)
	if err != nil {
		return err
	}

	cfgs := immudb.NewConfigs(immuCli)
	err = cfgs.Write(args[0], immudb.Config{Parser: flagParser, Type: "kv"})
	if err != nil {
		return fmt.Errorf("collection does not exist, please create one first")
	}

	return nil
}
