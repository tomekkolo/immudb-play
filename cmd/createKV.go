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
	Short: "Create collection in immudb with key-value",
	Example: `immudb-audit create kv samplecollection --parser pgaudit
immudb-audit create kv samplecollection --indexes unique_field1,field2,field3
immudb-audit create kv samplecollection --indexes field1+field2,field2,field3`,
	RunE: createKV,
	Args: cobra.ExactArgs(1),
}

func init() {
	createCmd.AddCommand(createKVCmd)
	createKVCmd.Flags().StringSlice("indexes", nil, "List of JSON fields to create indexes for. First entry is considered as unique primary key. If needed, multiple fields can be used as primary key with syntax field1+field2...")
}

func createKV(cmd *cobra.Command, args []string) error {
	err := runParentCmdE(cmd, args)
	if err != nil {
		return err
	}

	flagIndexes, _ := cmd.Flags().GetStringSlice("indexes")
	if flagParser == "pgaudit" {
		flagIndexes = []string{"statement_id", "log_timestamp", "timestamp", "audit_type", "class", "command"}
		log.WithField("indexes", flagIndexes).Info("Using default indexes for pgaudit parser")
	} else if flagParser == "wrap" {
		flagIndexes = []string{"uid", "timestamp"}
		log.WithField("indexes", flagIndexes).Info("Using default indexes for wrap parser")
	} else if flagParser != "" {
		return fmt.Errorf("unkown parser %s", flagParser)
	}

	if len(flagIndexes) == 0 {
		return errors.New("at least primary key needs to be specified")
	}

	cfgs := immudb.NewConfigs(immuCli)
	err = cfgs.Write(args[0], immudb.Config{Parser: flagParser, Type: "kv", Indexes: flagIndexes})
	if err != nil {
		return fmt.Errorf("collection does not exist, please create one first")
	}

	err = immudb.SetupJsonKVRepository(immuCli, args[0], flagIndexes)
	if err != nil {
		return fmt.Errorf("could not create json repository, %w", err)
	}

	return nil
}
