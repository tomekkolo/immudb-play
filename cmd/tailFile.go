package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
	"github.com/tomekkolo/immudb-play/pkg/lineparser"
	"github.com/tomekkolo/immudb-play/pkg/repository/immudb"
	"github.com/tomekkolo/immudb-play/pkg/service"
	"github.com/tomekkolo/immudb-play/pkg/source"
)

var tailFileCmd = &cobra.Command{
	Use:   "file",
	Short: "Tail from file and store audit data in immudb",
	RunE:  tailFile,
	Args:  cobra.ExactArgs(1),
}

func tailFile(cmd *cobra.Command, args []string) error {
	opts := client.DefaultOptions().WithAddress("localhost").WithPort(3322)
	immuCli := client.NewClient().WithOptions(opts)
	err := immuCli.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	if err != nil {
		return err
	}
	defer immuCli.CloseSession(context.TODO())

	collection, _ := cmd.Flags().GetString("collection")
	indexes, _ := cmd.Flags().GetStringSlice("indexes")
	parser, _ := cmd.Flags().GetString("parser")
	var lp service.LineParser
	if parser == "" {
		lp = lineparser.NewDefaultLineParser()
		if len(indexes) < 1 {
			return errors.New("indexes definition is empty")
		}
	} else if parser == "pgaudit" {
		lp = lineparser.NewPGAuditLineParser()
		indexes = []string{"statement_id", "timestamp", "audit_type", "class", "command"}
	} else {
		return fmt.Errorf("not supported parser: %s", parser)
	}

	jsonRepository := immudb.NewJsonKVRepository(immuCli, collection, indexes)
	follow, _ := cmd.Flags().GetBool("follow")
	fileTail, err := source.NewFileTail(args[0], follow)
	if err != nil {
		log.Panic(err)
	}

	s := service.NewAuditService(fileTail, lp, jsonRepository)
	return s.Run()
}

func init() {
	tailCmd.AddCommand(tailFileCmd)
}
