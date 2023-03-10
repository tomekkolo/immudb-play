package cmd

import (
	"context"
	"fmt"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
	"github.com/tomekkolo/immudb-play/pkg/repository/immudb"
)

var auditCmd = &cobra.Command{
	Use:   "audit",
	Short: "audit data from immudb",
	RunE:  audit,
	Args:  cobra.ExactArgs(1),
}

func init() {
	rootCmd.AddCommand(auditCmd)
}

func audit(cmd *cobra.Command, args []string) error {
	opts := client.DefaultOptions().WithAddress("localhost").WithPort(3322)
	immuCli := client.NewClient().WithOptions(opts)
	err := immuCli.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	if err != nil {
		return err
	}
	defer immuCli.CloseSession(context.TODO())

	collection, _ := cmd.Flags().GetString("collection")
	jsonRepository := immudb.NewJsonKVRepository(immuCli, collection, []string{"auditID"})
	history, err := jsonRepository.History(args[0])
	if err != nil {
		return fmt.Errorf("could not read, %w", err)
	}

	for _, h := range history {
		fmt.Printf("{\"tx_id\": %d, \"revision\": %d, \"entry\": %s}\n", h.TxID, h.Revision, string(h.Entry))
	}
	return nil
}

// func (pga *AuditService) History(primaryKey string) ([]AuditHistoryEntry, error) {
// 	historyEntries, err := pga.jsonRepository.History(primaryKey)
// 	if err != nil {
// 		return nil, fmt.Errorf("could not read history of %s, %w", primaryKey, err)
// 	}

// 	var auditHistoryEntries []AuditHistoryEntry
// 	for _, he := range historyEntries {
// 		var auditHistoryEntry AuditHistoryEntry
// 		auditHistoryEntry.Entry = he.Entry
// 		auditHistoryEntry.Revision = he.Revision
// 		auditHistoryEntry.TXID = he.TxID

// 		auditHistoryEntries = append(auditHistoryEntries, auditHistoryEntry)
// 	}

// 	return auditHistoryEntries, nil
// }

// }
