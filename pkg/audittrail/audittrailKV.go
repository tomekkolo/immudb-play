package audittrail

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
)

func AuditTrailKV(queryOnly bool) {
	log.Printf("Starting KV Audit trail")
	opts := immudb.DefaultOptions().WithAddress("localhost").WithPort(3322)

	client := immudb.NewClient().WithOptions(opts)
	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	if err != nil {
		log.Fatal(err)
	}

	defer client.CloseSession(context.TODO())

	if !queryOnly {
		start := time.Now()
		// create entries
		for i := 0; i < 100; i++ {
			log.Printf("generating audit trail %d\n", i)
			aes := generateAuditTrail()
			// no kv transactions
			for _, ae := range aes {
				txh, err := client.SetAll(context.TODO(), &schema.SetRequest{
					KVs: []*schema.KeyValue{
						{Key: []byte(fmt.Sprintf("trail.id.{%s}", ae.Id)), Value: []byte(ae.Id)},
						{Key: []byte(fmt.Sprintf("trail.{%s}.id", ae.Id)), Value: []byte(ae.Id)},
						{Key: []byte(fmt.Sprintf("trail.{%s}.user", ae.Id)), Value: []byte(ae.User)},
						{Key: []byte(fmt.Sprintf("trail.{%s}.ts", ae.Id)), Value: []byte(fmt.Sprintf("%d", ae.Timestamp.Unix()))},
						{Key: []byte(fmt.Sprintf("trail.{%s}.action", ae.Id)), Value: []byte(fmt.Sprintf("%d", ae.Action))},
						{Key: []byte(fmt.Sprintf("trail.{%s}.sourceip", ae.Id)), Value: []byte(ae.SourceIP)},
						{Key: []byte(fmt.Sprintf("trail.{%s}.context", ae.Id)), Value: []byte(ae.Context)},
					},
				})
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("SetAll txh: %+v\n", txh)

				// group into single set for fast access, aka primary key
				// client.ZAdd(context.TODO(), []byte(fmt.Sprintf("trail.{%s}", ae.Id)), 0, []byte(fmt.Sprintf("trail.id.{%s}", ae.Id)))
				// client.ZAdd(context.TODO(), []byte(fmt.Sprintf("trail.{%s}", ae.Id)), 0, []byte(fmt.Sprintf("trail.id.{%s}.user", ae.Id)))
				// client.ZAdd(context.TODO(), []byte(fmt.Sprintf("trail.{%s}", ae.Id)), 0, []byte(fmt.Sprintf("trail.id.{%s}.ts", ae.Id)))
				// client.ZAdd(context.TODO(), []byte(fmt.Sprintf("trail.{%s}", ae.Id)), 0, []byte(fmt.Sprintf("trail.id.{%s}.action", ae.Id)))
				// client.ZAdd(context.TODO(), []byte(fmt.Sprintf("trail.{%s}", ae.Id)), 0, []byte(fmt.Sprintf("trail.id.{%s}.sourceip", ae.Id)))
				// client.ZAdd(context.TODO(), []byte(fmt.Sprintf("trail.{%s}", ae.Id)), 0, []byte(fmt.Sprintf("trail.id.{%s}.context", ae.Id)))

				// create secondary index on column user
				// client.ZAdd(context.TODO(), []byte(fmt.Sprintf("trail.user.{%s}", ae.User)), 0, []byte(fmt.Sprintf("trail.id.{%s}", ae.Id)))
				// client.ZAdd(context.TODO(), []byte(fmt.Sprintf("trail.user.{%s}", ae.User)), 0, []byte(fmt.Sprintf("trail.id.{%s}.user", ae.Id)))
				// client.ZAdd(context.TODO(), []byte(fmt.Sprintf("trail.user.{%s}", ae.User)), 0, []byte(fmt.Sprintf("trail.id.{%s}.ts", ae.Id)))
				// client.ZAdd(context.TODO(), []byte(fmt.Sprintf("trail.user.{%s}", ae.User)), 0, []byte(fmt.Sprintf("trail.id.{%s}.action", ae.Id)))
				// client.ZAdd(context.TODO(), []byte(fmt.Sprintf("trail.user.{%s}", ae.User)), 0, []byte(fmt.Sprintf("trail.id.{%s}.sourceip", ae.Id)))
				// client.ZAdd(context.TODO(), []byte(fmt.Sprintf("trail.user.{%s}", ae.User)), 0, []byte(fmt.Sprintf("trail.id.{%s}.context", ae.Id)))
			}

			fmt.Printf("Create audit trail %d\n", i)
		}

		end := time.Now()
		fmt.Printf("Creating table took: %s", end.Sub(start).String())
	}

	entries, err := client.Scan(context.TODO(), &schema.ScanRequest{
		Prefix: []byte("trail.id"),
		Limit:  200,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range entries.Entries {
		log.Printf("Key: %s, Val: %s\n", string(e.Key), string(e.Value))
	}

	// retrieve some entries
	// var txOffset uint64
	// txOffset = 0
	// seekKey := []byte("")
	// for {
	// 	entries, err := client.ZScan(context.TODO(), &schema.ZScanRequest{
	// 		Set: []byte("trail.user.{user52}"),
	// 		//Set:           []byte("trail.id.{fd1c34ec-df98-4826-b363-504b2d031c25}"),
	// 		Limit:         999,
	// 		SeekKey:       seekKey,
	// 		InclusiveSeek: false,
	// 	})
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	if len(entries.Entries) == 0 {
	// 		break
	// 	}

	// 	for _, entry := range entries.Entries {
	// 		fmt.Printf("txOffset: %d, Key: %s, RefKey: %s, RefVal: %s\n", txOffset, string(entry.Key), string(entry.Entry.Key), string(entry.Entry.Value))
	// 		txOffset = entry.Entry.Tx
	// 		seekKey = entry.Entry.Key
	// 	}

	// 	txOffset = txOffset + 1
	// }
}
