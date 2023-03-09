package main

import (
	"context"
	"flag"
	"log"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/tomekkolo/immudb-play/pkg/audittrail"
	"github.com/tomekkolo/immudb-play/pkg/repository/immudb"
)

var flagQueryOnly bool
var flagAuditTrailJson bool
var flagPgauditTrail bool

func init() {
	flag.BoolVar(&flagQueryOnly, "query-only", false, "if True, do not save into immudb")
	flag.BoolVar(&flagAuditTrailJson, "audit-trail-json", false, "if True, run AuditTrailJson")
	flag.BoolVar(&flagPgauditTrail, "pgaudit", false, "if True, run pgaudit")
	flag.Parse()
}

func main() {
	// create immudb client
	opts := client.DefaultOptions().WithAddress("localhost").WithPort(3322)
	client := client.NewClient().WithOptions(opts)
	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	if err != nil {
		log.Fatal(err)
	}
	defer client.CloseSession(context.TODO())

	log.Printf("flagAuditTrailJson: %t\n", flagAuditTrailJson)
	if flagAuditTrailJson {
		// configure repository
		jsonRepository := immudb.NewJsonRepository(client, "trail", []string{"id", "user", "action"})
		auditTrail := audittrail.NewAuditTrailJson(jsonRepository, flagQueryOnly, 10, "user", "user1")
		auditTrail.Run()
	}

	//audittrail.AuditTrailSQL(queryOnly)
	//audittrail.AuditTrailKV(queryOnly)
	//audittrail.AuditTrailKVGjson(queryOnly)
	//audittrail.PopulatePSQL()
	//audittrail.PGAuditTrail(queryOnly)
}
