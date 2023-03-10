package main

import (
	"context"
	"flag"
	"log"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/tomekkolo/immudb-play/pkg/pgaudit"
	"github.com/tomekkolo/immudb-play/pkg/repository/immudb"
	"github.com/tomekkolo/immudb-play/pkg/service"
	"github.com/tomekkolo/immudb-play/pkg/source"
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
	immuCli := client.NewClient().WithOptions(opts)
	err := immuCli.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	if err != nil {
		log.Fatal(err)
	}
	defer immuCli.CloseSession(context.TODO())

	// log.Printf("flagAuditTrailJson: %t\n", flagAuditTrailJson)
	// if flagAuditTrailJson {
	// 	// configure repository
	// 	jsonRepository := immudb.NewJsonKVRepository(client, "trail", []string{"id", "user", "action"})
	// 	auditTrail := audittrail.NewAuditTrailJson(jsonRepository, flagQueryOnly, 10, "user", "user1")
	// 	auditTrail.Run()
	// }

	// if flagPgauditTrail {

	// }

	jsonRepository := immudb.NewJsonKVRepository(immuCli, "pgaudit", []string{"statement_id", "timestamp", "audit_type", "class", "command"})

	fileTail, err := source.NewFileTail("test/pgaudit.log", false)
	if err != nil {
		log.Panic(err)
	}

	s := service.NewAuditService(fileTail, pgaudit.NewPGAuditLineParser(), jsonRepository)
	s.Run()

	//audittrail.AuditTrailSQL(queryOnly)
	//audittrail.AuditTrailKV(queryOnly)
	//audittrail.AuditTrailKVGjson(queryOnly)
	//audittrail.PopulatePSQL()
	//audittrail.PGAuditTrail(queryOnly)
}
