package main

import (
	"os"

	"github.com/tomekkolo/immudb-play/pkg/audittrail"
)

func main() {
	queryOnly := false
	if len(os.Args) > 1 {
		if os.Args[1] == "query" {
			queryOnly = true
		}
	}
	//audittrail.AuditTrailSQL(queryOnly)
	//audittrail.AuditTrailKV(queryOnly)
	audittrail.AuditTrailKVGjson(queryOnly)
}
