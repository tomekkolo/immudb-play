package audittrail

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	immudb "github.com/codenotary/immudb/pkg/client"
	"github.com/tomekkolo/immudb-play/pkg/immuobject"
)

func AuditTrailKVGjson(queryOnly bool) {
	log.Printf("Starting KV Audit trail")
	opts := immudb.DefaultOptions().WithAddress("localhost").WithPort(3322)

	client := immudb.NewClient().WithOptions(opts)
	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	if err != nil {
		log.Fatal(err)
	}

	defer client.CloseSession(context.TODO())
	immuObjects := immuobject.New(client, "trail", []string{"id", "user", "action"})

	if !queryOnly {
		start := time.Now()
		// create entries
		for i := 0; i < 10; i++ {
			log.Printf("generating audit trail %d\n", i)
			aes := generateAuditTrail()
			// no kv transactions
			for _, ae := range aes {
				txID, err := immuObjects.Store(ae)
				if err != nil {
					log.Fatal(err)
				}

				log.Printf("Object set %d\n", txID)
			}

			fmt.Printf("Create audit trail %d\n", i)
		}

		end := time.Now()
		log.Printf("Creating table took: %s\n", end.Sub(start).String())
	}

	objects, err := immuObjects.Restore("user", "user")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("OBJECTS: %+v, COUNT: %d\n", objects, len(objects))
	for _, object := range objects {
		var ae auditEntry
		// this will not work as we do not keep types
		err = json.Unmarshal([]byte(object), &ae)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Audit entry restored: %+v\n", ae)
	}
}

type PGAudit struct {
	Timestamp      time.Time `json:"timestamp"`
	AuditType      string    `json:"audit_type"`
	StatementID    int       `json:"statement_id"`
	SubstatementID int       `json:"substatement_id,omitempty"`
	Class          string    `json:"class,omitempty"`
	Command        string    `json:"command,omitempty"`
	ObjectType     string    `json:"object_type,omitempty"`
	ObjectName     string    `json:"object_name,omitempty"`
	Statement      string    `json:"statement,omitempty"`
	Parameter      string    `json:"parameter,omitempty"`
}

func PGAuditTrail(queryOnly bool) {
	log.Printf("Starting KV PGAudit trail")
	opts := immudb.DefaultOptions().WithAddress("localhost").WithPort(3322)

	client := immudb.NewClient().WithOptions(opts)
	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	if err != nil {
		log.Fatal(err)
	}

	defer client.CloseSession(context.TODO())
	immuObjects := immuobject.New(client, "pgaudit", []string{"statement_id", "timestamp", "audit_type", "class", "command"})

	pgAuditFile, err := os.Open("test/pgaudit.log")
	if err != nil {
		log.Fatal(err)
	}

	pgAuditScanner := bufio.NewScanner(pgAuditFile)
	pgAuditScanner.Split(bufio.ScanLines)

	for pgAuditScanner.Scan() {
		line := pgAuditScanner.Text()
		splitLine := strings.Split(line, " [294] LOG:  AUDIT: ")
		if len(splitLine) != 2 {
			log.Println("could not split audit log")
			continue
		}
		//log.Printf("SPlit line: %+v\n", splitLine)
		ts, err := time.Parse("2006-02-01 15:04:05.000 GMT", splitLine[0])
		if err != nil {
			log.Printf("could not parse timestamp: %v\n", err)
			continue
		}

		auditCSV := csv.NewReader(strings.NewReader(splitLine[1]))
		auditCSVRecords, err := auditCSV.Read()
		if err != nil {
			log.Printf("could not parse csv line: %v", err)
			continue
		}

		if len(auditCSVRecords) < 9 {
			log.Printf("invalid audit length: %d\n", len(auditCSVRecords))
			continue
		}

		pga := PGAudit{
			Timestamp:  ts,
			AuditType:  auditCSVRecords[0],
			Class:      auditCSVRecords[3],
			Command:    auditCSVRecords[4],
			ObjectType: auditCSVRecords[5],
			ObjectName: auditCSVRecords[6],
			Statement:  auditCSVRecords[7],
			Parameter:  auditCSVRecords[8],
		}

		pga.StatementID, err = strconv.Atoi(auditCSVRecords[1])
		if err != nil {
			log.Printf("could not parse statementID, %v\n", err)
			continue
		}

		pga.SubstatementID, err = strconv.Atoi(auditCSVRecords[2])
		if err != nil {
			log.Printf("could not parse substatementID, %v\n", err)
			continue
		}

		txID, err := immuObjects.Store(pga)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("stored object with txID %d: %+v\n", txID, pga)
	}

	objects, err := immuObjects.Restore("statement_id", "92")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("OBJECTS: %+v, COUNT: %d\n", objects, len(objects))

	objectsHistory, err := immuObjects.RestoreHistory("921")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("History OBJECTS: %+v, COUNT: %d\n", objectsHistory, len(objectsHistory))
}
