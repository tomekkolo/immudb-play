package audittrail

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	immudb "github.com/codenotary/immudb/pkg/client"
	immudbrepository "github.com/tomekkolo/immudb-play/pkg/repository/immudb"
)

type PGAuditEntry struct {
	Timestamp      time.Time `json:"timestamp"`
	LogTimestamp   time.Time `json:"log_timestamp"`
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

type PGAudit struct {
	source io.Reader
}

func parsePgAuditLine(line string) (*PGAuditEntry, error) {
	// assumed default log_line_prefix '%m [%p] '
	if len(line) < 26 { // min length of timestamp with timezone
		return nil, fmt.Errorf("invalid log line prefix, too short")
	}

	pos := strings.Index(line[26:], " ") // find end of timezone abbreviation
	if pos < 0 {
		return nil, fmt.Errorf("invalid log line prefix")
	}

	ts, err := time.Parse("2006-02-01 15:04:05.000 MST", line[:26+pos])
	if err != nil {
		return nil, fmt.Errorf("could not parse timestamp: %w", err)
	}

	pos = strings.Index(line[:26+pos], "AUDIT: ")
	if pos < 0 {
		return nil, fmt.Errorf("not a pgaudit line")
	}

	csvReader := csv.NewReader(strings.NewReader(line[pos+len("AUDIT: "):]))
	csvFields, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("invalid csv line, %w", err)
	}

	if len(csvFields) < 9 {
		return nil, fmt.Errorf("invalid csv fields length: %d", len(csvFields))
	}

	return &PGAuditEntry{
		Timestamp:    time.Now().UTC(),
		LogTimestamp: ts,
		AuditType:    csvFields[0],
		Class:        csvFields[3],
		Command:      csvFields[4],
		ObjectType:   csvFields[5],
		ObjectName:   csvFields[6],
		Statement:    csvFields[7],
		Parameter:    csvFields[8],
	}, nil
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
	immuObjects := immudbrepository.NewJsonRepository(client, "pgaudit", []string{"statement_id", "timestamp", "audit_type", "class", "command"})

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

		pga := PGAuditEntry{
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

	objectsHistory, err := immuObjects.History("921")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("History OBJECTS: %+v, COUNT: %d\n", objectsHistory, len(objectsHistory))
}
