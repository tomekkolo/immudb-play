package lineparser

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
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

type PGAuditLineParser struct {
}

func NewPGAuditLineParser() *PGAuditLineParser {
	return &PGAuditLineParser{}
}

func (p *PGAuditLineParser) Parse(line string) ([]byte, error) {
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

	fmt.Printf("line[:26+pos]: %s\n", line[26+pos:])
	pos = strings.Index(line[26+pos:], "AUDIT: ")
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

	statementID, err := strconv.Atoi(csvFields[1])
	if err != nil {
		return nil, fmt.Errorf("could not parse statementID, %w", err)
	}

	substatementID, err := strconv.Atoi(csvFields[2])
	if err != nil {
		return nil, fmt.Errorf("could not parse substatementID, %w", err)
	}

	pgae := &PGAuditEntry{
		Timestamp:      time.Now().UTC(),
		LogTimestamp:   ts,
		AuditType:      csvFields[0],
		StatementID:    statementID,
		SubstatementID: substatementID,
		Class:          csvFields[3],
		Command:        csvFields[4],
		ObjectType:     csvFields[5],
		ObjectName:     csvFields[6],
		Statement:      csvFields[7],
		Parameter:      csvFields[8],
	}

	bytes, err := json.Marshal(pgae)
	if err != nil {
		return nil, fmt.Errorf("could not marshal pg audit entry, %w", err)
	}

	return bytes, nil
}

// func PGAuditTrail(queryOnly bool) {
// 	log.Printf("Starting KV PGAudit trail")
// 	opts := immudb.DefaultOptions().WithAddress("localhost").WithPort(3322)

// 	client := immudb.NewClient().WithOptions(opts)
// 	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	defer client.CloseSession(context.TODO())
// 	immuObjects := immudbrepository.NewJsonRepository(client, "pgaudit", []string{"statement_id", "timestamp", "audit_type", "class", "command"})

// 	pgAuditFile, err := os.Open("test/pgaudit.log")
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	pgAuditScanner := bufio.NewScanner(pgAuditFile)
// 	pgAuditScanner.Split(bufio.ScanLines)

// 	for pgAuditScanner.Scan() {
// 		line := pgAuditScanner.Text()

// 		pga, err := parsePgAuditLine(line)
// 		if err != nil {
// 			log.Printf("error parsing line: %v", err)
// 			continue
// 		}

// 		txID, err := immuObjects.Store(pga)
// 		if err != nil {
// 			log.Fatal(err)
// 		}

// 		log.Printf("stored object with txID %d: %+v\n", txID, pga)
// 	}

// 	objects, err := immuObjects.Restore("statement_id", "92")
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	log.Printf("OBJECTS: %+v, COUNT: %d\n", objects, len(objects))

// 	objectsHistory, err := immuObjects.History("921")
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	log.Printf("History OBJECTS: %+v, COUNT: %d\n", objectsHistory, len(objectsHistory))
// }
