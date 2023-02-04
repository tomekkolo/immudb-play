package audittrail

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type ImmudbObjects struct {
	client      immudb.ImmuClient
	collection  string
	indexedKeys []string
}

func (imo *ImmudbObjects) Store(object interface{}) (uint64, error) {
	objectBytes, err := json.Marshal(object)
	if err != nil {
		return 0, fmt.Errorf("could not marshal object: %w", err)
	}

	return imo.StoreBytes(objectBytes)
}

func (imo *ImmudbObjects) StoreBytes(objectBytes []byte) (uint64, error) {
	if len(imo.indexedKeys) == 0 {
		return 0, errors.New("primary key is mandataory")
	}

	//parse with gjson
	gjsonObject := gjson.ParseBytes(objectBytes)
	gjPK := gjsonObject.Get(imo.indexedKeys[0])
	if !gjPK.Exists() {
		return 0, errors.New("missing primary key in object")
	}

	immudbObjectRequest := &schema.SetRequest{
		KVs: []*schema.KeyValue{
			{ // crete primary key index
				Key:   []byte(fmt.Sprintf("%s.%s.{%s}", imo.collection, imo.indexedKeys[0], gjPK.String())),
				Value: []byte(gjPK.String()),
			},
		},
	}

	for i := 1; i < len(imo.indexedKeys); i++ {
		gjSK := gjsonObject.Get(imo.indexedKeys[i])
		if !gjSK.Exists() {
			return 0, errors.New("missing secondary key in object")
		}

		immudbObjectRequest.KVs = append(immudbObjectRequest.KVs,
			&schema.KeyValue{ // crete secondary key index <collection>.<SKName>.<SKVALUE>.<PKVALUE>
				Key:   []byte(fmt.Sprintf("%s.%s.{%s}.{%s}", imo.collection, imo.indexedKeys[i], gjSK.String(), gjPK.String())),
				Value: []byte(gjPK.String()),
			},
		)
	}

	// iterate over all object fields
	gjsonObject.ForEach(func(key, value gjson.Result) bool {
		immudbObjectRequest.KVs = append(immudbObjectRequest.KVs, &schema.KeyValue{
			Key:   []byte(fmt.Sprintf("%s.{%s}.%s", imo.collection, gjPK.String(), key.Str)),
			Value: []byte(value.String()),
		})
		return true
	})

	txh, err := imo.client.SetAll(context.TODO(), immudbObjectRequest)
	if err != nil {
		return 0, fmt.Errorf("could not store object: %w", err)
	}

	// Create set at transaction for later retrieval of versioned object
	for i, or := range immudbObjectRequest.KVs {
		if i < len(imo.indexedKeys) {
			continue
		}
		_, err := imo.client.ZAddAt(context.TODO(),
			[]byte(fmt.Sprintf("%s.%s.{%s}.%d", imo.collection, imo.indexedKeys[0], gjPK.String(), txh.Id)),
			0,
			or.Key,
			txh.Id,
		)
		if err != nil {
			log.Fatal(err)
		}
	}

	// create index for set versions
	_, err = imo.client.Set(context.TODO(),
		[]byte(fmt.Sprintf("%s.versions.%s.{%s}", imo.collection, imo.indexedKeys[0], gjPK.String())), []byte(fmt.Sprintf("%d", txh.Id)))

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Object set %v\n", txh)

	return txh.Id, nil
}

// for now just based on SK
func (imo *ImmudbObjects) Restore(key, condition string) ([]string, error) {
	log.Printf("Restore: %s\n", fmt.Sprintf("%s.%s.{%s", imo.collection, key, condition))
	seekKey := []byte("")
	var objects []string
	for {
		entries, err := imo.client.Scan(context.TODO(), &schema.ScanRequest{
			Prefix:  []byte(fmt.Sprintf("%s.%s.{%s", imo.collection, key, condition)),
			SeekKey: seekKey,
			Limit:   999,
		})
		if err != nil {
			return nil, fmt.Errorf("could not scan for objects, %w", err)
		}

		if len(entries.Entries) == 0 {
			break
		}

		for _, e := range entries.Entries {
			// retrieve an object
			objectEntries, err := imo.client.Scan(context.Background(), &schema.ScanRequest{
				Prefix: []byte(fmt.Sprintf("%s.{%s}", imo.collection, string(e.Value))),
			})
			if err != nil {
				return nil, fmt.Errorf("could not scan for object, %w", err)
			}

			jObject := ""
			for _, oe := range objectEntries.Entries {
				jObject, err = sjson.Set(jObject, strings.TrimPrefix(string(oe.Key), fmt.Sprintf("%s.{%s}.", imo.collection, string(e.Value))), string(oe.Value))
				if err != nil {
					return nil, fmt.Errorf("could not restore object: %w", err)
				}
			}

			// Maybe setting secondary index for fast retrieval of an object would speed up gets ?

			// var object map[string]interface{}
			// err = json.Unmarshal([]byte(jObject), &object)
			// if err != nil {
			// 	return nil, fmt.Errorf("could not unmarshal to an object: %w", err)
			// }

			seekKey = e.Key
			objects = append(objects, jObject)
		}
	}

	return objects, nil
}

type ObjectHistory struct {
	Object   string
	TxID     uint64
	Revision uint64
}

func (imo *ImmudbObjects) RestoreHistory(primaryKeyValue string) ([]ObjectHistory, error) {
	entries, err := imo.client.History(context.TODO(), &schema.HistoryRequest{
		Key: []byte(fmt.Sprintf("%s.versions.%s.{%s}", imo.collection, imo.indexedKeys[0], primaryKeyValue)),
	})
	if err != nil {
		log.Fatal(err)
	}

	objects := []ObjectHistory{}
	for _, e := range entries.Entries {
		log.Printf("HISTORY: Key: %s, Value: %s\n", string(e.Key), string(e.Value))
		ze, err := imo.client.ZScan(context.Background(), &schema.ZScanRequest{
			Set: []byte(fmt.Sprintf("%s.%s.{%s}.%s", imo.collection, imo.indexedKeys[0], primaryKeyValue, string(e.Value))),
		})
		if err != nil {
			log.Fatal(err)
		}

		jObject := ""
		for _, ze := range ze.Entries {
			jObject, err = sjson.Set(jObject, strings.TrimPrefix(string(ze.Entry.Key), fmt.Sprintf("%s.{%s}.", imo.collection, primaryKeyValue)), string(ze.Entry.Value))
			if err != nil {
				return nil, fmt.Errorf("could not restore object: %w", err)
			}
		}

		objects = append(objects, ObjectHistory{
			Object:   jObject,
			TxID:     e.Tx,
			Revision: e.Revision,
		})
	}

	return objects, nil
}

func AuditTrailKVGjson(queryOnly bool) {
	log.Printf("Starting KV Audit trail")
	opts := immudb.DefaultOptions().WithAddress("localhost").WithPort(3322)

	client := immudb.NewClient().WithOptions(opts)
	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	if err != nil {
		log.Fatal(err)
	}

	defer client.CloseSession(context.TODO())
	immuObjects := ImmudbObjects{
		client:      client,
		collection:  "trail",
		indexedKeys: []string{"id", "user", "action"},
	}

	if !queryOnly {
		start := time.Now()
		// create entries
		for i := 0; i < 1000; i++ {
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

	objects, err := immuObjects.Restore("user", "user10")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("OBJECTS: %+v, COUNT: %d\n", objects, len(objects))
	// for _, object := range objects {
	// 	var ae auditEntry
	// 	// this will not work as we do not keep types
	// 	err = json.Unmarshal([]byte(object), &ae)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	log.Printf("Audit entry restored: %+v\n", ae)
	// }
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
	immuObjects := ImmudbObjects{
		client:      client,
		collection:  "pgaudit",
		indexedKeys: []string{"statement_id", "timestamp", "audit_type", "class", "command"},
	}

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
