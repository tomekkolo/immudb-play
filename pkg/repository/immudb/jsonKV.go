package immudb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
	"github.com/tidwall/gjson"
)

type JsonKVRepository struct {
	client      immudb.ImmuClient
	collection  string
	indexedKeys []string // first key is considered primary key
}

func NewJsonKVRepository(client immudb.ImmuClient, collection string, indexedKeys []string) *JsonKVRepository {
	if len(indexedKeys) == 0 {
		log.Fatal("JsonRepository requires at least primary key")
	}

	return &JsonKVRepository{
		client:      client,
		collection:  collection,
		indexedKeys: indexedKeys,
	}
}

func (jr *JsonKVRepository) Write(jObject interface{}) (uint64, error) {
	objectBytes, err := json.Marshal(jObject)
	if err != nil {
		return 0, fmt.Errorf("could not marshal object: %w", err)
	}

	return jr.WriteBytes(objectBytes)
}

func (jr *JsonKVRepository) WriteBytes(jBytes []byte) (uint64, error) {
	if len(jr.indexedKeys) == 0 {
		return 0, errors.New("primary key is mandataory")
	}

	//parse with gjson
	gjsonObject := gjson.ParseBytes(jBytes)
	gjPK := gjsonObject.Get(jr.indexedKeys[0])
	if !gjPK.Exists() {
		return 0, errors.New("missing primary key in object")
	}

	log.Printf("PAYLOAD: %s", fmt.Sprintf("%s.payload.%s.{%s}", jr.collection, jr.indexedKeys[0], gjPK.String()))
	immudbObjectRequest := &schema.SetRequest{
		KVs: []*schema.KeyValue{
			{ // crete primary key index
				Key:   []byte(fmt.Sprintf("%s.%s.{%s}", jr.collection, jr.indexedKeys[0], gjPK.String())),
				Value: []byte(fmt.Sprintf("%s.payload.%s.{%s}", jr.collection, jr.indexedKeys[0], gjPK.String())), //value is link to payload
			},
			{ // create payload entry
				Key:   []byte(fmt.Sprintf("%s.payload.%s.{%s}", jr.collection, jr.indexedKeys[0], gjPK.String())),
				Value: jBytes,
			},
		},
	}

	for i := 1; i < len(jr.indexedKeys); i++ {
		gjSK := gjsonObject.Get(jr.indexedKeys[i])
		if !gjSK.Exists() {
			return 0, errors.New("missing secondary key in object")
		}

		immudbObjectRequest.KVs = append(immudbObjectRequest.KVs,
			&schema.KeyValue{ // crete secondary key index <collection>.<SKName>.<SKVALUE>.<PKVALUE>
				Key:   []byte(fmt.Sprintf("%s.%s.{%s}.{%s}", jr.collection, jr.indexedKeys[i], gjSK.String(), gjPK.String())),
				Value: []byte([]byte(fmt.Sprintf("%s.payload.%s.{%s}", jr.collection, jr.indexedKeys[0], gjPK.String()))), //value is link to payload
			},
		)
	}

	// iterate over all object fields
	// gjsonObject.ForEach(func(key, value gjson.Result) bool {
	// 	immudbObjectRequest.KVs = append(immudbObjectRequest.KVs, &schema.KeyValue{
	// 		Key:   []byte(fmt.Sprintf("%s.{%s}.%s", jr.collection, gjPK.String(), key.Str)),
	// 		Value: []byte(value.Raw),
	// 	})
	// 	return true
	// })

	txh, err := jr.client.SetAll(context.TODO(), immudbObjectRequest)
	if err != nil {
		return 0, fmt.Errorf("could not store object: %w", err)
	}

	// Create set at transaction for later retrieval of versioned object
	// for i, or := range immudbObjectRequest.KVs {
	// 	if i < len(jr.indexedKeys) {
	// 		continue
	// 	}
	// 	_, err := jr.client.ZAddAt(context.TODO(),
	// 		[]byte(fmt.Sprintf("%s.%s.{%s}.%d", jr.collection, jr.indexedKeys[0], gjPK.String(), txh.Id)),
	// 		0,
	// 		or.Key,
	// 		txh.Id,
	// 	)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }

	// create index for set versions
	// _, err = jr.client.Set(context.TODO(),
	// 	[]byte(fmt.Sprintf("%s.versions.%s.{%s}", jr.collection, jr.indexedKeys[0], gjPK.String())), []byte(fmt.Sprintf("%d", txh.Id)))

	// if err != nil {
	// 	log.Fatal(err)
	// }

	log.Printf("Object set %v\n", txh)

	return txh.Id, nil
}

// for now just based on SK
func (jr *JsonKVRepository) Read(key, condition string) ([][]byte, error) {
	log.Printf("Restore: %s\n", fmt.Sprintf("%s.%s.{%s", jr.collection, key, condition))
	seekKey := []byte("")
	var objects [][]byte
	for {
		entries, err := jr.client.Scan(context.TODO(), &schema.ScanRequest{
			Prefix:  []byte(fmt.Sprintf("%s.%s.{%s", jr.collection, key, condition)),
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
			objectEntry, err := jr.client.Get(context.Background(), e.Value)
			if err != nil {
				return nil, fmt.Errorf("could not scan for object, %w", err)
			}

			seekKey = e.Key
			objects = append(objects, objectEntry.Value)
		}
	}

	return objects, nil
}

type History struct {
	Entry    []byte
	TxID     uint64
	Revision uint64
}

func (imo *JsonKVRepository) History(primaryKeyValue string) ([]History, error) {
	log.Printf("HISTORY REQ: %s", fmt.Sprintf("%s.payload.%s.{%s}", imo.collection, imo.indexedKeys[0], primaryKeyValue))
	entries, err := imo.client.History(context.TODO(), &schema.HistoryRequest{
		Key: []byte(fmt.Sprintf("%s.payload.%s.{%s}", imo.collection, imo.indexedKeys[0], primaryKeyValue)),
	})
	if err != nil {
		log.Fatal(err)
	}

	objects := []History{}
	for _, e := range entries.Entries {
		log.Printf("HISTORY: Key: %s, Value: %s\n", string(e.Key), string(e.Value))
		objects = append(objects, History{
			Entry:    e.Value,
			Revision: e.Revision,
			TxID:     e.Tx,
		})
		// ze, err := imo.client.ZScan(context.Background(), &schema.ZScanRequest{
		// 	Set: []byte(fmt.Sprintf("%s.%s.{%s}.%s", imo.collection, imo.indexedKeys[0], primaryKeyValue, string(e.Value))),
		// })
		// if err != nil {
		// 	log.Fatal(err)
		// }

		// jObject := ""
		// for _, ze := range ze.Entries {
		// 	jObject, err = sjson.SetRaw(jObject, strings.TrimPrefix(string(ze.Entry.Key), fmt.Sprintf("%s.{%s}.", imo.collection, primaryKeyValue)), string(ze.Entry.Value))
		// 	if err != nil {
		// 		return nil, fmt.Errorf("could not restore object: %w", err)
		// 	}
		// }

		// txId, err := strconv.ParseUint(string(e.Value), 10, 64)
		// if err != nil {
		// 	log.Fatal(err)
		// }

		// objects = append(objects, History{
		// 	Entry:    []byte(jObject),
		// 	TxID:     txId,
		// 	Revision: e.Revision,
		// })
	}

	return objects, nil
}
