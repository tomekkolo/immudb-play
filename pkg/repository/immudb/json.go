package immudb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type JsonRepository struct {
	client      immudb.ImmuClient
	collection  string
	indexedKeys []string // first key is considered primary key
}

func NewJsonRepository(client immudb.ImmuClient, collection string, indexedKeys []string) *JsonRepository {
	if len(indexedKeys) == 0 {
		log.Fatal("JsonRepository requires at least primary key")
	}

	return &JsonRepository{
		client:      client,
		collection:  collection,
		indexedKeys: indexedKeys,
	}
}

func (imo *JsonRepository) Store(object interface{}) (uint64, error) {
	objectBytes, err := json.Marshal(object)
	if err != nil {
		return 0, fmt.Errorf("could not marshal object: %w", err)
	}

	return imo.StoreBytes(objectBytes)
}

func (imo *JsonRepository) StoreBytes(objectBytes []byte) (uint64, error) {
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
			Value: []byte(value.Raw),
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
func (imo *JsonRepository) Restore(key, condition string) ([]string, error) {
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
				jObject, err = sjson.SetRaw(jObject, strings.TrimPrefix(string(oe.Key), fmt.Sprintf("%s.{%s}.", imo.collection, string(e.Value))), string(oe.Value))
				if err != nil {
					return nil, fmt.Errorf("could not restore object: %w", err)
				}
			}

			seekKey = e.Key
			objects = append(objects, jObject)
		}
	}

	return objects, nil
}

type History struct {
	Object   string
	TxID     uint64
	Revision uint64
}

func (imo *JsonRepository) History(primaryKeyValue string) ([]History, error) {
	entries, err := imo.client.History(context.TODO(), &schema.HistoryRequest{
		Key: []byte(fmt.Sprintf("%s.versions.%s.{%s}", imo.collection, imo.indexedKeys[0], primaryKeyValue)),
	})
	if err != nil {
		log.Fatal(err)
	}

	objects := []History{}
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
			jObject, err = sjson.SetRaw(jObject, strings.TrimPrefix(string(ze.Entry.Key), fmt.Sprintf("%s.{%s}.", imo.collection, primaryKeyValue)), string(ze.Entry.Value))
			if err != nil {
				return nil, fmt.Errorf("could not restore object: %w", err)
			}
		}

		txId, err := strconv.ParseUint(string(e.Value), 10, 64)
		if err != nil {
			log.Fatal(err)
		}

		objects = append(objects, History{
			Object:   jObject,
			TxID:     txId,
			Revision: e.Revision,
		})
	}

	return objects, nil
}
