package immudb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
	"github.com/tidwall/gjson"
)

type JsonKVRepository struct {
	client      immudb.ImmuClient
	collection  string
	indexedKeys []string // first key is considered primary key
}

func NewJsonKVRepository(cli immudb.ImmuClient, collection string) (*JsonKVRepository, error) {
	if collection == "" {
		return nil, errors.New("collection cannot be empty")
	}

	// read collection definition
	indexedKeys := []string{}
	entry, err := cli.Get(context.TODO(), []byte(fmt.Sprintf("%s.collection", collection)))
	if err != nil {
		log.WithField("collection", collection).WithError(err).Warn("collection does not exist, create before use", collection, err)
	} else {
		err = json.Unmarshal(entry.Value, &indexedKeys)
		if err != nil {
			return nil, fmt.Errorf("could not unmarsahl indexes definition, %w", err)
		}

		log.WithField("indexes", indexedKeys).Info("Indexes from immudb")
	}

	return &JsonKVRepository{
		client:      cli,
		collection:  collection,
		indexedKeys: indexedKeys,
	}, nil
}

func (jr *JsonKVRepository) Create(indexedKeys []string) error {
	b, err := json.Marshal(indexedKeys)
	if err != nil {
		return fmt.Errorf("could not marshal indexes definition, %w", err)
	}

	_, err = jr.client.Set(context.TODO(), []byte(fmt.Sprintf("%s.collection", jr.collection)), b)
	if err != nil {
		return fmt.Errorf("could not store indexes definition, %w", err)
	}

	jr.indexedKeys = indexedKeys

	return nil
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

	// parse with gjson
	gjsonObject := gjson.ParseBytes(jBytes)

	// resolve primary key, format "key1+key2+..."
	var pk string
	for _, pkPart := range strings.Split(jr.indexedKeys[0], "+") {
		gjPK := gjsonObject.Get(pkPart)
		if !gjPK.Exists() {
			return 0, fmt.Errorf("missing primary key in object, %s", pkPart)
		}
		pk += gjPK.String()
	}

	immudbObjectRequest := &schema.SetRequest{
		KVs: []*schema.KeyValue{
			{ // crete primary key index
				Key:   []byte(fmt.Sprintf("%s.%s.{%s}", jr.collection, jr.indexedKeys[0], pk)),
				Value: []byte(fmt.Sprintf("%s.payload.%s.{%s}", jr.collection, jr.indexedKeys[0], pk)), //value is link to payload
			},
			{ // create payload entry
				Key:   []byte(fmt.Sprintf("%s.payload.%s.{%s}", jr.collection, jr.indexedKeys[0], pk)),
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
				Key:   []byte(fmt.Sprintf("%s.%s.{%s}.{%s}", jr.collection, jr.indexedKeys[i], gjSK.String(), pk)),
				Value: []byte([]byte(fmt.Sprintf("%s.payload.%s.{%s}", jr.collection, jr.indexedKeys[0], pk))), //value is link to payload
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

	log.WithField("txID", txh.Id).Trace("Wrote entry")

	return txh.Id, nil
}

// for now just based on SK
func (jr *JsonKVRepository) Read(key, condition string) ([][]byte, error) {
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
			log.WithField("key", key).WithField("condition", condition).Debug("No more entries matching condition")
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
	offset := uint64(0)
	objects := []History{}
	for {
		entries, err := imo.client.History(context.TODO(), &schema.HistoryRequest{
			Key:    []byte(fmt.Sprintf("%s.payload.%s.{%s}", imo.collection, imo.indexedKeys[0], primaryKeyValue)),
			Offset: offset,
			Limit:  999,
		})

		if err != nil {
			return nil, err
		}

		for _, e := range entries.Entries {
			objects = append(objects, History{
				Entry:    e.Value,
				Revision: e.Revision,
				TxID:     e.Tx,
			})
			offset++
		}

		if len(entries.Entries) < 999 {
			log.WithField("key", primaryKeyValue).Debug("No more history entries")
			break
		}
	}

	return objects, nil
}
