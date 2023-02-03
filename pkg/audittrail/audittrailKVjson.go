package audittrail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	log.Printf("Object set %v\n", txh)

	return txh.Id, nil
}

// for now just based on SK
func (imo *ImmudbObjects) Restore(key, condition string) ([]string, error) {
	log.Printf("Restore: %s\n", fmt.Sprintf("%s.%s.{%s", imo.collection, key, condition))
	entries, err := imo.client.Scan(context.TODO(), &schema.ScanRequest{
		Prefix: []byte(fmt.Sprintf("%s.%s.{%s", imo.collection, key, condition)),
		Limit:  200,
	})
	if err != nil {
		return nil, fmt.Errorf("could not scan for objects, %w", err)
	}

	var objects []string
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

		// var object map[string]interface{}
		// err = json.Unmarshal([]byte(jObject), &object)
		// if err != nil {
		// 	return nil, fmt.Errorf("could not unmarshal to an object: %w", err)
		// }

		objects = append(objects, jObject)
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

	start := time.Now()
	// create entries
	for i := 0; i < 1; i++ {
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

	objects, err := immuObjects.Restore("id", "")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("OBJECTS: %+v", objects)
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
