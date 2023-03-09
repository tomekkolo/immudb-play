package audittrail

import (
	"encoding/json"
	"log"
	"time"
)

type jsonRepository interface {
	Store(object interface{}) (uint64, error)
	Restore(key string, condition string) ([]string, error)
}

type AuditTrailJson struct {
	jsonRepository jsonRepository
	queryOnly      bool
	numOfTrails    int
	restoreKey     string
	restorePrefix  string
}

func NewAuditTrailJson(jsonRepository jsonRepository, queryOnly bool, numOfTrails int, restoreKey string, restorePrefix string) *AuditTrailJson {
	return &AuditTrailJson{
		jsonRepository: jsonRepository,
		queryOnly:      queryOnly,
		numOfTrails:    numOfTrails,
		restoreKey:     restoreKey,
		restorePrefix:  restorePrefix,
	}
}

func (atj *AuditTrailJson) Run() {
	log.Printf("Running AuditTrailJson")

	if !atj.queryOnly {
		start := time.Now()
		for i := 0; i < atj.numOfTrails; i++ {
			aes := generateAuditTrail()
			for j, ae := range aes {
				txID, err := atj.jsonRepository.Store(ae)
				if err != nil {
					log.Fatal(err)
				}

				log.Printf("Stored audit entry %d.%d with txID %d\n", i, j, txID)
			}
		}

		end := time.Now()
		log.Printf("Storing audit trail took: %s\n", end.Sub(start).String())
	}

	log.Printf("Restoring audit trail with index: %s and condition: %s\n", atj.restoreKey, atj.restorePrefix)
	objects, err := atj.jsonRepository.Restore(atj.restoreKey, atj.restorePrefix)
	if err != nil {
		log.Fatal(err)
	}

	for _, object := range objects {
		var ae auditEntry
		// this will not work as we do not keep types
		err = json.Unmarshal([]byte(object), &ae)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Restored audit entry: %+v\n", ae)
	}
}
