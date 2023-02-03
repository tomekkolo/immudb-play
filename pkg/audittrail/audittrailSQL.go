package audittrail

import (
	"context"
	"fmt"
	"hash/maphash"
	"log"
	"math/rand"
	"time"

	immudb "github.com/codenotary/immudb/pkg/client"
	"github.com/google/uuid"
)

var rGen = rand.New(rand.NewSource(int64(new(maphash.Hash).Sum64())))

type metadata struct {
	Value string `json:"value,omitempty"`
}

type auditEntry struct {
	Id        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	User      string    `json:"user"`
	Action    int       `json:"action"`
	SourceIP  string    `json:"sourceip,omitempty"`
	Context   string    `json:"context,omitempty"`
	Nested    metadata  `json:"nested,omitempty"`
}

func generateAuditTrail() []*auditEntry {
	user := fmt.Sprintf("user%d", rGen.Intn(100))
	actions := []string{"session_start", "update", "delete", "insert", "session_stop"}
	aes := []*auditEntry{}
	for i := 0; i < 100; i++ {
		aes = append(aes, &auditEntry{
			Id:        uuid.New().String(),
			Timestamp: time.Now(),
			User:      user,
			Action:    rGen.Intn(len(actions)),
			SourceIP:  "127.0.0.1",
			Context:   "assdfasdfasasdfasdfas asdfa  dafsdfsadf",
			Nested:    metadata{Value: actions[rGen.Intn(len(actions))]},
		})
	}

	return aes
}

func AuditTrailSQL(queryOnly bool) {
	log.Printf("Starting SQL Audit trail")
	opts := immudb.DefaultOptions().WithAddress("localhost").WithPort(3322)

	client := immudb.NewClient().WithOptions(opts)
	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	if err != nil {
		log.Fatal(err)
	}

	defer client.CloseSession(context.TODO())

	// create table representing audit log
	tx, err := client.NewTx(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	err = tx.SQLExec(context.TODO(),
		`CREATE TABLE IF NOT EXISTS audit_trail(id VARCHAR[36], ts TIMESTAMP, user VARCHAR[100], 
        action INTEGER, 
        sourceip VARCHAR[15], 
        context VARCHAR[256], 
        PRIMARY KEY (id)
        );`, nil)

	if err != nil {
		log.Fatal(err)
	}

	err = tx.SQLExec(context.TODO(),
		`CREATE INDEX IF NOT EXISTS ON audit_trail(user);`, nil)

	if err != nil {
		log.Fatal(err)
	}

	txh, err := tx.Commit(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Create table txh: %+v\n", txh)

	if !queryOnly {
		// create entries
		for i := 0; i < 1000; i++ {
			log.Printf("generating audit trail %d\n", i)
			aes := generateAuditTrail()
			tx, err := client.NewTx(context.TODO())
			if err != nil {
				log.Fatal(err)
			}

			for _, ae := range aes {
				err = tx.SQLExec(context.TODO(), `INSERT INTO audit_trail (id, ts, user, action, sourceip, context) 
        VALUES (@id, @ts, @user, @action, @sourceip, @context);`,
					map[string]interface{}{
						"id":       ae.Id,
						"user":     ae.User,
						"ts":       ae.Timestamp,
						"action":   ae.Action,
						"sourceip": ae.SourceIP,
						"context":  ae.Context})

				if err != nil {
					log.Fatal(err)
				}
			}

			txh, err := tx.Commit(context.TODO())
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Create audit trail txh: %+v\n", txh)
		}
	}

	// do a query
	log.Println("Doing queries")

	offset := 0

	for {
		qr, err := client.SQLQuery(context.Background(), fmt.Sprintf("SELECT id, user, ts, action, sourceip, context FROM audit_trail WHERE user='user65' ORDER BY ID LIMIT 999 OFFSET %d;", offset),
			nil, false)
		if err != nil {
			log.Fatal(err)
		}
		if len(qr.Rows) == 0 {
			break
		} else {
			offset += len(qr.Rows)
			log.Printf("Offset is: %d\n", offset)
		}

		for _, r := range qr.Rows {
			log.Printf("Offset is: %d\n", offset)
			fmt.Printf("%s, %s, %s, %s, %s, %s\n", r.Values[0].String(), r.Values[1].String(), r.Values[2].String(), r.Values[3].String(), r.Values[4].String(), r.Values[5].String())
		}
	}
}
