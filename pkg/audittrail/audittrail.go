package audittrail

import (
	"fmt"
	"hash/maphash"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

var rGen = rand.New(rand.NewSource(int64(new(maphash.Hash).Sum64())))

type metadata struct {
	Value      string `json:"value,omitempty"`
	OtherValue string `json:"other_value,omitempty"`
	IntValue   int    `'json:"int_value,omitempty"`
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
	for i := 0; i < 10; i++ {
		aes = append(aes, &auditEntry{
			Id:        uuid.New().String(),
			Timestamp: time.Now(),
			User:      user,
			Action:    rGen.Intn(len(actions)),
			SourceIP:  "127.0.0.1",
			Context:   "assdfasdfasasdfasdfas asdfa  dafsdfsadf",
			Nested:    metadata{Value: actions[rGen.Intn(len(actions))], OtherValue: actions[rGen.Intn(len(actions))], IntValue: rGen.Intn(1000)},
		})
	}

	return aes
}
