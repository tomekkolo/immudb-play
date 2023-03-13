package immudb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"

	immudb "github.com/codenotary/immudb/pkg/client"
)

type JsonSQLRepository struct {
	client     immudb.ImmuClient
	collection string
	indexes    []string // first key is considered primary key
}

func NewJsonSQLRepository(cli immudb.ImmuClient, collection string, primaryKey string, columns []string) (*JsonSQLRepository, error) {
	if collection == "" {
		return nil, errors.New("collection cannot be empty")
	}

	// create table representing audit log
	tx, err := cli.NewTx(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	sb := strings.Builder{}
	sb.WriteString("CREATE TABLE IF NOT EXISTS ")
	sb.WriteString(collection)
	sb.WriteString(" ( ")
	indexes := []string{}
	for _, column := range columns {
		splitted := strings.Split(column, "=")
		if len(splitted) != 2 {
			return nil, fmt.Errorf("invalid index definition, %s", column)
		}

		sb.WriteString(splitted[0])
		sb.WriteString(" ")
		sb.WriteString(splitted[1])
		sb.WriteString(",")
		indexes = append(indexes, splitted[0])
	}
	sb.WriteString("PRIMARY KEY (")
	sb.WriteString(primaryKey)
	sb.WriteString("));")

	log.WithField("sql", sb.String()).Info("Creating collection table")
	err = tx.SQLExec(context.TODO(), sb.String(), nil)

	if err != nil {
		log.Fatal(err)
	}

	sb = strings.Builder{}
	sb.WriteString("CREATE INDEX IF NOT EXISTS ON ")
	sb.WriteString(collection)
	sb.WriteString("(")
	sb.WriteString(strings.Join(indexes, ","))
	sb.WriteString(");")

	log.WithField("sql", sb.String()).Info("Creating indexes")
	err = tx.SQLExec(context.TODO(), sb.String(), nil)
	if err != nil {
		log.Fatal(err)
	}

	_, err = tx.Commit(context.TODO())
	if err != nil {
		return nil, err
	}

	return &JsonSQLRepository{
		client:  cli,
		indexes: append([]string{primaryKey}, indexes...),
	}, nil
}
