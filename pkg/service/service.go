package service

import (
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
	immudbrepository "github.com/tomekkolo/immudb-play/pkg/repository/immudb"
)

type lineProvider interface {
	ReadLine() (string, error)
}

type LineParser interface {
	Parse(line string) ([]byte, error)
}

type JsonRepository interface {
	WriteBytes(b []byte) (uint64, error)
	Read(key string, condition string) ([][]byte, error)
	History(primaryKeyValue string) ([]immudbrepository.History, error)
}

type AuditHistoryEntry struct {
	Entry    []byte
	Revision uint64
	TXID     uint64
}

type AuditService struct {
	lineProvider   lineProvider
	jsonRepository JsonRepository
	lineParser     LineParser
}

func NewAuditService(lineProvider lineProvider, lineParser LineParser, jsonRepository JsonRepository) *AuditService {
	return &AuditService{
		lineProvider:   lineProvider,
		lineParser:     lineParser,
		jsonRepository: jsonRepository,
	}
}

func (as *AuditService) Run() error {
	for {
		l, err := as.lineProvider.ReadLine()
		if err != nil {
			if err == io.EOF {
				log.Printf("Reached EOF")
				return nil
			}
			return err
		}

		b, err := as.lineParser.Parse(l)
		if err != nil {
			log.WithError(err).WithField("line", l).Debug("Invalid line format, skipping")
			continue
		}

		id, err := as.jsonRepository.WriteBytes(b)
		if err != nil {
			return fmt.Errorf("could not store audit entry, %w", err)
		}

		log.WithField("_id", id).WithField("line", l).Trace("Stored line")
	}
}
