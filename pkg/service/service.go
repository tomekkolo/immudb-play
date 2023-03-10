package service

import (
	"fmt"
	"log"

	immudbrepository "github.com/tomekkolo/immudb-play/pkg/repository/immudb"
)

type lineProvider interface {
	ReadLine() (string, error)
}

type LineParser interface {
	Parse(line string) ([]byte, error)
}

type jsonRepository interface {
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
	jsonRepository jsonRepository
	lineParser     LineParser
}

func NewAuditService(lineProvider lineProvider, lineParser LineParser, jsonRepository jsonRepository) *AuditService {
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
			return err
		}

		b, err := as.lineParser.Parse(l)
		if err != nil {
			log.Printf("Invalid line format, skipping, %v", err)
			continue
		}

		id, err := as.jsonRepository.WriteBytes(b)
		if err != nil {
			return fmt.Errorf("could not store pg audit entry, %w", err)
		}

		log.Printf("Stored pg entry with id %d", id)
	}
}
