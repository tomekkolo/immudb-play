package service

import (
	"fmt"
	"log"

	immudbrepository "github.com/tomekkolo/immudb-play/pkg/repository/immudb"
)

type lineProvider interface {
	ReadLine() (string, error)
}

type lineParser interface {
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
	lineParser     lineParser
}

func NewAuditService(lineProvider lineProvider, lineParser lineParser, jsonRepository jsonRepository) *AuditService {
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

func (pga *AuditService) Read(key string, condition string) ([][]byte, error) {
	jsons, err := pga.jsonRepository.Read(key, condition)
	if err != nil {
		return nil, fmt.Errorf("could not read pg audit, %w", err)
	}

	return jsons, nil
}

func (pga *AuditService) History(primaryKey string) ([]AuditHistoryEntry, error) {
	historyEntries, err := pga.jsonRepository.History(primaryKey)
	if err != nil {
		return nil, fmt.Errorf("could not read history of %s, %w", primaryKey, err)
	}

	var auditHistoryEntries []AuditHistoryEntry
	for _, he := range historyEntries {
		var auditHistoryEntry AuditHistoryEntry
		auditHistoryEntry.Entry = he.Entry
		auditHistoryEntry.Revision = he.Revision
		auditHistoryEntry.TXID = he.TxID

		auditHistoryEntries = append(auditHistoryEntries, auditHistoryEntry)
	}

	return auditHistoryEntries, nil
}
