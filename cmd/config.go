package cmd

import (
	"fmt"

	"github.com/tomekkolo/immudb-play/pkg/lineparser"
	"github.com/tomekkolo/immudb-play/pkg/repository/immudb"
	"github.com/tomekkolo/immudb-play/pkg/service"
)

var lp service.LineParser
var jsonRepository service.JsonRepository

func configure(collection string) error {
	cfgs := immudb.NewConfigs(immuCli)
	cfg, err := cfgs.Read(collection)
	if err != nil {
		return fmt.Errorf("collection does not exist, please create one first, %w", err)
	}

	if cfg.Parser == "" {
		lp = lineparser.NewDefaultLineParser()
	} else if cfg.Parser == "pgaudit" {
		lp = lineparser.NewPGAuditLineParser()
	} else if cfg.Parser == "wrap" {
		lp = lineparser.NewWrapLineParser()
	} else {
		return fmt.Errorf("not supported parser: %s", flagParser)
	}

	if cfg.Type == "kv" {
		jsonRepository, err = immudb.NewJsonKVRepository(immuCli, collection)
		if err != nil {
			return fmt.Errorf("could not create json repository, %w", err)
		}
	} else {
		return fmt.Errorf("invalid repository type %s", cfg.Type)
	}

	return nil
}
