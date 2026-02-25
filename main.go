package main

import (
	"log"
	"os"

	"github.com/xiezhayang/Carve/datamanager"
	"github.com/xiezhayang/Carve/server"
	"github.com/xiezhayang/Carve/server/config"
)

func main() {
	cfg := config.Load()
	_ = os.MkdirAll(cfg.CSVDir(), 0755)
	state := datamanager.NewState(cfg.CSVDir())
	writer := func(path string, rows []datamanager.Row) (int, error) {
		return datamanager.AppendRows(path, rows)
	}
	if writer == nil {
		log.Printf("[carve] main writer is nil")
	}
	srv := server.New(cfg, state, writer)
	_ = srv.Run()
}
