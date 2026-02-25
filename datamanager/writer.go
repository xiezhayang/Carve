package datamanager

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Row struct {
	TsMs    int64
	Metric  string
	Value   float64
	Service string
}

var (
	pathMu    sync.Mutex
	pathLocks = make(map[string]*sync.Mutex)
)

func lockForPath(fullPath string) *sync.Mutex {
	pathMu.Lock()
	defer pathMu.Unlock()
	if pathLocks[fullPath] == nil {
		pathLocks[fullPath] = &sync.Mutex{}
	}
	return pathLocks[fullPath]
}

func AppendRows(fullPath string, rows []Row) (int, error) {
	if fullPath == "" || len(rows) == 0 {
		return 0, nil
	}
	mu := lockForPath(fullPath)
	mu.Lock()
	defer mu.Unlock()
	exist := false
	if _, err := os.Stat(fullPath); err == nil {
		exist = true
	}
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, err
	}
	f, err := os.OpenFile(fullPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if !exist {
		_ = w.Write([]string{"ts", "metric", "value", "service"})
	}
	for _, r := range rows {
		_ = w.Write([]string{
			fmt.Sprintf("%d", r.TsMs),
			r.Metric,
			fmt.Sprintf("%v", r.Value),
			r.Service,
		})
	}
	w.Flush()
	return len(rows), w.Error()
}
