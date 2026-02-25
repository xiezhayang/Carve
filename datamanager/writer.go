package datamanager

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type Row struct {
	TsMs      int64
	Metric    string
	Value     float64
	Resource  map[string]string
	ScopeName string
	Attr      map[string]string
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

// InferHeader 从一批 Row 推断表头：ts, metric, value, scope + 排序的 Resource keys + attr. 前缀的 Attr keys
func InferHeader(rows []Row) []string {
	resourceKeys := make(map[string]struct{})
	attrKeys := make(map[string]struct{})
	for _, r := range rows {
		for k := range r.Resource {
			resourceKeys[k] = struct{}{}
		}
		for k := range r.Attr {
			attrKeys[k] = struct{}{}
		}
	}
	base := []string{"ts", "metric", "value", "scope"}
	var resKeys, aKeys []string
	for k := range resourceKeys {
		resKeys = append(resKeys, k)
	}
	for k := range attrKeys {
		aKeys = append(aKeys, k)
	}
	sort.Strings(resKeys)
	sort.Strings(aKeys)
	for _, k := range resKeys {
		base = append(base, k)
	}
	for _, k := range aKeys {
		base = append(base, "attr."+k)
	}
	return base
}

// RowToRecord 按表头顺序把 Row 转成 CSV 一行
func RowToRecord(row Row, header []string) []string {
	out := make([]string, len(header))
	for i, col := range header {
		switch col {
		case "ts":
			out[i] = fmt.Sprintf("%d", row.TsMs)
		case "metric":
			out[i] = row.Metric
		case "value":
			out[i] = fmt.Sprintf("%v", row.Value)
		case "scope":
			out[i] = row.ScopeName
		default:
			if v, ok := row.Resource[col]; ok {
				out[i] = v
			} else if len(col) > 5 && col[:5] == "attr." {
				out[i] = row.Attr[col[5:]]
			} else {
				out[i] = ""
			}
		}
	}
	return out
}

// AppendRows 写入 rows；文件不存在时用 InferHeader 写表头，已存在时读首行作表头
func AppendRows(fullPath string, rows []Row) (int, error) {
	log.Printf("[carve] writer AppendRows fullPath=%s rows=%d", fullPath, len(rows))
	if fullPath == "" || len(rows) == 0 {
		log.Printf("[carve] writer AppendRows empty path or rows")
		return 0, nil
	}
	log.Printf("[carve] writer AppendRows path=%s rows=%d", fullPath, len(rows))
	mu := lockForPath(fullPath)
	mu.Lock()
	defer mu.Unlock()
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("[carve] writer AppendRows mkdir error=%v", err)
		return 0, err
	}

	var header []string
	exist := false
	if _, err := os.Stat(fullPath); err == nil {
		exist = true
		f, err := os.Open(fullPath)
		if err != nil {
			log.Printf("[carve] writer AppendRows open error=%v", err)
			return 0, err
		}
		r := csv.NewReader(f)
		rec, err := r.Read()
		f.Close()
		if err != nil || len(rec) == 0 {
			header = InferHeader(rows)
		} else {
			header = rec
		}
	} else {
		header = InferHeader(rows)
	}

	f, err := os.OpenFile(fullPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[carve] writer AppendRows openfile error=%v", err)
		return 0, err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if !exist {
		err = w.Write(header)
		if err != nil {
			log.Printf("[carve] writer AppendRows write header error=%v", err)
			return 0, err
		}
	}
	for _, r := range rows {
		err = w.Write(RowToRecord(r, header))
		if err != nil {
			log.Printf("[carve] writer AppendRows write row error=%v", err)
			return 0, err
		}
	}
	w.Flush()
	return len(rows), w.Error()
}
