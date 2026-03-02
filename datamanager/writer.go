package datamanager

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// WriteTargetMeta 在创建新 CSV 时写第一行：# carve {"name":"...","filter":{...}}
// 仅当文件不存在时写入。
func WriteTargetMeta(path string, name string, filter Filter) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	line := struct {
		Name   string `json:"name"`
		Filter Filter `json:"filter"`
	}{Name: name, Filter: filter}
	data, err := json.Marshal(line)
	if err != nil {
		return err
	}
	_, err = f.WriteString("# carve ")
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	return err
}

// ReadTargetMeta 读 CSV 第一行，若为 # carve {...} 则解析并返回 name、filter，否则 ok==false。
func ReadTargetMeta(path string) (name string, filter Filter, ok bool) {
	f, err := os.Open(path)
	if err != nil {
		return "", Filter{}, false
	}
	defer f.Close()
	br := bufio.NewReader(f)
	line, _, err := br.ReadLine()
	if err != nil || len(line) == 0 {
		return "", Filter{}, false
	}
	s := strings.TrimSpace(string(line))
	const prefix = "# carve "
	if !strings.HasPrefix(s, prefix) {
		return "", Filter{}, false
	}
	var meta struct {
		Name   string  `json:"name"`
		Filter *Filter `json:"filter"`
	}
	if json.Unmarshal([]byte(s[len(prefix):]), &meta) != nil || meta.Filter == nil {
		return "", Filter{}, false
	}
	if meta.Filter.Resource == nil {
		meta.Filter.Resource = make(map[string]string)
	}
	if meta.Filter.Attr == nil {
		meta.Filter.Attr = make(map[string]string)
	}
	return meta.Name, *meta.Filter, true
}

// AppendRows 写入 rows；文件不存在时用 InferHeader 写表头，已存在时读首行作表头
func AppendRows(fullPath string, rows []Row) (int, error) {
	if fullPath == "" || len(rows) == 0 {
		return 0, errors.New("empty path or rows")
	}
	mu := lockForPath(fullPath)
	mu.Lock()
	defer mu.Unlock()
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, err
	}

	var header []string
	needWriteHeader := false
	if _, err := os.Stat(fullPath); err == nil {
		f, err := os.Open(fullPath)
		if err != nil {
			return 0, err
		}
		br := bufio.NewReader(f)
		line1, _, err := br.ReadLine()
		f.Close()
		if err != nil {
			header = InferHeader(rows)
			needWriteHeader = true
		} else {
			first := strings.TrimSpace(string(line1))
			if strings.HasPrefix(first, "# carve ") {
				// 第一行是 meta，表头在第二行，需再读
				f2, _ := os.Open(fullPath)
				br2 := bufio.NewReader(f2)
				_, _, _ = br2.ReadLine()
				line2, _, _ := br2.ReadLine()
				f2.Close()
				if len(line2) > 0 {
					rec, _ := csv.NewReader(strings.NewReader(string(line2))).Read()
					header = rec
				} else {
					header = InferHeader(rows)
					needWriteHeader = true
				}
			} else {
				rec, _ := csv.NewReader(strings.NewReader(string(line1))).Read()
				header = rec
			}
			if len(header) == 0 {
				header = InferHeader(rows)
				needWriteHeader = true
			}
		}
	} else {
		header = InferHeader(rows)
		needWriteHeader = true
	}

	f, err := os.OpenFile(fullPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if needWriteHeader {
		err = w.Write(header)
		if err != nil {
			return 0, err
		}
	}
	for _, r := range rows {
		err = w.Write(RowToRecord(r, header))
		if err != nil {
			return 0, err
		}
	}
	w.Flush()
	return len(rows), w.Error()
}
