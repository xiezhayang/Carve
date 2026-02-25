package handler

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xiezhayang/Carve/datamanager"
	"github.com/xiezhayang/Carve/otlp"
	"github.com/xiezhayang/Carve/server/config"
)

type Handlers struct {
	Cfg    *config.Config
	State  *datamanager.State
	Writer func(path string, rows []datamanager.Row) (int, error)
}

func (h *Handlers) Healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
		"service":   "carve",
	})
}

func (h *Handlers) CollectStart(c *gin.Context) {
	name := strings.TrimSpace(c.Query("name"))
	filename := strings.TrimSpace(c.Query("filename"))
	if name == "" || filename == "" || !datamanager.SafeFilename(name) || !datamanager.SafeFilename(filename) {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "缺少或无效的 name/filename（仅允许字母数字、下划线、横线、点）"})
		return
	}
	metrics := c.QueryArray("metric")
	var list []string
	for _, m := range metrics {
		if m != "" {
			list = append(list, m)
		}
	}
	if err := h.State.StartCollect(name, filename, list); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}
	_ = os.MkdirAll(h.Cfg.CSVDir(), 0755)
	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "started", "name": name, "filename": filename})
}

func (h *Handlers) CollectStop(c *gin.Context) {
	name := c.Query("name")
	h.State.StopCollect(name)
	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "stopped"})
}

func (h *Handlers) CollectStatus(c *gin.Context) {
	targets := h.State.AllTargets()
	list := make([]gin.H, 0, len(targets))
	for _, t := range targets {
		list = append(list, gin.H{
			"name":       t.Name,
			"filename":   filepath.Base(t.Path),
			"filters":    t.Filters,
			"collecting": t.Collecting,
		})
	}
	any, summary := h.State.Collecting()
	c.JSON(http.StatusOK, gin.H{"collecting": any, "summary": summary, "targets": list})
}

// GetAvailableMetrics 返回从历史 OTLP 中发现过的指标名，供用户选择要收集哪些
func (h *Handlers) GetAvailableMetrics(c *gin.Context) {
	list := h.State.KnownMetrics()
	c.JSON(http.StatusOK, gin.H{"metrics": list})
}

func (h *Handlers) CollectFiltersGet(c *gin.Context) {
	targets := h.State.AllTargets()
	list := make([]gin.H, 0, len(targets))
	for _, t := range targets {
		list = append(list, gin.H{
			"name":        t.Name,
			"metrics":     t.Filters,
			"collect_all": len(t.Filters) == 0,
			"collecting":  t.Collecting,
		})
	}
	available := h.State.KnownMetrics()
	c.JSON(http.StatusOK, gin.H{
		"targets":   list,
		"available": available,
	})
}

func (h *Handlers) ExportList(c *gin.Context) {
	dir := h.Cfg.CSVDir()
	entries, err := os.ReadDir(dir)
	files := []string{}
	if err == nil {
		base, _ := filepath.Abs(dir)
		for _, e := range entries {
			if e.IsDir() || e.Name()[0] == '.' {
				continue
			}
			full := filepath.Join(dir, e.Name())
			abs, _ := filepath.Abs(full)
			if len(abs) >= len(base) && abs[:len(base)] == base && datamanager.SafeFilename(e.Name()) {
				files = append(files, e.Name())
			}
		}
	}
	// sort strings
	for i := 0; i < len(files); i++ {
		for j := i + 1; j < len(files); j++ {
			if files[j] < files[i] {
				files[i], files[j] = files[j], files[i]
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "files": files})
}

func (h *Handlers) Export(c *gin.Context) {
	filename := c.Query("filename")
	if filename == "" || !datamanager.SafeFilename(filename) {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "缺少或无效的 filename"})
		return
	}
	dir := h.Cfg.CSVDir()
	full := filepath.Join(dir, filename)
	abs, _ := filepath.Abs(full)
	base, _ := filepath.Abs(dir)
	if len(abs) < len(base) || abs[:len(base)] != base {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "无效路径"})
		return
	}
	info, err := os.Stat(full)
	if err != nil || info.IsDir() {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "文件不存在"})
		return
	}
	c.Header("Content-Type", "text/csv; charset=utf-8")
	c.Header("Content-Disposition", `attachment; filename="`+filename+`"`)
	f, _ := os.Open(full)
	defer f.Close()
	io.Copy(c.Writer, f)
}

func (h *Handlers) V1Metrics(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}
	originalSize := len(body)
	if c.GetHeader("Content-Encoding") == "gzip" {
		r, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "gzip: " + err.Error()})
			return
		}
		body, err = io.ReadAll(r)
		r.Close()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
			return
		}
	}
	processedSize := len(body)
	parsed, err := otlp.Parse(body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "otlp: " + err.Error()})
		return
	}
	if names := parsed.MetricNames(); len(names) > 0 {
		h.State.AddKnownMetrics(names)
	}
	targets := h.State.ActiveTargets()
	if len(targets) == 0 {
		c.JSON(http.StatusOK, gin.H{
			"status":         "ok",
			"message":        "数据已接收，当前无活跃收集",
			"timestamp":      time.Now().Format(time.RFC3339),
			"original_size":  originalSize,
			"processed_size": processedSize,
			"compressed":     c.GetHeader("Content-Encoding") == "gzip",
		})
		return
	}
	var wg sync.WaitGroup
	for _, t := range targets {
		rows := parsed.RowsForFilter(t.Filters)
		if h.Writer == nil || len(rows) == 0 {
			continue
		}
		conv := make([]datamanager.Row, len(rows))
		for i := range rows {
			conv[i] = datamanager.Row{TsMs: rows[i].TsMs, Metric: rows[i].Metric, Value: rows[i].Value, Service: rows[i].Service}
		}
		wg.Add(1)
		go func(path string, r []datamanager.Row) {
			defer wg.Done()
			_, _ = h.Writer(path, r)
		}(t.Path, conv)
	}
	wg.Wait()
	c.JSON(http.StatusOK, gin.H{
		"status":          "ok",
		"message":         "数据已接收",
		"timestamp":       time.Now().Format(time.RFC3339),
		"original_size":   originalSize,
		"processed_size":  processedSize,
		"compressed":      c.GetHeader("Content-Encoding") == "gzip",
		"targets_written": len(targets),
	})
}
