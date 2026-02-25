package handler

import (
	"bytes"
	"compress/gzip"
	"io"
	"log"
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
	filter := parseFilterFromQuery(c)
	if err := h.State.StartCollect(name, filename, filter); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		log.Printf("[carve] CollectStart error=%v", err)
		return
	}
	if err := os.MkdirAll(h.Cfg.CSVDir(), 0755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "started", "name": name, "filename": filename})
}

func parseFilterFromQuery(c *gin.Context) datamanager.Filter {
	f := datamanager.Filter{
		Resource: make(map[string]string),
		Attr:     make(map[string]string),
	}
	for k, v := range c.Request.URL.Query() {
		if len(v) == 0 || v[0] == "" {
			continue
		}
		switch {
		case k == "metric":
			f.Metrics = append(f.Metrics, v...)
		case k == "scope":
			f.ScopeName = v[0]
		case strings.HasPrefix(k, "resource."):
			f.Resource[strings.TrimPrefix(k, "resource.")] = v[0]
		case strings.HasPrefix(k, "attr."):
			f.Attr[strings.TrimPrefix(k, "attr.")] = v[0]
		}
	}
	// 去空
	var metrics []string
	for _, m := range f.Metrics {
		if strings.TrimSpace(m) != "" {
			metrics = append(metrics, m)
		}
	}
	f.Metrics = metrics
	return f
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
			"filter":     t.Filter,
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
			"name":       t.Name,
			"filename":   filepath.Base(t.Path),
			"filter":     t.Filter,
			"collecting": t.Collecting,
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
			if e.IsDir() || (e.Name() != "" && e.Name()[0] == '.') {
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
	parsed.DebugLogRawPayload(250, "load")
	if names := parsed.MetricNames(); len(names) > 0 {
		h.State.AddKnownMetrics(names)
	}
	targets := h.State.ActiveTargets()
	log.Printf("[carve] V1Metrics targets=%d Writer==nil:%v", len(targets), h.Writer == nil)
	if len(targets) == 0 {
		c.JSON(http.StatusOK, gin.H{
			"status":         "ok",
			"message":        "数据已接收，当前无活跃收集",
			"timestamp":      time.Now().Format(time.RFC3339),
			"original_size":  originalSize,
			"processed_size": processedSize,
			"compressed":     c.GetHeader("Content-Encoding") == "gzip",
		})
		log.Printf("[carve] V1Metrics no active targets")
		return
	}
	var wg sync.WaitGroup
	for _, t := range targets {
		stats := &otlp.FilterFailureStats{}
		rows := parsed.RowsForFilterWithStats(t.Filter, stats)
		if h.Writer == nil || len(rows) == 0 {
			if h.Writer == nil {
				log.Printf("[carve] V1Metrics writer is nil")
			}
			if len(rows) == 0 {
				if stats.ParsedRows == 0 {
					log.Printf("[carve] V1Metrics rows empty target=%s path=%s (no rows in request, parsed_rows=0)", t.Name, t.Path)
				} else {
					log.Printf("[carve] V1Metrics rows empty target=%s path=%s parsed=%d failed_metric=%d failed_resource=%d failed_scope=%d failed_attr=%d",
						t.Name, t.Path, stats.ParsedRows, stats.FailedMetric, stats.FailedResource, stats.FailedScope, stats.FailedAttr)
				}
			}
			continue
		}
		log.Printf("[carve] DEBUG_RESOURCE target=%s path=%s first_row_resource=%v", t.Name, t.Path, rows[0].Resource)
		wg.Add(1)
		go func(path string, r []datamanager.Row) {
			defer wg.Done()
			_, err := h.Writer(path, r)
			log.Printf("[carve] V1Metrics writer path=%s rows=%d err=%v", path, len(r), err)
			if err != nil {
				log.Printf("[carve] V1Metrics writer error=%v", err)
			}
		}(t.Path, rows)
	}
	log.Printf("[carve] V1Metrics targets=%d,waiting", len(targets))
	wg.Wait()
	log.Printf("[carve] V1Metrics targets=%d,waiting done", len(targets))
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

// CollectTargetDelete 删除指定 name 的 target（query: name=）。name 为空则删除全部。
func (h *Handlers) CollectTargetDelete(c *gin.Context) {
	name := strings.TrimSpace(c.Query("name"))
	existed := h.State.DeleteTarget(name)
	if name != "" && !existed {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "target not found: " + name})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "deleted"})
}
