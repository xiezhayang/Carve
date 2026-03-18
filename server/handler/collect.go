package handler

import (
	"net/http"
	"strings"

	"log"
	"os"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"github.com/xiezhayang/Carve/datamanager"
)

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
	if err := h.State.SaveTargets(); err != nil {
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
	err := h.State.SaveTargets()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}
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

func (h *Handlers) CollectTargetDelete(c *gin.Context) {
	name := strings.TrimSpace(c.Query("name"))
	existed := h.State.DeleteTarget(name)
	err := h.State.SaveTargets()
	if err != nil {
		log.Printf("[carve] CollectTargetDelete SaveTargets error=%v", err)
	}
	if name != "" && !existed {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "target not found: " + name})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "deleted"})
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

// GetAvailableResourceKeys 返回已知的 resource 键名，供用户选择过滤
func (h *Handlers) GetAvailableResourceKeys(c *gin.Context) {
	list := h.State.KnownResourceKeys()
	c.JSON(http.StatusOK, gin.H{"resource_keys": list})
}
