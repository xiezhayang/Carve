package handler

import (
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xiezhayang/Carve/datamanager"
	"github.com/xiezhayang/Carve/otlp"
)

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
	//parsed.DebugLogRawPayload(250, "load")
	if names := parsed.MetricNames(); len(names) > 0 {
		h.State.AddKnownMetrics(names)
	}
	if keys := parsed.ResourceKeys(); len(keys) > 0 {
		h.State.AddKnownResourceKeys(keys)
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
		log.Printf("[carve] V1Metrics no active targets")
		return
	}
	var wg sync.WaitGroup
	for _, t := range targets {
		stats := &otlp.FilterFailureStats{}
		rows := parsed.RowsForFilterWithStats(t.Filter, stats)
		if h.Writer == nil || len(rows) == 0 {
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
		if _, err := os.Stat(t.Path); os.IsNotExist(err) {
			_ = datamanager.WriteTargetMeta(t.Path, t.Name, t.Filter)
		}
		wg.Add(1)
		go func(path string, r []datamanager.Row, targetName string) {
			defer wg.Done()
			_, err := h.Writer(path, r)
			if err != nil {
				log.Printf("[carve] V1Metrics writer error=%v", err)
			}
			if h.Hub != nil {
				h.Hub.Broadcast(targetName, r)
			}
		}(t.Path, rows, t.Name)
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
