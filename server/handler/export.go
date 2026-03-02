package handler

import (
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/xiezhayang/Carve/datamanager"
)

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

// ExportDelete 删除指定 filename 的 CSV 文件（仅允许 CSVDir 下、SafeFilename）；并移除指向该文件的 target。
func (h *Handlers) ExportDelete(c *gin.Context) {
	filename := strings.TrimSpace(c.Query("filename"))
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
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "文件不存在"})
		return
	}
	if info.IsDir() {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "不能删除目录"})
		return
	}
	if err := os.Remove(full); err != nil {
		log.Printf("[carve] ExportDelete remove %s: %v", full, err)
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}
	// 移除指向该文件的 target，保持 state 一致
	for _, t := range h.State.AllTargets() {
		if t.Filename == filename {
			h.State.DeleteTarget(t.Name)
		}
	}
	_ = h.State.SaveTargets()
	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "deleted", "filename": filename})
}
