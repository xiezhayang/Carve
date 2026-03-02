package handler

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/xiezhayang/Carve/server/job"
)

func (h *Handlers) ModelUpload(c *gin.Context) {
	name := strings.TrimSpace(c.PostForm("name"))
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "缺少 name"})
		return
	}
	// 只允许字母数字、横线、下划线，避免路径穿越
	safe := true
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			safe = false
			break
		}
	}
	if !safe || len(name) > 128 {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "name 仅允许字母数字、横线、下划线，且长度≤128"})
		return
	}
	file, err := c.FormFile("model")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "缺少 model 文件: " + err.Error()})
		return
	}
	const maxSize = 50 << 20 // 50MB
	if file.Size > maxSize {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"status": "error", "message": "模型文件超过 50MB"})
		return
	}
	if err := os.MkdirAll(h.Cfg.ModelDir(), 0755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}
	dst := filepath.Join(h.Cfg.ModelDir(), name+".pt")
	if err := c.SaveUploadedFile(file, dst); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}
	// 收到模型后删除对应训练 Job
	if h.JobDeleter != nil {
		jobName := job.JobName(name)
		if err := h.JobDeleter(c.Request.Context(), jobName); err != nil {
			log.Printf("[carve] ModelUpload: delete job %s: %v error", jobName, err)
			// 不因删 Job 失败而把整个上传判失败，模型已落盘
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "path": dst})
}

// ModelDownload 按 name 或 target 返回 model.pt。query: name=xxx 或 target=xxx（与 target 对应时用 target）
func (h *Handlers) ModelDownload(c *gin.Context) {
	name := strings.TrimSpace(c.Query("name"))
	if name == "" {
		target := strings.TrimSpace(c.Query("target"))
		if target != "" {
			name = job.SanitizeJobName(target)
		}
	}
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "缺少 name 或 target"})
		return
	}
	safe := true
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			safe = false
			break
		}
	}
	if !safe || len(name) > 128 {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "name/target 仅允许字母数字、横线、下划线，且长度≤128"})
		return
	}
	dir := h.Cfg.ModelDir()
	fullPath := filepath.Join(dir, name+".pt")
	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}
	dirAbs, _ := filepath.Abs(dir)
	if len(absPath) < len(dirAbs) || absPath[:len(dirAbs)] != dirAbs {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "无效路径"})
		return
	}
	info, err := os.Stat(fullPath)
	if err != nil || info.IsDir() {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "模型不存在"})
		return
	}
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", `attachment; filename="`+name+`.pt"`)
	c.File(fullPath)
}
