package handler

import (
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/xiezhayang/Carve/server/job"
)

func (h *Handlers) TrainStart(c *gin.Context) {
	var body struct {
		Target string `json:"target"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "请求体 JSON 无效: " + err.Error()})
		return
	}
	target := strings.TrimSpace(body.Target)
	if target == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "target 必填"})
		return
	}
	all := h.State.AllTargets()
	var csvFilename string
	var found bool
	for _, t := range all {
		if t.Name == target {
			csvFilename = t.CSVFilename
			found = true
			break
		}
	}
	if !found {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "target not found"})
		return
	}
	modelName := job.SanitizeJobName(target)
	if modelName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "target 无法转换为合法模型名"})
		return
	}
	if h.JobRunner == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "error", "message": "未配置 JobRunner（未启用 K8s）"})
		return
	}
	if err := h.JobRunner(csvFilename, modelName); err != nil {
		log.Printf("[carve] TrainStart JobRunner error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "创建训练 Job 失败: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "训练 Job 已创建", "target": target, "csv_filename": csvFilename, "model_name": modelName})
}
