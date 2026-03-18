package handler

import (
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/xiezhayang/Carve/server/job"
)

func (h *Handlers) AlertingStream(c *gin.Context) {
	target := strings.TrimSpace(c.Query("target"))
	if target == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "missing target"})
		return
	}
	// 校验 target 存在
	all := h.State.AllTargets()
	var found bool
	for _, t := range all {
		if t.Name == target {
			found = true
			break
		}
	}
	if !found {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "target not found"})
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer func() {
		h.Hub.Unsubscribe(target, conn)
		if !h.Hub.HasSubscribers(target) {
			h.State.SetAlerting(target, false)
		}
	}()

	h.Hub.Subscribe(target, conn)
	h.State.SetAlerting(target, true)
	// 读循环：客户端断开时 ReadMessage 会返回错误，然后 defer 退订
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
}

// AlertingDeploy 创建告警器 Deployment；body: { "target": "xxx" }，模型按 target 对应
func (h *Handlers) AlertingDeploy(c *gin.Context) {
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
	var found bool
	for _, t := range all {
		if t.Name == target {
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
	if h.AlerterDeployer == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "error", "message": "未配置 AlerterDeployer（未启用 K8s）"})
		return
	}
	deploymentName, err := h.AlerterDeployer(c.Request.Context(), target, modelName)
	if err != nil {
		log.Printf("[carve] AlertingDeploy error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "创建告警器 Deployment 失败: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "告警器 Deployment 已创建", "deployment": deploymentName})
}

// AlertingDelete 删除告警器 Deployment；query: deployment=alerter-xxx-yyy
func (h *Handlers) AlertingDelete(c *gin.Context) {
	deploymentName := strings.TrimSpace(c.Query("deployment"))
	if deploymentName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "缺少 deployment 参数"})
		return
	}
	if !strings.HasPrefix(deploymentName, "alerter-") {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "deployment 名必须以 alerter- 开头"})
		return
	}
	if len(deploymentName) > 63 {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "deployment 名过长"})
		return
	}
	if h.AlerterDeleter == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "error", "message": "未配置 AlerterDeleter（未启用 K8s）"})
		return
	}
	if err := h.AlerterDeleter(c.Request.Context(), deploymentName); err != nil {
		log.Printf("[carve] AlertingDelete error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "删除告警器 Deployment 失败: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "告警器 Deployment 已删除"})
}
