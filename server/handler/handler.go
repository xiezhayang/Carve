package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/xiezhayang/Carve/datamanager"
	"github.com/xiezhayang/Carve/server/config"
	"github.com/xiezhayang/Carve/server/stream"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Handlers struct {
	Cfg             *config.Config
	State           *datamanager.State
	Hub             *stream.Hub
	Writer          func(path string, rows []datamanager.Row) (int, error)
	JobRunner       func(csvFilename, modelName string) error       // 创建训练 Job，nil 表示未配置
	JobDeleter      func(ctx context.Context, jobName string) error // 收到模型后删 Job，nil 表示不删
	AlerterDeployer func(ctx context.Context, target, modelName string) (deploymentName string, err error)
	AlerterDeleter  func(ctx context.Context, deploymentName string) error
}

func (h *Handlers) Healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
		"service":   "carve",
	})
}
