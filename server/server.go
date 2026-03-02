package server

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xiezhayang/Carve/datamanager"
	"github.com/xiezhayang/Carve/server/config"
	"github.com/xiezhayang/Carve/server/handler"
	"github.com/xiezhayang/Carve/server/stream"
)

// Server wraps the Gin engine and runs the HTTP server.
type Server struct {
	engine *gin.Engine
	addr   string
}

// New builds a Server with config, collect state, and CSV writer. Call Run to start listening.
func New(cfg *config.Config, state *datamanager.State, writer func(path string, rows []datamanager.Row) (int, error), jobRunner func(csvFilename, modelName string) error, jobDeleter func(ctx context.Context, jobName string) error, alerterDeployer func(ctx context.Context, target, modelName string) (deploymentName string, err error), alerterDeleter func(ctx context.Context, deploymentName string) error) *Server {
	gin.SetMode(gin.ReleaseMode) // 关闭 [GIN-debug] 启动信息
	engine := gin.New()
	engine.Use(gin.Recovery())
	hub := stream.NewHub()
	h := &handler.Handlers{
		Cfg:             cfg,
		State:           state,
		Hub:             hub,
		Writer:          writer,
		JobRunner:       jobRunner,
		JobDeleter:      jobDeleter,
		AlerterDeployer: alerterDeployer,
		AlerterDeleter:  alerterDeleter,
	}
	registerRoutes(engine, h)
	return &Server{
		engine: engine,
		addr:   ":" + cfg.Port(),
	}
}

func registerRoutes(r *gin.Engine, h *handler.Handlers) {
	r.GET("/healthz", h.Healthz)
	r.POST("/v1/metrics", h.V1Metrics)
	r.POST("/collect/start", h.CollectStart)
	r.POST("/collect/stop", h.CollectStop)
	r.GET("/collect/status", h.CollectStatus)
	r.GET("/collect/filters", h.CollectFiltersGet)
	r.GET("/metrics/available", h.GetAvailableMetrics)
	r.GET("/export/list", h.ExportList)
	r.GET("/export", h.Export)
	r.POST("/export/delete", h.ExportDelete)
	r.POST("/target/delete", h.CollectTargetDelete)
	r.POST("/model/upload", h.ModelUpload)
	r.POST("/train", h.TrainStart)
	r.GET("/resource/available", h.GetAvailableResourceKeys)
	r.GET("/alerting/stream", h.AlertingStream)
	r.GET("/model/download", h.ModelDownload)
	r.POST("/alerting/deploy", h.AlertingDeploy)
	r.DELETE("/alerting/deploy", h.AlertingDelete)
}

// Run starts the HTTP server and blocks. It returns when the server is stopped or errors.
func (s *Server) Run() error {
	return s.engine.Run(s.addr)
}

// Handler returns the underlying http.Handler (e.g. for testing or embedding).
func (s *Server) Handler() http.Handler {
	return s.engine
}
