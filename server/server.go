package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xiezhayang/Carve/datamanager"
	"github.com/xiezhayang/Carve/server/config"
	"github.com/xiezhayang/Carve/server/handler"
)

// Server wraps the Gin engine and runs the HTTP server.
type Server struct {
	engine *gin.Engine
	addr   string
}

// New builds a Server with config, collect state, and CSV writer. Call Run to start listening.
func New(cfg *config.Config, state *datamanager.State, writer func(path string, rows []datamanager.Row) (int, error)) *Server {
	gin.SetMode(gin.ReleaseMode) // 关闭 [GIN-debug] 启动信息
	engine := gin.New()
	engine.Use(gin.Recovery())
	h := &handler.Handlers{
		Cfg:    cfg,
		State:  state,
		Writer: writer,
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
	r.POST("/target/delete", h.CollectTargetDelete)
}

// Run starts the HTTP server and blocks. It returns when the server is stopped or errors.
func (s *Server) Run() error {
	return s.engine.Run(s.addr)
}

// Handler returns the underlying http.Handler (e.g. for testing or embedding).
func (s *Server) Handler() http.Handler {
	return s.engine
}
