package httpapi

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/liuhaogui/kline-api/internal/config"
	"github.com/liuhaogui/kline-api/internal/kline"
	"github.com/liuhaogui/kline-api/internal/response"
	"github.com/sirupsen/logrus"
)

type Server struct {
	Handler http.Handler
	sockets *socketPool
	hubs    []*kline.Hub
}

func New(cfg config.Config, log *logrus.Logger, version string) *Server {
	router := kline.NewSourceRouter(kline.NewBinanceSource(), kline.NewBiQuoteSource(), kline.NewTencentSource())
	return newServer(cfg, log, version, router, kline.NewTradingViewSource())
}

func newServer(cfg config.Config, log *logrus.Logger, version string, router *kline.SourceRouter, tv TVSource) *Server {
	pool := newSocketPool(cfg.MaxConnections, cfg.MaxSubscriptions)
	multiHub, tvHub := kline.NewHub(), kline.NewHub()
	multi := NewKlineHandler(log, router, multiHub, pool)
	tradingview := NewKlineTVHandler(log, tv, tvHub, pool)
	r := gin.New()
	r.Use(gin.Recovery(), func(c *gin.Context) { c.Set(response.AppLoggerContextKey, log); c.Next() })
	r.GET("/healthz", func(c *gin.Context) { response.API(c, 0, gin.H{"status": "ok", "version": version}) })
	prefixes := []string{cfg.APIPrefix}
	if cfg.APIPrefix != "/api/v1" {
		prefixes = append(prefixes, "/api/v1")
	}
	for _, prefix := range prefixes {
		group := r.Group(prefix + "/kline")
		group.Use(cors(cfg.CORSOrigins), requestBudget(cfg.RequestTimeout))
		group.GET("/bars", multi.GetBars)
		group.GET("/ws", multi.ServeWS)
		group.GET("/tv/bars", tradingview.GetBars)
		group.GET("/tv/ws", tradingview.ServeWS)
		group.GET("/tv/search", tradingview.Search)
		group.OPTIONS("/*path", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	}
	r.NoRoute(func(c *gin.Context) { response.AbortAPI(c, 404, gin.H{"message": "接口不存在"}) })
	return &Server{Handler: r, sockets: pool, hubs: []*kline.Hub{multiHub, tvHub}}
}

// net/http Shutdown does not close hijacked WebSocket connections.
func (s *Server) Close() {
	s.sockets.close()
	for _, hub := range s.hubs {
		hub.Close()
	}
}

func requestBudget(timeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !strings.HasSuffix(c.Request.URL.Path, "/ws") {
			ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
			defer cancel()
			c.Request = c.Request.WithContext(ctx)
		}
		c.Next()
	}
}

func cors(origins []string) gin.HandlerFunc {
	allowed := make(map[string]bool, len(origins))
	for _, value := range origins {
		allowed[value] = true
	}
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if allowed["*"] {
			c.Header("Access-Control-Allow-Origin", "*")
		} else if origin != "" {
			c.Header("Vary", "Origin")
			if !allowed[origin] {
				response.AbortAPI(c, 403, gin.H{"message": "来源未获允许"})
				return
			}
			c.Header("Access-Control-Allow-Origin", origin)
		}
		c.Header("Access-Control-Allow-Methods", "GET, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
