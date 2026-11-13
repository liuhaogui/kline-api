package httpapi

import (
	"context"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/liuhaogui/kline-api/internal/kline"
	"github.com/liuhaogui/kline-api/internal/response"
	"github.com/sirupsen/logrus"
)

// KlineTVHandler 直接代理 tradingview.com 内部数据接口获取K线（非官方接口）。
// 与 KlineHandler（Binance/Tencent/BiQuote）完全独立，互不影响。公开接口，无需鉴权。
type TVSource interface {
	kline.Source
	SearchSymbols(context.Context, string, int) ([]kline.SearchResult, error)
}

type KlineTVHandler struct {
	log     *logrus.Logger
	source  TVSource
	hub     *kline.Hub
	sockets *socketPool
}

func NewKlineTVHandler(log *logrus.Logger, source TVSource, hub *kline.Hub, sockets *socketPool) *KlineTVHandler {
	return &KlineTVHandler{log: log, source: source, hub: hub, sockets: sockets}
}

// GetBars godoc
// GET /api/v1/kline/tv/bars?symbol=BTCUSDT&interval=1D&count=320
func (h *KlineTVHandler) GetBars(c *gin.Context) {
	symbol := c.Query("symbol")
	interval := c.Query("interval")
	if symbol == "" || interval == "" {
		response.API(c, 400, gin.H{"message": "参数有误"})
		return
	}

	count := klineDefaultBarsCount
	if countStr := c.Query("count"); countStr != "" {
		if n, err := strconv.Atoi(countStr); err == nil && n > 0 {
			count = n
		}
	}
	if count > klineMaxBarsCount {
		count = klineMaxBarsCount
	}

	if !h.source.Match(symbol) {
		response.API(c, 400, gin.H{"message": "暂不支持该交易对"})
		return
	}

	session := c.Query("session")
	source, sessionErr := kline.WithTVSession(h.source, symbol, session)
	if sessionErr != nil {
		response.API(c, 400, gin.H{"message": "交易时段参数有误"})
		return
	}
	bars, err := source.GetBars(c.Request.Context(), symbol, interval, count)
	if err != nil {
		if h.log != nil {
			h.log.Errorf("KlineTVHandler.GetBars error symbol=%s interval=%s: %v", symbol, interval, err)
		}
		response.API(c, 500, gin.H{"message": "获取K线失败"})
		return
	}

	response.API(c, 0, gin.H{"list": bars, "session": session})
}

// Search godoc
// GET /api/v1/kline/tv/search?text=小米&limit=30
// 代理 TradingView 官方符号搜索，返回可直接用于 K 线的原生符号（交易所:代码）。
func (h *KlineTVHandler) Search(c *gin.Context) {
	text := c.Query("text")
	if text == "" {
		response.API(c, 0, gin.H{"list": []any{}})
		return
	}
	limit := 30
	if s := c.Query("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			limit = n
		}
	}
	list, err := h.source.SearchSymbols(c.Request.Context(), text, limit)
	if err != nil {
		if h.log != nil {
			h.log.Errorf("KlineTVHandler.Search error text=%s: %v", text, err)
		}
		response.API(c, 500, gin.H{"message": "搜索失败"})
		return
	}
	response.API(c, 0, gin.H{"list": list})
}

// ServeWS godoc
// GET /api/v1/kline/tv/ws
func (h *KlineTVHandler) ServeWS(c *gin.Context) {
	session := c.Query("session")
	if kline.ValidateTVSession(session) != nil {
		response.API(c, 400, gin.H{"message": "交易时段参数有误"})
		return
	}
	conn, release, err := h.sockets.upgrade(c)
	if err != nil {
		if h.log != nil {
			h.log.Errorf("kline tv ws upgrade error: %v", err)
		}
		return
	}
	route := func(symbol string) (kline.Source, error) {
		if !h.source.Match(symbol) {
			return nil, errUnsupportedSymbol(symbol)
		}
		return kline.WithTVSession(h.source, symbol, session)
	}
	defer release()
	serveKlineWS(conn, route, h.hub, h.log, h.sockets.maxSubscriptions, session)
}

type unsupportedSymbolError string

func (e unsupportedSymbolError) Error() string { return "unsupported symbol: " + string(e) }

func errUnsupportedSymbol(symbol string) error {
	return unsupportedSymbolError(symbol)
}
