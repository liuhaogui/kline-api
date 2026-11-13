package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/liuhaogui/kline-api/internal/kline"
	"github.com/liuhaogui/kline-api/internal/response"
	"github.com/sirupsen/logrus"
)

const klineDefaultBarsCount = 320
const klineMaxBarsCount = 1000

var klineWSUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type klineWSSubscribeMsg struct {
	Action   string `json:"action"`
	Symbol   string `json:"symbol"`
	Interval string `json:"interval"`
}

type klineWSOutMsg struct {
	Type     string     `json:"type"`
	Session  string     `json:"session,omitempty"`
	Symbol   string     `json:"symbol,omitempty"`
	Interval string     `json:"interval,omitempty"`
	Bar      *kline.Bar `json:"bar,omitempty"`
	Message  string     `json:"message,omitempty"`
}

// KlineHandler 提供K线历史数据(REST)与实时推送(WebSocket)。公开接口，无需鉴权。
// 数据源：Binance/Tencent/BiQuote，按 symbol 前缀自动路由（services/kline 包内部逻辑）。
type KlineHandler struct {
	log     *logrus.Logger
	router  *kline.SourceRouter
	hub     *kline.Hub
	sockets *socketPool
}

func NewKlineHandler(log *logrus.Logger, router *kline.SourceRouter, hub *kline.Hub, sockets *socketPool) *KlineHandler {
	return &KlineHandler{log: log, router: router, hub: hub, sockets: sockets}
}

// GetBars godoc
// GET /api/v1/kline/bars?symbol=BTCUSDT&interval=1D&count=320
func (h *KlineHandler) GetBars(c *gin.Context) {
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

	source, err := h.router.Route(symbol)
	if err != nil {
		response.API(c, 400, gin.H{"message": "暂不支持该交易对"})
		return
	}

	bars, err := source.GetBars(c.Request.Context(), symbol, interval, count)
	if err != nil {
		h.logError("KlineHandler.GetBars", err, source.Name(), symbol, interval)
		response.API(c, 500, gin.H{"message": "获取K线失败"})
		return
	}

	response.API(c, 0, gin.H{"list": bars})
}

// ServeWS godoc
// GET /api/v1/kline/ws
func (h *KlineHandler) ServeWS(c *gin.Context) {
	conn, release, err := h.sockets.upgrade(c)
	if err != nil {
		if h.log != nil {
			h.log.Errorf("kline ws upgrade error: %v", err)
		}
		return
	}
	defer release()
	serveKlineWS(conn, h.router.Route, h.hub, h.log, h.sockets.maxSubscriptions, "")
}

func (h *KlineHandler) logError(prefix string, err error, source, symbol, interval string) {
	if h.log == nil {
		return
	}
	h.log.Errorf("%s error source=%s symbol=%s interval=%s: %v", prefix, source, symbol, interval, err)
}

// serveKlineWS 是多源(Binance/Tencent/BiQuote)与TradingView两个handler共用的WS会话循环。
// route 由调用方提供：多源版本传 h.router.Route，单源版本(TradingView)传一个
// 固定返回该 Source 的闭包。
func serveKlineWS(conn *websocket.Conn, route func(symbol string) (kline.Source, error), hub *kline.Hub, log *logrus.Logger, maxSubscriptions int, session string) {
	defer conn.Close()
	conn.SetReadLimit(64 << 10)
	conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(90 * time.Second)) })

	clientID := fmt.Sprintf("%p", conn)
	outbox := make(chan klineWSOutMsg, 64)
	quit := make(chan struct{})

	go func() {
		ping := time.NewTicker(30 * time.Second)
		defer ping.Stop()
		defer conn.Close() // A failed write must also unblock ReadMessage and unsubscribe.
		for {
			select {
			case <-ping.C:
				if conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)) != nil {
					return
				}
			case msg := <-outbox:
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteJSON(msg); err != nil {
					return
				}
			case <-quit:
				return
			}
		}
	}()

	subs := make(map[string]func())
	defer func() {
		close(quit)
		for _, unsub := range subs {
			unsub()
		}
	}()

	send := func(msg klineWSOutMsg) {
		msg.Session = session
		select {
		case outbox <- msg:
		case <-quit:
		default:
			if log != nil {
				log.Warnf("kline ws client %s outbox full, drop message", clientID)
			}
		}
	}

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var m klineWSSubscribeMsg
		if err := json.Unmarshal(data, &m); err != nil || m.Symbol == "" || m.Interval == "" {
			send(klineWSOutMsg{Type: "error", Message: "参数有误"})
			continue
		}
		key := m.Symbol + "|" + m.Interval

		switch m.Action {
		case "subscribe":
			if _, exists := subs[key]; exists {
				continue
			}
			if len(subs) >= maxSubscriptions {
				send(klineWSOutMsg{Type: "error", Message: "订阅数量已达上限"})
				continue
			}
			source, err := route(m.Symbol)
			if err != nil {
				send(klineWSOutMsg{Type: "error", Symbol: m.Symbol, Interval: m.Interval, Message: "暂不支持该交易对"})
				continue
			}
			symbol, interval := m.Symbol, m.Interval
			unsub, err := hub.Subscribe(source, symbol, interval, clientID, func(bar kline.Bar) {
				b := bar
				send(klineWSOutMsg{Type: "bar", Symbol: symbol, Interval: interval, Bar: &b})
			})
			if err != nil {
				if log != nil {
					log.Errorf("kline ws subscribe error source=%s symbol=%s interval=%s: %v", source.Name(), symbol, interval, err)
				}
				send(klineWSOutMsg{Type: "error", Symbol: symbol, Interval: interval, Message: "订阅失败"})
				continue
			}
			subs[key] = unsub
			send(klineWSOutMsg{Type: "subscribed", Symbol: symbol, Interval: interval})

		case "unsubscribe":
			if unsub, exists := subs[key]; exists {
				unsub()
				delete(subs, key)
			}
			send(klineWSOutMsg{Type: "unsubscribed", Symbol: m.Symbol, Interval: m.Interval})

		default:
			send(klineWSOutMsg{Type: "error", Message: "未知 action"})
		}
	}
}
