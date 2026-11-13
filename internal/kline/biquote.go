package kline

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

// biquoteSymbols: hf_ 商品代码 → BiQuote symbol
var biquoteSymbols = map[string]string{
	"hf_XAU":   "XAUUSD", // 黄金
	"hf_XAG":   "XAGUSD", // 白银
	"hf_CL":    "USOIL",  // WTI 原油
	"hf_BRENT": "UKOIL",  // 布伦特原油
}

// biquoteInterval: 前端 interval → BiQuote interval，不支持的周期降级到最近精度
var biquoteInterval = map[string]string{
	"5m": "5m", "15m": "15m", "30m": "30m",
	"1h": "1h", "4h": "4h", "1D": "1d",
	"1m": "5m", "2h": "1h", "6h": "4h", "12h": "4h", "3m": "5m",
	"1W": "1d", "1M": "1d",
}

type BiQuoteSource struct {
	httpClient *http.Client
}

func NewBiQuoteSource() *BiQuoteSource {
	return &BiQuoteSource{httpClient: &http.Client{Timeout: 10 * time.Second}}
}

func (s *BiQuoteSource) Name() string { return "biquote" }

func (s *BiQuoteSource) Match(symbol string) bool { return IsCommodity(symbol) }

type biquoteBar struct {
	OpenTime   string  `json:"openTime"`
	Open       float64 `json:"open"`
	High       float64 `json:"high"`
	Low        float64 `json:"low"`
	Close      float64 `json:"close"`
	TickVolume float64 `json:"tickVolume"`
}

func (s *BiQuoteSource) GetBars(ctx context.Context, symbol, interval string, count int) ([]Bar, error) {
	bqSym, ok := biquoteSymbols[symbol]
	if !ok {
		return nil, fmt.Errorf("biquote: unknown symbol %q", symbol)
	}
	itv, ok := biquoteInterval[interval]
	if !ok {
		itv = "1h"
	}
	limit := 200
	if itv == "1d" {
		limit = 500
	}
	if count > 0 && count < limit {
		limit = count
	}
	url := fmt.Sprintf("https://biquote.io/api/%s/ohlc?interval=%s&limit=%d", bqSym, itv, limit)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var payload struct {
		Bars []biquoteBar `json:"bars"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("biquote ohlc parse error: %w, body: %s", err, string(body))
	}
	// bars 是降序（最新在前），翻转为升序
	bars := make([]Bar, 0, len(payload.Bars))
	for i := len(payload.Bars) - 1; i >= 0; i-- {
		r := payload.Bars[i]
		t, err := time.Parse(time.RFC3339, r.OpenTime)
		if err != nil {
			continue
		}
		bars = append(bars, Bar{
			Time:   t.Unix(),
			Open:   r.Open,
			High:   r.High,
			Low:    r.Low,
			Close:  r.Close,
			Volume: r.TickVolume,
		})
	}
	return bars, nil
}

// biquoteNegotiate 对应 SignalR 的 /negotiate 握手，返回用于 WS 连接的 connectionToken
func biquoteNegotiate(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", "https://biquote.io/hubs/tick/negotiate?negotiateVersion=1", nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var n struct {
		ConnectionToken string `json:"connectionToken"`
	}
	if err := json.Unmarshal(body, &n); err != nil {
		return "", fmt.Errorf("biquote negotiate parse error: %w, body: %s", err, string(body))
	}
	if n.ConnectionToken == "" {
		return "", fmt.Errorf("biquote negotiate: empty connectionToken, body: %s", string(body))
	}
	return n.ConnectionToken, nil
}

type biquoteWsMsg struct {
	Type      int               `json:"type"`
	Target    string            `json:"target"`
	Arguments []json.RawMessage `json:"arguments"`
}

// Ask/Bid 用 interface{} 接收，因为上游可能返回数字或字符串
type biquoteTick struct {
	Ask interface{} `json:"ask"`
	Bid interface{} `json:"bid"`
}

// Subscribe: BiQuote 无 K 线级 WS，走 SignalR tick 流，服务端维护 lastBar 并用 tick 更新 close/high/low。
// 订阅建立时先用 REST 拉一次最新 bar 作为 lastBar 初值，逻辑与前端 biquoteWsSubscribe 一致。
func (s *BiQuoteSource) Subscribe(ctx context.Context, symbol, interval string, onBar func(Bar)) (stop func(), err error) {
	bqSym, ok := biquoteSymbols[symbol]
	if !ok {
		return nil, fmt.Errorf("biquote: unknown symbol %q", symbol)
	}

	var mu sync.Mutex
	var lastBar *Bar
	if bars, err := s.GetBars(ctx, symbol, interval, 1); err == nil && len(bars) > 0 {
		b := bars[len(bars)-1]
		lastBar = &b
	}

	var closed int32
	done := make(chan struct{})

	go func() {
		for atomic.LoadInt32(&closed) == 0 {
			// BiQuote 是 ASP.NET SignalR hub，未 negotiate 直接连 WS 会被拒绝（400 Connection ID required）
			connToken, negErr := biquoteNegotiate(ctx)
			if negErr != nil {
				log.Errorf("biquote negotiate error (%s): %v", bqSym, negErr)
				if !sleepOrDone(3*time.Second, done) {
					return
				}
				continue
			}
			wsURL := "wss://biquote.io/hubs/tick?id=" + connToken
			conn, _, dialErr := websocket.DefaultDialer.Dial(wsURL, nil)
			if dialErr != nil {
				log.Errorf("biquote ws dial error (%s): %v", bqSym, dialErr)
				if !sleepOrDone(3*time.Second, done) {
					return
				}
				continue
			}
			// SignalR JSON protocol 握手
			if err := conn.WriteMessage(websocket.TextMessage, append([]byte(`{"protocol":"json","version":1}`), 0x1e)); err != nil {
				conn.Close()
				if !sleepOrDone(3*time.Second, done) {
					return
				}
				continue
			}
			// 握手确认后订阅（简单延迟，无需强一致）
			time.AfterFunc(300*time.Millisecond, func() {
				sub := fmt.Sprintf(`{"type":1,"target":"Subscribe","arguments":[["%s"]]}`, bqSym)
				conn.WriteMessage(websocket.TextMessage, append([]byte(sub), 0x1e))
			})

			readLoop(conn, done, func(data []byte) {
				parts := strings.Split(string(data), "\x1e")
				for _, part := range parts {
					if part == "" {
						continue
					}
					var msg biquoteWsMsg
					if err := json.Unmarshal([]byte(part), &msg); err != nil {
						continue
					}
					if msg.Type != 1 || msg.Target != "ReceiveTick" || len(msg.Arguments) == 0 {
						continue
					}
					var tick biquoteTick
					if err := json.Unmarshal(msg.Arguments[0], &tick); err != nil {
						continue
					}
					ask, bid := parseFloatAny(tick.Ask), parseFloatAny(tick.Bid)
					price := (ask + bid) / 2
					if price == 0 {
						price = ask
					}
					if price == 0 {
						price = bid
					}
					if price == 0 {
						continue
					}
					mu.Lock()
					if lastBar == nil {
						mu.Unlock()
						continue
					}
					updated := *lastBar
					updated.Close = price
					if price > updated.High {
						updated.High = price
					}
					if price < updated.Low {
						updated.Low = price
					}
					lastBar = &updated
					mu.Unlock()
					onBar(updated)
				}
			})
			conn.Close()
			if atomic.LoadInt32(&closed) == 0 {
				if !sleepOrDone(3*time.Second, done) {
					return
				}
			}
		}
	}()

	return func() {
		atomic.StoreInt32(&closed, 1)
		close(done)
	}, nil
}
