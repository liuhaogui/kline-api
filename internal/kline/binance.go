package kline

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

// binanceInterval 是前端 interval 字符串 → Binance REST/WS interval 参数的映射，
// 与前端 stockKline.js 里的 BINANCE_INTERVAL / BINANCE_WS_INTERVAL 完全一致。
var binanceInterval = map[string]string{
	"1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
	"1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "12h": "12h",
	"1D": "1d", "1W": "1w", "1M": "1M",
}

type BinanceSource struct {
	httpClient *http.Client
}

func NewBinanceSource() *BinanceSource {
	return &BinanceSource{httpClient: &http.Client{Timeout: 10 * time.Second}}
}

func (s *BinanceSource) Name() string { return "binance" }

func (s *BinanceSource) Match(symbol string) bool { return IsCrypto(symbol) }

func (s *BinanceSource) GetBars(ctx context.Context, symbol, interval string, count int) ([]Bar, error) {
	itv, ok := binanceInterval[interval]
	if !ok {
		itv = "1d"
	}
	if count <= 0 {
		count = 320
	}
	url := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&limit=%d",
		strings.ToUpper(symbol), itv, count)
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

	// Binance kline 行: [openTime, open, high, low, close, volume, ...]
	var raw [][]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("binance klines parse error: %w, body: %s", err, string(body))
	}

	bars := make([]Bar, 0, len(raw))
	for _, r := range raw {
		if len(r) < 6 {
			continue
		}
		openTimeMs, _ := r[0].(float64)
		bars = append(bars, Bar{
			Time:   int64(openTimeMs) / 1000,
			Open:   parseFloatAny(r[1]),
			High:   parseFloatAny(r[2]),
			Low:    parseFloatAny(r[3]),
			Close:  parseFloatAny(r[4]),
			Volume: parseFloatAny(r[5]),
		})
	}
	return bars, nil
}

// binanceKlineMsg 是 Binance WS kline 推送的消息结构。
// Binance payload 里大量键名只有大小写之差且含义不同（e/E、t/T、l/L、v/V...）。
// encoding/json 在没有精确 tag 匹配时会做大小写不敏感兜底匹配，如果只声明其中
// 一个大小写，另一个会被错误地匹配到同一个字段上（数字类型会直接报错导致解析
// 失败，字符串类型则静默写入错误的值）。因此这里把上游 payload 里出现的字段
// 全部显式声明，即使部分字段本身用不到，以避免任何隐式的大小写归并。
type binanceKlineMsg struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	K         struct {
		StartTime     int64  `json:"t"`
		CloseTime     int64  `json:"T"`
		Symbol        string `json:"s"`
		Interval      string `json:"i"`
		FirstTradeID  int64  `json:"f"`
		LastTradeID   int64  `json:"L"`
		Open          string `json:"o"`
		Close         string `json:"c"`
		High          string `json:"h"`
		Low           string `json:"l"`
		Volume        string `json:"v"`
		NumTrades     int64  `json:"n"`
		IsClosed      bool   `json:"x"`
		QuoteVolume   string `json:"q"`
		TakerBuyBase  string `json:"V"`
		TakerBuyQuote string `json:"Q"`
		Ignore        string `json:"B"`
	} `json:"k"`
}

func (s *BinanceSource) Subscribe(ctx context.Context, symbol, interval string, onBar func(Bar)) (stop func(), err error) {
	wsItv, ok := binanceInterval[interval]
	if !ok {
		return nil, fmt.Errorf("binance: unsupported interval %q", interval)
	}
	url := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@kline_%s", strings.ToLower(symbol), wsItv)

	var closed int32
	done := make(chan struct{})

	go func() {
		for atomic.LoadInt32(&closed) == 0 {
			conn, _, err := websocket.DefaultDialer.Dial(url, nil)
			if err != nil {
				log.Errorf("binance ws dial error (%s): %v", symbol, err)
				if !sleepOrDone(3*time.Second, done) {
					return
				}
				continue
			}
			readLoop(conn, done, func(data []byte) {
				var msg binanceKlineMsg
				if err := json.Unmarshal(data, &msg); err != nil || msg.EventType != "kline" {
					return
				}
				onBar(Bar{
					Time:   msg.K.StartTime / 1000,
					Open:   parseFloatStr(msg.K.Open),
					High:   parseFloatStr(msg.K.High),
					Low:    parseFloatStr(msg.K.Low),
					Close:  parseFloatStr(msg.K.Close),
					Volume: parseFloatStr(msg.K.Volume),
				})
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
