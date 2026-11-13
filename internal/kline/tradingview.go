package kline

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

// tvSearchURL 是 TradingView 官方符号搜索接口（网页搜索框同源，非 K 线那个 WS）。
const tvSearchURL = "https://symbol-search.tradingview.com/symbol_search/"

var tvEmRe = regexp.MustCompile(`</?em>`)

// SearchResult 是归一后的搜索结果：symbol 为可直接用于本项目/透传的 TradingView 原生符号
// （交易所:代码，如 HKEX:1810 / BINANCE:BTCUSDT / OANDA:EURUSD）。
type SearchResult struct {
	Symbol      string `json:"symbol"`      // 交易所:代码（可解析）
	Description string `json:"description"` // 名称
	Exchange    string `json:"exchange"`    // 交易所显示名
	Type        string `json:"type"`        // stock/spot/forex/futures/index...
}

// SearchSymbols 代理 TradingView 官方符号搜索，返回归一后的原生符号列表。
// 覆盖 TradingView 全市场（美股/A股/港股/外汇/加密/期货/指数…），无需逐市场维护映射。
func (s *TradingViewSource) SearchSymbols(ctx context.Context, text string, limit int) ([]SearchResult, error) {
	if limit <= 0 {
		limit = 30
	}
	q := url.Values{}
	q.Set("text", text)
	q.Set("hl", "1")
	q.Set("lang", "zh")
	q.Set("domain", "production")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tvSearchURL+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	// tradingview.com 内部接口按 Origin 校验来源，与 K 线 WS 一致。
	req.Header.Set("Origin", "https://www.tradingview.com")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}

	var raw []struct {
		Symbol      string `json:"symbol"`
		Description string `json:"description"`
		Type        string `json:"type"`
		Exchange    string `json:"exchange"`
		Prefix      string `json:"prefix"`
		SourceID    string `json:"source_id"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	out := make([]SearchResult, 0, len(raw))
	for _, r := range raw {
		// 过滤衍生品噪音（窝轮/结构化产品），它们会把真正的股票/标的淹没。
		if r.Type == "warrant" || r.Type == "structured" {
			continue
		}
		code := tvEmRe.ReplaceAllString(r.Symbol, "") // 去掉高亮 <em> 标签
		if code == "" {
			continue
		}
		// 解析前缀优先用 prefix / source_id（如 BINANCE），exchange 只是显示名（如 Binance）。
		ex := r.Prefix
		if ex == "" {
			ex = r.SourceID
		}
		if ex == "" {
			ex = r.Exchange
		}
		full := code
		if ex != "" && !strings.Contains(code, ":") {
			full = ex + ":" + code
		}
		out = append(out, SearchResult{
			Symbol:      full,
			Description: tvEmRe.ReplaceAllString(r.Description, ""),
			Exchange:    r.Exchange,
			Type:        r.Type,
		})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// TradingViewSource 是独立实现，对接 tradingview.com 网站自己图表用的未公开内部接口
// （非官方 API，没有文档，随时可能变化/失效；仅用于本地自测）。
// 与 Binance/Tencent/BiQuote 三个 adapter 完全独立，不共享任何代码路径。
const tvWSURL = "wss://data.tradingview.com/socket.io/websocket?from=chart&type=chart"

var tvFrameRe = regexp.MustCompile(`~m~(\d+)~m~`)

// tvSymbol 把 symbol 归一成 TradingView 的 "交易所:代码" 格式。
// 首选透传：前端搜索返回的就是 TradingView 原生符号（如 HKEX:1810 / OANDA:EURUSD /
// BINANCE:BTCUSDT），带 ":" 直接用，覆盖 TradingView 全市场，无需逐市场维护映射。
// 其余分支是对项目自定义格式（历史自选 / 直接输入 BTCUSDT、sh600000）的兼容映射。
func tvSymbol(symbol string) (string, error) {
	if strings.Contains(symbol, ":") {
		return symbol, nil
	}
	if IsCrypto(symbol) {
		return "BINANCE:" + strings.ToUpper(symbol), nil
	}
	if IsCommodity(symbol) {
		m := map[string]string{
			"hf_XAU":   "OANDA:XAUUSD",
			"hf_XAG":   "OANDA:XAGUSD",
			"hf_CL":    "TVC:USOIL",
			"hf_BRENT": "TVC:UKOIL",
		}
		if v, ok := m[symbol]; ok {
			return v, nil
		}
		return "", fmt.Errorf("tradingview: unmapped commodity symbol %q", symbol)
	}
	if strings.HasPrefix(symbol, "sh") {
		return "SSE:" + symbol[2:], nil
	}
	if strings.HasPrefix(symbol, "sz") {
		return "SZSE:" + symbol[2:], nil
	}
	return "", fmt.Errorf("tradingview: unmapped symbol %q", symbol)
}

// tvResolution: 项目 interval 字符串 -> TradingView resolution 参数
var tvResolution = map[string]string{
	"1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
	"1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
	"1D": "1D", "1W": "1W", "1M": "1M",
}

func randTVSession(prefix string) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 12)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		b[i] = alphabet[n.Int64()]
	}
	return prefix + string(b)
}

func tvFrame(m string, p []interface{}) []byte {
	payload, _ := json.Marshal(map[string]interface{}{"m": m, "p": p})
	return []byte(fmt.Sprintf("~m~%d~m~%s", len(payload), payload))
}

// splitTVFrames 拆分一条 WS 消息里可能粘连的多个 ~m~len~m~payload 帧
// （实测同一条物理 WS 消息里可能一次带好几个逻辑帧）。
func splitTVFrames(data []byte) [][]byte {
	var frames [][]byte
	s := data
	for len(s) > 0 {
		loc := tvFrameRe.FindSubmatchIndex(s)
		if loc == nil {
			break
		}
		n, err := strconv.Atoi(string(s[loc[2]:loc[3]]))
		if err != nil {
			break
		}
		start := loc[1]
		if start+n > len(s) {
			break
		}
		frames = append(frames, s[start:start+n])
		s = s[start+n:]
	}
	return frames
}

// parseTVBars 解析 timescale_update / du 消息里的 s 数组：[{"i":idx,"v":[time,open,high,low,close,volume]}]
func parseTVBars(raw json.RawMessage) []Bar {
	var arr []struct {
		V []float64 `json:"v"`
	}
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil
	}
	bars := make([]Bar, 0, len(arr))
	for _, item := range arr {
		if len(item.V) < 6 {
			continue
		}
		bars = append(bars, Bar{
			Time:   int64(item.V[0]),
			Open:   item.V[1],
			High:   item.V[2],
			Low:    item.V[3],
			Close:  item.V[4],
			Volume: item.V[5],
		})
	}
	return bars
}

// tvSeriesBars 从一个 {m, p} 消息 envelope 里提取 sds_1 series 的 bar 数组，不是目标消息类型返回 nil
func tvSeriesBars(envM string, envP json.RawMessage) []Bar {
	if envM != "du" && envM != "timescale_update" {
		return nil
	}
	var p []json.RawMessage
	if json.Unmarshal(envP, &p) != nil || len(p) < 2 {
		return nil
	}
	var wrap map[string]struct {
		S json.RawMessage `json:"s"`
	}
	if json.Unmarshal(p[1], &wrap) != nil {
		return nil
	}
	series, ok := wrap["sds_1"]
	if !ok {
		return nil
	}
	return parseTVBars(series.S)
}

const tvHistoryTimeout = 8 * time.Second
const tvHandshakeTimeout = 3 * time.Second
const tvReconnectMax = 30 * time.Second

type TradingViewSource struct {
	wsURL        string
	historySlots chan struct{}
}

func NewTradingViewSource() *TradingViewSource {
	return &TradingViewSource{wsURL: tvWSURL, historySlots: make(chan struct{}, 8)}
}

func (s *TradingViewSource) Name() string { return "tradingview" }

func (s *TradingViewSource) Match(symbol string) bool {
	_, err := tvSymbol(symbol)
	return err == nil
}

// Keep failed-handshake status in logs; a generic "bad handshake" cannot
// distinguish upstream rate limits from a protocol or network failure.
func tvDial(ctx context.Context, endpoint string) (*websocket.Conn, error) {
	header := http.Header{}
	header.Set("Origin", "https://www.tradingview.com")
	header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = tvHandshakeTimeout
	// Gorilla applies deadlines to the HTTP upgrade, but cancellation after TCP
	// connect must also close that socket while ReadResponse is blocked.
	var stopDialCancel func() bool
	dialer.NetDialContext = func(dialCtx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{Timeout: tvHandshakeTimeout, KeepAlive: 30 * time.Second}).DialContext(dialCtx, network, address)
		if err == nil {
			stopDialCancel = context.AfterFunc(ctx, func() { conn.Close() })
		}
		return conn, err
	}
	defer func() {
		if stopDialCancel != nil {
			stopDialCancel()
		}
	}()
	conn, response, err := dialer.DialContext(ctx, endpoint, header)
	if err != nil && response != nil {
		if response.Body != nil {
			response.Body.Close()
		}
		return nil, fmt.Errorf("tradingview handshake HTTP %d: %w", response.StatusCode, err)
	}
	return conn, err
}

var errTVSeries = errors.New("tradingview symbol or series error")

func (s *TradingViewSource) GetBars(ctx context.Context, symbol, interval string, count int) ([]Bar, error) {
	tvSym, err := tvSymbol(symbol)
	if err != nil {
		return nil, err
	}
	res, ok := tvResolution[interval]
	if !ok {
		res = "1D"
	}
	if count <= 0 {
		count = 320
	}
	ctx, cancel := context.WithTimeout(ctx, tvHistoryTimeout)
	defer cancel()
	select {
	case s.historySlots <- struct{}{}:
		defer func() { <-s.historySlots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	for attempt := 0; attempt < 2; attempt++ {
		var bars []Bar
		bars, err = s.historyOnce(ctx, tvSym, res, count)
		if err == nil {
			return bars, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, errTVSeries) {
			return nil, err
		}
		if attempt == 0 && !sleepOrDone(150*time.Millisecond, ctx.Done()) {
			return nil, ctx.Err()
		}
	}
	return nil, err
}

func (s *TradingViewSource) historyOnce(ctx context.Context, tvSym, resolution string, count int) ([]Bar, error) {
	conn, err := tvDial(ctx, s.wsURL)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stopCancel := context.AfterFunc(ctx, func() { conn.Close() })
	defer stopCancel()
	deadline, _ := ctx.Deadline()
	conn.SetWriteDeadline(deadline)
	chartSession := randTVSession("cs_")
	resolveObj, _ := json.Marshal(tvResolveObject(ctx, tvSym))
	for _, command := range []struct {
		method string
		params []interface{}
	}{
		{"set_auth_token", []interface{}{"unauthorized_user_token"}},
		{"chart_create_session", []interface{}{chartSession, ""}},
		{"resolve_symbol", []interface{}{chartSession, "sds_sym_1", "=" + string(resolveObj)}},
		{"create_series", []interface{}{chartSession, "sds_1", "s1", "sds_sym_1", resolution, count, ""}},
	} {
		if err := conn.WriteMessage(websocket.TextMessage, tvFrame(command.method, command.params)); err != nil {
			return nil, err
		}
	}
	for {
		readDeadline := time.Now().Add(3 * time.Second)
		if deadline.Before(readDeadline) {
			readDeadline = deadline
		}
		conn.SetReadDeadline(readDeadline)
		_, data, err := conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("tradingview read error: %w", err)
		}
		for _, f := range splitTVFrames(data) {
			if strings.HasPrefix(string(f), "~h~") {
				if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("~m~%d~m~%s", len(f), f))); err != nil {
					return nil, err
				}
				continue
			}
			var env struct {
				M string
				P json.RawMessage
			}
			if json.Unmarshal(f, &env) != nil {
				continue
			}
			if env.M == "symbol_error" || env.M == "series_error" {
				return nil, fmt.Errorf("%w: %s", errTVSeries, string(f))
			}
			if bars := tvSeriesBars(env.M, env.P); bars != nil {
				return bars, nil
			}
		}
	}
}

func (s *TradingViewSource) Subscribe(ctx context.Context, symbol, interval string, onBar func(Bar)) (func(), error) {
	tvSym, err := tvSymbol(symbol)
	if err != nil {
		return nil, err
	}
	resolution, ok := tvResolution[interval]
	if !ok {
		resolution = "1D"
	}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		backoff := time.Second
		for ctx.Err() == nil {
			received, err := s.subscriptionOnce(ctx, tvSym, resolution, onBar)
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, errTVSeries) {
				log.Warnf("tradingview subscription rejected (%s): %v", symbol, err)
				return
			}
			log.Warnf("tradingview subscription disconnected (%s): %v; retry in %s", symbol, err, backoff)
			if received {
				backoff = time.Second
			}
			if !sleepOrDone(backoff, ctx.Done()) {
				return
			}
			backoff *= 2
			if backoff > tvReconnectMax {
				backoff = tvReconnectMax
			}
		}
	}()
	return cancel, nil
}

func (s *TradingViewSource) subscriptionOnce(ctx context.Context, symbol, resolution string, onBar func(Bar)) (bool, error) {
	conn, err := tvDial(ctx, s.wsURL)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	stopCancel := context.AfterFunc(ctx, func() { conn.Close() })
	defer stopCancel()
	chartSession := randTVSession("cs_")
	resolveObj, _ := json.Marshal(tvResolveObject(ctx, symbol))
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	for _, command := range []struct {
		method string
		params []interface{}
	}{
		{"set_auth_token", []interface{}{"unauthorized_user_token"}},
		{"chart_create_session", []interface{}{chartSession, ""}},
		{"resolve_symbol", []interface{}{chartSession, "sds_sym_1", "=" + string(resolveObj)}},
		{"create_series", []interface{}{chartSession, "sds_1", "s1", "sds_sym_1", resolution, 1, ""}},
	} {
		if err := conn.WriteMessage(websocket.TextMessage, tvFrame(command.method, command.params)); err != nil {
			return false, err
		}
	}
	conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	received := false
	var seriesErr error
	err = readLoop(conn, ctx.Done(), func(data []byte) {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		for _, f := range splitTVFrames(data) {
			if strings.HasPrefix(string(f), "~h~") {
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("~m~%d~m~%s", len(f), f))) != nil {
					conn.Close()
				}
				continue
			}
			var env struct {
				M string
				P json.RawMessage
			}
			if json.Unmarshal(f, &env) != nil {
				continue
			}
			if env.M == "symbol_error" || env.M == "series_error" {
				seriesErr = fmt.Errorf("%w: %s", errTVSeries, string(f))
				conn.Close()
				return
			}
			for _, bar := range tvSeriesBars(env.M, env.P) {
				received = true
				onBar(bar)
			}
		}
	})
	if seriesErr != nil {
		return received, seriesErr
	}
	return received, err
}
