package kline

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

var cst = time.FixedZone("CST", 8*3600) // 北京时间，固定偏移，避免依赖系统 tzdata

// tencentPeriod: 日/周/月线 interval → fqkline period 参数
var tencentPeriod = map[string]string{"1D": "day", "1W": "week", "1M": "month"}

// tencentMinuteToken: 分钟/小时线 interval → mkline mtoken；不在表里的走聚合
var tencentMinuteToken = map[string]string{"1m": "m1", "5m": "m5", "15m": "m15", "30m": "m30", "1h": "m60"}

// tencentAggregateN: 由 m60(1小时线) 聚合出更大周期
var tencentAggregateN = map[string]int{"2h": 2, "4h": 4, "6h": 6, "12h": 12}

type TencentSource struct {
	httpClient *http.Client
}

func NewTencentSource() *TencentSource {
	return &TencentSource{httpClient: &http.Client{Timeout: 10 * time.Second}}
}

func (s *TencentSource) Name() string { return "tencent" }

// Match 是兜底源：不是 *USDT 也不是 hf_* 前缀的都归它（A股/港股/指数）
func (s *TencentSource) Match(symbol string) bool {
	return !IsCrypto(symbol) && !IsCommodity(symbol)
}

func (s *TencentSource) httpGetJSON(ctx context.Context, url string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("tencent response parse error: %w, body: %s", err, string(body))
	}
	return nil
}

func (s *TencentSource) GetBars(ctx context.Context, symbol, interval string, count int) ([]Bar, error) {
	if count <= 0 {
		count = 320
	}
	if period, ok := tencentPeriod[interval]; ok {
		return s.fqklineBars(ctx, symbol, period, count)
	}
	if mtoken, ok := tencentMinuteToken[interval]; ok {
		return s.mklineBars(ctx, symbol, mtoken, count)
	}
	if n, ok := tencentAggregateN[interval]; ok {
		hourly, err := s.mklineBars(ctx, symbol, "m60", count*n)
		if err != nil {
			return nil, err
		}
		return chunkAggregate(hourly, n), nil
	}
	return s.fqklineBars(ctx, symbol, "day", count)
}

// fqklineBars: 日/周/月K，行 [date, open, close, high, low, volume, ...]
func (s *TencentSource) fqklineBars(ctx context.Context, symbol, period string, count int) ([]Bar, error) {
	url := fmt.Sprintf("https://web.ifzq.gtimg.cn/appstuff/app/newfqkline/get?param=%s,%s,,,%d,qfq", symbol, period, count)
	var payload struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := s.httpGetJSON(ctx, url, &payload); err != nil {
		return nil, err
	}
	node, ok := payload.Data[symbol]
	if !ok {
		return nil, nil
	}
	var wrap map[string]json.RawMessage
	if err := json.Unmarshal(node, &wrap); err != nil {
		return nil, err
	}
	rowsRaw := wrap["qfqday"]
	if len(rowsRaw) == 0 {
		rowsRaw = wrap["day"]
	}
	if len(rowsRaw) == 0 {
		rowsRaw = wrap["hfqday"]
	}
	var rows [][]interface{}
	if len(rowsRaw) > 0 {
		if err := json.Unmarshal(rowsRaw, &rows); err != nil {
			return nil, err
		}
	}
	bars := make([]Bar, 0, len(rows))
	for _, r := range rows {
		if len(r) < 6 {
			continue
		}
		dateStr, _ := r[0].(string)
		t, err := time.ParseInLocation("2006-01-02", dateStr, cst)
		if err != nil {
			continue
		}
		bars = append(bars, Bar{
			Time:   t.Unix(),
			Open:   parseFloatAny(r[1]),
			Close:  parseFloatAny(r[2]),
			High:   parseFloatAny(r[3]),
			Low:    parseFloatAny(r[4]),
			Volume: parseFloatAny(r[5]),
		})
	}
	return bars, nil
}

// mklineBars: 分钟/小时K，行 ["YYYYMMDDHHmm", open, close, high, low, volume, ...]
func (s *TencentSource) mklineBars(ctx context.Context, symbol, mtoken string, count int) ([]Bar, error) {
	url := fmt.Sprintf("https://ifzq.gtimg.cn/appstock/app/kline/mkline?param=%s,%s,,%d", symbol, mtoken, count)
	var payload struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := s.httpGetJSON(ctx, url, &payload); err != nil {
		return nil, err
	}
	node, ok := payload.Data[symbol]
	if !ok {
		return nil, nil
	}
	var wrap map[string]json.RawMessage
	if err := json.Unmarshal(node, &wrap); err != nil {
		return nil, err
	}
	var rows [][]interface{}
	if raw, ok := wrap[mtoken]; ok {
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, err
		}
	}
	bars := make([]Bar, 0, len(rows))
	for _, r := range rows {
		if len(r) < 6 {
			continue
		}
		s0, _ := r[0].(string)
		if len(s0) < 12 {
			continue
		}
		t, err := time.ParseInLocation("200601021504", s0, cst)
		if err != nil {
			continue
		}
		bars = append(bars, Bar{
			Time:   t.Unix(),
			Open:   parseFloatAny(r[1]),
			Close:  parseFloatAny(r[2]),
			High:   parseFloatAny(r[3]),
			Low:    parseFloatAny(r[4]),
			Volume: parseFloatAny(r[5]),
		})
	}
	return bars, nil
}

// chunkAggregate 把 N 根小周期K合并成大周期（bars 需按时间升序）
func chunkAggregate(bars []Bar, n int) []Bar {
	if n <= 1 {
		return bars
	}
	out := make([]Bar, 0, len(bars)/n+1)
	for i := 0; i < len(bars); i += n {
		end := i + n
		if end > len(bars) {
			end = len(bars)
		}
		slice := bars[i:end]
		high, low, volume := slice[0].High, slice[0].Low, 0.0
		for _, b := range slice {
			if b.High > high {
				high = b.High
			}
			if b.Low < low {
				low = b.Low
			}
			volume += b.Volume
		}
		out = append(out, Bar{
			Time: slice[0].Time, Open: slice[0].Open, High: high, Low: low,
			Close: slice[len(slice)-1].Close, Volume: volume,
		})
	}
	return out
}

// isAShareTradingHours 判断当前是否处于 A 股交易时间（北京时间工作日 9:30-11:30 / 13:00-15:00）
func isAShareTradingHours() bool {
	now := time.Now().In(cst)
	if now.Weekday() == time.Sunday || now.Weekday() == time.Saturday {
		return false
	}
	mins := now.Hour()*60 + now.Minute()
	return (mins >= 9*60+30 && mins <= 11*60+30) || (mins >= 13*60 && mins <= 15*60)
}

// pollMs 按图表周期返回合适的轮询间隔（长周期意义不大，降低频率）
func pollMs(interval string) time.Duration {
	switch interval {
	case "1W", "1M":
		return 5 * time.Minute
	case "6h", "12h", "1D":
		return 30 * time.Second
	case "2h", "4h":
		return 15 * time.Second
	default:
		return 5 * time.Second
	}
}

var qtLineRe = regexp.MustCompile(`v_(\w+)="([^"]*)"`)

// tencentRealtimeDayBar 用轻量 qt 接口拿今日日线 bar
// qt 字段（~ 分隔）：[3]现价 [5]今开 [6]成交量 [33]最高 [34]最低
func (s *TencentSource) tencentRealtimeDayBar(ctx context.Context, symbol string) (*Bar, error) {
	url := "https://qt.gtimg.cn/q=" + symbol
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	m := qtLineRe.FindStringSubmatch(string(body))
	if len(m) < 3 {
		return nil, nil
	}
	f := strings.Split(m[2], "~")
	if len(f) < 35 {
		return nil, nil
	}
	close_, _ := strconv.ParseFloat(f[3], 64)
	open, _ := strconv.ParseFloat(f[5], 64)
	high, _ := strconv.ParseFloat(f[33], 64)
	low, _ := strconv.ParseFloat(f[34], 64)
	vol, _ := strconv.ParseFloat(f[6], 64)
	if close_ == 0 {
		return nil, nil
	}
	now := time.Now().In(cst)
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, cst)
	return &Bar{Time: midnight.Unix(), Open: open, High: high, Low: low, Close: close_, Volume: vol}, nil
}

// Subscribe: 腾讯无原生 WS，服务端按 pollMs 在 A 股交易时段内轮询 REST，末根 bar 推给下游。
func (s *TencentSource) Subscribe(ctx context.Context, symbol, interval string, onBar func(Bar)) (stop func(), err error) {
	var closed int32
	done := make(chan struct{})
	interval2 := interval
	ms := pollMs(interval2)

	go func() {
		for {
			if !sleepOrDone(ms, done) {
				return
			}
			if atomic.LoadInt32(&closed) != 0 {
				return
			}
			if !isAShareTradingHours() {
				continue
			}
			var bar *Bar
			if interval2 == "1D" {
				b, derr := s.tencentRealtimeDayBar(ctx, symbol)
				if derr != nil {
					log.Errorf("tencent poll dayBar error (%s): %v", symbol, derr)
					continue
				}
				bar = b
			} else {
				bars, derr := s.GetBars(ctx, symbol, interval2, 1)
				if derr != nil {
					log.Errorf("tencent poll GetBars error (%s): %v", symbol, derr)
					continue
				}
				if len(bars) > 0 {
					bar = &bars[len(bars)-1]
				}
			}
			if bar != nil {
				onBar(*bar)
			}
		}
	}()

	return func() {
		atomic.StoreInt32(&closed, 1)
		close(done)
	}, nil
}
