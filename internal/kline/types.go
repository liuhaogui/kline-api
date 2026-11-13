package kline

import "context"

// Bar 是统一的 K 线格式，字段与前端 lightweight-charts / tv-chart datafeed 契约完全一致。
type Bar struct {
	Time   int64   `json:"time"` // unix 秒
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`
}

// Source 是每个上游行情数据源（Binance / Tencent / BiQuote ...）需要实现的适配器接口。
// 新增数据源只需实现该接口并注册到 SourceRouter，不影响其它源。
type Source interface {
	Name() string
	// Match 判断该 symbol 是否归属本源（按前后缀规则）。
	Match(symbol string) bool
	GetBars(ctx context.Context, symbol, interval string, count int) ([]Bar, error)
	// Subscribe 建立到上游的实时订阅，每收到一根新/更新的 bar 就调用 onBar。
	// 返回的 stop 用于关闭这条上游连接。Subscribe 只关心与上游的单条连接，
	// 多下游客户端的去重/引用计数由 Hub 负责，Source 不感知下游客户端数量。
	Subscribe(ctx context.Context, symbol, interval string, onBar func(Bar)) (stop func(), err error)
}
