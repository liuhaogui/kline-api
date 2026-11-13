package kline

import (
	"fmt"
	"regexp"
)

var usdtSuffix = regexp.MustCompile(`(?i)USDT$`)
var commodityPrefix = regexp.MustCompile(`^hf_`)

// SourceRouter 按 symbol 前后缀选择数据源，规则与前端 multiDatafeed 保持一致：
//
//	*USDT  → Binance
//	hf_*   → BiQuote（商品）
//	其它    → Tencent（A股/港股/指数）
type SourceRouter struct {
	sources []Source
}

func NewSourceRouter(sources ...Source) *SourceRouter {
	return &SourceRouter{sources: sources}
}

func (r *SourceRouter) Route(symbol string) (Source, error) {
	for _, s := range r.sources {
		if s.Match(symbol) {
			return s, nil
		}
	}
	return nil, fmt.Errorf("no data source matched for symbol %q", symbol)
}

// 供各 adapter 复用的通用前缀判断
func IsCrypto(symbol string) bool    { return usdtSuffix.MatchString(symbol) }
func IsCommodity(symbol string) bool { return commodityPrefix.MatchString(symbol) }
