package kline

import (
	"context"
	"fmt"
	"strings"
)

type tvSessionKey struct{}

func ValidateTVSession(session string) error {
	if session != "" && session != "regular" && session != "extended" {
		return fmt.Errorf("unsupported trading session")
	}
	return nil
}
func tvResolveObject(ctx context.Context, symbol string) map[string]string {
	obj := map[string]string{"symbol": symbol, "adjustment": "splits"}
	if session, ok := ctx.Value(tvSessionKey{}).(string); ok && session != "" {
		obj["session"] = session
	}
	return obj
}

type sessionSource struct {
	Source
	session string
}

func (s sessionSource) Name() string { return s.Source.Name() + ":" + s.session }
func (s sessionSource) GetBars(ctx context.Context, symbol, interval string, count int) ([]Bar, error) {
	return s.Source.GetBars(context.WithValue(ctx, tvSessionKey{}, s.session), symbol, interval, count)
}
func (s sessionSource) Subscribe(ctx context.Context, symbol, interval string, onBar func(Bar)) (func(), error) {
	return s.Source.Subscribe(context.WithValue(ctx, tvSessionKey{}, s.session), symbol, interval, onBar)
}
func WithTVSession(source Source, symbol, session string) (Source, error) {
	if err := ValidateTVSession(session); err != nil {
		return nil, err
	}
	if session == "" {
		return source, nil
	}
	prefix := strings.ToUpper(strings.SplitN(symbol, ":", 2)[0])
	if prefix != "NASDAQ" && prefix != "NYSE" && prefix != "AMEX" && prefix != "NYSEARCA" {
		return nil, fmt.Errorf("trading sessions require a US equity symbol")
	}
	return sessionSource{source, session}, nil
}
