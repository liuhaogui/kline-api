package kline

import (
	"context"
	"testing"
)

func TestTVSessions(t *testing.T) {
	regular, err := WithTVSession(NewTradingViewSource(), "NASDAQ:AAPL", "regular")
	if err != nil {
		t.Fatal(err)
	}
	extended, err := WithTVSession(NewTradingViewSource(), "NASDAQ:AAPL", "extended")
	if err != nil {
		t.Fatal(err)
	}
	if upstreamKey(regular, "NASDAQ:AAPL", "1m") == upstreamKey(extended, "NASDAQ:AAPL", "1m") {
		t.Fatal("sessions share subscription")
	}
	for _, session := range []string{"", "regular", "extended"} {
		obj := tvResolveObject(context.WithValue(context.Background(), tvSessionKey{}, session), "NASDAQ:AAPL")
		if obj["session"] != session {
			t.Fatal(obj)
		}
	}
	if _, err := WithTVSession(NewTradingViewSource(), "BINANCE:BTCUSDT", "extended"); err == nil {
		t.Fatal("accepted crypto session")
	}
	if ValidateTVSession("overnight") == nil {
		t.Fatal("accepted unsupported session")
	}
}
