package kline

import (
	"context"
	"testing"
	"time"
)

func TestHubCloseCancelsInitializingSource(t *testing.T) {
	h := NewHub()
	s := &lifecycleSource{}
	entered := make(chan struct{})
	s.start = func(ctx context.Context, _ func(Bar)) error { close(entered); <-ctx.Done(); return ctx.Err() }
	done := make(chan error, 1)
	go func() { _, err := h.Subscribe(s, "A", "1D", "one", func(Bar) {}); done <- err }()
	<-entered
	h.Close()
	h.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("accepted closed hub")
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel initialization")
	}
	if _, err := h.Subscribe(s, "A", "1D", "two", func(Bar) {}); err == nil {
		t.Fatal("closed hub accepted subscriber")
	}
}
