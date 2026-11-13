package kline

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func mockTVSource(t *testing.T, handler http.HandlerFunc) *TradingViewSource {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	s := NewTradingViewSource()
	s.wsURL = "ws" + strings.TrimPrefix(server.URL, "http")
	return s
}
func readTVCommands(conn *websocket.Conn) (string, error) {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return "", err
		}
		for _, frame := range splitTVFrames(raw) {
			var msg struct {
				M string
				P []json.RawMessage
			}
			json.Unmarshal(frame, &msg)
			if msg.M == "create_series" {
				var session string
				json.Unmarshal(msg.P[0], &session)
				return session, nil
			}
		}
	}
}
func writeTVBars(conn *websocket.Conn, session string) error {
	return conn.WriteMessage(websocket.TextMessage, tvFrame("timescale_update", []interface{}{session, map[string]interface{}{"sds_1": map[string]interface{}{"s": []interface{}{map[string]interface{}{"v": []float64{1700000000, 1, 2, .5, 1.5, 10}}}}}}))
}
func TestTVHistoryRetriesTransientHandshakeAndKeepsStatus(t *testing.T) {
	var calls atomic.Int64
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	s := mockTVSource(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "busy", http.StatusTooManyRequests)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		session, err := readTVCommands(conn)
		if err == nil {
			writeTVBars(conn, session)
		}
	})
	bars, err := s.GetBars(context.Background(), "TVC:USOIL", "1D", 2)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || len(bars) != 1 || bars[0].Close != 1.5 {
		t.Fatalf("retry failed: calls=%d bars=%v", calls.Load(), bars)
	}
	denied := mockTVSource(t, func(w http.ResponseWriter, r *http.Request) { http.Error(w, "busy", http.StatusTooManyRequests) })
	_, err = denied.GetBars(context.Background(), "TVC:USOIL", "1D", 2)
	if err == nil || !strings.Contains(err.Error(), "HTTP 429") {
		t.Fatalf("lost upstream status: %v", err)
	}
}
func TestTVHistoryCancellationClosesUpstream(t *testing.T) {
	ready := make(chan struct{})
	peerClosed := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	s := mockTVSource(t, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := readTVCommands(conn); err != nil {
			return
		}
		close(ready)
		conn.ReadMessage()
		close(peerClosed)
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := s.GetBars(ctx, "TVC:USOIL", "1D", 2); result <- err }()
	<-ready
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("history did not cancel")
	}
	select {
	case <-peerClosed:
	case <-time.After(time.Second):
		t.Fatal("history left upstream open")
	}
}
func TestTVHistoryCancellationDuringHandshake(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	s := mockTVSource(t, func(w http.ResponseWriter, r *http.Request) { close(entered); <-release })
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := s.GetBars(ctx, "TVC:USOIL", "1D", 2); result <- err }()
	<-entered
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("dial did not honor cancellation")
	}
}
func TestTVHistoryConcurrencyBoundAndQueueCancellation(t *testing.T) {
	entered := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	s := mockTVSource(t, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := readTVCommands(conn); err != nil {
			return
		}
		close(entered)
		conn.ReadMessage()
	})
	s.historySlots = make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.GetBars(ctx, "TVC:USOIL", "1D", 2); done <- err }()
	<-entered
	queued, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	_, err := s.GetBars(queued, "TVC:USOIL", "1D", 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	<-done
	if len(s.historySlots) != 0 {
		t.Fatal("history slot leaked")
	}
}
func TestTVPermanentSymbolFailureIsNotRetried(t *testing.T) {
	var calls atomic.Int64
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	s := mockTVSource(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		session, err := readTVCommands(conn)
		if err == nil {
			conn.WriteMessage(websocket.TextMessage, tvFrame("symbol_error", []interface{}{session, "sds_sym_1", "invalid symbol"}))
		}
	})
	_, err := s.GetBars(context.Background(), "TVC:USOIL", "1D", 2)
	if !errors.Is(err, errTVSeries) || calls.Load() != 1 {
		t.Fatalf("permanent failure retried: %v calls=%d", err, calls.Load())
	}
}
func TestTVSubscriptionStopIsIdempotentAndClosesPeer(t *testing.T) {
	ready := make(chan struct{})
	closed := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	s := mockTVSource(t, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		session, err := readTVCommands(conn)
		if err != nil {
			return
		}
		writeTVBars(conn, session)
		close(ready)
		conn.ReadMessage()
		close(closed)
	})
	bars := make(chan Bar, 1)
	stop, err := s.Subscribe(context.Background(), "TVC:USOIL", "1D", func(bar Bar) { bars <- bar })
	if err != nil {
		t.Fatal(err)
	}
	<-ready
	select {
	case <-bars:
	case <-time.After(time.Second):
		t.Fatal("no subscription bar")
	}
	stop()
	stop()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("subscription stop left connection open")
	}
}
