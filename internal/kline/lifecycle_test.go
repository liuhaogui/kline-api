package kline

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type lifecycleSource struct {
	starts atomic.Int64
	stops  atomic.Int64
	start  func(context.Context, func(Bar)) error
	stop   func()
}

func (s *lifecycleSource) Name() string      { return "lifecycle-test" }
func (s *lifecycleSource) Match(string) bool { return true }
func (s *lifecycleSource) GetBars(context.Context, string, string, int) ([]Bar, error) {
	return nil, nil
}
func (s *lifecycleSource) Subscribe(ctx context.Context, _ string, _ string, cb func(Bar)) (func(), error) {
	s.starts.Add(1)
	if s.start != nil {
		if err := s.start(ctx, cb); err != nil {
			return nil, err
		}
	}
	return func() {
		s.stops.Add(1)
		if s.stop != nil {
			s.stop()
		}
	}, nil
}
func waitLifecycle(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("lifecycle condition did not complete")
		}
		time.Sleep(time.Millisecond)
	}
}
func shortLinger(h *Hub) { h.mu.Lock(); h.linger = time.Millisecond; h.mu.Unlock() }

func TestHubReusesUpstreamDuringLinger(t *testing.T) {
	h := NewHub()
	h.linger = time.Hour
	s := &lifecycleSource{}
	first, err := h.Subscribe(s, "A", "1D", "one", func(Bar) {})
	if err != nil {
		t.Fatal(err)
	}
	first()
	second, err := h.Subscribe(s, "A", "1D", "two", func(Bar) {})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.starts.Load(); got != 1 {
		t.Fatalf("linger reopened upstream: %d", got)
	}
	shortLinger(h)
	second()
	second()
	waitLifecycle(t, func() bool { return s.stops.Load() == 1 })
}
func TestHubConcurrentInitializationAndFailure(t *testing.T) {
	h := NewHub()
	s := &lifecycleSource{}
	entered := make(chan struct{})
	release := make(chan struct{})
	failure := errors.New("upstream unavailable")
	s.start = func(context.Context, func(Bar)) error { close(entered); <-release; return failure }
	results := make(chan error, 2)
	go func() { _, err := h.Subscribe(s, "A", "1D", "one", func(Bar) {}); results <- err }()
	<-entered
	go func() { _, err := h.Subscribe(s, "A", "1D", "two", func(Bar) {}); results <- err }()
	waitLifecycle(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.upstreams[upstreamKey(s, "A", "1D")].clients) == 2
	})
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-results; !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}
	if s.starts.Load() != 1 {
		t.Fatal("duplicate concurrent upstream")
	}
	h.mu.Lock()
	left := len(h.upstreams)
	h.mu.Unlock()
	if left != 0 {
		t.Fatal("failed initialization retained hub entry")
	}
}
func TestHubStaleUnsubscribeDoesNotRemoveReplacementClient(t *testing.T) {
	h := NewHub()
	shortLinger(h)
	s := &lifecycleSource{}
	first, _ := h.Subscribe(s, "A", "1D", "same", func(Bar) {})
	second, _ := h.Subscribe(s, "A", "1D", "same", func(Bar) {})
	first()
	h.mu.Lock()
	clients := len(h.upstreams[upstreamKey(s, "A", "1D")].clients)
	h.mu.Unlock()
	if clients != 1 {
		t.Fatal("stale unsubscribe removed replacement")
	}
	second()
	waitLifecycle(t, func() bool { return s.stops.Load() == 1 })
}
func TestHubIdleCleanupCannotDeleteNewGeneration(t *testing.T) {
	h := NewHub()
	shortLinger(h)
	entered := make(chan struct{})
	release := make(chan struct{})
	s := &lifecycleSource{}
	s.stop = func() {
		if s.stops.Load() == 1 {
			close(entered)
			<-release
		}
	}
	first, _ := h.Subscribe(s, "A", "1D", "one", func(Bar) {})
	first()
	<-entered
	second, err := h.Subscribe(s, "A", "1D", "two", func(Bar) {})
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	h.mu.Lock()
	clients := len(h.upstreams[upstreamKey(s, "A", "1D")].clients)
	h.mu.Unlock()
	if clients != 1 || s.starts.Load() != 2 {
		t.Fatal("old timer removed new upstream")
	}
	second()
	waitLifecycle(t, func() bool { return s.stops.Load() == 2 })
}
func TestHubConcurrentChurnClosesEveryUpstream(t *testing.T) {
	h := NewHub()
	shortLinger(h)
	s := &lifecycleSource{}
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				stop, err := h.Subscribe(s, "A", "1D", strings.Repeat("x", n+1), func(Bar) {})
				if err != nil {
					t.Error(err)
					return
				}
				stop()
			}
		}(worker)
	}
	wg.Wait()
	waitLifecycle(t, func() bool { return s.stops.Load() == s.starts.Load() })
	h.mu.Lock()
	left := len(h.upstreams)
	h.mu.Unlock()
	if left != 0 {
		t.Fatalf("retained %d upstreams", left)
	}
}
func readLoopWatchers() int {
	stack := make([]byte, 2<<20)
	n := runtime.Stack(stack, true)
	return strings.Count(string(stack[:n]), "kline.readLoop.func1(")
}
func TestReadLoopReconnectDoesNotRetainWatchers(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn.Close()
	}))
	defer server.Close()
	done := make(chan struct{})
	defer close(done)
	before := readLoopWatchers()
	for i := 0; i < 80; i++ {
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		readLoop(conn, done, func([]byte) {})
		conn.Close()
	}
	if after := readLoopWatchers(); after > before {
		t.Fatalf("readLoop leaked cancellation watchers: before=%d after=%d", before, after)
	}
}
func TestReadLoopCancellationClosesBlockedRead(t *testing.T) {
	upgrader := websocket.Upgrader{}
	peerClosed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.ReadMessage()
		close(peerClosed)
	}))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() { readLoop(conn, done, func([]byte) {}); close(finished) }()
	close(done)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("read remained blocked after cancel")
	}
	select {
	case <-peerClosed:
	case <-time.After(time.Second):
		t.Fatal("peer connection was not closed")
	}
}

func TestHubSynchronousInitialBarAllowsReentrantSubscription(t *testing.T) {
	h := NewHub()
	shortLinger(h)
	s := &lifecycleSource{}
	var sourceContext context.Context
	s.start = func(ctx context.Context, cb func(Bar)) error { sourceContext = ctx; cb(Bar{Time: 1}); return nil }
	nested := make(chan func(), 1)
	finished := make(chan func(), 1)
	go func() {
		stop, err := h.Subscribe(s, "A", "1D", "one", func(Bar) {
			stop, err := h.Subscribe(s, "A", "1D", "two", func(Bar) {})
			if err == nil {
				nested <- stop
			}
		})
		if err == nil {
			finished <- stop
		}
	}()
	var first, second func()
	select {
	case first = <-finished:
	case <-time.After(time.Second):
		t.Fatal("synchronous initial bar deadlocked subscription")
	}
	select {
	case second = <-nested:
	case <-time.After(time.Second):
		t.Fatal("missing nested subscription")
	}
	first()
	second()
	waitLifecycle(t, func() bool { return s.stops.Load() == 1 })
	if sourceContext.Err() != context.Canceled {
		t.Fatal("upstream context was not cancelled")
	}
}
