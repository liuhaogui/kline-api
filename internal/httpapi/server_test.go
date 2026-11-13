package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/liuhaogui/kline-api/internal/config"
	"github.com/liuhaogui/kline-api/internal/kline"
	"github.com/sirupsen/logrus"
)

type fakeSource struct {
	count  atomic.Int64
	starts atomic.Int64
	stops  atomic.Int64
	fail   bool
	wait   bool
}

func (s *fakeSource) Name() string             { return "fake" }
func (s *fakeSource) Match(symbol string) bool { return symbol != "unsupported" }
func (s *fakeSource) GetBars(ctx context.Context, _, _ string, count int) ([]kline.Bar, error) {
	s.count.Store(int64(count))
	if s.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if s.fail {
		return nil, errors.New("upstream offline")
	}
	return []kline.Bar{{Time: 1700000000, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 10}}, nil
}
func (s *fakeSource) SearchSymbols(context.Context, string, int) ([]kline.SearchResult, error) {
	if s.fail {
		return nil, errors.New("upstream offline")
	}
	return []kline.SearchResult{{Symbol: "NASDAQ:AAPL", Description: "Apple", Exchange: "NASDAQ", Type: "stock"}}, nil
}
func (s *fakeSource) Subscribe(_ context.Context, _, _ string, cb func(kline.Bar)) (func(), error) {
	s.starts.Add(1)
	cb(kline.Bar{Time: 1700000000, Close: 1.5})
	return func() { s.stops.Add(1) }, nil
}
func testServer(t *testing.T, source *fakeSource, change func(*config.Config)) (*Server, *httptest.Server) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if change != nil {
		change(&cfg)
	}
	log := logrus.New()
	log.SetOutput(io.Discard)
	app := newServer(cfg, log, "test", kline.NewSourceRouter(source), source)
	server := httptest.NewServer(app.Handler)
	t.Cleanup(func() { app.Close(); server.Close() })
	return app, server
}
func get(t *testing.T, server *httptest.Server, path string) (*http.Response, map[string]any) {
	t.Helper()
	res, err := server.Client().Get(server.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body map[string]any
	if err = json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return res, body
}
func TestHTTPCompatibility(t *testing.T) {
	source := &fakeSource{}
	_, server := testServer(t, source, nil)
	for _, prefix := range []string{"/trade/api/v1", "/api/v1"} {
		for _, route := range []string{"/kline/bars", "/kline/tv/bars"} {
			res, body := get(t, server, prefix+route+"?symbol=NASDAQ:AAPL&interval=1D&count=99999")
			if res.StatusCode != 200 || body["code"] != float64(0) || len(body["list"].([]any)) != 1 || source.count.Load() != 1000 {
				t.Fatalf("%v %+v", res.Status, body)
			}
			_, body = get(t, server, prefix+route)
			if body["code"] != float64(400) {
				t.Fatal(body)
			}
		}
	}
	for _, session := range []string{"regular", "extended"} {
		_, body := get(t, server, "/trade/api/v1/kline/tv/bars?symbol=NASDAQ:AAPL&interval=1D&session="+session)
		if body["session"] != session || body["code"] != float64(0) {
			t.Fatal(body)
		}
	}
	for _, query := range []string{"symbol=TVC:USOIL&interval=1D&session=extended", "symbol=NASDAQ:AAPL&interval=1D&session=overnight", "symbol=unsupported&interval=1D"} {
		_, body := get(t, server, "/trade/api/v1/kline/tv/bars?"+query)
		if body["code"] != float64(400) {
			t.Fatal(body)
		}
	}
	_, body := get(t, server, "/trade/api/v1/kline/tv/search?text=apple")
	if body["code"] != float64(0) || len(body["list"].([]any)) != 1 {
		t.Fatal(body)
	}
	_, body = get(t, server, "/trade/api/v1/kline/tv/search")
	if len(body["list"].([]any)) != 0 {
		t.Fatal(body)
	}
	_, body = get(t, server, "/healthz")
	if body["status"] != "ok" || body["version"] != "test" {
		t.Fatal(body)
	}
}
func TestUpstreamFailureAndBudget(t *testing.T) {
	for _, source := range []*fakeSource{{fail: true}, {wait: true}} {
		_, server := testServer(t, source, func(c *config.Config) { c.RequestTimeout = 20 * time.Millisecond })
		for _, route := range []string{"/kline/bars", "/kline/tv/bars"} {
			res, body := get(t, server, "/trade/api/v1"+route+"?symbol=NASDAQ:AAPL&interval=1D")
			if res.StatusCode != 200 || body["code"] != float64(500) {
				t.Fatal(body)
			}
		}
	}
}
func TestCORSAndCustomPrefix(t *testing.T) {
	_, server := testServer(t, &fakeSource{}, func(c *config.Config) {
		c.APIPrefix = "/custom/api/v1"
		c.CORSOrigins = []string{"https://example.com"}
	})
	for _, origin := range []string{"https://example.com", "https://other.invalid"} {
		req, _ := http.NewRequest("OPTIONS", server.URL+"/custom/api/v1/kline/tv/bars", nil)
		req.Header.Set("Origin", origin)
		res, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if origin == "https://example.com" {
			if res.StatusCode != 204 || res.Header.Get("Access-Control-Allow-Origin") != origin {
				t.Fatal(res)
			}
		} else if res.Header.Get("Access-Control-Allow-Origin") != "" {
			t.Fatal("untrusted origin accepted")
		}
	}
	res, body := get(t, server, "/custom/api/v1/kline/tv/bars?symbol=NASDAQ:AAPL&interval=1D")
	if res.StatusCode != 200 || body["code"] != float64(0) {
		t.Fatal(body)
	}
	_, body = get(t, server, "/missing")
	if body["code"] != float64(404) {
		t.Fatal(body)
	}
}
func readWS(t *testing.T, c *websocket.Conn) klineWSOutMsg {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(time.Second))
	var msg klineWSOutMsg
	if err := c.ReadJSON(&msg); err != nil {
		t.Fatal(err)
	}
	return msg
}
func TestWebsocketLifecycleLimitsAndShutdown(t *testing.T) {
	source := &fakeSource{}
	app, server := testServer(t, source, func(c *config.Config) { c.MaxConnections = 1; c.MaxSubscriptions = 1 })
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/trade/api/v1/kline/tv/ws?session=extended"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.WriteJSON(klineWSSubscribeMsg{Action: "subscribe", Symbol: "NASDAQ:AAPL", Interval: "1D"})
	first, second := readWS(t, conn), readWS(t, conn)
	if first.Type != "bar" || first.Session != "extended" || second.Type != "subscribed" {
		t.Fatalf("%+v %+v", first, second)
	}
	conn.WriteJSON(klineWSSubscribeMsg{Action: "subscribe", Symbol: "NYSE:IBM", Interval: "1D"})
	if readWS(t, conn).Type != "error" || source.starts.Load() != 1 {
		t.Fatal("subscription limit ignored")
	}
	if extra, res, err := websocket.DefaultDialer.Dial(url, nil); err == nil {
		extra.Close()
		t.Fatal("connection limit ignored")
	} else if res != nil {
		res.Body.Close()
	}
	conn.WriteJSON(klineWSSubscribeMsg{Action: "unsubscribe", Symbol: "NASDAQ:AAPL", Interval: "1D"})
	if readWS(t, conn).Type != "unsubscribed" {
		t.Fatal("unsubscribe not acknowledged")
	}
	app.Close()
	app.Close()
	if source.stops.Load() != 1 {
		t.Fatal("shutdown retained upstream")
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("shutdown kept websocket open")
	}
}
