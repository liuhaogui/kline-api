package config

import (
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	c, err := load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if c.ListenAddr != ":8082" || c.APIPrefix != "/trade/api/v1" || c.RequestTimeout != 12*time.Second || c.MaxConnections != 256 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestOverridesAndValidation(t *testing.T) {
	values := map[string]string{"LISTEN_ADDR": "127.0.0.1:9090", "API_PREFIX": "/custom/api/v1/", "CORS_ORIGINS": "https://example.com, chrome-extension://example", "REQUEST_TIMEOUT": "20s"}
	c, err := load(func(key string) (string, bool) { v, ok := values[key]; return v, ok })
	if err != nil || c.APIPrefix != "/custom/api/v1" || len(c.CORSOrigins) != 2 || c.RequestTimeout != 20*time.Second {
		t.Fatalf("%+v %v", c, err)
	}
	for _, item := range []struct{ key, value string }{
		{"LISTEN_ADDR", "bad"}, {"API_PREFIX", "relative"}, {"API_PREFIX", "/"}, {"API_PREFIX", "/a//b"}, {"API_PREFIX", "/a/../b"},
		{"REQUEST_TIMEOUT", "bad"}, {"REQUEST_TIMEOUT", "0s"}, {"SHUTDOWN_TIMEOUT", "-1s"}, {"MAX_WS_CONNECTIONS", "0"}, {"MAX_WS_SUBSCRIPTIONS", "bad"}, {"CORS_ORIGINS", ","},
	} {
		t.Run(item.key+item.value, func(t *testing.T) {
			_, err := load(func(key string) (string, bool) {
				if key == item.key {
					return item.value, true
				}
				return "", false
			})
			if err == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
}
