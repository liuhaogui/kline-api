package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config contains only public service settings. No database or account is needed.
type Config struct {
	ListenAddr       string
	APIPrefix        string
	CORSOrigins      []string
	LogLevel         string
	RequestTimeout   time.Duration
	ShutdownTimeout  time.Duration
	MaxConnections   int
	MaxSubscriptions int
}

func Load() (Config, error) { return load(os.LookupEnv) }

func load(lookup func(string) (string, bool)) (Config, error) {
	get := func(key, fallback string) string {
		if value, ok := lookup(key); ok {
			return strings.TrimSpace(value)
		}
		return fallback
	}
	c := Config{ListenAddr: get("LISTEN_ADDR", ":8082"), APIPrefix: strings.TrimRight(get("API_PREFIX", "/trade/api/v1"), "/"), LogLevel: get("LOG_LEVEL", "info")}
	if _, _, err := net.SplitHostPort(c.ListenAddr); err != nil {
		return c, fmt.Errorf("LISTEN_ADDR: %w", err)
	}
	if !strings.HasPrefix(c.APIPrefix, "/") || strings.ContainsAny(c.APIPrefix, "?# ") || strings.Contains(c.APIPrefix, "//") || strings.Contains(c.APIPrefix, "..") || c.APIPrefix == "" {
		return c, fmt.Errorf("API_PREFIX must be an absolute URL path, e.g. /trade/api/v1")
	}
	for _, origin := range strings.Split(get("CORS_ORIGINS", "*"), ",") {
		if origin = strings.TrimSpace(origin); origin != "" {
			c.CORSOrigins = append(c.CORSOrigins, origin)
		}
	}
	if len(c.CORSOrigins) == 0 {
		return c, fmt.Errorf("CORS_ORIGINS must not be empty")
	}
	var err error
	if c.RequestTimeout, err = time.ParseDuration(get("REQUEST_TIMEOUT", "12s")); err != nil || c.RequestTimeout <= 0 {
		return c, fmt.Errorf("REQUEST_TIMEOUT must be a positive duration")
	}
	if c.ShutdownTimeout, err = time.ParseDuration(get("SHUTDOWN_TIMEOUT", "15s")); err != nil || c.ShutdownTimeout <= 0 {
		return c, fmt.Errorf("SHUTDOWN_TIMEOUT must be a positive duration")
	}
	if c.MaxConnections, err = strconv.Atoi(get("MAX_WS_CONNECTIONS", "256")); err != nil || c.MaxConnections <= 0 {
		return c, fmt.Errorf("MAX_WS_CONNECTIONS must be a positive integer")
	}
	if c.MaxSubscriptions, err = strconv.Atoi(get("MAX_WS_SUBSCRIPTIONS", "32")); err != nil || c.MaxSubscriptions <= 0 {
		return c, fmt.Errorf("MAX_WS_SUBSCRIPTIONS must be a positive integer")
	}
	return c, nil
}
