package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/liuhaogui/kline-api/internal/config"
	"github.com/liuhaogui/kline-api/internal/httpapi"
	"github.com/sirupsen/logrus"
)

var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	log := logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{})
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	level, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		log.Fatal("invalid LOG_LEVEL: ", err)
	}
	log.SetLevel(level)
	logrus.SetLevel(level)
	logrus.SetFormatter(&logrus.JSONFormatter{})
	gin.SetMode(gin.ReleaseMode)
	app := httpapi.New(cfg, log, version)
	defer app.Close()
	server := &http.Server{Addr: cfg.ListenAddr, Handler: app.Handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: cfg.RequestTimeout + 5*time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	failed := make(chan error, 1)
	go func() { failed <- server.ListenAndServe() }()
	log.WithFields(logrus.Fields{"listen": cfg.ListenAddr, "api_prefix": cfg.APIPrefix, "version": version}).Info("kline API started")
	select {
	case err := <-failed:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
		app.Close()
		shutdown, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			server.Close()
			log.WithError(err).Warn("forced shutdown")
		}
	}
}
