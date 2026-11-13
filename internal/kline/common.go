package kline

import (
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

func parseFloatStr(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// parseFloatAny 兼容 JSON 数字被解析成 float64/string 两种情况（Binance 部分字段是字符串）
func parseFloatAny(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case string:
		return parseFloatStr(t)
	default:
		return 0
	}
}

// sleepOrDone 睡眠 d，若期间 done 被关闭则提前返回 false（表示应停止重连循环）
func sleepOrDone(d time.Duration, done <-chan struct{}) bool {
	select {
	case <-time.After(d):
		return true
	case <-done:
		return false
	}
}

// readLoop 持续读取 WS 消息直到连接出错或 done 被关闭；每条消息回调 onMsg。
func readLoop(conn *websocket.Conn, done <-chan struct{}, onMsg func([]byte)) error {
	finished := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-done:
			conn.Close()
		case <-finished:
		}
	}()
	defer func() { close(finished); <-watcherDone }()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		onMsg(data)
	}
}
