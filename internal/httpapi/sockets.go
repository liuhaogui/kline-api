package httpapi

import (
	"fmt"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/liuhaogui/kline-api/internal/response"
)

// Reserve before upgrading so concurrent handshakes cannot exceed the limit.
type socketPool struct {
	mu               sync.Mutex
	connections      map[*websocket.Conn]struct{}
	reserved         int
	closed           bool
	limit            int
	maxSubscriptions int
}

func newSocketPool(limit, subscriptions int) *socketPool {
	return &socketPool{connections: make(map[*websocket.Conn]struct{}), limit: limit, maxSubscriptions: subscriptions}
}

func (p *socketPool) upgrade(c *gin.Context) (*websocket.Conn, func(), error) {
	p.mu.Lock()
	if p.closed || p.reserved >= p.limit {
		p.mu.Unlock()
		response.API(c, 500, gin.H{"message": "实时连接繁忙，请稍后重试"})
		return nil, nil, fmt.Errorf("websocket capacity unavailable")
	}
	p.reserved++
	p.mu.Unlock()
	conn, err := klineWSUpgrader.Upgrade(c.Writer, c.Request, nil)
	p.mu.Lock()
	if err != nil {
		p.reserved--
		p.mu.Unlock()
		return nil, nil, err
	}
	if p.closed {
		p.reserved--
		p.mu.Unlock()
		conn.Close()
		return nil, nil, fmt.Errorf("server shutting down")
	}
	p.connections[conn] = struct{}{}
	p.mu.Unlock()
	var once sync.Once
	return conn, func() {
		once.Do(func() {
			conn.Close()
			p.mu.Lock()
			delete(p.connections, conn)
			p.reserved--
			p.mu.Unlock()
		})
	}, nil
}

func (p *socketPool) close() {
	p.mu.Lock()
	p.closed = true
	connections := make([]*websocket.Conn, 0, len(p.connections))
	for conn := range p.connections {
		connections = append(connections, conn)
	}
	p.mu.Unlock()
	for _, conn := range connections {
		conn.Close()
	}
}
