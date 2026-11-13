package kline

import (
	"context"
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const lingerDuration = 10 * time.Second

type hubClient struct{ onBar func(Bar) }

// All upstream state is protected by Hub.mu. ready publishes initialization to
// concurrent subscribers; an empty client list during linger still owns one feed.
type upstream struct {
	ctx         context.Context
	cancel      context.CancelFunc
	clients     map[string]*hubClient
	ready       chan struct{}
	err         error
	stopUpst    func()
	lingerTimer *time.Timer
	generation  uint64
	lastBar     *Bar
}

type Hub struct {
	closed    bool
	mu        sync.Mutex
	upstreams map[string]*upstream
	linger    time.Duration
}

func NewHub() *Hub { return &Hub{upstreams: make(map[string]*upstream), linger: lingerDuration} }
func upstreamKey(source Source, symbol, interval string) string {
	return source.Name() + "|" + symbol + "|" + interval
}

func (h *Hub) Subscribe(source Source, symbol, interval, clientID string, onBar func(Bar)) (func(), error) {
	key := upstreamKey(source, symbol, interval)
	client := &hubClient{onBar: onBar}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, errors.New("kline hub closed")
	}
	up, exists := h.upstreams[key]
	if !exists {
		ctx, cancel := context.WithCancel(context.Background())
		up = &upstream{ctx: ctx, cancel: cancel, clients: make(map[string]*hubClient), ready: make(chan struct{})}
		h.upstreams[key] = up
	}
	up.generation++
	if up.lingerTimer != nil {
		up.lingerTimer.Stop()
		up.lingerTimer = nil
	}
	up.clients[clientID] = client
	h.mu.Unlock()

	if !exists {
		ctx, cancel := up.ctx, up.cancel
		stop, err := source.Subscribe(ctx, symbol, interval, func(bar Bar) {
			h.mu.Lock()
			if h.upstreams[key] != up {
				h.mu.Unlock()
				return
			}
			up.lastBar = &bar
			select {
			case <-up.ready:
			default:
				h.mu.Unlock()
				return // replay the synchronous initial bar after publication
			}
			callbacks := make([]func(Bar), 0, len(up.clients))
			for _, c := range up.clients {
				callbacks = append(callbacks, c.onBar)
			}
			h.mu.Unlock()
			for _, cb := range callbacks {
				cb(bar)
			}
		})
		h.mu.Lock()
		if h.closed && err == nil {
			err = errors.New("kline hub closed")
		}
		up.err = err
		if err != nil {
			if h.upstreams[key] == up {
				delete(h.upstreams, key)
			}
		} else {
			var once sync.Once
			up.stopUpst = func() {
				once.Do(func() {
					cancel()
					if stop != nil {
						stop()
					}
				})
			}
		}
		close(up.ready)
		h.mu.Unlock()
		if err != nil {
			cancel()
			if stop != nil {
				stop()
			}
			return nil, err
		}
		log.Infof("kline hub: opened upstream %s", key)
	} else {
		<-up.ready
		if up.err != nil {
			return nil, up.err
		}
	}
	h.mu.Lock()
	last := up.lastBar
	h.mu.Unlock()
	if last != nil {
		onBar(*last)
	}
	var once sync.Once
	return func() { once.Do(func() { h.unsubscribe(key, up, clientID, client) }) }, nil
}

func (h *Hub) unsubscribe(key string, up *upstream, clientID string, client *hubClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.upstreams[key] != up || up.clients[clientID] != client {
		return
	}
	delete(up.clients, clientID)
	if len(up.clients) != 0 {
		return
	}
	up.generation++
	generation := up.generation
	up.lingerTimer = time.AfterFunc(h.linger, func() {
		h.mu.Lock()
		if h.upstreams[key] != up || up.generation != generation || len(up.clients) != 0 {
			h.mu.Unlock()
			return
		}
		delete(h.upstreams, key)
		stop := up.stopUpst
		h.mu.Unlock()
		if stop != nil {
			stop()
		}
		log.Infof("kline hub: closed upstream %s (idle)", key)
	})
}

// Close cancels active and initializing upstreams without waiting for the linger timer.
// A source which finishes initialization after Close is stopped on publication.
func (h *Hub) Close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	stops := make([]func(), 0, len(h.upstreams)*2)
	for _, up := range h.upstreams {
		if up.lingerTimer != nil {
			up.lingerTimer.Stop()
		}
		stops = append(stops, up.cancel)
		if up.stopUpst != nil {
			stops = append(stops, up.stopUpst)
		}
	}
	h.upstreams = make(map[string]*upstream)
	h.mu.Unlock()
	for _, stop := range stops {
		stop()
	}
}
