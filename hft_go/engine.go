package main

import (
	"sync"
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────
//  UserSession — one SPSC queue per user
// ─────────────────────────────────────────────

type UserSession struct {
	ID          UserID
	Inbound     SPSCQueue
	nextOrderID atomic.Uint64
}

func NewSession(id UserID) *UserSession {
	s := &UserSession{ID: id}
	s.nextOrderID.Store(1)
	return s
}

func (s *UserSession) NewOrderID() OrderID {
	// pack UserID into high 32 bits
	seq := s.nextOrderID.Add(1)
	return OrderID(uint64(s.ID)<<32 | seq)
}

// ─────────────────────────────────────────────
//  TradingEngine — single matching goroutine
// ─────────────────────────────────────────────

type TradingEngine struct {
	books   map[string]*OrderBook
	mu      sync.RWMutex
	sessions []*UserSession
	sessMu  sync.RWMutex

	metrics *Metrics
	logger  *AsyncLogger

	running  atomic.Bool
	stopPerf chan struct{}
}

func NewTradingEngine(logPath string, perfIntervalMs int) (*TradingEngine, error) {
	logger, err := NewAsyncLogger(logPath)
	if err != nil {
		return nil, err
	}
	m := NewMetrics()
	e := &TradingEngine{
		books:    make(map[string]*OrderBook),
		metrics:  m,
		logger:   logger,
		stopPerf: make(chan struct{}),
	}
	startPerfReporter(logger, m, perfIntervalMs, e.stopPerf)
	return e, nil
}

func (e *TradingEngine) AddSymbol(sym string) {
	cb := func(t Trade) { e.logger.LogTrade(t) }
	e.mu.Lock()
	e.books[sym] = NewOrderBook(sym, e.metrics, cb)
	e.mu.Unlock()
	e.logger.Log(LvlInfo, "Symbol '%s' added to engine", sym)
}

func (e *TradingEngine) CreateSession(uid UserID) *UserSession {
	sess := NewSession(uid)
	e.sessMu.Lock()
	e.sessions = append(e.sessions, sess)
	n := len(e.sessions)
	e.sessMu.Unlock()
	e.logger.Log(LvlInfo, "Session created for user %d  (total: %d)", uid, n)
	return sess
}

func (e *TradingEngine) Start() {
	e.running.Store(true)
	go e.engineLoop()
	e.logger.Log(LvlInfo, "Engine started — %d symbols", len(e.books))
}

func (e *TradingEngine) Stop() {
	e.running.Store(false)
	close(e.stopPerf)
	e.logger.Log(LvlInfo, "Engine stopped")
	e.logger.LogPerf(e.metrics)
	// flush logger
	time.Sleep(50 * time.Millisecond)
	e.logger.Close()
}

// ── matching loop ─────────────────────────────

func (e *TradingEngine) engineLoop() {
	for e.running.Load() {
		idle := true

		e.sessMu.RLock()
		sessions := e.sessions
		e.sessMu.RUnlock()

		for _, sess := range sessions {
			o, ok := sess.Inbound.Pop()
			if !ok {
				continue
			}
			idle = false
			e.dispatch(o)
		}

		if idle {
			time.Sleep(1 * time.Microsecond)
		}
	}
}

func (e *TradingEngine) dispatch(o Order) {
	e.mu.RLock()
	book, ok := e.books[o.Symbol]
	e.mu.RUnlock()

	if !ok {
		e.logger.Log(LvlWarn, "Unknown symbol '%s' in order #%d – dropped", o.Symbol, o.ID)
		return
	}

	side := "BUY "
	if o.Side == Sell {
		side = "SELL"
	}
	e.logger.Log(LvlDebug, "ORDER side=%s sym=%s px=%d qty=%d id=%d user=%d",
		side, o.Symbol, o.Price, o.Qty, o.ID, o.UserID)

	book.AddOrder(o)

	bid, ask := book.Spread()
	e.logger.Log(LvlInfo, "BOOK  %s bid=%d ask=%d bdepth=%d adepth=%d",
		o.Symbol, bid, ask, book.BidDepth(), book.AskDepth())
}
