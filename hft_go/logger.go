package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────
//  Log levels
// ─────────────────────────────────────────────

const (
	LvlDebug = 0
	LvlInfo  = 1
	LvlWarn  = 2
	LvlTrade = 3
	LvlPerf  = 4
)

var levelStr = []string{"DEBUG", "INFO ", "WARN ", "TRADE", "PERF "}
var levelColor = []string{"\033[0m", "\033[32m", "\033[33m", "\033[36m", "\033[35m"}

// ─────────────────────────────────────────────
//  Log entry
// ─────────────────────────────────────────────

type LogEntry struct {
	Msg   string
	Level uint8
	Ts    int64
}

// ─────────────────────────────────────────────
//  AsyncLogger — channel-backed, non-blocking hot path
// ─────────────────────────────────────────────

type AsyncLogger struct {
	ch      chan LogEntry
	file    *os.File
	running atomic.Bool
	done    chan struct{}
}

func NewAsyncLogger(path string) (*AsyncLogger, error) {
	if err := os.MkdirAll("logs", 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	l := &AsyncLogger{
		ch:   make(chan LogEntry, 16384),
		file: f,
		done: make(chan struct{}),
	}
	l.running.Store(true)
	go l.writerLoop()
	return l, nil
}

func (l *AsyncLogger) writerLoop() {
	defer close(l.done)
	for e := range l.ch {
		line := l.format(e)
		_, _ = l.file.WriteString(line)
		color := levelColor[e.Level]
		fmt.Printf("%s%s\033[0m", color, line)
	}
}

func (l *AsyncLogger) format(e LogEntry) string {
	ns := e.Ts
	sec := ns / 1_000_000_000
	ms := (ns % 1_000_000_000) / 1_000_000
	us := (ns % 1_000_000) / 1_000
	nano := ns % 1_000
	return fmt.Sprintf("[%s][%d.%03d.%03d.%03d] %s\n",
		levelStr[e.Level], sec, ms, us, nano, e.Msg)
}

func (l *AsyncLogger) Log(level uint8, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	select {
	case l.ch <- LogEntry{Msg: msg, Level: level, Ts: nowNs()}:
	default:
		// queue full — drop (rare in practice)
	}
}

func (l *AsyncLogger) LogTrade(t Trade) {
	l.Log(LvlTrade, "EXEC  sym=%s buy#%d sell#%d price=%d qty=%d",
		t.Symbol, t.BuyID, t.SellID, t.Price, t.Qty)
}

func (l *AsyncLogger) LogPerf(m *Metrics) {
	recv := m.OrdersReceived.Load()
	trd := m.TradesExecuted.Load()
	can := m.OrdersCancelled.Load()
	minL := m.MinLatencyNs.Load()
	maxL := m.MaxLatencyNs.Load()
	if minL == ^uint64(0) {
		minL = 0
	}
	l.Log(LvlPerf,
		"PERF  recv=%d trades=%d cancelled=%d avg_lat=%.2f µs  min=%d ns  max=%d ns",
		recv, trd, can, m.AvgLatencyUs(), minL, maxL)
}

func (l *AsyncLogger) Close() {
	close(l.ch)
	<-l.done
	_ = l.file.Close()
}

// ─────────────────────────────────────────────
//  Periodic perf reporter
// ─────────────────────────────────────────────

func startPerfReporter(logger *AsyncLogger, m *Metrics, intervalMs int, stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				logger.LogPerf(m)
			case <-stop:
				return
			}
		}
	}()
}
