package main

import (
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────
//  Core types
// ─────────────────────────────────────────────

type OrderID uint64
type UserID uint32
type Price int64   // fixed-point: cents
type Quantity uint32

type Side uint8

const (
	Buy  Side = 0
	Sell Side = 1
)

type OrderType uint8

const (
	Limit  OrderType = 0
	Market OrderType = 1
)

type OrderStatus uint8

const (
	StatusNew     OrderStatus = 0
	StatusPartial OrderStatus = 1
	StatusFilled  OrderStatus = 2
	StatusCancelled OrderStatus = 3
)

func nowNs() int64 {
	return time.Now().UnixNano()
}

// ─────────────────────────────────────────────
//  Order
// ─────────────────────────────────────────────

type Order struct {
	ID       OrderID
	UserID   UserID
	Price    Price
	Qty      Quantity
	Filled   Quantity
	Side     Side
	Type     OrderType
	Status   OrderStatus
	Symbol   string
	TsIn     int64
	TsFilled int64
}

func (o *Order) Remaining() Quantity { return o.Qty - o.Filled }

// ─────────────────────────────────────────────
//  Trade (execution report)
// ─────────────────────────────────────────────

type Trade struct {
	BuyID  OrderID
	SellID OrderID
	Price  Price
	Qty    Quantity
	Symbol string
	Ts     int64
}

// ─────────────────────────────────────────────
//  Lock-free SPSC ring buffer
//  Uses atomic head/tail; capacity must be power of 2
// ─────────────────────────────────────────────

const queueCap = 1024 // power of 2

type SPSCQueue struct {
	_    [64]byte // padding
	head atomic.Uint64
	_    [56]byte
	tail atomic.Uint64
	_    [56]byte
	buf  [queueCap]Order
}

func (q *SPSCQueue) Push(o Order) bool {
	h := q.head.Load()
	next := (h + 1) & (queueCap - 1)
	if next == q.tail.Load() {
		return false // full
	}
	q.buf[h] = o
	q.head.Store(next)
	return true
}

func (q *SPSCQueue) Pop() (Order, bool) {
	t := q.tail.Load()
	if t == q.head.Load() {
		return Order{}, false // empty
	}
	o := q.buf[t]
	q.tail.Store((t + 1) & (queueCap - 1))
	return o, true
}

func (q *SPSCQueue) Size() uint64 {
	h := q.head.Load()
	t := q.tail.Load()
	return (h - t) & (queueCap - 1)
}

// ─────────────────────────────────────────────
//  Lock-free performance metrics
// ─────────────────────────────────────────────

type Metrics struct {
	_               [64]byte
	OrdersReceived  atomic.Uint64
	OrdersMatched   atomic.Uint64
	OrdersCancelled atomic.Uint64
	TradesExecuted  atomic.Uint64
	TotalLatencyNs  atomic.Uint64
	MaxLatencyNs    atomic.Uint64
	MinLatencyNs    atomic.Uint64
}

func NewMetrics() *Metrics {
	m := &Metrics{}
	m.MinLatencyNs.Store(^uint64(0)) // max uint64
	return m
}

func (m *Metrics) RecordLatency(lat uint64) {
	m.TotalLatencyNs.Add(lat)
	m.OrdersMatched.Add(1)
	// CAS for max
	for {
		old := m.MaxLatencyNs.Load()
		if lat <= old || m.MaxLatencyNs.CompareAndSwap(old, lat) {
			break
		}
	}
	// CAS for min
	for {
		old := m.MinLatencyNs.Load()
		if lat >= old || m.MinLatencyNs.CompareAndSwap(old, lat) {
			break
		}
	}
}

func (m *Metrics) AvgLatencyUs() float64 {
	matched := m.OrdersMatched.Load()
	if matched == 0 {
		return 0
	}
	return float64(m.TotalLatencyNs.Load()) / float64(matched) / 1000.0
}
