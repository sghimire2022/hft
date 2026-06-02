use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::cell::UnsafeCell;

pub type OrderId  = u64;
pub type UserId   = u32;
pub type Price    = i64;
pub type Quantity = u32;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Side { Buy, Sell }

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum OrderType { Limit, Market }

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum OrderStatus { New, Partial, Filled, Cancelled }

pub fn now_ns() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
}

// ─────────────────────────────────────────────
//  Order
// ─────────────────────────────────────────────
#[derive(Clone, Debug)]
pub struct Order {
    pub id:         OrderId,
    pub user_id:    UserId,
    pub price:      Price,
    pub qty:        Quantity,
    pub filled:     Quantity,
    pub side:       Side,
    pub order_type: OrderType,
    pub status:     OrderStatus,
    pub symbol:     [u8; 8],
    pub ts_in:      i64,
    pub ts_filled:  i64,
}

impl Order {
    pub fn remaining(&self) -> Quantity { self.qty - self.filled }
    pub fn symbol_str(&self) -> &str {
        let end = self.symbol.iter().position(|&b| b == 0).unwrap_or(8);
        std::str::from_utf8(&self.symbol[..end]).unwrap_or("????")
    }
    pub fn make(sym: &str) -> [u8; 8] {
        let mut buf = [0u8; 8];
        let b = sym.as_bytes();
        let len = b.len().min(7);
        buf[..len].copy_from_slice(&b[..len]);
        buf
    }
}

// ─────────────────────────────────────────────
//  Trade
// ─────────────────────────────────────────────
#[derive(Clone, Debug)]
pub struct Trade {
    pub buy_id:  OrderId,
    pub sell_id: OrderId,
    pub price:   Price,
    pub qty:     Quantity,
    pub symbol:  [u8; 8],
    pub ts:      i64,
}

impl Trade {
    pub fn symbol_str(&self) -> &str {
        let end = self.symbol.iter().position(|&b| b == 0).unwrap_or(8);
        std::str::from_utf8(&self.symbol[..end]).unwrap_or("????")
    }
}

// ─────────────────────────────────────────────
//  Lock-free SPSC ring buffer
//  Uses UnsafeCell for interior mutability of the slot array,
//  with atomics on head/tail to synchronise producer/consumer.
// ─────────────────────────────────────────────
const QUEUE_CAP:  usize = 1024;
const QUEUE_MASK: u64   = (QUEUE_CAP - 1) as u64;

pub struct SPSCQueue {
    _pad0: [u8; 64],
    head:  AtomicU64,
    _pad1: [u8; 56],
    tail:  AtomicU64,
    _pad2: [u8; 56],
    buf:   Box<[UnsafeCell<Option<Order>>; QUEUE_CAP]>,
}

impl SPSCQueue {
    pub fn new() -> Self {
        SPSCQueue {
            _pad0: [0; 64],
            head:  AtomicU64::new(0),
            _pad1: [0; 56],
            tail:  AtomicU64::new(0),
            _pad2: [0; 56],
            buf:   Box::new(std::array::from_fn(|_| UnsafeCell::new(None))),
        }
    }

    /// Push from the producer thread. Returns false if full.
    pub fn push(&self, o: Order) -> bool {
        let h = self.head.load(Ordering::Relaxed);
        let next = (h + 1) & QUEUE_MASK;
        if next == self.tail.load(Ordering::Acquire) { return false; }
        // SAFETY: only producer accesses buf[h]
        unsafe { *self.buf[h as usize].get() = Some(o); }
        self.head.store(next, Ordering::Release);
        true
    }

    /// Pop from the consumer thread. Returns None if empty.
    pub fn pop(&self) -> Option<Order> {
        let t = self.tail.load(Ordering::Relaxed);
        if t == self.head.load(Ordering::Acquire) { return None; }
        // SAFETY: only consumer accesses buf[t]
        let o = unsafe { (*self.buf[t as usize].get()).take() };
        self.tail.store((t + 1) & QUEUE_MASK, Ordering::Release);
        o
    }
}

// SAFETY: designed for one producer + one consumer on separate threads
unsafe impl Send for SPSCQueue {}
unsafe impl Sync for SPSCQueue {}

// ─────────────────────────────────────────────
//  Lock-free metrics
// ─────────────────────────────────────────────
pub struct Metrics {
    pub orders_received:  AtomicU64,
    pub orders_matched:   AtomicU64,
    pub orders_cancelled: AtomicU64,
    pub trades_executed:  AtomicU64,
    pub total_latency_ns: AtomicU64,
    pub max_latency_ns:   AtomicU64,
    pub min_latency_ns:   AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            orders_received:  AtomicU64::new(0),
            orders_matched:   AtomicU64::new(0),
            orders_cancelled: AtomicU64::new(0),
            trades_executed:  AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            max_latency_ns:   AtomicU64::new(0),
            min_latency_ns:   AtomicU64::new(u64::MAX),
        }
    }

    pub fn record_latency(&self, lat: u64) {
        self.total_latency_ns.fetch_add(lat, Ordering::Relaxed);
        self.orders_matched.fetch_add(1, Ordering::Relaxed);
        let mut old = self.max_latency_ns.load(Ordering::Relaxed);
        while lat > old {
            match self.max_latency_ns.compare_exchange_weak(old, lat, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break, Err(x) => old = x,
            }
        }
        let mut old = self.min_latency_ns.load(Ordering::Relaxed);
        while lat < old {
            match self.min_latency_ns.compare_exchange_weak(old, lat, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break, Err(x) => old = x,
            }
        }
    }

    pub fn avg_latency_us(&self) -> f64 {
        let m = self.orders_matched.load(Ordering::Relaxed);
        if m == 0 { return 0.0; }
        self.total_latency_ns.load(Ordering::Relaxed) as f64 / m as f64 / 1000.0
    }
}
