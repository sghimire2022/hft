use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::mpsc::{self, SyncSender};
use std::thread;

use crate::types::{Metrics, Trade, now_ns};

pub const LVL_DEBUG: u8 = 0;
pub const LVL_INFO:  u8 = 1;
pub const LVL_WARN:  u8 = 2;
pub const LVL_TRADE: u8 = 3;
pub const LVL_PERF:  u8 = 4;

const LEVEL_STR:   [&str; 5] = ["DEBUG", "INFO ", "WARN ", "TRADE", "PERF "];
const LEVEL_COLOR: [&str; 5] = [
    "\x1b[0m", "\x1b[32m", "\x1b[33m", "\x1b[36m", "\x1b[35m",
];

pub struct LogEntry {
    pub msg:   String,
    pub level: u8,
    pub ts:    i64,
}

pub struct AsyncLogger {
    tx:     SyncSender<Option<LogEntry>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl AsyncLogger {
    pub fn new(path: &str) -> std::io::Result<Self> {
        fs::create_dir_all("logs")?;
        let mut file = OpenOptions::new()
            .create(true).append(true).open(path)?;

        let (tx, rx) = mpsc::sync_channel::<Option<LogEntry>>(16_384);

        let handle = thread::spawn(move || {
            while let Ok(Some(e)) = rx.recv() {
                let line = Self::format_entry(&e);
                let _ = file.write_all(line.as_bytes());
                let _ = file.flush();
                let color = LEVEL_COLOR[e.level as usize];
                print!("{}{}\x1b[0m", color, line);
                // flush stdout immediately so tee captures it
                let _ = std::io::stdout().flush();
            }
        });

        Ok(AsyncLogger { tx, handle: Some(handle) })
    }

    fn format_entry(e: &LogEntry) -> String {
        let ns   = e.ts as u64;
        let sec  = ns / 1_000_000_000;
        let ms   = (ns % 1_000_000_000) / 1_000_000;
        let us   = (ns % 1_000_000) / 1_000;
        let nano = ns % 1_000;
        format!("[{}][{}.{:03}.{:03}.{:03}] {}\n",
            LEVEL_STR[e.level as usize], sec, ms, us, nano, e.msg)
    }

    pub fn log(&self, level: u8, msg: String) {
        let entry = LogEntry { msg, level, ts: now_ns() };
        // blocking send — guarantees delivery, never drops
        let _ = self.tx.send(Some(entry));
    }

    pub fn debug(&self, msg: String) { self.log(LVL_DEBUG, msg); }
    pub fn info(&self,  msg: String) { self.log(LVL_INFO,  msg); }
    pub fn warn(&self,  msg: String) { self.log(LVL_WARN,  msg); }

    pub fn log_trade(&self, t: &Trade) {
        self.log(LVL_TRADE, format!(
            "EXEC  sym={} buy#{} sell#{} price={} qty={}",
            t.symbol_str(), t.buy_id, t.sell_id, t.price, t.qty
        ));
    }

    pub fn log_perf(&self, m: &Metrics) {
        use std::sync::atomic::Ordering::Relaxed;
        let recv = m.orders_received.load(Relaxed);
        let trd  = m.trades_executed.load(Relaxed);
        let can  = m.orders_cancelled.load(Relaxed);
        let minl = m.min_latency_ns.load(Relaxed);
        let maxl = m.max_latency_ns.load(Relaxed);
        let minl = if minl == u64::MAX { 0 } else { minl };
        self.log(LVL_PERF, format!(
            "PERF  recv={} trades={} cancelled={} avg_lat={:.2} µs  min={} ns  max={} ns",
            recv, trd, can, m.avg_latency_us(), minl, maxl
        ));
    }

    /// Flush: send shutdown signal and wait for writer thread to finish
    pub fn flush(&mut self) {
        let _ = self.tx.send(None);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for AsyncLogger {
    fn drop(&mut self) {
        self.flush();
    }
}
