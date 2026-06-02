mod types;
mod logger;
mod orderbook;
mod engine;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rand::Rng;

use engine::TradingEngine;
use types::{Order, OrderType, Quantity, Side, UserId};

const NUM_USERS:        u32 = 16;
const ORDERS_PER_USER:  u32 = 500;
const PERF_INTERVAL_MS: u64 = 500;

static SYMBOLS: &[&str] = &["AAPL", "MSFT", "TSLA", "AMZN"];

fn user_thread(sess: Arc<engine::UserSession>, orders: u32, done: Arc<AtomicU32>) {
    let mut rng = rand::thread_rng();
    for _ in 0..orders {
        let sym = SYMBOLS[rng.gen_range(0..SYMBOLS.len())];
        let side = if rng.gen_bool(0.5) { Side::Buy } else { Side::Sell };
        let (order_type, price) = if rng.gen_range(0..5) == 0 {
            (OrderType::Market, 0)
        } else {
            (OrderType::Limit, 9950 + rng.gen_range(0..=100) as i64)
        };
        let o = Order {
            id:         sess.new_order_id(),
            user_id:    sess.id,
            price,
            qty:        1 + rng.gen_range(0..100) as Quantity,
            filled:     0,
            side,
            order_type,
            status:     types::OrderStatus::New,
            symbol:     Order::make(sym),
            ts_in:      types::now_ns(),
            ts_filled:  0,
        };
        while !sess.inbound.push(o.clone()) {
            thread::sleep(Duration::from_nanos(1));
        }
        let jitter = rng.gen_range(0..=50u64);
        if jitter > 0 { thread::sleep(Duration::from_micros(jitter)); }
    }
    done.fetch_add(1, Ordering::Relaxed);
}

fn main() {
    println!("\x1b[1;34m");
    println!("╔══════════════════════════════════════════════════╗");
    println!("║   HFT Learning Engine  –  Rust Demo              ║");
    println!("║   {} users × {} orders  ({} total)          ║",
        NUM_USERS, ORDERS_PER_USER, NUM_USERS * ORDERS_PER_USER);
    println!("╚══════════════════════════════════════════════════╝");
    println!("\x1b[0m");

    let mut engine = TradingEngine::new("logs/engine.log", PERF_INTERVAL_MS)
        .expect("Failed to create engine");
    for sym in SYMBOLS { engine.add_symbol(sym); }
    engine.start();
    thread::sleep(Duration::from_millis(50));

    let done_count = Arc::new(AtomicU32::new(0));
    let mut handles = Vec::new();
    let wall_start  = Instant::now();

    for uid in 1..=NUM_USERS as UserId {
        let sess   = engine.create_session(uid);
        let done_c = Arc::clone(&done_count);
        handles.push(thread::spawn(move || user_thread(sess, ORDERS_PER_USER, done_c)));
    }
    for h in handles { let _ = h.join(); }

    engine.logger.lock().info("All users done sending. Draining engine...".to_string());
    thread::sleep(Duration::from_millis(300));

    let wall_ms    = wall_start.elapsed().as_millis() as f64;
    let total      = (NUM_USERS * ORDERS_PER_USER) as u64;
    let throughput = total as f64 / (wall_ms / 1000.0);
    let m = &engine.metrics;
    use std::sync::atomic::Ordering::Relaxed;

    // log summary — blocking sends, guaranteed to flush
    engine.logger.lock().log(logger::LVL_PERF,
        "═══════════════════ FINAL SUMMARY ═══════════════════".to_string());
    engine.logger.lock().log(logger::LVL_PERF,
        format!("Wall time      : {:.2} ms", wall_ms));
    engine.logger.lock().log(logger::LVL_PERF,
        format!("Total orders   : {}", total));
    engine.logger.lock().log(logger::LVL_PERF,
        format!("Throughput     : {:.0} orders/sec", throughput));
    engine.logger.lock().log(logger::LVL_PERF,
        format!("Orders recv    : {}", m.orders_received.load(Relaxed)));
    engine.logger.lock().log(logger::LVL_PERF,
        format!("Trades exec    : {}", m.trades_executed.load(Relaxed)));
    engine.logger.lock().log(logger::LVL_PERF,
        format!("Cancelled      : {}", m.orders_cancelled.load(Relaxed)));
    engine.logger.lock().log(logger::LVL_PERF,
        format!("Avg latency    : {:.2} µs", m.avg_latency_us()));

    engine.stop(); // waits for logger to fully flush before exit

    println!("\n\x1b[1;32mLog saved to: logs/engine.log\x1b[0m\n");
}
