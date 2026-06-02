use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::Duration;

use parking_lot::RwLock;

use crate::logger::{AsyncLogger, LVL_DEBUG, LVL_INFO, LVL_WARN};
use crate::orderbook::OrderBook;
use crate::types::{Metrics, Order, OrderId, Side, SPSCQueue, UserId};

pub struct UserSession {
    pub id:       UserId,
    pub inbound:  SPSCQueue,
    next_seq:     std::sync::atomic::AtomicU64,
}

impl UserSession {
    pub fn new(id: UserId) -> Arc<Self> {
        Arc::new(UserSession {
            id,
            inbound:  SPSCQueue::new(),
            next_seq: std::sync::atomic::AtomicU64::new(1),
        })
    }

    pub fn new_order_id(&self) -> OrderId {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        ((self.id as u64) << 32) | seq
    }
}

pub struct TradingEngine {
    books:         Arc<RwLock<HashMap<String, OrderBook>>>,
    sessions:      Arc<RwLock<Vec<Arc<UserSession>>>>,
    pub metrics:   Arc<Metrics>,
    pub logger:    Arc<parking_lot::Mutex<AsyncLogger>>,
    running:       Arc<AtomicBool>,
    engine_thread: Option<thread::JoinHandle<()>>,
}

impl TradingEngine {
    pub fn new(log_path: &str, perf_interval_ms: u64) -> std::io::Result<Self> {
        let logger  = Arc::new(parking_lot::Mutex::new(AsyncLogger::new(log_path)?));
        let metrics = Arc::new(Metrics::new());

        // periodic perf reporter
        {
            let logger_c  = Arc::clone(&logger);
            let metrics_c = Arc::clone(&metrics);
            thread::spawn(move || loop {
                thread::sleep(Duration::from_millis(perf_interval_ms));
                logger_c.lock().log_perf(&metrics_c);
            });
        }

        Ok(TradingEngine {
            books:         Arc::new(RwLock::new(HashMap::new())),
            sessions:      Arc::new(RwLock::new(Vec::new())),
            metrics,
            logger,
            running:       Arc::new(AtomicBool::new(false)),
            engine_thread: None,
        })
    }

    pub fn add_symbol(&self, sym: &str) {
        let symbol_bytes = Order::make(sym);
        let metrics_c = Arc::clone(&self.metrics);
        let logger_c  = Arc::clone(&self.logger);
        let book = OrderBook::new(
            symbol_bytes,
            metrics_c,
            move |t| logger_c.lock().log_trade(&t),
        );
        self.books.write().insert(sym.to_string(), book);
        self.logger.lock().info(format!("Symbol '{}' added to engine", sym));
    }

    pub fn create_session(&self, uid: UserId) -> Arc<UserSession> {
        let sess = UserSession::new(uid);
        let mut sessions = self.sessions.write();
        sessions.push(Arc::clone(&sess));
        let n = sessions.len();
        self.logger.lock().info(format!("Session created for user {}  (total: {})", uid, n));
        sess
    }

    pub fn start(&mut self) {
        self.running.store(true, Ordering::SeqCst);
        let running  = Arc::clone(&self.running);
        let sessions = Arc::clone(&self.sessions);
        let books    = Arc::clone(&self.books);
        let logger   = Arc::clone(&self.logger);
        let metrics  = Arc::clone(&self.metrics);
        self.engine_thread = Some(thread::spawn(move || {
            Self::engine_loop(running, sessions, books, logger, metrics);
        }));
        let n = self.books.read().len();
        self.logger.lock().info(format!("Engine started — {} symbols", n));
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(h) = self.engine_thread.take() { let _ = h.join(); }
        self.logger.lock().info("Engine stopped".to_string());
        self.logger.lock().log_perf(&self.metrics);
        // explicitly flush — waits for writer thread to drain all entries
        self.logger.lock().flush();
    }

    fn engine_loop(
        running:  Arc<AtomicBool>,
        sessions: Arc<RwLock<Vec<Arc<UserSession>>>>,
        books:    Arc<RwLock<HashMap<String, OrderBook>>>,
        logger:   Arc<parking_lot::Mutex<AsyncLogger>>,
        _metrics: Arc<Metrics>,
    ) {
        while running.load(Ordering::Relaxed) {
            let mut idle = true;
            let snaps: Vec<Arc<UserSession>> = sessions.read().clone();
            for sess in &snaps {
                while let Some(o) = sess.inbound.pop() {
                    idle = false;
                    Self::dispatch(&books, &logger, o);
                }
            }
            if idle { thread::sleep(Duration::from_micros(1)); }
        }
    }

    fn dispatch(
        books:  &RwLock<HashMap<String, OrderBook>>,
        logger: &parking_lot::Mutex<AsyncLogger>,
        o: Order,
    ) {
        let sym  = o.symbol_str().to_string();
        let side = if o.side == Side::Buy { "BUY " } else { "SELL" };
        logger.lock().log(LVL_DEBUG, format!(
            "ORDER side={} sym={} px={} qty={} id={} user={}",
            side, sym, o.price, o.qty, o.id, o.user_id
        ));
        let mut books_w = books.write();
        if let Some(book) = books_w.get_mut(&sym) {
            book.add_order(o);
            let (bid, ask) = book.spread();
            let (bd, ad)   = (book.bid_depth(), book.ask_depth());
            drop(books_w);
            logger.lock().log(LVL_INFO, format!(
                "BOOK  {} bid={} ask={} bdepth={} adepth={}", sym, bid, ask, bd, ad
            ));
        } else {
            drop(books_w);
            logger.lock().log(LVL_WARN, format!(
                "Unknown symbol '{}' in order #{} – dropped", sym, o.id
            ));
        }
    }
}
