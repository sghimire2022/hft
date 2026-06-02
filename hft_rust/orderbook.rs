use std::collections::{BTreeMap, VecDeque, HashMap};
use std::sync::Arc;
use std::cmp::Reverse;

use crate::types::{
    Metrics, Order, OrderId, OrderStatus, OrderType,
    Price, Quantity, Side, Trade, now_ns,
};

struct IndexEntry { side: Side, price: Price }

pub struct OrderBook {
    symbol:   [u8; 8],
    bids:     BTreeMap<Reverse<Price>, VecDeque<Order>>,
    asks:     BTreeMap<Price, VecDeque<Order>>,
    index:    HashMap<OrderId, IndexEntry>,
    on_trade: Box<dyn Fn(Trade) + Send + Sync>,
    metrics:  Arc<Metrics>,
}

impl OrderBook {
    pub fn new(
        symbol: [u8; 8],
        metrics: Arc<Metrics>,
        on_trade: impl Fn(Trade) + Send + Sync + 'static,
    ) -> Self {
        OrderBook {
            symbol,
            bids:     BTreeMap::new(),
            asks:     BTreeMap::new(),
            index:    HashMap::new(),
            on_trade: Box::new(on_trade),
            metrics,
        }
    }

    fn do_fill(
        symbol:   [u8; 8],
        metrics:  &Metrics,
        on_trade: &dyn Fn(Trade),
        agg:      &mut Order,
        rest:     &mut Order,
    ) -> bool {
        let fill: Quantity = agg.remaining().min(rest.remaining());
        agg.filled  += fill;
        rest.filled += fill;

        let (buy_id, sell_id) = if agg.side == Side::Buy {
            (agg.id, rest.id)
        } else {
            (rest.id, agg.id)
        };
        let ts = now_ns();
        let trade = Trade { buy_id, sell_id, price: rest.price, qty: fill, symbol, ts };

        metrics.trades_executed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics.record_latency((ts - agg.ts_in) as u64);
        on_trade(trade);

        if rest.remaining() == 0 {
            rest.status    = OrderStatus::Filled;
            rest.ts_filled = ts;
            true   // resting order fully filled → remove it
        } else {
            rest.status = OrderStatus::Partial;
            false
        }
    }

    fn match_asks(&mut self, agg: &mut Order) {
        let mut exhausted: Vec<Price> = Vec::new();
        for (&ask_price, level) in self.asks.iter_mut() {
            if agg.remaining() == 0 { break; }
            if agg.order_type == OrderType::Limit && agg.price < ask_price { break; }

            let mut i = 0;
            while i < level.len() && agg.remaining() > 0 {
                let filled = Self::do_fill(
                    self.symbol, &self.metrics, &*self.on_trade,
                    agg, &mut level[i],
                );
                if filled {
                    self.index.remove(&level[i].id);
                    level.remove(i);
                } else {
                    i += 1;
                }
            }
            if level.is_empty() { exhausted.push(ask_price); }
        }
        for p in exhausted { self.asks.remove(&p); }
    }

    fn match_bids(&mut self, agg: &mut Order) {
        let mut exhausted: Vec<Reverse<Price>> = Vec::new();
        for (&Reverse(bid_price), level) in self.bids.iter_mut() {
            if agg.remaining() == 0 { break; }
            if agg.order_type == OrderType::Limit && agg.price > bid_price { break; }

            let mut i = 0;
            while i < level.len() && agg.remaining() > 0 {
                let filled = Self::do_fill(
                    self.symbol, &self.metrics, &*self.on_trade,
                    agg, &mut level[i],
                );
                if filled {
                    self.index.remove(&level[i].id);
                    level.remove(i);
                } else {
                    i += 1;
                }
            }
            if level.is_empty() { exhausted.push(Reverse(bid_price)); }
        }
        for k in exhausted { self.bids.remove(&k); }
    }

    pub fn add_order(&mut self, mut o: Order) {
        self.metrics.orders_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match o.side {
            Side::Buy  => self.match_asks(&mut o),
            Side::Sell => self.match_bids(&mut o),
        }
        if o.order_type == OrderType::Limit && o.remaining() > 0 {
            o.status = if o.filled > 0 { OrderStatus::Partial } else { OrderStatus::New };
            let id = o.id;
            match o.side {
                Side::Buy => {
                    self.index.insert(id, IndexEntry { side: Side::Buy, price: o.price });
                    self.bids.entry(Reverse(o.price)).or_insert_with(VecDeque::new).push_back(o);
                }
                Side::Sell => {
                    self.index.insert(id, IndexEntry { side: Side::Sell, price: o.price });
                    self.asks.entry(o.price).or_insert_with(VecDeque::new).push_back(o);
                }
            }
        }
    }

    pub fn cancel_order(&mut self, id: OrderId) -> bool {
        if let Some(e) = self.index.remove(&id) {
            match e.side {
                Side::Buy => {
                    if let Some(lvl) = self.bids.get_mut(&Reverse(e.price)) {
                        lvl.retain(|o| o.id != id);
                        if lvl.is_empty() { self.bids.remove(&Reverse(e.price)); }
                    }
                }
                Side::Sell => {
                    if let Some(lvl) = self.asks.get_mut(&e.price) {
                        lvl.retain(|o| o.id != id);
                        if lvl.is_empty() { self.asks.remove(&e.price); }
                    }
                }
            }
            self.metrics.orders_cancelled.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            true
        } else { false }
    }

    pub fn spread(&self) -> (Price, Price) {
        let bid = self.bids.keys().next().map(|r| r.0).unwrap_or(0);
        let ask = self.asks.keys().next().copied().unwrap_or(0);
        (bid, ask)
    }
    pub fn bid_depth(&self) -> usize { self.bids.len() }
    pub fn ask_depth(&self) -> usize { self.asks.len() }
}
