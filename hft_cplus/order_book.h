#pragma once
#include <cstring>
#include <functional>
#include <unordered_map>
#include <vector>
#include <deque>
#include <memory>
#include <algorithm>
#include "trading_engine.h"

using TradeCallback = std::function<void(const Trade&)>;

// ── Safe pool: stores Orders in stable heap blocks ────────────────────
//   Uses a free-list of raw pointers into a list of fixed-size blocks.
//   Blocks never move → pointers stay valid across allocations.
class OrderPool {
    static constexpr size_t BLOCK = 512;
    std::vector<std::unique_ptr<Order[]>> blocks_;
    std::vector<Order*>                   free_;

    void grow() {
        auto& blk = blocks_.emplace_back(new Order[BLOCK]);
        free_.reserve(free_.size() + BLOCK);
        for (size_t i = 0; i < BLOCK; ++i)
            free_.push_back(&blk[i]);
    }

public:
    OrderPool() { grow(); }

    FORCE_INLINE Order* alloc() {
        if (UNLIKELY(free_.empty())) grow();
        Order* p = free_.back(); free_.pop_back();
        return p;
    }
    FORCE_INLINE void dealloc(Order* p) { free_.push_back(p); }
};

// ── Price level ────────────────────────────────────────────────────────
struct PriceLevel {
    Price              price = 0;
    std::deque<Order*> orders;
};

// ── Flat sorted book side ─────────────────────────────────────────────
struct BookSide {
    std::vector<PriceLevel> levels;
    bool desc;

    explicit BookSide(bool d) : desc(d) { levels.reserve(64); }

    FORCE_INLINE PriceLevel* best() {
        return levels.empty() ? nullptr : &levels.front();
    }

    FORCE_INLINE PriceLevel* find_or_create(Price p) {
        for (auto& lvl : levels)
            if (lvl.price == p) return &lvl;
        auto it = std::lower_bound(levels.begin(), levels.end(), p,
            [&](const PriceLevel& l, Price px) {
                return desc ? (l.price > px) : (l.price < px);
            });
        return &*levels.insert(it, PriceLevel{p, {}});
    }

    FORCE_INLINE void erase_empty(Price p) {
        for (auto it = levels.begin(); it != levels.end(); ++it) {
            if (it->price == p && it->orders.empty()) {
                levels.erase(it); return;
            }
        }
    }

    bool empty() const { return levels.empty(); }
};

// ─────────────────────────────────────────────
//  OrderBook
// ─────────────────────────────────────────────
class OrderBook {
    char     symbol_[8] = {};
    BookSide bids_{true};
    BookSide asks_{false};

    struct CancelEntry { Side side; Price price; };
    std::unordered_map<OrderId, CancelEntry> index_;

    OrderPool     pool_;
    TradeCallback on_trade_;
    Metrics&      metrics_;

    HOT void match(Order& agg, BookSide& opposite) {
        while (LIKELY(agg.remaining() > 0 && !opposite.empty())) {
            PriceLevel* lvl = opposite.best();

            if (agg.type == OrderType::LIMIT) {
                if (agg.side == Side::BUY  && agg.price < lvl->price) break;
                if (agg.side == Side::SELL && agg.price > lvl->price) break;
            }

            while (LIKELY(agg.remaining() > 0 && !lvl->orders.empty())) {
                Order* rest  = lvl->orders.front();
                const Quantity fill = std::min(agg.remaining(), rest->remaining());

                agg.filled   += fill;
                rest->filled += fill;

                Trade tr;
                tr.price = rest->price;
                tr.qty   = fill;
                tr.ts    = now_ns();
                std::memcpy(tr.symbol, symbol_, 8);
                if (agg.side == Side::BUY) { tr.buy_id = agg.id;  tr.sell_id = rest->id; }
                else                       { tr.buy_id = rest->id; tr.sell_id = agg.id;  }

                metrics_.trades_executed.fetch_add(1, std::memory_order_relaxed);
                metrics_.record_latency(tr.ts - agg.ts_in);
                if (LIKELY(on_trade_)) on_trade_(tr);

                if (rest->remaining() == 0) {
                    rest->status    = OrderStatus::FILLED;
                    rest->ts_filled = tr.ts;
                    index_.erase(rest->id);
                    lvl->orders.pop_front();
                    pool_.dealloc(rest);
                } else {
                    rest->status = OrderStatus::PARTIAL;
                    break;
                }
            }
            if (lvl->orders.empty()) opposite.erase_empty(lvl->price);
        }
        agg.status = (agg.remaining() == 0) ? OrderStatus::FILLED : OrderStatus::PARTIAL;
        if (agg.status == OrderStatus::FILLED) agg.ts_filled = now_ns();
    }

public:
    explicit OrderBook(const char* sym, Metrics& m, TradeCallback cb)
        : on_trade_(std::move(cb)), metrics_(m) {
        std::strncpy(symbol_, sym, 7);
        index_.reserve(4096);
    }

    HOT void add_order(Order o) {
        metrics_.orders_received.fetch_add(1, std::memory_order_relaxed);
        if (o.side == Side::BUY) match(o, asks_);
        else                     match(o, bids_);

        if (o.type == OrderType::LIMIT && o.remaining() > 0) {
            o.status = (o.filled > 0) ? OrderStatus::PARTIAL : OrderStatus::NEW;
            Order* node = pool_.alloc();
            *node = o;
            if (o.side == Side::BUY) bids_.find_or_create(o.price)->orders.push_back(node);
            else                     asks_.find_or_create(o.price)->orders.push_back(node);
            index_[o.id] = {o.side, o.price};
        }
    }

    bool cancel_order(OrderId id) {
        auto it = index_.find(id);
        if (it == index_.end()) return false;
        auto [side, price] = it->second;
        BookSide& book = (side == Side::BUY) ? bids_ : asks_;
        PriceLevel* lvl = book.find_or_create(price);
        for (auto oit = lvl->orders.begin(); oit != lvl->orders.end(); ++oit) {
            if ((*oit)->id == id) {
                pool_.dealloc(*oit);
                lvl->orders.erase(oit);
                break;
            }
        }
        book.erase_empty(price);
        index_.erase(it);
        metrics_.orders_cancelled.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    std::pair<Price,Price> spread() const {
        Price bid = bids_.levels.empty() ? 0 : bids_.levels[0].price;
        Price ask = asks_.levels.empty() ? 0 : asks_.levels[0].price;
        return {bid, ask};
    }
    size_t bid_depth() const { return bids_.levels.size(); }
    size_t ask_depth() const { return asks_.levels.size(); }
};