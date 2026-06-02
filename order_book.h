#pragma once
#include <map>
#include <deque>
#include <unordered_map>
#include <vector>
#include <functional>
#include <cstring>
#include "trading_engine.h"

using TradeCallback = std::function<void(const Trade&)>;

class OrderBook {
    char symbol_[8] = {};

    std::map<Price, std::deque<Order>, std::greater<Price>> bids_;
    std::map<Price, std::deque<Order>, std::less<Price>>    asks_;
    std::unordered_map<OrderId, std::pair<Side,Price>>      index_;

    TradeCallback on_trade_;
    Metrics&      metrics_;

    void erase_empty_bid(Price p) {
        auto it = bids_.find(p);
        if (it != bids_.end() && it->second.empty()) bids_.erase(it);
    }
    void erase_empty_ask(Price p) {
        auto it = asks_.find(p);
        if (it != asks_.end() && it->second.empty()) asks_.erase(it);
    }

    Trade make_trade(Order& aggressor, Order& resting, Quantity qty) {
        Trade t;
        std::memcpy(t.symbol, symbol_, 8);
        t.price   = resting.price;
        t.qty     = qty;
        t.ts      = now_ns();
        if (aggressor.side == Side::BUY) { t.buy_id = aggressor.id; t.sell_id = resting.id; }
        else                              { t.buy_id = resting.id;   t.sell_id = aggressor.id; }
        return t;
    }

    template<typename BookMap>
    void match(Order& agg, BookMap& opposite, bool is_ask_book) {
        while (agg.remaining() > 0 && !opposite.empty()) {
            auto& [best_price, level] = *opposite.begin();
            if (agg.type == OrderType::LIMIT) {
                if (agg.side == Side::BUY  && agg.price < best_price) break;
                if (agg.side == Side::SELL && agg.price > best_price) break;
            }
            while (agg.remaining() > 0 && !level.empty()) {
                Order& rest = level.front();
                Quantity fill = std::min(agg.remaining(), rest.remaining());
                agg.filled  += fill;
                rest.filled += fill;
                Trade tr = make_trade(agg, rest, fill);
                metrics_.trades_executed.fetch_add(1, std::memory_order_relaxed);
                uint64_t lat = tr.ts - agg.ts_in;
                metrics_.record_latency(lat);
                if (on_trade_) on_trade_(tr);
                if (rest.remaining() == 0) {
                    rest.status = OrderStatus::FILLED;
                    rest.ts_filled = tr.ts;
                    index_.erase(rest.id);
                    level.pop_front();
                } else {
                    rest.status = OrderStatus::PARTIAL;
                }
            }
            if (is_ask_book) erase_empty_ask(best_price);
            else             erase_empty_bid(best_price);
        }
        agg.status = (agg.remaining() == 0) ? OrderStatus::FILLED : OrderStatus::PARTIAL;
        if (agg.status == OrderStatus::FILLED) agg.ts_filled = now_ns();
    }

public:
    explicit OrderBook(const char* sym, Metrics& m, TradeCallback cb)
        : on_trade_(std::move(cb)), metrics_(m) { std::strncpy(symbol_, sym, 7); }

    void add_order(Order o) {
        metrics_.orders_received.fetch_add(1, std::memory_order_relaxed);
        if (o.side == Side::BUY) match(o, asks_, true);
        else                     match(o, bids_, false);

        if (o.type == OrderType::LIMIT && o.remaining() > 0) {
            o.status = (o.filled > 0) ? OrderStatus::PARTIAL : OrderStatus::NEW;
            index_[o.id] = {o.side, o.price};
            if (o.side == Side::BUY) bids_[o.price].push_back(o);
            else                     asks_[o.price].push_back(o);
        }
    }

    bool cancel_order(OrderId id) {
        auto it = index_.find(id);
        if (it == index_.end()) return false;
        auto [side, price] = it->second;
        if (side == Side::BUY) {
            auto li = bids_.find(price);
            if (li != bids_.end()) {
                auto& q = li->second;
                for (auto oi = q.begin(); oi != q.end(); ++oi)
                    if (oi->id == id) { q.erase(oi); break; }
                if (q.empty()) bids_.erase(li);
            }
        } else {
            auto li = asks_.find(price);
            if (li != asks_.end()) {
                auto& q = li->second;
                for (auto oi = q.begin(); oi != q.end(); ++oi)
                    if (oi->id == id) { q.erase(oi); break; }
                if (q.empty()) asks_.erase(li);
            }
        }
        index_.erase(it);
        metrics_.orders_cancelled.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    std::pair<Price,Price> spread() const {
        Price bid = bids_.empty() ? 0 : bids_.begin()->first;
        Price ask = asks_.empty() ? 0 : asks_.begin()->first;
        return {bid, ask};
    }
    size_t bid_depth() const { return bids_.size(); }
    size_t ask_depth() const { return asks_.size(); }
};
