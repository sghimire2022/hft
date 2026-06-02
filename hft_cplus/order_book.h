#pragma once
#include <map>
#include <deque>
#include <unordered_map>
#include <functional>
#include <cstring>
#include "trading_engine.h"

using TradeCallback = std::function<void(const Trade&)>;

class OrderBook {
    char symbol_[8] = {};

    // Keep std::map — it's actually well-optimised for small N
    // Just remove the redundant find() calls on erase
    std::map<Price, std::deque<Order>, std::greater<Price>> bids_;
    std::map<Price, std::deque<Order>, std::less<Price>>    asks_;
    std::unordered_map<OrderId, std::pair<Side,Price>>      index_;

    TradeCallback on_trade_;
    Metrics&      metrics_;

    HOT FORCE_INLINE Trade make_trade(Order& agg, Order& rest, Quantity qty) {
        Trade t;
        // single 8-byte copy instead of memcpy loop
        *reinterpret_cast<uint64_t*>(t.symbol) = *reinterpret_cast<const uint64_t*>(symbol_);
        t.price = rest.price;
        t.qty   = qty;
        t.ts    = now_ns();
        if (agg.side == Side::BUY) { t.buy_id = agg.id;  t.sell_id = rest.id; }
        else                       { t.buy_id = rest.id; t.sell_id = agg.id;  }
        return t;
    }

    template<typename BookMap>
    HOT void match(Order& agg, BookMap& opposite, bool is_ask) {
        while (LIKELY(agg.remaining() > 0 && !opposite.empty())) {
            auto it = opposite.begin();
            const Price best_price = it->first;
            auto& level            = it->second;

            if (agg.type == OrderType::LIMIT) {
                if (agg.side == Side::BUY  && agg.price < best_price) break;
                if (agg.side == Side::SELL && agg.price > best_price) break;
            }

            while (LIKELY(agg.remaining() > 0 && !level.empty())) {
                Order& rest = level.front();
                const Quantity fill = std::min(agg.remaining(), rest.remaining());

                agg.filled  += fill;
                rest.filled += fill;

                Trade tr = make_trade(agg, rest, fill);
                metrics_.trades_executed.fetch_add(1, std::memory_order_relaxed);
                metrics_.record_latency(tr.ts - agg.ts_in);
                if (LIKELY(on_trade_)) on_trade_(tr);

                if (rest.remaining() == 0) {
                    rest.status    = OrderStatus::FILLED;
                    rest.ts_filled = tr.ts;
                    index_.erase(rest.id);
                    level.pop_front();
                } else {
                    rest.status = OrderStatus::PARTIAL;
                    break; // resting order partially filled, stop inner loop
                }
            }
            // erase level using the iterator we already have — no second find()
            if (level.empty()) opposite.erase(it);
        }
        agg.status = (agg.remaining() == 0) ? OrderStatus::FILLED : OrderStatus::PARTIAL;
        if (agg.status == OrderStatus::FILLED) agg.ts_filled = now_ns();
    }

public:
    explicit OrderBook(const char* sym, Metrics& m, TradeCallback cb)
        : on_trade_(std::move(cb)), metrics_(m) {
        std::strncpy(symbol_, sym, 7);
        index_.reserve(4096); // pre-size hash map — avoid rehash on hot path
    }

    HOT void add_order(Order o) {
        metrics_.orders_received.fetch_add(1, std::memory_order_relaxed);

        if (o.side == Side::BUY) match(o, asks_, true);
        else                     match(o, bids_, false);

        if (o.type == OrderType::LIMIT && o.remaining() > 0) {
            o.status = (o.filled > 0) ? OrderStatus::PARTIAL : OrderStatus::NEW;
            index_.emplace(o.id, std::make_pair(o.side, o.price));
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