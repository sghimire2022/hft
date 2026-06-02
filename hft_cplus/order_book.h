#pragma once
#include <cstring>
#include <functional>
#include <unordered_map>
// ── Use flat sorted arrays instead of std::map ───────────────────────
// std::map = red-black tree = pointer chasing = cache misses
// Flat sorted vector = contiguous memory = cache friendly
// Price range is narrow (9950-10050 = 101 levels) so arrays stay tiny
#include <vector>
#include <algorithm>
#include "trading_engine.h"

using TradeCallback = std::function<void(const Trade&)>;

// ── Pool allocator for Order nodes ────────────────────────────────────
// Avoids heap allocation per order — reuses a fixed pool
template<typename T, size_t PoolSize = 8192>
class ObjectPool {
    alignas(64) T pool_[PoolSize];
    std::vector<T*> free_;
public:
    ObjectPool() {
        free_.reserve(PoolSize);
        for (size_t i = 0; i < PoolSize; ++i) free_.push_back(&pool_[i]);
    }
    T* alloc() {
        if (UNLIKELY(free_.empty())) return new T{};  // fallback
        T* p = free_.back(); free_.pop_back(); return p;
    }
    void dealloc(T* p) {
        if (LIKELY(p >= pool_ && p < pool_ + PoolSize)) free_.push_back(p);
        else delete p;
    }
};

// ── Price level: intrusive linked list of orders ──────────────────────
// Better cache locality than std::deque for sequential pop_front
struct PriceLevel {
    Price    price   = 0;
    Order*   head    = nullptr;   // front of FIFO queue
    Order*   tail    = nullptr;   // back  of FIFO queue
    uint32_t count   = 0;

    void push_back(Order* o) {
        o->_pad2[0] = 0;          // reuse pad byte as next-ptr flag
        // store next pointer in pad bytes (8 bytes available)
        memset(o->_pad2, 0, 8);
        if (!head) { head = tail = o; }
        else {
            // use tail's pad2 as next pointer
            memcpy(tail->_pad2, &o, sizeof(Order*));
            tail = o;
        }
        ++count;
    }

    Order* front() const { return head; }

    void pop_front() {
        if (!head) return;
        Order* next = nullptr;
        memcpy(&next, head->_pad2, sizeof(Order*));
        head = next;
        if (!head) tail = nullptr;
        --count;
    }

    bool empty() const { return head == nullptr; }
};

// ── Flat book side: sorted vector of PriceLevels ─────────────────────
struct BookSide {
    std::vector<PriceLevel> levels;
    bool descending; // true=bids, false=asks

    BookSide(bool desc, size_t reserve = 128) : descending(desc) {
        levels.reserve(reserve);
    }

    PriceLevel* best() { return levels.empty() ? nullptr : &levels[0]; }

    PriceLevel* find_or_create(Price p) {
        // binary search for existing level
        auto it = std::lower_bound(levels.begin(), levels.end(), p,
            [this](const PriceLevel& lvl, Price px) {
                return descending ? lvl.price > px : lvl.price < px;
            });
        if (it != levels.end() && it->price == p) return &*it;
        // insert in sorted position
        auto ins = levels.insert(it, PriceLevel{p, nullptr, nullptr, 0});
        return &*ins;
    }

    void erase_if_empty(Price p) {
        auto it = std::lower_bound(levels.begin(), levels.end(), p,
            [this](const PriceLevel& lvl, Price px) {
                return descending ? lvl.price > px : lvl.price < px;
            });
        if (it != levels.end() && it->price == p && it->empty())
            levels.erase(it);
    }

    bool empty() const { return levels.empty(); }
};

// ─────────────────────────────────────────────
//  OrderBook — optimized
// ─────────────────────────────────────────────
class OrderBook {
    char     symbol_[8] = {};
    BookSide bids_{true,  64};   // descending
    BookSide asks_{false, 64};   // ascending

    // cancel index: order id → (side, price, ptr)
    struct CancelEntry { Side side; Price price; Order* ptr; };
    std::unordered_map<OrderId, CancelEntry> index_;

    ObjectPool<Order> pool_;
    TradeCallback     on_trade_;
    Metrics&          metrics_;

    // ── inline match loop ─────────────────────
    HOT void match(Order& agg, BookSide& opposite) {
        while (LIKELY(agg.remaining() > 0) && LIKELY(!opposite.empty())) {
            PriceLevel* lvl = opposite.best();

            // price check
            if (agg.type == OrderType::LIMIT) {
                if (agg.side == Side::BUY  && agg.price < lvl->price) break;
                if (agg.side == Side::SELL && agg.price > lvl->price) break;
            }

            while (LIKELY(agg.remaining() > 0) && LIKELY(!lvl->empty())) {
                Order* rest = lvl->front();
                const Quantity fill = std::min(agg.remaining(), rest->remaining());

                agg.filled   += fill;
                rest->filled += fill;

                // build trade inline — avoid function call overhead
                Trade tr;
                tr.price = rest->price;
                tr.qty   = fill;
                tr.ts    = now_ns();
                memcpy(tr.symbol, symbol_, 8);
                if (agg.side == Side::BUY) {
                    tr.buy_id = agg.id; tr.sell_id = rest->id;
                } else {
                    tr.buy_id = rest->id; tr.sell_id = agg.id;
                }

                metrics_.trades_executed.fetch_add(1, std::memory_order_relaxed);
                metrics_.record_latency(tr.ts - agg.ts_in);
                if (LIKELY(on_trade_)) on_trade_(tr);

                if (rest->remaining() == 0) {
                    rest->status    = OrderStatus::FILLED;
                    rest->ts_filled = tr.ts;
                    index_.erase(rest->id);
                    lvl->pop_front();
                    pool_.dealloc(rest);
                } else {
                    rest->status = OrderStatus::PARTIAL;
                    break;
                }
            }
            if (lvl->empty()) opposite.erase_if_empty(lvl->price);
        }
        agg.status = (agg.remaining() == 0)
                   ? OrderStatus::FILLED : OrderStatus::PARTIAL;
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
            // copy order into pool-allocated node
            Order* node = pool_.alloc();
            *node = o;
            memset(node->_pad2, 0, 8); // clear next pointer
            if (o.side == Side::BUY) {
                bids_.find_or_create(o.price)->push_back(node);
            } else {
                asks_.find_or_create(o.price)->push_back(node);
            }
            index_[o.id] = {o.side, o.price, node};
        }
    }

    bool cancel_order(OrderId id) {
        auto it = index_.find(id);
        if (it == index_.end()) return false;
        auto& [side, price, ptr] = it->second;
        BookSide& book = (side == Side::BUY) ? bids_ : asks_;
        auto* lvl = book.find_or_create(price);
        // walk linked list to unlink
        Order** cur = &lvl->head;
        while (*cur) {
            if (*cur == ptr) {
                Order* next = nullptr;
                memcpy(&next, (*cur)->_pad2, sizeof(Order*));
                if (lvl->tail == *cur) lvl->tail = nullptr;
                pool_.dealloc(*cur);
                *cur = next;
                --lvl->count;
                break;
            }
            Order* nxt = nullptr;
            memcpy(&nxt, (*cur)->_pad2, sizeof(Order*));
            cur = reinterpret_cast<Order**>((*cur)->_pad2);
            (void)nxt;
        }
        book.erase_if_empty(price);
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
