#pragma once
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <memory>
#include <cstring>
#include <immintrin.h>
#include "trading_engine.h"
#include "order_book.h"
#include "logger.h"

// ── Set to 0 to disable per-order DEBUG/INFO logs on hot path ─────────
//    Logging every order at 100k/sec floods the async queue
//    and adds ~100µs of backpressure per order
#define LOG_ORDERS 0

struct UserSession {
    UserId id = 0;
    alignas(64) SPSCQueue<Order, 4096> inbound;
    std::atomic<uint64_t> next_order_id{1};

    FORCE_INLINE OrderId new_order_id() {
        return ((uint64_t)id << 32)
             | next_order_id.fetch_add(1, std::memory_order_relaxed);
    }
};

class TradingEngine {
    static constexpr size_t MAX_SYM = 8;
    struct SymEntry {
        uint32_t                   key = 0;
        std::unique_ptr<OrderBook> book;
    };
    SymEntry sym_table_[MAX_SYM];
    size_t   num_sym_ = 0;

    std::vector<std::shared_ptr<UserSession>> sessions_;
    std::mutex                                sess_mu_;

    Metrics     metrics_;
    AsyncLogger logger_;
    std::thread engine_thread_;
    std::atomic<bool>     running_{false};
    std::atomic<uint64_t> perf_interval_ns_;

    FORCE_INLINE OrderBook* find_book(const char* sym) {
        uint32_t key = 0;
        memcpy(&key, sym, 4);
        for (size_t i = 0; i < num_sym_; ++i)
            if (sym_table_[i].key == key) return sym_table_[i].book.get();
        return nullptr;
    }

    HOT void engine_loop() {
        uint64_t last_perf = now_ns();
        std::vector<std::shared_ptr<UserSession>> snap;
        snap.reserve(64);

        while (LIKELY(running_.load(std::memory_order_relaxed))) {
            {
                std::lock_guard<std::mutex> lk(sess_mu_);
                if (snap.size() != sessions_.size()) snap = sessions_;
            }

            bool idle = true;
            for (auto& sess : snap) {
                Order o;
                while (sess->inbound.pop(o)) {
                    idle = false;
                    dispatch(o);
                }
            }

            if (UNLIKELY(idle)) {
                _mm_pause(); _mm_pause(); _mm_pause(); _mm_pause();
            }

            const uint64_t now = now_ns();
            if (UNLIKELY(now - last_perf >=
                    perf_interval_ns_.load(std::memory_order_relaxed))) {
                logger_.log_perf(metrics_);
                last_perf = now;
            }
        }
    }

    HOT void dispatch(Order& o) {
        OrderBook* book = find_book(o.symbol);
        if (UNLIKELY(!book)) {
            logger_.log(2, "Unknown symbol '%.7s' dropped", o.symbol);
            return;
        }
#if LOG_ORDERS
        // only log when LOG_ORDERS=1 — disabled at 100k+/sec
        logger_.log(0, "ORDER side=%s sym=%.7s px=%lld qty=%u id=%llu user=%u",
            o.side == Side::BUY ? "BUY " : "SELL",
            o.symbol, (long long)o.price, o.qty,
            (unsigned long long)o.id, o.user_id);
#endif
        book->add_order(o);
#if LOG_ORDERS
        auto [bid, ask] = book->spread();
        logger_.log(1, "BOOK  %.7s bid=%lld ask=%lld bdepth=%zu adepth=%zu",
            o.symbol, (long long)bid, (long long)ask,
            book->bid_depth(), book->ask_depth());
#endif
    }

public:
    explicit TradingEngine(const std::string& log_path = "logs/engine.log",
                           uint64_t perf_interval_ms = 1000)
        : logger_(log_path),
          perf_interval_ns_(perf_interval_ms * 1'000'000ULL) {}

    void add_symbol(const char* sym) {
        auto cb = [this](const Trade& t){ logger_.log_trade(t); };
        memcpy(&sym_table_[num_sym_].key, sym, 4);
        sym_table_[num_sym_].book = std::make_unique<OrderBook>(sym, metrics_, cb);
        ++num_sym_;
        logger_.log(1, "Symbol '%s' added", sym);
    }

    std::shared_ptr<UserSession> create_session(UserId uid) {
        auto sess = std::make_shared<UserSession>();
        sess->id = uid;
        std::lock_guard<std::mutex> lk(sess_mu_);
        sessions_.push_back(sess);
        logger_.log(1, "Session created for user %u (total: %zu)", uid, sessions_.size());
        return sess;
    }

    void start() {
        running_ = true;
        engine_thread_ = std::thread(&TradingEngine::engine_loop, this);
        logger_.log(1, "Engine started — %zu symbols (LOG_ORDERS=%d)", num_sym_, LOG_ORDERS);
    }

    void stop() {
        running_ = false;
        if (engine_thread_.joinable()) engine_thread_.join();
        logger_.log(1, "Engine stopped");
        logger_.log_perf(metrics_);
    }

    Metrics&     metrics() { return metrics_; }
    AsyncLogger& log()     { return logger_;  }
};