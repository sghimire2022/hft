#pragma once
#include <thread>
#include <vector>
#include <array>
#include <atomic>
#include <mutex>
#include <memory>
#include <cstring>
#include "trading_engine.h"
#include "order_book.h"
#include "logger.h"

// Max symbols fixed at compile time — avoids hash map lookup on hot path
static constexpr size_t MAX_SYMBOLS = 8;

struct UserSession {
    UserId  id        = 0;
    uint8_t _pad[60]  = {};  // push inbound to its own cache line
    SPSCQueue<Order, 4096> inbound;
    std::atomic<uint64_t>  next_order_id{1};

    FORCE_INLINE OrderId new_order_id() {
        return ((uint64_t)id << 32)
             | next_order_id.fetch_add(1, std::memory_order_relaxed);
    }
};

class TradingEngine {
    // ── symbol table: fixed array, O(1) lookup by index ──────────────
    struct SymbolEntry {
        char                     name[8] = {};
        std::unique_ptr<OrderBook> book;
    };
    std::array<SymbolEntry, MAX_SYMBOLS> symbols_{};
    size_t  num_symbols_ = 0;

    // ── pre-built dispatch table: symbol char[4] → index ─────────────
    // store first 4 bytes of symbol as uint32 for fast comparison
    std::array<uint32_t, MAX_SYMBOLS> sym_keys_{};

    // ── sessions: snapshot updated rarely, read hot ───────────────────
    std::vector<std::shared_ptr<UserSession>> sessions_;
    std::mutex                                session_mutex_;
    // atomic snapshot pointer — engine reads without lock
    std::atomic<std::vector<std::shared_ptr<UserSession>>*> snap_ptr_{nullptr};
    std::vector<std::shared_ptr<UserSession>> snap_storage_;

    Metrics     metrics_;
    AsyncLogger logger_;
    std::thread engine_thread_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> perf_interval_ns_;

    // ── fast symbol lookup by first 4 bytes ───────────────────────────
    FORCE_INLINE OrderBook* find_book(const char* sym) {
        uint32_t key = 0;
        memcpy(&key, sym, 4);
        for (size_t i = 0; i < num_symbols_; ++i)
            if (sym_keys_[i] == key) return symbols_[i].book.get();
        return nullptr;
    }

    HOT void engine_loop() {
        uint64_t last_perf = now_ns();

        while (LIKELY(running_.load(std::memory_order_relaxed))) {
            // read session snapshot without lock
            auto* snap = snap_ptr_.load(std::memory_order_acquire);
            bool idle = true;

            if (LIKELY(snap != nullptr)) {
                for (auto& sess : *snap) {
                    Order o;
                    // drain entire queue per session before moving on
                    // reduces round-trip overhead for bursty senders
                    while (sess->inbound.pop(o)) {
                        idle = false;
                        dispatch(o);
                    }
                }
            }

            if (UNLIKELY(idle)) {
                // spin with pause — tells CPU this is a wait loop
                // saves power and helps sibling SMT thread
                for (int i = 0; i < 8; ++i) _mm_pause();
            }

            uint64_t now = now_ns();
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
        logger_.log(0, "ORDER side=%s sym=%.7s px=%lld qty=%u id=%llu user=%u",
            o.side == Side::BUY ? "BUY " : "SELL",
            o.symbol, (long long)o.price, o.qty,
            (unsigned long long)o.id, o.user_id);
        book->add_order(o);
        auto [bid, ask] = book->spread();
        logger_.log(1, "BOOK  %.7s bid=%lld ask=%lld bdepth=%zu adepth=%zu",
            o.symbol, (long long)bid, (long long)ask,
            book->bid_depth(), book->ask_depth());
    }

public:
    explicit TradingEngine(const std::string& log_path = "logs/engine.log",
                           uint64_t perf_interval_ms = 1000)
        : logger_(log_path),
          perf_interval_ns_(perf_interval_ms * 1'000'000ULL) {}

    void add_symbol(const char* sym) {
        auto cb = [this](const Trade& t){ logger_.log_trade(t); };
        auto& e = symbols_[num_symbols_];
        std::strncpy(e.name, sym, 7);
        e.book = std::make_unique<OrderBook>(sym, metrics_, cb);
        memcpy(&sym_keys_[num_symbols_], sym, 4);
        ++num_symbols_;
        logger_.log(1, "Symbol '%s' added", sym);
    }

    std::shared_ptr<UserSession> create_session(UserId uid) {
        auto sess = std::make_shared<UserSession>();
        sess->id = uid;
        {
            std::lock_guard<std::mutex> lk(session_mutex_);
            sessions_.push_back(sess);
            // update snapshot
            snap_storage_ = sessions_;
            snap_ptr_.store(&snap_storage_, std::memory_order_release);
        }
        logger_.log(1, "Session created for user %u (total: %zu)",
                    uid, sessions_.size());
        return sess;
    }

    void start() {
        running_ = true;
        engine_thread_ = std::thread(&TradingEngine::engine_loop, this);
        logger_.log(1, "Engine started — %zu symbols", num_symbols_);
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
