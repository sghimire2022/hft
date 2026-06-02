#pragma once
#include <thread>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <memory>
#include "trading_engine.h"
#include "order_book.h"
#include "logger.h"

// ─────────────────────────────────────────────
//  Per-user session  (one SPSC queue each)
// ─────────────────────────────────────────────
struct UserSession {
    UserId  id;
    SPSCQueue<Order, 1024> inbound;   // user → engine
    std::atomic<uint64_t>  next_order_id{1};

    OrderId new_order_id() {
        // pack user_id into high 32 bits
        return ((uint64_t)id << 32) | next_order_id.fetch_add(1, std::memory_order_relaxed);
    }
};

// ─────────────────────────────────────────────
//  Engine  – single-threaded matching core
//  (no lock needed on the order books; all
//   mutations happen inside this one thread)
// ─────────────────────────────────────────────
class TradingEngine {
    std::unordered_map<std::string, std::unique_ptr<OrderBook>> books_;
    std::vector<std::shared_ptr<UserSession>>                   sessions_;
    std::mutex                                                  session_mutex_;

    Metrics     metrics_;
    AsyncLogger logger_;

    std::thread engine_thread_;
    std::atomic<bool> running_{false};

    std::atomic<uint64_t> perf_report_interval_ns_;

    // ── matching loop (runs on dedicated core) ──
    void engine_loop() {
        uint64_t last_perf = now_ns();

        while (running_.load(std::memory_order_relaxed)) {
            // drain all user queues (round-robin, starvation-free)
            bool idle = true;
            {
                // snapshot session list without holding lock
                std::vector<std::shared_ptr<UserSession>> snap;
                {
                    std::lock_guard<std::mutex> lk(session_mutex_);
                    snap = sessions_;
                }
                for (auto& sess : snap) {
                    Order o;
                    while (sess->inbound.pop(o)) {
                        idle = false;
                        dispatch(o);
                    }
                }
            }

            if (idle) {
                // brief spin-sleep keeps latency <1µs when traffic resumes
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            }

            // periodic performance report
            uint64_t now = now_ns();
            if (now - last_perf >= perf_report_interval_ns_.load(std::memory_order_relaxed)) {
                logger_.log_perf(metrics_);
                last_perf = now;
            }
        }
    }

    void dispatch(Order& o) {
        auto it = books_.find(std::string(o.symbol));
        if (it == books_.end()) {
            logger_.log(2, "Unknown symbol '%.7s' in order #%llu – dropped",
                        o.symbol, (unsigned long long)o.id);
            return;
        }
        logger_.log(0, "ORDER side=%s sym=%.7s px=%lld qty=%u id=%llu user=%u",
                    o.side == Side::BUY ? "BUY " : "SELL",
                    o.symbol,
                    (long long)o.price, o.qty,
                    (unsigned long long)o.id, o.user_id);
        it->second->add_order(o);

        auto [bid, ask] = it->second->spread();
        logger_.log(1, "BOOK  %.7s bid=%lld ask=%lld bdepth=%zu adepth=%zu",
                    o.symbol, (long long)bid, (long long)ask,
                    it->second->bid_depth(), it->second->ask_depth());
    }

public:
    explicit TradingEngine(const std::string& log_path = "logs/engine.log",
                           uint64_t perf_interval_ms = 1000)
        : logger_(log_path),
          perf_report_interval_ns_(perf_interval_ms * 1'000'000ULL)
    {}

    // ── lifecycle ────────────────────────────
    void add_symbol(const char* sym) {
        auto cb = [this](const Trade& t){ logger_.log_trade(t); };
        books_.emplace(sym, std::make_unique<OrderBook>(sym, metrics_, cb));
        logger_.log(1, "Symbol '%s' added to engine", sym);
    }

    std::shared_ptr<UserSession> create_session(UserId uid) {
        auto sess = std::make_shared<UserSession>();
        sess->id = uid;
        {
            std::lock_guard<std::mutex> lk(session_mutex_);
            sessions_.push_back(sess);
        }
        logger_.log(1, "Session created for user %u  (total sessions: %zu)",
                    uid, sessions_.size());
        return sess;
    }

    void start() {
        running_ = true;
        engine_thread_ = std::thread(&TradingEngine::engine_loop, this);
        logger_.log(1, "Engine started — %zu symbols", books_.size());
    }

    void stop() {
        running_ = false;
        if (engine_thread_.joinable()) engine_thread_.join();
        logger_.log(1, "Engine stopped");
        logger_.log_perf(metrics_);
    }

    Metrics& metrics() { return metrics_; }
    AsyncLogger& log()  { return logger_;  }
};
