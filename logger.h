#pragma once
#include <atomic>
#include <thread>
#include <fstream>
#include <sstream>
#include <string>
#include <array>
#include <cstdio>
#include <cstring>
#include <cstdarg>
#include "trading_engine.h"

// ─────────────────────────────────────────────
//  Async Logger
//  Producer threads write log entries to a
//  lock-free ring buffer; a dedicated I/O
//  thread flushes them to disk — zero blocking
//  on the hot path.
// ─────────────────────────────────────────────

struct LogEntry {
    char     msg[256];
    uint8_t  level;  // 0=DEBUG 1=INFO 2=WARN 3=TRADE 4=PERF
    Timestamp ts;
};

static constexpr const char* LEVEL_STR[] = {"DEBUG","INFO ","WARN ","TRADE","PERF "};

class AsyncLogger {
    static constexpr size_t BUF_SIZE = 1 << 14; // 16k entries

    SPSCQueue<LogEntry, BUF_SIZE> queue_;
    std::ofstream                 file_;
    std::thread                   writer_;
    std::atomic<bool>             running_{true};

    void writer_loop() {
        LogEntry e;
        while (running_.load(std::memory_order_relaxed) || queue_.size() > 0) {
            if (queue_.pop(e)) {
                // nanoseconds → human readable
                uint64_t ns = e.ts;
                uint64_t sec  = ns / 1'000'000'000ULL;
                uint64_t msec = (ns % 1'000'000'000ULL) / 1'000'000;
                uint64_t usec = (ns % 1'000'000ULL) / 1'000;
                uint64_t nsec = (ns % 1'000ULL);

                char line[320];
                snprintf(line, sizeof(line),
                    "[%s][%llu.%03llu.%03llu.%03llu] %s\n",
                    LEVEL_STR[e.level],
                    (unsigned long long)sec,
                    (unsigned long long)msec,
                    (unsigned long long)usec,
                    (unsigned long long)nsec,
                    e.msg
                );
                file_ << line;
                file_.flush();

                // also print to stdout (coloured)
                const char* color = "\033[0m";
                if (e.level == 1) color = "\033[32m"; // green  INFO
                if (e.level == 2) color = "\033[33m"; // yellow WARN
                if (e.level == 3) color = "\033[36m"; // cyan   TRADE
                if (e.level == 4) color = "\033[35m"; // purple PERF
                printf("%s%s\033[0m", color, line);
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        }
    }

public:
    explicit AsyncLogger(const std::string& path) : file_(path, std::ios::app) {
        writer_ = std::thread(&AsyncLogger::writer_loop, this);
    }

    ~AsyncLogger() {
        running_ = false;
        if (writer_.joinable()) writer_.join();
    }

    void log(uint8_t level, const char* fmt, ...) {
        LogEntry e;
        e.ts    = now_ns();
        e.level = level;
        va_list args;
        va_start(args, fmt);
        vsnprintf(e.msg, sizeof(e.msg), fmt, args);
        va_end(args);
        // spin briefly if full (should be rare)
        while (!queue_.push(e)) std::this_thread::yield();
    }

    void log_trade(const Trade& t) {
        log(3, "EXEC  sym=%.7s buy#%llu sell#%llu price=%lld qty=%u",
            t.symbol,
            (unsigned long long)t.buy_id,
            (unsigned long long)t.sell_id,
            (long long)t.price,
            t.qty);
    }

    void log_perf(const Metrics& m) {
        uint64_t recv = m.orders_received.load(std::memory_order_relaxed);
        uint64_t trd  = m.trades_executed.load(std::memory_order_relaxed);
        uint64_t can  = m.orders_cancelled.load(std::memory_order_relaxed);
        uint64_t minL = m.min_latency_ns.load(std::memory_order_relaxed);
        uint64_t maxL = m.max_latency_ns.load(std::memory_order_relaxed);
        log(4,
            "PERF  recv=%llu trades=%llu cancelled=%llu "
            "avg_lat=%.2f µs  min=%llu ns  max=%llu ns",
            (unsigned long long)recv,
            (unsigned long long)trd,
            (unsigned long long)can,
            m.avg_latency_us(),
            (unsigned long long)(minL == UINT64_MAX ? 0 : minL),
            (unsigned long long)maxL);
    }
};
