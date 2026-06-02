#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <array>
#include <functional>

// ─────────────────────────────────────────────
//  Types & Enums
// ─────────────────────────────────────────────
using OrderId   = uint64_t;
using UserId    = uint32_t;
using Price     = int64_t;   // fixed-point: cents
using Quantity  = uint32_t;
using Timestamp = uint64_t;  // nanoseconds since epoch

inline Timestamp now_ns() {
    return static_cast<Timestamp>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()
        ).count()
    );
}

enum class Side    : uint8_t { BUY = 0, SELL = 1 };
enum class OrderType: uint8_t { LIMIT = 0, MARKET = 1 };
enum class OrderStatus: uint8_t { NEW, PARTIAL, FILLED, CANCELLED };

// ─────────────────────────────────────────────
//  Order
// ─────────────────────────────────────────────
struct alignas(64) Order {          // cache-line aligned
    OrderId    id        = 0;
    UserId     user_id   = 0;
    Price      price     = 0;
    Quantity   qty       = 0;
    Quantity   filled    = 0;
    Side       side      = Side::BUY;
    OrderType  type      = OrderType::LIMIT;
    OrderStatus status   = OrderStatus::NEW;
    Timestamp  ts_in     = 0;
    Timestamp  ts_filled = 0;
    char       symbol[8] = {};

    Quantity remaining() const { return qty - filled; }
};

// ─────────────────────────────────────────────
//  Trade (execution report)
// ─────────────────────────────────────────────
struct Trade {
    OrderId   buy_id, sell_id;
    Price     price;
    Quantity  qty;
    Timestamp ts;
    char      symbol[8];
};

// ─────────────────────────────────────────────
//  Lock-Free SPSC Ring Buffer  (Single Producer Single Consumer)
//  Used per-user thread → engine thread
// ─────────────────────────────────────────────
template<typename T, size_t Capacity>
class SPSCQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power of 2");
    static constexpr size_t MASK = Capacity - 1;

    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
    std::array<T, Capacity>         buf_;

public:
    bool push(const T& item) {
        size_t h = head_.load(std::memory_order_relaxed);
        size_t next = (h + 1) & MASK;
        if (next == tail_.load(std::memory_order_acquire)) return false; // full
        buf_[h] = item;
        head_.store(next, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        size_t t = tail_.load(std::memory_order_relaxed);
        if (t == head_.load(std::memory_order_acquire)) return false; // empty
        item = buf_[t];
        tail_.store((t + 1) & MASK, std::memory_order_release);
        return true;
    }

    size_t size() const {
        size_t h = head_.load(std::memory_order_relaxed);
        size_t t = tail_.load(std::memory_order_relaxed);
        return (h - t) & MASK;
    }
};

// ─────────────────────────────────────────────
//  Performance Metrics (lock-free counters)
// ─────────────────────────────────────────────
struct alignas(64) Metrics {
    std::atomic<uint64_t> orders_received {0};
    std::atomic<uint64_t> orders_matched  {0};
    std::atomic<uint64_t> orders_cancelled{0};
    std::atomic<uint64_t> trades_executed {0};
    std::atomic<uint64_t> total_latency_ns{0};   // sum for avg calc
    std::atomic<uint64_t> max_latency_ns  {0};
    std::atomic<uint64_t> min_latency_ns  {UINT64_MAX};

    void record_latency(uint64_t lat) {
        total_latency_ns.fetch_add(lat, std::memory_order_relaxed);
        orders_matched.fetch_add(1,   std::memory_order_relaxed);
        // CAS loop for max
        uint64_t old = max_latency_ns.load(std::memory_order_relaxed);
        while (lat > old && !max_latency_ns.compare_exchange_weak(old, lat,
               std::memory_order_relaxed));
        // CAS loop for min
        old = min_latency_ns.load(std::memory_order_relaxed);
        while (lat < old && !min_latency_ns.compare_exchange_weak(old, lat,
               std::memory_order_relaxed));
    }

    double avg_latency_us() const {
        uint64_t m = orders_matched.load(std::memory_order_relaxed);
        if (m == 0) return 0.0;
        return (double)total_latency_ns.load(std::memory_order_relaxed) / m / 1000.0;
    }
};
