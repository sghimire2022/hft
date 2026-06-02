#pragma once
#include <atomic>
#include <cstdint>
#include <array>
#include <time.h>

#define LIKELY(x)    __builtin_expect(!!(x), 1)
#define UNLIKELY(x)  __builtin_expect(!!(x), 0)
#define FORCE_INLINE __attribute__((always_inline)) inline
#define HOT          __attribute__((hot))

using OrderId   = uint64_t;
using UserId    = uint32_t;
using Price     = int64_t;
using Quantity  = uint32_t;
using Timestamp = uint64_t;

// ── Faster clock: VDSO-mapped, no syscall ────────────────────────────
FORCE_INLINE Timestamp now_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (Timestamp)ts.tv_sec * 1'000'000'000ULL + (Timestamp)ts.tv_nsec;
}

enum class Side       : uint8_t { BUY = 0, SELL = 1 };
enum class OrderType  : uint8_t { LIMIT = 0, MARKET = 1 };
enum class OrderStatus: uint8_t { NEW, PARTIAL, FILLED, CANCELLED };

// ── Order: hot fields first, one cache line ───────────────────────────
struct alignas(64) Order {
    OrderId    id         = 0;   //  8
    Price      price      = 0;   //  8
    Timestamp  ts_in      = 0;   //  8
    Quantity   qty        = 0;   //  4
    Quantity   filled     = 0;   //  4
    UserId     user_id    = 0;   //  4
    Side       side       = Side::BUY;
    OrderType  type       = OrderType::LIMIT;
    OrderStatus status    = OrderStatus::NEW;
    uint8_t    _pad       = 0;
    char       symbol[8]  = {};  //  8
    Timestamp  ts_filled  = 0;   //  8
    // total: 56 bytes → padded to 64 by alignas
    uint8_t    _pad2[8]   = {};

    FORCE_INLINE Quantity remaining() const { return qty - filled; }
};
static_assert(sizeof(Order) == 64, "Order must be one cache line");

struct Trade {
    OrderId   buy_id, sell_id;
    Price     price;
    Quantity  qty;
    Timestamp ts;
    char      symbol[8];
};

// ── SPSC Queue with cached head/tail ─────────────────────────────────
template<typename T, size_t Capacity = 4096>
class SPSCQueue {
    static_assert((Capacity & (Capacity - 1)) == 0);
    static constexpr size_t MASK = Capacity - 1;

    alignas(64) std::atomic<size_t> head_{0};
    size_t  cached_tail_ = 0;
    uint8_t _pad0[64 - 2*sizeof(size_t)] = {};

    alignas(64) std::atomic<size_t> tail_{0};
    size_t  cached_head_ = 0;
    uint8_t _pad1[64 - 2*sizeof(size_t)] = {};

    alignas(64) std::array<T, Capacity> buf_;

public:
    HOT FORCE_INLINE bool push(const T& item) {
        const size_t h    = head_.load(std::memory_order_relaxed);
        const size_t next = (h + 1) & MASK;
        if (UNLIKELY(next == cached_tail_)) {
            cached_tail_ = tail_.load(std::memory_order_acquire);
            if (UNLIKELY(next == cached_tail_)) return false;
        }
        buf_[h] = item;
        head_.store(next, std::memory_order_release);
        return true;
    }

    HOT FORCE_INLINE bool pop(T& item) {
        const size_t t = tail_.load(std::memory_order_relaxed);
        if (UNLIKELY(t == cached_head_)) {
            cached_head_ = head_.load(std::memory_order_acquire);
            if (UNLIKELY(t == cached_head_)) return false;
        }
        __builtin_prefetch(&buf_[(t + 1) & MASK], 0, 1);
        item = buf_[t];
        tail_.store((t + 1) & MASK, std::memory_order_release);
        return true;
    }

    FORCE_INLINE bool empty() const {
        return head_.load(std::memory_order_relaxed)
            == tail_.load(std::memory_order_relaxed);
    }
};

// ── Metrics: each counter on its own cache line ───────────────────────
struct Metrics {
    alignas(64) std::atomic<uint64_t> orders_received {0};
    alignas(64) std::atomic<uint64_t> orders_matched  {0};
    alignas(64) std::atomic<uint64_t> orders_cancelled{0};
    alignas(64) std::atomic<uint64_t> trades_executed {0};
    alignas(64) std::atomic<uint64_t> total_latency_ns{0};
    alignas(64) std::atomic<uint64_t> max_latency_ns  {0};
    alignas(64) std::atomic<uint64_t> min_latency_ns  {UINT64_MAX};

    HOT FORCE_INLINE void record_latency(uint64_t lat) {
        total_latency_ns.fetch_add(lat, std::memory_order_relaxed);
        orders_matched.fetch_add(1,     std::memory_order_relaxed);
        uint64_t old = max_latency_ns.load(std::memory_order_relaxed);
        while (lat > old && !max_latency_ns.compare_exchange_weak(
            old, lat, std::memory_order_relaxed));
        old = min_latency_ns.load(std::memory_order_relaxed);
        while (lat < old && !min_latency_ns.compare_exchange_weak(
            old, lat, std::memory_order_relaxed));
    }

    double avg_latency_us() const {
        uint64_t m = orders_matched.load(std::memory_order_relaxed);
        if (!m) return 0.0;
        return (double)total_latency_ns.load(std::memory_order_relaxed) / m / 1000.0;
    }
};