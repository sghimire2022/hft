// ═══════════════════════════════════════════════════════════════════
//  HFT Learning Engine  –  main.cpp
//  Simulates N concurrent users sending orders against a live engine.
//  All logs go to stdout (coloured) AND logs/engine.log
// ═══════════════════════════════════════════════════════════════════
#include <iostream>
#include <thread>
#include <vector>
#include <random>
#include <atomic>
#include <cstring>
#include <chrono>
#include "engine.h"

// ── Simulation settings ─────────────────────────────────────────────
static constexpr uint32_t NUM_USERS        = 16;     // concurrent sessions
static constexpr uint32_t ORDERS_PER_USER  = 500;    // orders each user fires
static constexpr uint64_t PERF_INTERVAL_MS = 500;    // perf log every 500 ms

// ── User simulation thread ──────────────────────────────────────────
void user_thread(std::shared_ptr<UserSession> sess,
                 uint32_t orders,
                 std::atomic<uint32_t>& done_counter)
{
    // each thread has its own RNG – no contention
    std::mt19937 rng(sess->id * 1234567891ULL + 42);
    std::uniform_int_distribution<int>      side_dist(0, 1);
    std::uniform_int_distribution<int>      type_dist(0, 4); // 80% LIMIT, 20% MARKET
    std::uniform_int_distribution<int64_t>  px_dist(9950, 10050); // ~100 spread
    std::uniform_int_distribution<uint32_t> qty_dist(1, 100);
    std::uniform_int_distribution<int>      delay_dist(0, 50); // µs jitter

    const char* symbols[] = {"AAPL","MSFT","TSLA","AMZN"};
    std::uniform_int_distribution<int> sym_dist(0, 3);

    for (uint32_t i = 0; i < orders; ++i) {
        Order o;
        o.id      = sess->new_order_id();
        o.user_id = sess->id;
        o.side    = (side_dist(rng) == 0) ? Side::BUY : Side::SELL;
        o.type    = (type_dist(rng) == 0) ? OrderType::MARKET : OrderType::LIMIT;
        o.price   = (o.type == OrderType::MARKET) ? 0 : px_dist(rng);
        o.qty     = qty_dist(rng);
        o.ts_in   = now_ns();
        std::strncpy(o.symbol, symbols[sym_dist(rng)], 7);

        // spin until queue has space (rare)
        while (!sess->inbound.push(o)) std::this_thread::yield();

        // realistic inter-order delay
        if (delay_dist(rng) > 0)
            std::this_thread::sleep_for(std::chrono::microseconds(delay_dist(rng)));
    }
    done_counter.fetch_add(1, std::memory_order_relaxed);
}

// ── main ─────────────────────────────────────────────────────────────
int main() {
    // ensure logs directory exists
    system("mkdir -p logs");

    printf("\033[1;34m");
    printf("╔══════════════════════════════════════════════════╗\n");
    printf("║   HFT Learning Engine  –  C++ Demo               ║\n");
    printf("║   %u users × %u orders  (%u total)          ║\n",
           NUM_USERS, ORDERS_PER_USER, NUM_USERS * ORDERS_PER_USER);
    printf("╚══════════════════════════════════════════════════╝\n");
    printf("\033[0m\n");

    // ── create & configure engine ─────────────────────────────────
    TradingEngine engine("logs/engine.log", PERF_INTERVAL_MS);
    engine.add_symbol("AAPL");
    engine.add_symbol("MSFT");
    engine.add_symbol("TSLA");
    engine.add_symbol("AMZN");
    engine.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(50)); // engine warmup

    // ── spawn user sessions + threads ────────────────────────────
    std::vector<std::thread>   threads;
    std::atomic<uint32_t>      done_counter{0};

    auto wall_start = std::chrono::high_resolution_clock::now();

    for (uint32_t uid = 1; uid <= NUM_USERS; ++uid) {
        auto sess = engine.create_session(uid);
        threads.emplace_back(user_thread, sess, ORDERS_PER_USER,
                             std::ref(done_counter));
    }

    // ── wait for all users to finish sending ─────────────────────
    for (auto& t : threads) t.join();

    // let engine drain remaining queued orders
    engine.log().log(1, "All users done sending. Draining engine...");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto wall_end  = std::chrono::high_resolution_clock::now();
    double wall_ms = std::chrono::duration<double, std::milli>(
                         wall_end - wall_start).count();

    uint64_t total_orders = (uint64_t)NUM_USERS * ORDERS_PER_USER;
    double throughput     = total_orders / (wall_ms / 1000.0);

    // ── final summary ─────────────────────────────────────────────
    engine.log().log(4, "═══════════════════ FINAL SUMMARY ═══════════════════");
    engine.log().log(4, "Wall time      : %.2f ms", wall_ms);
    engine.log().log(4, "Total orders   : %llu", (unsigned long long)total_orders);
    engine.log().log(4, "Throughput     : %.0f orders/sec", throughput);

    auto& m = engine.metrics();
    engine.log().log(4, "Orders recv    : %llu",
                     (unsigned long long)m.orders_received.load());
    engine.log().log(4, "Trades exec    : %llu",
                     (unsigned long long)m.trades_executed.load());
    engine.log().log(4, "Cancelled      : %llu",
                     (unsigned long long)m.orders_cancelled.load());
    engine.log().log(4, "Avg latency    : %.2f µs", m.avg_latency_us());

    engine.stop();

    printf("\n\033[1;32mLog saved to: logs/engine.log\033[0m\n\n");
    return 0;
}
