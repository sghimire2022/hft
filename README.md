# HFT Learning Engine

A high-frequency trading matching engine built in **C++**, **Go**, and **Rust** — for learning purposes.

Benchmarked on AWS EC2 `t2.micro` (1 vCPU, 1GB RAM).

---

## What It Does

- Price-time priority order matching (bids/asks)
- 16 concurrent user sessions
- 4 symbols: AAPL, MSFT, TSLA, AMZN
- Lock-free SPSC queue per user
- Single-threaded matching core (no locks on order books)
- Async logger (background thread, never blocks hot path)
- Nanosecond latency metrics

---

## Results (AWS EC2 t2.micro)

| Metric | C++ | Go | Rust |
|---|---|---|---|
| Throughput | 29,870/sec | 15,238/sec | 20,619/sec |
| Avg latency | 15,055 µs | **232 µs** | 16,978 µs |
| Min latency | 1,072 ns | 1,554 ns | **756 ns** |
| Max latency | 62,233 ms | **16,776 ms** | 67,679 ms |

> Optimized C++ (no jitter): **~101,000 orders/sec**

---

## Project Structure

```
hft/
├── C++/
│   ├── trading_engine.h   # Core types, SPSC queue, metrics
│   ├── order_book.h       # Price-time priority matching
│   ├── engine.h           # Session manager, dispatch loop
│   ├── logger.h           # Async ring-buffer logger
│   ├── main.cpp           # Simulation entry point
│   └── Makefile
│
├── Go/
│   ├── types.go           # Core types, SPSC queue, metrics
│   ├── orderbook.go       # Order book matching
│   ├── engine.go          # Trading engine
│   ├── logger.go          # Async logger
│   ├── main.go            # Simulation entry point
│   └── go.mod
│
└── Rust/
    ├── src/
    │   ├── types.rs       # Core types, SPSC queue, metrics
    │   ├── orderbook.rs   # Order book matching
    │   ├── engine.rs      # Trading engine
    │   ├── logger.rs      # Async logger
    │   └── main.rs        # Simulation entry point
    └── Cargo.toml
```

---

## How to Run

### C++
```bash
# Install compiler (Amazon Linux)
sudo yum install -y gcc-c++

cd C++
make run
```

### Go
```bash
# Install Go (Amazon Linux)
sudo yum install -y golang

cd Go
go run .
```

### Rust
```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

cd Rust
cargo run --release
```

---

## Log Output

All versions produce coloured logs to stdout and save to `logs/engine.log`:

```
[DEBUG] ORDER side=BUY  sym=AAPL px=10003 qty=40 id=... user=5
[TRADE] EXEC  sym=AAPL buy#... sell#... price=10003 qty=40
[INFO ] BOOK  AAPL bid=10003 ask=10025 bdepth=19 adepth=17
[PERF ] PERF  recv=8000 trades=7344 avg_lat=232.39 µs min=1554 ns max=16776934 ns
```

| Level | Colour | Meaning |
|---|---|---|
| `DEBUG` | white | Every order received |
| `TRADE` | cyan | Every match execution |
| `INFO` | green | Order book state after each order |
| `PERF` | magenta | Performance metrics (every 500ms + final) |

---

## Key Design Decisions

**Lock-free SPSC queue** — each user gets a dedicated ring buffer. No mutex between user threads and the engine.

**Single-threaded matching core** — all order book mutations happen on one thread. No locking needed on the books.

**Cache-line alignment** — `Order` struct is exactly 64 bytes (`alignas(64)`). No false sharing between orders.

**Async logger** — trades and metrics are logged on a background I/O thread. The matching loop never touches disk.

---

## Optimizations (C++)

| Change | File | Impact |
|---|---|---|
| `clock_gettime(CLOCK_MONOTONIC)` | `trading_engine.h` | 10ns vs 25ns per timestamp |
| Cached head/tail in SPSC | `trading_engine.h` | Fewer atomic reads on hot path |
| `_mm_pause()` idle spin | `engine.h` | ~10ns wakeup vs 1µs sleep |
| O(1) symbol lookup by `uint32` key | `engine.h` | No `std::string` on hot path |
| `index_.reserve(4096)` | `order_book.h` | No hash map rehash mid-trade |
| `-O3 -march=native -flto` | `Makefile` | AVX/SSE + cross-file inlining |

---

## Language Comparison

| | C++ | Go | Rust |
|---|---|---|---|
| GC pauses | None | ~100µs | None |
| Memory safety | Manual | ✅ | ✅ compile-time |
| Concurrency model | `std::thread` | Goroutines | `std::thread` |
| Best for HFT | ✅ Throughput | ✅ Avg latency | ✅ Min latency |

---

## Requirements

| Language | Minimum version |
|---|---|
| C++ | g++ 11+ |
| Go | 1.21+ |
| Rust | 1.70+ |

---

*Built for learning. Not financial advice. Not production-ready.*