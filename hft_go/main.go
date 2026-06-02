package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ── Simulation settings ──────────────────────

const (
	numUsers       = 16
	ordersPerUser  = 500
	perfIntervalMs = 500
)

var symbols = []string{"AAPL", "MSFT", "TSLA", "AMZN"}

// ── User goroutine ────────────────────────────

func userWorker(sess *UserSession, orders int, done *atomic.Uint32, wg *sync.WaitGroup) {
	defer wg.Done()

	rng := rand.New(rand.NewSource(int64(sess.ID)*1234567891 + 42))

	for i := 0; i < orders; i++ {
		var o Order
		o.ID = sess.NewOrderID()
		o.UserID = sess.ID
		o.Symbol = symbols[rng.Intn(len(symbols))]

		if rng.Intn(2) == 0 {
			o.Side = Buy
		} else {
			o.Side = Sell
		}

		if rng.Intn(5) == 0 {
			o.Type = Market
			o.Price = 0
		} else {
			o.Type = Limit
			o.Price = Price(9950 + rng.Intn(101)) // 9950..10050
		}

		o.Qty = Quantity(1 + rng.Intn(100))
		o.TsIn = nowNs()

		// spin until queue has space (rare)
		for !sess.Inbound.Push(o) {
			// tiny yield
			time.Sleep(time.Nanosecond)
		}

		// realistic inter-order jitter
		if jitter := rng.Intn(51); jitter > 0 {
			time.Sleep(time.Duration(jitter) * time.Microsecond)
		}
	}
	done.Add(1)
}

// ── main ──────────────────────────────────────

func main() {
	fmt.Printf("\033[1;34m")
	fmt.Printf("╔══════════════════════════════════════════════════╗\n")
	fmt.Printf("║   HFT Learning Engine  –  Go Demo                ║\n")
	fmt.Printf("║   %d users × %d orders  (%d total)          ║\n",
		numUsers, ordersPerUser, numUsers*ordersPerUser)
	fmt.Printf("╚══════════════════════════════════════════════════╝\n")
	fmt.Printf("\033[0m\n")

	engine, err := NewTradingEngine("logs/engine.log", perfIntervalMs)
	if err != nil {
		panic(err)
	}

	for _, sym := range symbols {
		engine.AddSymbol(sym)
	}
	engine.Start()

	time.Sleep(50 * time.Millisecond) // warmup

	// spawn user sessions + goroutines
	var wg sync.WaitGroup
	var doneCount atomic.Uint32
	wallStart := time.Now()

	for uid := UserID(1); uid <= numUsers; uid++ {
		sess := engine.CreateSession(uid)
		wg.Add(1)
		go userWorker(sess, ordersPerUser, &doneCount, &wg)
	}

	wg.Wait()
	engine.logger.Log(LvlInfo, "All users done sending. Draining engine...")
	time.Sleep(200 * time.Millisecond) // drain

	wallMs := float64(time.Since(wallStart).Milliseconds())
	totalOrders := uint64(numUsers * ordersPerUser)
	throughput := float64(totalOrders) / (wallMs / 1000.0)

	m := engine.metrics
	engine.logger.Log(LvlPerf, "═══════════════════ FINAL SUMMARY ═══════════════════")
	engine.logger.Log(LvlPerf, "Wall time      : %.2f ms", wallMs)
	engine.logger.Log(LvlPerf, "Total orders   : %d", totalOrders)
	engine.logger.Log(LvlPerf, "Throughput     : %.0f orders/sec", throughput)
	engine.logger.Log(LvlPerf, "Orders recv    : %d", m.OrdersReceived.Load())
	engine.logger.Log(LvlPerf, "Trades exec    : %d", m.TradesExecuted.Load())
	engine.logger.Log(LvlPerf, "Cancelled      : %d", m.OrdersCancelled.Load())
	engine.logger.Log(LvlPerf, "Avg latency    : %.2f µs", m.AvgLatencyUs())

	engine.Stop()

	fmt.Printf("\n\033[1;32mLog saved to: logs/engine.log\033[0m\n\n")
}
