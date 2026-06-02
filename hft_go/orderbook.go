package main

import (
	"container/list"
	"sort"
)

// ─────────────────────────────────────────────
//  PriceLevel — FIFO queue of orders at one price
// ─────────────────────────────────────────────

type PriceLevel struct {
	price  Price
	orders *list.List // *Order elements
}

// ─────────────────────────────────────────────
//  OrderBook — price-time priority matching
//
//  Bids: sorted descending (highest first)
//  Asks: sorted ascending  (lowest first)
// ─────────────────────────────────────────────

type TradeCallback func(Trade)

type OrderBook struct {
	symbol string
	bids   []*PriceLevel // descending
	asks   []*PriceLevel // ascending

	// order-id → (side, *list.Element) for O(1) cancel
	index map[OrderID]*indexEntry

	onTrade TradeCallback
	metrics *Metrics
}

type indexEntry struct {
	side  Side
	price Price
	elem  *list.Element
}

func NewOrderBook(symbol string, m *Metrics, cb TradeCallback) *OrderBook {
	return &OrderBook{
		symbol:  symbol,
		index:   make(map[OrderID]*indexEntry),
		onTrade: cb,
		metrics: m,
	}
}

// ── helpers ──────────────────────────────────

func (b *OrderBook) findOrCreateBid(p Price) *PriceLevel {
	for _, lvl := range b.bids {
		if lvl.price == p {
			return lvl
		}
	}
	lvl := &PriceLevel{price: p, orders: list.New()}
	b.bids = append(b.bids, lvl)
	sort.Slice(b.bids, func(i, j int) bool { return b.bids[i].price > b.bids[j].price })
	return lvl
}

func (b *OrderBook) findOrCreateAsk(p Price) *PriceLevel {
	for _, lvl := range b.asks {
		if lvl.price == p {
			return lvl
		}
	}
	lvl := &PriceLevel{price: p, orders: list.New()}
	b.asks = append(b.asks, lvl)
	sort.Slice(b.asks, func(i, j int) bool { return b.asks[i].price < b.asks[j].price })
	return lvl
}

func (b *OrderBook) removeBidLevel(p Price) {
	for i, lvl := range b.bids {
		if lvl.price == p {
			b.bids = append(b.bids[:i], b.bids[i+1:]...)
			return
		}
	}
}

func (b *OrderBook) removeAskLevel(p Price) {
	for i, lvl := range b.asks {
		if lvl.price == p {
			b.asks = append(b.asks[:i], b.asks[i+1:]...)
			return
		}
	}
}

func (b *OrderBook) makeTrade(agg, rest *Order, qty Quantity) Trade {
	t := Trade{
		Price:  rest.Price,
		Qty:    qty,
		Symbol: b.symbol,
		Ts:     nowNs(),
	}
	if agg.Side == Buy {
		t.BuyID, t.SellID = agg.ID, rest.ID
	} else {
		t.BuyID, t.SellID = rest.ID, agg.ID
	}
	return t
}

// ── match ─────────────────────────────────────

func (b *OrderBook) matchAgainstAsks(agg *Order) {
	for len(b.asks) > 0 && agg.Remaining() > 0 {
		lvl := b.asks[0]
		if agg.Type == Limit && agg.Price < lvl.price {
			break
		}
		for lvl.orders.Len() > 0 && agg.Remaining() > 0 {
			elem := lvl.orders.Front()
			rest := elem.Value.(*Order)

			fill := agg.Remaining()
			if rest.Remaining() < fill {
				fill = rest.Remaining()
			}

			agg.Filled += fill
			rest.Filled += fill

			tr := b.makeTrade(agg, rest, fill)
			b.metrics.TradesExecuted.Add(1)
			lat := uint64(tr.Ts - agg.TsIn)
			b.metrics.RecordLatency(lat)

			if b.onTrade != nil {
				b.onTrade(tr)
			}

			if rest.Remaining() == 0 {
				rest.Status = StatusFilled
				rest.TsFilled = tr.Ts
				delete(b.index, rest.ID)
				lvl.orders.Remove(elem)
			} else {
				rest.Status = StatusPartial
			}
		}
		if lvl.orders.Len() == 0 {
			b.removeAskLevel(lvl.price)
		}
	}
}

func (b *OrderBook) matchAgainstBids(agg *Order) {
	for len(b.bids) > 0 && agg.Remaining() > 0 {
		lvl := b.bids[0]
		if agg.Type == Limit && agg.Price > lvl.price {
			break
		}
		for lvl.orders.Len() > 0 && agg.Remaining() > 0 {
			elem := lvl.orders.Front()
			rest := elem.Value.(*Order)

			fill := agg.Remaining()
			if rest.Remaining() < fill {
				fill = rest.Remaining()
			}

			agg.Filled += fill
			rest.Filled += fill

			tr := b.makeTrade(agg, rest, fill)
			b.metrics.TradesExecuted.Add(1)
			lat := uint64(tr.Ts - agg.TsIn)
			b.metrics.RecordLatency(lat)

			if b.onTrade != nil {
				b.onTrade(tr)
			}

			if rest.Remaining() == 0 {
				rest.Status = StatusFilled
				rest.TsFilled = tr.Ts
				delete(b.index, rest.ID)
				lvl.orders.Remove(elem)
			} else {
				rest.Status = StatusPartial
			}
		}
		if lvl.orders.Len() == 0 {
			b.removeBidLevel(lvl.price)
		}
	}
}

// ── public API ───────────────────────────────

func (b *OrderBook) AddOrder(o Order) {
	b.metrics.OrdersReceived.Add(1)

	if o.Side == Buy {
		b.matchAgainstAsks(&o)
	} else {
		b.matchAgainstBids(&o)
	}

	// rest unfilled limit orders
	if o.Type == Limit && o.Remaining() > 0 {
		if o.Filled > 0 {
			o.Status = StatusPartial
		} else {
			o.Status = StatusNew
		}
		ptr := new(Order)
		*ptr = o

		if o.Side == Buy {
			lvl := b.findOrCreateBid(o.Price)
			elem := lvl.orders.PushBack(ptr)
			b.index[o.ID] = &indexEntry{side: Buy, price: o.Price, elem: elem}
		} else {
			lvl := b.findOrCreateAsk(o.Price)
			elem := lvl.orders.PushBack(ptr)
			b.index[o.ID] = &indexEntry{side: Sell, price: o.Price, elem: elem}
		}
	}
}

func (b *OrderBook) CancelOrder(id OrderID) bool {
	entry, ok := b.index[id]
	if !ok {
		return false
	}
	if entry.side == Buy {
		for _, lvl := range b.bids {
			if lvl.price == entry.price {
				lvl.orders.Remove(entry.elem)
				if lvl.orders.Len() == 0 {
					b.removeBidLevel(entry.price)
				}
				break
			}
		}
	} else {
		for _, lvl := range b.asks {
			if lvl.price == entry.price {
				lvl.orders.Remove(entry.elem)
				if lvl.orders.Len() == 0 {
					b.removeAskLevel(entry.price)
				}
				break
			}
		}
	}
	delete(b.index, id)
	b.metrics.OrdersCancelled.Add(1)
	return true
}

func (b *OrderBook) Spread() (bid, ask Price) {
	if len(b.bids) > 0 {
		bid = b.bids[0].price
	}
	if len(b.asks) > 0 {
		ask = b.asks[0].price
	}
	return
}

func (b *OrderBook) BidDepth() int { return len(b.bids) }
func (b *OrderBook) AskDepth() int { return len(b.asks) }
