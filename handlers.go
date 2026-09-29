package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	StatusCart       = "CART"
	StatusPlaced     = "PLACED"
	StatusReserved   = "RESERVED"
	StatusPaid       = "PAID"
	StatusOutOfStock = "OUT_OF_STOCK"
	StatusDeclined   = "DECLINED"
	StatusExpired    = "EXPIRED"
)

// querier is satisfied by both *sql.DB and *sql.Tx.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type Item struct {
	ProductID  string `json:"product_id"`
	Quantity   int    `json:"quantity"`
	PriceCents int64  `json:"price_cents"`
}

func loadItems(ctx context.Context, q querier, orderID string) ([]Item, error) {
	rows, err := q.QueryContext(ctx, "SELECT product_id, quantity, price_cents FROM order_items WHERE order_id=$1", orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Item
	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.ProductID, &it.Quantity, &it.PriceCents); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return items, rows.Err()
}
func totalCents(items []Item) int64 {
	var t int64
	for _, it := range items {
		t += int64(it.Quantity) * it.PriceCents
	}
	return t
}

// withTx commits if fn returns nil, otherwise rolls back and returns fn's error.
func withTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// markProcessed records a message id; false means it was already processed and the delivery is a duplicate.
func markProcessed(ctx context.Context, tx *sql.Tx, id string) (bool, error) {
	return affectedOne(tx.ExecContext(ctx, "INSERT INTO processed_messages (id) VALUES ($1) ON CONFLICT DO NOTHING", id))
}

// casStatus moves the order from → to only if it is still in from; false means another transition won.
func casStatus(ctx context.Context, tx *sql.Tx, orderID, from, to string) (bool, error) {
	return affectedOne(tx.ExecContext(ctx, "UPDATE orders SET status=$3, updated_at=now() WHERE id=$1 AND status=$2", orderID, from, to))
}

func affectedOne(res sql.Result, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}
func stockHandler(db *sql.DB) Handler {
	return func(ctx context.Context, ch *amqp.Channel, env Envelope) error {
		switch env.Type {
		case KeyOrderPlaced:
			return reserve(ctx, db, ch, env)
		case KeyPaymentDeclined:
			return release(ctx, db, env, StatusDeclined)
		case KeyReservationExpired:
			return release(ctx, db, env, StatusExpired)
		}
		return fmt.Errorf("%w: stock got %s", ErrPermanent, env.Type)
	}
}

var errStockShort = errors.New("stock short")

const (
	stockReserveSQL = "UPDATE products SET available = available - $1 WHERE id = $2 AND available >= $1"
	stockReleaseSQL = "UPDATE products SET available = available + $1 WHERE id = $2"
)

// reserve runs the order status CAS before touching stock: the api publishes order.placed inside its
// still-open tx, so the CAS waits on that row lock and sees the committed status (or 0 rows on rollback).
// Out of stock rolls back every item and moves the order to OUT_OF_STOCK in a fresh tx.
func reserve(ctx context.Context, db *sql.DB, ch *amqp.Channel, env Envelope) error {
	won, err := stockTransition(ctx, db, env, StatusPlaced, StatusReserved, stockReserveSQL)
	if errors.Is(err, errStockShort) {
		if won, err = stockTransition(ctx, db, env, StatusPlaced, StatusOutOfStock, ""); err != nil || !won {
			return err
		}
		log.Printf("stock: out_of_stock order_id=%s", env.OrderID)
		return stockPublish(ctx, ch, ExOrders, KeyReservationRejected, env.OrderID)
	}
	if err != nil || !won {
		return err
	}
	log.Printf("stock: reserved order_id=%s", env.OrderID)
	if err := stockPublish(ctx, ch, ExOrders, KeyReservationCreated, env.OrderID); err != nil {
		return err
	}
	return stockPublish(ctx, ch, ExExpiry, KeyReservationExpired, env.OrderID)
}

func release(ctx context.Context, db *sql.DB, env Envelope, to string) error {
	won, err := stockTransition(ctx, db, env, StatusReserved, to, stockReleaseSQL)
	if won {
		log.Printf("stock: released order_id=%s status=%s", env.OrderID, to)
	}
	return err
}

// stockTransition, in one tx: dedupes env.ID, moves the order from → to and, only if that CAS won,
// runs itemSQL for every item. Items go in product id order so concurrent txs lock rows in the same order.
// Returns true only when this call made the transition.
func stockTransition(ctx context.Context, db *sql.DB, env Envelope, from, to, itemSQL string) (bool, error) {
	var first, won bool
	err := withTx(ctx, db, func(tx *sql.Tx) error {
		var err error
		if first, err = markProcessed(ctx, tx, env.ID); err != nil || !first {
			return err
		}
		if won, err = casStatus(ctx, tx, env.OrderID, from, to); err != nil || !won || itemSQL == "" {
			return err
		}
		items, err := loadItems(ctx, tx, env.OrderID)
		if err != nil {
			return err
		}
		slices.SortFunc(items, func(a, b Item) int { return strings.Compare(a.ProductID, b.ProductID) })
		for _, it := range items {
			ok, err := affectedOne(tx.ExecContext(ctx, itemSQL, it.Quantity, it.ProductID))
			if err != nil {
				return err
			}
			if !ok {
				return errStockShort
			}
		}
		return nil
	})
	switch {
	case err != nil:
		return false, err
	case !first:
		log.Printf("stock: duplicate id=%s", env.ID)
	case !won:
		log.Printf("stock: noop order_id=%s", env.OrderID)
	}
	return first && won, nil
}

func stockPublish(ctx context.Context, ch *amqp.Channel, exchange, typ, orderID string) error {
	e, err := NewEnvelope(typ, orderID, nil)
	if err != nil {
		return err
	}
	return Publish(ctx, ch, exchange, e)
}

func paymentHandler(db *sql.DB, failRate float64) Handler {
	return func(ctx context.Context, ch *amqp.Channel, env Envelope) error { return errNotImplemented("05") }
}

func notificationHandler() Handler {
	return func(ctx context.Context, ch *amqp.Channel, env Envelope) error { return errNotImplemented("05") }
}
