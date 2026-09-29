package main

import (
	"context"
	"database/sql"

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
	return func(ctx context.Context, ch *amqp.Channel, env Envelope) error { return errNotImplemented("04") }
}
func paymentHandler(db *sql.DB, failRate float64) Handler {
	return func(ctx context.Context, ch *amqp.Channel, env Envelope) error { return errNotImplemented("05") }
}

func notificationHandler() Handler {
	return func(ctx context.Context, ch *amqp.Channel, env Envelope) error { return errNotImplemented("05") }
}
