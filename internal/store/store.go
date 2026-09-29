package store

import (
	"context"
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"
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

// Querier is satisfied by both *sql.DB and *sql.Tx.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type Item struct {
	ProductID  string `json:"product_id"`
	Quantity   int    `json:"quantity"`
	PriceCents int64  `json:"price_cents"`
}

func LoadItems(ctx context.Context, q Querier, orderID string) ([]Item, error) {
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
func TotalCents(items []Item) int64 {
	var t int64
	for _, it := range items {
		t += int64(it.Quantity) * it.PriceCents
	}
	return t
}

// WithTx commits if fn returns nil, otherwise rolls back and returns fn's error.
func WithTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
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

// MarkProcessed records a message id; false means it was already processed and the delivery is a duplicate.
func MarkProcessed(ctx context.Context, tx *sql.Tx, id string) (bool, error) {
	return affectedOne(tx.ExecContext(ctx, "INSERT INTO processed_messages (id) VALUES ($1) ON CONFLICT DO NOTHING", id))
}

// CASStatus moves the order from → to only if it is still in from; false means another transition won.
func CASStatus(ctx context.Context, tx *sql.Tx, orderID, from, to string) (bool, error) {
	return affectedOne(tx.ExecContext(ctx, "UPDATE orders SET status=$3, updated_at=now() WHERE id=$1 AND status=$2", orderID, from, to))
}

func affectedOne(res sql.Result, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}
