package stock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"

	"message-queue-ecommerce/internal/mq"
	"message-queue-ecommerce/internal/store"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Handler(db *sql.DB) mq.Handler {
	return func(ctx context.Context, ch *amqp.Channel, env mq.Envelope) error {
		switch env.Type {
		case mq.KeyOrderPlaced:
			return reserve(ctx, db, ch, env)
		case mq.KeyPaymentDeclined:
			return release(ctx, db, env, store.StatusDeclined)
		case mq.KeyReservationExpired:
			return release(ctx, db, env, store.StatusExpired)
		}
		return fmt.Errorf("%w: stock got %s", mq.ErrPermanent, env.Type)
	}
}

var errStockShort = errors.New("stock short")

const (
	reserveSQL = "UPDATE products SET available = available - $1 WHERE id = $2 AND available >= $1"
	releaseSQL = "UPDATE products SET available = available + $1 WHERE id = $2"
)

// reserve runs the order status CAS before touching stock: the api publishes order.placed inside its
// still-open tx, so the CAS waits on that row lock and sees the committed status (or 0 rows on rollback).
// Out of stock rolls back every item and moves the order to OUT_OF_STOCK in a fresh tx.
func reserve(ctx context.Context, db *sql.DB, ch *amqp.Channel, env mq.Envelope) error {
	won, err := transition(ctx, db, env, store.StatusPlaced, store.StatusReserved, reserveSQL)
	if errors.Is(err, errStockShort) {
		if won, err = transition(ctx, db, env, store.StatusPlaced, store.StatusOutOfStock, ""); err != nil || !won {
			return err
		}
		log.Printf("stock: out_of_stock order_id=%s", env.OrderID)
		return publish(ctx, ch, mq.ExOrders, mq.KeyReservationRejected, env.OrderID)
	}
	if err != nil || !won {
		return err
	}
	log.Printf("stock: reserved order_id=%s", env.OrderID)
	if err := publish(ctx, ch, mq.ExOrders, mq.KeyReservationCreated, env.OrderID); err != nil {
		return err
	}
	return publish(ctx, ch, mq.ExExpiry, mq.KeyReservationExpired, env.OrderID)
}

func release(ctx context.Context, db *sql.DB, env mq.Envelope, to string) error {
	won, err := transition(ctx, db, env, store.StatusReserved, to, releaseSQL)
	if won {
		log.Printf("stock: released order_id=%s status=%s", env.OrderID, to)
	}
	return err
}

// transition, in one tx: dedupes env.ID, moves the order from → to and, only if that CAS won,
// runs itemSQL for every item. Items go in product id order so concurrent txs lock rows in the same order.
// Returns true only when this call made the transition.
func transition(ctx context.Context, db *sql.DB, env mq.Envelope, from, to, itemSQL string) (bool, error) {
	var first, won bool
	err := store.WithTx(ctx, db, func(tx *sql.Tx) error {
		var err error
		if first, err = store.MarkProcessed(ctx, tx, env.ID); err != nil || !first {
			return err
		}
		if won, err = store.CASStatus(ctx, tx, env.OrderID, from, to); err != nil || !won || itemSQL == "" {
			return err
		}
		items, err := store.LoadItems(ctx, tx, env.OrderID)
		if err != nil {
			return err
		}
		slices.SortFunc(items, func(a, b store.Item) int { return strings.Compare(a.ProductID, b.ProductID) })
		for _, it := range items {
			res, err := tx.ExecContext(ctx, itemSQL, it.Quantity, it.ProductID)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n == 0 {
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

func publish(ctx context.Context, ch *amqp.Channel, exchange, typ, orderID string) error {
	e, err := mq.NewEnvelope(typ, orderID, nil)
	if err != nil {
		return err
	}
	return mq.Publish(ctx, ch, exchange, e)
}
