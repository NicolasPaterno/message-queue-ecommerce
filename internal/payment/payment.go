package payment

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"

	"message-queue-ecommerce/internal/mq"
	"message-queue-ecommerce/internal/store"

	amqp "github.com/rabbitmq/amqp091-go"
)

const declineRate = 0.15

// Handler charges a reserved order. The order is only moved to PAID here; a decline writes no status,
// because stock owns RESERVED→DECLINED when it releases the reservation on payment.declined.
// The result is published inside the tx, before commit, so a failed publish rolls back the dedupe row too.
func Handler(db *sql.DB, failRate float64) mq.Handler {
	return func(ctx context.Context, ch *amqp.Channel, env mq.Envelope) error {
		if env.Type != mq.KeyReservationCreated {
			return fmt.Errorf("%w: payment got %s", mq.ErrPermanent, env.Type)
		}
		if err := simulated(ctx, db, env.OrderID); err != nil {
			log.Printf("payment: gateway error (simulate) order_id=%s", env.OrderID)
			return err
		}
		if err := failMaybe(failRate); err != nil {
			log.Printf("payment: gateway error (FAIL_RATE) order_id=%s", env.OrderID)
			return err
		}
		declined := rand.Float64() < declineRate
		var dup, won bool
		var amount int64
		var txID string
		err := store.WithTx(ctx, db, func(tx *sql.Tx) error {
			first, err := store.MarkProcessed(ctx, tx, env.ID)
			if err != nil || !first {
				dup = !first
				return err
			}
			if declined {
				return publish(ctx, ch, mq.KeyPaymentDeclined, env.OrderID, nil)
			}
			items, err := store.LoadItems(ctx, tx, env.OrderID)
			if err != nil {
				return err
			}
			amount = store.TotalCents(items)
			if won, err = store.CASStatus(ctx, tx, env.OrderID, store.StatusReserved, store.StatusPaid); err != nil || !won {
				return err
			}
			txID = "TX-" + mq.NewID()[:8]
			data := struct {
				TransactionID string `json:"transaction_id"`
				AmountCents   int64  `json:"amount_cents"`
			}{txID, amount}
			return publish(ctx, ch, mq.KeyPaymentApproved, env.OrderID, data)
		})
		switch {
		case err != nil:
			return err
		case dup:
			log.Printf("payment: duplicate id=%s", env.ID)
		case declined:
			log.Printf("payment: declined order_id=%s", env.OrderID)
		case won:
			log.Printf("payment: approved order_id=%s transaction_id=%s amount_cents=%d", env.OrderID, txID, amount)
		default:
			log.Printf("payment: refund simulated order_id=%s amount_cents=%d", env.OrderID, amount)
		}
		return nil
	}
}

// simulated fails this order's payment on request from the web demo, independent of FAIL_RATE.
// payment_once clears itself in the same statement, so the retry passes; payment_always stays set,
// so every attempt fails and the third goes to the dlq (and the reservation later expires).
// The error text matches failMaybe's so the timeline reads the same either way; a DB error here is a normal retry.
func simulated(ctx context.Context, db *sql.DB, orderID string) error {
	err := db.QueryRowContext(ctx, "UPDATE orders SET simulate = NULLIF(simulate, 'payment_once') WHERE id = $1 AND simulate IS NOT NULL RETURNING true", orderID).Scan(new(bool))
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("simulated gateway error")
}

func failMaybe(rate float64) error {
	if rand.Float64() < rate {
		return errors.New("simulated gateway error")
	}
	return nil
}

func publish(ctx context.Context, ch *amqp.Channel, typ, orderID string, data any) error {
	e, err := mq.NewEnvelope(typ, orderID, data)
	if err != nil {
		return err
	}
	return mq.Publish(ctx, ch, mq.ExOrders, e)
}
