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
func Handler(db *sql.DB, failRate float64) mq.Handler {
	return func(ctx context.Context, ch *amqp.Channel, env mq.Envelope) error {
		if env.Type != mq.KeyReservationCreated {
			return fmt.Errorf("%w: payment got %s", mq.ErrPermanent, env.Type)
		}
		if err := failMaybe(failRate); err != nil {
			log.Printf("payment: gateway error (FAIL_RATE) order_id=%s", env.OrderID)
			return err
		}
		declined := rand.Float64() < declineRate
		var dup, won bool
		var amount int64
		err := store.WithTx(ctx, db, func(tx *sql.Tx) error {
			first, err := store.MarkProcessed(ctx, tx, env.ID)
			if err != nil || !first || declined {
				dup = !first
				return err
			}
			items, err := store.LoadItems(ctx, tx, env.OrderID)
			if err != nil {
				return err
			}
			amount = store.TotalCents(items)
			won, err = store.CASStatus(ctx, tx, env.OrderID, store.StatusReserved, store.StatusPaid)
			return err
		})
		switch {
		case err != nil:
			return err
		case dup:
			log.Printf("payment: duplicate id=%s", env.ID)
		case declined:
			if err := publish(ctx, ch, mq.KeyPaymentDeclined, env.OrderID, nil); err != nil {
				return err
			}
			log.Printf("payment: declined order_id=%s", env.OrderID)
		case won:
			txID := "TX-" + mq.NewID()[:8]
			data := struct {
				TransactionID string `json:"transaction_id"`
				AmountCents   int64  `json:"amount_cents"`
			}{txID, amount}
			if err := publish(ctx, ch, mq.KeyPaymentApproved, env.OrderID, data); err != nil {
				return err
			}
			log.Printf("payment: approved order_id=%s transaction_id=%s amount_cents=%d", env.OrderID, txID, amount)
		default:
			log.Printf("payment: refund simulated order_id=%s amount_cents=%d", env.OrderID, amount)
		}
		return nil
	}
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
