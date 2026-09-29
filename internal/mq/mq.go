package mq

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExOrders = "orders"
	ExRetry  = "retry"
	ExDLX    = "dlx"
	ExExpiry = "expiry"

	QStock        = "stock"
	QPayment      = "payment"
	QNotification = "notification"
	QRetry        = "retry.q"
	QExpiry       = "expiry.q"
	QDLQ          = "dlq"

	KeyOrderPlaced         = "order.placed"
	KeyReservationCreated  = "reservation.created"
	KeyReservationRejected = "reservation.rejected"
	KeyPaymentApproved     = "payment.approved"
	KeyPaymentDeclined     = "payment.declined"
	KeyReservationExpired  = "reservation.expired"

	prefetch    = 10     // unacked deliveries per consumer
	maxAttempts = 3      // deliveries per queue before dlq
	retryTTL    = 10000  // ms a rejected message waits in retry.q before redelivery
	expiryTTL   = 120000 // ms a reservation lives in expiry.q before it is released
)

// Envelope is the body of every message. Routing key == Type; ID is the idempotency key and AMQP MessageId.
type Envelope struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	OrderID string          `json:"order_id"`
	Data    json.RawMessage `json:"data"`
}

func NewID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// NewEnvelope marshals data into Data; nil data becomes {} so the wire shape is always an object.
func NewEnvelope(typ, orderID string, data any) (Envelope, error) {
	env := Envelope{ID: NewID(), Type: typ, OrderID: orderID, Data: json.RawMessage("{}")}
	if data != nil {
		var err error
		if env.Data, err = json.Marshal(data); err != nil {
			return Envelope{}, err
		}
	}
	return env, nil
}

// Handler processes one message. ch is the consumer's own confirm-mode channel, used for publishing.
// nil → ack; error wrapping ErrPermanent → straight to dlq; any other error → retry.
type Handler func(ctx context.Context, ch *amqp.Channel, env Envelope) error

var ErrPermanent = errors.New("permanent failure")
