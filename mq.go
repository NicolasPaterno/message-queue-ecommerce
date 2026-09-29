package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

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

func newID() string {
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
	env := Envelope{ID: newID(), Type: typ, OrderID: orderID, Data: json.RawMessage("{}")}
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

func errNotImplemented(plan string) error { return fmt.Errorf("not implemented: plan %s", plan) }

// Connect dials with growing backoff until it succeeds or ctx is done.
func Connect(ctx context.Context, url string) (*amqp.Connection, error) {
	backoff := time.Second
	for {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn, nil
		}
		log.Printf("amqp: connect failed, retry in %s: %v", backoff, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

// OpenChannel opens a channel in publisher-confirm mode, so every Publish on it waits for the broker ack.
func OpenChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, err
	}
	return ch, nil
}

var pubQueues = []struct {
	name string
	args amqp.Table
}{
	{QStock, amqp.Table{"x-dead-letter-exchange": ExRetry}},
	{QPayment, amqp.Table{"x-dead-letter-exchange": ExRetry}},
	{QNotification, amqp.Table{"x-dead-letter-exchange": ExRetry}},
	{QRetry, amqp.Table{"x-message-ttl": int32(retryTTL), "x-dead-letter-exchange": ExOrders}},
	{QExpiry, amqp.Table{"x-message-ttl": int32(expiryTTL), "x-dead-letter-exchange": ExOrders}},
	{QDLQ, nil},
}

var pubBindings = []struct{ queue, key, exchange string }{
	{QStock, KeyOrderPlaced, ExOrders},
	{QStock, KeyPaymentDeclined, ExOrders},
	{QStock, KeyReservationExpired, ExOrders},
	{QPayment, KeyReservationCreated, ExOrders},
	{QNotification, "#", ExOrders},
	{QRetry, "#", ExRetry},
	{QExpiry, "#", ExExpiry},
	{QDLQ, "#", ExDLX},
}

// DeclareTopology idempotently declares the 4 durable topic exchanges, 6 durable queues, their bindings
// and the dead-letter/TTL args that implement retry, expiry and dlq.
// Args must never change between runs: redeclaring a queue with different args fails with PRECONDITION_FAILED.
func DeclareTopology(ch *amqp.Channel) error {
	for _, ex := range []string{ExOrders, ExRetry, ExDLX, ExExpiry} {
		if err := ch.ExchangeDeclare(ex, "topic", true, false, false, false, nil); err != nil {
			return err
		}
	}
	for _, q := range pubQueues {
		if _, err := ch.QueueDeclare(q.name, true, false, false, false, q.args); err != nil {
			return err
		}
	}
	for _, b := range pubBindings {
		if err := ch.QueueBind(b.queue, b.key, b.exchange, false, nil); err != nil {
			return err
		}
	}
	return nil
}

// Publish sends env persistent with key env.Type and blocks until the broker confirms; nack or timeout → error.
func Publish(ctx context.Context, ch *amqp.Channel, exchange string, env Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchange, env.Type, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		MessageId:    env.ID,
		Body:         body,
	})
	if err != nil {
		return err
	}
	ok, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("nacked")
	}
	return nil
}

// Publisher is the api-side publisher: one connection and one confirm channel shared behind a mutex,
// reopened on demand when the broker drops them.
type Publisher struct {
	url  string
	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewPublisher(ctx context.Context, url string) (*Publisher, error) {
	conn, err := Connect(ctx, url)
	if err != nil {
		return nil, err
	}
	ch, err := OpenChannel(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if err := DeclareTopology(ch); err != nil {
		conn.Close()
		return nil, err
	}
	return &Publisher{url: url, conn: conn, ch: ch}, nil
}

// Publish reconnects lazily: a dropped conn or channel is reopened here, and a failed publish
// discards the channel so the next call starts on a fresh one.
func (p *Publisher) Publish(ctx context.Context, exchange string, env Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var err error
	if p.conn == nil || p.conn.IsClosed() {
		if p.conn, err = Connect(ctx, p.url); err != nil {
			return err
		}
		p.ch = nil
	}
	if p.ch == nil || p.ch.IsClosed() {
		if p.ch, err = OpenChannel(p.conn); err != nil {
			return err
		}
	}
	if err = Publish(ctx, p.ch, exchange, env); err != nil {
		p.ch.Close()
		p.ch = nil
	}
	return err
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch != nil {
		p.ch.Close()
	}
	if p.conn == nil {
		return nil
	}
	if err := p.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return err
	}
	return nil
}

// RunConsumers runs one Consume goroutine per queue on a shared connection, reconnects with backoff on loss,
// and on ctx done waits for in-flight messages before returning nil.
func RunConsumers(ctx context.Context, url string, handlers map[string]Handler) error {
	return errNotImplemented("03")
}

// Consume uses manual ack. Bad JSON, ErrPermanent or the last allowed attempt → dlq + ack;
// other failures → nack without requeue, which dead-letters to retry.q.
func Consume(ctx context.Context, conn *amqp.Connection, queue string, h Handler) error {
	return errNotImplemented("03")
}

// deathCount reads how many times d was rejected from queue, from the x-death header the broker maintains.
func deathCount(d amqp.Delivery, queue string) int64 {
	return 0
}

// toDLQ republishes the raw body and headers to the dlx exchange with a confirm, keeping the original routing key.
func toDLQ(ctx context.Context, ch *amqp.Channel, d amqp.Delivery) error {
	return errNotImplemented("03")
}
