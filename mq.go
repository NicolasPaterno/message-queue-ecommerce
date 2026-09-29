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
	for {
		conn, err := Connect(ctx, url)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := consumeDeclare(conn); err != nil {
			conn.Close()
			log.Printf("amqp: declare topology: %v", err)
			if ctx.Err() != nil {
				return nil
			}
			continue
		}

		cctx, cancel := context.WithCancel(ctx)
		closed := conn.NotifyClose(make(chan *amqp.Error, 1))
		var wg sync.WaitGroup
		for queue, h := range handlers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := Consume(cctx, conn, queue, h); err != nil {
					log.Printf("queue=%s consumer stopped: %v", queue, err)
					cancel()
				}
			}()
		}
		select {
		case <-ctx.Done():
		case err := <-closed:
			log.Printf("amqp: connection closed: %v", err)
		case <-cctx.Done():
		}
		cancel()
		wg.Wait()
		conn.Close()
		if ctx.Err() != nil {
			return nil
		}
	}
}

func consumeDeclare(conn *amqp.Connection) error {
	ch, err := OpenChannel(conn)
	if err != nil {
		return err
	}
	defer ch.Close()
	return DeclareTopology(ch)
}

// Consume uses manual ack. Bad JSON, ErrPermanent or the last allowed attempt → dlq + ack;
// other failures → nack without requeue, which dead-letters to retry.q.
// Handlers get a context that ignores cancellation so a shutdown never aborts an in-flight tx;
// ctx is only checked between deliveries, and closing the channel requeues the unacked prefetch.
func Consume(ctx context.Context, conn *amqp.Connection, queue string, h Handler) error {
	ch, err := OpenChannel(conn)
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return err
	}
	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	hctx := context.WithoutCancel(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("queue %s: deliveries closed", queue)
			}
			var env Envelope
			if err := json.Unmarshal(d.Body, &env); err != nil {
				log.Printf("queue=%s id=%s -> dlq: %v", queue, d.MessageId, err)
				consumeDLQ(hctx, ch, d)
				continue
			}
			n := deathCount(d, queue)
			log.Printf("queue=%s type=%s id=%s order_id=%s attempt=%d", queue, env.Type, env.ID, env.OrderID, n+1)
			err := h(hctx, ch, env)
			switch {
			case err == nil:
				d.Ack(false)
			case errors.Is(err, ErrPermanent) || n+1 >= maxAttempts:
				log.Printf("queue=%s id=%s -> dlq: %v", queue, env.ID, err)
				consumeDLQ(hctx, ch, d)
			default:
				log.Printf("queue=%s id=%s attempt=%d failed, retry: %v", queue, env.ID, n+1, err)
				d.Nack(false, false)
			}
		}
	}
}

// consumeDLQ acks only after the dlq publish is confirmed; if it fails the message goes to retry instead of being lost.
func consumeDLQ(ctx context.Context, ch *amqp.Channel, d amqp.Delivery) {
	if err := toDLQ(ctx, ch, d); err != nil {
		log.Printf("dlq publish failed, retry: %v", err)
		d.Nack(false, false)
		return
	}
	d.Ack(false)
}

// deathCount reads how many times d was rejected from queue, from the x-death header the broker maintains.
// Each retry cycle also adds a retry.q "expired" entry, so only the own-queue "rejected" entry counts.
func deathCount(d amqp.Delivery, queue string) int64 {
	xs, _ := d.Headers["x-death"].([]interface{})
	for _, x := range xs {
		e, ok := x.(amqp.Table)
		if !ok || e["queue"] != queue || e["reason"] != "rejected" {
			continue
		}
		switch c := e["count"].(type) {
		case int64:
			return c
		case int32:
			return int64(c)
		case int:
			return int64(c)
		}
	}
	return 0
}

// toDLQ republishes the raw body and headers to the dlx exchange with a confirm, keeping the original routing key.
func toDLQ(ctx context.Context, ch *amqp.Channel, d amqp.Delivery) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, ExDLX, d.RoutingKey, false, false, amqp.Publishing{
		Headers:      d.Headers,
		Body:         d.Body,
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Timestamp:    d.Timestamp,
	})
	if err != nil {
		return err
	}
	ok, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("dlq publish nacked")
	}
	return nil
}
