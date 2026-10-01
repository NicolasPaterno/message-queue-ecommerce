package mq

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connect dials with growing backoff until it succeeds or ctx is done.
func Connect(ctx context.Context, url string) (*amqp.Connection, error) {
	backoff := time.Second
	for {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn, nil
		}
		log.Printf("amqp: connect failed, retry in %s: %v", backoff, err)
		if !sleep(ctx, &backoff) {
			return nil, ctx.Err()
		}
	}
}

// sleep waits *d, then doubles it up to 30s; false means ctx ended first.
func sleep(ctx context.Context, d *time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(*d):
	}
	*d = min(*d*2, 30*time.Second)
	return true
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
