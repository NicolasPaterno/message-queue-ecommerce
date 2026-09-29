package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RunConsumers runs one consume goroutine per queue on a shared connection, reconnects with backoff on loss,
// and on ctx done waits for in-flight messages before returning nil.
// A failed pass waits before retrying: Connect only backs off when the dial fails, so a reachable broker that
// refuses the declare or a consumer (e.g. ACCESS_REFUSED) would otherwise spin in a tight loop.
func RunConsumers(ctx context.Context, url string, handlers map[string]Handler) error {
	backoff := time.Second
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
			if !sleep(ctx, &backoff) {
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
				if err := consume(cctx, conn, queue, h); err != nil {
					log.Printf("queue=%s consumer stopped: %v", queue, err)
					cancel()
				}
			}()
		}
		select {
		case <-ctx.Done():
		case err := <-closed:
			log.Printf("amqp: connection closed: %v", err)
			backoff = time.Second
		case <-cctx.Done():
		}
		cancel()
		wg.Wait()
		conn.Close()
		if !sleep(ctx, &backoff) {
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

// consume uses manual ack. Bad JSON, ErrPermanent or the last allowed attempt → dlq + ack;
// other failures → nack without requeue, which dead-letters to retry.q.
// Handlers get a context that ignores cancellation so a shutdown never aborts an in-flight tx;
// ctx is only checked between deliveries, and closing the channel requeues the unacked prefetch.
func consume(ctx context.Context, conn *amqp.Connection, queue string, h Handler) error {
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
