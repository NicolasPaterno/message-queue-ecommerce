package notification

import (
	"context"
	"log"

	"message-queue-ecommerce/internal/mq"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Handler() mq.Handler {
	return func(ctx context.Context, ch *amqp.Channel, env mq.Envelope) error {
		log.Printf("notification: type=%s id=%s order_id=%s", env.Type, env.ID, env.OrderID)
		return nil
	}
}
