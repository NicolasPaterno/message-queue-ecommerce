package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"message-queue-ecommerce/internal/api"
	"message-queue-ecommerce/internal/mq"
	"message-queue-ecommerce/internal/notification"
	"message-queue-ecommerce/internal/payment"
	"message-queue-ecommerce/internal/stock"
)

func main() {
	if len(os.Args) < 2 || (os.Args[1] != "api" && os.Args[1] != "worker") {
		fmt.Fprintln(os.Stderr, "usage: app api|worker")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	amqpURL := os.Getenv("AMQP_URL")
	db, err := openDB(ctx, os.Getenv("DB_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if os.Args[1] == "api" {
		err = api.Run(ctx, db, amqpURL)
	} else {
		failRate, _ := strconv.ParseFloat(os.Getenv("FAIL_RATE"), 64)
		log.Println("worker: started")
		err = mq.RunConsumers(ctx, amqpURL, map[string]mq.Handler{
			mq.QStock:        stock.Handler(db),
			mq.QPayment:      payment.Handler(db, failRate),
			mq.QNotification: notification.Handler(),
		})
		log.Println("worker: stopped")
	}
	if err != nil {
		log.Fatal(err)
	}
}

// openDB retries the ping because compose starts the app without waiting for Postgres to be ready.
func openDB(ctx context.Context, url string) (*sql.DB, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}
	d := time.Second
	for {
		err := db.PingContext(ctx)
		if err == nil {
			return db, nil
		}
		log.Printf("db: ping failed, retry in %s: %v", d, err)
		select {
		case <-ctx.Done():
			db.Close()
			return nil, ctx.Err()
		case <-time.After(d):
		}
		d = min(2*d, 30*time.Second)
	}
}
