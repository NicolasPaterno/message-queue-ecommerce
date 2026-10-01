package api

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
	"uuid"

	"message-queue-ecommerce/internal/mq"
	"message-queue-ecommerce/internal/store"
)

// The API contract (OpenAPI 3.1), shipped inside the binary; /docs renders it from /openapi.yaml.
//
//go:embed openapi.yaml
var openapi []byte

// docsHTML renders /openapi.yaml with Swagger UI loaded from a CDN (the browser needs internet; the api doesn't).
// Same origin as the api, so "Try it out" calls the real routes.
const docsHTML = `<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>message-queue-ecommerce API</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.33.1/swagger-ui.css">
</head>
<body>
<div id="ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.33.1/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({ url: "/openapi.yaml", dom_id: "#ui" });</script>
</body>
</html>
`

var (
	errBadRequest  = errors.New("bad request")
	errNotFound    = errors.New("not found")
	errConflict    = errors.New("conflict")
	errUnavailable = errors.New("broker unavailable")
)

type server struct {
	db  *sql.DB
	pub *mq.Publisher
}

func Run(ctx context.Context, db *sql.DB, amqpURL string) error {
	pub, err := mq.NewPublisher(ctx, amqpURL)
	if err != nil {
		return err
	}
	defer pub.Close()
	s := &server{db: db, pub: pub}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /carts", handle(s.createCart))
	mux.HandleFunc("POST /carts/{id}/items", handle(s.addItem))
	mux.HandleFunc("POST /carts/{id}/checkout", handle(s.checkout))
	mux.HandleFunc("GET /orders/{id}", handle(s.getOrder))
	mux.HandleFunc("GET /products", handle(s.listProducts))
	mux.HandleFunc("POST /products/{id}/stock", handle(s.restock))
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.Write(openapi)
	})
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(docsHTML))
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Println("api: listening on :8080")
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}

// handle maps the sentinel errors to status codes in one place; anything else is a logged 500.
func handle(h func(r *http.Request) (int, any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		code, body, err := h(r)
		if err != nil {
			switch {
			case errors.Is(err, errBadRequest):
				code = http.StatusBadRequest
			case errors.Is(err, errNotFound):
				code = http.StatusNotFound
			case errors.Is(err, errConflict):
				code = http.StatusConflict
			case errors.Is(err, errUnavailable):
				code = http.StatusServiceUnavailable
			default:
				log.Printf("api: %s %s: %v", r.Method, r.URL.Path, err)
				code, err = http.StatusInternalServerError, errors.New("internal error")
			}
			body = map[string]string{"error": err.Error()}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(body)
	}
}

// parseID turns a malformed id into a 404 before it reaches Postgres, where the uuid column would reject it as a 500.
func parseID(s string) (string, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return "", fmt.Errorf("%w: %q", errNotFound, s)
	}
	return u.String(), nil
}

func (s *server) createCart(r *http.Request) (int, any, error) {
	var id string
	err := s.db.QueryRowContext(r.Context(), "INSERT INTO orders (id, status) VALUES (gen_random_uuid(), $1) RETURNING id", store.StatusCart).Scan(&id)
	if err != nil {
		return 0, nil, err
	}
	return http.StatusCreated, map[string]string{"id": id, "status": store.StatusCart}, nil
}

func (s *server) addItem(r *http.Request) (int, any, error) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		return 0, nil, err
	}
	var in struct {
		ProductID string `json:"product_id"`
		Quantity  int    `json:"quantity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Quantity <= 0 {
		return 0, nil, fmt.Errorf("%w: need product_id and quantity > 0", errBadRequest)
	}
	pid, err := parseID(in.ProductID)
	if err != nil {
		return 0, nil, err
	}
	it := store.Item{ProductID: pid, Quantity: in.Quantity}
	err = store.WithTx(r.Context(), s.db, func(tx *sql.Tx) error {
		if err := lockCart(r.Context(), tx, id); err != nil {
			return err
		}
		err := tx.QueryRowContext(r.Context(), "SELECT price_cents FROM products WHERE id = $1", pid).Scan(&it.PriceCents)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: product %s", errNotFound, pid)
		}
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(r.Context(), `INSERT INTO order_items (order_id, product_id, quantity, price_cents) VALUES ($1, $2, $3, $4)
			ON CONFLICT (order_id, product_id) DO UPDATE SET quantity = EXCLUDED.quantity, price_cents = EXCLUDED.price_cents`,
			id, pid, it.Quantity, it.PriceCents)
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusCreated, it, nil
}

// checkout publishes order.placed inside the tx that moves CART→PLACED: 201 only after the broker confirms,
// and an unconfirmed publish rolls the order back to CART (503) instead of leaving a PLACED order with no event.
// The publish is bounded so a broker outage fails fast instead of holding the cart's row lock.
// An optional {"simulate": ...} body marks the order for a demo payment failure (see payment.simulated).
func (s *server) checkout(r *http.Request) (int, any, error) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		return 0, nil, err
	}
	var in struct {
		Simulate string `json:"simulate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); (err != nil && !errors.Is(err, io.EOF)) ||
		(in.Simulate != "" && in.Simulate != "payment_once" && in.Simulate != "payment_always") {
		return 0, nil, fmt.Errorf("%w: simulate must be payment_once or payment_always", errBadRequest)
	}
	ctx := r.Context()
	err = store.WithTx(ctx, s.db, func(tx *sql.Tx) error {
		if err := lockCart(ctx, tx, id); err != nil {
			return err
		}
		items, err := store.LoadItems(ctx, tx, id)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return fmt.Errorf("%w: cart is empty", errConflict)
		}
		if _, err := store.CASStatus(ctx, tx, id, store.StatusCart, store.StatusPlaced); err != nil {
			return err
		}
		if in.Simulate != "" {
			if _, err := tx.ExecContext(ctx, "UPDATE orders SET simulate = $2 WHERE id = $1", id, in.Simulate); err != nil {
				return err
			}
		}
		env, err := mq.NewEnvelope(mq.KeyOrderPlaced, id, nil)
		if err != nil {
			return err
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := s.pub.Publish(pctx, mq.ExOrders, env); err != nil {
			return fmt.Errorf("%w: %v", errUnavailable, err)
		}
		log.Printf("api: order.placed id=%s order_id=%s", env.ID, id)
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusCreated, map[string]string{"id": id, "status": store.StatusPlaced}, nil
}

func (s *server) getOrder(r *http.Request) (int, any, error) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		return 0, nil, err
	}
	var status string
	err = s.db.QueryRowContext(r.Context(), "SELECT status FROM orders WHERE id = $1", id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, fmt.Errorf("%w: order %s", errNotFound, id)
	}
	if err != nil {
		return 0, nil, err
	}
	items, err := store.LoadItems(r.Context(), s.db, id)
	if err != nil {
		return 0, nil, err
	}
	if items == nil {
		items = []store.Item{}
	}
	events, err := s.orderEvents(r.Context(), id)
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, map[string]any{"id": id, "status": status, "items": items, "total_cents": store.TotalCents(items), "events": events}, nil
}

type event struct {
	At      time.Time `json:"at"`
	Queue   string    `json:"queue"`
	Type    string    `json:"type"`
	Attempt int       `json:"attempt"`
	Outcome string    `json:"outcome"`
	Error   string    `json:"error,omitempty"`
}

func (s *server) orderEvents(ctx context.Context, id string) ([]event, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT at, queue, type, attempt, outcome, coalesce(error, '') FROM order_events WHERE order_id = $1 ORDER BY at, id", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []event{}
	for rows.Next() {
		var e event
		if err := rows.Scan(&e.At, &e.Queue, &e.Type, &e.Attempt, &e.Outcome, &e.Error); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

type product struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	PriceCents int64  `json:"price_cents"`
	Available  int    `json:"available"`
}

func (s *server) listProducts(r *http.Request) (int, any, error) {
	rows, err := s.db.QueryContext(r.Context(), "SELECT id, name, price_cents, available FROM products ORDER BY name")
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	out := []product{}
	for rows.Next() {
		var p product
		if err := rows.Scan(&p.ID, &p.Name, &p.PriceCents, &p.Available); err != nil {
			return 0, nil, err
		}
		out = append(out, p)
	}
	return http.StatusOK, out, rows.Err()
}

// restock only adds: a concurrent reservation's decrement is never overwritten, which is why there is no "set available" route.
func (s *server) restock(r *http.Request) (int, any, error) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		return 0, nil, err
	}
	var in struct {
		Add int `json:"add"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Add < 1 || in.Add > 1000 {
		return 0, nil, fmt.Errorf("%w: need add between 1 and 1000", errBadRequest)
	}
	var p product
	err = s.db.QueryRowContext(r.Context(), "UPDATE products SET available = available + $1 WHERE id = $2 RETURNING id, name, price_cents, available", in.Add, id).
		Scan(&p.ID, &p.Name, &p.PriceCents, &p.Available)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, fmt.Errorf("%w: product %s", errNotFound, id)
	}
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, p, nil
}

// lockCart row-locks the order until the tx ends, so concurrent add-item/checkout on one cart serialize.
func lockCart(ctx context.Context, tx *sql.Tx, id string) error {
	var status string
	err := tx.QueryRowContext(ctx, "SELECT status FROM orders WHERE id = $1 FOR UPDATE", id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: cart %s", errNotFound, id)
	}
	if err != nil {
		return err
	}
	if status != store.StatusCart {
		return fmt.Errorf("%w: order is %s, not %s", errConflict, status, store.StatusCart)
	}
	return nil
}
