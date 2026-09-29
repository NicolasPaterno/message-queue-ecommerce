# message-queue-ecommerce — PRD

Build spec for the team. Prose in English. **All identifiers (tables, columns,
queues, routing keys, statuses, routes, Go names) are in English**; the graded
documents keep Portuguese prose and cite these same identifiers.

The scenario, architecture and design rationale are **not repeated here** — they
live in the delivered documents, which are the single source of truth:

| Document | Owns |
|---|---|
| [`etapa1.md`](etapa1.md) | Scenario, why messaging |
| [`etapa2.md`](etapa2.md) | Producers/consumers, message flow, **topology**, reservation cycle, scalability, reliability, fault tolerance, design decisions |
| [`diagramas/`](diagramas) | 6 diagrams (`.mmd` + `.png`) |

This PRD owns what those do not: naming, use cases, implementation rules, code
layout, user stories, and the plan.

| Delivery | Content | Due | Points | Status |
|---|---|---|---|---|
| Stages 1–2 | Scenario + Architecture | 2026-09-17 23:59 | 2.5 | **done**, revised after the professor's feedback ([`adjustents.md`](adjustents.md)) |
| Stages 3–5 | Config + Use cases + Technical | TBD (was 2026-09-24) | 3.5 | to do |
| Presentation | 10 min, every member presents | TBD (was 2026-09-24) | 4.0 | to do |

**The change, in one sentence:** stock is reserved **before** payment (a
temporary pre-reservation), never after — so no customer is charged for a unit
that does not exist.

---

## 1. Names

One table, so nothing drifts. Topology arguments are in [`etapa2.md` §2.2](etapa2.md).

| Kind | Identifiers |
|---|---|
| Exchanges | `orders` · `retry` · `dlx` · `expiry` |
| Queues | `stock` · `payment` · `notification` · `retry.q` · `expiry.q` · `dlq` |
| Routing keys | `order.placed` · `reservation.created` · `reservation.rejected` · `payment.approved` · `payment.declined` · `reservation.expired` |
| Order status | `CART` → `PLACED` → `RESERVED` → `PAID`, or `OUT_OF_STOCK` / `DECLINED` / `EXPIRED` |
| Tables | `orders` · `order_items` · `products` · `processed_messages` |
| HTTP | `POST /carts` · `POST /carts/:id/items` · `POST /carts/:id/checkout` · `GET /orders/:id` |
| Binary modes | `/app api` · `/app worker` |
| Env | `AMQP_URL` · `DB_URL` · `FAIL_RATE` (0–1, forces handler errors) |

**Envelope** — every message:

```json
{
  "id": "uuid",
  "type": "payment.approved",
  "order_id": "uuid",
  "data": { }
}
```

`id` is the idempotency key. Timestamp and content-type come from the AMQP
properties — do not duplicate them in the body.

---

## 2. Security (Stage 3)

- Users `api` and `worker` via `definitions.json`; `guest` removed.
- Least privilege: `api` writes only to the `orders` exchange; `worker` reads the
  queues and writes to the exchanges (`orders`, `dlx`, `expiry`).
- Dedicated vhost `/shop`.
- Encryption: TLS config (port 5671) documented in `etapa3.md`, **not enabled** —
  a self-signed certificate on localhost demonstrates certificate generation, not
  security. Declared as a limitation.

---

## 3. Use Cases (Stage 4)

Each with input → processing → output, reproducible by command. Setup for all:

```sh
C=$(curl -s -X POST localhost:8080/carts | jq -r .id)
curl -X POST localhost:8080/carts/$C/items -d '{"product_id":"<keyboard>","quantity":1}'
curl -X POST localhost:8080/carts/$C/checkout
curl localhost:8080/orders/$C
```

| # | Case | Command | Expected output | US |
|---|---|---|---|---|
| 1 | Happy path | Setup above | Status reaches `PAID`; `available` down by 1; events in order `order.placed` → `reservation.created` → `payment.approved` | US1, US3 |
| 2 | Payment declined | Repeat until it hits the ~15% | Status `DECLINED`; `available` restored | US3 |
| 3 | Insufficient stock | Add more than `products.available` | Status `OUT_OF_STOCK`; `payment` never receives a message | US3 |
| 4 | Last unit, two customers | Set `available = 1`, two carts, checkout both at once (`& wait`) | One `RESERVED`→`PAID`, the other `OUT_OF_STOCK`; `available` never negative | US9 |
| 5 | Reservation expiry | `FAIL_RATE=1`, checkout one cart | Payment retries 3×, goes to `dlq`; order stays `RESERVED`; after 120 s → `EXPIRED`, stock restored | US10 |
| 6 | Fan-out | One `reservation.created` | Appears in both `payment` and `notification` logs | US4 |
| 7 | Retry | `FAIL_RATE=1 docker compose up worker` | Same message reprocessed every 10 s | US5 |
| 8 | DLQ | Same as above, after 3 attempts | Message visible in `dlq` in the Management UI | US5 |
| 9 | Consumer down | `docker compose stop worker`, check out carts, restart | Queue piles up, then drains | US2 |
| 10 | Scaling | `docker compose up --scale worker=3` | 3 consumers per queue in the Management UI | US6 |
| 11 | Idempotency | Republish the same `order.placed` `id` | `available` drops only once | US7 |

Cases 5, 7 and 8 are the same run — show them together.

---

## 4. Implementation Rules

Non-negotiable — these are what the reliability and scalability claims in
`etapa2.md` rest on.

1. Manual ack **after** the database write, never before. No `autoAck` anywhere.
2. Publisher confirms — without a broker ACK, the publish counts as failed.
3. Durable queues; messages with `delivery_mode: 2`.
4. Idempotency before the side effect, same transaction:
   `INSERT INTO processed_messages (id) VALUES ($1) ON CONFLICT DO NOTHING`.
5. `prefetch = 10` — without it one worker swallows the queue and scaling is invisible.
6. Stock changes are **single atomic statements**, never read-then-write:
   - reserve: `UPDATE products SET available = available - $1 WHERE id = $2 AND available >= $1`
     — `RowsAffected == 0` means insufficient stock.
   - release: `UPDATE products SET available = available + $1 WHERE id = $2`

   There is no "confirm" step: a reserved unit already left `available`, so
   payment approval changes only the order status.

   All items of an order are reserved in **one transaction**; any item failing
   rolls back the whole reservation.
7. Status updates are **compare-and-set** from the one allowed previous status:
   `UPDATE orders SET status = $new WHERE id = $1 AND status = $previous`,
   written by the handler that causes the transition, in the same transaction as
   its side effect (see the transition table in `etapa2.md` §2.4).
   `RowsAffected == 0` means another event won → no-op, ack. `notification` never
   writes status. This is what stops "paid after the reservation expired": `PAID`
   and `EXPIRED` both need `status = 'RESERVED'`, so only one wins. If
   `payment` loses, it logs a simulated refund and publishes nothing.
8. **One AMQP channel per handler.** Channels are not thread-safe.
9. Deserialization errors go straight to the DLQ — retrying does not fix broken JSON.
10. Money in cents (`int64`), never `float`. The price is frozen into
    `order_items.price_cents` when the item is added to the cart.
11. Graceful shutdown: on `SIGTERM`, finish the in-flight message before exiting.
12. Reconnect to the broker with growing backoff; a dropped connection must not
    kill the process.
13. Publish `reservation.expired` to `expiry` right after the reservation commit,
    next to `reservation.created`. Crash between commit and publish = a reservation
    that never expires; accepted, same gap as the Outbox cut (§8).

---

## 5. Code

**One package per module + 4 infra files.** One binary; Compose varies the `command:`.
Dependencies point one way: `cmd/shop` → `api`, `stock`, `payment`, `notification` → `mq`, `store`.

```
message-queue-ecommerce/
├── cmd/shop/main.go         # switch: "api" | "worker"; env, DB connect, worker wiring
├── internal/
│   ├── mq/                  # RabbitMQ: names + Envelope (mq.go), publish.go, consume.go (retry/DLQ)
│   ├── store/               # order statuses, items, tx, idempotency, CAS
│   ├── stock/               # stock.Handler: reserve / release
│   ├── payment/             # payment.Handler
│   ├── notification/        # notification.Handler
│   └── api/                 # api.Run: cart routes + GET /orders/:id
├── Dockerfile
├── deploy/
│   ├── docker-compose.yml
│   ├── init.sql
│   └── definitions.json     # RabbitMQ users and permissions
└── docs/
```

```yaml
# deploy/docker-compose.yml
services:
  rabbitmq: { image: rabbitmq:3.13-management, ports: ["5672:5672","15672:15672"] }
  postgres: { image: postgres:16, volumes: ["./init.sql:/docker-entrypoint-initdb.d/init.sql"] }
  api:      { build: .., command: ["/app","api"], ports: ["8080:8080"] }
  worker:   { build: .., command: ["/app","worker"] }
```

```sql
-- deploy/init.sql
CREATE TABLE products (
  id UUID PRIMARY KEY, name TEXT,
  price_cents BIGINT NOT NULL,
  available INT NOT NULL CHECK (available >= 0)  -- units free to sell; last line of defence
);
CREATE TABLE orders (  -- a cart is an order in status CART
  id UUID PRIMARY KEY,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE order_items (
  order_id UUID REFERENCES orders(id),
  product_id UUID REFERENCES products(id),
  quantity INT NOT NULL CHECK (quantity > 0),
  price_cents BIGINT NOT NULL,  -- frozen when added
  PRIMARY KEY (order_id, product_id)
);
CREATE TABLE processed_messages (id UUID PRIMARY KEY);  -- idempotency
INSERT INTO products (id, name, price_cents, available)
  VALUES (gen_random_uuid(), 'Keyboard', 24990, 10);
```

No `total_cents` column: total = `SUM(quantity * price_cents)` over
`order_items`, computed when needed.

### 5.1 API surface

JSON in and out, `net/http` only. No auth (out of scope).

| Method | Path | Body | Success | Errors |
|---|---|---|---|---|
| POST | `/carts` | — | `201 {"id":"uuid","status":"CART"}` | — |
| POST | `/carts/:id/items` | `{"product_id":"uuid","quantity":1}` | `201` — upsert on `(order_id, product_id)` | `404` unknown cart/product · `409` not `CART` · `400` `quantity <= 0` |
| POST | `/carts/:id/checkout` | — | `201 {"id":"uuid","status":"PLACED"}` after publisher confirm | `404` · `409` not `CART` or empty cart |
| GET | `/orders/:id` | — | `200 {"id","status","items":[…],"total_cents"}` | `404` |

Checkout does **not** check stock — the atomic reservation in `stock` is the
only gate (§8 has the cut).

---

## 6. User Stories

| ID | As a | I want | So that |
|---|---|---|---|
| US1 | Customer | to fill a cart and get an immediate checkout confirmation | I don't wait on stock or the payment gateway |
| US2 | Dev | to publish with confirms to durable queues | no message is lost if the broker restarts |
| US3 | Dev | `stock` to reserve before `payment`, chained by routing keys | nobody is charged without stock, and topic routing is demonstrated |
| US4 | Dev | two handlers to receive the same event | fan-out is demonstrated |
| US5 | Dev | failures to enter retry and then land in the DLQ | fault tolerance is demonstrated |
| US6 | Dev | to scale consumers with `--scale` | scalability is demonstrated |
| US7 | Dev | redelivery not to reserve stock twice | idempotency is demonstrated |
| US8 | Dev | to see queues, rates and the DLQ in the Management UI | we get a dashboard without writing a frontend |
| US9 | Customer | the last unit to go to whoever reserved it first | I am never charged for a unit someone else got |
| US10 | Store | abandoned or failed payments to release their reservation | stock is not locked forever |

**Acceptance** — each one is a check performed live during the presentation:

- US1: `POST /carts/:id/checkout` returns 201 in < 300 ms with `worker` stopped.
- US2: `docker restart rabbitmq` with a full queue → messages are still there.
- US3: `payment` only receives a message after `reservation.created`; an `OUT_OF_STOCK` order never appears in the `payment` log.
- US4: one `reservation.created` shows up in both `payment` and `notification` logs.
- US5: `FAIL_RATE=1` → 3 attempts 10 s apart → message in `dlq`.
- US6: `--scale worker=3` → Management UI shows 3 consumers per queue.
- US7: republish the same `order.placed` `id` → `available` drops only once.
- US8: `localhost:15672` shows the 6 queues, and `dlq` is inspectable.
- US9: `available = 1`, two concurrent checkouts → exactly one `PAID`/`RESERVED`, one `OUT_OF_STOCK`, `available = 0`.
- US10: `FAIL_RATE=1` → order `RESERVED` → 120 s later `EXPIRED`, `available` restored.

---

## 7. Plan

### Stages 1–2 · 2026-09-17 · **done**, revised

`etapa1.md`, `etapa2.md` and the diagrams are written; revised for
reserve-before-payment (new `reserva` diagram, `.png`s regenerated). No code is
graded here.

### Stages 3–5 · TBD (was 2026-09-24)

| # | Task | Output | US |
|---|---|---|---|
| 3.1 | `internal/mq`: connect, declare 4 exchanges + 6 queues, publish with confirms | code | US2 |
| 3.2 | `internal/mq`: consume with prefetch, manual ack, `x-death` retry/DLQ routing | code | US5 |
| 3.3 | `definitions.json`: vhost, users, permissions | config | — |
| 3.4 | `etapa3.md`: every parameter with its rationale + TLS config | doc | — |
| 4.1 | `internal/stock`: reserve (one tx, all items) + CAS + publish `reservation.created` and `reservation.expired`; release | code | US3, US7, US9, US10 |
| 4.2 | `internal/payment` (CAS → `PAID`, refund log on lost CAS) + `internal/notification` (log only) | code | US3, US4 |
| 4.3 | `internal/api`: 4 HTTP routes + `cmd/shop/main.go`: mode switch | code | US1 |
| 4.4 | `deploy/`: Compose, `init.sql`, Dockerfile | config | US6 |
| 4.5 | Run the 11 use cases, capture evidence | `etapa4.md` | all |
| 5.1 | `etapa5.md`: stack, message format, practices (§4 here, in Portuguese) | doc | — |
| 5.2 | README: step-by-step run instructions | `README.md` | — |

**Order:** 3.1 → 3.2 → 4.1 → 4.2 → 4.3 → 4.4, then the docs. Everything depends on `internal/mq` and `internal/store`.

Estimate: **~500 lines of Go** (was ~400; +cart routes, +release, +CAS).

### Presentation · TBD · 10 min

| Time | Content |
|---|---|
| 0–2 | Scenario, why messaging, why reserve before paying (`etapa1.md`) |
| 2–4 | Architecture: `arquitetura.png`, `reserva.png`, topic exchange, the 6 queues. **Start case 5 (`FAIL_RATE=1` checkout) here** — its 120 s run under the talk |
| 4–6 | Demo: cart → checkout → `PAID`, Management UI moving (cases 1, 6); last unit, two customers (case 4) |
| 6–8 | Demo: case 5 lands — retries → DLQ → `EXPIRED`, stock back (cases 5, 7, 8); `stop worker` → pile up → drain (case 9) |
| 8–10 | Demo: `--scale worker=3` (case 10), close with the decisions table |

### Split by team member

| Track | Owns | Presents |
|---|---|---|
| A | `internal/mq` — topology (incl. `expiry`), publisher | Architecture and RabbitMQ configuration |
| B | `internal/stock` + `internal/api` cart routes | Happy path, last-unit concurrency |
| C | `internal/payment`/`notification`, retry/DLQ + `definitions.json` | Failure → DLQ → expiry demo, and security |
| D | `deploy/`, docs, diagrams | Scenario and scaling demo |

With fewer members, merge adjacent tracks. **Anyone who does not present gets a
zero; if any member leaves before all presentations finish, the team's grade is
divided by 7.** GitHub link goes to the AVA.

---

## 8. Cut, and When to Add It

| Cut | Add when |
|---|---|
| Separate service per domain | The team needs independent deploys |
| `ROLES` env var for selective scaling | One queue needs to scale apart from the others |
| Outbox pattern | Losing a message between commit and publish becomes a real problem (also closes the "reservation never expires" gap, rule 13) |
| 3-level retry (5s/30s/2m) | A single 10 s level no longer covers real recovery time |
| TLS enabled | It leaves the local machine |
| Quorum queues | There is more than one broker node |
| Migration tool | There is a second migration |
| Dedicated audit service | The audit trail needs its own retention or queries |
| Separate `carts` table | A cart needs data an order doesn't |
| Synchronous stock check at checkout (409 before publishing) | Users complain about finding out `OUT_OF_STOCK` asynchronously |
| Real refund flow for "paid after expiry" | There is a real gateway; today it is a log line |
| Configurable reservation TTL | 120 s stops fitting — today it is a queue argument, change it there |
