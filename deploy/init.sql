-- deploy/init.sql
CREATE TABLE products (
  id UUID PRIMARY KEY, name TEXT,
  price_cents BIGINT NOT NULL,
  available INT NOT NULL CHECK (available >= 0)  -- units free to sell; last line of defence
);
CREATE TABLE orders (  -- a cart is an order in status CART
  id UUID PRIMARY KEY,
  status TEXT NOT NULL,
  simulate TEXT CHECK (simulate IN ('payment_once', 'payment_always')),  -- demo only; null = normal order
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
CREATE TABLE order_events (  -- one row per delivery outcome, written by the worker (best-effort)
  id BIGSERIAL PRIMARY KEY,
  order_id UUID NOT NULL,    -- no FK: recording must never fail on the order row
  at TIMESTAMPTZ NOT NULL,   -- when the worker received the delivery
  queue TEXT NOT NULL,
  type TEXT NOT NULL,
  attempt INT NOT NULL,
  outcome TEXT NOT NULL,     -- ack | retry | dlq
  error TEXT
);
CREATE INDEX ON order_events (order_id);
INSERT INTO products (id, name, price_cents, available)
  VALUES (gen_random_uuid(), 'Keyboard', 24990, 10);
