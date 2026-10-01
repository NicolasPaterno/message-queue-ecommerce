import { test } from "node:test";
import assert from "node:assert/strict";
import { diff, paint, parse, type Hop, type Snapshot, type Src, type Totals } from "./flow.ts";

type Stats = { publish?: number; deliver_get?: number; ack?: number };
type Q = { name: string; messages?: number; messages_ready?: number; messages_unacknowledged?: number; message_stats?: Stats };
const snap = (queues: Q[], publishIn = 0, checkouts = 0) => parse(queues, { message_stats: { publish_in: publishIn } }, checkouts, 0);
const edges = (hops: Hop[]) => hops.reduce<Record<string, number>>((m, h) => ({ ...m, [h.edge]: (m[h.edge] ?? 0) + h.n }), {});
const colored = (hops: Hop[]) => hops.reduce<Record<string, number>>((m, h) => ({ ...m, [`${h.edge}|${h.src}`]: (m[`${h.edge}|${h.src}`] ?? 0) + h.n }), {});

// Feeds snapshots in order like useLive does; returns the hops of each step after the first.
function run(...snaps: Snapshot[]) {
  let high: Totals = {};
  let prev: Snapshot | undefined;
  return snaps.map((s) => {
    const r = diff(high, prev, s);
    high = r.high;
    prev = s;
    return edges(r.hops);
  }).slice(1);
}

test("happy path: order.placed routed, delivered, worker publishes, reservation waits in expiry.q", () => {
  const a = snap([]);
  const b = snap([
    { name: "stock", message_stats: { publish: 1, deliver_get: 1, ack: 1 } },
    { name: "expiry.q", messages: 1, message_stats: { publish: 1 } },
  ], 2, 1);
  assert.deepEqual(run(a, b), [{ "orders-stock": 1, "stock-worker": 1, "worker-orders": 1, "worker-expiry": 1 }]);
});

test("backlog: stopped worker shows arrivals, restart shows deliveries only", () => {
  const a = snap([{ name: "stock" }]);
  const b = snap([{ name: "stock", messages: 3, messages_ready: 3, message_stats: { publish: 3 } }]);
  const c = snap([{ name: "stock", message_stats: { publish: 3, deliver_get: 3, ack: 3 } }]);
  assert.deepEqual(run(a, b, c), [{ "orders-stock": 3 }, { "stock-worker": 3 }]);
});

test("retry loop inside one poll: retry.q depth stays 1, loop still drawn (re-entry one poll late); 3rd failure goes to dlq", () => {
  const pay = (d: number, a: number): Q => ({ name: "payment", message_stats: { publish: 1, deliver_get: d, ack: a } });
  const first = snap([pay(1, 0), { name: "retry.q", messages: 1 }]);
  const second = snap([pay(2, 0), { name: "retry.q", messages: 1 }]); // expired, redelivered, failed again
  const third = snap([pay(3, 1), { name: "retry.q", messages: 0 }, { name: "dlq", messages: 1, message_stats: { publish: 1 } }]);
  assert.deepEqual(run(snap([]), first, first, second, second, third, third), [
    { "payment-worker": 1, "orders-payment": 1, "bus-retry": 1 },
    {},
    { "payment-worker": 1, "bus-retry": 1 },
    { "orders-payment": 1, "retryq-orders": 1 },
    { "payment-worker": 1, "bus-dlq": 1, "retryq-orders": 1 }, // retry.q empty: the depth fallback sees the exit first
    { "orders-payment": 1 },
  ]);
});

test("depth lagging its counter: no phantom release when a message enters a TTL queue", () => {
  const lag = snap([{ name: "expiry.q", messages: 0, message_stats: { publish: 1 } }]);
  const caught = snap([{ name: "expiry.q", messages: 1, message_stats: { publish: 1 } }]);
  assert.deepEqual(run(snap([]), lag, caught), [{ "worker-expiry": 1 }, {}]);
});

test("stats sampled apart (ack before unacked drops) never produces a phantom retry", () => {
  const a = snap([{ name: "payment", message_stats: { deliver_get: 1, ack: 1 } }]);
  const skew = snap([{ name: "payment", messages_unacknowledged: 1, message_stats: { deliver_get: 2, ack: 2 } }]);
  const settled = snap([{ name: "payment", message_stats: { deliver_get: 2, ack: 2 } }]);
  const [, back] = run(a, skew, settled);
  assert.equal(back["bus-retry"], undefined);
});

test("expiry release drawn from stock's re-entry, even while expiry.q depth still lags", () => {
  const stock = (p: number, d: number): Q => ({ name: "stock", message_stats: { publish: p, deliver_get: d, ack: d } });
  const a = snap([stock(1, 1), { name: "expiry.q", messages: 1, message_stats: { publish: 1 } }]);
  const b = snap([stock(1, 2), { name: "expiry.q", messages: 1, message_stats: { publish: 1 } }]);
  assert.deepEqual(run(a, b, b), [{ "stock-worker": 1 }, { "expiryq-orders": 1, "orders-stock": 1 }]);
});

test("delivery counted before its publish (different channels): no phantom re-entry or expiry", () => {
  const stock = (p: number, d: number): Q => ({ name: "stock", message_stats: { publish: p, deliver_get: d, ack: d } });
  const a = snap([stock(0, 0)]);
  const early = snap([stock(0, 1)]); // worker channel reported, api channel not yet
  const caught = snap([stock(1, 1)]);
  assert.deepEqual(run(a, early, caught), [{ "stock-worker": 1 }, { "orders-stock": 1 }]);
});

test("backlog after earlier re-entries: new publishes still counted while the worker is stopped", () => {
  const a = snap([{ name: "notification", message_stats: { publish: 14, deliver_get: 17, ack: 17 } }]);
  const b = snap([{ name: "notification", messages: 3, messages_ready: 3, message_stats: { publish: 17, deliver_get: 17, ack: 17 } }]);
  const c = snap([{ name: "notification", message_stats: { publish: 17, deliver_get: 20, ack: 20 } }]);
  assert.deepEqual(run(a, b, c), [{ "orders-notification": 3 }, { "notification-worker": 3 }]);
});

test("backlog draining: deliveries counted before ready drops never double the arrivals", () => {
  const a = snap([{ name: "stock" }]);
  const b = snap([{ name: "stock", messages: 3, messages_ready: 3, message_stats: { publish: 3 } }]);
  const skew = snap([{ name: "stock", messages: 3, messages_ready: 3, message_stats: { publish: 3, deliver_get: 3, ack: 3 } }]);
  assert.deepEqual(run(a, b, skew), [{ "orders-stock": 3 }, { "stock-worker": 3 }]);
});

test("expiry.q releases after TTL (depth fallback)", () => {
  const a = snap([{ name: "expiry.q", messages: 2, message_stats: { publish: 2 } }]);
  const b = snap([{ name: "expiry.q", messages: 0, message_stats: { publish: 2 } }]);
  assert.deepEqual(run(a, b), [{ "expiryq-orders": 2 }]);
});

test("the page's own checkouts are not worker publishes", () => {
  assert.deepEqual(run(snap([], 10, 0), snap([], 13, 1)), [{ "worker-orders": 2 }]);
});

test("broker restart resets counters: nothing drawn, then counts resume", () => {
  const before = snap([{ name: "payment", message_stats: { publish: 9, deliver_get: 9, ack: 9 } }], 30);
  const after = snap([], 0);
  const next = snap([{ name: "payment", message_stats: { publish: 1, deliver_get: 1, ack: 1 } }], 1);
  assert.deepEqual(run(before, after, next), [{}, { "orders-payment": 1, "payment-worker": 1, "worker-orders": 1 }]);
});

test("unknown queues ignored, missing ones zero", () => {
  const s = snap([{ name: "other", messages: 5 }]);
  assert.equal(s.queues.stock.messages, 0);
  assert.equal(Object.keys(s.queues).length, 6);
});

test("each queue's failures keep its color on the way to retry", () => {
  const q = (name: string, d: number): Q => ({ name, message_stats: { publish: 1, deliver_get: d, ack: 0 } });
  const a = snap([q("payment", 0), q("notification", 0)]);
  const b = snap([q("payment", 1), q("notification", 1)]);
  const r = diff({}, undefined, a);
  const hops = diff(r.high, a, b).hops.filter((h) => h.edge === "bus-retry");
  assert.deepEqual(colored(hops), { "bus-retry|payment": 1, "bus-retry|notification": 1 });
});

test("worker output split: reservation.created is stock's, the rest payment's", () => {
  const a = snap([], 0, 0);
  const b = snap([{ name: "payment", message_stats: { publish: 2 } }], 5, 1); // 4 worker publishes, 2 of them stock's
  const hops = diff(diff({}, undefined, a).high, a, b).hops.filter((h) => h.edge === "worker-orders");
  assert.deepEqual(colored(hops), { "worker-orders|stock": 2, "worker-orders|payment": 2 });
});

test("paint: retry.q exits take the colors of earlier nacks, oldest first", () => {
  const nack = (src: Src, n = 1): Hop => ({ edge: "bus-retry", src, n });
  const exit = (n: number): Hop => ({ edge: "retryq-orders", src: "none", n });
  let fifo: Src[] = [];
  ({ fifo } = paint([nack("payment"), nack("notification")], fifo));
  const r = paint([exit(1), nack("stock")], fifo); // the exit left before this poll's nack entered
  assert.deepEqual(r.hops.filter((h) => h.edge === "retryq-orders").map((h) => h.src), ["payment"]);
  assert.deepEqual(r.fifo, ["notification", "stock"]);
  assert.deepEqual(paint([exit(3)], r.fifo).hops.map((h) => h.src), ["notification", "stock", "none"]);
});
