// Turns Management API readings into "hops": how many messages crossed each map edge since the last reading.
// Pure and import-free so `node --test lib/flow.test.ts` can load it.
//
// Every edge is a running total that only ever grows, and diff emits only what exceeds the highest total
// seen, so a total that dips for a poll never becomes a phantom token.
//
// Totals come from the per-queue counters (publish, deliver_get, ack), which move the instant the message does.
// Depths (messages, ready, unacked) are sampled apart from them and lag, so mixing the two draws ghosts;
// depth is used only where no counter exists. Two facts from the live stack (plan 14 Findings):
//   - publish skips dead-lettered arrivals: a retried payment keeps publish=1 while deliver_get climbs;
//   - retry.q depth stays 1 through a whole retry loop (it expires and refills within one poll).
// So "re-entered" = deliver_get − publish is what came back from retry.q / expiry.q. Publishes and deliveries
// are counted on different channels (api/worker publisher vs consumer), each reporting on its own ~1 s cycle,
// so a delivery can show before its publish; re-entries therefore use the previous poll's deliver_get
// (everything delivered by then has its publish counted now). Every estimate is a lower bound, never extra;
// re-entries and TTL-queue exits show one poll late.

export const QUEUES = ["stock", "payment", "notification", "retry.q", "expiry.q", "dlq"] as const;
export type QueueName = (typeof QUEUES)[number];

export interface QueueStat {
  messages: number;
  ready: number;
  unacked: number;
  consumers: number;
  published: number; // cumulative message_stats.publish (real publishes only)
  delivered: number; // cumulative message_stats.deliver_get (includes dead-lettered redeliveries)
  acked: number; // cumulative message_stats.ack
}

// Where a message came from, for its color: the business queue that routed or handled it; "api" before routing,
// "none" when the counters can't tell (dlq peek failed, or a message that never had a queue).
export const SRCS = ["api", "stock", "payment", "notification", "none"] as const;
export type Src = (typeof SRCS)[number];

export type EdgeId =
  | "client-api" | "api-orders" | "worker-orders"
  | "orders-stock" | "orders-payment" | "orders-notification"
  | "stock-worker" | "payment-worker" | "notification-worker"
  | "worker-expiry" | "expiryq-orders"
  | "bus-retry" | "retryq-orders"
  | "bus-dlq";

// Keyed "edge|src"; "edge|src+part" keys are extra totals whose increases add to the same edge and src.
export type Totals = Record<string, number>;

export interface Snapshot {
  at: number;
  queues: Record<QueueName, QueueStat>;
  totals: Totals;
  counters: number; // sum of raw cumulative counters; dropping means the broker restarted
}

export interface Hop { edge: EdgeId; src: Src; n: number }

interface RawQueue {
  name: string;
  messages?: number;
  messages_ready?: number;
  messages_unacknowledged?: number;
  consumers?: number;
  message_stats?: { publish?: number; deliver_get?: number; ack?: number };
}

const empty: QueueStat = { messages: 0, ready: 0, unacked: 0, consumers: 0, published: 0, delivered: 0, acked: 0 };

// checkouts: cumulative checkouts made by this page; they enter "orders" from the API, not from the worker.
export function parse(
  raw: RawQueue[],
  ordersEx: { message_stats?: { publish_in?: number } },
  checkouts = 0,
  at = Date.now(),
): Snapshot {
  const q = Object.fromEntries(QUEUES.map((n) => [n, empty])) as Record<QueueName, QueueStat>;
  for (const r of raw) {
    if (!(QUEUES as readonly string[]).includes(r.name)) continue;
    q[r.name as QueueName] = {
      messages: r.messages ?? 0,
      ready: r.messages_ready ?? 0,
      unacked: r.messages_unacknowledged ?? 0,
      consumers: r.consumers ?? 0,
      published: r.message_stats?.publish ?? 0,
      delivered: r.message_stats?.deliver_get ?? 0,
      acked: r.message_stats?.ack ?? 0,
    };
  }
  const publishIn = ordersEx.message_stats?.publish_in ?? 0;
  const inflight = (n: QueueName) => q[n].delivered - q[n].acked; // ≥ failures
  // ponytail: uses the unacked depth, so a poll landing between deliver and ack can draw one extra retry; needs a nack counter the API doesn't have.
  const failed = (n: QueueName) => inflight(n) - q[n].unacked;
  // Only stock publishes reservation.created, and only payment consumes it, so payment's publishes are stock's
  // share of the worker's output; the rest is payment's.
  // ponytail: reservation.rejected (stock, rare) is counted as payment; a binding-level counter would split it exactly.
  // Capped by the worker's total: the two counters report on different channels and can be a poll apart.
  const fromStock = Math.min(q.payment.published, publishIn - checkouts);
  const totals: Totals = {
    "worker-orders|stock": fromStock,
    "worker-orders|payment": publishIn - checkouts - fromStock,
    "worker-expiry|stock": q["expiry.q"].published, // only stock publishes reservation.expired
    "bus-dlq|none": q.dlq.published, // painted from the dlq's x-death by useLive
  };
  for (const n of ["stock", "payment", "notification"] as const) {
    totals[`orders-${n}|${n}`] = q[n].published;
    totals[`${n}-worker|${n}`] = q[n].delivered;
    totals[`bus-retry|${n}`] = failed(n);
  }
  const counters = publishIn + QUEUES.reduce((s, n) => s + q[n].published + q[n].delivered + q[n].acked, 0);
  return { at, queues: q, totals, counters };
}

// high: the highest total seen per edge; prev: the previous snapshot (undefined on the first poll).
// Returns the hops since prev and the new high-water marks.
export function diff(high: Totals, prev: Snapshot | undefined, next: Snapshot): { hops: Hop[]; high: Totals } {
  const before = prev ?? next;
  const q = next.queues;
  // Dips while a backlog waits, so only new re-entries count.
  // ponytail: a page opened mid-backlog baselines the dip and draws the drain as re-entries once; reload after it drains.
  const reentered = (n: QueueName) => before.queues[n].delivered - q[n].published;
  const inflight = (n: QueueName) => q[n].delivered - q[n].acked;
  // Fallback for exits the re-entries miss: previous poll's "in" minus current depth
  // (previous, because depth lags the counter that fed it and would draw a release on arrival).
  const into = (edge: EdgeId, t: Totals) => Object.entries(t).reduce((s, [k, v]) => (k.startsWith(`${edge}|`) ? s + v : s), 0);
  const lagged = (inEdge: EdgeId, n: QueueName) => into(inEdge, before.totals) - q[n].messages;
  const totals: Totals = {
    ...next.totals,
    "orders-stock|stock+re": reentered("stock"),
    "orders-payment|payment+re": reentered("payment"),
    "orders-notification|notification+re": reentered("notification"),
    // payment is only fed reservation.created, so its re-entries all came out of retry.q; color comes from paint's FIFO
    "retryq-orders|none": Math.max(reentered("payment"), lagged("bus-retry", "retry.q")),
    // stock re-entries are expiries, except retried messages a stock/notification failure sent back
    "expiryq-orders|stock": Math.max(reentered("stock") - inflight("stock") - inflight("notification"), lagged("worker-expiry", "expiry.q")),
  };
  if (prev && next.counters < prev.counters) return { hops: [], high: totals }; // stats reset: start over
  const moved: Record<string, number> = {};
  const out: Totals = {};
  for (const [key, total] of Object.entries(totals)) {
    const top = high[key] ?? total; // first sighting: baseline, no tokens for history
    const lane = key.split("+")[0];
    if (total > top) moved[lane] = (moved[lane] ?? 0) + total - top;
    out[key] = Math.max(top, total);
  }
  const hops = Object.entries(moved).map(([lane, n]) => {
    const [edge, src] = lane.split("|") as [EdgeId, Src];
    return { edge, src, n };
  });
  return { hops, high: out };
}

const FIFO_MAX = 200;

// retry.q has one fixed TTL, so messages leave it in the order they entered: each nack pushes its queue's color,
// each exit out of retry.q pops one. Exits are taken before this poll's nacks (those still have 10 s to wait).
// Pure: returns the colored hops and the new FIFO.
export function paint(hops: Hop[], fifo: Src[]): { hops: Hop[]; fifo: Src[] } {
  const f = [...fifo];
  const out: Hop[] = [];
  for (const h of hops.filter((h) => h.edge === "retryq-orders"))
    for (let i = 0; i < h.n; i++) out.push({ edge: h.edge, src: f.shift() ?? "none", n: 1 });
  for (const h of hops.filter((h) => h.edge !== "retryq-orders")) {
    out.push(h);
    if (h.edge === "bus-retry") f.push(...Array<Src>(h.n).fill(h.src));
  }
  return { hops: out, fifo: f.slice(-FIFO_MAX) };
}
