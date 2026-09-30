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

export type EdgeId =
  | "client-api" | "api-orders" | "worker-orders"
  | "orders-stock" | "orders-payment" | "orders-notification"
  | "stock-worker" | "payment-worker" | "notification-worker"
  | "worker-expiry" | "expiryq-orders"
  | "bus-retry" | "retryq-orders"
  | "bus-dlq";

// Keyed by edge; "edge+part" keys are extra totals whose increases add to the same edge.
export type Totals = Record<string, number>;

export interface Snapshot {
  at: number;
  queues: Record<QueueName, QueueStat>;
  totals: Totals;
  counters: number; // sum of raw cumulative counters; dropping means the broker restarted
}

export interface Hop { edge: EdgeId; n: number }

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
  const totals: Totals = {
    "worker-orders": publishIn - checkouts,
    "bus-retry": failed("stock") + failed("payment") + failed("notification"),
    "worker-expiry": q["expiry.q"].published,
    "bus-dlq": q.dlq.published,
  };
  for (const n of ["stock", "payment", "notification"] as const) {
    totals[`orders-${n}`] = q[n].published;
    totals[`${n}-worker`] = q[n].delivered;
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
  const lagged = (inEdge: EdgeId, n: QueueName) => (before.totals[inEdge] ?? 0) - q[n].messages;
  const totals: Totals = {
    ...next.totals,
    "orders-stock+re": reentered("stock"),
    "orders-payment+re": reentered("payment"),
    "orders-notification+re": reentered("notification"),
    // payment is only fed reservation.created, so its re-entries all came out of retry.q
    "retryq-orders": Math.max(reentered("payment"), lagged("bus-retry", "retry.q")),
    // stock re-entries are expiries, except retried messages a stock/notification failure sent back
    "expiryq-orders": Math.max(reentered("stock") - inflight("stock") - inflight("notification"), lagged("worker-expiry", "expiry.q")),
  };
  if (prev && next.counters < prev.counters) return { hops: [], high: totals }; // stats reset: start over
  const moved: Partial<Record<EdgeId, number>> = {};
  const out: Totals = {};
  for (const [key, total] of Object.entries(totals)) {
    const top = high[key] ?? total; // first sighting: baseline, no tokens for history
    const edge = key.split("+")[0] as EdgeId;
    if (total > top) moved[edge] = (moved[edge] ?? 0) + total - top;
    out[key] = Math.max(top, total);
  }
  return { hops: Object.entries(moved).map(([edge, n]) => ({ edge: edge as EdgeId, n })), high: out };
}
