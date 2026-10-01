"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { diff, paint, parse, type EdgeId, type Hop, type Snapshot, type Src, type Totals } from "./flow";
import { getProducts, type Product } from "./shop";

export interface LiveHop { id: number; edge: EdgeId; src: Src } // ids only grow; consumers draw the ids they haven't seen

const POLL_MS = 1000;
const MAX_PER_EDGE = 20; // a burst can't flood the DOM
const KEEP = 100;
const QUEUES_URL = "/mq/queues/%2Fshop?columns=name,messages,messages_ready,messages_unacknowledged,consumers,message_stats.publish,message_stats.deliver_get,message_stats.ack";
const ORDERS_URL = "/mq/exchanges/%2Fshop/orders?columns=message_stats.publish_in";

const DLQ_PEEK_MAX = 50;

// The dlq doesn't say which queue gave up on a message, but each message's x-death does: the queue that rejected it.
// Peeks (requeue) the newest n messages; a failed peek or a message with no rejection (bad JSON) stays "none".
async function dlqSources(n: number, depth: number): Promise<Src[]> {
  try {
    const res = await fetch("/mq/queues/%2Fshop/dlq/get", {
      method: "POST",
      body: JSON.stringify({ count: Math.min(depth, DLQ_PEEK_MAX), ackmode: "ack_requeue_true", encoding: "auto" }),
    });
    if (!res.ok) throw new Error(`${res.status}`);
    const msgs: { properties: { headers?: { "x-death"?: { queue: string; reason: string }[] } } }[] = await res.json();
    const srcs = msgs.slice(-n).map((m) => {
      const q = m.properties.headers?.["x-death"]?.find((d) => d.reason === "rejected")?.queue;
      return q === "stock" || q === "payment" || q === "notification" ? q : "none";
    });
    return [...Array<Src>(Math.max(n - srcs.length, 0)).fill("none"), ...srcs];
  } catch {
    return Array<Src>(n).fill("none");
  }
}

const json = async (url: string) => {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`${res.status}`);
  return res.json();
};

let nextId = 0;

export function useLive() {
  const [snap, setSnap] = useState<Snapshot>();
  const [hops, setHops] = useState<LiveHop[]>([]);
  const [products, setProducts] = useState<Product[]>([]);
  const [online, setOnline] = useState(true);
  const high = useRef<Totals>({});
  const prev = useRef<Snapshot>(undefined);
  const checkouts = useRef(0); // cumulative; parse subtracts it from orders publish_in
  const failures = useRef(0);
  const fifo = useRef<Src[]>([]); // colors waiting in retry.q, see paint

  useEffect(() => {
    let alive = true;
    const tick = async () => {
      try {
        const [queues, orders, prods] = await Promise.all([json(QUEUES_URL), json(ORDERS_URL), getProducts()]);
        if (!alive) return;
        const next = parse(queues, orders, checkouts.current);
        const r = diff(high.current, prev.current, next);
        high.current = r.high;
        prev.current = next;
        const painted = paint(r.hops, fifo.current);
        fifo.current = painted.fifo;
        const hops: Hop[] = [];
        for (const h of painted.hops) {
          if (h.edge !== "bus-dlq") hops.push(h);
          else for (const src of await dlqSources(h.n, next.queues.dlq.messages)) hops.push({ edge: h.edge, src, n: 1 });
        }
        if (!alive) return;
        const fresh = hops.flatMap((h) => Array.from({ length: Math.min(h.n, MAX_PER_EDGE) }, () => ({ edge: h.edge, src: h.src })));
        if (fresh.length) setHops((h) => [...h, ...fresh.map((f) => ({ id: nextId++, ...f }))].slice(-KEEP));
        failures.current = 0;
        setSnap(next);
        setProducts(prods);
        setOnline(true);
      } catch {
        if (alive && ++failures.current >= 2) setOnline(false);
      }
    };
    tick();
    const t = setInterval(tick, POLL_MS);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, []);

  // A 201 checkout: the API's publish is drawn now (the broker stats can't tell it from the worker's).
  const noteCheckout = useCallback(() => {
    checkouts.current++;
    setHops((h) =>
      [...h, { id: nextId++, edge: "client-api" as const, src: "api" as const }, { id: nextId++, edge: "api-orders" as const, src: "api" as const }].slice(-KEEP),
    );
  }, []);

  return { snap, hops, products, online, noteCheckout };
}
