"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { diff, parse, type EdgeId, type Snapshot, type Totals } from "./flow";
import { getProducts, type Product } from "./shop";

export interface LiveHop { id: number; edge: EdgeId }

const POLL_MS = 1000;
const MAX_PER_EDGE = 20; // a burst can't flood the DOM
const QUEUES_URL = "/mq/queues/%2Fshop?columns=name,messages,messages_ready,messages_unacknowledged,consumers,message_stats.publish,message_stats.deliver_get,message_stats.ack";
const ORDERS_URL = "/mq/exchanges/%2Fshop/orders?columns=message_stats.publish_in";

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
        const fresh = r.hops.flatMap((h) => Array.from({ length: Math.min(h.n, MAX_PER_EDGE) }, () => h.edge));
        setHops(fresh.map((edge) => ({ id: nextId++, edge })));
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

  const noteCheckout = useCallback(() => {
    checkouts.current++;
  }, []);

  return { snap, hops, products, online, noteCheckout };
}
