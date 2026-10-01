"use client";

import { AnimatePresence } from "motion/react";
import { useCallback, useEffect, useRef, useState } from "react";
import DlqPanel from "@/components/DlqPanel";
import FlowMap from "@/components/FlowMap";
import Sidebar from "@/components/Sidebar";
import { useLive } from "@/lib/live";
import { getOrder, placeOrder, type OrderEvent, type OrderStatus, type Simulate, type TrackedOrder } from "@/lib/shop";

const TRACK_MS = 700;
const GIVE_UP_MS = 150_000; // > the 120 s reservation TTL
const MAX_ROWS = 12;

let nextKey = 0;

// A PAID/DECLINED order still has its reservation.expired coming ~120 s later, so polling stops on that event, not on the status.
const done = (o: TrackedOrder) =>
  o.steps.at(-1)?.status === "OUT_OF_STOCK" || o.events.some((e) => e.queue === "stock" && e.type === "reservation.expired");

export default function Page() {
  const { snap, hops, products, online, noteCheckout } = useLive();
  const [orders, setOrders] = useState<TrackedOrder[]>([]);
  const [announce, setAnnounce] = useState("");
  const [dlq, setDlq] = useState(false);
  const current = useRef(orders);
  useEffect(() => {
    current.current = orders;
  });

  const onOrder = (quantity: number, times = 1, simulate?: Simulate) => {
    const productId = products[0]?.id;
    if (!productId) return;
    for (let i = 0; i < times; i++) {
      const key = nextKey++;
      const startedAt = performance.now();
      placeOrder(productId, quantity, simulate)
        .then(({ id }) => {
          noteCheckout();
          return { key, id, quantity, simulate, startedAt, steps: [{ status: "PLACED" as OrderStatus, at: performance.now() }], events: [] };
        })
        .catch((e: Error) => ({ key, quantity, simulate, startedAt, steps: [], events: [], error: e.message }))
        .then((o: TrackedOrder) => setOrders((os) => [o, ...os].slice(0, MAX_ROWS)));
    }
  };

  // Follow every order until its last scheduled message (the reservation expiry) has been handled.
  // ponytail: up to 12 orders × 1.4 req/s on localhost, no backoff; add one if rows ever go past 12.
  useEffect(() => {
    const t = setInterval(async () => {
      const now = performance.now();
      const pending = current.current.filter((o) => o.id && !o.error && !done(o) && now - o.startedAt < GIVE_UP_MS);
      const seen = await Promise.all(pending.map((o) => getOrder(o.id!).then((r) => ({ o, ...r }), () => null)));
      const changed = new Map<string, { status: OrderStatus; events: OrderEvent[] }>();
      let newest: { id: string; e: OrderEvent } | undefined;
      for (const s of seen) {
        if (!s || (s.status === s.o.steps.at(-1)?.status && s.events.length === s.o.events.length)) continue;
        changed.set(s.o.id!, s);
        const e = s.events.at(-1);
        if (s.events.length > s.o.events.length && e && (!newest || e.at > newest.e.at)) newest = { id: s.o.id!, e };
      }
      if (!changed.size) return;
      const at = performance.now();
      setOrders((os) =>
        os.map((o) => {
          const c = o.id && changed.get(o.id);
          if (!c) return o;
          return { ...o, events: c.events, steps: c.status === o.steps.at(-1)?.status ? o.steps : [...o.steps, { status: c.status, at }] };
        }),
      );
      if (newest) {
        const { id, e } = newest;
        const what = e.outcome === "retry" ? `tentativa ${e.attempt}/3 falhou` : e.outcome === "dlq" ? "→ dlq" : "ok";
        setAnnounce(`Pedido ${id.slice(0, 8)}: ${e.queue} ${what}`);
      }
    }, TRACK_MS);
    return () => clearInterval(t);
  }, []);

  const closeDlq = useCallback(() => {
    setDlq(false);
    document.querySelector<SVGElement>('[aria-label^="Abrir DLQ"]')?.focus();
  }, []);

  return (
    <>
      {!online && (
        <div role="alert" className="fixed inset-x-0 top-0 z-30 bg-line-dlx px-6 py-2 text-center font-semibold text-white">
          Stack fora do ar — <code className="font-mono">cd deploy &amp;&amp; docker compose up -d</code>
        </div>
      )}
      <main className="grid h-screen grid-cols-[360px_1fr]">
        <Sidebar products={products} online={online} orders={orders} announce={announce} onOrder={onOrder} />
        <FlowMap snap={snap} hops={hops} products={products} online={online} onDlq={() => setDlq(true)} />
      </main>
      <AnimatePresence>{dlq && <DlqPanel onClose={closeDlq} />}</AnimatePresence>
    </>
  );
}
