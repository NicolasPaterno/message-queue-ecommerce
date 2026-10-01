"use client";

import { AnimatePresence } from "motion/react";
import { useCallback, useEffect, useRef, useState } from "react";
import DlqPanel from "@/components/DlqPanel";
import FlowMap from "@/components/FlowMap";
import Sidebar from "@/components/Sidebar";
import { useLive } from "@/lib/live";
import { TERMINAL, getOrder, placeOrder, type OrderStatus, type TrackedOrder } from "@/lib/shop";

const TRACK_MS = 700;
const GIVE_UP_MS = 150_000; // > the 120 s reservation TTL
const MAX_ROWS = 12;

let nextKey = 0;

export default function Page() {
  const { snap, hops, products, online, noteCheckout } = useLive();
  const [orders, setOrders] = useState<TrackedOrder[]>([]);
  const [announce, setAnnounce] = useState("");
  const [dlq, setDlq] = useState(false);
  const current = useRef(orders);
  useEffect(() => {
    current.current = orders;
  });

  const onOrder = (quantity: number, times = 1) => {
    const productId = products[0]?.id;
    if (!productId) return;
    for (let i = 0; i < times; i++) {
      const key = nextKey++;
      const startedAt = performance.now();
      placeOrder(productId, quantity)
        .then(({ id }) => {
          noteCheckout();
          return { key, id, quantity, startedAt, steps: [{ status: "PLACED" as OrderStatus, at: performance.now() }] };
        })
        .catch((e: Error) => ({ key, quantity, startedAt, steps: [], error: e.message }))
        .then((o: TrackedOrder) => setOrders((os) => [o, ...os].slice(0, MAX_ROWS)));
    }
  };

  // Follow every order still in flight until it settles.
  useEffect(() => {
    const t = setInterval(async () => {
      const now = performance.now();
      const pending = current.current.filter(
        (o) => o.id && !o.error && !TERMINAL.has(o.steps.at(-1)!.status) && now - o.startedAt < GIVE_UP_MS,
      );
      const seen = await Promise.all(pending.map((o) => getOrder(o.id!).then((r) => [o.id!, r.status] as const, () => null)));
      const changed = new Map(seen.filter((s) => s && s[1] !== current.current.find((o) => o.id === s[0])?.steps.at(-1)?.status) as [string, OrderStatus][]);
      if (!changed.size) return;
      const at = performance.now();
      setOrders((os) => os.map((o) => (o.id && changed.has(o.id) ? { ...o, steps: [...o.steps, { status: changed.get(o.id)!, at }] } : o)));
      setAnnounce([...changed].map(([id, st]) => `Pedido ${id.slice(0, 8)}: ${st}`).join(". "));
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
