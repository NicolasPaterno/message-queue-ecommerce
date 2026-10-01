"use client";

import { useState } from "react";
import { restock, type OrderStatus, type Product, type TrackedOrder } from "@/lib/shop";

const CHIP: Record<OrderStatus, string> = {
  CART: "border-ink text-ink",
  PLACED: "border-ink text-ink",
  RESERVED: "border-line-retry bg-line-retry text-ink",
  PAID: "border-line-orders bg-line-orders text-white",
  DECLINED: "border-line-dlx bg-line-dlx text-white",
  OUT_OF_STOCK: "border-line-dlx bg-line-dlx text-white",
  EXPIRED: "border-line-expiry bg-line-expiry text-white",
};

const BTN =
  "rounded-md border-2 border-ink bg-white transition-colors hover:bg-ink hover:text-paper focus-visible:outline-3 focus-visible:outline-offset-2 focus-visible:outline-ink disabled:pointer-events-none disabled:opacity-40";

const brl = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" });
const ms = (n: number) => (n < 1000 ? `${Math.round(n)} ms` : `${(n / 1000).toFixed(1)} s`);

export default function Sidebar({
  products,
  online,
  orders,
  announce,
  onOrder,
}: {
  products: Product[];
  online: boolean;
  orders: TrackedOrder[];
  announce: string;
  onOrder: (quantity: number, times?: number) => void;
}) {
  const [stockErr, setStockErr] = useState<{ id: string; msg: string }>();
  const add = (id: string, n: number) =>
    restock(id, n).catch((e: Error) => {
      setStockErr({ id, msg: e.message });
      setTimeout(() => setStockErr((cur) => (cur?.id === id ? undefined : cur)), 4000);
    });
  const p = products[0];
  const off = !online || !p;
  const actions = [
    { label: "Novo pedido", sub: "1 unidade", run: () => onOrder(1) },
    { label: "Rajada ×5", sub: "5 pedidos simultâneos", run: () => onOrder(1, 5) },
    { label: "Sem estoque", sub: p ? `${p.available + 1} unidades (estoque + 1)` : "—", run: () => p && onOrder(p.available + 1) },
  ];
  return (
    <aside className="flex min-h-0 flex-col gap-6 overflow-y-auto border-r-2 border-ink/10 bg-paper/80 p-6">
      <header>
        <h1 className="text-[28px] leading-tight font-extrabold">Mapa de Mensagens</h1>
        <p className="font-mono text-xs text-muted">message-queue-ecommerce · RabbitMQ</p>
        <p className="mt-3 flex items-center gap-2 font-mono text-xs">
          <span className={`size-2.5 rounded-full ${online ? "animate-pulse bg-line-orders" : "bg-line-dlx"}`} />
          {online ? "ao vivo · 1 s" : "fora do ar"}
        </p>
      </header>

      <section className="flex flex-col gap-2" aria-label="Ações">
        {actions.map((a) => (
          <button
            key={a.label}
            onClick={a.run}
            disabled={off}
            className={`${BTN} px-4 py-2.5 text-left`}
          >
            <span className="block font-semibold">{a.label}</span>
            <span className="block font-mono text-[11px] opacity-70">{a.sub}</span>
          </button>
        ))}
      </section>

      <section className="flex flex-col gap-2" aria-label="Estoque">
        <h2 className="text-sm font-extrabold tracking-widest text-muted uppercase">Estoque</h2>
        {products.map((x) => (
          <div key={x.id} className="font-mono text-xs">
            <div className="flex items-center gap-2">
              <span className="text-muted">
                {x.name} · {brl.format(x.price_cents / 100)} · <span className="font-semibold text-ink">{x.available} un.</span>
              </span>
              {x.available === 0 && (
                <span className="rounded border-2 border-line-dlx bg-line-dlx px-1.5 py-px text-[10px] font-semibold text-white">ESGOTADO</span>
              )}
              <span className="ml-auto flex gap-1">
                <button onClick={() => add(x.id, 5)} disabled={!online} className={`${BTN} px-2 py-1 text-xs`}>
                  +5
                </button>
                {/* ponytail: 10 - available reads a value up to 1 s stale; it can only over-add a little, never oversell. GREATEST server-side if exact counts matter. */}
                <button
                  onClick={() => add(x.id, 10 - x.available)}
                  disabled={!online || x.available >= 10}
                  className={`${BTN} px-2 py-1 text-xs ${x.available === 0 ? "bg-ink! text-paper!" : ""}`}
                >
                  Repor 10
                </button>
              </span>
            </div>
            {stockErr?.id === x.id && <p className="mt-1 text-line-dlx">{stockErr.msg}</p>}
          </div>
        ))}
      </section>

      <section className="flex min-h-0 flex-col gap-2" aria-label="Pedidos">
        <h2 className="text-sm font-extrabold tracking-widest text-muted uppercase">Pedidos</h2>
        {orders.length === 0 && <p className="font-mono text-xs text-muted">Nenhum pedido ainda — clique em Novo pedido.</p>}
        <ol className="flex flex-col gap-2">
          {orders.map((o) => {
            const last = o.steps.at(-1)?.status;
            return (
              <li key={o.key} className="rounded-md border border-ink/20 bg-white px-3 py-2">
                <div className="flex items-center gap-2 font-mono text-xs">
                  <span className="font-semibold">{o.id?.slice(0, 8) ?? "—"}</span>
                  <span className="text-muted">×{o.quantity}</span>
                  {o.error ? (
                    <span className="ml-auto text-line-dlx">{o.error}</span>
                  ) : (
                    last && <span className={`ml-auto rounded border-2 px-1.5 py-px text-[10px] font-semibold ${CHIP[last]}`}>{last}</span>
                  )}
                </div>
                {o.steps.length > 0 && (
                  <p className="mt-1 font-mono text-[11px] text-muted">
                    {o.steps.map((s) => `${s.status} ${ms(s.at - o.startedAt)}`).join(" → ")}
                  </p>
                )}
              </li>
            );
          })}
        </ol>
      </section>

      <p aria-live="polite" className="sr-only">
        {announce}
      </p>
    </aside>
  );
}
