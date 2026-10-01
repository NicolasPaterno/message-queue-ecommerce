"use client";

import { useEffect, useId, useState } from "react";
import { explain, restock, type OrderEvent, type OrderStatus, type Outcome, type Product, type Simulate, type TrackedOrder } from "@/lib/shop";

const CHIP: Record<OrderStatus, string> = {
  CART: "border-ink text-ink",
  PLACED: "border-ink text-ink",
  RESERVED: "border-line-retry bg-line-retry text-ink",
  PAID: "border-line-orders bg-line-orders text-white",
  DECLINED: "border-line-dlx bg-line-dlx text-white",
  OUT_OF_STOCK: "border-line-dlx bg-line-dlx text-white",
  EXPIRED: "border-line-expiry bg-line-expiry text-white",
};

const OUTCOME: Record<Outcome, [string, string]> = {
  ack: ["ok", "border-line-orders text-line-orders"],
  retry: ["falha", "border-line-retry bg-line-retry text-ink"],
  dlq: ["→ dlq", "border-line-dlx bg-line-dlx text-white"],
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
  onOrder: (quantity: number, times?: number, simulate?: Simulate) => void;
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
  const failures = [
    { label: "Pagamento falha 1×", sub: "retry → recupera", run: () => onOrder(1, 1, "payment_once") },
    { label: "Pagamento sempre falha", sub: "3× → dlq → expira em 120 s", run: () => onOrder(1, 1, "payment_always") },
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

      <section className="flex flex-col gap-2" aria-label="Falhas">
        <h2 className="text-sm font-extrabold tracking-widest text-muted uppercase">Falhas</h2>
        {failures.map((a) => (
          <button key={a.label} onClick={a.run} disabled={off} className={`${BTN} px-4 py-2.5 text-left`}>
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
          {orders.map((o) => (
            <OrderRow key={o.key} o={o} initialOpen={!!o.simulate} />
          ))}
        </ol>
      </section>

      <p aria-live="polite" className="sr-only">
        {announce}
      </p>
    </aside>
  );
}

function OrderRow({ o, initialOpen = false }: { o: TrackedOrder; initialOpen?: boolean }) {
  const [open, setOpen] = useState(initialOpen);
  const panel = useId();
  const last = o.steps.at(-1)?.status;
  return (
    <li className="rounded-md border border-ink/20 bg-white px-3 py-2">
      <div className="flex items-center gap-2 font-mono text-xs">
        {!o.error && (
          <button
            onClick={() => setOpen(!open)}
            aria-expanded={open}
            aria-controls={panel}
            aria-label={`Eventos do pedido ${o.id?.slice(0, 8) ?? ""}`}
            className="rounded px-0.5 text-muted hover:text-ink focus-visible:outline-2 focus-visible:outline-ink"
          >
            {open ? "▾" : "▸"}
          </button>
        )}
        <span className="font-semibold">{o.id?.slice(0, 8) ?? "—"}</span>
        <span className="text-muted">×{o.quantity}</span>
        {o.simulate && <span className="text-line-retry">{o.simulate === "payment_once" ? "falha 1×" : "sempre falha"}</span>}
        {o.error ? (
          <span className="ml-auto text-line-dlx">{o.error}</span>
        ) : (
          last && <span className={`ml-auto rounded border-2 px-1.5 py-px text-[10px] font-semibold ${CHIP[last]}`}>{last}</span>
        )}
      </div>
      {o.steps.length > 0 && (
        <p className="mt-1 font-mono text-[11px] text-muted">{o.steps.map((s) => `${s.status} ${ms(s.at - o.startedAt)}`).join(" → ")}</p>
      )}
      {!o.error && last && <Countdown events={o.events} status={last} />}
      {open && !o.error && <EventList id={panel} events={o.events} status={last ?? "PLACED"} />}
    </li>
  );
}

function EventList({ id, events, status }: { id: string; events: OrderEvent[]; status: OrderStatus }) {
  if (events.length === 0)
    return (
      <p id={id} className="mt-2 font-mono text-[11px] text-muted">
        aguardando o worker…
      </p>
    );
  const t0 = Date.parse(events[0].at);
  return (
    <ol id={id} className="mt-2 flex flex-col gap-1.5 border-t border-ink/10 pt-2">
      {events.map((e, i) => {
        const [label, chip] = OUTCOME[e.outcome];
        return (
          <li key={i} className={`font-mono text-[11px] ${e.queue === "notification" ? "opacity-60" : ""}`}>
            <div className="flex items-center gap-1.5">
              <span className="w-12 shrink-0 text-muted">+{((Date.parse(e.at) - t0) / 1000).toFixed(1)} s</span>
              <span className="font-semibold">{e.queue === "notification" ? "notif." : e.queue}</span>
              <span className="truncate text-muted">{e.type}</span>
              <span className={`ml-auto shrink-0 rounded border px-1 text-[10px] font-semibold ${chip}`}>{label}</span>
            </div>
            <p className="pl-[54px] text-muted">{explain(e, status)}</p>
          </li>
        );
      })}
    </ol>
  );
}

// The next TTL that will move this order: retry.q (10 s) after a failed payment, else expiry.q (120 s) after the reservation.
// Deadlines compare Date.now() with the server's at: api, worker and browser share one machine, so there's no skew.
function deadline(events: OrderEvent[], status: OrderStatus): { label: string; at: number } | undefined {
  const pay = events.findLast((e) => e.queue === "payment");
  if (pay?.outcome === "retry") return { label: "volta em", at: Date.parse(pay.at) + 10_000 };
  const reserved = events.find((e) => e.queue === "stock" && e.type === "order.placed" && e.outcome === "ack");
  if (!reserved || status === "OUT_OF_STOCK" || events.some((e) => e.queue === "stock" && e.type === "reservation.expired")) return;
  return { label: "expira em", at: Date.parse(reserved.at) + 120_000 };
}

function Countdown({ events, status }: { events: OrderEvent[]; status: OrderStatus }) {
  const d = deadline(events, status);
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    if (!d) return;
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [d?.at]); // eslint-disable-line react-hooks/exhaustive-deps
  if (!d) return null;
  const s = Math.max(0, Math.ceil((d.at - now) / 1000));
  const text = d.label === "volta em" ? `${s} s` : `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
  return (
    <p className="mt-1 font-mono text-[11px] text-line-retry">
      {d.label} {text}
    </p>
  );
}
