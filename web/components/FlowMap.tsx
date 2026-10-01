"use client";

import { AnimatePresence, animate, motion, useMotionValue, useReducedMotion, useTransform } from "motion/react";
import { useCallback, useEffect, useState } from "react";
import type { EdgeId, QueueName, Snapshot, Src } from "@/lib/flow";
import type { LiveHop } from "@/lib/live";
import type { Product } from "@/lib/shop";
import { API, CLIENT, DASH, EDGES, EXCHANGES, LANES, POSTGRES, STATIONS, WORKER, along } from "@/lib/topology";

// Full class names so Tailwind sees them. Color = the message's source queue.
const FILL: Record<Src, string> = {
  api: "fill-src-api",
  stock: "fill-src-stock",
  payment: "fill-src-payment",
  notification: "fill-src-notification",
  none: "fill-src-none",
};
const TAB: Record<QueueName, string> = {
  stock: FILL.stock,
  payment: FILL.payment,
  notification: FILL.notification,
  "retry.q": "fill-rail",
  "expiry.q": "fill-rail",
  dlq: "fill-line-dlx",
};
const SRC_NAME: [Src, string][] = [
  ["stock", "stock"],
  ["payment", "payment"],
  ["notification", "notification"],
  ["api", "ainda sem fila"],
  ["none", "desconhecida"],
];

// One knob for every duration on the map; 1 = the original speed.
const PACE = 2.5;

// Hops of one poll are drawn as a wave in causal order: out of a TTL queue → into a queue → to the worker → out of the worker.
const STAGE: Record<EdgeId, number> = {
  "client-api": 0, "api-orders": 1, "retryq-orders": 0, "expiryq-orders": 0,
  "orders-stock": 1, "orders-payment": 1, "orders-notification": 1,
  "stock-worker": 2, "payment-worker": 2, "notification-worker": 2,
  "worker-orders": 3, "worker-expiry": 3, "bus-retry": 3, "bus-dlq": 3,
};
const MAX_TOKENS = 160;
const AT = Object.fromEntries(Object.entries(EDGES).map(([id, e]) => [id, along(e.d)])) as Record<EdgeId, ReturnType<typeof along>>;
const WORKER_EDGES: Partial<Record<EdgeId, keyof typeof LANES>> = {
  "stock-worker": "stock",
  "payment-worker": "payment",
  "notification-worker": "notification",
};

interface Tok { id: number; edge: EdgeId; src: Src; delay: number }

function schedule(hops: LiveHop[]): Tok[] {
  const perStage = [0, 0, 0, 0];
  return hops.map((h) => {
    const s = STAGE[h.edge];
    return { ...h, delay: PACE * (s * 0.35 + Math.min(perStage[s]++, 8) * 0.05) };
  });
}

export default function FlowMap({
  snap,
  hops,
  products,
  online,
  onDlq,
}: {
  snap?: Snapshot;
  hops: LiveHop[];
  products: Product[];
  online: boolean;
  onDlq?: () => void;
}) {
  const reduce = useReducedMotion();
  const [seen, setSeen] = useState(hops);
  const [lastId, setLastId] = useState(() => hops.at(-1)?.id ?? -1);
  const [tokens, setTokens] = useState<Tok[]>([]);
  const [flash, setFlash] = useState<Partial<Record<keyof typeof LANES, number>>>({});
  const [pulses, setPulses] = useState<{ id: number; src: Src }[]>([]);

  // New poll → queue its tokens (adjusting state during render, no effect needed).
  // A hidden tab pauses animations; skip its tokens instead of replaying a backlog on return.
  if (hops !== seen) {
    const fresh = hops.filter((h) => h.id > lastId);
    setSeen(hops);
    setLastId(hops.at(-1)?.id ?? lastId);
    if (!reduce && fresh.length && !document.hidden) setTokens((t) => [...t, ...schedule(fresh)].slice(-MAX_TOKENS));
  }

  const arrived = useCallback((tok: Tok) => {
    setTokens((t) => t.filter((x) => x.id !== tok.id));
    const lane = WORKER_EDGES[tok.edge];
    if (lane) {
      setFlash((f) => ({ ...f, [lane]: tok.id }));
      setPulses((p) => [...p, { id: tok.id, src: tok.src }]);
    }
  }, []);

  const q = snap?.queues;
  const workers = q ? Math.max(q.stock.consumers, q.payment.consumers, q.notification.consumers) : 1;

  return (
    <figure className="flex h-full flex-col">
      <motion.svg
        viewBox="0 0 1600 900"
        role="img"
        aria-label={
          q
            ? `Mapa das filas: stock ${q.stock.ready}, payment ${q.payment.ready}, notification ${q.notification.ready}, retry.q ${q["retry.q"].messages}, expiry.q ${q["expiry.q"].messages}, dlq ${q.dlq.messages}; ${workers} worker(s)`
            : "Mapa das filas: carregando"
        }
        className={`min-h-0 w-full flex-1 transition-opacity ${online ? "" : "opacity-40"}`}
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.4 }}
      >
        {/* lines: neutral rails, the exchange told apart by its stroke pattern */}
        {(Object.entries(EDGES) as [EdgeId, (typeof EDGES)[EdgeId]][]).map(([id, e]) => (
          <path
            key={id}
            d={e.d}
            className="fill-none stroke-rail"
            strokeWidth={e.line === "http" ? 4 : 6}
            strokeDasharray={DASH[e.line]}
            strokeLinecap={DASH[e.line] ? "butt" : "round"}
            strokeLinejoin="round"
          />
        ))}
        {/* worker → postgres (a commit per handled message, drawn as the pulse) */}
        <path d={`M1260 ${POSTGRES.y + 20} L1416 ${POSTGRES.y + 20}`} className="fill-none stroke-ink" strokeWidth={3} strokeDasharray="2 6" strokeLinecap="round" />
        {Object.values(EDGES).map(
          (e) =>
            e.label && (
              <text
                key={e.label[0]}
                x={e.label[1]}
                y={e.label[2]}
                textAnchor={e.label[3] ?? "middle"}
                className="fill-muted stroke-paper font-mono text-[13px] [paint-order:stroke]"
                strokeWidth={5}
              >
                {e.label[4] && <title>{e.label[4]}</title>}
                {e.label[0]}
              </text>
            ),
        )}
        <Legend />

        {/* client, api */}
        <Box x={CLIENT.x} y={CLIENT.y} w={100} h={60} title="cliente" sub="HTTP" />
        <Box x={API.x} y={API.y} w={120} h={70} title="api" sub=":8080" />

        {(Object.entries(EXCHANGES) as [string, (typeof EXCHANGES)[keyof typeof EXCHANGES]][]).map(([name, ex]) => (
          <g key={name}>
            <circle cx={ex.x} cy={ex.y} r={26} className="fill-white stroke-ink" strokeWidth={4} />
            <circle cx={ex.x} cy={ex.y} r={13} className="fill-white stroke-ink" strokeWidth={3} />
            <text
              x={ex.label ? ex.x - 14 : ex.x + 34}
              y={ex.label ? ex.y - 56 : ex.y + 48}
              textAnchor={ex.label ? "end" : "start"}
              className="fill-ink text-[16px] font-semibold"
            >
              {name}
            </text>
            <text
              x={ex.label ? ex.x - 14 : ex.x + 34}
              y={ex.label ? ex.y - 41 : ex.y + 63}
              textAnchor={ex.label ? "end" : "start"}
              className="fill-muted font-mono text-[11px]"
            >
              topic exchange
            </text>
          </g>
        ))}

        {(Object.keys(STATIONS) as QueueName[]).map((name) => (
          <Station key={name} name={name} stat={q?.[name]} reduce={!!reduce} onClick={name === "dlq" ? onDlq : undefined} />
        ))}

        <Worker count={workers} flash={flash} />

        {/* postgres */}
        <g>
          {pulses.map(({ id, src }) => (
            <motion.circle
              key={id}
              cx={POSTGRES.x}
              cy={POSTGRES.y + 20}
              className={`fill-none ${STROKE_SRC[src]}`}
              strokeWidth={3}
              initial={{ r: 30, opacity: 0.7 }}
              animate={{ r: 70, opacity: 0 }}
              transition={{ duration: 0.7 * PACE, ease: "easeOut" }}
              onAnimationComplete={() => setPulses((p) => p.filter((x) => x.id !== id))}
            />
          ))}
          <path
            d={`M${POSTGRES.x - 44} ${POSTGRES.y - 20} v80 a44 13 0 0 0 88 0 v-80`}
            className="fill-white stroke-ink"
            strokeWidth={3}
          />
          <ellipse cx={POSTGRES.x} cy={POSTGRES.y - 20} rx={44} ry={13} className="fill-white stroke-ink" strokeWidth={3} />
          <text x={POSTGRES.x} y={POSTGRES.y + 30} textAnchor="middle" className="fill-ink text-[16px] font-semibold">
            postgres
          </text>
          {products.map((p, i) => (
            <text key={p.id} x={POSTGRES.x} y={POSTGRES.y + 104 + i * 18} textAnchor="middle" className="fill-ink font-mono text-[13px]">
              {p.name}: {p.available} un.
            </text>
          ))}
        </g>

        {tokens.map((t) => (
          <Token key={t.id} tok={t} onDone={arrived} />
        ))}
      </motion.svg>
      <figcaption className="px-6 pb-4 font-mono text-xs text-muted">
        Contadores reais da Management API do RabbitMQ (atualizados a cada 1 s). Cada bolinha = 1 mensagem contada no
        intervalo, não uma mensagem identificada.
      </figcaption>
    </figure>
  );
}

function Box({ x, y, w, h, title, sub }: { x: number; y: number; w: number; h: number; title: string; sub: string }) {
  return (
    <g>
      <rect x={x - w / 2} y={y - h / 2} width={w} height={h} rx={8} className="fill-white stroke-ink" strokeWidth={3} />
      <text x={x} y={y - 2} textAnchor="middle" className="fill-ink text-[17px] font-semibold">
        {title}
      </text>
      <text x={x} y={y + 17} textAnchor="middle" className="fill-muted font-mono text-[11px]">
        {sub}
      </text>
    </g>
  );
}

const SQUARES = 12;

function Station({
  name,
  stat,
  reduce,
  onClick,
}: {
  name: QueueName;
  stat?: Snapshot["queues"][QueueName];
  reduce: boolean;
  onClick?: () => void;
}) {
  const { x, y, tab } = STATIONS[name];
  const ready = stat?.ready ?? 0;
  const shown = Math.min(ready, SQUARES);
  const button = onClick
    ? {
        role: "button",
        tabIndex: 0,
        "aria-label": `Abrir DLQ (${stat?.messages ?? 0} mensagens)`,
        onClick,
        onKeyDown: (e: React.KeyboardEvent) => (e.key === "Enter" || e.key === " ") && onClick(),
        className: "cursor-pointer outline-none [&:focus-visible>rect:first-child]:stroke-[6]",
      }
    : {};
  return (
    <g {...button}>
      <rect x={x - 90} y={y - 48} width={180} height={96} rx={10} className="fill-white stroke-ink" strokeWidth={3} />
      <rect x={tab === "left" ? x - 90 : x + 80} y={y - 48} width={10} height={96} className={TAB[name]} />
      <text x={x - 72} y={y - 18} className="fill-ink text-[17px] font-semibold">
        {name}
      </text>
      <text x={x + 76} y={y - 6} textAnchor="end" className="fill-ink font-mono text-[30px] font-semibold">
        {stat ? stat.messages : "—"}
      </text>
      <text x={x - 72} y={y + 14} className="fill-muted font-mono text-[11px]">
        {!stat ? "" : stat.consumers ? `${stat.unacked} unacked · ${stat.consumers} cons.` : name === "dlq" ? "clique para inspecionar" : "sem consumidor"}
      </text>
      <AnimatePresence>
        {Array.from({ length: shown }, (_, i) => (
          <motion.rect
            key={i}
            x={x - 72 + i * 12}
            y={y + 23}
            width={9}
            height={9}
            className={TAB[name]}
            style={{ transformBox: "fill-box", transformOrigin: "center" }}
            initial={reduce ? false : { scale: 0 }}
            animate={{ scale: 1 }}
            exit={reduce ? undefined : { scale: 0, opacity: 0 }}
            transition={{ type: "spring", stiffness: 500, damping: 30 }}
          />
        ))}
      </AnimatePresence>
      {ready > SQUARES && (
        <text x={x - 72 + SQUARES * 12 + 2} y={y + 32} className="fill-muted font-mono text-[11px]">
          +{ready - SQUARES}
        </text>
      )}
    </g>
  );
}

function Worker({ count, flash }: { count: number; flash: Partial<Record<keyof typeof LANES, number>> }) {
  const left = WORKER.x - 110;
  const top = WORKER.y - 150;
  if (count === 0)
    return (
      <g>
        <rect x={left} y={top} width={220} height={300} rx={12} className="fill-paper stroke-muted" strokeWidth={3} strokeDasharray="10 8" />
        <text x={WORKER.x} y={WORKER.y + 5} textAnchor="middle" className="fill-muted font-mono text-[14px]">
          nenhum consumidor
        </text>
      </g>
    );
  // Back cards first; card k sits 10 px up-right of card k−1.
  const cards = Array.from({ length: count }, (_, k) => count - 1 - k);
  return (
    <g>
      <AnimatePresence>
        {cards.map((k) => (
          <motion.g
            key={k}
            initial={{ x: k * 10 + 24, y: -k * 10, opacity: 0 }}
            animate={{ x: k * 10, y: -k * 10, opacity: 1 }}
            exit={{ x: k * 10 + 24, opacity: 0 }}
            transition={{ duration: 0.35 }}
          >
            <rect x={left} y={top} width={220} height={300} rx={12} className="fill-white stroke-ink" strokeWidth={3} />
            {k === 0 && (
              <>
                <text x={left + 16} y={top + 30} className="fill-ink text-[20px] font-extrabold">
                  worker
                </text>
                <text x={left + 204} y={top + 30} textAnchor="end" className="fill-muted font-mono text-[11px]">
                  {count} {count === 1 ? "instância" : "instâncias"}
                </text>
                {(Object.entries(LANES) as [keyof typeof LANES, number][]).map(([lane, ly]) => (
                  <g key={lane}>
                    <rect x={left + 12} y={ly - 32} width={196} height={64} rx={6} className="fill-paper stroke-ink" strokeWidth={1.5} />
                    {flash[lane] !== undefined && (
                      <motion.rect
                        key={flash[lane]}
                        x={left + 12}
                        y={ly - 32}
                        width={196}
                        height={64}
                        rx={6}
                        className={FILL[lane]}
                        initial={{ opacity: 0.35 }}
                        animate={{ opacity: 0 }}
                        transition={{ duration: 0.4 * PACE }}
                      />
                    )}
                    <text x={left + 26} y={ly + 5} className="fill-ink font-mono text-[14px]">
                      {lane}.Handler
                    </text>
                  </g>
                ))}
              </>
            )}
          </motion.g>
        ))}
      </AnimatePresence>
    </g>
  );
}

function Token({ tok, onDone }: { tok: Tok; onDone: (t: Tok) => void }) {
  const p = useMotionValue(0);
  const cx = useTransform(p, (t) => AT[tok.edge](t).x);
  const cy = useTransform(p, (t) => AT[tok.edge](t).y);
  const opacity = useTransform(p, [0, 0.04, 0.96, 1], [0, 1, 1, 0]);
  useEffect(() => {
    const c = animate(p, 1, { duration: 0.6 * PACE, ease: "easeInOut", delay: tok.delay });
    c.then(() => onDone(tok));
    return () => c.stop();
  }, [p, tok, onDone]);
  return <motion.circle r={8} cx={cx} cy={cy} style={{ opacity }} className={`${FILL[tok.src]} stroke-ink`} strokeWidth={2.5} />;
}

const STROKE_SRC: Record<Src, string> = {
  api: "stroke-src-api",
  stock: "stroke-src-stock",
  payment: "stroke-src-payment",
  notification: "stroke-src-notification",
  none: "stroke-src-none",
};

const LEGEND_LINES: [string, keyof typeof DASH][] = [
  ["orders", "orders"],
  ["retry", "retry"],
  ["dlx", "dlx"],
  ["expiry", "expiry"],
];

// Bottom-right, under the dlq: what a token's color and a line's pattern mean.
function Legend() {
  const x = 1290;
  const y = 728;
  return (
    <g aria-hidden>
      <rect x={x} y={y} width={300} height={130} rx={10} className="fill-white stroke-ink" strokeWidth={2} />
      <text x={x + 16} y={y + 24} className="fill-ink text-[13px] font-semibold">
        cor = fila de origem
      </text>
      {SRC_NAME.map(([src, label], i) => (
        <g key={src}>
          <circle cx={x + 24} cy={y + 42 + i * 18} r={6} className={`${FILL[src]} stroke-ink`} strokeWidth={2} />
          <text x={x + 38} y={y + 46 + i * 18} className="fill-ink font-mono text-[11px]">
            {label}
          </text>
        </g>
      ))}
      <text x={x + 170} y={y + 24} className="fill-ink text-[13px] font-semibold">
        traço
      </text>
      {LEGEND_LINES.map(([label, line], i) => (
        <g key={line}>
          <path d={`M${x + 170} ${y + 42 + i * 18} h56`} className="stroke-ink" strokeWidth={3} strokeDasharray={DASH[line]} />
          <text x={x + 234} y={y + 46 + i * 18} className="fill-ink font-mono text-[11px]">
            {label}
          </text>
        </g>
      ))}
    </g>
  );
}
