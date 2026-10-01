"use client";

import { motion } from "motion/react";
import { useEffect, useRef, useState } from "react";

interface Death { queue: string; reason: string; count: number }
interface Row { type: string; orderId: string; tries: number; reason: string }

// ack_requeue_true: a peek, the messages stay in the dlq.
const PEEK = { count: 10, ackmode: "ack_requeue_true", encoding: "auto" };

function toRow(m: { payload: string; properties: { headers?: { "x-death"?: Death[] } } }): Row {
  let env: { type?: string; order_id?: string } = {};
  try {
    env = JSON.parse(m.payload);
  } catch {} // an undecodable message is exactly what a dlq holds
  const deaths = m.properties.headers?.["x-death"] ?? [];
  const d = deaths.find((x) => x.queue === "payment") ?? deaths[0];
  return { type: env.type ?? "(inválida)", orderId: env.order_id?.slice(0, 8) ?? "—", tries: d?.count ?? 0, reason: d?.reason ?? "—" };
}

export default function DlqPanel({ onClose }: { onClose: () => void }) {
  const [rows, setRows] = useState<Row[]>();
  const [error, setError] = useState("");
  const close = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    close.current?.focus();
    fetch("/mq/queues/%2Fshop/dlq/get", { method: "POST", body: JSON.stringify(PEEK) })
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`${r.status}`))))
      .then((ms) => setRows(ms.map(toRow)))
      .catch((e) => setError(`Falha ao ler a DLQ (${e.message})`));
    const esc = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", esc);
    return () => window.removeEventListener("keydown", esc);
  }, [onClose]);

  return (
    <motion.aside
      role="dialog"
      aria-label="Dead letter queue"
      className="fixed inset-y-0 right-0 z-20 flex w-[420px] flex-col gap-4 border-l-4 border-line-dlx bg-paper p-6 shadow-2xl"
      initial={{ x: "100%" }}
      animate={{ x: 0 }}
      exit={{ x: "100%" }}
      transition={{ duration: 0.25, ease: "easeOut" }}
    >
      <header className="flex items-start justify-between">
        <div>
          <h2 className="text-xl font-extrabold">dlq</h2>
          <p className="font-mono text-xs text-muted">até 10 mensagens · leitura sem remover</p>
        </div>
        <button
          ref={close}
          onClick={onClose}
          className="rounded-md border-2 border-ink px-3 py-1 text-sm font-semibold hover:bg-ink hover:text-paper focus-visible:outline-3 focus-visible:outline-offset-2 focus-visible:outline-ink"
        >
          Fechar
        </button>
      </header>
      {error && <p className="font-mono text-xs text-line-dlx">{error}</p>}
      {!rows && !error && <p className="font-mono text-xs text-muted">Carregando…</p>}
      {rows?.length === 0 && <p className="font-mono text-xs text-muted">DLQ vazia.</p>}
      {rows && rows.length > 0 && (
        <table className="w-full font-mono text-xs">
          <thead className="text-left text-muted">
            <tr>
              <th className="pb-2 font-normal">type</th>
              <th className="pb-2 font-normal">pedido</th>
              <th className="pb-2 text-right font-normal">x-death</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="border-t border-ink/15">
                <td className="py-2">{r.type}</td>
                <td className="py-2">{r.orderId}</td>
                <td className="py-2 text-right" title={r.reason}>
                  {r.tries}× {r.reason}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <p className="mt-auto font-mono text-[11px] text-muted">
        x-death conta as rejeições na fila de origem; a 3ª falha é publicada no dlx pelo worker.
      </p>
    </motion.aside>
  );
}
