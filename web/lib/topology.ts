import type { EdgeId, QueueName } from "./flow";

// Fixed layout in a 1600×900 viewBox. Stations are 180×96 boxes centered on their point, exchanges are
// r=26 interchanges, the worker card is 220×300 centered on WORKER with lanes at LANES.
export type Line = "orders" | "retry" | "dlx" | "expiry" | "http";

export const EXCHANGES: Record<"orders" | "retry" | "dlx" | "expiry", { x: number; y: number; line: Line; label: "below" | "right" }> = {
  orders: { x: 520, y: 330, line: "orders", label: "right" },
  retry: { x: 860, y: 690, line: "retry", label: "below" },
  dlx: { x: 1190, y: 690, line: "dlx", label: "below" },
  expiry: { x: 860, y: 830, line: "expiry", label: "below" },
};

// tab: the side the line comes in from, drawn in the line's color.
export const STATIONS: Record<QueueName, { x: number; y: number; line: Line; tab: "left" | "right" }> = {
  stock: { x: 860, y: 160, line: "orders", tab: "left" },
  payment: { x: 860, y: 330, line: "orders", tab: "left" },
  notification: { x: 860, y: 500, line: "orders", tab: "left" },
  "retry.q": { x: 460, y: 690, line: "retry", tab: "right" },
  "expiry.q": { x: 460, y: 830, line: "expiry", tab: "right" },
  dlq: { x: 1420, y: 690, line: "dlx", tab: "left" },
};

export const CLIENT = { x: 70, y: 330 };
export const API = { x: 250, y: 330 };
export const WORKER = { x: 1190, y: 330 };
export const LANES = { stock: 260, payment: 350, notification: 440 } as const;
export const POSTGRES = { x: 1470, y: 330 };

export const EDGES: Record<EdgeId, { d: string; line: Line; label?: [text: string, x: number, y: number, anchor?: "start" | "middle"] }> = {
  "client-api": { d: "M120 330 L190 330", line: "http", label: ["HTTP", 155, 314] },
  "api-orders": { d: "M310 330 L494 330", line: "orders", label: ["order.placed", 402, 316] },
  "orders-stock": { d: "M546 330 L600 330 L700 160 L770 160", line: "orders", label: ["order.placed · payment.declined · reservation.expired", 700, 108] },
  "orders-payment": { d: "M546 330 L770 330", line: "orders", label: ["reservation.created", 622, 316, "start"] },
  "orders-notification": { d: "M546 330 L600 330 L700 500 L770 500", line: "orders", label: ["# (todos)", 700, 540] },
  "stock-worker": { d: "M950 160 L1010 160 L1060 260 L1080 260", line: "orders" },
  "payment-worker": { d: "M950 330 L1010 330 L1030 350 L1080 350", line: "orders" },
  "notification-worker": { d: "M950 500 L1010 500 L1040 440 L1080 440", line: "orders" },
  "worker-orders": { d: "M1190 180 L1190 60 L520 60 L520 304", line: "orders", label: ["reservation.* · payment.*", 855, 46] },
  "bus-retry": { d: "M860 548 L860 690 L550 690", line: "retry", label: ["nack", 874, 616, "start"] },
  "retryq-orders": { d: "M460 642 L460 410 L507 353", line: "retry", label: ["TTL 10 s", 474, 560, "start"] },
  "bus-dlq": { d: "M1190 480 L1190 690 L1330 690", line: "dlx", label: ["3ª falha", 1204, 600, "start"] },
  "worker-expiry": { d: "M1110 480 L1110 830 L550 830", line: "expiry", label: ["reservation.expired", 990, 818] },
  "expiryq-orders": { d: "M370 830 L320 830 L320 400 L497 340", line: "expiry", label: ["TTL 120 s", 334, 620, "start"] },
};

// Point at fraction t (0..1) of a path made only of "M x y L x y …" segments, by length.
export function along(d: string): (t: number) => { x: number; y: number } {
  const n = d.match(/-?\d+(\.\d+)?/g)!.map(Number);
  const pts = Array.from({ length: n.length / 2 }, (_, i) => ({ x: n[2 * i], y: n[2 * i + 1] }));
  const seg = pts.slice(1).map((p, i) => Math.hypot(p.x - pts[i].x, p.y - pts[i].y));
  const total = seg.reduce((a, b) => a + b, 0);
  return (t) => {
    let left = Math.min(Math.max(t, 0), 1) * total;
    for (let i = 0; i < seg.length; i++) {
      if (left <= seg[i] || i === seg.length - 1) {
        const f = seg[i] ? Math.min(left / seg[i], 1) : 0;
        return { x: pts[i].x + (pts[i + 1].x - pts[i].x) * f, y: pts[i].y + (pts[i + 1].y - pts[i].y) * f };
      }
      left -= seg[i];
    }
    return pts[0];
  };
}
