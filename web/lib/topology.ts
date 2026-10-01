import type { EdgeId, QueueName } from "./flow";

// Fixed layout in a 1600×900 viewBox, in bands: main flow at y=360, the worker's return rail on top (y=70),
// failures at y=640 (retry, dlx), the reservation timer at y=800 (expiry). Return rails into orders run parallel
// on the left (retry x=410, expiry x=370) and never cross. Stations are 180×96 boxes centered on their point,
// exchanges r=26 rings, the worker card 220×300 centered on WORKER with lanes at LANES; it leaves from its base
// at three separate points (retry x=1070, expiry x=1110, dlx x=1210).
export type Line = "orders" | "retry" | "dlx" | "expiry" | "http";

// Lines are neutral (color is reserved for the message's source queue); each exchange keeps its own stroke pattern.
export const DASH: Record<Line, string | undefined> = {
  orders: undefined,
  retry: "14 8",
  dlx: "2 9",
  expiry: "20 6 2 6",
  http: "8 8",
};

// label: where the name goes, clear of every line ("left" = above-left, "right" = below-right).
export const EXCHANGES: Record<"orders" | "retry" | "dlx" | "expiry", { x: number; y: number; line: Line; label?: "left" }> = {
  orders: { x: 470, y: 360, line: "orders", label: "left" },
  retry: { x: 760, y: 640, line: "retry" },
  dlx: { x: 1210, y: 640, line: "dlx" },
  expiry: { x: 760, y: 800, line: "expiry" },
};

// tab: the side the line comes in from; business queues wear their own color, dlq red, the timers neutral.
export const STATIONS: Record<QueueName, { x: number; y: number; line: Line; tab: "left" | "right" }> = {
  stock: { x: 840, y: 200, line: "orders", tab: "left" },
  payment: { x: 840, y: 360, line: "orders", tab: "left" },
  notification: { x: 840, y: 520, line: "orders", tab: "left" },
  "retry.q": { x: 540, y: 640, line: "retry", tab: "right" },
  "expiry.q": { x: 540, y: 800, line: "expiry", tab: "right" },
  dlq: { x: 1410, y: 640, line: "dlx", tab: "left" },
};

export const CLIENT = { x: 70, y: 360 };
export const API = { x: 230, y: 360 };
export const WORKER = { x: 1150, y: 360 };
export const LANES = { stock: 290, payment: 370, notification: 450 } as const;
export const POSTGRES = { x: 1460, y: 360 };

type Label = [text: string, x: number, y: number, anchor?: "start" | "middle" | "end", title?: string];

export const EDGES: Record<EdgeId, { d: string; line: Line; label?: Label }> = {
  "client-api": { d: "M120 360 L170 360", line: "http", label: ["HTTP", 145, 344] },
  "api-orders": { d: "M290 360 L444 360", line: "orders", label: ["order.placed", 367, 344] },
  "orders-stock": {
    d: "M496 360 L540 360 L620 200 L750 200",
    line: "orders",
    label: ["order.placed …", 744, 186, "end", "order.placed · payment.declined · reservation.expired"],
  },
  "orders-payment": { d: "M496 360 L750 360", line: "orders", label: ["reservation.created", 744, 346, "end"] },
  "orders-notification": { d: "M496 360 L540 360 L620 520 L750 520", line: "orders", label: ["# (todos)", 744, 506, "end"] },
  "stock-worker": { d: "M930 200 L960 200 L1010 290 L1040 290", line: "orders" },
  "payment-worker": { d: "M930 360 L1010 360 L1020 370 L1040 370", line: "orders" },
  "notification-worker": { d: "M930 520 L960 520 L1010 450 L1040 450", line: "orders" },
  "worker-orders": { d: "M1150 210 L1150 70 L470 70 L470 334", line: "orders", label: ["reservation.* · payment.*", 810, 56] },
  "bus-retry": { d: "M1070 510 L1070 640 L630 640", line: "retry", label: ["nack", 1060, 600, "end"] },
  "retryq-orders": { d: "M450 640 L410 640 L410 420 L470 420 L470 386", line: "retry", label: ["TTL 10 s", 420, 540, "start"] },
  "bus-dlq": { d: "M1210 510 L1210 640 L1320 640", line: "dlx", label: ["3ª falha", 1222, 580, "start"] },
  "worker-expiry": { d: "M1110 510 L1110 800 L630 800", line: "expiry", label: ["reservation.expired", 940, 786] },
  "expiryq-orders": { d: "M450 800 L370 800 L370 400 L452 378", line: "expiry", label: ["TTL 120 s", 360, 620, "end"] },
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
