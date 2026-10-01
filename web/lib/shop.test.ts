import { test } from "node:test";
import assert from "node:assert/strict";
import { explain, type OrderEvent, type OrderStatus, type Outcome } from "./shop.ts";

const statuses: OrderStatus[] = ["PAID", "DECLINED", "EXPIRED", "OUT_OF_STOCK", "RESERVED"];
const kinds: Pick<OrderEvent, "queue" | "type">[] = [
  { queue: "stock", type: "order.placed" },
  { queue: "stock", type: "payment.declined" },
  { queue: "stock", type: "reservation.expired" },
  { queue: "payment", type: "reservation.created" },
  { queue: "notification", type: "payment.approved" },
];

test("explain covers every outcome × status without holes", () => {
  for (const outcome of ["ack", "retry", "dlq"] as Outcome[])
    for (const k of kinds)
      for (const st of statuses) {
        const s = explain({ at: "", attempt: 2, outcome, error: "simulated gateway error", ...k }, st);
        assert.ok(s && !s.includes("undefined"), `${outcome} ${k.queue} ${k.type} ${st}: ${s}`);
      }
});

test("retry names the attempt and error; expiry tells PAID from EXPIRED", () => {
  const e: OrderEvent = { at: "", queue: "payment", type: "reservation.created", attempt: 2, outcome: "retry", error: "simulated gateway error" };
  assert.match(explain(e, "RESERVED"), /2\/3 .*simulated gateway error/);
  const exp: OrderEvent = { at: "", queue: "stock", type: "reservation.expired", attempt: 1, outcome: "ack" };
  assert.match(explain(exp, "PAID"), /CAS perdeu/);
  assert.match(explain(exp, "EXPIRED"), /reserva liberada/);
});
