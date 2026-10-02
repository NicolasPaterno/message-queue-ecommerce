export type OrderStatus = "CART" | "PLACED" | "RESERVED" | "OUT_OF_STOCK" | "PAID" | "DECLINED" | "EXPIRED";

export interface Product { id: string; name: string; price_cents: number; available: number }

async function call(method: string, path: string, body?: unknown) {
  const res = await fetch(`/shop${path}`, { method, body: body === undefined ? undefined : JSON.stringify(body), cache: "no-store" });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${res.status} ${json.error ?? res.statusText}`);
  return json;
}

export const getProducts = (): Promise<Product[]> => call("GET", "/products");

export const restock = (id: string, add: number): Promise<Product> => call("POST", `/products/${id}/stock`, { add });

export type Outcome = "ack" | "retry" | "dlq";

// One delivery the worker handled for an order; at is the receive time (server clock, ISO8601).
export interface OrderEvent { at: string; queue: "stock" | "payment" | "notification"; type: string; attempt: number; outcome: Outcome; error?: string }

export const getOrder = (id: string): Promise<{ status: OrderStatus; events: OrderEvent[] }> => call("GET", `/orders/${id}`);

// Why the event happened, in plain words. First match wins; mirrors the retry/DLQ/expiry rules in internal/mq and stock.
export function explain(e: OrderEvent, status: OrderStatus): string {
  if (e.outcome === "retry") return `tentativa ${e.attempt}/3 falhou: ${e.error} → nack → retry.q (10 s) → orders`;
  if (e.outcome === "dlq") return `${e.attempt}ª falha → worker publica no dlx → dlq${e.queue === "payment" ? "; pedido segue RESERVED até expirar" : ""}`;
  if (e.queue === "notification") return `notificação (${e.type})`;
  if (e.queue === "stock" && e.type === "order.placed")
    return status === "OUT_OF_STOCK" ? "estoque insuficiente → reservation.rejected" : "estoque reservado → reservation.created + reservation.expired agendada (expiry.q, 120 s)";
  if (e.queue === "payment" && e.type === "reservation.created")
    return status === "PAID" ? "pagamento aprovado → payment.approved" : status === "DECLINED" ? "pagamento recusado → payment.declined" : "pagamento processado";
  if (e.queue === "stock" && e.type === "payment.declined") return "reserva liberada → DECLINED";
  if (e.queue === "stock" && e.type === "reservation.expired")
    return status === "EXPIRED" ? "120 s em expiry.q → reserva liberada → EXPIRED" : `120 s em expiry.q; pedido já ${status} — CAS perdeu, nada liberado`;
  return `${e.queue} ${e.outcome}`;
}

// Demo-only payment failure for one order: once (retry recovers) or always (3 attempts → dlq → expiry).
export type Simulate = "payment_once" | "payment_always";

// Cart → item → checkout. Resolves after the broker confirmed order.placed (201); errors read "503 broker unavailable".
export async function placeOrder(productId: string, quantity: number, simulate?: Simulate): Promise<{ id: string }> {
  const { id } = await call("POST", "/carts");
  await call("POST", `/carts/${id}/items`, { product_id: productId, quantity });
  await call("POST", `/carts/${id}/checkout`, simulate && { simulate });
  return { id };
}

export interface TrackedOrder {
  key: number; // local, the order id is unknown until the cart exists
  id?: string;
  quantity: number;
  simulate?: Simulate;
  startedAt: number; // ms, click time
  steps: { status: OrderStatus; at: number }[]; // appended when the polled status changes
  events: OrderEvent[]; // replaced on each poll; [] until the first
  error?: string; // "503 broker unavailable"
}
