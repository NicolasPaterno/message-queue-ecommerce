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

export const getOrder = (id: string): Promise<{ status: OrderStatus }> => call("GET", `/orders/${id}`);

// Cart → item → checkout. Resolves after the broker confirmed order.placed (201); errors read "503 broker unavailable".
export async function placeOrder(productId: string, quantity: number): Promise<{ id: string }> {
  const { id } = await call("POST", "/carts");
  await call("POST", `/carts/${id}/items`, { product_id: productId, quantity });
  await call("POST", `/carts/${id}/checkout`);
  return { id };
}

export const TERMINAL: ReadonlySet<OrderStatus> = new Set(["PAID", "DECLINED", "OUT_OF_STOCK", "EXPIRED"]);

export interface TrackedOrder {
  key: number; // local, the order id is unknown until the cart exists
  id?: string;
  quantity: number;
  startedAt: number; // ms, click time
  steps: { status: OrderStatus; at: number }[]; // appended when the polled status changes
  error?: string; // "503 broker unavailable"
}
