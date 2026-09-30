// Proxy to the RabbitMQ Management API so its credentials stay on the server.
// Read-only, except the one POST the DLQ panel needs (a requeueing peek).

const base = process.env.MQ_URL ?? "http://localhost:15672";
const auth = "Basic " + btoa(`${process.env.MQ_USER ?? "worker"}:${process.env.MQ_PASS ?? "worker"}`);
const DLQ_GET = "queues/%2Fshop/dlq/get";

async function forward(req: Request, path: string[]) {
  // Next decodes segments ("%2Fshop" → "/shop"); re-encode so the vhost stays one segment.
  const target = `${base}/api/${path.map(encodeURIComponent).join("/")}${new URL(req.url).search}`;
  try {
    const res = await fetch(target, {
      method: req.method,
      headers: { authorization: auth, "content-type": "application/json" },
      body: req.method === "POST" ? await req.text() : undefined,
      cache: "no-store",
    });
    return new Response(res.body, { status: res.status, headers: { "content-type": "application/json" } });
  } catch {
    return Response.json({ error: "upstream" }, { status: 502 });
  }
}

export async function GET(req: Request, ctx: RouteContext<"/mq/[...path]">) {
  return forward(req, (await ctx.params).path);
}

export async function POST(req: Request, ctx: RouteContext<"/mq/[...path]">) {
  const { path } = await ctx.params;
  if (path.map(encodeURIComponent).join("/") !== DLQ_GET) return Response.json({ error: "method not allowed" }, { status: 405 });
  return forward(req, path);
}
