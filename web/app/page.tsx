"use client";

import FlowMap from "@/components/FlowMap";
import { useLive } from "@/lib/live";

export default function Page() {
  const { snap, hops, products, online } = useLive();
  return (
    <main className="grid h-screen grid-cols-[360px_1fr]">
      <aside className="border-r-2 border-ink/10" />
      <FlowMap snap={snap} hops={hops} products={products} online={online} />
    </main>
  );
}
