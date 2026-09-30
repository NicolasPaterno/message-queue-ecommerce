import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // The shop API has no CORS; proxying keeps the page same-origin.
  rewrites: async () => [{ source: "/shop/:path*", destination: `${process.env.SHOP_URL ?? "http://localhost:8080"}/:path*` }],
};

export default nextConfig;
