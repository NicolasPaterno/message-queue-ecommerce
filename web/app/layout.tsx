import type { Metadata } from "next";
import { IBM_Plex_Mono, Overpass } from "next/font/google";
import "./globals.css";

const sign = Overpass({ subsets: ["latin"], weight: ["400", "600", "800"], variable: "--font-overpass" });
const mono = IBM_Plex_Mono({ subsets: ["latin"], weight: ["400", "600"], variable: "--font-plex-mono" });

export const metadata: Metadata = {
  title: "Mapa de Mensagens",
  description: "Fluxo ao vivo das mensagens do message-queue-ecommerce",
};

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html lang="pt-BR" className={`${sign.variable} ${mono.variable}`}>
      <body className="min-h-screen bg-paper bg-[linear-gradient(var(--color-grid)_1px,transparent_1px),linear-gradient(90deg,var(--color-grid)_1px,transparent_1px)] bg-size-[24px_24px] font-sign text-ink antialiased">
        {children}
      </body>
    </html>
  );
}
