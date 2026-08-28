import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "DataLake Météo · Bronze · Silver · Gold",
  description:
    "Restitution du datalake météo : agrégats quotidiens, tendances, événements extrêmes, "
    + "profil climatique, prédictions XGBoost et bulletin IA.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="fr">
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link
          href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Space+Grotesk:wght@500;600;700&display=swap"
          rel="stylesheet"
        />
      </head>
      <body>{children}</body>
    </html>
  );
}
