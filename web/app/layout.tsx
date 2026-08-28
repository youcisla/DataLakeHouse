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
      <body>{children}</body>
    </html>
  );
}
