"use client";
import { useEffect, useState } from "react";

/** Rampes sequentielles claire -> foncee (light to dark, comme demande). */
export const RAMP_TEMP = ["#fde725", "#a6d75b", "#5ec962", "#21918c", "#3b528b", "#440154"];
export const RAMP_PRECIP = ["#f7fbff", "#c6dbef", "#9ecae1", "#6baed6", "#3182bd", "#08519c"];
export const RAMP_ERROR = ["#fff5f0", "#fcbba1", "#fb6a4a", "#cb181d", "#67000d"];

/** Couleur d'une temperature (carte meteorologique, bandes continues). */
export function tempColor(t: number | null): string {
  if (t === null) return "#8d99ae";
  if (t < 0) return "#3b82f6";
  if (t < 8) return "#60a5fa";
  if (t < 14) return "#2dd4bf";
  if (t < 20) return "#4ade80";
  if (t < 26) return "#facc15";
  if (t < 32) return "#fb923c";
  return "#ef4444";
}

/** Couleurs de chart selon le theme (lues au rendu). */
export function chartTheme(dark: boolean) {
  return dark
    ? { axis: "#7a7f8a", grid: "#26282e", text: "#e7eaf0", muted: "#8f98a6", tooltip: "#17181c", border: "#2a2d34", mapFill: "#1c2640", mapLine: "#364564" }
    : { axis: "#a2a7b0", grid: "#e6e7eb", text: "#1a1d24", muted: "#5b616c", tooltip: "#ffffff", border: "#d8dbe0", mapFill: "#e8edf4", mapLine: "#b8c4d4" };
}

/** Hook theme : suit l'attribut data-theme du document. */
export function useTheme(): "light" | "dark" {
  const [theme, setTheme] = useState<"light" | "dark">("light");
  useEffect(() => {
    const read = () => {
      const attr = document.documentElement.getAttribute("data-theme");
      const dark = attr === "dark"
        || (!attr && window.matchMedia("(prefers-color-scheme: dark)").matches);
      setTheme(dark ? "dark" : "light");
    };
    read();
    const obs = new MutationObserver(read);
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
    return () => obs.disconnect();
  }, []);
  return theme;
}
