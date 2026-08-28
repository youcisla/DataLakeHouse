"use client";
import { useEffect, useState } from "react";

/** Ordre fixe des villes et leurs couleurs (validées daltonisme). */
export const CITY_ORDER = ["Paris", "Lyon", "Marseille", "Bordeaux", "Lille"] as const;
export const CITY_COLORS: Record<string, string> = {
  Paris: "#3987e5",
  Lyon: "#eb6834",
  Marseille: "#1baf7a",
  Bordeaux: "#eda100",
  Lille: "#e87ba4",
};

export function cityColor(city: string): string {
  return CITY_COLORS[city] ?? "#8d99ae";
}

/** Rampes séquentielles (viridis 7 classes, claire -> foncée). */
export const VIRIDIS = ["#440154", "#3b528b", "#21918c", "#5ec962", "#a6d75b", "#fde725"];

/** Couleurs de chart selon le thème (lues au rendu). */
export function chartTheme(dark: boolean) {
  return dark
    ? { axis: "#7a7f8a", grid: "#26282e", text: "#e7eaf0", muted: "#8f98a6", tooltip: "#17181c", border: "#2a2d34" }
    : { axis: "#a2a7b0", grid: "#e6e7eb", text: "#1a1d24", muted: "#5b616c", tooltip: "#ffffff", border: "#d8dbe0" };
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
