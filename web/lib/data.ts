/**
 * Chargement des donnees : les JSON produits par `make export-web` sont
 * incorpores AU BUILD (import statique). Le site n'appelle donc aucune API a
 * l'execution, c'est ce qui le rend deployable sur Vercel sans acces au
 * cluster local, et consultable meme cluster eteint.
 */
import dailyRaw from "@/public/data/daily.json";
import weeklyRaw from "@/public/data/weekly.json";
import extremesRaw from "@/public/data/extremes.json";
import climateRaw from "@/public/data/climate.json";
import predictionsRaw from "@/public/data/predictions.json";
import mapRaw from "@/public/data/map.json";
import bulletinRaw from "@/public/data/bulletin.json";
import metaRaw from "@/public/data/meta.json";

export type Daily = {
  dt: string; city: string; source: string; n_obs: number | null;
  temp_avg: number | null; temp_min: number | null; temp_max: number | null;
  precip_sum: number | null; wind_avg: number | null; temp_std: number | null;
};
export type Weekly = {
  year: number; week: number; city: string; temp_avg: number | null;
  trend_slope: number | null; temp_vs_prev_week: number | null; n_days: number | null;
};
export type Extreme = {
  dt: string; city: string; event_type: string; severity: string;
  value: number | null; threshold: number | null; detail: string | null;
};
export type Climate = {
  city: string; month: number; season: string; temp_normal: number | null;
  temp_min_record: number | null; temp_max_record: number | null;
  precip_avg: number | null; rain_days: number | null; rain_day_ratio: number | null;
};
export type Prediction = {
  dt: string; city: string; temp_actual: number | null; temp_predicted: number | null;
  error_abs: number | null; confidence: number | null; model_version: number | null;
};
export type MapPoint = { city: string; lat: number; lon: number; temperature: number | null };
export type Meta = {
  generated_at: string | null; window_days: number; cities: string[];
  rows: Record<string, number>;
  summary: {
    cities: number; observations: number; temp_avg: number | null;
    precip_total: number | null; last_day: string | null;
  };
  source_batch: string; source_stream: string;
};

export const daily = dailyRaw as unknown as Daily[];
export const weekly = weeklyRaw as unknown as Weekly[];
export const extremes = extremesRaw as unknown as Extreme[];
export const climate = climateRaw as unknown as Climate[];
export const predictions = predictionsRaw as unknown as Prediction[];
export const mapPoints = mapRaw as unknown as MapPoint[];
export const bulletin = bulletinRaw as unknown as { bulletin?: string; dt?: string } | null;
export const meta = metaRaw as unknown as Meta;

/** Ordre FIXE des villes : une ville garde sa couleur quels que soient les filtres. */
export const CITY_ORDER = ["Paris", "Lyon", "Marseille", "Bordeaux", "Lille"];

/** Villes reellement presentes, dans l'ordre fixe (les inconnues a la fin). */
export function orderedCities(rows: { city: string }[]): string[] {
  const present = new Set(rows.map((r) => r.city).filter(Boolean));
  const known = CITY_ORDER.filter((c) => present.has(c));
  const extra = [...present].filter((c) => !CITY_ORDER.includes(c)).sort();
  return [...known, ...extra];
}

/** Couleur d'une ville : indexee sur l'ordre FIXE, jamais sur le rang affiche. */
export function cityColor(city: string): string {
  const index = CITY_ORDER.indexOf(city);
  const slot = index >= 0 ? index + 1 : 5;
  return `var(--series-${Math.min(slot, 5)})`;
}

/** Pivote les agregats quotidiens en series par ville, pretes pour Recharts. */
export function pivotByCity(
  rows: { dt: string; city: string }[],
  field: string,
): Record<string, string | number | null>[] {
  const byDate = new Map<string, Record<string, string | number | null>>();
  for (const row of rows) {
    if (!row.dt) continue;
    const entry = byDate.get(row.dt) ?? { dt: row.dt };
    const value = (row as Record<string, unknown>)[field];
    entry[row.city] = typeof value === "number" ? value : null;
    byDate.set(row.dt, entry);
  }
  return [...byDate.values()].sort((a, b) => String(a.dt).localeCompare(String(b.dt)));
}

export function formatNumber(value: number | null | undefined, digits = 1): string {
  return value === null || value === undefined || Number.isNaN(value)
    ? "-"
    : value.toFixed(digits);
}

export function formatDate(value: string | null | undefined): string {
  if (!value) return "-";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? String(value).slice(0, 10)
    : date.toLocaleDateString("fr-FR", { day: "2-digit", month: "short", year: "numeric" });
}

export const hasData = daily.length > 0;
