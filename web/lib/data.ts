/**
 * Chargement des donnees : les JSON produits par `make export-web` sont
 * incorpores AU BUILD (import statique). Le site n'appelle donc aucune API a
 * l'execution, c'est ce qui le rend deployable sur Vercel sans acces au
 * cluster local, et consultable meme cluster eteint.
 */
import dailyRaw from "@/public/data/daily.json";
import trendRaw from "@/public/data/trend.json";
import extremesRaw from "@/public/data/extremes.json";
import climateRaw from "@/public/data/climate.json";
import predictionsRaw from "@/public/data/predictions.json";
import mapRaw from "@/public/data/map.json";
import stationsRaw from "@/public/data/stations.json";
import bulletinRaw from "@/public/data/bulletin.json";
import metaRaw from "@/public/data/meta.json";

export type Daily = {
  dt: string; city: string; source: string; n_obs: number | null;
  temp_avg: number | null; temp_min: number | null; temp_max: number | null;
  precip_sum: number | null; wind_avg: number | null; temp_std: number | null;
};
export type Trend = { year: number; avg: number; min: number; max: number };
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
export type Station = { city: string; lat: number; lon: number };
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
export const trend = trendRaw as unknown as Trend[];
export const extremes = extremesRaw as unknown as Extreme[];
export const climate = climateRaw as unknown as Climate[];
export const predictions = predictionsRaw as unknown as Prediction[];
export const mapPoints = mapRaw as unknown as MapPoint[];
export const stations = stationsRaw as unknown as Station[];
export const bulletin = bulletinRaw as unknown as { bulletin?: string; dt?: string } | null;
export const meta = metaRaw as unknown as Meta;

/** Noms de stations uniques, tries (ordre francophone), presents dans `rows`. */
export function uniqueStations(rows: { city: string }[]): string[] {
  return [...new Set(rows.map((r) => r.city).filter(Boolean))]
    .sort((a, b) => a.localeCompare(b, "fr"));
}

/** Coordonnees d'une station via le catalogue Silver, ou null. */
export function stationCoord(city: string): { lat: number; lon: number } | null {
  const s = stations.find((x) => x.city === city);
  return s ? { lat: s.lat, lon: s.lon } : null;
}

export function formatNumber(value: number | null | undefined, digits = 1): string {
  return value === null || value === undefined || Number.isNaN(value)
    ? "-"
    : value.toFixed(digits);
}

export function formatDate(value: string | null | undefined): string {
  if (!value) return "-";
  // On formate la partie date (YYYY-MM-DD) en UTC pour rester identique
  // cote serveur et cote navigateur, quel que soit le fuseau horaire.
  const [y, m, d] = String(value).slice(0, 10).split("-").map(Number);
  if (!y || !m || !d) return String(value).slice(0, 10);
  const date = new Date(Date.UTC(y, m - 1, d));
  return date.toLocaleDateString("fr-FR",
    { day: "2-digit", month: "short", year: "numeric", timeZone: "UTC" });
}

export const hasData = daily.length > 0;
