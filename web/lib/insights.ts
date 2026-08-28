import type { Daily, Climate, Prediction, Extreme, Weekly } from "./data";

function num(v: number | null | undefined): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

/** Pic de température : jour + ville + valeur. */
export function tempInsight(daily: Daily[]): string {
  let best: Daily | null = null;
  for (const r of daily) {
    const v = num(r.temp_avg);
    if (v !== null && (!best || v > (num(best.temp_avg) ?? -Infinity))) best = r;
  }
  if (!best) return "Aucune mesure de température exploitable.";
  const avg = daily.reduce((s, r) => s + (num(r.temp_avg) ?? 0), 0) /
    daily.filter((r) => num(r.temp_avg) !== null).length;
  return "Pic de " + (best.temp_avg as number).toFixed(1) + " °C à " + best.city +
    " le " + best.dt + ". Moyenne sur la fenêtre : " + avg.toFixed(1) + " °C.";
}

/** Cumul de pluie maximal. */
export function rainInsight(daily: Daily[]): string {
  let best: Daily | null = null;
  for (const r of daily) {
    const v = num(r.precip_sum);
    if (v !== null && (!best || v > (num(best.precip_sum) ?? -Infinity))) best = r;
  }
  if (!best || num(best.precip_sum) === null) return "Aucun cumul de pluie mesuré.";
  return "Jour le plus arrosé : " + best.city + " le " + best.dt +
    " (" + (best.precip_sum as number).toFixed(1) + " mm).";
}

/** Profil climatique : mois le plus chaud / froid. */
export function climateInsight(climate: Climate[]): string {
  if (!climate.length) return "Profil climatique indisponible.";
  const hottest = [...climate].sort((a, b) => (b.temp_normal ?? -99) - (a.temp_normal ?? -99))[0];
  const coldest = [...climate].sort((a, b) => (a.temp_normal ?? 99) - (b.temp_normal ?? 99))[0];
  const mois = ["janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.", "août", "sept.", "oct.", "nov.", "déc."];
  return "Normale la plus chaude : " + hottest.city + " en " + mois[hottest.month - 1] +
    " (" + (hottest.temp_normal as number).toFixed(1) + " °C). La plus froide : " +
    coldest.city + " en " + mois[coldest.month - 1] + ".";
}

/** Qualité du modèle : MAE + confiance moyenne. */
export function mlInsight(predictions: Prediction[]): string {
  const errs = predictions.map((p) => num(p.error_abs)).filter((e): e is number => e !== null);
  const confs = predictions.map((p) => num(p.confidence)).filter((c): c is number => c !== null);
  if (!errs.length) return "Aucune prédiction ML exportée.";
  const mae = errs.reduce((a, b) => a + b, 0) / errs.length;
  const conf = confs.length ? confs.reduce((a, b) => a + b, 0) / confs.length : null;
  return "Erreur absolue moyenne de " + mae.toFixed(2) + " °C" +
    (conf !== null ? ", confiance moyenne " + (conf * 100).toFixed(0) + " %." : ".");
}

/** Événements extrêmes : décompte + plus sévère. */
export function extremesInsight(extremes: Extreme[]): string {
  if (!extremes.length) return "Aucun événement extrême sur la fenêtre.";
  const severe = extremes.filter((e) => e.severity === "extreme").length;
  const total = extremes.length;
  const kinds = [...new Set(extremes.map((e) => e.event_type.replace(/_/g, " ")))].join(", ");
  return total + " alerte" + (total > 1 ? "s" : "") + " (" + severe + " extrême" +
    (severe > 1 ? "s" : "") + ") : " + kinds + ".";
}

/** Tendance hebdo : ville avec la plus forte hausse / baisse. */
export function weeklyInsight(weekly: Weekly[]): string {
  if (!weekly.length) return "Tendances hebdomadaires indisponibles.";
  const latest = new Map<string, Weekly>();
  for (const w of weekly) {
    const key = w.year + "-" + w.week;
    const prev = latest.get(key);
    if (!prev || (w.city < prev.city)) latest.set(key, w);
  }
  const rows = [...latest.values()];
  const up = [...rows].sort((a, b) => (b.temp_vs_prev_week ?? -99) - (a.temp_vs_prev_week ?? -99))[0];
  const down = [...rows].sort((a, b) => (a.temp_vs_prev_week ?? 99) - (b.temp_vs_prev_week ?? 99))[0];
  const d = (v: number | null) => v === null ? "–" : (v > 0 ? "+" : "") + v.toFixed(1);
  return "Plus forte hausse : " + up.city + " (" + d(up.temp_vs_prev_week) + " °C). " +
    "Plus forte baisse : " + down.city + " (" + d(down.temp_vs_prev_week) + " °C).";
}
