"use client";

import { Fragment, useMemo, useState } from "react";
import { ThemeToggle, Status, Empty } from "@/components/Primitives";
import { WeatherIcon, eventIcon } from "@/components/Icons";
import { StationTimeHeatmap, TrendLine, StationLine, Donut, Sparkline } from "@/components/Charts";
import FranceMap from "@/components/FranceMap";
import { RAMP_TEMP, RAMP_PRECIP, RAMP_ERROR, tempColor } from "@/lib/palette";
import {
  climate, daily, extremes, formatDate, formatNumber, hasData, mapPoints,
  meta, predictions, trend, bulletin, uniqueStations,
} from "@/lib/data";

type Metric = "temp_avg" | "temp_min" | "temp_max";
const METRICS: { id: Metric; label: string }[] = [
  { id: "temp_avg", label: "Moyenne" },
  { id: "temp_min", label: "Min" },
  { id: "temp_max", label: "Max" },
];
const PERIODS: { label: string; days: number | null }[] = [
  { label: "14 j", days: 14 },
  { label: "30 j", days: 30 },
  { label: "90 j", days: 90 },
  { label: "Tout", days: null },
];
const MONTHS = ["jan", "fév", "mar", "avr", "mai", "juin", "juil", "aoû", "sep", "oct", "nov", "déc"];

/** Matrice heatmap [x, y, valeur] a partir de lignes station x pas de temps. */
function heatMatrix(
  rows: { dt: string; city: string; [k: string]: unknown }[],
  stations: string[], steps: string[], field: string,
): [number, number, number][] {
  const si = new Map(stations.map((s, i) => [s, i] as const));
  const di = new Map(steps.map((s, i) => [s, i] as const));
  const out: [number, number, number][] = [];
  for (const r of rows) {
    const y = si.get(r.city);
    const x = di.get(r.dt);
    const v = r[field];
    if (y !== undefined && x !== undefined && typeof v === "number") out.push([x, y, v]);
  }
  return out;
}

/** Serie d'une station : [label, valeur] sur la fenetre. */
function stationSeries(
  rows: { dt: string; city: string; [k: string]: unknown }[],
  station: string, field: string,
): { label: string; value: number | null }[] {
  return rows
    .filter((r) => r.city === station)
    .sort((a, b) => a.dt.localeCompare(b.dt))
    .map((r) => ({ label: String(r.dt).slice(5), value: (r[field] as number) ?? null }));
}

/** Moyenne nationale par jour (pour les sparklines KPI). */
function nationalDaily(rows: { dt: string; [k: string]: unknown }[], field: string): (number | null)[] {
  const byDate = new Map<string, number[]>();
  for (const r of rows) {
    const v = r[field];
    if (typeof v !== "number") continue;
    const arr = byDate.get(r.dt);
    if (arr) arr.push(v); else byDate.set(r.dt, [v]);
  }
  return [...byDate.keys()].sort().map((d) => {
    const vals = byDate.get(d)!;
    return vals.reduce((a, b) => a + b, 0) / vals.length;
  });
}

export default function Page() {
  const allStations = useMemo(() => uniqueStations(daily), []);
  const [activeStation, setActiveStation] = useState<string | null>(null);
  const [stationQuery, setStationQuery] = useState("");
  const [metric, setMetric] = useState<Metric>("temp_avg");
  const [period, setPeriod] = useState<number | null>(30);

  // Fenetre temporelle sur le recent (daily / predictions).
  const filteredDaily = useMemo(() => {
    if (!period) return daily;
    const latest = Math.max(...daily.map((r) => +new Date(r.dt)));
    const cut = new Date(latest);
    cut.setDate(cut.getDate() - period);
    const iso = cut.toISOString().slice(0, 10);
    return daily.filter((r) => r.dt >= iso);
  }, [period]);

  const days = useMemo(
    () => [...new Set(filteredDaily.map((r) => r.dt))].sort(),
    [filteredDaily],
  );

  const tempHeat = useMemo(
    () => heatMatrix(filteredDaily, allStations, days, metric),
    [filteredDaily, allStations, days, metric],
  );
  const rainHeat = useMemo(
    () => heatMatrix(filteredDaily, allStations, days, "precip_sum"),
    [filteredDaily, allStations, days],
  );

  // ML : residus (|erreur|) station x jour, sur la fenetre.
  const predFiltered = useMemo(() => {
    if (!period) return predictions;
    const latest = Math.max(...predictions.map((r) => +new Date(r.dt)));
    const cut = new Date(latest);
    cut.setDate(cut.getDate() - period);
    const iso = cut.toISOString().slice(0, 10);
    return predictions.filter((r) => r.dt >= iso);
  }, [period]);
  const predDays = useMemo(
    () => [...new Set(predFiltered.map((r) => r.dt))].sort(),
    [predFiltered],
  );
  const mlHeat = useMemo(
    () => heatMatrix(predFiltered, allStations, predDays, "error_abs"),
    [predFiltered, allStations, predDays],
  );
  const mae = useMemo(() => {
    const errs = predictions.map((p) => p.error_abs).filter((e): e is number => e !== null);
    return errs.length ? errs.reduce((a, b) => a + b, 0) / errs.length : null;
  }, []);

  // Climat : station x mois (208 stations, historique 2000-2026).
  const climateStations = useMemo(() => uniqueStations(climate), []);
  const climateHeat = useMemo(() => {
    const si = new Map(climateStations.map((s, i) => [s, i] as const));
    const out: [number, number, number][] = [];
    for (const r of climate) {
      const y = si.get(r.city);
      if (y !== undefined && r.temp_normal !== null) out.push([r.month - 1, y, r.temp_normal]);
    }
    return out;
  }, [climateStations]);

  // Tendance nationale 2000-2026 (deja agregee par annee a l'export).
  const trendData = trend;

  const years = useMemo(() => {
    const ys = trend.map((t) => t.year).filter((y) => Number.isFinite(y));
    return ys.length ? [Math.min(...ys), Math.max(...ys)] : [null, null];
  }, []);

  // KPI sparklines (moyenne nationale, derniers 30 jours).
  const tempSpark = useMemo(() => nationalDaily(daily, "temp_avg").slice(-30), []);
  const rainSpark = useMemo(() => nationalDaily(daily, "precip_sum").slice(-30), []);

  // Evenements extremes (donut + liste).
  const eventCounts = new Map<string, number>();
  for (const e of extremes) eventCounts.set(e.event_type, (eventCounts.get(e.event_type) ?? 0) + 1);
  const donutData = [...eventCounts.entries()].map(([name, value]) => ({
    name: name.replace(/_/g, " "), value,
    color: ({ canicule: "#ef4444", fortes_pluies: "#3b82f6", vents_violents: "#8b5cf6", vague_de_froid: "#22d3ee" } as Record<string, string>)[name],
  }));

  const totalRows = Object.values(meta.rows ?? {}).reduce((a, b) => a + (Number(b) || 0), 0);
  const silverStations = climateStations.length || meta.summary.cities;

  const archSteps = [
    { badge: "BZ", name: "Bronze", note: "archives Météo-France + flux Open-Meteo" },
    { badge: "SI", name: "Silver", note: silverStations + " stations · " + (years[0] ?? "–") + "-" + (years[1] ?? "–") },
    { badge: "GO", name: "Gold", note: totalRows.toLocaleString("fr-FR") + " lignes agrégées" },
    { badge: "ML", name: "Machine Learning", note: "XGBoost · " + predictions.length.toLocaleString("fr-FR") + " prédictions J+1" },
  ];

  const filteredMapPoints = mapPoints.filter((p) =>
    p.city.toLowerCase().includes(stationQuery.toLowerCase()));

  function selectStation(city: string | null) {
    if (city === null) { setActiveStation(null); return; }
    setActiveStation((cur) => (cur === city ? null : city));
  }

  return (
    <main className="wrap">
      <nav className="nav">
        <div className="brand">DataLake <b>Météo</b></div>
        <div className="navlinks">
          {["carte", "temp", "pluie", "climat", "tendance", "ml", "extrêmes"].map((s) => (
            <a key={s} href={"#" + s}>{s}</a>
          ))}
        </div>
        <div className="navright">
          <span className="chip">{meta.generated_at ? "Snap " + formatDate(meta.generated_at) : "Aucun export"}</span>
          <ThemeToggle />
        </div>
      </nav>

      <header className="hero">
        <h1>Le climat de la France, mesuré station par station.</h1>
        <p className="sub">Données Météo-France 2000-2026, de l&apos;archive brute aux prédictions ML, en un tableau de bord.</p>
        <div className="archstrip" aria-label="Architecture du pipeline Bronze vers Silver vers Gold vers ML">
          {archSteps.map((s, i) => (
            <Fragment key={s.name}>
              {i > 0 ? <span className="archarrow" aria-hidden="true">→</span> : null}
              <div className="archstep">
                <span className="archbadge">{s.badge}</span>
                <div>
                  <div className="archname">{s.name}</div>
                  <div className="archnote">{s.note}</div>
                </div>
              </div>
            </Fragment>
          ))}
        </div>
      </header>

      <section id="carte">
        <h2>Carte météo du réseau</h2>
        <p className="sectionlead">Dernière température relevée sur les {mapPoints.length} stations actives.</p>
        <div className="duo mapduo">
          <div className="card">
            <FranceMap points={mapPoints} selected={activeStation} onSelect={selectStation} />
          </div>
          <div className="card">
            <input className="search" type="search" placeholder="Rechercher une station…"
              value={stationQuery} onChange={(e) => setStationQuery(e.target.value)}
              aria-label="Rechercher une station" />
            <div className="maplist scroll">
              {filteredMapPoints.length ? filteredMapPoints.map((p) => (
                <button key={p.city} type="button"
                  className={"row" + (activeStation === p.city ? " on" : "")}
                  onClick={() => selectStation(p.city)} aria-pressed={activeStation === p.city}>
                  <span className="swatch" style={{ background: tempColor(p.temperature) }} />
                  <span className="cname">{p.city}</span>
                  <span className="cval">{formatNumber(p.temperature)} °C</span>
                </button>
              )) : <div className="empty">Aucune station trouvée.</div>}
            </div>
            {activeStation ? (
              <button type="button" className="chipbtn ghost" onClick={() => setActiveStation(null)}>
                Réinitialiser la sélection
              </button>
            ) : null}
          </div>
        </div>
      </section>

      <section className="kpis" aria-label="Indicateurs clés">
        <div className="card kpi">
          <div className="klabel">Température moyenne</div>
          <div className="kvalue">{formatNumber(meta.summary.temp_avg)}<span className="kunit"> °C</span></div>
          <Sparkline data={tempSpark} color="#eb6834" />
          <div className="knote">dernier jour, {meta.summary.cities} stations</div>
        </div>
        <div className="card kpi">
          <div className="klabel">Cumul pluie</div>
          <div className="kvalue">{formatNumber(meta.summary.precip_total)}<span className="kunit"> mm</span></div>
          <Sparkline data={rainSpark} color="#3987e5" />
          <div className="knote">dernier jour, réseau entier</div>
        </div>
        <div className="card kpi">
          <div className="klabel">Stations actives</div>
          <div className="kvalue">{meta.summary.cities}</div>
          <div className="knote">{silverStations} sur {years[0]}-{years[1]}</div>
        </div>
        <div className="card kpi">
          <div className="klabel">Observations</div>
          <div className="kvalue">{(meta.summary.observations ?? 0).toLocaleString("fr-FR")}</div>
          <div className="knote">relevés agrégés en Gold</div>
        </div>
        <div className="card kpi">
          <div className="klabel">Événements extrêmes</div>
          <div className="kvalue">{extremes.length}</div>
          <div className="knote">canicule, pluies, vents, froid</div>
        </div>
      </section>

      <section className="controls" aria-label="Filtres">
        <div className="controlgroup">
          {PERIODS.map((p) => (
            <button key={p.label} className={"chipbtn" + (period === p.days ? " on" : "")}
              onClick={() => setPeriod(p.days)} aria-pressed={period === p.days}>{p.label}</button>
          ))}
        </div>
        <label className="picker">
          <span>Station</span>
          <select value={activeStation ?? ""} onChange={(e) => setActiveStation(e.target.value || null)}>
            <option value="">Toutes les stations</option>
            {allStations.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </label>
      </section>

      {!hasData ? <section><Empty what="agrégats quotidiens" /></section> : null}

      <section id="temp">
        <h2>Températures</h2>
        <p className="sectionlead">
          {activeStation
            ? "Évolution de la station « " + activeStation + " » sur la fenêtre."
            : "Une ligne par station, une colonne par jour : la chaleur se lit du clair au foncé."}
        </p>
        <div className="seg">
          {METRICS.map((m) => (
            <button key={m.id} className={"segbtn" + (metric === m.id ? " on" : "")}
              onClick={() => setMetric(m.id)} aria-pressed={metric === m.id}>{m.label}</button>
          ))}
        </div>
        <div className="card">
          {activeStation ? (
            <StationLine data={stationSeries(filteredDaily, activeStation, metric)}
              unit=" °C" height={360} />
          ) : tempHeat.length ? (
            <StationTimeHeatmap data={tempHeat} xLabels={days.map((d) => d.slice(5))}
              stations={allStations} unit=" °C" ramp={RAMP_TEMP} height={540} zoomX={days.length > 40} zoomY />
          ) : <Empty what="températures" />}
        </div>
      </section>

      <section id="pluie">
        <h2>Précipitations</h2>
        <p className="sectionlead">
          {activeStation
            ? "Cumuls quotidiens de la station « " + activeStation + " »."
            : "Cumuls quotidiens par station, du sec (clair) à l’arrosé (foncé)."}
        </p>
        <div className="card">
          {activeStation ? (
            <StationLine data={stationSeries(filteredDaily, activeStation, "precip_sum")}
              unit=" mm" height={340} color="#3987e5" />
          ) : rainHeat.length ? (
            <StationTimeHeatmap data={rainHeat} xLabels={days.map((d) => d.slice(5))}
              stations={allStations} unit=" mm" ramp={RAMP_PRECIP} height={540} zoomX={days.length > 40} zoomY />
          ) : <Empty what="précipitations" />}
        </div>
      </section>

      <section id="climat">
        <h2>Profil climatique mensuel</h2>
        <p className="sectionlead">Normale de température par mois pour {climateStations.length} stations, 2000-2026.</p>
        <div className="card">
          {climateHeat.length ? (
            <StationTimeHeatmap data={climateHeat} xLabels={MONTHS}
              stations={climateStations} unit=" °C" ramp={RAMP_TEMP} height={520} zoomX={false} zoomY />
          ) : <Empty what="profil climatique" />}
        </div>
      </section>

      <section id="tendance">
        <h2>Tendance climatique 2000-2026</h2>
        <p className="sectionlead">Température moyenne nationale par année, avec la bande min-max du réseau.</p>
        <div className="card">
          {trendData.length ? <TrendLine data={trendData} height={360} /> : <Empty what="tendance climatique" />}
        </div>
      </section>

      <section id="ml">
        <h2>Prédictions ML (J+1)</h2>
        <p className="sectionlead">
          Erreur absolue du modèle XGBoost par station et par jour
          {mae !== null ? " · erreur moyenne " + mae.toFixed(2) + " °C" : ""}.
        </p>
        <div className="card">
          {mlHeat.length ? (
            <StationTimeHeatmap data={mlHeat} xLabels={predDays.map((d) => d.slice(5))}
              stations={allStations} unit=" °C" ramp={RAMP_ERROR} height={540} zoomX={predDays.length > 40} zoomY />
          ) : <Empty what="prédictions ML" />}
        </div>
      </section>

      <section id="extrêmes">
        <h2>Événements extrêmes</h2>
        <p className="sectionlead">Seuils franchis et sévérité, sur la fenêtre d&apos;export.</p>
        <div className="extgrid">
          {donutData.length ? <div className="card"><Donut data={donutData} height={320} /></div> : null}
          <div className="card">
            {extremes.length ? (
              <div className="evlist">
                {[...extremes].sort((a, b) => String(b.dt).localeCompare(String(a.dt))).slice(0, 10).map((e, i) => (
                  <div key={i} className="ev">
                    <span style={{ color: "var(--accent)", display: "inline-flex", marginTop: 2 }}>
                      <WeatherIcon kind={eventIcon(e.event_type)} size={20} />
                    </span>
                    <Status level={e.severity}>{e.severity === "extreme" ? "Extrême" : "Alerte"}</Status>
                    <div className="evbody">
                      <b>{e.city}</b> · {e.event_type.replace(/_/g, " ")} · {e.dt}
                      <div className="evdetail">{e.detail}</div>
                    </div>
                  </div>
                ))}
              </div>
            ) : <Empty what="événements extrêmes" />}
          </div>
        </div>
      </section>

      {bulletin?.bulletin ? (
        <section id="bulletin">
          <h2>Bulletin météo généré</h2>
          <p className="sectionlead">Synthèse rédigée par le LLM depuis les tables Gold.</p>
          <div className="card bulletin">
            <span className="aitag">Généré par IA</span>
            <div className="bulletintext">{bulletin.bulletin}</div>
          </div>
        </section>
      ) : null}

      <footer>
        <p>Instantané des tables Gold : les données voyagent avec le site, aucune API à l&apos;exécution. Régénérer : <code>make export-web</code>.</p>
        <p className="fmuted">Lignes exportées : {Object.entries(meta.rows ?? {}).map(([k, v]) => k + " " + v).join(" · ")} · {formatDate(meta.generated_at)}</p>
      </footer>
    </main>
  );
}
