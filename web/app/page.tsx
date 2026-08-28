"use client";

import { useMemo, useState } from "react";
import { ThemeToggle, Status, Empty } from "@/components/Primitives";
import { WeatherIcon, eventIcon, tempIcon } from "@/components/Icons";
import { SeriesChart, GroupedBars, ClimateHeatmap, PredictionScatter, WeeklyDelta, Donut, Sparkline } from "@/components/Charts";
import FranceMap from "@/components/FranceMap";
import { climateInsight, extremesInsight, mlInsight, rainInsight, tempInsight, weeklyInsight } from "@/lib/insights";
import { CITY_ORDER, cityColor } from "@/lib/palette";
import {
  climate, daily, extremes, formatDate, formatNumber, hasData, mapPoints,
  meta, orderedCities, pivotByCity, predictions, weekly, bulletin,
} from "@/lib/data";

type Metric = "temp_avg" | "temp_min" | "temp_max";
const METRICS: { id: Metric; label: string; unit: string }[] = [
  { id: "temp_avg", label: "Moyenne", unit: " °C" },
  { id: "temp_min", label: "Min", unit: " °C" },
  { id: "temp_max", label: "Max", unit: " °C" },
];
const PERIODS: { label: string; days: number | null }[] = [
  { label: "14 j", days: 14 },
  { label: "30 j", days: 30 },
  { label: "90 j", days: 90 },
  { label: "Tout", days: null },
];
const MONTHS = ["jan", "fév", "mar", "avr", "mai", "juin", "juil", "aoû", "sep", "oct", "nov", "déc"];

export default function Page() {
  const allCities = orderedCities(daily);
  const [active, setActive] = useState<string[]>(allCities);
  const [metric, setMetric] = useState<Metric>("temp_avg");
  const [period, setPeriod] = useState<number | null>(30);

  const cities = useMemo(() => CITY_ORDER.filter((c) => active.includes(c)), [active]);

  const filtered = useMemo(() => {
    const inCity = new Set(active);
    let rows = daily.filter((r) => inCity.has(r.city));
    if (period) {
      const cut = new Date(Math.max(...rows.map((r) => +new Date(r.dt))));
      cut.setDate(cut.getDate() - period);
      const iso = cut.toISOString().slice(0, 10);
      rows = rows.filter((r) => r.dt >= iso);
    }
    return rows;
  }, [active, period]);

  const unit = METRICS.find((m) => m.id === metric)!.unit;
  const tempSeries = pivotByCity(filtered, metric).map((r) => ({ ...r, label: String(r.dt).slice(5) }));
  const rainSeries = pivotByCity(filtered, "precip_sum").slice(-14).map((r) => ({ ...r, label: String(r.dt).slice(5) }));

  const cityIndex = new Map<string, number>(cities.map((c, i) => [c, i] as [string, number]));
  const heatData: [number, number, number][] = [];
  for (const row of climate) {
    if (!active.includes(row.city)) continue;
    const ci = cityIndex.get(row.city);
    if (ci !== undefined && row.temp_normal !== null) heatData.push([row.month - 1, ci, row.temp_normal]);
  }

  const predData = predictions
    .filter((p) => active.includes(p.city) && p.temp_actual !== null && p.temp_predicted !== null)
    .map((p) => ({ city: p.city, actual: p.temp_actual as number, predicted: p.temp_predicted as number }));

  const latestWeek = new Map<string, (typeof weekly)[number]>();
  for (const w of weekly) {
    if (!active.includes(w.city)) continue;
    const key = w.year + "-" + w.week;
    const cur = latestWeek.get(key);
    if (!cur) latestWeek.set(key, w);
  }
  const deltaData = [...latestWeek.values()]
    .filter((w) => w.temp_vs_prev_week !== null)
    .map((w) => ({ label: w.city, value: w.temp_vs_prev_week as number }))
    .sort((a, b) => a.value - b.value);

  const eventCounts = new Map<string, number>();
  for (const e of extremes) eventCounts.set(e.event_type, (eventCounts.get(e.event_type) ?? 0) + 1);
  const donutData = [...eventCounts.entries()].map(([name, value]) => ({
    name: name.replace(/_/g, " "), value,
    color: { canicule: "#ef4444", fortes_pluies: "#3b82f6", vents_violents: "#8b5cf6", vague_de_froid: "#22d3ee" }[name],
  }));

  const tempSpark = pivotByCity(daily, "temp_avg").slice(-30).map((r) => {
    const vals = cities.map((c) => r[c]).filter((v): v is number => typeof v === "number");
    return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
  });
  const rainSpark = pivotByCity(daily, "precip_sum").slice(-30).map((r) => {
    const vals = cities.map((c) => r[c]).filter((v): v is number => typeof v === "number");
    return vals.length ? vals.reduce((a, b) => a + b, 0) : null;
  });

  function toggleCity(city: string) {
    setActive((cur) => cur.includes(city) ? cur.filter((c) => c !== city) : [...cur, city]);
  }
  function focusCity(city: string | null) {
    setActive(city ? [city] : allCities);
  }


  return (
    <main className="wrap">
      <nav className="nav">
        <div className="brand">DataLake <b>Météo</b></div>
        <div className="navlinks">
          {["carte", "temp", "climat", "ml", "extrêmes"].map((s) => (
            <a key={s} href={"#" + s}>{s}</a>
          ))}
        </div>
        <div className="navright">
          <span className="chip">{formatDate(meta.generated_at) ? "Snap " + formatDate(meta.generated_at) : "Aucun export"}</span>
          <ThemeToggle />
        </div>
      </nav>

      <header className="hero">
        <h1>Le climat de <em>cinq villes</em> françaises, en un coup d’œil.</h1>
        <p className="sub">{meta.source_batch} · {meta.source_stream}</p>
        <div className="badges">
          <span className="badge bz">Bronze</span>
          <span className="badge si">Silver</span>
          <span className="badge go">Gold</span>
          <span className="badge">XGBoost</span>
        </div>
      </header>

      <section className="nowstrip" aria-label="Météo actuelle par ville">
        {CITY_ORDER.map((city) => {
          const rows = daily.filter((r) => r.city === city).sort((a, b) => a.dt.localeCompare(b.dt));
          const latest = rows[rows.length - 1];
          const spark = rows.slice(-7).map((r) => r.temp_avg);
          return (
            <button type="button" key={city} className="nowcard" onClick={() => focusCity(city)}
              aria-pressed={active.includes(city)}
              style={{ opacity: active.includes(city) ? 1 : 0.4 }}>
              <div className="top" style={{ color: cityColor(city) }}>
                <WeatherIcon kind={tempIcon(latest?.temp_avg ?? null, latest?.precip_sum ?? null)} size={18} />
                <span style={{ color: "var(--text-secondary)" }}>{city}</span>
              </div>
              <div className="ntemp">{formatNumber(latest?.temp_avg)}°</div>
              <div className="nnote">
                {latest?.precip_sum != null ? formatNumber(latest.precip_sum) + " mm de pluie" : "pas de donnée"}
              </div>
              <div className="nspark"><Sparkline data={spark} color={cityColor(city)} height={36} /></div>
            </button>
          );
        })}
      </section>

      {!hasData ? <section><Empty what="agrégats quotidiens" /></section> : null}

      <section className="kpis">
        <div className="card kpi">
          <div className="klabel">Température moyenne</div>
          <div className="kvalue">{formatNumber(meta.summary.temp_avg)}<span className="kunit"> °C</span></div>
          <Sparkline data={tempSpark} color="#eb6834" />
          <div className="knote">dernier jour, 5 villes</div>
        </div>
        <div className="card kpi">
          <div className="klabel">Cumul pluie</div>
          <div className="kvalue">{formatNumber(meta.summary.precip_total)}<span className="kunit"> mm</span></div>
          <Sparkline data={rainSpark} color="#3987e5" />
          <div className="knote">dernier jour</div>
        </div>
        <div className="card kpi">
          <div className="klabel">Villes suivies</div>
          <div className="kvalue">{meta.summary.cities}<span className="kunit"></span></div>
          <div className="knote">batch + flux temps réel</div>
        </div>
        <div className="card kpi">
          <div className="klabel">Observations</div>
          <div className="kvalue">{meta.summary.observations.toLocaleString("fr-FR")}<span className="kunit"></span></div>
          <div className="knote">relevés agrégés en Gold</div>
        </div>
        <div className="card kpi">
          <div className="klabel">Événements extrêmes</div>
          <div className="kvalue">{extremes.length}<span className="kunit"></span></div>
          <div className="knote">canicule, pluies, vents, froid</div>
        </div>
      </section>

      <section className="controls">
        <div className="controlgroup">
          {PERIODS.map((p) => (
            <button key={p.label} className={"chipbtn" + (period === p.days ? " on" : "")} onClick={() => setPeriod(p.days)} aria-pressed={period === p.days}>{p.label}</button>
          ))}
        </div>
        <div className="controlgroup">
          {CITY_ORDER.map((c) => (
            <button key={c} className={"chipbtn dot" + (active.includes(c) ? " on" : "")} onClick={() => toggleCity(c)} aria-pressed={active.includes(c)}>
              <span className="dot" style={{ background: cityColor(c) }} />{c}
            </button>
          ))}
          {active.length !== allCities.length ? (
            <button type="button" className="chipbtn ghost" onClick={() => setActive(allCities)}>Toutes les villes</button>
          ) : null}
        </div>
      </section>

      <section id="carte">
        <h2>Carte de France</h2>
        <blockquote>Où fait-il le plus chaud en ce moment ?</blockquote>
        <div className="duo mapduo">
          <div className="card"><FranceMap points={mapPoints} selected={active.length === 1 ? active[0] : null} onSelect={focusCity} /></div>
          <div className="card">
            <div className="maplist">
              {mapPoints.map((p) => (
                <button key={p.city} className={"row" + (active.length === 1 && active[0] === p.city ? " on" : "")} onClick={() => focusCity(p.city)}>
                  <span className="swatch" style={{ background: cityColor(p.city) }} />
                  <span className="cname">{p.city}</span>
                  <span className="cval">{formatNumber(p.temperature)} °C</span>
                </button>
              ))}
            </div>
            <p className="takeaway">{tempInsight(filtered)}</p>
          </div>
        </div>
      </section>

      <section id="temp">
        <h2>Températures</h2>
        <blockquote>Comment la température évolue-t-elle sur la fenêtre ?</blockquote>
        <div className="seg">
          {METRICS.map((m) => (
            <button key={m.id} className={"segbtn" + (metric === m.id ? " on" : "")} onClick={() => setMetric(m.id)} aria-pressed={metric === m.id}>{m.label}</button>
          ))}
        </div>
        <div className="card">
          {tempSeries.length ? <SeriesChart data={tempSeries} cities={cities} unit={unit} height={440} /> : <Empty what="températures" />}
        </div>
        <p className="takeaway">{tempInsight(filtered)}</p>
      </section>

      <section id="pluie">
        <h2>Précipitations</h2>
        <blockquote>Quels jours ont été les plus arrosés ?</blockquote>
        <div className="card">
          {rainSeries.length ? <GroupedBars data={rainSeries} cities={cities} height={340} /> : <Empty what="précipitations" />}
        </div>
        <p className="takeaway">{rainInsight(filtered)}</p>
      </section>

      <section id="climat">
        <h2>Profil climatique mensuel</h2>
        <blockquote>Quelle est la normale de température de chaque ville, mois par mois ?</blockquote>
        <div className="card">
          {heatData.length ? <ClimateHeatmap data={heatData} months={MONTHS} cities={cities} height={380} /> : <Empty what="profil climatique" />}
        </div>
        <p className="takeaway">{climateInsight(climate)}</p>
      </section>

      <section id="ml">
        <h2>Prédictions ML (J+1)</h2>
        <blockquote>Le modèle XGBoost prédit-il bien la température du lendemain ?</blockquote>
        <div className="card">
          {predData.length ? <PredictionScatter data={predData} height={380} /> : <Empty what="prédictions" />}
        </div>
        <p className="takeaway">{mlInsight(predictions)}</p>
      </section>

      <section id="tendance">
        <h2>Tendances hebdomadaires</h2>
        <blockquote>Quelle ville se réchauffe ou se refroidit le plus d’une semaine à l’autre ?</blockquote>
        <div className="card">
          {deltaData.length ? <WeeklyDelta data={deltaData} height={360} /> : <Empty what="tendances hebdomadaires" />}
        </div>
        <p className="takeaway">{weeklyInsight(weekly)}</p>
      </section>

      <section id="extrêmes">
        <h2>Événements extrêmes</h2>
        <blockquote>Quels seuils ont été franchis, et à quelle sévérité ?</blockquote>
        <div className="extgrid">
          {donutData.length ? (
            <div className="card"><Donut data={donutData} height={320} /></div>
          ) : null}
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
        <p className="takeaway">{extremesInsight(extremes)}</p>
      </section>

      {bulletin?.bulletin ? (
        <section id="bulletin">
          <h2>Bulletin météo généré</h2>
          <blockquote>Une synthèse rédigée, générée depuis les tables Gold.</blockquote>
          <div className="card bulletin">{bulletin.bulletin}</div>
        </section>
      ) : null}

      <footer>
        <p>Instantané des tables Gold : les données voyagent avec le site, aucune API à l’exécution. Régénérer : <code>make export-web</code>.</p>
        <p className="fmuted">Lignes exportées : {Object.entries(meta.rows).map(([k, v]) => k + " " + v).join(" · ")} · {formatDate(meta.generated_at)}</p>
      </footer>
    </main>
  );
}
