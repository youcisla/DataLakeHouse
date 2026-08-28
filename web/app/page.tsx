import { ChartOrTable, Empty, Legend, Status, ThemeToggle, Tile } from "@/components/Primitives";
import { CityBars, CityLines, PredictionChart } from "@/components/Charts";
import {
  climate, cityColor, daily, extremes, formatDate, formatNumber, hasData,
  mapPoints, meta, orderedCities, pivotByCity, predictions, weekly, bulletin,
} from "@/lib/data";

export default function Page() {
  const cities = orderedCities(daily);
  const colors = Object.fromEntries(cities.map((c) => [c, cityColor(c)]));
  const legendItems = cities.map((c) => ({ label: c, color: cityColor(c) }));

  const tempSeries = pivotByCity(daily, "temp_avg");
  const rainSeries = pivotByCity(daily, "precip_sum").slice(-14).map((row) => ({
    ...row, label: String(row.dt).slice(5),
  }));

  const recentExtremes = [...extremes]
    .sort((a, b) => String(b.dt).localeCompare(String(a.dt)))
    .slice(0, 12);

  const predByDate = new Map<string, Record<string, unknown>>();
  for (const p of predictions) {
    const entry = predByDate.get(p.dt) ?? { dt: p.dt };
    if (typeof p.temp_actual === "number") entry["Réel"] = p.temp_actual;
    if (typeof p.temp_predicted === "number") entry["Prédit"] = p.temp_predicted;
    predByDate.set(p.dt, entry);
  }
  const predSeries = [...predByDate.values()].sort((a, b) =>
    String(a.dt).localeCompare(String(b.dt)));

  const errors = predictions
    .map((p) => p.error_abs)
    .filter((e): e is number => typeof e === "number");
  const mae = errors.length ? errors.reduce((a, b) => a + b, 0) / errors.length : null;

  const climateByCity = new Map<string, typeof climate>();
  for (const row of climate) {
    climateByCity.set(row.city, [...(climateByCity.get(row.city) ?? []), row]);
  }
  const monthlySeries = Array.from({ length: 12 }, (_, i) => {
    const month = i + 1;
    const entry: Record<string, unknown> = {
      label: ["jan", "fév", "mar", "avr", "mai", "juin",
        "juil", "août", "sep", "oct", "nov", "déc"][i],
    };
    for (const city of cities) {
      const row = (climateByCity.get(city) ?? []).find((r) => r.month === month);
      entry[city] = row?.temp_normal ?? null;
    }
    return entry;
  });

  return (
    <main className="wrap">
      <header className="masthead">
        <div>
          <h1>DataLake Météo</h1>
          <p>
            Architecture Bronze → Silver → Gold · {meta.source_batch} + {meta.source_stream}
          </p>
        </div>
        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <span className="chip">
            {meta.generated_at
              ? `Instantané du ${formatDate(meta.generated_at)}`
              : "Aucun export"}
          </span>
          <ThemeToggle />
        </div>
      </header>

      {!hasData ? (
        <section><Empty what="agrégats quotidiens" /></section>
      ) : null}

      {/* ---------------- KPIs ---------------- */}
      <section>
        <h2>Vue d’ensemble</h2>
        <p className="sub">
          Dernier jour couvert : {formatDate(meta.summary.last_day)} · fenêtre exportée{" "}
          {meta.window_days} jours.
        </p>
        <div className="grid kpi">
          <Tile label="Villes suivies" value={String(meta.summary.cities || cities.length)}
            note="Batch Météo-France + flux Open-Meteo" />
          <Tile label="Température moyenne" value={formatNumber(meta.summary.temp_avg)}
            unit=" °C" note="Moyenne des villes, dernier jour" />
          <Tile label="Précipitations" value={formatNumber(meta.summary.precip_total)}
            unit=" mm" note="Cumul du dernier jour" />
          <Tile label="Observations" value={meta.summary.observations.toLocaleString("fr-FR")}
            note="Relevés agrégés en Gold" />
          <Tile label="Événements extrêmes" value={String(extremes.length)}
            note="Canicule, pluies, vents, froid" />
        </div>
      </section>

      {/* ---------------- Températures ---------------- */}
      <section>
        <h2>Températures moyennes</h2>
        <p className="sub">
          Une couleur par ville, fixe : un filtre ne repeint jamais les séries restantes.
        </p>
        <div className="card">
          {tempSeries.length ? (
            <>
              <Legend items={legendItems} />
              <ChartOrTable
                chart={<CityLines data={tempSeries} cities={cities} colors={colors} />}
                table={
                  <table>
                    <thead>
                      <tr>
                        <th>Date</th>
                        {cities.map((c) => <th key={c} className="num">{c} (°C)</th>)}
                      </tr>
                    </thead>
                    <tbody>
                      {tempSeries.slice(-30).reverse().map((row) => (
                        <tr key={String(row.dt)}>
                          <td>{String(row.dt)}</td>
                          {cities.map((c) => (
                            <td key={c} className="num">
                              {formatNumber(row[c] as number | null)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                }
              />
            </>
          ) : <Empty what="températures" />}
        </div>
      </section>

      {/* ---------------- Pluie + carte ---------------- */}
      <section>
        <h2>Précipitations et relevés du jour</h2>
        <p className="sub">Cumuls des 14 derniers jours, et dernière température par ville.</p>
        <div className="grid two">
          <div className="card">
            {rainSeries.length ? (
              <>
                <Legend items={legendItems} />
                <CityBars data={rainSeries} cities={cities} colors={colors} />
              </>
            ) : <Empty what="précipitations" />}
          </div>
          <div className="card">
            {mapPoints.length ? (
              <div className="scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Ville</th><th className="num">Température</th><th>Position</th>
                    </tr>
                  </thead>
                  <tbody>
                    {mapPoints.map((point) => (
                      <tr key={point.city}>
                        <td>
                          <span className="swatch" aria-hidden="true"
                            style={{ background: cityColor(point.city), display: "inline-block",
                              marginRight: 8, verticalAlign: "middle" }} />
                          {point.city}
                        </td>
                        <td className="num">{formatNumber(point.temperature)} °C</td>
                        <td style={{ color: "var(--text-muted)" }}>
                          {point.lat.toFixed(2)}, {point.lon.toFixed(2)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : <Empty what="relevés du jour" />}
          </div>
        </div>
      </section>

      {/* ---------------- Profil climatique ---------------- */}
      <section>
        <h2>Profil climatique mensuel</h2>
        <p className="sub">
          Normales de température par ville : l’équivalent météo d’un profil client,
          construit depuis les archives.
        </p>
        <div className="card">
          {climate.length ? (
            <>
              <Legend items={legendItems} />
              <CityBars data={monthlySeries} cities={cities} colors={colors} unit=" °C" />
            </>
          ) : <Empty what="profil climatique" />}
        </div>
      </section>

      {/* ---------------- Événements extrêmes ---------------- */}
      <section>
        <h2>Événements extrêmes</h2>
        <p className="sub">
          Seuils configurables : canicule, fortes pluies, vents violents, vague de froid.
        </p>
        <div className="card scroll">
          {recentExtremes.length ? (
            <table>
              <thead>
                <tr>
                  <th>Date</th><th>Ville</th><th>Type</th><th>Sévérité</th>
                  <th className="num">Valeur</th><th className="num">Seuil</th>
                </tr>
              </thead>
              <tbody>
                {recentExtremes.map((event, i) => (
                  <tr key={`${event.dt}-${event.city}-${event.event_type}-${i}`}>
                    <td>{event.dt}</td>
                    <td>{event.city}</td>
                    <td>{event.event_type.replace(/_/g, " ")}</td>
                    <td>
                      <Status level={event.severity}>
                        {event.severity === "extreme" ? "Extrême" : "Alerte"}
                      </Status>
                    </td>
                    <td className="num">{formatNumber(event.value)}</td>
                    <td className="num">{formatNumber(event.threshold)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : <Empty what="événements extrêmes" />}
        </div>
      </section>

      {/* ---------------- ML ---------------- */}
      <section>
        <h2>Prédictions à J+1 (XGBoost)</h2>
        <p className="sub">
          Température prédite contre température réellement observée.
          {mae !== null ? ` Erreur absolue moyenne : ${mae.toFixed(2)} °C.` : ""}
        </p>
        <div className="card">
          {predSeries.length ? (
            <PredictionChart data={predSeries} />
          ) : (
            <Empty what="prédictions" />
          )}
        </div>
      </section>

      {/* ---------------- Bulletin IA ---------------- */}
      <section>
        <h2>Bulletin météo généré</h2>
        <p className="sub">Produit par un LLM local (Ollama), avec repli déterministe.</p>
        <div className="card">
          {bulletin?.bulletin ? (
            <p style={{ margin: 0, whiteSpace: "pre-wrap", color: "var(--text-secondary)" }}>
              {bulletin.bulletin}
            </p>
          ) : <Empty what="bulletin" />}
        </div>
      </section>

      {/* ---------------- Tendances hebdo ---------------- */}
      <section>
        <h2>Tendances hebdomadaires</h2>
        <p className="sub">Pente de régression et écart à la semaine précédente.</p>
        <div className="card scroll">
          {weekly.length ? (
            <table>
              <thead>
                <tr>
                  <th>Semaine</th><th>Ville</th><th className="num">T° moy.</th>
                  <th className="num">Pente</th><th className="num">Δ / sem. préc.</th>
                </tr>
              </thead>
              <tbody>
                {[...weekly].sort((a, b) => (b.year - a.year) || (b.week - a.week))
                  .slice(0, 15).map((row, i) => (
                    <tr key={`${row.year}-${row.week}-${row.city}-${i}`}>
                      <td>{row.year} · S{String(row.week).padStart(2, "0")}</td>
                      <td>{row.city}</td>
                      <td className="num">{formatNumber(row.temp_avg)} °C</td>
                      <td className="num">{formatNumber(row.trend_slope, 2)}</td>
                      <td className="num" style={{
                        color: (row.temp_vs_prev_week ?? 0) > 0
                          ? "var(--text-primary)" : "var(--text-secondary)",
                      }}>
                        {row.temp_vs_prev_week !== null && row.temp_vs_prev_week !== undefined
                          ? `${row.temp_vs_prev_week > 0 ? "+" : ""}${formatNumber(row.temp_vs_prev_week)}`
                          : "-"}
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          ) : <Empty what="tendances hebdomadaires" />}
        </div>
      </section>

      <footer>
        <p style={{ margin: 0 }}>
          Instantané des tables Gold : les données voyagent avec le site, aucune API
          n’est appelée à l’exécution. Régénérer : <code>make export-web</code>.
        </p>
        <p style={{ margin: "6px 0 0" }}>
          Lignes exportées :{" "}
          {Object.entries(meta.rows).map(([k, v]) => `${k} ${v}`).join(" · ") || "-"}
        </p>
      </footer>
    </main>
  );
}
