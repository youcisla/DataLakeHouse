"use client";

import {
  CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
  Bar, BarChart, Legend as RLegend,
} from "recharts";

const AXIS = { stroke: "var(--axis)", fontSize: 12, tick: { fill: "var(--text-muted)" } };

function shortDate(value: string) {
  return typeof value === "string" ? value.slice(5) : String(value);
}

/** Infobulle commune : encre de texte, jamais la couleur de serie pour le texte. */
function Tip({ active, payload, label, unit }: any) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: "var(--surface)", border: "1px solid var(--border)",
      borderRadius: 8, padding: "9px 11px", fontSize: 13,
      color: "var(--text-primary)", boxShadow: "0 4px 14px rgba(0,0,0,0.10)",
    }}>
      <div style={{ color: "var(--text-secondary)", marginBottom: 5 }}>{label}</div>
      {payload
        .filter((p: any) => p.value !== null && p.value !== undefined)
        .map((p: any) => (
          <div key={p.name} style={{ display: "flex", alignItems: "center", gap: 7 }}>
            <span className="swatch" style={{ background: p.color }} aria-hidden="true" />
            <span style={{ color: "var(--text-secondary)" }}>{p.name}</span>
            <strong style={{ marginLeft: "auto", fontVariantNumeric: "tabular-nums" }}>
              {Number(p.value).toFixed(1)}{unit}
            </strong>
          </div>
        ))}
    </div>
  );
}

/** Series temporelles multi-villes : 2px, marqueurs masques, croix au survol. */
export function CityLines({ data, cities, colors, unit = " °C", height = 300 }: {
  data: Record<string, any>[]; cities: string[];
  colors: Record<string, string>; unit?: string; height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={data} margin={{ top: 8, right: 16, bottom: 4, left: -8 }}>
        <CartesianGrid stroke="var(--grid)" vertical={false} />
        <XAxis dataKey="dt" tickFormatter={shortDate} {...AXIS} minTickGap={28} />
        <YAxis {...AXIS} width={46} />
        <Tooltip
          content={<Tip unit={unit} />}
          cursor={{ stroke: "var(--axis)", strokeWidth: 1 }}
        />
        {cities.map((city) => (
          <Line
            key={city}
            type="monotone"
            dataKey={city}
            name={city}
            stroke={colors[city]}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--surface)" }}
            connectNulls={false}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

/** Barres : extremites arrondies 4px, ancrees a la ligne de base. */
export function CityBars({ data, cities, colors, unit = " mm", height = 280 }: {
  data: Record<string, any>[]; cities: string[];
  colors: Record<string, string>; unit?: string; height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 8, right: 16, bottom: 4, left: -8 }}>
        <CartesianGrid stroke="var(--grid)" vertical={false} />
        <XAxis dataKey="label" {...AXIS} />
        <YAxis {...AXIS} width={46} />
        <Tooltip content={<Tip unit={unit} />} cursor={{ fill: "var(--grid)", opacity: 0.4 }} />
        {cities.map((city) => (
          <Bar key={city} dataKey={city} name={city} fill={colors[city]} radius={[4, 4, 0, 0]} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}

/** Prevu vs reel : exactement deux series, validees en all-pairs. */
export function PredictionChart({ data, height = 300 }: {
  data: Record<string, any>[]; height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={data} margin={{ top: 8, right: 16, bottom: 4, left: -8 }}>
        <CartesianGrid stroke="var(--grid)" vertical={false} />
        <XAxis dataKey="dt" tickFormatter={shortDate} {...AXIS} minTickGap={28} />
        <YAxis {...AXIS} width={46} />
        <Tooltip content={<Tip unit=" °C" />} cursor={{ stroke: "var(--axis)", strokeWidth: 1 }} />
        <RLegend wrapperStyle={{ fontSize: 13, color: "var(--text-secondary)" }} />
        <Line type="monotone" dataKey="Réel" stroke="var(--series-1)" strokeWidth={2}
          dot={false} activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--surface)" }} connectNulls={false} />
        <Line type="monotone" dataKey="Prédit" stroke="var(--series-2)" strokeWidth={2}
          strokeDasharray="5 3" dot={false}
          activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--surface)" }} connectNulls={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}
