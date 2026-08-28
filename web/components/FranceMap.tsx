"use client";

import { useState } from "react";
import { corsicaPath, francePath, project } from "@/lib/france";

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

export default function FranceMap({ points, selected, onSelect }: {
  points: { city: string; lat: number; lon: number; temperature: number | null }[];
  selected: string | null;
  onSelect: (city: string | null) => void;
}) {
  const W = 420, H = 460;
  const [hover, setHover] = useState<string | null>(null);
  return (
    <svg viewBox={"0 0 " + W + " " + H} role="img" aria-label="Carte de France des températures par ville"
      style={{ width: "100%", height: "auto", display: "block" }}>
      <path d={francePath(W, H)} style={{ fill: "var(--map-fill)", stroke: "var(--map-line)" }} strokeWidth={1.5} />
      <path d={corsicaPath(W, H)} style={{ fill: "var(--map-fill)", stroke: "var(--map-line)" }} strokeWidth={1.5} />
      {points.map((p) => {
        const [x, y] = project(p.lon, p.lat, W, H);
        const isSel = selected === p.city;
        const isHover = hover === p.city;
        const r = isSel ? 13 : isHover ? 11 : 9;
        return (
          <g key={p.city}
            onMouseEnter={() => setHover(p.city)}
            onMouseLeave={() => setHover(null)}
            onClick={() => onSelect(isSel ? null : p.city)}
            style={{ cursor: "pointer" }}>
            <circle cx={x} cy={y} r={r} fill={tempColor(p.temperature)}
              style={{ stroke: "var(--plane)", transition: "r .15s, opacity .15s" }}
              strokeWidth={2} opacity={selected && !isSel ? 0.35 : 1} />
            {isSel ? <circle cx={x} cy={y} r={r + 4} fill="none"
              style={{ stroke: "var(--accent)" }} strokeWidth={2} /> : null}
            <text x={x} y={y - r - 6} textAnchor="middle"
              style={{ fill: "var(--text-secondary)", fontSize: 12, fontWeight: 600 }}>
              {p.city}
            </text>
            {(isHover || isSel) ? (
              <text x={x} y={y + r + 15} textAnchor="middle"
                style={{ fill: "var(--text-primary)", fontSize: 12 }}>
                {p.temperature === null ? "–" : p.temperature.toFixed(1) + " °C"}
              </text>) : null}
          </g>
        );
      })}
    </svg>
  );
}
