"use client";

import * as echarts from "echarts/core";
import franceGeo from "@/lib/france-regions.json";
import EChart from "./EChart";
import { chartTheme, useTheme } from "@/lib/palette";

echarts.registerMap("france", franceGeo as any);

/** Rampe meteorologique reelle : froid (bleu) -> chaud (rouge). */
const WEATHER_RAMP = ["#3b82f6", "#60a5fa", "#2dd4bf", "#4ade80", "#facc15", "#fb923c", "#ef4444"];

export default function FranceMap({ points, selected, onSelect }: {
  points: { city: string; lat: number; lon: number; temperature: number | null }[];
  selected: string | null;
  onSelect: (city: string | null) => void;
}) {
  const theme = useTheme();
  const c = chartTheme(theme === "dark");
  const temps = points.map((p) => p.temperature).filter((t): t is number => t !== null);
  const min = temps.length ? Math.min(...temps) : 0;
  const max = temps.length ? Math.max(...temps) : 1;
  const span = max - min || 1;

  const option = {
    backgroundColor: "transparent",
    tooltip: {
      trigger: "item", backgroundColor: c.tooltip, borderColor: c.border,
      textStyle: { color: c.text, fontSize: 12 },
      formatter: (p: any) => p.name + " · " +
        (p.value[2] === null ? "–" : p.value[2].toFixed(1) + " °C"),
    },
    geo: {
      map: "france", roam: true, center: [2.4, 46.6], zoom: 1.1, silent: true,
      itemStyle: { areaColor: c.mapFill, borderColor: c.mapLine, borderWidth: 1 },
      emphasis: { itemStyle: { areaColor: c.mapFill }, label: { show: false } },
      label: { show: false },
    },
    visualMap: {
      type: "continuous", min, max, dimension: 2, calculable: true,
      orient: "vertical", right: 4, top: "middle", itemWidth: 12, itemHeight: 110,
      text: [max.toFixed(0) + " °C", min.toFixed(0) + " °C"],
      textStyle: { color: c.muted, fontSize: 10 },
      inRange: { color: WEATHER_RAMP },
    },
    series: [{
      type: "scatter", coordinateSystem: "geo",
      data: points.map((p) => ({ name: p.city, value: [p.lon, p.lat, p.temperature] })),
      symbolSize: (v: any, params: any) => {
        if (v[2] === null) return 6;
        const base = 7 + ((v[2] - min) / span) * 15;
        return params.name === selected ? base + 6 : base;
      },
      itemStyle: {
        shadowBlur: 6, shadowColor: "rgba(0,0,0,0.3)",
        borderColor: (params: any) => params.name === selected ? c.text : "transparent",
        borderWidth: 2,
      },
      label: {
        show: true, formatter: (p: any) => (p.name === selected ? p.name : ""),
        position: "top", color: c.text, fontSize: 12,
        textBorderColor: c.tooltip, textBorderWidth: 3,
      },
      emphasis: { scale: 1.6 },
    }],
  };
  return (
    <EChart option={option} height={520}
      onClick={(p: any) => onSelect(p.name === selected ? null : p.name)} />
  );
}
