"use client";

import * as echarts from "echarts/core";
import franceGeo from "@/lib/france-regions.json";
import EChart from "./EChart";
import { chartTheme, useTheme } from "@/lib/palette";

echarts.registerMap("france", franceGeo as any);

function tempColor(t: number | null): string {
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
  const theme = useTheme();
  const c = chartTheme(theme === "dark");
  const option = {
    backgroundColor: "transparent",
    tooltip: {
      trigger: "item", backgroundColor: c.tooltip, borderColor: c.border,
      textStyle: { color: c.text, fontSize: 12 },
      formatter: (p: any) => p.name + " · " + (p.value[2] === null ? "–" : p.value[2].toFixed(1) + " °C"),
    },
    geo: {
      map: "france", roam: true, center: [2.4, 46.6], zoom: 1.05, silent: true,
      itemStyle: { areaColor: c.mapFill, borderColor: c.mapLine, borderWidth: 1 },
      emphasis: { itemStyle: { areaColor: c.mapFill }, label: { show: false } },
      label: { show: false },
    },
    series: [{
      type: "scatter", coordinateSystem: "geo",
      data: points.map((p) => ({ name: p.city, value: [p.lon, p.lat, p.temperature] })),
      symbolSize: (v: any, params: any) => (params.name === selected ? 22 : 13),
      itemStyle: {
        color: (params: any) => tempColor(params.value[2]),
        shadowBlur: 8, shadowColor: "rgba(0,0,0,0.35)",
        borderColor: (params: any) => params.name === selected ? c.tooltip : "transparent", borderWidth: 2,
      },
      label: {
        show: true, formatter: "{b}", position: "top", color: c.text, fontSize: 11,
        textBorderColor: c.tooltip, textBorderWidth: 2,
      },
      emphasis: { scale: 1.5 },
    }],
  };
  return (
    <EChart option={option} height={500}
      onClick={(p: any) => onSelect(p.name === selected ? null : p.name)} />
  );
}
