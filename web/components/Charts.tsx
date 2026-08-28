"use client";

import EChart from "./EChart";
import { chartTheme, useTheme, RAMP_TEMP } from "@/lib/palette";

function useChartColors() {
  const theme = useTheme();
  return chartTheme(theme === "dark");
}

const TOOLTIP = {
  backgroundColor: "#17181c", borderColor: "#2a2d34",
  textStyle: { color: "#e7eaf0", fontSize: 12 }, padding: [8, 11],
};

/**
 * Heatmap station x temps (ou x mois) : l'encodage qui rend 60 a 208 stations
 * lisibles sans legende a 200 entrees. Couleur = valeur (rampe claire->foncee),
 * dataZoom pour zoomer les axes, une ligne = une station, une colonne = un pas.
 */
export function StationTimeHeatmap({ data, xLabels, stations, unit = "", height = 480,
  ramp = RAMP_TEMP, zoomX = true, zoomY = false, valueLabel = "" }: {
  data: [number, number, number][]; xLabels: string[]; stations: string[];
  unit?: string; height?: number; ramp?: string[];
  zoomX?: boolean; zoomY?: boolean; valueLabel?: string;
}) {
  const c = useChartColors();
  const values = data.map((d) => d[2]).filter((v) => v !== null && v !== undefined);
  const min = values.length ? Math.min(...values) : 0;
  const max = values.length ? Math.max(...values) : 1;
  const dataZooms: any[] = [];
  if (zoomX) {
    dataZooms.push({ type: "inside", xAxisIndex: 0, filterMode: "none" });
    dataZooms.push({ type: "slider", xAxisIndex: 0, height: 16, bottom: 2,
      borderColor: c.border, backgroundColor: "transparent",
      fillerColor: "rgba(128,132,144,0.15)", textStyle: { color: c.muted, fontSize: 10 } });
  }
  if (zoomY) {
    const visible = Math.min(26, stations.length);
    const end = stations.length <= visible ? 100 : Math.round((visible / stations.length) * 100);
    dataZooms.push({ type: "inside", yAxisIndex: 0, filterMode: "none" });
    dataZooms.push({ type: "slider", yAxisIndex: 0, width: 14, right: 2, start: 0, end,
      borderColor: c.border, backgroundColor: "transparent",
      fillerColor: "rgba(128,132,144,0.15)" });
  }
  const option = {
    backgroundColor: "transparent",
    grid: { left: 110, right: 28, top: 14, bottom: zoomX ? 84 : 58 },
    tooltip: { ...TOOLTIP, backgroundColor: c.tooltip, borderColor: c.border,
      textStyle: { color: c.text, fontSize: 12 },
      formatter: (p: any) => {
        const v = p.value[2];
        return stations[p.value[1]] + " · " + xLabels[p.value[0]] +
          "<br/><b>" + (v === null ? "–" : v.toFixed(1) + unit) + "</b>" +
          (valueLabel ? " " + valueLabel : "");
      } },
    xAxis: { type: "category", data: xLabels, axisLine: { lineStyle: { color: c.axis } },
      axisTick: { show: false }, axisLabel: { color: c.muted, fontSize: 10, rotate: 40 } },
    yAxis: { type: "category", data: stations, axisLine: { lineStyle: { color: c.axis } },
      axisLabel: { color: c.muted, fontSize: 11 } },
    dataZoom: dataZooms,
    visualMap: { min, max, calculable: true, orient: "horizontal", left: "center",
      bottom: zoomX ? 26 : 0, itemWidth: 12, itemHeight: 72,
      textStyle: { color: c.muted, fontSize: 10 }, inRange: { color: ramp } },
    series: [{ type: "heatmap", data,
      label: { show: false },
      itemStyle: { borderColor: c.tooltip, borderWidth: 1 },
      emphasis: { itemStyle: { shadowBlur: 8, shadowColor: "rgba(0,0,0,.3)" } } }],
  };
  return <EChart option={option} height={height} />;
}

/** Ligne nationale 2000-2026 avec bande min/max (tendance climatique). */
export function TrendLine({ data, height = 360, unit = " °C" }: {
  data: { year: number; avg: number; min: number; max: number }[]; height?: number; unit?: string;
}) {
  const c = useChartColors();
  const option = {
    backgroundColor: "transparent",
    grid: { left: 52, right: 24, top: 30, bottom: 44 },
    tooltip: { ...TOOLTIP, trigger: "axis", backgroundColor: c.tooltip, borderColor: c.border,
      textStyle: { color: c.text, fontSize: 12 },
      formatter: (params: any) => {
        const year = params[0].axisValue;
        const d = data.find((x) => String(x.year) === String(year));
        if (!d) return String(year);
        return "<b>" + year + "</b><br/>moyenne " + d.avg.toFixed(1) + unit +
          "<br/>min " + d.min.toFixed(1) + " · max " + d.max.toFixed(1) + unit;
      } },
    xAxis: { type: "category", data: data.map((d) => String(d.year)),
      axisLine: { lineStyle: { color: c.axis } },
      axisLabel: { color: c.muted, fontSize: 11, interval: 2 } },
    yAxis: { type: "value", scale: true, name: unit, nameTextStyle: { color: c.muted },
      splitLine: { lineStyle: { color: c.grid } }, axisLabel: { color: c.muted, fontSize: 11 } },
    series: [
      { name: "min", type: "line", stack: "band", data: data.map((d) => d.min),
        lineStyle: { opacity: 0 }, symbol: "none", silent: true, tooltip: { show: false } },
      { name: "range", type: "line", stack: "band", data: data.map((d) => d.max - d.min),
        lineStyle: { opacity: 0 }, symbol: "none", silent: true, tooltip: { show: false },
        areaStyle: { color: "rgba(11,126,165,0.12)" } },
      { name: "Moyenne", type: "line", data: data.map((d) => d.avg), smooth: 0.2, symbol: "none",
        lineStyle: { width: 2.5, color: c.text === "#1a1d24" ? "#0b7ea5" : "#38bdf8" },
        itemStyle: { color: c.text === "#1a1d24" ? "#0b7ea5" : "#38bdf8" } },
    ],
  };
  return <EChart option={option} height={height} />;
}

/** Donut : repartition par categorie. */
export function Donut({ data, height = 300 }: { data: { name: string; value: number; color?: string }[]; height?: number }) {
  const c = useChartColors();
  const option = {
    backgroundColor: "transparent",
    tooltip: { ...TOOLTIP, backgroundColor: c.tooltip, borderColor: c.border,
      textStyle: { color: c.text, fontSize: 12 }, formatter: (p: any) => p.name + " : " + p.value },
    legend: { bottom: 0, textStyle: { color: c.muted, fontSize: 12 }, icon: "circle", itemWidth: 9, itemHeight: 9 },
    series: [{ type: "pie", radius: ["55%", "78%"], center: ["50%", "44%"],
      itemStyle: { borderColor: c.tooltip, borderWidth: 2, borderRadius: 5 },
      label: { color: c.text, fontSize: 11, formatter: "{b}\n{d}%" },
      data: data.map((d) => ({ name: d.name, value: d.value, itemStyle: { color: d.color } })) }],
  };
  return <EChart option={option as any} height={height} />;
}

/** Courbe d'une seule station (drill depuis la heatmap). */
export function StationLine({ data, unit = " °C", height = 280, color }: {
  data: { label: string; value: number | null }[]; unit?: string; height?: number; color?: string;
}) {
  const c = useChartColors();
  const accent = color || (c.text === "#1a1d24" ? "#0b7ea5" : "#38bdf8");
  const option = {
    backgroundColor: "transparent",
    grid: { left: 46, right: 18, top: 22, bottom: 46 },
    tooltip: { ...TOOLTIP, trigger: "axis", backgroundColor: c.tooltip, borderColor: c.border,
      textStyle: { color: c.text, fontSize: 12 },
      formatter: (p: any) => {
        const x = p[0].axisValue;
        const v = p[0].value;
        return x + "<br/><b>" + (v === null ? "–" : v.toFixed(1) + unit) + "</b>";
      } },
    xAxis: { type: "category", data: data.map((d) => d.label),
      axisLine: { lineStyle: { color: c.axis } }, axisTick: { show: false },
      axisLabel: { color: c.muted, fontSize: 10, rotate: 40 } },
    yAxis: { type: "value", scale: true, splitLine: { lineStyle: { color: c.grid } },
      axisLabel: { color: c.muted, fontSize: 11 } },
    series: [{ type: "line", data: data.map((d) => d.value), smooth: 0.25, symbol: "none",
      lineStyle: { width: 2, color: accent }, areaStyle: { color: accent, opacity: 0.12 } }],
  };
  return <EChart option={option} height={height} />;
}

/** Sparkline mini (KPI). */
export function Sparkline({ data, color, height = 44 }: { data: (number | null)[]; color: string; height?: number }) {
  const option = {
    backgroundColor: "transparent",
    grid: { left: 0, right: 0, top: 4, bottom: 4 },
    xAxis: { type: "category", show: false, data: data.map((_, i) => i) },
    yAxis: { type: "value", show: false, scale: true },
    series: [{ type: "line", data, symbol: "none", smooth: true, lineStyle: { width: 1.5, color },
      areaStyle: { color, opacity: 0.12 } }],
  };
  return <EChart option={option as any} height={height} />;
}
