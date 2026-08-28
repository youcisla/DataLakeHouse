"use client";

import EChart from "./EChart";
import { chartTheme, cityColor, useTheme } from "@/lib/palette";

function useChartColors() {
  const theme = useTheme();
  return chartTheme(theme === "dark");
}

const TOOLTIP = {
  backgroundColor: "#17181c", borderColor: "#2a2d34",
  textStyle: { color: "#e7eaf0", fontSize: 12 }, padding: [8, 11],
};

/** Graphique en lignes multi-villes : dataZoom + survol focus. */
export function SeriesChart({ data, cities, unit = " °C", height = 360 }: {
  data: Record<string, any>[]; cities: string[]; unit?: string; height?: number;
}) {
  const c = useChartColors();
  const option = {
    backgroundColor: "transparent",
    grid: { left: 46, right: 18, top: 34, bottom: 52, containLabel: false },
    legend: { top: 0, textStyle: { color: c.muted, fontSize: 12 }, icon: "circle", itemWidth: 9, itemHeight: 9 },
    tooltip: {
      ...TOOLTIP, trigger: "axis", backgroundColor: c.tooltip, borderColor: c.border,
      textStyle: { color: c.text, fontSize: 12 },
      formatter: (params: any) => {
        const p = Array.isArray(params) ? params : [params];
        const rows = p.filter((x: any) => x.value !== null && x.value !== undefined)
          .map((x: any) => {
            const col = x.color as string;
            return "<span style=\"display:inline-block;width:9px;height:9px;border-radius:50%;background:" + col + ";margin-right:6px\"></span>" + x.seriesName + "&nbsp;&nbsp;<b>" + Number(x.value).toFixed(1) + unit + "</b>";
          }).join("<br/>");
        return "<div style=\"font-weight:600;margin-bottom:4px\">" + p[0].axisValue + "</div>" + rows;
      },
    },
    dataZoom: [
      { type: "inside", filterMode: "weakFilter" },
      { type: "slider", height: 20, bottom: 4, borderColor: c.border, backgroundColor: "transparent",
        fillerColor: "rgba(128,132,144,0.15)", textStyle: { color: c.muted, fontSize: 10 } },
    ],
    xAxis: { type: "category", data: data.map((r) => String(r.label ?? r.dt).slice(5)),
      axisLine: { lineStyle: { color: c.axis } }, axisTick: { show: false },
      axisLabel: { color: c.muted, fontSize: 11 } },
    yAxis: { type: "value", scale: true, name: unit, nameTextStyle: { color: c.muted },
      splitLine: { lineStyle: { color: c.grid } }, axisLabel: { color: c.muted, fontSize: 11 } },
    series: cities.map((city) => ({
      name: city, type: "line", smooth: 0.25, symbol: "none",
      data: data.map((r) => r[city] ?? null),
      lineStyle: { width: 2, color: cityColor(city) }, itemStyle: { color: cityColor(city) },
      emphasis: { focus: "series" }, connectNulls: false,
    })),
  };
  return <EChart option={option as any} height={height} />;
}

/** Barres : cumuls par jour, une couleur par ville. */
export function GroupedBars({ data, cities, unit = " mm", height = 300 }: {
  data: Record<string, any>[]; cities: string[]; unit?: string; height?: number;
}) {
  const c = useChartColors();
  const option = {
    backgroundColor: "transparent",
    grid: { left: 46, right: 18, top: 34, bottom: 30 },
    legend: { top: 0, textStyle: { color: c.muted, fontSize: 12 }, icon: "circle", itemWidth: 9, itemHeight: 9 },
    tooltip: { ...TOOLTIP, trigger: "axis", backgroundColor: c.tooltip, borderColor: c.border,
      textStyle: { color: c.text, fontSize: 12 } },
    xAxis: { type: "category", data: data.map((r) => String(r.label)), axisLine: { lineStyle: { color: c.axis } },
      axisLabel: { color: c.muted, fontSize: 11 } },
    yAxis: { type: "value", name: unit, splitLine: { lineStyle: { color: c.grid } },
      axisLabel: { color: c.muted, fontSize: 11 } },
    series: cities.map((city) => ({
      name: city, type: "bar", data: data.map((r) => r[city] ?? null),
      itemStyle: { color: cityColor(city), borderRadius: [4, 4, 0, 0] }, barMaxWidth: 22,
    })),
  };
  return <EChart option={option as any} height={height} />;
}

/** Heatmap mois × ville (normale de température). */
export function ClimateHeatmap({ data, months, cities, height = 320 }: {
  data: [number, number, number][]; months: string[]; cities: string[]; height?: number;
}) {
  const c = useChartColors();
  const max = Math.max(...data.map((d) => d[2]), 1);
  const min = Math.min(...data.map((d) => d[2]), 0);
  const option = {
    backgroundColor: "transparent",
    grid: { left: 60, right: 20, top: 20, bottom: 40 },
    tooltip: { ...TOOLTIP, backgroundColor: c.tooltip, borderColor: c.border,
      textStyle: { color: c.text, fontSize: 12 },
      formatter: (p: any) => cities[p.value[1]] + " · " + months[p.value[0]] + "<br/><b>" + p.value[2].toFixed(1) + " °C</b>" },
    xAxis: { type: "category", data: months, axisLine: { lineStyle: { color: c.axis } },
      axisLabel: { color: c.muted, fontSize: 11 } },
    yAxis: { type: "category", data: cities, axisLine: { lineStyle: { color: c.axis } },
      axisLabel: { color: c.muted, fontSize: 12 } },
    visualMap: { min, max, calculable: true, orient: "horizontal", left: "center", bottom: 0,
      itemHeight: 90, textStyle: { color: c.muted, fontSize: 10 },
      inRange: { color: ["#440154", "#3b528b", "#21918c", "#5ec962", "#a6d75b", "#fde725"] } },
    series: [{ type: "heatmap", data,
      label: { show: true, color: "#fff", fontSize: 10, formatter: (p: any) => p.value[2].toFixed(0) },
      itemStyle: { borderColor: c.tooltip, borderWidth: 2 },
      emphasis: { itemStyle: { shadowBlur: 8, shadowColor: "rgba(0,0,0,.3)" } } }],
  };
  return <EChart option={option as any} height={height} />;
}

/** Prédit vs réel : nuage + droite y=x. */
export function PredictionScatter({ data, height = 320 }: {
  data: { city: string; actual: number; predicted: number }[]; height?: number;
}) {
  const c = useChartColors();
  const cities = [...new Set(data.map((d) => d.city))];
  const all = data.flatMap((d) => [d.actual, d.predicted]);
  const mn = Math.min(...all) - 1, mx = Math.max(...all) + 1;
  const option = {
    backgroundColor: "transparent",
    grid: { left: 46, right: 20, top: 34, bottom: 46 },
    legend: { top: 0, textStyle: { color: c.muted, fontSize: 12 }, icon: "circle", itemWidth: 9, itemHeight: 9 },
    tooltip: { ...TOOLTIP, backgroundColor: c.tooltip, borderColor: c.border,
      textStyle: { color: c.text, fontSize: 12 },
      formatter: (p: any) => p.seriesName + "<br/>réel " + p.value[0].toFixed(1) + " · prédit " + p.value[1].toFixed(1) + " °C" },
    xAxis: { type: "value", name: "réel °C", min: mn, max: mx, splitLine: { lineStyle: { color: c.grid } },
      axisLabel: { color: c.muted, fontSize: 11 } },
    yAxis: { type: "value", name: "prédit °C", min: mn, max: mx, splitLine: { lineStyle: { color: c.grid } },
      axisLabel: { color: c.muted, fontSize: 11 } },
    series: [
      ...cities.map((city) => ({ name: city, type: "scatter", symbolSize: 9,
        data: data.filter((d) => d.city === city).map((d) => [d.actual, d.predicted]),
        itemStyle: { color: cityColor(city) } })),
      { type: "line", name: "y = x", data: [[mn, mn], [mx, mx]], symbol: "none",
        lineStyle: { color: c.axis, width: 1, type: "dashed" }, silent: true,
        tooltip: { show: false }, legendHoverLink: false },
    ],
  };
  return <EChart option={option as any} height={height} />;
}

/** Écart à la semaine précédente : barres divergentes signées. */
export function WeeklyDelta({ data, height = 320 }: { data: { label: string; value: number }[]; height?: number }) {
  const c = useChartColors();
  const option = {
    backgroundColor: "transparent",
    grid: { left: 90, right: 30, top: 10, bottom: 30 },
    tooltip: { ...TOOLTIP, backgroundColor: c.tooltip, borderColor: c.border,
      textStyle: { color: c.text, fontSize: 12 },
      formatter: (p: any) => p.name + "<br/>Δ " + (p.value >= 0 ? "+" : "") + p.value.toFixed(2) + " °C" },
    xAxis: { type: "value", axisLabel: { color: c.muted, fontSize: 11 },
      splitLine: { lineStyle: { color: c.grid } } },
    yAxis: { type: "category", data: data.map((d) => d.label), axisLine: { lineStyle: { color: c.axis } },
      axisLabel: { color: c.muted, fontSize: 12 } },
    series: [{ type: "bar",
      data: data.map((d) => ({ value: d.value,
        itemStyle: { color: d.value >= 0 ? "#e76f51" : "#2a9d8f",
          borderRadius: d.value >= 0 ? [0, 4, 4, 0] : [4, 0, 0, 4] } })),
      label: { show: true, position: "right", color: c.text, fontSize: 11,
        formatter: (p: any) => (p.value >= 0 ? "+" : "") + p.value.toFixed(2) },
      markLine: { silent: true, symbol: "none", lineStyle: { color: c.axis, width: 1, type: "dashed" },
        data: [{ xAxis: 0 }] } }],
  };
  return <EChart option={option as any} height={height} />;
}

/** Donut : répartition par catégorie. */
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
