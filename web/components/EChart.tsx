"use client";

import { useEffect, useRef } from "react";
import * as echarts from "echarts/core";
import { LineChart, BarChart, ScatterChart, HeatmapChart, PieChart, MapChart } from "echarts/charts";
import {
  GridComponent, TooltipComponent, LegendComponent, DataZoomComponent,
  VisualMapComponent, MarkLineComponent, MarkPointComponent, MarkAreaComponent, GeoComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([
  LineChart, BarChart, ScatterChart, HeatmapChart, PieChart, MapChart,
  GridComponent, TooltipComponent, LegendComponent, DataZoomComponent,
  VisualMapComponent, MarkLineComponent, MarkPointComponent, MarkAreaComponent, GeoComponent,
  CanvasRenderer,
]);

export default function EChart({ option, height = 320, onClick }: {
  option: any; height?: number; onClick?: (p: any) => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const chartRef = useRef<any>(null);
  const clickRef = useRef(onClick);
  clickRef.current = onClick;

  useEffect(() => {
    if (!ref.current) return;
    const chart = echarts.init(ref.current);
    chartRef.current = chart;
    chart.on("click", (p: any) => clickRef.current?.(p));
    const onResize = () => chart.resize();
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      chart.dispose();
      chartRef.current = null;
    };
  }, []);

  useEffect(() => { chartRef.current?.setOption(option, true); }, [option]);

  return <div ref={ref} style={{ width: "100%", height }} />;
}
