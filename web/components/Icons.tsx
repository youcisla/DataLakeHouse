export function WeatherIcon({ kind, size = 20 }: { kind: string; size?: number }) {
  const common = { width: size, height: size, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 1.8, strokeLinecap: "round" as const, strokeLinejoin: "round" as const };
  switch (kind) {
    case "sun":
      return <svg {...common}><circle cx="12" cy="12" r="4" fill="currentColor" stroke="none" /><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" /></svg>;
    case "rain":
      return <svg {...common}><path d="M7 16a4 4 0 0 1-.3-8A5.5 5.5 0 0 1 17.5 8.5 3.5 3.5 0 0 1 17 16H7z" /><path d="M8 19l-1 2M12 19l-1 2M16 19l-1 2" /></svg>;
    case "cloud":
      return <svg {...common}><path d="M7 16a4 4 0 0 1-.3-8A5.5 5.5 0 0 1 17.5 8.5 3.5 3.5 0 0 1 17 16H7z" /></svg>;
    case "snow":
      return <svg {...common}><path d="M7 16a4 4 0 0 1-.3-8A5.5 5.5 0 0 1 17.5 8.5 3.5 3.5 0 0 1 17 16H7z" /><path d="M8 19h.01M12 19h.01M16 19h.01M10 21h.01M14 21h.01" /></svg>;
    case "wind":
      return <svg {...common}><path d="M3 8h11a2 2 0 1 0-2-2M3 12h15a2 2 0 1 1-2 2M3 16h9a2 2 0 1 1-2 2" /></svg>;
    case "hot":
      return <svg {...common}><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2" /><circle cx="12" cy="12" r="4" /></svg>;
    case "cold":
      return <svg {...common}><path d="M12 2v20M6 8l6-4 6 4M6 16l6 4 6-4" /></svg>;
    default:
      return <svg {...common}><circle cx="12" cy="12" r="4" fill="currentColor" stroke="none" /></svg>;
  }
}

export function eventIcon(kind: string): string {
  if (kind.includes("canicule")) return "hot";
  if (kind.includes("pluie")) return "rain";
  if (kind.includes("vent")) return "wind";
  if (kind.includes("froid")) return "cold";
  return "cloud";
}

export function tempIcon(t: number | null, precip: number | null): string {
  if (precip !== null && precip > 5) return "rain";
  if (t === null) return "cloud";
  if (t >= 28) return "hot";
  if (t >= 20) return "sun";
  if (t <= 0) return "cold";
  return "cloud";
}