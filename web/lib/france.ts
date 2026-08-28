/**
 * Géométrie France simplifiée + projection équirectangulaire.
 * Le tracé est une approximation reconnaissable ; les positions des villes
 * sont exactes (coordonnées réelles).
 */

export type LonLat = [number, number]; // [lon, lat]

const BOUNDS = { minLon: -5.5, maxLon: 9.9, minLat: 41.0, maxLat: 51.4 };

const FRANCE: LonLat[] = [
  [2.37, 51.03], [4.0, 51.4], [5.8, 51.0], [7.0, 50.5], [8.0, 50.0], [8.1, 49.0],
  [7.9, 48.4], [7.75, 48.57], [7.6, 47.6], [6.9, 46.3], [7.0, 45.8], [7.27, 43.7],
  [6.0, 43.2], [5.37, 43.3], [4.5, 43.0], [3.0, 42.5], [2.9, 42.7], [1.5, 42.6],
  [0.0, 43.0], [-1.7, 43.5], [-1.2, 44.5], [-1.6, 45.6], [-1.1, 46.5], [-2.1, 47.0],
  [-2.6, 47.6], [-4.5, 48.4], [-3.2, 48.8], [-1.6, 49.6], [-1.0, 49.2], [0.0, 49.6],
  [1.4, 50.0],
];

const CORSICA: LonLat[] = [
  [9.56, 43.0], [8.8, 42.6], [8.6, 41.4], [9.3, 41.6], [9.6, 42.4],
];

export function project(lon: number, lat: number, w: number, h: number): [number, number] {
  const x = ((lon - BOUNDS.minLon) / (BOUNDS.maxLon - BOUNDS.minLon)) * w;
  const y = ((BOUNDS.maxLat - lat) / (BOUNDS.maxLat - BOUNDS.minLat)) * h;
  return [x, y];
}

function toPath(pts: LonLat[], w: number, h: number): string {
  return pts.map(([lon, lat], i) => {
    const [x, y] = project(lon, lat, w, h);
    return (i === 0 ? "M" : "L") + x.toFixed(1) + " " + y.toFixed(1);
  }).join(" ") + " Z";
}

export function francePath(w: number, h: number): string {
  return toPath(FRANCE, w, h);
}

export function corsicaPath(w: number, h: number): string {
  return toPath(CORSICA, w, h);
}
