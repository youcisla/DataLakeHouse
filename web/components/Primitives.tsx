"use client";

import { ReactNode, useEffect, useState } from "react";

/** Bascule de theme : elle doit gagner sur le reglage systeme, dans les deux sens. */
export function ThemeToggle() {
  const [theme, setTheme] = useState<"light" | "dark" | null>(null);

  useEffect(() => {
    const stored = (() => {
      try { return localStorage.getItem("theme"); } catch { return null; }
    })();
    if (stored === "light" || stored === "dark") {
      setTheme(stored);
      document.documentElement.setAttribute("data-theme", stored);
    }
  }, []);

  function toggle() {
    const current =
      theme ??
      (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
    const next = current === "dark" ? "light" : "dark";
    setTheme(next);
    document.documentElement.setAttribute("data-theme", next);
    try { localStorage.setItem("theme", next); } catch { /* mode prive */ }
  }

  return (
    <button className="toggle" onClick={toggle} aria-label="Changer de thème">
      {theme === "dark" ? "☀︎ Clair" : "☾ Sombre"}
    </button>
  );
}

/**
 * Bascule graphique / tableau.
 *
 * Exigee, pas decorative : trois couleurs de serie passent sous 3:1 sur la
 * surface claire, et la regle de secours impose alors un tableau consultable.
 */
export function ChartOrTable({ chart, table }: { chart: ReactNode; table: ReactNode }) {
  const [showTable, setShowTable] = useState(false);
  return (
    <div>
      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 8 }}>
        <button className="toggle" onClick={() => setShowTable((v) => !v)}>
          {showTable ? "Voir le graphique" : "Voir le tableau"}
        </button>
      </div>
      {showTable ? <div className="scroll">{table}</div> : chart}
    </div>
  );
}

export function Tile({ label, value, unit, note }: {
  label: string; value: string; unit?: string; note?: string;
}) {
  return (
    <div className="card tile">
      <div className="label">{label}</div>
      <div className="value">
        {value}
        {unit ? <span className="unit">{unit}</span> : null}
      </div>
      {note ? <div className="note">{note}</div> : null}
    </div>
  );
}

/** Statut : couleur + pastille + libelle. La couleur ne porte jamais le sens seule. */
export function Status({ level, children }: { level: string; children: ReactNode }) {
  const color =
    level === "extreme" ? "var(--critical)"
      : level === "alerte" ? "var(--warning)"
        : level === "good" ? "var(--good)"
          : "var(--serious)";
  return (
    <span className="status" style={{ color }}>
      <span className="dot" style={{ background: color }} aria-hidden="true" />
      {children}
    </span>
  );
}

export function Legend({ items }: { items: { label: string; color: string }[] }) {
  return (
    <ul className="legend">
      {items.map((item) => (
        <li key={item.label}>
          <span className="swatch" style={{ background: item.color }} aria-hidden="true" />
          {item.label}
        </li>
      ))}
    </ul>
  );
}

export function Empty({ what }: { what: string }) {
  return (
    <div className="card empty">
      Aucune donnée « {what} » exportée.<br />
      Lancez <code>make all</code> puis <code>make export-web</code>, et redéployez.
    </div>
  );
}
