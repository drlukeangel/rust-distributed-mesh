import { useEffect, useState } from "react";
import { api } from "../api";

// Fixed column order as specified in the task contract.
const NODE_COLS = ["admin-ui", "gateway", "broker", "compute", "registry"] as const;
type NodeCol = (typeof NODE_COLS)[number];

interface CacheItem {
  name: string;
  type: "Shared" | "Leader" | "Key" | "KeyGossip";
  channel: string;
  entry_count: number;
  distinct_publishers: string[];
  rejected_count: number;
  node_types: string[];
}

interface CachesResponse {
  caches: CacheItem[];
}

interface CacheEntryRow {
  key: string;
  value: number | string;
  epoch: number;
  publisher: string;
  updated_ms: number;
}
interface CacheDetail {
  name: string;
  type: string;
  channel: string;
  entry_count: number;
  distinct_publishers: string[];
  rejected_count: number;
  entries: CacheEntryRow[];
}

function fmtTime(ms: number): string {
  const d = new Date(ms);
  return [d.getHours(), d.getMinutes(), d.getSeconds()]
    .map((n) => String(n).padStart(2, "0"))
    .join(":");
}

// 4 distinct colors for type badges.
const TYPE_COLORS: Record<string, string> = {
  Shared:    "#6366f1", // indigo
  Leader:    "#f59e0b", // amber
  Key:       "#10b981", // emerald
  KeyGossip: "#ef4444", // red
};

// Channel badge: "main" gets a distinct accent; dedicated cache channels are muted.
const channelColor = (ch: string) =>
  ch === "main" ? "#f97316" : "#64748b"; // orange vs slate

export function Caches() {
  const [data, setData] = useState<CachesResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<CacheDetail | null>(null);

  useEffect(() => {
    let alive = true;
    const refresh = () => {
      api.caches().then((d) => { if (alive) { setData(d); setError(null); } })
         .catch((e: Error) => { if (alive) setError(e.message); });
    };
    refresh();
    const id = setInterval(refresh, 1500);
    return () => { alive = false; clearInterval(id); };
  }, []);

  // When a cache is selected, poll its actual contents.
  useEffect(() => {
    if (!selected) { setDetail(null); return; }
    let alive = true;
    const load = () => {
      api.cacheDetail(selected)
        .then((d) => { if (alive) setDetail(d as unknown as CacheDetail); })
        .catch(() => {});
    };
    load();
    const id = setInterval(load, 1500);
    return () => { alive = false; clearInterval(id); };
  }, [selected]);

  if (error) return <div style={{ color: "#ef4444", padding: 16 }}>Error: {error}</div>;
  if (!data)  return <div style={{ color: "#94a3b8", padding: 16 }}>Loading caches…</div>;

  // Sort rows by name (alphabetical).
  const rows = [...data.caches].sort((a, b) => a.name.localeCompare(b.name));

  return (
    <div style={{ padding: "12px 16px", overflowX: "auto" }}>
      <table
        data-testid="cache-matrix"
        style={{
          borderCollapse: "collapse",
          width: "100%",
          fontFamily: "monospace",
          fontSize: 13,
          color: "#e2e8f0",
        }}
      >
        <caption
          style={{
            textAlign: "left",
            fontSize: 11,
            color: "#64748b",
            paddingBottom: 8,
            captionSide: "bottom",
          }}
        >
          source: /api/caches — node caches by type. rows=caches, cols=node types; held cell = node type runs that cache. node-admin/admin-ui holds all.
        </caption>
        <thead>
          <tr>
            <th
              style={{
                textAlign: "left",
                padding: "6px 12px",
                borderBottom: "1px solid #334155",
                fontWeight: 600,
                color: "#94a3b8",
                whiteSpace: "nowrap",
              }}
            >
              Cache
            </th>
            {NODE_COLS.map((col) => (
              <th
                key={col}
                style={{
                  padding: "6px 10px",
                  borderBottom: "1px solid #334155",
                  fontWeight: 600,
                  color: "#94a3b8",
                  textAlign: "center",
                  whiteSpace: "nowrap",
                }}
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((cache) => (
            <tr
              key={cache.name}
              data-testid={`cache-row-${cache.name}`}
              style={{ borderBottom: "1px solid #1e293b" }}
            >
              {/* Row left-header: name + type badge + channel badge */}
              <td
                style={{
                  padding: "7px 12px",
                  whiteSpace: "nowrap",
                  display: "flex",
                  alignItems: "center",
                  gap: 6,
                }}
              >
                <button
                  onClick={() => setSelected(cache.name)}
                  title="View actual cache contents"
                  style={{
                    background: "transparent", border: "none", padding: 0,
                    color: "#7dd3fc", fontWeight: 600, fontFamily: "monospace", fontSize: 13,
                    cursor: "pointer", textDecoration: "underline", textDecorationStyle: "dotted",
                  }}
                >
                  {cache.name}
                </button>
                <span
                  data-testid={`cache-type-${cache.name}`}
                  style={{
                    background: TYPE_COLORS[cache.type] ?? "#475569",
                    color: "#fff",
                    borderRadius: 4,
                    padding: "1px 6px",
                    fontSize: 11,
                    fontWeight: 700,
                    letterSpacing: "0.02em",
                  }}
                >
                  {cache.type}
                </span>
                <span
                  data-testid={`cache-channel-${cache.name}`}
                  style={{
                    background: channelColor(cache.channel),
                    color: "#fff",
                    borderRadius: 4,
                    padding: "1px 6px",
                    fontSize: 11,
                    fontWeight: 500,
                    opacity: cache.channel === "main" ? 1 : 0.85,
                  }}
                >
                  {cache.channel}
                </span>
              </td>

              {/* One cell per node type column */}
              {NODE_COLS.map((col: NodeCol) => {
                const held = cache.node_types.includes(col);
                return (
                  <td
                    key={col}
                    data-testid={`cache-cell-${cache.name}-${col}`}
                    data-held={held ? "true" : "false"}
                    title={held ? `${cache.entry_count} entries` : "not held"}
                    style={{
                      textAlign: "center",
                      padding: "7px 10px",
                      background: held ? "rgba(99, 102, 241, 0.15)" : "transparent",
                      color: held ? "#a5b4fc" : "#334155",
                      fontWeight: held ? 700 : 400,
                      transition: "background 0.3s",
                      cursor: "default",
                    }}
                  >
                    {held ? cache.entry_count : "·"}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>

      {/* Click-to-open modal: the ACTUAL contents of one cache. */}
      {selected && (
        <div
          onClick={() => setSelected(null)}
          style={{
            position: "fixed", inset: 0, background: "rgba(0,0,0,0.72)", zIndex: 1000,
            display: "flex", alignItems: "center", justifyContent: "center", padding: 24,
          }}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            data-testid={`cache-modal-${selected}`}
            style={{
              background: "#0b1220", border: "1px solid #1e293b", borderRadius: 8,
              width: "90vw", maxWidth: 1200, maxHeight: "88vh",
              display: "flex", flexDirection: "column", overflow: "hidden",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "12px 16px", borderBottom: "1px solid #1e293b" }}>
              <span style={{ fontFamily: "monospace", fontWeight: 700, fontSize: 16, color: "#e2e8f0" }}>{selected}</span>
              {detail && (
                <span style={{ fontSize: 11, color: "#64748b" }}>
                  {detail.type} · {detail.channel} · {detail.entry_count} entries · {detail.rejected_count} rejected
                </span>
              )}
              <button
                onClick={() => setSelected(null)}
                style={{ marginLeft: "auto", background: "transparent", border: "1px solid #334155", color: "#cbd5e1", borderRadius: 4, cursor: "pointer", fontSize: 13, padding: "3px 12px" }}
              >
                ✕ close
              </button>
            </div>
            <div style={{ overflow: "auto", fontFamily: "monospace", fontSize: 13 }}>
              {!detail ? (
                <div style={{ padding: 16, color: "#64748b" }}>Loading…</div>
              ) : detail.entries.length === 0 ? (
                <div style={{ padding: 16, color: "#475569" }}>— cache is empty —</div>
              ) : (
                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ color: "#64748b", textAlign: "left", position: "sticky", top: 0, background: "#0b1220" }}>
                      <th style={{ padding: "6px 14px" }}>key</th>
                      <th style={{ padding: "6px 14px" }}>value</th>
                      <th style={{ padding: "6px 14px" }}>publisher</th>
                      <th style={{ padding: "6px 14px" }}>epoch</th>
                      <th style={{ padding: "6px 14px" }}>updated</th>
                    </tr>
                  </thead>
                  <tbody>
                    {detail.entries.map((e, i) => (
                      <tr key={`${e.key}-${i}`} style={{ borderBottom: "1px solid #131c2e", background: i % 2 ? "rgba(255,255,255,0.02)" : "transparent" }}>
                        <td style={{ padding: "5px 14px", color: "#cbd5e1", wordBreak: "break-all" }}>{String(e.key)}</td>
                        <td style={{ padding: "5px 14px", color: "#fbbf24" }}>{String(e.value)}</td>
                        <td style={{ padding: "5px 14px", color: "#7dd3fc", wordBreak: "break-all" }}>{e.publisher}</td>
                        <td style={{ padding: "5px 14px", color: "#64748b" }}>{e.epoch}</td>
                        <td style={{ padding: "5px 14px", color: "#64748b" }}>{fmtTime(e.updated_ms)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
