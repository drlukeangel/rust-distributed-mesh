import { useEffect, useState, useRef } from "react";
import { api, type ChannelEvent } from "../api";

// Channels are fetched individually. We always show "main" as the first column
// (topology-key-gossip rides the main channel); the rest come from /api/channels
// with the "cache:" prefix stripped for display and testids.
const MAIN_CHANNEL = "main";
const MAX_EVENTS = 20;

interface ChannelCol {
  // Raw API channel name (used for fetch URL). "main" stays as "main".
  rawName: string;
  // Display name (stripped of "cache:" prefix).
  displayName: string;
  // testid suffix — same as displayName.
  slug: string;
  // "real" = main/backbone; "cache" = everything else.
  kind: "real" | "cache";
  events: ChannelEvent[];
}

function truncate(id: string, len = 8): string {
  return id.length > len ? id.slice(0, len) : id;
}

function fmtTime(tsMs: number): string {
  const d = new Date(tsMs);
  return (
    String(d.getHours()).padStart(2, "0") +
    ":" +
    String(d.getMinutes()).padStart(2, "0") +
    ":" +
    String(d.getSeconds()).padStart(2, "0")
  );
}

// Map the op to the full-payload gossip view: a direction arrow, an
// accept/reject/broadcast result label, and a color.
function opMeta(op: string): { arrow: string; label: string; color: string } {
  if (op === "publish")  return { arrow: "→", label: "broadcast", color: "#60a5fa" };
  if (op === "received") return { arrow: "←", label: "ACCEPT", color: "#34d399" };
  if (op === "rejected") return { arrow: "←", label: "REJECT", color: "#f87171" };
  return { arrow: "·", label: op, color: "#94a3b8" };
}

export function Channels() {
  const [cols, setCols] = useState<ChannelCol[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => { mountedRef.current = false; };
  }, []);

  useEffect(() => {
    let alive = true;

    const refresh = async () => {
      try {
        // 1. Get the list of cache channels.
        let cacheChannels: { rawName: string; displayName: string; slug: string }[] = [];
        try {
          const list = await api.channels();
          cacheChannels = list.channels.filter((c) => c.channel !== MAIN_CHANNEL).map((c) => {
            const display = c.channel.startsWith("cache:") ? c.channel.slice(6) : c.channel;
            return { rawName: c.channel, displayName: display, slug: display };
          });
        } catch {
          // channels list failed; still show main
        }

        // 2. Fetch events for "main" (may return empty / error gracefully).
        let mainEvents: ChannelEvent[] = [];
        try {
          const detail = await api.channelDetail(MAIN_CHANNEL);
          mainEvents = detail.events ?? [];
        } catch {
          // main channel not found — show column with 0 events
        }

        // 3. Fetch events for each cache channel in parallel.
        const cacheDetails = await Promise.allSettled(
          cacheChannels.map((ch) => api.channelDetail(ch.rawName)),
        );

        if (!alive) return;

        // Build final column list: main first, then cache channels sorted by slug.
        const sorted = [...cacheChannels].sort((a, b) =>
          a.slug.localeCompare(b.slug),
        );

        const result: ChannelCol[] = [
          {
            rawName: MAIN_CHANNEL,
            displayName: "main",
            slug: "main",
            kind: "real",
            events: mainEvents.slice(0, MAX_EVENTS).reverse(),
          },
          ...sorted.map((ch, i) => {
            const settled = cacheDetails[i];
            const events =
              settled.status === "fulfilled"
                ? (settled.value.events ?? []).slice(0, MAX_EVENTS).reverse()
                : [];
            return {
              ...ch,
              kind: "cache" as const,
              events,
            };
          }),
        ];

        setCols(result);
        setError(null);
      } catch (e: unknown) {
        if (alive) setError((e as Error).message);
      }
    };

    refresh();
    const id = setInterval(refresh, 1500);
    return () => { alive = false; clearInterval(id); };
  }, []);

  if (error) return <div style={{ color: "#ef4444", padding: 16 }}>Error: {error}</div>;
  if (cols.length === 0)
    return <div style={{ color: "#94a3b8", padding: 16 }}>Loading channels…</div>;

  return (
    <div style={{ padding: "12px 16px", display: "flex", flexDirection: "column", gap: 8 }}>
      {/* Caption */}
      <div style={{ fontSize: 11, color: "#64748b" }}>
        source: /api/channels — one live gossip stream per topic; main+backbone real, cache channels per-cache.
      </div>

      {/* Horizontally scrollable grid */}
      <div
        data-testid="channels-grid"
        style={{
          display: "flex",
          gap: 12,
          overflowX: "auto",
          paddingBottom: 8,
          alignItems: "flex-start",
        }}
      >
        {cols.map((col) => (
          <div
            key={col.slug}
            data-testid={`channel-col-${col.slug}`}
            style={{
              minWidth: 300,
              maxWidth: 380,
              flex: "0 0 320px",
              display: "flex",
              flexDirection: "column",
              gap: 4,
              background: "#0f172a",
              borderRadius: 6,
              border: `1px solid ${col.kind === "real" ? "#f97316" : "#1e293b"}`,
              padding: "8px 0 4px",
            }}
          >
            {/* Column header */}
            <div
              style={{
                padding: "0 10px 6px",
                borderBottom: "1px solid #1e293b",
                display: "flex",
                alignItems: "center",
                gap: 6,
              }}
            >
              <span
                style={{
                  fontFamily: "monospace",
                  fontWeight: 700,
                  fontSize: 13,
                  color: col.kind === "real" ? "#fb923c" : "#94a3b8",
                }}
              >
                {col.displayName}
              </span>
              <span
                style={{
                  fontSize: 10,
                  padding: "1px 5px",
                  borderRadius: 3,
                  background: col.kind === "real" ? "rgba(249,115,22,0.15)" : "#1e293b",
                  color: col.kind === "real" ? "#fb923c" : "#64748b",
                  fontWeight: 600,
                  textTransform: "uppercase",
                  letterSpacing: "0.04em",
                }}
              >
                {col.kind}
              </span>
              <span
                style={{
                  marginLeft: "auto",
                  fontSize: 10,
                  color: "#475569",
                }}
              >
                {col.events.length}
              </span>
              <button
                onClick={() => setExpanded(col.slug)}
                title="Expand — full screen, full messages"
                style={{
                  marginLeft: 6,
                  background: "transparent",
                  border: "1px solid #334155",
                  color: "#94a3b8",
                  borderRadius: 4,
                  cursor: "pointer",
                  fontSize: 12,
                  lineHeight: 1,
                  padding: "2px 5px",
                }}
              >
                ⛶
              </button>
            </div>

            {/* Event list */}
            <div
              data-testid={`channel-events-${col.slug}`}
              style={{
                display: "flex",
                flexDirection: "column",
                gap: 0,
                maxHeight: 420,
                overflowY: "auto",
                fontFamily: "monospace",
                fontSize: 11,
              }}
            >
              {col.events.length === 0 ? (
                <div style={{ color: "#334155", padding: "6px 10px" }}>—</div>
              ) : (
                col.events.map((ev, idx) => {
                  const m = opMeta(ev.op);
                  return (
                  <div
                    key={`${ev.ts_ms}-${idx}`}
                    data-testid="channel-event"
                    style={{
                      display: "grid",
                      gridTemplateColumns: "52px 14px 64px 1fr 44px 64px",
                      gap: "0 5px",
                      padding: "3px 10px",
                      borderBottom: "1px solid #0f172a",
                      background: idx % 2 === 0 ? "transparent" : "rgba(255,255,255,0.02)",
                      alignItems: "center",
                      color: "#cbd5e1",
                    }}
                    title={`${ev.op} ${ev.key} = ${ev.value} (epoch ${ev.epoch}) ${ev.publisher}`}
                  >
                    {/* time */}
                    <span style={{ color: "#475569", fontSize: 10 }}>{fmtTime(ev.ts_ms)}</span>
                    {/* direction arrow */}
                    <span style={{ color: m.color, fontWeight: 700, textAlign: "center" }}>{m.arrow}</span>
                    {/* publisher (originator) */}
                    <span title={ev.publisher} style={{ color: "#7dd3fc", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {truncate(ev.publisher)}
                    </span>
                    {/* key = value (the actual payload). Key truncated so the
                        value is always visible (key caches use a long node_id key). */}
                    <span style={{ whiteSpace: "nowrap" }} title={`${ev.key} = ${ev.value}`}>
                      <span style={{ color: "#cbd5e1" }}>{truncate(String(ev.key), 14)}</span>
                      <span style={{ color: "#475569" }}> = </span>
                      <span style={{ color: "#fbbf24" }}>{String(ev.value)}</span>
                    </span>
                    {/* epoch */}
                    <span style={{ color: "#64748b", textAlign: "right", fontSize: 10 }}>e{ev.epoch % 100000}</span>
                    {/* result: broadcast / ACCEPT / REJECT */}
                    <span style={{ color: m.color, fontWeight: 600, fontSize: 10, textAlign: "right" }}>{m.label}</span>
                  </div>
                  );
                })
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Full-screen modal — complete, untruncated messages for one channel. */}
      {expanded && (() => {
        const col = cols.find((c) => c.slug === expanded);
        if (!col) return null;
        return (
          <div
            onClick={() => setExpanded(null)}
            style={{
              position: "fixed", inset: 0, background: "rgba(0,0,0,0.72)", zIndex: 1000,
              display: "flex", alignItems: "center", justifyContent: "center", padding: 24,
            }}
          >
            <div
              onClick={(e) => e.stopPropagation()}
              data-testid={`channel-modal-${col.slug}`}
              style={{
                background: "#0b1220", border: "1px solid #1e293b", borderRadius: 8,
                width: "94vw", maxWidth: 1500, maxHeight: "90vh",
                display: "flex", flexDirection: "column", overflow: "hidden",
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "12px 16px", borderBottom: "1px solid #1e293b" }}>
                <span style={{ fontFamily: "monospace", fontWeight: 700, fontSize: 16, color: col.kind === "real" ? "#fb923c" : "#e2e8f0" }}>{col.displayName}</span>
                <span style={{ fontSize: 11, color: "#64748b" }}>{col.kind} channel · {col.events.length} events · full message</span>
                <button
                  onClick={() => setExpanded(null)}
                  style={{ marginLeft: "auto", background: "transparent", border: "1px solid #334155", color: "#cbd5e1", borderRadius: 4, cursor: "pointer", fontSize: 13, padding: "3px 12px" }}
                >
                  ✕ close
                </button>
              </div>
              <div style={{ overflow: "auto", fontFamily: "monospace", fontSize: 13 }}>
                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ color: "#64748b", textAlign: "left", position: "sticky", top: 0, background: "#0b1220" }}>
                      <th style={{ padding: "6px 14px" }}>time</th>
                      <th style={{ padding: "6px 14px" }}>dir</th>
                      <th style={{ padding: "6px 14px" }}>publisher (full node id)</th>
                      <th style={{ padding: "6px 14px" }}>key</th>
                      <th style={{ padding: "6px 14px" }}>value</th>
                      <th style={{ padding: "6px 14px" }}>epoch</th>
                      <th style={{ padding: "6px 14px" }}>result</th>
                    </tr>
                  </thead>
                  <tbody>
                    {col.events.length === 0 ? (
                      <tr><td colSpan={7} style={{ padding: "12px 14px", color: "#475569" }}>— no events —</td></tr>
                    ) : col.events.map((ev, idx) => {
                      const m = opMeta(ev.op);
                      return (
                        <tr key={`${ev.ts_ms}-${idx}`} style={{ borderBottom: "1px solid #131c2e", background: idx % 2 ? "rgba(255,255,255,0.02)" : "transparent" }}>
                          <td style={{ padding: "5px 14px", color: "#64748b", whiteSpace: "nowrap" }}>{fmtTime(ev.ts_ms)}</td>
                          <td style={{ padding: "5px 14px", color: m.color, fontWeight: 700, textAlign: "center" }}>{m.arrow}</td>
                          <td style={{ padding: "5px 14px", color: "#7dd3fc", wordBreak: "break-all" }}>{ev.publisher}</td>
                          <td style={{ padding: "5px 14px", color: "#cbd5e1", wordBreak: "break-all" }}>{String(ev.key)}</td>
                          <td style={{ padding: "5px 14px", color: "#fbbf24" }}>{String(ev.value)}</td>
                          <td style={{ padding: "5px 14px", color: "#64748b", whiteSpace: "nowrap" }}>{ev.epoch}</td>
                          <td style={{ padding: "5px 14px", color: m.color, fontWeight: 600 }}>{m.label}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        );
      })()}
    </div>
  );
}
