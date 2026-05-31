import { useEffect, useState } from "react";
import { api, type TopologyCacheEntry } from "../api";

/// Cache tab — the gossiped topology directory.
/// Shows `name → {mesh, type, location}` built from live_digests().
/// Polls /api/topology-cache every 2s; server reads the gossip map in real time.
export function Cache() {
  const [entries, setEntries] = useState<TopologyCacheEntry[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<number>(0);

  useEffect(() => {
    let cancelled = false;
    const tick = () => {
      api
        .topologyCache()
        .then((r) => {
          if (!cancelled) {
            setEntries(r.entries ?? []);
            setErr(null);
            setLastUpdated(Date.now());
          }
        })
        .catch((e) => !cancelled && setErr(String(e)));
    };
    tick();
    const id = setInterval(tick, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const now = Date.now();
  const ageStr = lastUpdated
    ? `${((now - lastUpdated) / 1000).toFixed(1)}s ago`
    : "—";

  return (
    <div className="flex flex-col gap-2">
      <div className="text-sm text-gray-400">
        gossiped topology directory — name → &#123;mesh, type, location&#125; ·{" "}
        {entries.length} entr{entries.length === 1 ? "y" : "ies"} · updated {ageStr}
      </div>
      {err && <div className="text-red-400 text-xs">{err}</div>}
      <div className="border border-gray-800 rounded overflow-hidden">
        <table className="w-full text-xs font-mono">
          <thead className="bg-gray-900 text-gray-400">
            <tr>
              <th className="text-left p-2">name</th>
              <th className="text-left p-2 w-24">mesh</th>
              <th className="text-left p-2 w-24">type</th>
              <th className="text-left p-2">location</th>
              <th className="text-left p-2">node_id (prefix)</th>
            </tr>
          </thead>
          <tbody>
            {entries.map((e, i) => (
              <tr
                key={e.node_id || i}
                className="border-t border-gray-800 hover:bg-gray-900/50"
              >
                <td className="p-2 text-white font-semibold">{e.name}</td>
                <td className="p-2 text-blue-400">{e.mesh}</td>
                <td className="p-2 text-green-400">{e.type}</td>
                <td className="p-2 text-yellow-400">{e.location || "—"}</td>
                <td className="p-2 text-gray-500">
                  {e.node_id ? `${e.node_id.slice(0, 12)}…` : "—"}
                </td>
              </tr>
            ))}
            {entries.length === 0 && (
              <tr>
                <td colSpan={5} className="p-4 text-center text-gray-600">
                  no nodes in cache — spawn a node and wait for gossip (~2–5s)
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
