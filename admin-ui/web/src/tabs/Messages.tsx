import { useEffect, useState } from "react";
import { api, type MeshMessage } from "../api";

/// Messages tab — live per-node data-plane traffic, derived from the gossiped
/// frame counters (sprint-13 B4: the gateway→admin-ui CC was removed, so this no
/// longer shows per-frame rows captured by admin-ui; it shows each node's
/// TX/RX totals from its GossipDigest). Polls /api/messages every 1s.
export function Messages() {
  const [msgs, setMsgs] = useState<MeshMessage[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [paused, setPaused] = useState(false);

  useEffect(() => {
    if (paused) return;
    let cancelled = false;
    const tick = () => {
      api
        .messages()
        .then((r) => {
          if (!cancelled) {
            setMsgs(r.messages ?? []);
            setErr(null);
          }
        })
        .catch((e) => !cancelled && setErr(String(e)));
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [paused]);

  const now = Date.now();

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <div className="text-sm text-gray-400">
          per-node traffic (TX/RX) from gossip · {msgs.length} nodes ·
          updates every 1s
        </div>
        <button
          onClick={() => setPaused((p) => !p)}
          className="px-3 py-1 text-xs rounded bg-gray-800 text-gray-200 hover:bg-gray-700"
        >
          {paused ? "▶ resume" : "⏸ pause"}
        </button>
      </div>
      {err && (
        <div className="text-red-400 text-xs">{err}</div>
      )}
      <div className="border border-gray-800 rounded overflow-hidden">
        <table className="w-full text-xs font-mono">
          <thead className="bg-gray-900 text-gray-400">
            <tr>
              <th className="text-left p-2 w-20">age</th>
              <th className="text-left p-2 w-20">kind</th>
              <th className="text-left p-2 w-44">from peer</th>
              <th className="text-left p-2">payload (decoded)</th>
              <th className="text-right p-2 w-20">bytes</th>
            </tr>
          </thead>
          <tbody>
            {msgs.map((m, i) => {
              const ageMs = now - m.ts_ms;
              const ageStr =
                ageMs < 1000
                  ? `${ageMs}ms`
                  : ageMs < 60000
                    ? `${(ageMs / 1000).toFixed(1)}s`
                    : `${Math.floor(ageMs / 60000)}m${Math.floor((ageMs % 60000) / 1000)}s`;
              const kindColor =
                m.frame_kind === "ping"
                  ? "text-blue-400"
                  : m.frame_kind === "pong"
                    ? "text-green-400"
                    : m.frame_kind === "hello"
                      ? "text-purple-400"
                      : m.frame_kind === "write"
                        ? "text-amber-400"
                        : m.frame_kind === "traffic"
                          ? "text-cyan-400"
                          : "text-red-400";
              return (
                <tr
                  key={`${m.ts_ms}-${i}`}
                  className="border-t border-gray-800 hover:bg-gray-900/50"
                >
                  <td className="p-2 text-gray-500">{ageStr}</td>
                  <td className={`p-2 ${kindColor}`}>{m.frame_kind}</td>
                  <td className="p-2 text-gray-400">
                    {m.from_peer_id.slice(0, 16)}…
                  </td>
                  <td className="p-2 text-gray-200">{m.summary}</td>
                  <td className="p-2 text-right text-gray-500">{m.bytes}</td>
                </tr>
              );
            })}
            {msgs.length === 0 && (
              <tr>
                <td colSpan={5} className="p-4 text-center text-gray-600">
                  no nodes in gossip yet — spawn or bootstrap a fleet
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
