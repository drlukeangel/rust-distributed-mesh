export type NodeType = "gateway" | "broker" | "compute" | "registry" | "bridge";

export interface TopologyNode {
  id: string;
  type: NodeType;
  mesh_id: string;
  node_id?: string;
  peer_count?: number;
  /// monotonic frame counters from GossipDigest (live mesh, no Jaeger)
  frames_sent_total?: number;
  frames_recv_total?: number;
  wall_time_ms?: number;
  status?: "live" | "pending";
  /// legacy — Jaeger-era, kept for back-compat
  frames_per_min?: number;
}
export interface TopologyEdge {
  from: string;
  to: string;
  kind: "within" | "cross";
  frame_count?: number;
}
export interface TopologyResponse {
  nodes: TopologyNode[];
  edges: TopologyEdge[];
}

export interface Heartbeat {
  node_id: string;
  node_name: string;
  node_type: NodeType;
  mesh_id: string;
  peer_count: number;
  age_ms: number;
}
export interface HeartbeatsResponse {
  heartbeats: Heartbeat[];
}

export interface ClusterSummary {
  spawned: number;
  meshes: string[];
  chaos_per_min: number;
  mean_peers: number;
}

export interface BootSpan {
  name: string;
  start_us: number;
  duration_ms: number;
}
/// Server returns raw Jaeger trace format: `{data: [{spans: [...]}]}`. We
/// transform it client-side into a sorted list of BootSpan rows. Keeping
/// the raw type here documents the wire contract.
export interface BootWaterfallResponse {
  data?: Array<{
    spans?: Array<{
      operationName: string;
      startTime: number;
      duration: number;
    }>;
  }>;
}

export interface TimelineEvent {
  ts_us: number;
  kind: string;
  node_name?: string;
  node_type?: NodeType;
  mesh_id?: string;
  detail?: string;
}
export interface TimelineResponse {
  events: TimelineEvent[];
}

export interface TestReport {
  name: string;
  seed?: number;
  status: "passed" | "failed" | "running";
  duration_ms?: number;
  events?: number;
  passed?: number;
  failed?: number;
  detail?: string;
  finished_at?: number;
}
export interface TestsResponse {
  reports: TestReport[];
  registry: { name: string; kind: string; description: string }[];
}

export interface AlertItem {
  ts_us: number;
  severity: "info" | "warn" | "error";
  node_name?: string;
  mesh_id?: string;
  message: string;
}
export interface AlertsResponse {
  alerts: AlertItem[];
}

export interface ChaosState {
  running: boolean;
  cadence_ms: number;
  total_events: number;
  last_event_ts_us?: number;
}

const j = async <T,>(path: string, init?: RequestInit): Promise<T> => {
  const r = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!r.ok) throw new Error(`${path}: ${r.status} ${r.statusText}`);
  return r.json() as Promise<T>;
};

export interface MeshMessage {
  ts_ms: number;
  from_peer_id: string;
  frame_kind: string;
  bytes: number;
  summary: string;
}
export interface MessagesResponse {
  messages: MeshMessage[];
}

export const api = {
  topology: () => j<TopologyResponse>("/api/topology"),
  heartbeats: () => j<HeartbeatsResponse>("/api/heartbeats"),
  summary: () => j<ClusterSummary>("/api/cluster/summary"),
  messages: () => j<MessagesResponse>("/api/messages"),
  bootWaterfall: (node?: string) =>
    j<BootWaterfallResponse>(
      `/api/boot-trace${node ? `?service=${encodeURIComponent(node)}` : ""}`,
    ),
  timeline: () => j<TimelineResponse>("/api/timeline"),
  tests: () => j<TestsResponse>("/api/tests"),
  alerts: () => j<AlertsResponse>("/api/alerts"),
  chaosState: () => j<ChaosState>("/api/chaos/state"),
  chaosStart: () => j<ChaosState>("/api/chaos/start", { method: "POST" }),
  chaosStop: () => j<ChaosState>("/api/chaos/stop", { method: "POST" }),
  bootstrap: () =>
    j<{ spawned: string[] }>("/api/bootstrap", { method: "POST" }),
  runTest: (name: string, seed = 42) =>
    j<TestReport>("/api/tests/run", {
      method: "POST",
      body: JSON.stringify({ name, seed }),
    }),
  spawn: (node_type: NodeType, mesh_id: string) =>
    j<{ node_name: string; pid: number }>("/api/nodes/spawn", {
      method: "POST",
      body: JSON.stringify({ node_type, extra_env: { RAFKA_MESH_ID: mesh_id } }),
    }),
  kill: (node_name: string) =>
    j<{ node_name: string; reason: string }>(
      `/api/nodes/${encodeURIComponent(node_name)}`,
      { method: "DELETE" },
    ),
};
