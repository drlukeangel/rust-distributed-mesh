export type NodeType = "gateway" | "broker" | "compute" | "registry" | "rpc_node" | "node_admin";

/// The node kinds node-admin manages; every change is a Build there.
export type ManagedKind = "rpc_node" | "node_admin";

/// A Build node-admin accepted.
export interface BuildAccepted {
  build_id: string;
}

/// One node node-admin manages, as its `GET /api/nodes` view reports it.
export interface TopologyNode {
  id: string;
  type: NodeType;
  mesh_id: string;
  node_id?: string;
  status?: string;
  is_primary?: boolean;
  is_fabric_primary?: boolean;
  incarnation_id?: string | null;
  /// The lifecycle state the birth declared to its authority, once applied.
  declared?: string | null;
}
export interface TopologyResponse {
  nodes: TopologyNode[];
}

export interface Heartbeat {
  node_id: string;
  node_name: string;
  node_type: NodeType;
  mesh_id: string;
  status?: string;
  is_primary?: boolean;
  is_fabric_primary?: boolean;
  incarnation_id?: string | null;
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

export const api = {
  topology: () => j<TopologyResponse>("/api/topology"),
  heartbeats: () => j<HeartbeatsResponse>("/api/heartbeats"),
  summary: () => j<ClusterSummary>("/api/cluster/summary"),
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
  bootstrap: () => j<BuildAccepted>("/api/bootstrap", { method: "POST" }),
  runTest: (name: string, seed = 42) =>
    j<TestReport>("/api/tests/run", {
      method: "POST",
      body: JSON.stringify({ name, seed }),
    }),
  spawn: (kind: ManagedKind, mesh: string) =>
    j<BuildAccepted>("/api/nodes/spawn", {
      method: "POST",
      body: JSON.stringify({ mesh, kind }),
    }),
  restart: (node_name: string) =>
    j<BuildAccepted>(`/api/nodes/${encodeURIComponent(node_name)}/restart`, { method: "POST" }),
  kill: (node_name: string) =>
    j<BuildAccepted>(`/api/nodes/${encodeURIComponent(node_name)}`, { method: "DELETE" }),
};
