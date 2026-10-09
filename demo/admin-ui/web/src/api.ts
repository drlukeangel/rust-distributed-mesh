export type ManagedKind = "node_admin" | "gateway" | "broker" | "compute";

export interface BuildAccepted {
  build_id: string;
  attempt: number;
}

export interface FabricInfo {
  name: string;
  status: string;
  provider: string;
  build_id: string | null;
  fabric_primary: string | null;
}
export interface MeshInfo {
  name: string;
  status: string;
  primary_admin: string | null;
  nodes: number;
}
export interface BuildBrief {
  build_id: string;
  state: string;
  attempt: number;
  reason: string;
  executor: string | null;
  change: string;
}
export interface Summary {
  spawned: number;
  meshes: string[];
  chaos_per_min: number;
  mean_peers: number | null;
}
export interface Overview {
  summary?: Summary;
  fabric: FabricInfo | null;
  meshes: MeshInfo[];
  nodes_total: number;
  build: BuildBrief | null;
  error: string | null;
}

export interface TopologyNode {
  name: string;
  kind: string;
  mesh: string;
  status: string;
  seat: string;
  is_primary: boolean;
  is_fabric_primary: boolean;
  declared: string | null;
  incarnation_id: string | null;
  node_id: string;
  has_runtime: boolean;
  backbone: "" | "listener" | "publisher";
  load: NodeLoad | null;
}
export interface NodeLoad {
  cpu_used_millicores: number;
  cpu_budget_millicores: number;
  ram_used_bytes: number;
  ram_budget_bytes: number;
}
export interface Edge {
  source: string;
  destination: string;
  kind: "direct" | "proxy";
  /// connected | recovered | failed | disconnected: the pair's CURRENT state (see the Topology legend).
  state: string;
  /// Why the pair is judged in that state.
  basis: string;
  /// The newest fact's own state.
  last_fact: string;
  cross_mesh: boolean;
  backbone: boolean;
  carrier: string | null;
  reason: string | null;
  logged_at_ms: number;
}
export interface Topology {
  nodes: TopologyNode[];
  edges: Edge[];
  edge_errors: string[];
}

export interface StepView {
  attempt: number;
  operation: string;
  step: string;
  outcome: unknown;
}
export interface BuildView {
  build_id: string;
  state: string;
  attempt: number;
  reason: string;
  executor: string | null;
  last_failure: string | null;
  submitted_at_ms: number;
  submitted_change: { kind?: string } | null;
  steps: StepView[];
  topology: unknown;
}
export interface Builds {
  current: string | null;
  builds: BuildView[];
}

export interface FaultRecord {
  id: number;
  ts_ms: number;
  target: string;
  action: string;
  outcome: "applied" | "refused";
  detail: unknown;
}
export interface Cut {
  members: string[];
  id: number;
  scope: string;
  target: string;
  ports: number[];
  since_ms: number;
}
export interface ChaosState {
  fabric_primary: string | null;
  faults: FaultRecord[];
  cuts: Cut[];
}

export interface TimelineEvent {
  ts_ms: number;
  node: string;
  name: string;
  summary: string;
  high_volume: boolean;
  source: "span" | "ui";
}
export interface Timeline {
  events: TimelineEvent[];
  shown: number;
  hidden_high_volume: number;
  files: number;
}

export interface MessageRow {
  ts_ms: number;
  kind: "rpc" | "gossip";
  span: string;
  node: string;
  from: string;
  to: string;
  op: string;
  protocol: string;
  outcome: string;
  elapsed_ms: string;
  detail: string;
}

export interface AlertItem {
  id: number;
  ts_ms: number;
  severity: "info" | "warn" | "error";
  kind: string;
  message: string;
  node: string | null;
  mesh: string | null;
}
export interface TrafficState {
  updated_ms: number;
  issued: number;
  by_outcome: Record<string, number>;
  by_route: Record<string, number>;
  recent: Array<{ seq: number; at_ms: number; route: string; target: string; via: string | null; outcome: string; reason: string; trace_id: string }>;
}

export interface BootWaterfallResponse {
  service?: string;
  jaeger_url?: string;
  trace_url?: string;
  data?: Array<{ spans?: Array<{ operationName: string; startTime: number; duration: number }> }>;
}
export interface BootSpan {
  name: string;
  start_us: number;
  duration_ms: number;
}

const call = async <T,>(path: string, init?: RequestInit): Promise<T> => {
  const r = await fetch(path, { headers: { "Content-Type": "application/json" }, ...init });
  const text = await r.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text;
  }
  if (!r.ok) {
    const b = body as { error?: string; detail?: string } | null;
    throw new Error(b && b.error ? `${b.error}: ${b.detail ?? ""}` : `${path}: ${r.status}`);
  }
  return body as T;
};
const post = (body?: unknown): RequestInit => ({
  method: "POST",
  body: body === undefined ? undefined : JSON.stringify(body),
});

export const api = {
  overview: () => call<Overview>("/api/overview"),
  topology: () => call<Topology>("/api/topology"),
  builds: () => call<Builds>("/api/builds"),
  chaos: () => call<ChaosState>("/api/chaos"),
  fault: (node: string, action: "kill" | "stop" | "continue") =>
    call<FaultRecord>("/api/chaos/fault", post({ node, action })),
  cut: (scope: "node" | "mesh", target: string) =>
    call<FaultRecord>("/api/chaos/cut", post({ scope, target })),
  heal: (id: number) => call<FaultRecord>("/api/chaos/heal", post({ id })),
  timeline: (all: boolean) => call<Timeline>(`/api/timeline?all=${all ? 1 : 0}&limit=400`),
  bootWaterfall: (node: string) =>
    call<BootWaterfallResponse>(`/api/boot-trace?service=${encodeURIComponent(node)}`),
  bootstrap: () => call<BuildAccepted>("/api/bootstrap", { method: "POST" }),
  spawn: (kind: ManagedKind, mesh: string) =>
    call<BuildAccepted>("/api/nodes/spawn", post({ mesh, kind })),
  restart: (name: string) =>
    call<BuildAccepted>(`/api/nodes/${encodeURIComponent(name)}/restart`, { method: "POST" }),
  remove: (name: string) =>
    call<BuildAccepted>(`/api/nodes/${encodeURIComponent(name)}`, { method: "DELETE" }),
  createMesh: (name: string) => call<BuildAccepted>("/api/meshes", post({ name })),
  messages: (kind: string, q: string) => call<{ messages: MessageRow[] }>(`/api/messages?kind=${kind}&limit=300&q=${encodeURIComponent(q)}`),
  alerts: () => call<{ alerts: AlertItem[] }>("/api/alerts"),
  traffic: () => call<TrafficState>("/api/traffic"),
  removeMesh: (name: string) =>
    call<BuildAccepted>(`/api/meshes/${encodeURIComponent(name)}`, { method: "DELETE" }),
};
