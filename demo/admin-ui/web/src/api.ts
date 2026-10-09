export type ManagedKind = "rpc_node" | "node_admin";

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
export interface Overview {
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
}
export interface Edge {
  source: string;
  destination: string;
  kind: "direct" | "proxy";
  state: string;
  carrier: string | null;
  reason: string | null;
  logged_at_ms: number;
  reported_by: string;
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

export interface BootWaterfallResponse {
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
  createMesh: (name: string, node_admin: number, rpc_node: number) =>
    call<BuildAccepted>("/api/meshes", post({ name, node_admin, rpc_node })),
  removeMesh: (name: string) =>
    call<BuildAccepted>(`/api/meshes/${encodeURIComponent(name)}`, { method: "DELETE" }),
};
