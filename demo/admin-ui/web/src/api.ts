export type ManagedType = "node-admin" | "gateway" | "compute" | "broker";
/// A mesh's desired counts, as node-admin's Build takes them.
export type MeshShape = { name: string; node_admin: number; rpc_node?: number; gateway?: number; compute?: number; broker?: number };

/// A node's two properties, its path and its name: never joined into one string.
export interface PathName {
  path: string;
  name: string;
}
export const samePathName = (a: PathName, b: PathName) => a.path === b.path && a.name === b.name;
export const comparePathName = (a: PathName, b: PathName) => a.path.localeCompare(b.path) || a.name.localeCompare(b.name);
/// A node's path and name as text, for a message or an option.
export const pathNameText = (n: PathName) => `path ${n.path}, name ${n.name}`;

export interface BuildAccepted {
  build_id: string;
}

/// A node command's answer: it is never a Build, so it names its terminal frame.
export interface CommandEnded {
  terminal: string;
}

export interface FabricInfo {
  name: string;
  status: string;
  provider: string;
  build_id: string | null;
  /// The fabric-primary's path and name, when node-admin's view holds its row.
  fabric_primary: PathName | null;
  /// The fabric-primary's node id, as `fabric.get` names it.
  fabric_primary_id: string | null;
}
export interface MeshInfo {
  name: string;
  status: string;
  primary_admin: string | null;
  nodes: number;
}
/// How a Build ended, its last `rdm.build.ended`'s state: `complete`, `incomplete { sections }` or
/// `failed { reason, code }` (it could not start).
export type BuildEnded = "complete" | { incomplete: { sections: { mesh: string; op: string; step: string; reason: string; code: string }[] } } | { failed: { reason: string; code: string } } | null;
/// The change a Build record carries as it came in (build.md §1): `{ Create: {...} }`, `{ Delete: {...} }`
/// or `"DeleteFabric"`.
export type BuildChange = Record<string, unknown> | string;
export interface BuildBrief {
  build_id: string;
  state: string;
  change: BuildChange;
  /// When the Build was accepted, ms since the Unix epoch.
  time: number;
  ended: BuildEnded;
}
export interface Overview {
  fabric: FabricInfo | null;
  meshes: MeshInfo[];
  nodes_total: number;
  build: BuildBrief | null;
  error: string | null;
}

export interface TopologyNode {
  path: string;
  name: string;
  type: string;
  mesh: string;
  state: string;
  role: string;
  is_primary: boolean;
  is_fabric_primary: boolean;
  incarnation: string | null;
  node_id: string;
  has_runtime: boolean;
}
export interface Connection {
  source: string;
  destination: string;
  kind: "direct" | "proxy";
  state: string;
  carrier: string | null;
  reason: string | null;
  logged_at_ms: number;
  /// The node id of the node that answered the fact.
  reported_by: string;
}
export interface Topology {
  nodes: TopologyNode[];
  connections: Connection[];
  connection_errors: string[];
}

/// One `rdm.build.step` event of a Build: its mesh's section, its operation, the step and how it ended.
export interface StepView {
  mesh: string;
  op: string;
  step: string;
  state: unknown;
  subject: string | null;
  time: string;
}
/// A Build as `build.get` carries it: its state, its topology's node count, its events and its end.
export interface BuildView {
  build_id: string;
  state: string;
  current: boolean;
  nodes: number;
  change: BuildChange;
  /// When the Build was accepted, ms since the Unix epoch.
  time: number;
  runs: number;
  sections: string[];
  steps: StepView[];
  ended: BuildEnded;
}
export interface Builds {
  current: string | null;
  builds: BuildView[];
}

/// What a fault, a cut or a heal is aimed at.
export type Target =
  | { scope: "node"; path: string; name: string }
  | { scope: "mesh"; mesh: string }
  | { scope: "cut"; cut: number };
export interface FaultRecord {
  id: number;
  ts_ms: number;
  target: Target;
  action: string;
  outcome: "applied" | "refused";
  detail: unknown;
}
export interface Cut {
  id: number;
  scope: string;
  target: Target;
  ports: number[];
  since_ms: number;
}
export interface ChaosState {
  fabric_primary: PathName | null;
  faults: FaultRecord[];
  cuts: Cut[];
}

export interface TimelineEvent {
  ts_ms: number;
  /// The node the event's span names: its path and name, and its id, each when carried.
  node_path: string | null;
  node_name: string | null;
  node_id: string | null;
  /// The process that wrote it (its span file's name before `.spans.jsonl`), or `ui`.
  process: string;
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

/// The UI's route of one node: `/api/nodes/{path}/{name}`.
const nodeRoute = (n: PathName) => `/api/nodes/${encodeURIComponent(n.path)}/${encodeURIComponent(n.name)}`;

export const api = {
  overview: () => call<Overview>("/api/overview"),
  topology: () => call<Topology>("/api/topology"),
  builds: () => call<Builds>("/api/builds"),
  chaos: () => call<ChaosState>("/api/chaos"),
  fault: (node: PathName, action: "kill" | "stop" | "continue") =>
    call<FaultRecord>("/api/chaos/fault", post({ path: node.path, name: node.name, action })),
  cut: (target: Exclude<Target, { scope: "cut" }>) =>
    call<FaultRecord>("/api/chaos/cut", post(target)),
  heal: (id: number) => call<FaultRecord>("/api/chaos/heal", post({ id })),
  timeline: (all: boolean) => call<Timeline>(`/api/timeline?all=${all ? 1 : 0}&limit=400`),
  bootWaterfall: (node_id: string, type: string) =>
    call<BootWaterfallResponse>(`/api/boot-trace?node_id=${encodeURIComponent(node_id)}&type=${encodeURIComponent(type)}`),
  bootstrap: (meshes?: MeshShape[]) => call<BuildAccepted>("/api/bootstrap", meshes ? post({ meshes }) : { method: "POST" }),
  spawn: (type: ManagedType, mesh: string) =>
    call<BuildAccepted>("/api/nodes/spawn", post({ mesh, type })),
  stop: (node: PathName) =>
    call<CommandEnded>(`${nodeRoute(node)}/stop`, { method: "POST" }),
  start: (node: PathName) =>
    call<CommandEnded>(`${nodeRoute(node)}/start`, { method: "POST" }),
  remove: (node: PathName) =>
    call<BuildAccepted>(nodeRoute(node), { method: "DELETE" }),
  createMesh: (shape: MeshShape) => call<BuildAccepted>("/api/meshes", post(shape)),
  removeMesh: (name: string) =>
    call<BuildAccepted>(`/api/meshes/${encodeURIComponent(name)}`, { method: "DELETE" }),
};
