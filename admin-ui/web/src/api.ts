export type NodeType = "gateway" | "broker" | "compute" | "registry" | "admin-ui";

/// sprint-20 lifecycle/health state (mirrors rafka_node_base::NodeState, locked,
/// append-only). Leaving/Dead evict before render; the rest are rendered + colored.
export type NodeState =
  | "Joining"
  | "Alive"
  | "Degraded"
  | "Updating"
  | "Draining"
  | "Leaving"
  | "Dead";

export interface TopologyNode {
  id: string;
  type: NodeType;
  mesh_id: string;
  node_id?: string;
  peer_count?: number;
  /// hex node_ids of every peer this node has an active iroh connection
  /// to. Resolve to friendly names via the topology's id→name map.
  peer_ids?: string[];
  /// monotonic frame counters from GossipDigest (live mesh, no Jaeger)
  frames_sent_total?: number;
  frames_recv_total?: number;
  /// per-digest emit time (staleness, bounces with gossip cadence)
  wall_time_ms?: number;
  /// UNIX-ms timestamp when admin-ui spawned this child. Used for the
  /// "age" (lifetime) display — monotonically increasing.
  spawn_time_ms?: number;
  /// CPU + RAM from GossipDigest. cores / GB respectively. Optional
  /// because pre-load-telemetry nodes (mid-rollout) may not populate them.
  cpu_used?: number;
  cpu_budget?: number;
  ram_used?: number;
  ram_budget?: number;
  status?: "live" | "pending";
  /// sprint-20: node lifecycle/health state from GossipDigest.state (or the
  /// backbone directory entry's state for remote-mesh nodes). Drives the node
  /// halo color. Terminal states (Leaving/Dead) are evicted before render, so
  /// in practice this is one of Joining/Alive/Degraded/Updating/Draining.
  state?: NodeState;
  /// sprint-14: "gossip" (home-mesh full detail) or "backbone" (remote-mesh
  /// summary directory entry). The backbone directory now carries per-node
  /// CPU/RAM, so cross-mesh nodes render their real metrics too.
  source?: "gossip" | "backbone";
  /// legacy — Jaeger-era, kept for back-compat
  frames_per_min?: number;
  /// sprint-stateful-node-restart: true if this node was spawned with
  /// `stateful:true` (GossipDigest.stateful or SpawnedMeta.stateful).
  /// Admin-ui never auto-wipes a stateful node's data dir; the
  /// `/api/nodes/{name}/restart` route restarts it with the same NodeId.
  stateful?: boolean;
}
export interface TopologyEdge {
  from: string;
  to: string;
  kind: "within" | "cross";
  frame_count?: number;
}
/// The per-mesh rollup that rides the backbone (`MeshAggregate`). node_count /
/// cpu / ram are instantaneous totals for the whole mesh; frames_per_sec is a
/// rate over the interval since the last sample. `source` says whether this
/// console summed it locally (own mesh) or received it off the backbone (remote).
export interface MeshAggregate {
  node_count: number;
  cpu_used: number;
  cpu_budget: number;
  ram_used: number;
  ram_budget: number;
  frames_per_sec: number;
  source: "local" | "backbone";
}
export interface TopologyResponse {
  nodes: TopologyNode[];
  edges: TopologyEdge[];
  /// keyed by mesh_id
  mesh_aggregates?: Record<string, MeshAggregate>;
}

export interface Heartbeat {
  node_id: string;
  node_name: string;
  node_type: NodeType;
  mesh_id: string;
  peer_count: number;
  age_ms: number;
  stateful?: boolean;
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

/// A single entry in the gossiped topology cache directory.
/// `name` is deterministic (mesh1.broker1 style); `location` is the
/// node's reachable bind address from GossipDigest.location.
export interface TopologyCacheEntry {
  name: string;
  mesh: string;
  type: string;
  location: string;
  node_id: string;
}
export interface TopologyCacheResponse {
  entries: TopologyCacheEntry[];
}

/// Entity-cache node entry from `GET /api/topology/node`. Each field maps
/// directly to what the server emits from topology_cache::TopologyCache.
export interface TopologyNodeEntry {
  node_id: string;
  node_name: string;
  mesh_id: string;
  node_type: string;
  ip_port: string;
  state: string;
  stateful: boolean;
  cpu_used: number;
  cpu_budget: number;
  ram_used: number;
  ram_budget: number;
}
export interface TopologyNodeCacheResponse {
  nodes: TopologyNodeEntry[];
}

// --- caches ---

export interface CacheItem {
  name: string;
  type: "Shared" | "Leader" | "Key" | "KeyGossip";
  channel: string;
  entry_count: number;
  distinct_publishers: string[];
  rejected_count: number;
  node_types: string[];
}

export interface CachesResponse {
  caches: CacheItem[];
}

export interface CacheEntry {
  key: string;
  value: number | string | Record<string, unknown>;
  epoch: number;
  publisher: string;
  updated_ms: number;
}

export interface CacheDetailResponse {
  name: string;
  type: string;
  channel: string;
  entry_count: number;
  distinct_publishers: string[];
  rejected_count: number;
  entries: CacheEntry[];
}

// --- channels ---

export interface ChannelListItem {
  channel: string;
  recent_event_count: number;
}

export interface ChannelsListResponse {
  channels: ChannelListItem[];
}

export interface ChannelEvent {
  ts_ms: number;
  publisher: string;
  op: string;
  key: string;
  epoch: number;
  value: number | string | Record<string, unknown>;
  cache_name?: string;
}

export interface ChannelDetailResponse {
  channel: string;
  events: ChannelEvent[];
}

// --- sim/caches ---

export interface SimCacheColumn {
  node_type: string;
  holds: boolean;
  entry_count: number;
  sample_entries: Array<Record<string, string | number>>;
  source: "real" | "sim";
}

export interface SimCacheRow {
  entity_kind: string;
  channel: string;
  write_model: "LeaderOnly" | "SelfKey" | "SharedKey";
  owners: string[];
  columns: SimCacheColumn[];
}

export interface SimCachesResponse {
  caches: SimCacheRow[];
}

// --- sim/channels ---

export interface SimChannelEvent {
  ts_ms: number;
  publisher: string;
  op: string;
  key: string;
  epoch: number;
  value: number | string | Record<string, unknown>;
  entity_kind?: string;
  channel?: string;
}

export interface SimChannel {
  channel: string;
  event_count?: number;
  events: SimChannelEvent[];
}

export interface SimChannelsResponse {
  channels: SimChannel[];
}

export const api = {
  topology: () => j<TopologyResponse>("/api/topology"),
  topologyCache: () => j<TopologyCacheResponse>("/api/topology-cache"),
  topologyNode: () => j<TopologyNodeCacheResponse>("/api/topology/node"),
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
  caches: () => j<CachesResponse>("/api/caches"),
  cacheDetail: (name: string) => j<CacheDetailResponse>(`/api/caches/${encodeURIComponent(name)}`),
  channels: () => j<ChannelsListResponse>("/api/channels"),
  channelDetail: (name: string) => j<ChannelDetailResponse>(`/api/channels/${encodeURIComponent(name)}`),
  simCaches: () => j<SimCachesResponse>("/api/sim/caches"),
  simChannels: () => j<SimChannelsResponse>("/api/sim/channels"),
  bootstrap: () =>
    j<{ mesh: string; spawned: string[] }>("/api/bootstrap", { method: "POST" }),
  bootstrap2mesh: () =>
    j<{
      peer_admin: string;
      peer_http_port: number;
      shared_root: boolean;
      mesh1_spawned: string[];
      mesh2_spawned: string[];
      total_nodes: number;
    }>("/api/bootstrap-2mesh", { method: "POST" }),
  runTest: (name: string, seed = 42) =>
    j<TestReport>("/api/tests/run", {
      method: "POST",
      body: JSON.stringify({ name, seed }),
    }),
  spawn: (
    node_type: NodeType,
    mesh_id: string,
    opts?: { cpu_budget?: number; ram_budget?: number; extra_env?: Record<string, string> },
  ) =>
    j<{ node_name: string; pid: number }>("/api/nodes/spawn", {
      method: "POST",
      body: JSON.stringify({
        node_type,
        extra_env: { RAFKA_MESH_ID: mesh_id, ...(opts?.extra_env ?? {}) },
        ...(opts?.cpu_budget !== undefined ? { cpu_budget: opts.cpu_budget } : {}),
        ...(opts?.ram_budget !== undefined ? { ram_budget: opts.ram_budget } : {}),
      }),
    }),
  kill: (node_name: string) =>
    j<{ node_name: string; reason: string }>(
      `/api/nodes/${encodeURIComponent(node_name)}`,
      { method: "DELETE" },
    ),
  // sprint-21: send a lifecycle SetState control op. state ∈ Updating|Draining|Alive(resume).
  setState: (node_name: string, state: NodeState) =>
    j<{ node_name: string; reason: string }>(
      `/api/nodes/${encodeURIComponent(node_name)}/state`,
      { method: "POST", body: JSON.stringify({ state }) },
    ),
};
