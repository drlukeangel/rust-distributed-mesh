# Stateful Node Restart: Preserving Mesh Identity Across Process Restarts

In a QUIC-based P2P mesh, every node's identity is a cryptographic keypair. Your `node_id` IS your public key. When you restart a node, by default you get a new keypair — a new identity — and every peer has to re-establish a connection to what looks like a brand-new node. For many mesh participants that's fine. For nodes you care about maintaining continuity on (persistent state, specific routing roles, pinned-port infrastructure), getting a different NodeId on every restart is friction.

Rafka v2 now ships **stateful node restart**: the ability to kill a process and respawn it with the exact same iroh NodeId.

## How identity works in iroh

Every node in rafka's mesh has an `iroh::SecretKey`. The corresponding `PublicKey` is the node's `node_id` — the address the rest of the mesh uses to route QUIC connections to it. By default, if you don't persist the key, every boot mints a new one.

Rafka's admin-ui already had a solution for this: `RAFKA_NODE_SECRET_KEY`. When a child process starts with this env var set to a hex-encoded 32-byte key, `load_or_mint_identity` uses it directly instead of loading or generating a file. The key is also persisted to `node-identity.json` in the node's data dir for subsequent boots without the env var.

Since admin-ui pre-mints each child's key before spawning (to guarantee the node boots with the identity admin-ui recorded — eliminating a race condition where concurrent spawns could produce duplicate-name nodes), the key is already in memory. The stateful restart feature stores it, and re-passes it.

## The design

When you spawn a node with `"stateful": true`, three things change:

**1. The node's data dir is never auto-wiped.**

Normally, when a node exits, both `kill_one` (operator DELETE) and the reaper loop clean up `E:/tmp/rafka-ui-nodes/<node_name>/`. This includes `node-identity.json`. For stateful nodes, this cleanup is suppressed — the dir is the identity.

**2. The `SpawnedMeta` entry survives the kill.**

Admin-ui keeps an in-memory record of every spawned child. For stateful nodes this record persists across kills so (a) the reaper's orphan-sweep pass — which wipes any dir not referenced by either a live process or a meta entry — doesn't race-wipe the stateful dir, and (b) the restart route can read the original `secret_key_hex` and `bind_port`.

**3. A `restarting` DashSet prevents races.**

During the kill→respawn window, the node name is added to a process-global `DashSet<String>`. The reaper checks this set before wiping. Without it, a 5-second reaper tick coinciding with the kill phase could wipe the dir before the respawn path completed.

## What the restart route does

`POST /api/nodes/{name}/restart`:

1. Reads `SpawnedMeta` → rejects with 422 if node isn't stateful.
2. Adds to `restarting` set.
3. Removes the `Child` handle from `processes` and calls `start_kill()` + waits for genuine OS process exit (up to 10s — we need the port released before rebinding).
4. Rebuilds the spawn command with the SAME `spawn_dir`, `bind_port`, `mesh_id`, and crucially the SAME `RAFKA_NODE_SECRET_KEY`. The node boots with the same PublicKey.
5. Inserts the new `Child`, updates `SpawnedMeta.pid`.
6. Clears from `restarting`.
7. Emits `rafka.ui.node.restart` span to Jaeger with `node_id`, `old_pid`, `new_pid`.

The mesh sees the node temporarily leave (its gossip goes stale) and rejoin. Because the NodeId is the same, HyParView reconnects via the existing address; no bootstrapping needed.

## Contrast with chaos restart

The chaos `restart_node` primitive deliberately creates a new identity — that's the chaos. It tests that the mesh heals from a node disappearing and a different node joining in its place. Stateful restart is the operational path: "I want to upgrade or reconfigure this node without disrupting its mesh identity."

They share no code. The chaos path calls `spawn_one` with `stateful=false`. The stateful restart route is a separate `restart_one` function that does not call `spawn_one` at all (which would mint a fresh key).

## The gossip field

Each node now broadcasts `stateful: bool` in its `GossipDigest` — the gossip payload it sends every 2 seconds to all mesh members. Admin-ui's topology and heartbeats endpoints expose this field. The UI renders a badge on stateful nodes.

The field is appended at the end of `GossipDigest` with `#[serde(default)]`. Postcard (the binary serialization format for gossip) is positional — new fields must append, not insert. Old nodes deserializing a new digest with this field receive `stateful: false` via the default. The constraint is why there's a design comment about it in the code.

## Validating it

After a restart, you should see:

```bash
curl -s http://localhost:19090/api/topology | \
  jq '.nodes[] | select(.id=="<your_node_name>") | {node_id, stateful}'
# → {"node_id":"<same public key as before>","stateful":true}
```

In Jaeger, `rafka.ui.node.restart` carries `old_pid` and `new_pid` as attributes. The `node_id` attribute is the iroh public key — check that it matches the node's key before and after restart.

For the adversarial test of the no-wipe invariant: kill a stateful node via DELETE without restarting it. Wait 15 seconds (three reaper cycles). The data dir should still be present. Only an explicit operator-initiated wipe (manual `Remove-Item` or a fresh non-stateful spawn with the same name) cleans it up.

## Why this matters for the open-source substrate

Rafka v2 is a telemetry-first mesh substrate for services that need P2P coordination without a Kafka dependency. Stateful node restart is the first step toward treating mesh participants as pets rather than cattle when the workload demands it — without giving up the mesh's self-healing topology or the cryptographic identity that makes QUIC routing work.
