# Multi-mesh streaming substrate — architecture

A design for a horizontally-scaled, **multi-mesh / multi-region** streaming log system
(Kafka/Pulsar-class: topics, partitions, ordered durable logs) built on a **peer-to-peer QUIC mesh**
with **no central consensus service** (no KRaft, no etcd, no ZooKeeper). Multi-mesh is assumed
*always* — a deployment is two or more meshes (regions/zones) from day one; there is no single-mesh
mode.

This is a design overview, not a single implementation plan; it decomposes into the initiatives in
§13. Terminology is deliberate (these words are routinely conflated):

- **ordering** — a record's position in a partition's log. *Not* consensus.
- **membership / agreement** — the mesh converging on who is alive and who holds what.
- **mesh** — one region/zone deployment, identified by a `mesh_id` like `east1.zone1`. *Not* a tenant
  boundary.
- **relay** — transport-layer connectivity (NAT traversal). A server, *not* a node, *not* a router of
  application data.
- **bridge** — (an anti-pattern we rejected) a dedicated cross-mesh data relay. Its job folds into the
  gateway; no bridge component exists.

Substrate assumptions (satisfied by a P2P QUIC transport plus a gossip-membership layer):
- Nodes are addressed by a **stable public key** ("node key"), not by hostname/IP.
- **Discovery** resolves a node key to current reachability (a decentralized DHT works; no server
  required).
- **Relay** provides NAT traversal and a connectivity fallback when a direct hole-punched path can't
  be formed. End-to-end encryption means relays carry only ciphertext.
- **Gossip** provides eventually-consistent membership and small-payload dissemination within a mesh.

---

## 1. Goals

- Kafka/Pulsar-class streaming over a P2P QUIC mesh.
- **No central consensus service** — no single point of failure, no metadata bottleneck.
- **Multi-mesh always** — every deployment spans multiple meshes (regions/zones); cross-mesh is a
  first-class, ever-present concern, not an add-on.
- A node knows its type, boots, and joins its mesh "available for work."
- One mechanism everywhere; environments differ only in configuration values, never in behavior.

---

## 2. Node roles

| role | responsibility | state |
|---|---|---|
| **gateway** | write authority: sequences the log (ordering), fans a write to all replicas (replication), ships cross-mesh. Owns no partition permanently. | in-memory, durably backed by a **journal topic** stored on storage nodes; persists nothing locally → recoverable by replaying the journal |
| **storage node** ("broker") | dumb I/O pump: append to its write-ahead log (WAL), serve reads, **fence** writes for partitions it isn't a replica of. No peer-to-peer replication, no ordering logic. | local WAL (the durable log) |
| **registry** | schema distribution; read-heavy, eventual consistency is fine. | schemas |
| **compute** | data-intensive jobs (e.g. topic migration / re-partitioning). | job-local |
| **monitor** | observer + bootstrap rendezvous. Renders the whole system from the global topology cache. Not on the data path. | none (reads the cache) |

A "stateless gateway" means **no local persistence**: its per-partition write position (next offset,
replica progress, cross-mesh shipped-offset) lives in memory and is journaled to a topic on the
storage tier, so any gateway recovers or takes over by replaying that journal.

---

## 3. The write path

1. Producer → a gateway.
2. Gateway resolves the partition's replica set (N storage-node keys) from the **global topology cache**.
3. Gateway writes the record to **all N replicas simultaneously**.
4. Each storage node appends to its WAL — **WAL position = offset = order**. Ordering falls out of the
   append; storage nodes don't "decide" it.
5. Durable once a **local quorum** of replicas ack; then the producer is acked.

Storage nodes never replicate to one another. The gateway is the only smart component on the write
path. No partition "leader" is elected, so no consensus is required here: ordering is the WAL
position, durability is the local quorum of dumb storage nodes.

---

## 4. Durability & gateway recovery

- Gateway state is **journaled to a topic on the storage tier** — same ordering/replication machinery
  as data, so the journal is as durable as the log.
- On crash or takeover, a gateway **replays the journal topic** to reconstruct its position. Durable
  state lives *in the system*, not on a gateway's disk.
- **Journal ordering rule:** record intent *before* acting (journal "shipping P through offset X" →
  then ship). On recovery you may re-do the last step → at-least-once → **dedup by offset** at the
  destination → no loss. Never act-then-journal (risks losing acted-but-unjournaled work).

---

## 5. Cross-mesh replication (no bridge component)

Cross-mesh replication is **gateway-direct and asynchronous**:

- Local replicas satisfy the durability quorum and ack the producer; the cross-mesh copy is **shipped
  without blocking**. You cannot put WAN round-trips on the write path, and a mesh partition must not
  stall writes.
- Reliability comes from the **journal**: the gateway ships from the committed log and records a
  shipped-through offset, so after any crash it resumes and misses nothing (at-least-once + dedup).
- A gateway in mesh A ships to a storage node in mesh B **by node key** (resolved via the global cache;
  connectivity via discovery + relay). No flat network or VPC peering is required.
- **There is no bridge component.** Both data *and* the global topology cache cross meshes via this
  same gateway-direct mechanism; the cache is just an internal, geo-replicated topic.

### Cross-mesh egress concentration — soft leader

A per-mesh **gossip-elected leader** (a role on a gateway) may concentrate cross-mesh egress. This is
safe with *soft* (eventually-consistent) election **only because egress is idempotent**: a brief
two-leader window causes duplicate cross-mesh sends, which dedup by offset — harmless. **Guardrail:**
only split-brain-*benign* (idempotent) work runs on a soft-elected leader. Anything that *corrupts*
under two leaders (e.g. exclusive placement assignment) requires fencing/quorum, not soft election.

---

## 6. Connectivity — discovery + relay (the cross-mesh foundation)

Because multi-mesh is always-on, **key-based connectivity is foundational**, not optional:

- **Discovery** — nodes publish and resolve `node key → reachability`. A **decentralized DHT** is
  preferred (no server, no SPOF); a DNS-style discovery service or local mDNS are alternatives. This
  replaces hardcoded IP peer lists with "resolve a peer by its key."
- **Relay** — transport-layer NAT traversal and fallback. Self-hosted, **≥2 per region across
  providers**, with automatic client failover → not a SPOF. Relay is a *fallback* (a direct
  hole-punched path is preferred), so steady-state cost is bounded; encryption means relays see only
  ciphertext. A relay is a **server addressed by URL — not a node** in the mesh; it is the one piece of
  dedicated infrastructure the system requires.

Discovery (the directory: *who/where*) and relay (the pipe: *reach them*) are distinct layers and do
not overlap.

---

## 7. Global topology cache — the control plane

The cache is **mandatory** and is the heart of the system: a gateway cannot ship to a remote storage
node unless it knows that node exists, its key, and that it is a replica for the partition. That
knowledge *is* the cache.

- **Contents:** a name/placement directory (logical name `region.zone.type.N` + partition → replica
  set → node key) **and** observability (per-node status / load).
- **Maintained and gossiped by the gateways**, propagated cross-mesh as an internal geo-replicated
  topic. It is the **control-plane metadata** — the role a consensus store plays in other systems —
  realized here as a gossiped cache rather than a strongly-consistent store.
- **Name → key directory:** logical names resolve to node keys here; discovery + relay turn a key into
  a connection. (Cache = directory; relay = connectivity; they don't overlap.)
- **Two readers:** gateways (routing/replication) and the monitor (display). Same artifact.

### Two consistency tiers (the key discipline)

- **Observability** (status / load) → eventual consistency is free; stale-by-seconds is fine.
- **Placement** (partition → replica set) → the write-routing truth. Stale placement on the write path
  risks misrouting → lost/misplaced data. **Safe pattern: the storage node fences** — it rejects
  writes for partitions it isn't a replica of, and the gateway **refreshes the cache and retries** on
  reject. A stale placement read then costs at most a reject+refresh, never silent loss. *Fence
  placement at the storage node; let observability stay loose.*

---

## 8. Multi-tenancy

- **Default: many tenants share a mesh.** Isolation is **logical** — `tenant_id` scoping (every
  record/digest namespaced by tenant). (Authorization/ACLs are a separate concern, not the tenancy
  mechanism.)
- **Dedicated mesh per tenant** — physical isolation via the mesh boundary — is an option for tenants
  that require it.
- Cross-tenant federation, if needed, rides the same gateway-direct cross-mesh path; rare and
  policy-gated.

---

## 9. Bootstrap

- Nodes connect **by node key**: each binds an **ephemeral port**, publishes its address to discovery,
  and is reached by others resolving its key (discovery + relay/hole-punch). There are **no fixed
  ports and no per-node port assignment** — a node's local port is invisible to the mesh.
- Joining requires only well-known **entry points** — DHT bootstrap nodes and/or relay URLs, supplied
  as configuration — *not* a list of peer addresses. This is the single bootstrap mechanism across all
  meshes and environments; only the entry-point values differ.
- The monitor is a **pure observer**. Discovery (not a designated seed node) is what lets peers find
  each other, so no single node sits on a bootstrap-critical or data path.
- HA: provide multiple discovery entry points and ≥2 relays. The *running* mesh survives the loss of
  any one; only *new* joins depend on at least one entry point being reachable.

---

## 10. Single-pane monitoring

Requirement: **one view of the entire system**, not one view per mesh.

- The monitor **consumes the global topology cache.** Because the cache is replicated into every mesh,
  **any** monitor reads its *local* copy and renders the *whole* system — it needs **zero cross-mesh
  connectivity of its own**.
- Run **one monitor per mesh** for locality/HA; each shows the global view → **no monitoring SPOF**.
- Build-time decisions: what is propagated globally vs. drill-down (don't flood the WAN with every
  node's high-frequency load metrics — propagate membership/placement/status globally, sample or
  drill-down for fine-grained metrics); label remote-mesh data with its as-of age (async lag); region
  as a first-class field in cache and UI.

---

## 11. How "no consensus service" is satisfied

| need | mechanism | strength |
|---|---|---|
| node liveness / membership | gossip | soft / eventual |
| log ordering | WAL append position (gateway-sequenced) | exact, local |
| durability | local quorum of dumb storage nodes | quorum |
| cross-mesh egress role | soft gossip-elected leader | soft (idempotent work only) |
| placement correctness on write | **storage node fences non-replica writes** | hard, local to the replica quorum |
| topology / placement directory | gossiped global cache | eventual + fence-on-use |

No KRaft, no etcd. The only "hard" agreement is the storage-side fence on the write path, local to a
partition's replica quorum — not a global service.

---

## 12. Open questions

- **Gateway → partition routing:** what pins a partition's writes to a single gateway at a time
  (consistent hash over membership? per-producer affinity?).
- **Placement-change protocol:** how a partition's replica set changes safely (add/remove replica,
  rebalance) — the "corrupting under split-brain" case that needs fencing/quorum, not soft gossip.
- **Cache schema:** exact contents; the global-vs-drill-down split for metrics.
- **Discovery choice:** decentralized DHT vs. a self-hosted discovery service.
- **Relay operations:** count, placement, self-hosting model.

---

## 13. Decomposition (sequencing)

Each is its own design → plan → implementation cycle:

1. **Connectivity foundation** — discovery (DHT) + relay (self-hosted, ≥2 per region), key-based
   connect. Multi-mesh is always-on, so this is foundational. Validate with a forced-no-direct-path
   testbed (network-isolated node groups + a shared relay; confirm cross-group connect-by-key is
   relay-carried).
2. **Global topology cache** — the control-plane directory + observability, gossiped by gateways,
   cross-mesh propagated, with the two consistency tiers and storage-side placement fencing.
3. **Gateway write path** — ordering (WAL position), simultaneous replication to N storage nodes,
   local-quorum durability, journal-topic state + recovery.
4. **Cross-mesh replication** — gateway-direct async shipping, journal-tracked shipped-offset, dedup;
   soft-leader egress.
5. **Single-pane monitoring** — consume the global cache; region + staleness in the UI.
