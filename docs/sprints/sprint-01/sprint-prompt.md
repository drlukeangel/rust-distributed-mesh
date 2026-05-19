# Sprint-01 — Mesh Substrate Spike + Day-1 UI + Day-1 rfa CLI

Self-contained brief. Read the linked files; do not infer from prior session memory.

## Sprint identity

- **Sprint config:** `docs/sprints/sprint-01/sprint-config.json` (currently `planned`; flip to `open` + set `opens` to today's date in your first commit).
- **Sprint PRD:** `docs/sprints/sprint-01/sprint-prd.md` (the north-star PRD for this initiative).
- **Branch:** create `sprint-01-substrate-spike` off `main`. Push directly to origin; no PR.
- **Initiative:** `mesh-v1` Phase 0. The first sprint of a fresh-substrate rebuild.

## No PR — push to branch + report

Push commits directly to `origin/sprint-01-substrate-spike`. Final commit flips `sprint-config.json` status to `closed` + sets `closes` date. Reply to team-lead with branch tip SHA + GREEN evidence + the OTLP artifact path.

## What's customer-observable

A developer clones the repo, runs `cargo run -p rafka-topology-ui`, opens `http://localhost:19090` in a browser, sees an empty topology graph with four "Spawn <type>" buttons. They click each button in turn (or run `rfa mesh node add --type <type>` from the CLI). Within 5 seconds of each click, a new node appears on the graph with edges to existing peers. They kill any node externally; the graph updates within 10 seconds. They restart the killed node with the same identity; it rejoins immediately.

The invariant: every state change visible in the UI is also queryable via `rfa mesh topology show` and `rfa mesh node describe`. UI and CLI are equivalent control surfaces.

## Required pre-reads

1. **`docs/sprints/sprint-01/sprint-prd.md`** — the north-star PRD. Read first.
2. **`docs/plans/mesh-v1/01-substrate-prd.md`** — iroh substrate selection, 14 substrate spans, boot sequence per node.
3. **`docs/plans/mesh-v1/02-topology-ui-prd.md`** — UI requirements. Plain HTML+JS, no SPA framework, no `node_modules`. UI joins mesh as a view-only node.
4. **`docs/plans/mesh-v1/03-rfa-cli-prd.md`** — `rfa` CLI commands. Thin REST client targeting the topology-ui process (single control surface).
5. **`docs/plans/mesh-v1/06-decisions.md`** — 18 locked decisions. Especially D-001 (custom QUIC retired), D-002 (iroh default), D-007 (UI is a mesh participant), D-008 (no SPA framework), D-009 (CLI calls UI backend), D-017 (compute zero HTTP).
6. **`iroh` docs** (https://docs.rs/iroh) — `Endpoint`, `EndpointId`, discovery providers (mdns + dns), ALPN registration, bidirectional streams.

## What's already on main

- Empty `crates/`, `gateway/`, `broker/`, `compute/`, `schema/` workspace skeleton.
- `crates/rafka-mesh-ops/` — `InternalMeshFrame` codec crate (copied from main rafka repo; shape per `03-wire-and-mesh.md §3.1`).
- `MeshTransport` trait in `crates/rafka-mesh-transport/` — currently has no impl. Your job to add `IrohMeshTransport`.

## Implementation surface (sketch)

```
crates/
├── rafka-mesh-ops/           # codec (exists)
├── rafka-mesh-transport/     # MeshTransport trait + IrohMeshTransport (NEW impl)
├── rfa/                      # CLI binary (NEW)
gateway/                      # bare gateway node (mesh participant, zero app logic)
broker/                       # bare broker node
compute/                      # bare compute node (zero HTTP per principle #6)
schema/                       # bare schema node
topology-ui/                  # web UI binary (NEW)
deployment/dev/
└── docker-compose.otlp.yml   # Jaeger + OTLP collector for span capture
```

Each node binary at minimum:
1. Loads/mints identity to `$RAFKA_DATA_DIR/node-identity.json`
2. Creates `iroh::Endpoint` bound to `$RAFKA_NODE_BIND_ADDR` (default `0.0.0.0:0`)
3. Registers ALPN `rafka-mesh-v1`
4. Starts gossip membership (subscribe to peer arrivals/departures)
5. Starts `InternalMeshFrame` accept loop
6. Emits all 14 substrate spans on appropriate events

Topology UI:
- Joins mesh on ALPN `rafka-topology-v1`
- Subscribes to gossip + span stream from peers
- Serves single-page HTML + WebSocket at `:19090`
- REST endpoints for spawn/kill that fork subprocesses

rfa CLI:
- Thin HTTP client to `http://localhost:19090`
- Every command supports `--format json`

## Exit criteria

See `sprint-config.json::exit_criteria` for the full list. Headline gates:

1. 4 node binaries boot on iroh and join a mesh
2. Topology UI shows all 4 node types via spawn buttons
3. CLI replicates every UI op
4. 14 substrate spans land in OTLP artifacts
5. Workspace `cargo check` = zero/zero
6. Zero hand-rolled mesh primitives (grep test)
7. Zero custom QUIC code (grep test)

## Discipline (CLAUDE.md mandates)

- Sonnet only for any subagent dispatch (set model explicitly)
- No defer language — full scope in one merge
- OTLP artifact evidence before sprint close
- stash → pull --rebase → commit → push between fix batches
- No Claude attribution in commit messages
- No `cargo clean` as debugging shortcut
- Workspace gate zero/zero before every commit
- 80/20 verify your own work via git diff before claiming done
- One task per subagent if you spawn any (you likely don't need to for a 2-week sprint)
- No HTTP routes on any node binary except `rafka-topology-ui`

## Out-of-band acceptance test (the user will run this)

```bash
cargo run -p rafka-topology-ui &
sleep 3
open http://localhost:19090
# Click spawn buttons for each type
# Verify graph updates real-time

rfa mesh node add --type gateway --name gw-1
rfa mesh node add --type broker --name br-1
rfa mesh node add --type compute --name cp-1
rfa mesh node add --type schema --name sc-1
rfa mesh wait-converged --timeout 30s
rfa mesh topology show --format dot | dot -Tpng > topology.png

# Kill a node, verify topology updates
rfa mesh node remove br-1

# Verify spans landed
ls tests/artifacts/mesh-substrate/*.spans.jsonl
for span in rafka.mesh.node.started rafka.mesh.peer.connected rafka.mesh.peer.staleness_timeout; do
  count=$(grep -l "$span" tests/artifacts/mesh-substrate/*.jsonl | wc -l)
  echo "$span : $count files"
done
```

All 14 spans must show count ≥1.

## When done

1. Commit final state with `sprint-config.json` status flipped to `closed`
2. Push to `origin/sprint-01-substrate-spike`
3. SendMessage to team-lead with: branch tip SHA, OTLP artifact path, all-green-criteria summary
4. Stand by for audit — team-lead will independently verify before merging to main
