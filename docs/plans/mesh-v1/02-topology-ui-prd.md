# PRD — Topology UI (Day-1 Mandatory)

**Status:** Open
**Companion to:** `00-mesh-rebuild-prd.md`
**Ships in:** Sprint 0 (substrate spike); no deferral.

---

## 1. The mandate

From day 1, operators must SEE the mesh. The topology UI is not a "nice to have" — it is the proof that the substrate works. If the substrate is built without the UI, we are repeating the past mistake (mesh as black-box opaque substrate that nobody can directly observe).

## 2. What ships in Sprint 0

A single-binary web UI process (`rafka-topology-ui`) bound to `http://localhost:19090` that:

1. **Renders a live topology graph.** Nodes drawn by `EndpointId`, colored by `node_type` (gateway / broker / compute / registry). Edges drawn between actively-connected peers.
2. **Updates in real-time** as nodes join/leave/disconnect — sub-second latency between mesh event and UI redraw.
3. **Exposes a "Node Actions" panel:**
   - "Spawn node" — launches a new node process of the selected type
   - "Kill node" — terminates the selected node's process
   - "Inject chaos" — partition pair, flap link, etc. (Sprint 1 deliverable)
4. **Shows per-node detail:**
   - Identity (`EndpointId`)
   - Node type
   - Started-at timestamp
   - Connected peers (list of `EndpointId` + last-seen-ms)
   - Recent span emissions (rolling window of the 14 substrate spans)
   - Process resource usage (CPU, RSS) — optional, time-permitting

## 3. Technical shape

**Stack:** axum for the HTTP server + WebSocket. **No** SPA framework. Plain HTML + minimal vanilla JS + a graph rendering library (`vis-network` or `cytoscape.js`) for the topology view. Cheap, debuggable, no node_modules.

**Architecture:**

```
                                ┌──────────────────────┐
                                │  Browser             │
                                │  - vis-network graph │
                                │  - WebSocket client  │
                                │  - Action buttons    │
                                └──────────┬───────────┘
                                           │ WebSocket (ws://localhost:19090/topology)
                                           ▼
                                ┌──────────────────────┐
                                │  rafka-topology-ui   │
                                │  - axum HTTP+WS      │
                                │  - subprocess mgmt   │
                                │  - iroh mesh client  │  ◄── joins mesh as a "view-only" node
                                └──────────┬───────────┘
                                           │ subscribes to gossip + span stream
                                           ▼
                                  ┌────────────────┐
                                  │  Mesh (iroh)   │
                                  │  N nodes       │
                                  └────────────────┘
```

`rafka-topology-ui` is itself a mesh participant — it joins as a view-only node (special ALPN `rafka-topology-v1` so it doesn't participate in `InternalMeshFrame` traffic) and subscribes to:
- Gossip membership events
- The OTLP span stream from each peer (subscribe to a debug ALPN that mirrors local spans)

It re-emits these as JSON messages over the WebSocket to the browser.

## 4. Browser-side rendering

**Graph view:**
- Nodes are circles. Color by type: gateway=blue, broker=green, compute=orange, schema=purple.
- Edges are lines between actively-connected peers. Solid = healthy, dashed = stale (gossip timeout pending), red = recently-failed.
- Hover over a node → side panel populates with detail (see §2).
- Click "Spawn Node" → modal asks for type → POSTs to `/api/nodes/spawn` → backend launches subprocess.

**Span timeline:**
- A sliding 30-second window at the bottom of the page shows recent span emissions.
- Color-coded by span name. Click a span to see its full attributes.

**Real-time updates:**
- WebSocket pushes deltas (`node_added`, `node_removed`, `edge_changed`, `span_emitted`).
- Browser applies deltas to the graph without redraw flash.

## 5. REST API (backend → frontend)

```
GET  /                           # serves the HTML page
GET  /static/<file>              # serves vanilla JS + vis-network + CSS
WS   /topology                   # WebSocket: server-pushed delta stream
POST /api/nodes/spawn            # { node_type } → launches subprocess
DELETE /api/nodes/{endpoint_id}  # terminates the subprocess
POST /api/chaos/partition        # Sprint 1: partition a pair
POST /api/chaos/flap             # Sprint 1: flap a link
GET  /api/nodes                  # current node list
GET  /api/nodes/{endpoint_id}/spans?since=...  # recent spans for a node
```

## 6. Subprocess management

`rafka-topology-ui` spawns/kills node subprocesses using `tokio::process::Command`. Each spawned node:
- Inherits `RAFKA_OTLP_ENDPOINT` so its spans flow back via the OTLP collector
- Gets a unique `RAFKA_DATA_DIR` under `${TOPOLOGY_UI_WORK_DIR}/nodes/<endpoint_id>/`
- Logs to `${TOPOLOGY_UI_WORK_DIR}/nodes/<endpoint_id>/stdout.log`

This is a developer-experience tool. Production deployment uses k8s / systemd / docker-compose — the topology UI is for local development and chaos testing, not production node mgmt.

## 7. Acceptance criteria (Sprint 0)

1. `cargo run -p rafka-topology-ui` starts the UI process on `http://localhost:19090`
2. Open browser to that URL: see an empty graph + "Spawn Node" buttons for each type
3. Click "Spawn gateway": within 5s, see a blue circle appear in the graph
4. Click "Spawn broker": within 5s, see a green circle appear, with an edge to the gateway
5. Click "Spawn compute": within 5s, see an orange circle appear, edges to both
6. Click a node → kill it: within 10s, see the circle disappear and edges removed
7. Span timeline shows substrate span emissions in real-time (≤1s delay)
8. No node_modules, no transpilation step, no SPA framework — pure HTML+JS

## 8. Non-goals (Sprint 0)

- **No multi-page UI.** One page, one purpose: see the mesh.
- **No persistent state.** Refresh = re-subscribe; topology rebuilds from current gossip view.
- **No authentication.** Localhost dev tool. Not exposed externally.
- **No editing of node config beyond spawn/kill.** Detail panel is read-only.
- **No multi-mesh visualization in Sprint 0.** Just one mesh. Sprint 2 adds multi-mesh view.

## 9. Sprint 1 extensions (chaos panel)

After Sprint 0 ships:

- "Partition" button: select two nodes → temporarily block traffic between them (chaos harness API)
- "Flap link" button: select an edge → repeatedly disconnect/reconnect every N seconds
- "NAT shift" button: select a node → restart its endpoint on a new ephemeral port (simulating mobile carrier handoff)
- "Soak run" button: start the 24h chaos battery, watch the graph thrash in real-time

## 10. Sprint 2 extensions (multi-mesh)

- Graph splits into multi-mesh panels (one per mesh)
- Relay servers drawn as a separate node-type tier
- Cross-mesh edges drawn differently from intra-mesh edges
- "Peer meshes" button to manually trigger cross-mesh relay establishment

## 11. Why "no SPA framework" is the right call

Per Golden Principle #10 (KISS): the UI is a debugging surface, not a product. Plain HTML + vanilla JS + one graph library has zero build pipeline, no node_modules, no transpilation, no framework upgrades. A new engineer can debug the UI by opening DevTools. If the UI becomes a product later, it gets its own initiative; for THIS initiative it's a dev tool.

## 12. Why the UI is a mesh participant (not a polling client)

If the UI polled REST endpoints on each node, it would itself become a substrate consumer with its own discovery problem ("which nodes do I know about?"). Joining the mesh as a view-only node:

- Discovery is automatic via gossip
- Span subscription is real-time via stream
- Adding new node types doesn't require UI config updates
- Killing the UI doesn't affect mesh state

It also exercises the substrate by being a real client — if the UI can't join, the substrate failed.
