# mesh-to-mesh — how-to

> **Status:** PLANNED. This how-to describes the intended sprint-13 surface; commands don't exist yet.

## Plan two meshes

Each mesh needs its own:
- `mesh_id` (UUID, set via `RAFKA_MESH_ID` env var at every node's boot)
- Distinct mdns scope OR distinct seed list (so nodes don't auto-discover across meshes)

## Boot two meshes

```bash
# Mesh A
RAFKA_MESH_ID=mesh-a RAFKA_DATA_DIR=./data/a-gw cargo run -p rafka-gateway &
RAFKA_MESH_ID=mesh-a RAFKA_DATA_DIR=./data/a-br cargo run -p rafka-broker &

# Mesh B
RAFKA_MESH_ID=mesh-b RAFKA_DATA_DIR=./data/b-gw cargo run -p rafka-gateway &
RAFKA_MESH_ID=mesh-b RAFKA_DATA_DIR=./data/b-br cargo run -p rafka-broker &
```

## Launch a bridge gateway joining both

```bash
RAFKA_MESH_ID=mesh-bridge \
RAFKA_BRIDGE_MESHES=mesh-a,mesh-b \
RAFKA_SEED_NODES_MESH_A=<a-gw-node-id>@<a-gw-addr> \
RAFKA_SEED_NODES_MESH_B=<b-gw-node-id>@<b-gw-addr> \
cargo run -p rafka-gateway
```

## Verify cross-mesh connection in Jaeger

```bash
# Bridge emits one peer.connected per peer in each mesh
http://localhost:16686/search?service=gateway&operation=rafka.mesh.cross.peer_connected&lookback=15m
```

## View both meshes in topology-ui

```
http://localhost:19090
# (sprint-14) two-pane view, one per mesh, with bridge node spanning both
```

## Chaos across meshes

```bash
# (sprint-14 chaos extension) — kill an entire mesh's gateway, verify other mesh continues
rfa mesh chaos kill-mesh mesh-a --target gateway
```
