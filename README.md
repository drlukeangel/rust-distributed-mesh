# rust-distributed-mesh (RDM)

RDM is a distributed runtime for operating a [fabric](docs/architecture/fabric-mesh-node-lifecycles.md)
of [meshes](docs/architecture/lifecycles/mesh-create.md) of [nodes](docs/architecture/node-identity-and-state.md)
over one [iroh](https://iroh.computer) QUIC network:

- **A [fabric](docs/architecture/fabric-mesh-node-lifecycles.md)** is the whole deployment. It is
  divided into one or more **[meshes](docs/architecture/lifecycles/mesh-create.md)**, and each mesh
  runs application **[nodes](docs/architecture/node-identity-and-state.md)** alongside
  **[node-admins](docs/architecture/node-admin-lifecycle.md)**.
- **[Node-admins](docs/architecture/node-admin-lifecycle.md)** run the control plane. One elected
  **[fabric-primary](docs/architecture/fabric-primary.md)** accepts topology changes, and each mesh's
  elected **[mesh-primary](docs/architecture/fabric-node-lifecycle-elections.md)** launches, monitors,
  and recovers the nodes in its mesh through a
  **[deployment provider](docs/architecture/deployment-provider.md)** (`process` or `container`).
- **A [Build](docs/architecture/build.md)** is the unit of topology change and self-healing execution.
  Creating a mesh, adding or removing a node, replacing a node's executable, or recovering an exited
  node runs as an ordered sequence of [Build](docs/architecture/build.md) steps with durable step
  events.
- **[Gossip](docs/architecture/gossip.md) and [Node RPC](docs/architecture/node-rpc-envelope.md)**
  connect the fleet: nodes publish [topology](docs/architecture/gossip.md) digests and
  discover peers over `iroh-gossip`, and call one another over authenticated, framed
  [Node RPC](docs/architecture/node-rpc.md) streams admitted by
  [fabric certificates](docs/architecture/fabric-certs.md).

## Who RDM is for

RDM is for any Rust application that runs a fleet of nodes and needs self-healing process or
container placement, live topology views, [elections](docs/architecture/fabric-node-lifecycle-elections.md)
for its own node kinds, and typed [Node RPC](docs/architecture/node-rpc-envelope.md) between its
processes. RDM owns the generic Fabric, Mesh, and Node lifecycle; your application registers its own
node kinds, [lifecycle hooks](docs/architecture/fabric-node-hooks.md), and application operations on
top (see [`demo/`](demo/) for a complete standalone application with `compute`, `gateway`, and
`broker` node kinds).

## Prerequisites

To build and run RDM on Linux:

- **Rust toolchain**: `stable` (pinned in [`rust-toolchain.toml`](rust-toolchain.toml)) and a C/C++
  compiler (`cc`, `c++`).
- **`sccache` and `mold`**: configured in [`.cargo/config.toml`](.cargo/config.toml) via
  `scripts/rustc-wrapper.sh`, `scripts/cc-wrapper.sh`, and `scripts/c++-wrapper.sh` to cache
  dependency compilation and speed up linking. When either tool is absent from `PATH`, the wrappers
  fall back to plain `rustc`, `cc`, `c++`, and the system linker (printing a one-line notice); the
  full verification gate (`scripts/rdm-gate-at.sh`) requires both.
- **Docker**: required only when running a fabric with the `container`
  [deployment provider](docs/architecture/deployment-provider.md) (`MESH_SPAWN_TYPE=container`). The
  default `process` provider (`MESH_SPAWN_TYPE=process`) runs nodes as local OS processes and needs
  no container runtime.

## 5-minute quickstart

Build the Day-0 [`rdm-node-admin`](docs/architecture/node-admin-lifecycle.md) binary and the
[`rdm-rpc-probe`](docs/architecture/node-rpc-envelope.md) client:

```bash
cargo build -p rdm-admin --bin rdm-node-admin -p rdm-test --bin rdm-rpc-probe
```

Start a Day-0 [node-admin](docs/architecture/node-admin-lifecycle.md) (path `mesh1.node-admin`, name
`1`), wait for it to print its birth record (`RDM_NODE_BIRTH`), and read its live
[topology](docs/architecture/lifecycles/node-topology-get.md) over
[Node RPC](docs/architecture/node-rpc-envelope.md). The birth line comes a moment before the admin
answers its first read, so the read is asked again until it answers:

```bash
ROOT=$(mktemp -d)
mkdir -p "$ROOT/store" "$ROOT/data"
MESH_SPAWN_TYPE=process RDM_FABRIC=fabric-quick RDM_MESH=mesh1 \
  RDM_STORE_DIR="$ROOT/store" RDM_DATA_DIR="$ROOT/data" \
  RDM_NODE_ADMIN_API_BIND=127.0.0.1:0 \
  ./target/debug/rdm-node-admin > "$ROOT/admin.log" 2>&1 &
PID=$!
for _ in $(seq 1 40); do
  grep -q '^RDM_NODE_BIRTH=' "$ROOT/admin.log" 2>/dev/null && break
  sleep 0.25
done
grep '^RDM_NODE_' "$ROOT/admin.log"
BIRTH=$(grep '^RDM_NODE_BIRTH=' "$ROOT/admin.log" | cut -d= -f2-)
for _ in $(seq 1 40); do
  OUT=$(./target/debug/rdm-rpc-probe --admin-birth "$BIRTH" topology \
    --target 'path:{"path":"mesh1.node-admin","name":"1"}' 2>/dev/null) && break
  sleep 0.25
done
echo "$OUT"
kill "$PID" && wait "$PID" 2>/dev/null || true
rm -rf "$ROOT"
```

Output:

```text
RDM_NODE_BIRTH={"mesh":"mesh1","type":"node-admin","node_id":"9zfcbqgbc2ba","path":"mesh1.node-admin","name":"1","endpoint_id":"7e943b8a3bd274289a2bf25c8c4370d6d8cb28e3227b517e715d782c8a073d6e","transport_addr":"127.0.0.1:52291","incarnation":"zywaemhqgv1w","supersedes":null}
{"frames":[{"detail":"Started","frame":"started"},{"chunk_count":1,"chunk_index":0,"frame":"snapshot","members":["9zfcbqgbc2ba"],"mesh":"mesh1","statuses":["ready"],"topology_version":1},{"detail":"Primaries { kind: MeshPrimary, primary: Primary { mesh: \"mesh1\", node_id: NodeId(\"9zfcbqgbc2ba\"), incarnation: IncarnationId(\"zywaemhqgv1w\"), revision: 1 }, gone: false }","frame":"primaries"},{"detail":"Primaries { kind: FabricPrimary, primary: Primary { mesh: \"mesh1\", node_id: NodeId(\"9zfcbqgbc2ba\"), incarnation: IncarnationId(\"zywaemhqgv1w\"), revision: 1 }, gone: false }","frame":"primaries"},{"detail":"RafkaTime { ms: 1794121312305, kind: Some(FabricPrimary) }","frame":"rafka-time"},{"frame":"end","meshes":1}],"outcome":"Reply"}
```

## Where next

- **[Introduction](docs/introduction.md)** — how the documentation and architecture contract are
  organized.
- **[Getting started](docs/getting-started.md)** — step-by-step tutorial that brings up a two-mesh,
  20-node demo estate (`scripts/demo-estate.sh up`), runs an application operation
  ([`demo/src/consumer_example.rs`](demo/src/consumer_example.rs)), and executes a
  [Build](docs/architecture/build.md).
- **[Glossary](docs/glossary.md)** — every RDM term defined and linked to the architecture page that
  owns it.
- **[Building an application on RDM](docs/eng/consumer-guide.md)** — how to wire node kinds,
  [hooks](docs/architecture/fabric-node-hooks.md), [connections](docs/architecture/connections.md),
  and [queries](docs/architecture/query.md).
- **[Your own ops on Node RPC](docs/eng/consumer-guide-ops.md)** — how to define, register, and
  recover an application operation over [Node RPC](docs/architecture/node-rpc-envelope.md).
- **[Runnable examples (`demo/examples/`)](demo/examples/README.md)** — small, single-feature Rust
  programs that run against a live local estate.
- **Local documentation site** — run `scripts/docs-site.sh` to build the guide and API documentation
  (`rustdoc`) into `target/docs-site/index.html`.
- **[Contributing](CONTRIBUTING.md)** — how to build, run tests, check vocabulary, and navigate the
  repository layout.
