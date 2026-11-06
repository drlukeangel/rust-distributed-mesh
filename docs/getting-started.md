# Getting started

This tutorial walks from a clean checkout to a running two-[mesh](architecture/fabric-mesh-node-lifecycles.md)
estate (`scripts/demo-estate.sh up`), runs one application op (`demo/src/consumer_example.rs`,
documented in [Your own ops on Node RPC](eng/consumer-guide-ops.md)) on
[Node RPC](architecture/node-rpc.md), inspects a [Build](architecture/build.md), and tears the
[fabric](architecture/fabric-mesh-node-lifecycles.md) down.

## Prerequisites

Install the tools required by `.cargo/config.toml` and `rust-toolchain.toml`:

- **Rust `stable`** (the channel `rust-toolchain.toml` names; `rustup` installs it on first use)
- **`sccache`** and **`mold`** (`sudo apt-get install -y sccache mold clang`)
- **Node.js and `npm`** (to build the web asset bundle served by `rdm-admin-ui`)
- **Python 3** (used by `scripts/demo-estate.sh` to format JSON replies)

## 1. Build the demo workspace and the RPC probe

Build `rdm-rpc-probe` from the root workspace, build the web UI bundle in `demo/admin-ui/web`, and
build the binaries in `demo/`:

```bash
cargo build -p rdm-test --bin rdm-rpc-probe
npm --prefix demo/admin-ui/web ci && npm --prefix demo/admin-ui/web run build
cargo build --locked --manifest-path demo/Cargo.toml --workspace --bins
```

Output:

```text
   Compiling rdm-test v0.1.0 (crates/rdm-test)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 5.83s
added 154 packages, and audited 155 packages in 2s
✓ built in 1.18s
   Compiling rshape-consumer v0.1.0 (demo)
   Compiling rdm-admin-ui v0.1.0 (demo/admin-ui)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 14.37s
```

This builds the R-shape reference application (`rshape-node-admin`, `rshape-gateway`,
`rshape-broker`, `rshape-compute`, `rshape-example`) and the HTTP control client (`rdm-admin-ui`).

## 2. Start the two-mesh estate

Run `scripts/demo-estate.sh up`, pointing `RDM_DEMO_BIN` at the compiled `demo/target/debug`
directory:

```bash
RDM_DEMO_BIN=$PWD/demo/target/debug scripts/demo-estate.sh up
```

Output:

```text
fabric fabric-gg4wtw, meshes mesh-gg4wtw-1 and mesh-gg4wtw-2
day-0 admin up: rdm://h38q5jqafmd8 (pid 3612540)
UI up: http://127.0.0.1:46571
bootstrap: {"build_id":"f4q5rfcv0kxr"}
topology after 4s: 20 of 20 ready
ESTATE UP at target/demo-estate-20261106T172300; UI http://127.0.0.1:46571
```

What `scripts/demo-estate.sh up` did:

1. Started a Day-0 [`node-admin`](architecture/node-admin-lifecycle.md) process, which created the [fabric](architecture/lifecycles/fabric-create.md),
   the first [mesh](architecture/lifecycles/mesh-create.md) (`mesh-gg4wtw-1`), and its own
   [birth](architecture/node-identity-and-state.md) (`mesh-gg4wtw-1.admin.1`) as
   [fabric-primary](architecture/fabric-primary.md).
2. Started `rdm-admin-ui` attached to that Day-0 `node-admin`.
3. Submitted a bootstrap [Build](architecture/build.md) (`y4qny3jg6n45`) through the
   `process` [deployment provider](architecture/deployment-provider.md) that created the remaining
   19 [nodes](architecture/lifecycles/node-create.md) across `mesh-gg4wtw-1` and `mesh-gg4wtw-2`
   (3 `admin`, 2 `gateway`, 3 `broker`, and 2 `compute` nodes per mesh) and waited until all 20
   reached [`Ready`](architecture/lifecycles/states.md) in the [topology](architecture/gossip.md).

Verify the cross-mesh [Node RPC](architecture/node-rpc.md) stream between nodes in the two meshes:

```bash
RDM_DEMO_BIN=$PWD/demo/target/debug scripts/demo-estate.sh stream
```

Output:

```text
mesh-gg4wtw-1.compute.1 -> mesh-gg4wtw-2.broker.1: {'outcome': 'Reply', 'streamed': True, 'kind': 'direct', 'peer': {'name': '1', 'path': 'mesh-gg4wtw-2.broker'}, 'answered': 'echo y8vgq5wz8bkq: hello from mesh-gg4wtw-1.compute.1', 'reason': None, 'refused': None} trace acab52642013efdcb9b77f23d2528693
mesh-gg4wtw-1.compute.1 -> mesh-gg4wtw-2.gateway.2: {'outcome': 'Reply', 'streamed': True, 'kind': 'direct', 'peer': {'name': '2', 'path': 'mesh-gg4wtw-2.gateway'}, 'answered': 'echo yq5zcaz4jg77: hello again from mesh-gg4wtw-1.compute.1', 'reason': None, 'refused': None} trace f9284ce25804128e351a35c029cd8722
mesh-gg4wtw-2.compute.2 -> mesh-gg4wtw-1.broker.3: {'outcome': 'Reply', 'streamed': True, 'kind': 'direct', 'peer': {'name': '3', 'path': 'mesh-gg4wtw-1.broker'}, 'answered': 'echo 8q8k077st3m8: hello from mesh-gg4wtw-2.compute.2', 'reason': None, 'refused': None} trace c5b50025d8b1da3ae669a0e4cd43142b
mesh-gg4wtw-2.gateway.1 -> mesh-gg4wtw-1.compute.2: {'outcome': 'Reply', 'streamed': True, 'kind': 'direct', 'peer': {'name': '2', 'path': 'mesh-gg4wtw-1.compute'}, 'answered': 'echo yqp9rd9tyqch: hello from mesh-gg4wtw-2.gateway.1', 'reason': None, 'refused': None} trace 4482b6f85fb4cacaed0f77536a6e2e07
mesh-gg4wtw-1.gateway.1 -> mesh-gg4wtw-1.broker.2: {'outcome': 'Reply', 'streamed': True, 'kind': 'direct', 'peer': {'name': '2', 'path': 'mesh-gg4wtw-1.broker'}, 'answered': 'echo 6mfag8ebn67w: same mesh, gateway to broker', 'reason': None, 'refused': None} trace f2eca67a367884773321c13919900325
```

## 3. Run one application op and inspect one Build

`demo/src/consumer_example.rs` is the compiled reference for
[Your own ops on Node RPC](eng/consumer-guide-ops.md). It defines two product protocols, each at a code no
[op ledger](architecture/node-rpc-envelope.md#the-op-ledger) row holds:

- `ExampleWork` (`0x50`, `"example-work"`): a unary op where a `compute` node submits an operation
  by id (`WorkRequest::Submit`) or queries its state (`WorkRequest::Get`), and a `broker` node
  records it in its durable `OperationStore` (`operations.jsonl`) before acknowledging acceptance
  ([`pre_node_ready`](architecture/fabric-node-hooks.md) resumes any pending operations on restart).
- `ExampleRecords` (`0x51`, `"example-records"`): a server-streaming op that copies the serving
  `broker` node's operation record entries (`Started`, `Entry`, `Through`).

Run `scripts/demo-estate.sh example` to put nodes on the `rshape-example` executable at
`<mesh-2>.broker.3` and `<mesh-2>.compute.1`. A new executable is a new node
([build-deployment.md §3](architecture/build-deployment.md)): each node is deleted by one
[Build](architecture/build.md), then one Build creates a node of its type on the new executable at the
path and name the delete released:

```bash
RDM_DEMO_BIN=$PWD/demo/target/debug scripts/demo-estate.sh example
```

Output:

```text
delete path mesh-gg4wtw-2.broker, name 3: {"build_id":"vpbzkzk525a6"}
create a broker in mesh-gg4wtw-2 onto rshape-example: {"build_id":"xkfbzrnckdb8"}
delete path mesh-gg4wtw-2.compute, name 1: {"build_id":"4xwww1mb0v9z"}
create a compute in mesh-gg4wtw-2 onto rshape-example: {"build_id":"jfa90t7fsdsh"}
the example's lines (target rshape) are in each node's stderr.log under target/demo-estate-20261106T172300/<node id>/
```

Read the broker's Create whole with `build.get`, through the `build_get` example. Its steps are the
new node's [node.create](architecture/lifecycles/node-create.md), keyed `node.create:<node_id>` by the
node.id the fabric-primary minted for it when it accepted the Build:

```bash
ROOT="$(cat target/demo-estate.current)"
RDM_NODE_ADMIN_BIRTH="$(cat "$ROOT/birth.json")" demo/target/debug/examples/build_get xkfbzrnckdb8
```

Output (abbreviated: the `change` left out, and the first and last steps):

```json
{
  "build_id": "xkfbzrnckdb8",
  "current": false,
  "ended": "complete",
  "nodes": 20,
  "phase": "Complete",
  "time": 1793985062831,
  "steps": [
    {
      "mesh": "mesh-gg4wtw-2",
      "operation": "node.create:jptbe7p1v0z8",
      "state": "complete",
      "step": "AllocateIdentity"
    },
    {
      "mesh": "mesh-gg4wtw-2",
      "operation": "node.create:jptbe7p1v0z8",
      "state": "complete",
      "step": "Complete"
    }
  ]
}
```

Now read the `rshape` log lines emitted by the serving broker (`mesh-gg4wtw-2.broker.3`) and the
calling compute node (`mesh-gg4wtw-2.compute.1`) as they execute `ExampleWork` (`0x50`) and
`ExampleRecords` (`0x51`):

```bash
grep -h "example-work" "$ROOT"/*/stderr.log | sed 's/\x1b\[[0-9;]*m//g' | tail -n 2
grep -hE "an example call answered|a serving node's record copied" "$ROOT"/*/stderr.log | sed 's/\x1b\[[0-9;]*m//g' | tail -n 2
```

Output:

```text
2026-11-06T17:25:34.710315Z  INFO rdm.node_rpc.request.serve.via-direct{protocol="example-work" op=80 peer=1b25368a7596e6b37902f99204ff747df02c0d7f68a3433dab00027ca723dabe caller_system="rdm"}: rshape: an operation recorded, then accepted; its work started operation=mesh-gg4wtw-2.compute-1-4t9xkqvm2r63-000033
2026-11-06T17:25:34.713591Z  INFO rdm.node_rpc.request.serve.via-direct{protocol="example-work" op=80 peer=1b25368a7596e6b37902f99204ff747df02c0d7f68a3433dab00027ca723dabe caller_system="rdm" code="OK" detail="accepted"}: rshape: an operation's work completed and its completion is recorded operation=mesh-gg4wtw-2.compute-1-4t9xkqvm2r63-000031
```

```text
2026-11-06T17:25:34.711248Z  INFO rshape: an example call answered call="submit" operation=mesh-gg4wtw-2.compute-1-4t9xkqvm2r63-000033 path=mesh-gg4wtw-2.broker name=3 outcome="Reply" detail="accepted" code=Ok
2026-11-06T17:25:34.726746Z  INFO rshape: a serving node's record copied path=mesh-gg4wtw-2.broker name=3 from=61 next=64 ended=Ok(64)
```

While the estate is up, you can also run any standalone example under `demo/examples/`:

```bash
RDM_NODE_ADMIN_BIRTH="$(cat "$ROOT/birth.json")" cargo run --locked --manifest-path demo/Cargo.toml --example fabric_atlas_read
```

## 4. Tear down the estate

Stop the estate with [`fabric.delete`](architecture/lifecycles/fabric-delete.md) and [`node.delete`](architecture/lifecycles/node-delete.md) on the Day-0 `node-admin`:

```bash
RDM_DEMO_BIN=$PWD/demo/target/debug scripts/demo-estate.sh down
```

Output:

```text
fabric.delete: {'outcome': 'Reply', 'terminal': 'Complete', 'build_id': 'mr6wwa34z472', 'elapsed_ms': 16228, 'reason': None}
node.delete h38q5jqafmd8: {'outcome': 'Reply', 'deleted': True, 'elapsed_ms': 346, 'reason': None}
the fabric-primary h38q5jqafmd8 (pid 3612540) ended
estate at target/demo-estate-20261106T172300 stopped; left: 0
```

## Next steps

- **Your node's data dir.** A node keeps its data under `RDM_DATA_DIR`. A node re-created at the same `path` and `name` gets its predecessor's directory when its storage is `persistent` and the directory is still there (`inherited = true` in `pre_node_ready`), a new one otherwise; RDM never looks inside it. Your `pre_node_ready` decides whether the data is usable and answers `Ok`, `Pending` or `Failed` ([consumer guide, Where my data goes](eng/consumer-guide.md#where-my-data-goes)).
- Read the [Glossary](glossary.md) for definitions of every RDM term, or [Fabric, mesh and node lifecycles](architecture/fabric-mesh-node-lifecycles.md) for the index of architecture pages and workflows.
- Read [Building an application on RDM](eng/consumer-guide.md) and [Your own ops on Node RPC](eng/consumer-guide-ops.md) to wire your own node types and protocols.
- Browse the runnable examples in `demo/examples/README.md`.
