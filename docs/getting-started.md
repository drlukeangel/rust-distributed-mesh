# Getting started

This tutorial walks from a clean checkout to a running two-[mesh](architecture/fabric-mesh-node-lifecycles.md)
estate (`scripts/demo-estate.sh up`), runs one application op (`demo/src/consumer_example.rs`,
documented in [Your own ops on Node RPC](eng/consumer-guide-ops.md)) on
[Node RPC](architecture/node-rpc.md), inspects a [Build](architecture/build.md), and tears the
[fabric](architecture/fabric-mesh-node-lifecycles.md) down.

## Prerequisites

Install the tools required by `.cargo/config.toml` and `rust-toolchain.toml`:

- **Rust `1.94.0`** (`rustup toolchain install 1.94.0`)
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
fabric fabric-wcy2e6, meshes mesh-wcy2e6-1 and mesh-wcy2e6-2
day-0 admin up: rdm://n0g9awfvsgsc (pid 2381819)
UI up: http://127.0.0.1:46809
bootstrap: {"attempt":1,"build_id":"y4qny3jg6n45"}
topology after 6s: 20 of 20 ready
ESTATE UP at target/demo-estate-20261029T103315; UI http://127.0.0.1:46809
```

What `scripts/demo-estate.sh up` did:

1. Started a Day-0 [`node-admin`](architecture/node-admin-lifecycle.md) process with
   `RDM_BOOT_PROFILE=day0`, which created the [fabric](architecture/lifecycles/fabric-create.md),
   the first [mesh](architecture/lifecycles/mesh-create.md) (`mesh-wcy2e6-1`), and its own
   [birth](architecture/node-identity-and-state.md) (`mesh-wcy2e6-1.admin.1`) as
   [fabric-primary](architecture/fabric-primary.md).
2. Started `rdm-admin-ui` attached to that Day-0 `node-admin`.
3. Submitted a bootstrap [Build](architecture/build.md) (`y4qny3jg6n45`) through the
   `process` [deployment provider](architecture/deployment-provider.md) that created the remaining
   19 [nodes](architecture/lifecycles/node-create.md) across `mesh-wcy2e6-1` and `mesh-wcy2e6-2`
   (3 `admin`, 2 `gateway`, 3 `broker`, and 2 `compute` nodes per mesh) and waited until all 20
   reached [`Ready`](architecture/lifecycles/states.md) in the [topology](architecture/gossip.md).

Verify the cross-mesh [Node RPC](architecture/node-rpc.md) stream between nodes in the two meshes:

```bash
RDM_DEMO_BIN=$PWD/demo/target/debug scripts/demo-estate.sh stream
```

Output:

```text
mesh-wcy2e6-1.compute.1 -> mesh-wcy2e6-2.broker.1: {'outcome': 'Reply', 'streamed': True, 'kind': 'direct', 'peer': 'mesh-wcy2e6-2.broker.1', 'answered': 'echo mesh-wcy2e6-2.broker.1: hello from mesh-wcy2e6-1.compute.1', 'reason': None, 'refused': None} trace da197df58b2035e529067667f290ffff
mesh-wcy2e6-1.compute.1 -> mesh-wcy2e6-2.gateway.2: {'outcome': 'Reply', 'streamed': True, 'kind': 'direct', 'peer': 'mesh-wcy2e6-2.gateway.2', 'answered': 'echo mesh-wcy2e6-2.gateway.2: hello again from mesh-wcy2e6-1.compute.1', 'reason': None, 'refused': None} trace 57760dc3d1435e8904f5d6188ce19a27
mesh-wcy2e6-2.compute.2 -> mesh-wcy2e6-1.broker.3: {'outcome': 'Reply', 'streamed': True, 'kind': 'direct', 'peer': 'mesh-wcy2e6-1.broker.3', 'answered': 'echo mesh-wcy2e6-1.broker.3: hello from mesh-wcy2e6-2.compute.2', 'reason': None, 'refused': None} trace 9accfd33b1fd57063c3eca5a9146b892
mesh-wcy2e6-2.gateway.1 -> mesh-wcy2e6-1.compute.2: {'outcome': 'Reply', 'streamed': True, 'kind': 'direct', 'peer': 'mesh-wcy2e6-1.compute.2', 'answered': 'echo mesh-wcy2e6-1.compute.2: hello from mesh-wcy2e6-2.gateway.1', 'reason': None, 'refused': None} trace eb3d50a7f552274dac98e6fc163b9c03
mesh-wcy2e6-1.gateway.1 -> mesh-wcy2e6-1.broker.2: {'outcome': 'Reply', 'streamed': True, 'kind': 'direct', 'peer': 'mesh-wcy2e6-1.broker.2', 'answered': 'echo mesh-wcy2e6-1.broker.2: same mesh, gateway to broker', 'reason': None, 'refused': None} trace f882225ca995393a48b6276225f74049
```

## 3. Run one application op and inspect one Build

`demo/src/consumer_example.rs` is the compiled reference for
[Your own ops on Node RPC](eng/consumer-guide-ops.md). It defines two product protocols in the
`0x50..=0x7F` product op range:

- `ExampleWork` (`0x50`, `"example-work"`): a unary op where a `compute` node submits an operation
  by id (`WorkRequest::Submit`) or queries its state (`WorkRequest::Get`), and a `broker` node
  records it in its durable `OperationStore` (`operations.jsonl`) before acknowledging acceptance
  ([`pre_node_ready`](architecture/fabric-node-hooks.md) resumes any pending operations on restart).
- `ExampleRecords` (`0x51`, `"example-records"`): a server-streaming op that copies the serving
  `broker` node's operation record entries (`Started`, `Entry`, `Through`).

Run `scripts/demo-estate.sh example` to submit two [`node.replace`](architecture/lifecycles/node-replace.md)
[Builds](architecture/build.md) that switch `<mesh-2>.broker.3` and `<mesh-2>.compute.1` onto the
`rshape-example` executable:

```bash
RDM_DEMO_BIN=$PWD/demo/target/debug scripts/demo-estate.sh example
```

Output:

```text
replace mesh-wcy2e6-2.broker.3 onto rshape-example: {"attempt":1,"build_id":"npb95jwte51k"}
replace mesh-wcy2e6-2.compute.1 onto rshape-example: {"attempt":1,"build_id":"g5e04pz1wbzt"}
the example's lines (target rshape) are in each node's stderr.log under target/demo-estate-20261029T103315/<path.name>-<node id>/
```

Inspect the [Build](architecture/build.md) that replaced `mesh-wcy2e6-2.broker.3` (`npb95jwte51k`),
showing its [attempt](architecture/build.md), [steps](architecture/build.md), and terminal state:

```bash
UI_URL=$(tr -d '\n' < target/demo-estate-current/ui.url)
curl -fsS "$UI_URL/api/builds?id=npb95jwte51k" | python3 -m json.tool
```

Output (abbreviated to the Build summary and steps):

```json
[
  {
    "build_id": "npb95jwte51k",
    "operation": "node.replace",
    "state": "Complete",
    "attempt": 1,
    "steps": [
      {
        "step_id": "node.replace:mesh-wcy2e6-2.broker.3:prepare",
        "state": "Complete"
      },
      {
        "step_id": "node.replace:mesh-wcy2e6-2.broker.3:cutover",
        "state": "Complete"
      }
    ]
  }
]
```

Now read the `rshape` log lines emitted by the serving broker (`mesh-wcy2e6-2.broker.3`) and the
calling compute node (`mesh-wcy2e6-2.compute.1`) as they execute `ExampleWork` (`0x50`) and
`ExampleRecords` (`0x51`):

```bash
grep "rshape:" target/demo-estate-current/mesh-*-2.broker.3-*/stderr.log | tail -n 2
grep "rshape:" target/demo-estate-current/mesh-*-2.compute.1-*/stderr.log | tail -n 2
```

Output:

```text
2026-10-29T10:33:47.049036Z  INFO rdm.node_rpc.request.serve.via-direct{protocol="example-work" op=80 peer=621faf96a677ec06ce5a421afbacb0b90733b6be28c6b74f652aceeff8c08f78 caller_system="rdm"}: rshape: an operation recorded, then accepted; its work started operation=mesh-wcy2e6-2.compute.1-sfphgmj1drew-000001
2026-10-29T10:34:07.054665Z  INFO rdm.node_rpc.request.serve.via-direct{protocol="example-work" op=80 peer=621faf96a677ec06ce5a421afbacb0b90733b6be28c6b74f652aceeff8c08f78 caller_system="rdm" code="OK" detail="accepted"}: rshape: an operation's work completed and its completion is recorded operation=mesh-wcy2e6-2.compute.1-sfphgmj1drew-000001
```

```text
2026-10-29T10:33:57.041527Z  INFO rshape: an example call answered call="submit" operation=mesh-wcy2e6-2.compute.1-sfphgmj1drew-000004 node=mesh-wcy2e6-2.broker.3 outcome="Reply" detail="accepted" code=Ok
2026-10-29T10:33:57.047993Z  INFO rshape: a serving node's record copied node=mesh-wcy2e6-2.broker.3 from=3 next=4 ended=Ok(4)
```

While the estate is up, you can also run any standalone example under `demo/examples/`:

```bash
cargo run --locked --manifest-path demo/Cargo.toml --example topology_and_atlas
```

## 4. Tear down the estate

Stop the estate with [`fabric.delete`](architecture/lifecycles/fabric-delete.md) and [`node.delete`](architecture/lifecycles/node-delete.md) on the Day-0 `node-admin`:

```bash
RDM_DEMO_BIN=$PWD/demo/target/debug scripts/demo-estate.sh down
```

Output:

```text
fabric.delete: {'outcome': 'Reply', 'terminal': 'Complete', 'build_id': '6nydzg6dh2bb', 'elapsed_ms': 14130, 'reason': None}
node.delete mesh-wcy2e6-1.admin.1: {'outcome': 'Reply', 'deleted': True, 'elapsed_ms': 410, 'reason': None}
the fabric-primary mesh-wcy2e6-1.admin.1 (pid 2381819) ended
estate at target/demo-estate-20261029T103315 stopped; left: 0
```

## Next steps

- Read [Fabric, mesh and node lifecycles](architecture/fabric-mesh-node-lifecycles.md) for the index of architecture pages and workflows.
- Read [Building an application on RDM](eng/consumer-guide.md) and [Your own ops on Node RPC](eng/consumer-guide-ops.md) to wire your own node kinds and protocols.
- Browse the runnable examples in `demo/examples/README.md`.
