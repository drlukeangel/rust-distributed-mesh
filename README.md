# rust-distributed-mesh (RDM)

The generic Mesh product: a fabric of meshes of nodes on one iroh network. Node-admins run the
control plane (membership on iroh-gossip, Node RPC, Builds, deployment providers); rafka-v2 pins
this repository and puts its gateways, brokers and compute nodes on it.

- **Spec:** [`docs/architecture/`](docs/architecture/) with the lifecycle pages under
  [`docs/architecture/lifecycles/`](docs/architecture/lifecycles/); start at
  [`fabric-mesh-node-lifecycles.md`](docs/architecture/fabric-mesh-node-lifecycles.md).
- **Record:** [`docs/plans/i143-prd.md`](docs/plans/i143-prd.md), rulings in
  [`docs/plans/rulings/`](docs/plans/rulings/), proposals in [`docs/plans/proposal/`](docs/plans/proposal/).
- **Agent instructions:** [`CLAUDE.md`](CLAUDE.md).

## Layout

`crates/` holds the product crates and their tests; `demo/` holds the R-shape consumer (the HTTP
API and the product role binaries) that exercises them. The target crate layout is
[`docs/plans/proposal/rdm-crate-and-test-layout.md`](docs/plans/proposal/rdm-crate-and-test-layout.md).

## Build and test

```bash
cargo build --tests
```

Stems run through the gate scripts, not raw `cargo test` (see `CLAUDE.md`, "Lanes, gate, landing").
Spans go to the OTLP collector named by `OTEL_EXPORTER_OTLP_ENDPOINT`; every scenario's evidence
is its Jaeger trace.
