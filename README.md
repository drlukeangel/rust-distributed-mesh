# rust-distributed-mesh (RDM)

The generic Mesh product: a fabric of meshes of nodes on one iroh network. Node-admins run the
control plane (topology on iroh-gossip, Node RPC, Builds, deployment providers); rafka-v2 pins
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

A fresh clone builds with the Rust toolchain (`rust-toolchain.toml`) and a C compiler (`cc`, `c++`).
Two tools are optional; `.cargo/config.toml` uses each one that is in `PATH`:

- `sccache`: caches the vendor crates' compiles across checkouts (`scripts/rustc-wrapper.sh`) and the
  C/C++ compiles (`scripts/cc-wrapper.sh`). Without it, plain `rustc`, `cc` and `c++` compile.
- `mold` (Linux): the linker. Without it, the default linker links.

A build without one of them names it once at its start (`rdm build: … is not in PATH …`). The
gate (`scripts/rdm-gate-at.sh`) requires both and refuses by name without them.

Stems run through the gate scripts, not raw `cargo test` (see `CLAUDE.md`, "Lanes, gate, landing").
Spans go to the OTLP collector named by `OTEL_EXPORTER_OTLP_ENDPOINT`; every scenario's evidence
is its Jaeger trace.
