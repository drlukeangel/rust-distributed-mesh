# Contributing to RDM

RDM (`rust-distributed-mesh`) is the generic control-plane and data-plane substrate for a
[fabric](docs/architecture/fabric-primary.md) of [meshes](docs/architecture/fabric-mesh-node-lifecycles.md)
of [nodes](docs/architecture/node-identity-and-state.md). Before making changes, read the
[Glossary](docs/glossary.md) and [Fabric, mesh and node lifecycles](docs/architecture/fabric-mesh-node-lifecycles.md).

## Where design decisions are recorded

1. **Architecture pages (`docs/architecture/` and `docs/architecture/lifecycles/`):**
   Every page under `docs/architecture/` is the normative contract for the concept or workflow it
   owns ([index](docs/architecture/fabric-mesh-node-lifecycles.md)). Code conforms to the page,
   never the other way around. An architecture page is updated only with explicit sign-off on the
   exact old and new text, and a change to a page's contract lands in the same commit as the code
   that implements it.
2. **Rulings (`docs/plans/rulings/`):**
   Numbered architectural rulings (`Q1`, `Q2`, …) record the context, trace evidence, and
   chosen design arm whenever a design question is settled.
3. **Proposals and work records (`docs/plans/`):**
   Design proposals live in `docs/plans/proposal/` and active initiative records live in
   `docs/plans/i143-prd.md`.

## Prerequisites

The repository pins its toolchain in `rust-toolchain.toml` and configures `sccache` and `mold` in
`.cargo/config.toml`:

- **Rust `stable`** (the channel `rust-toolchain.toml` names; `rustup` installs it on first use)
- **`sccache`** and **`mold`** (`sudo apt-get install -y sccache mold clang`)
- **Docker** (required when testing the `container` [deployment provider](docs/architecture/deployment-provider.md))
- **`mdbook`**, **`mdbook-mermaid`**, and **`mdbook-linkcheck2`** (optional, for `scripts/docs-site.sh`:
  `cargo install mdbook mdbook-mermaid mdbook-linkcheck2`)

## Building

Build all workspace crates and test binaries from the repository root:

```bash
cargo build --tests
```

To build the reference consumer application (`demo/`, which is an independent Cargo workspace in
`exclude` of the root `Cargo.toml`):

```bash
npm --prefix demo/admin-ui/web ci && npm --prefix demo/admin-ui/web run build
cargo build --locked --manifest-path demo/Cargo.toml --workspace --bins
```

Or run the consumer build verification script, which stages `demo/` in an isolated directory,
rewrites the RDM dependency rev to the candidate commit, builds every binary, and records a build
receipt:

```bash
scripts/i143-rshape-build-consumer.sh --candidate-sha "$(git rev-parse HEAD)" \
    --workspace target/i143-rshape/consumer-workspace \
    --bin-dir target/i143-rshape/consumer-bin \
    --output target/i143-rshape/consumer-build
```

## Running tests

See [Testing strategy](docs/eng/testing-strategy.md) for how tests are organized across `rdm-core`,
`rdm-transport`, `rdm-node`, `rdm-admin`, `rdm-client`, `rdm-test`, and `rdm-mesh-audit`.

- **Never export `CARGO_TARGET_DIR`** and avoid running ad-hoc `cargo test -p <crate>` subsets that
  repeatedly recompile `rdm-admin` with different feature sets.
- **Run a single test or stem with `scripts/cells.sh`**, which compiles the exact test binary shape
  the gate uses and runs the matching test(s) under the fast test cadence:

  ```bash
  # Run a test stem in crates/rdm-test/tests/main.rs:
  scripts/cells.sh rdm-test main -- node_stop::

  # Run a unit test stem in crates/rdm-admin:
  scripts/cells.sh rdm-admin lib -- status_declare::

  # Run a mesh-audit check in tools/mesh-audit/tests/main.rs:
  scripts/cells.sh rdm-mesh-audit main -- rshape_definition::
  ```

- **Run the full verification gate with `scripts/rdm-gate-at.sh`**:

  ```bash
  # Fast gate (unit, functional, e2e, and audit ratchets at fast timers):
  scripts/rdm-gate-at.sh . fast

  # Release gate (adds the release-only jobs, each stem at its own cadence):
  scripts/rdm-gate-at.sh . release
  ```

## Vocabulary check

RDM enforces a strict domain vocabulary across identifiers, file names, comments, and documentation
(see [Glossary](docs/glossary.md) and [Naming conventions](docs/eng/testing-naming-conventions.md)).
Banned words and issue-numbered test filenames are rejected before merge.

Before committing, run the name check against `origin/main`:

```bash
python3 scripts/name-check.py origin/main HEAD
```

An empty output (exit code `0`) means the diff is clean.

## Building the documentation site

To regenerate `docs/reference/rdm/` (`config.md` and `spans.md`), build `rustdoc`, build the mdBook
guide, and check links into `target/docs-site`:

```bash
scripts/docs-site.sh
```
