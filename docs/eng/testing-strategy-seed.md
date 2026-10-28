# Seeds: declarative scenarios and seeded schedules

RDM's tests use "seed" in two senses, both in `crates/rdm-test`:

1. **A scenario file**: a YAML declaration of a fabric's shape, the operations to run on it and
   the results to assert, run end to end through node-admin and real Node RPC
   (`crates/rdm-test/src/scenario.rs`, `runner.rs`). The one in the repo is
   `crates/rdm-test/scenarios/i143-node-rpc-seed.yaml`.
2. **A run's seed**: the one number a soak's whole schedule of actions is drawn from, so a run can
   be repeated exactly (`crates/rdm-test/src/soak.rs`; `RDM_SOAK_SEED`).

Neither is a fixture an application provisions: RDM has no application entities. A cell creates
the nodes it needs through node-admin ([testing-with-node-admin.md](testing-with-node-admin.md)).

## 1. Scenario files

### Shape

```yaml
version: 1                 # the only version; another is refused
product: mesh              # the run's ownership, recorded in the estate manifest
feature: node-rpc
subfeature: rpc-certainty
rung: multi-node
provider: process          # process | container
shape: MN                  # SN | MN | MM
fabric: fabric1
meshes:
  - name: mesh1
    node_admin: 2
    rpc_node: 3
operations:                # each a one-key map
  - put:    { target: mesh1.rpc.1, key: 100, value: "seed-000" }
  - cas:    { target: mesh1.rpc.1, key: 100, expected: "seed-000", value: "seed-000-cas" }
  - delete: { target: mesh1.rpc.3, key: 105 }
assert:
  - get: { target: mesh1.rpc.1, key: 100, equals: "seed-000-cas" }
  - get: { target: mesh1.rpc.3, key: 105, absent: true }
```

(abridged from `crates/rdm-test/scenarios/i143-node-rpc-seed.yaml`)

### What the parser refuses

`Scenario::parse` refuses by name, before anything starts (`scenario.rs`, `ScenarioError`):

| refusal | when |
|---|---|
| `Parse` | the YAML does not parse, or carries an unknown field (every type is `deny_unknown_fields`) |
| `UnsupportedVersion` | `version` is not 1 |
| `UnknownProvider` | `provider` is not `process` or `container` |
| `UnknownShape` | `shape` is not `SN`, `MN` or `MM` |
| `ShapeMismatch` | the meshes are below the shape's floor: SN is 1 mesh of 1 node-admin and 1 rpc node; MN is 1 mesh of 2 node-admins and 3 rpc nodes; MM is two MN meshes |
| `UnknownTarget` | an operation or assertion targets a `path.name` that is not `<mesh>.rpc.<n>` of a declared mesh |
| `AmbiguousGet` | a `get` does not say exactly one of `equals` or `absent: true` |

The parser's own cells are in `scenario.rs`'s `mod tests`.

### How a scenario runs

`rdm_test::runner::run(&scenario, provider, test)` (`crates/rdm-test/src/runner.rs`):

1. bootstraps an estate on the provider (the scenario's own, unless overridden), owned by the
   scenario's product, feature, subfeature and rung;
2. submits one `build.create` that reconciles the fabric to the declared meshes
   (`Scenario::desired`), and waits until every declared node is ready;
3. runs each operation, then each assertion, through `rdm-rpc-probe` over real Node RPC against
   `path:<target>`: no in-process shortcut. An operation passes on a `Reply` (a `cas` also needs
   `swapped`); an assertion needs a `Reply` naming its executing node and the value, or its
   absence;
4. writes `scenario.json`, `build-request.json`, `build-status.json`, `nodes-before.json`,
   `nodes-after.json` and `result.json` into the estate's artifacts, then shuts the estate down;
5. returns every failed step by name with the probe's answer.

From a shell, the `rdm-scenario` binary runs one file and exits 1 naming every failed step:

```bash
target/debug/rdm-scenario crates/rdm-test/scenarios/i143-node-rpc-seed.yaml [--provider process|container]
```

In a cell: `crates/rdm-test/tests/node/restart_canary_seed.rs` parses the file with
`include_str!` and runs it through `runner::run` on the provider its acceptance job names
(`scripts/i143-acceptance-gate.sh i143-2777-process`, `i143-2777-container`).

### Writing a scenario

- Declare the shape at or above its floor; name every target by its `path.name`.
- Put each operation's expected result in an `assert` `get`, with `equals` or `absent: true`.
- A run is replayable: `rdm_test::replay` holds the manifest that runs one scenario again exactly
  (ownership, seed, source, shapes, fault schedule and the ordered typed outcomes) and refuses a
  manifest that is lost, of another schema version or names another provider
  (`crates/rdm-test/src/replay.rs`, module doc).

## 2. A run's seed

The soak driver draws its whole schedule from one seed: every legal topology and fault action,
and the traffic's targets and values from a second generator seeded from the same seed
(`crates/rdm-test/src/soak.rs`, module doc).

- `crates/rdm-test/tests/fabric/seeded.rs` runs the driver for `RDM_SOAK_SECS` seconds (default
  60) of `RDM_SOAK_SEED` (default random) on the provider `MESH_SPAWN_TYPE` names, and prints
  `SOAK seed=<n> secs=<s>  (rerun: RDM_SOAK_SEED=<n> RDM_SOAK_SECS=<s>)` first, so any run is
  repeated exactly. A failure names the seed and prints the minimized reproduction.
- The estate records the seed in its manifest (`Estate::set_seed`).
- The registry's soak jobs (layer `soak-*`) are held by `scripts/i143-export-run.sh` until the
  fast gate and the release suite are green on one commit; `--soaks` runs them.
- The gate leaves `seeded` out of its sharded run (`SCENARIO_SKIP` in `scripts/rdm-gate-at.sh`).
