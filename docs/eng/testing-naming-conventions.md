# Testing naming conventions: cells, files and modules

How an RDM test is named, where its file goes, and what the landing refuses. Every rule below
names its source and an RDM example that exists. Span names are a sister convention:
[telemetry-patterns.md](telemetry-patterns.md) §"Span names".

## TL;DR

- A cell is named `<actor>_<action>_<expected_outcome>`, a sentence in snake_case that a reader
  can check against the code.
- An e2e file is named for the operation or mechanism it proves, in the family directory of the
  page that owns it; a functional file is named for the source module it proves.
- A file is a module of its crate's one test executable, `tests/main.rs`.
- No issue id and no retired word in a file name or an identifier; the name check refuses both at
  landing.
- The same behaviour on both providers lives in `mod process` and `mod container` in one file.

## 1. The cell name

**Rule 1.1: `<actor>_<action>_<expected_outcome>()`** (rafka-v2 CLAUDE.md "Test architecture",
which applies here in full by RDM's CLAUDE.md). The actor is the subject of the sentence, the
action is what it does or what is done to it, and the outcome is what the cell asserts:

| cell | actor | action | outcome |
|---|---|---|---|
| `crates/rdm-test/tests/node/node_stop.rs:79` `a_stop_keeps_the_process_and_a_start_rejoins_it_as_the_same_birth` | a stop | keeps the process; a start rejoins it | as the same birth |
| `crates/rdm-admin/tests/build_claim.rs:134` `a_claim_of_a_held_attempt_is_lost_to_its_claimant_and_changes_nothing` | a claim of a held attempt | is lost to its claimant | changes nothing |
| `crates/rdm-test/tests/fabric/seeded.rs:21` `a_seeded_fault_schedule_holds_every_invariant` | a seeded fault schedule | holds | every invariant |
| `crates/rdm-core/src/certs.rs:606` `an_expired_issuer_cert_is_refused_issuer_expired` (unit) | an expired issuer cert | is refused | `IssuerExpired` |

The outcome is the assertion, never the operation's name: a cell named for what it calls
(`node_stop_works`) cannot be checked against its body.

**Rule 1.2: no label fragments.** No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or
`_full_` in a cell name (rafka-v2 CLAUDE.md "Test architecture"). A name says what is proved, not
what kind of test it is.

**Rule 1.3: no banned vocabulary.** CLAUDE.md's vocabulary bans hold in every name: a cell, a
file, a module and a helper. Words with a page meaning keep that meaning: a node is stopped then
started, or re-born at the same `path.name` with a new node id; connections are deleted; the
primaries are the fabric-primary, the mesh-primary and the node-type primary (CLAUDE.md
"Vocabulary"). The name check (§4) refuses the `RETIRED` subset in identifiers a diff adds; the
rest of the list is the reviewer's to refuse.

**Rule 1.4: the contract.** Most RDM cells carry a `/// CONTRACT:` doc comment above the
function stating what must hold and what must not happen (122 test files hold one; for example
`crates/rdm-test/tests/node/node_stop.rs:67`, `crates/rdm-test/tests/fabric/seeded.rs:14`). The
name is the contract's short form.

## 2. Files and families

**Rule 2.1: e2e files mirror the page that owns the mechanism.** `rdm-test`'s cells live in
`crates/rdm-test/tests/<family>/<op or mechanism>.rs`; the families are `build/`, `chaos/`,
`connections/`, `elections/`, `fabric/`, `gossip/`, `mesh/`, `node/`, `rpc/` and `runtime/`
(`crates/rdm-test/tests/main.rs:1-4`). A lifecycle operation's file is named for it:

| file | page |
|---|---|
| `crates/rdm-test/tests/node/node_stop.rs` | [lifecycles/node-stop.md](../architecture/lifecycles/node-stop.md) |
| `crates/rdm-test/tests/node/node_delete.rs` | [lifecycles/node-delete.md](../architecture/lifecycles/node-delete.md) |
| `crates/rdm-test/tests/mesh/mesh_create.rs` | [lifecycles/mesh-create.md](../architecture/lifecycles/mesh-create.md) |

Some files still sit at the top of `crates/rdm-test/tests/` outside a family
(`node_start_sequence.rs`, `stop_start.rs`, …).

**Rule 2.2: functional files mirror the source module.** A crate's functional file is named for
the module it proves: `crates/rdm-admin/tests/build_claim.rs` proves
`crates/rdm-admin/src/build_claim.rs`.

**Rule 2.3: unit cells live in `mod tests`.** Inline `#[cfg(test)] mod tests` in the source file
(`crates/rdm-core/src/ping.rs:122`, `crates/rdm-core/src/certs.rs`).

**Rule 2.4: no issue id in a test file name.** A test file under `tests/` named
`i<N>_acceptance…` or carrying a four-digit number as a word of its name is refused at landing
(`scripts/name-check.py:29-30`). Acceptance jobs keep the issue in the registry
(`tools/mesh-audit/i143-acceptance-jobs.json`, `"issue": 2938`) and in their artifact folders,
never in a `.rs` name.

**Rule 2.5: no retired word in a file name.** An added or renamed `.rs` file whose name holds a
`RETIRED` word is refused (`scripts/name-check.py:26-28`; its test
`scripts/test_name_check.py:39-40` refuses `crates/rdm-admin/src/seat_watch.rs`).

## 3. Modules and stems

**Rule 3.1: one executable per crate, one module per file.** Every crate with integration tests
sets `autotests = false` and names one `[[test]]`, `tests/main.rs` (`crates/rdm-test/Cargo.toml`;
`rdm-test` adds `memory_estate`). Each file is a module of it, named for the file's stem:

```rust
// crates/rdm-test/tests/main.rs:202-203
#[path = "node/node_stop.rs"]
mod node_stop;
```

**Rule 3.2: a module name is a stem.** The gate runs a scenario stem as `main <stem>::`. A module
whose name is the suffix of another's needs that other skipped (`--skip <other>::`); the gate adds
the skip itself (`scripts/rdm-gate-at.sh:286`), and `crates/rdm-test/tests/main.rs:1-4` states
it for a hand run. Choose a module name that is no other module's suffix.

**Rule 3.3: the provider is a module boundary.** The same behaviour proved on both deployment
providers lives in one file, its provider variant in `mod process` and `mod container`:
`crates/rdm-admin/tests/provider_state.rs:151` (`mod process`) and `:344` (`mod container`);
`crates/rdm-test/tests/fabric/declared_secrets.rs:497` (`mod container`). The gate filters on it
(`declared_secrets::container::` in its `container-proof` job, `scripts/rdm-gate-at.sh:393`). A
cell's full name reads `<module>::<provider>::<cell>`.

**Rule 3.4: a removed cell is named.** A cell deleted with its code is named in the landing's
commit body as `removed: <module>::<cell>`, the name `--list` prints; the gate's discovery check
refuses any other drop (`scripts/rdm-gate-at.sh:165-168`).

## 4. What the landing refuses: `scripts/name-check.py`

`rebase-gate-land.sh` runs `name-check.py origin/main HEAD` before any gate; a hit refuses the
landing (`scripts/rebase-gate-land.sh:29`). It reads only `.rs` files, and only what the diff
introduces: prose, quotes, comments and commit bodies are never matched (`scripts/name-check.py:2-5`).

| refusal | rule | source | example from its tests |
|---|---|---|---|
| a retired word in an added or renamed `.rs` file name | the `RETIRED` set (`drain`, `park`, `fence`, `relaunch`, `reborn`, `cluster`, `relay`, `tailer`, `sidecar`, `sysadmin`, `viewer`, `operator`, `departed`, `retire`, `seat`, `authority`, `authoritative`, `authorizes`, `cohort`, `lane`, `sticky`, `book`, `roll`, `climb`, `round` and their forms) | `name-check.py:8-14`, `:26-28` | `seat_watch.rs` (`test_name_check.py:40`) |
| an issue id in an added test file name | `i<N>_acceptance`, or a four-digit word, under `/tests/` | `name-check.py:29-30` | |
| a retired word in a definition the diff adds | `fn`, `struct`, `enum`, `mod`, `trait`, `type`, `const`, `static` names, split on `_` and case; a name a removed line also defined is not new | `name-check.py:15-18`, `:31-38` | `fn take_seat`, `struct SeatHolder`, `enum Authority`, `fn cohort_primary`, `mod build_lanes`, `fn sticky_primary`, `fn authorizes` (`test_name_check.py:34`) |
| an issue id in an added identifier | `_acceptance_<N>` | `name-check.py:39` | |
| a runtime construction of a permission code | `PermissionDenied`, `Unauthenticated`, `Unauthorized`, `SenderNotSubject`, `CallerRefused` (and aliases, `Code::from_number(7\|16)`, `Code::ALL`, `unauthorized(..)`), outside enum definitions, imports, test functions, match patterns, `let` patterns and assertion macros | `name-check.py:40-181` | `test_name_check.py:49-124` |

The primaries are not refused (`test_name_check.py:42`). The check refuses only what a diff
adds, so names that predate it stand until their own rename; a cell or file that still carries a
banned word is not a model for a new one.

Run it before committing, in the lane:

```bash
python3 scripts/name-check.py origin/main HEAD   # prints nothing when clean
python3 scripts/test_name_check.py               # the check's own tests
```

## 5. Names inside a cell

- A span a cell asserts is named by its literal, the name the code emits
  (`rdm_test::estate::named(&spans, "rdm.node_rpc.request.serve.via-direct")`); never build a span
  name from parts.
- A node is named in a cell by its `path.name` (`mesh1.rpc.1`, `mesh1.admin.2`) or its node id;
  `id`, `index` and `name` are three things, and `key` is none of them (CLAUDE.md "Vocabulary").
- An estate's root is named for its cell: `rdm_test::estate::estate_root(test)` makes
  `i143-<test>-<pid>-<n>` under the temporary directory, so two runs never share a root.
