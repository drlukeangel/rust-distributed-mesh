# [PLAN TITLE] — [one-line customer-frame summary]

> **Template notice:** This is the Seed-First PRD template. Copy it as a starting point for any new `docs/plans/i<N>.e<M>.s<K>-<slug>.md` (or equivalent PRD) doc. See `docs/testing-strategy-seed.md` §Seed-First Strategy for why.
>
> Replace every `[BRACKETED]` field. Delete this template notice on copy.

**Initiative / Epic / Story ID:** `iN.eM.sK`
**Sprint:** [number]
**Status:** opened [YYYY-MM-DD]
**Architecture doc:** `docs/architecture/[name].md` — the FINAL VISION this PRD's gaps reach; created or extended by this PRD's review, never absent
**Signed off:** qa [YYYY-MM-DD HH:MM UTC] · dev [YYYY-MM-DD HH:MM UTC] — each lead writes their own half after running the eight-step adversarial review (`docs/agents/orchestrator-workflow.md`) on this file AND the architecture doc; blank until then
**Approved:** Luke [YYYY-MM-DD HH:MM UTC] — written the moment approval is given; a PRD without this line is not approved, and a rejection replaces it with `**Rejected:** Luke [date time] — [reason]` until the rework is approved
**Owner:** [name or "autonomous"]

## Context

[What's the customer-frame problem? Which persona is affected? Why does this work exist? 2-4 sentences.]

## Requirements

> **CANON (Luke ruling 2026-09-11):** *"this isn't a 'don't build it because it doesn't exist' — it's 'this is
> the gap, here is what needs to be built'."* A PRD is the GAP LIST and the BUILD LIST, nothing else. Every
> row of an audit or requirements table resolves to exactly one of: **HOLDS** (the code does it — file:line),
> or **BUILD** (the gap — file:line of what exists today, and the epic that builds it). The following are
> BANNED as a row's disposition, because each one records a gap as a state to keep: *"not built"*,
> *"correct by accident"*, *"never wired"*, *"banked … pending"*, *"not now"*, *"out of scope"*,
> *"future work"*. If a row would read that way, it is a BUILD row with an epic number, or it is deleted.
> A gap the PRD names but does not assign to an epic is a deferral and fails sign-off.


[Bulleted list of customer-facing requirements. Concrete, not aspirational. If you can't list them concretely enough to write a seed from them, the PRD isn't ready.]

1. [Requirement 1 — phrased as "the customer can X" or "when the customer does X, Y happens"]
2. [Requirement 2]
3. [Requirement 3]

## § Open forks (REQUIRED — a fork reaches Luke only after the TREE has failed to answer it)

A fork is a decision only Luke can make: a contract the customer sees, a design the arch docs do
not settle. **A fork the tree already answers is lazy PRD work, not a ruling** (Luke 2026-09-11:
*"these are rulings hiding lazy PRD work again — that last one verbatim already existed"*, after
a REST-fetch "fork" that `routes.rs` and the handler's doc comment had answered). Every fork row
therefore carries, BEFORE its arms, the evidence that the absence was checked:

| fork | the POSITIVE surface opened | the arch-doc grep | **the OUTLIER document** | why none of it answers it |
|---|---|---|---|---|
| F-N — one sentence | `routes.rs:<line>` entries, `openapi.json` paths, the handler's doc comment, the wire decoder's `ApiKey`, the store/edge declaration — listed, with what each DOES | `grep -n -i '<terms>' docs/architecture/<doc>.md` → the lines, quoted | `docs/architecture/<doc>.md §N` (or the PRD/code comment) — the ONE document whose text is the reason the fork exists: it contradicts the tree, contradicts another doc, or is silent where it should speak — quoted, so the lead and Luke can open it in one step (Luke 2026-09-11: *"make them document the doc that is the outlier so at least you can look it up quickly"*) | one sentence naming the gap |

- **No row without the first three columns.** The outlier column is the one Luke reads first: a fork with no named outlier document is a fork nobody looked for.
- **No row without the first two columns.** "Nothing exists" is stated as the listing that proves
  it (`docs/eng/…lack-of-surface…`: an empty grep is absence, a positive listing is data).
- **If the surface or the doc answers it, it is not a fork** — it becomes a HOLDS / BUILD row in
  `## Requirements` with the coordinate, and the seat implements it.
- **Already-ruled is not a fork.** Grep the memory rulings, the arch docs and the closed PRDs for
  the subject before writing "open"; cite the ruling if found.
- **The lead runs this audit over every fork BEFORE Luke's sign-off read**, and the seven-step
  read's step (2) includes it: open each fork's listed surface and confirm the gap is real.
- Arms come AFTER the evidence: both stated, the cost of each, a recommendation, and the epic that
  owns whichever comes back (the lane that asked still owns the work).

## § Seed spec

> **Ordering rule (user directive 2026-04-25):** the §Restart-recovery canary below is the **FIRST test the test-author writes**, before this seed assertion. Both must be RED before any implementation begins. The canary proves the feature survives the broker process boundary; the seed proves the feature works for the customer in the happy path. Both are non-negotiable; neither alone is sufficient.

[The YAML fragment (new theme or extension to an existing theme) + suite assertion that encodes the acceptance criterion. The feature is done when this is green.]

### Theme manifest fragment

**The fragment must LOAD.** Before a PRD is handed back, its fragment is checked against the
theme's own structs (`crates/rafka-test-harness/src/theme.rs` — `ThemeSpec`, `TopicSpec`, `ActorSpec`,
`WebhookSpec`, …, several of which carry `deny_unknown_fields`): every key it uses exists on the
struct, every name it binds it declares (an actor bound by a role_binding is an `actors:` entry in
the fragment), every coordinate it gives holds for EVERY theme it names, and the base commit it is
derived on resolves with `git cat-file -e`. A fragment that would refuse to load kills every cell
at mesh birth instead of at an assertion (QA adversarial passes 2026-09-12: four of six PRDs
failed this — GH#443, #445, #446, #447). Until the harness ships a fragment lint, the check is the
seat opening `theme.rs` and diffing key by key; the handback states it was done.



```yaml
# Appended to (or new file at) tests/e2e/fixtures/themes/<theme>.yaml
<section>:
  - name: <something>
    <field>: <value>
```

### Suite assertion

```rust
// In tests/e2e/tests/<feature>.rs (file named after the registry slug)
// @feature: <slug>
// @requires-test-categories: <category1>, <category2>

#[tokio::test]
#[ignore]
async fn <assertion-name>() -> anyhow::Result<()> {
    // Setup: hydrate the theme that contains the new section.
    // Action: the customer-facing operation this feature enables.
    // Assert: the customer-observable outcome.
    todo!("seed the feature; implementation comes AFTER this test exists and is red");
}
```

### What a passing seed proves

[One paragraph: if the seed above is green, which customer workflow is definitively working? Name the customer persona and the scenario.]

## § Surfaces — ONE capability, IDENTICAL on every surface (REQUIRED)

> **CANON (Luke ruling 2026-09-11, verbatim):** *"we need to stop talking about things as wire vs. REST.
> Those two surfaces must expose the identical surfaces, period."*

**A PRD specifies an operation ONCE.** Every operation an entity exposes exists on BOTH customer
surfaces with IDENTICAL semantics — same inputs, same outcome, same refusal reasons (the wire
error code and the REST status are the one mapping in CLAUDE.md § Cross-org access) — over ONE
shared core in a shared crate; each surface is a THIN DECODER over that core (CLAUDE.md § Test
architecture: a capability on multiple surfaces = one shared black box). The two surfaces are:

| Module | Surface | Decoder |
|---|---|---|
| `mod via_tcp` | rafka-client over the Kafka wire (TCP) | the wire handler in `crates/rafka-gateway-wire` |
| `mod rest` | the REST plane (`/topics/<id|path>/...`, `/iam/...`, `/orgs/<id|path>`, API key) | the REST handler in `crates/rafka-gateway-rest` / the entity crate's `rest.rs` |

QUIC is a framework TRANSPORT, proven once in the framework suite, never per feature — a
`via_quic` module in a feature file is a defect (Luke ruling 2026-09-11; CLAUDE.md § Test architecture).
The CLI surfaces are DELETED — no `mod cli_rf` / `mod cli_rfa` exists or returns.

**What the PRD must NOT do:** frame "wire" and "REST" as separate deliverables — separate epics,
rows, cells, "arms", or dispatch units. An operation that today exists on only ONE surface is a
**DEFECT the PRD names**, with the missing decoder owed in the same epic as the operation, never
a design. A behavior matrix is invariant across surfaces; only the plumbing differs.

**Per-PRD operation table (REQUIRED).** One row per operation, the two decoders as plumbing columns:

| Operation (once) | wire decoder | REST decoder | core | authz (one evaluator) |
|---|---|---|---|---|
| `<entity>.<verb>` | `<Kafka API name>` / `— (owed: <reason it is missing>)` | `<METHOD> /<route>/<id|path>` | `<crate>::<fn>_core` | `<entity>:<op>` at the entity's path |

**Tests: the SAME behaviors under both mods, assertion bodies IDENTICAL, only the plumbing differs.**
Every behavior named in §Seed spec has a `#[tokio::test] fn` in `mod via_tcp` AND `mod rest` within
the same `tests/e2e/tests/<slug>.rs`; the fn name is behavior-only; the surface lives at the module
boundary. Where a behavior genuinely has no analogue on a surface (rare: an in-process-only
behavior), leave `// no mod <surface>: <reason>` so a reviewer sees it was deliberate. Reasons that
are NOT acceptable: "out of scope", "future work", "TODO", "wire not built yet" — the last is the
defect the operation table must name, not a skip.

**Worked example:**

```rust
// In tests/e2e/tests/<feature>.rs (file named after the registry slug)
// @feature: <slug>

mod via_tcp {
    #[tokio::test]
    async fn <actor>_<action>_<expected_outcome>() -> anyhow::Result<()> { /* .client_tcp() ... identical asserts */ Ok(()) }
}

mod rest {
    #[tokio::test]
    async fn <actor>_<action>_<expected_outcome>() -> anyhow::Result<()> { /* .rest().post(...) ... identical asserts */ Ok(()) }
}
```

**Discipline rules:**

- **Live data, real surfaces.** Every test in every mod block exercises the REAL wire/REST surface against a REAL mesh through the `BaseTree` harness (attached to the lane's mesh; the harness provisions nothing — the theme does). NO mocks (`with_test_*_mock` / `fake_auth` / `MockBroker` / `stub_broker` / `with_test_subscriber_index` are forbidden in the e2e tier). If the test body uses a mock, it is NOT proof.
- **Customer-surface assertions only.** Assertions read back through a customer surface. NO internal-state probes (`COMPILED_ACLS.contains_key(...)`, `wal.virtual_index.bitsets.contains_key(...)`, etc.) — those belong in the **functional** tier under `<crate>/tests/<topic>.rs`.
- **Assert the specific reason, never the generic outcome.** A bare 401/403/404 (or a bare wire error code) passes for ANY failure in the layer — the check-that-cannot-fail class.
- **The gate is the feature gate.** `cargo run -p rafka-feature-gate -- test --feature <slug> --tier e2e` is the only dispatch; the operation table is the coverage commitment; the test file is the executable proof.

## § Restart-recovery canary (REQUIRED — no exceptions; FIRST test written)

**Why this section exists:** the canonical failure mode that lost rafka two weeks (2026-04-25 incident) was a 400-test suite that never asserted "produced records survive a broker restart." Phase 8.C documented the persistence contract; the implementation declared it; no test ever proved it. Customer-facing produce → restart → fetch returned empty for months while CI was green. This section makes that failure mode structurally impossible by forcing every PRD to answer the question explicitly.

**Ordering rule:** **this canary is the FIRST test the test-author writes.** Before the §Seed spec assertion above. Before any implementation. The discipline: a feature that doesn't survive its own restart isn't a feature; it's a temporary illusion. The seed proves the feature exists; the canary proves it persists.

**The question every PRD must answer:** *if the rafka process exits and restarts (clean shutdown OR crash) at any point during a customer using this feature, which customer-observable state is preserved, and what test proves it byte-exact?*

**Two process boundaries (user directive 2026-04-27):** the canary MUST cover BOTH boundaries the customer ever sees:

1. **Broker restart** — `kill -9` the broker process; respawn at the SAME data-dir; assert the customer-observable state survives byte-exact through the customer-facing surface.
2. **Gateway restart** — `kill -9` the gateway process; respawn; assert (a) clients reconnect transparently, (b) any gateway-held cache (auth, COMPILED_ACLS, replication state) re-warms WITHOUT customer-visible data loss or stale-data bugs, (c) in-flight produce/fetch semantics on either side of the gateway restart are correct.

If the feature touches both, ship two named canaries: `<feature>_state_survives_broker_restart_at_same_data_dir` AND `<feature>_state_survives_gateway_restart`. If the feature only touches one process boundary (e.g., a pure-gateway routing concern), ship the one applicable canary AND explicitly declare the other as out-of-scope per option (b) below.

**Three valid answers:**

### (a) Persistence is in scope — the canary lives here

This feature touches state that survives process restart (storage, replication, IAM tokens, billing fuel ledger, schema registry, org lifecycle, compacted-state, secrets, ACL compiled tables, anything written to disk OR external store). Write the canary inline, the same way as the seed assertion above. Same shape every time:

```rust
// In tests/e2e/tests/<feature>.rs — same file as the seed assertion
// @feature: <slug>
// @requires-test-categories: error-injection, invariant-boundary

#[tokio::test]
#[ignore]
async fn <feature>_state_survives_broker_restart_at_same_data_dir() -> anyhow::Result<()> {
    // Phase 1: tempdir held by the test (NOT the harness — survives drop).
    let tmp = tempfile::tempdir()?;

    let produced_state = {
        let harness = <spawn at tmp.path()>;
        // Customer-facing operation that creates the state under test.
        // Capture whatever the customer would observe (records, offsets, configs, IAM bindings, ...).
        let observed = <perform action; capture observable>;
        observed
        // harness drops here — simulates broker process exit.
    };

    // Phase 2: new process boots at SAME data dir.
    let harness2 = <re-spawn at tmp.path()>;

    // Phase 3: read back via the customer-facing surface (NOT internal probes).
    let recovered_state = <perform read via REST/wire/CLI>;

    // Phase 4: byte-exact / value-exact / set-equal — whatever the contract says.
    assert_eq!(produced_state, recovered_state, "post-restart: customer-observable state diverged");

    Ok(())
}
```

**Discipline rules:**
- The canary fetches via the **customer-facing surface** (Kafka wire / REST `/...` / `rf` CLI), NOT internal probes (no `wal.virtual_index.bitsets.contains_key(...)` etc.). If the customer can't see it, the test doesn't prove it.
- If the canary writes 100 records, it asserts on **all 100**, not "at least one." Sampling hides off-by-N persistence bugs.
- Test name is literal `<feature>_state_survives_broker_restart_at_same_data_dir` (or close — the discoverable phrase is "survives restart"). When grep finds N hits, the canary count IS the durability coverage.
- File **MUST** carry `// @requires-test-categories: error-injection` (process restart IS error-injection). `cargo run -p rafka-feature-gate -- check-tags` will start enforcing this requirement on persistence-tagged files.

### (b) Persistence is NOT in scope — explicit declaration required

This feature is purely in-process / pure-compute / pure-routing — touches NO state that survives a process restart. State this explicitly in this section:

```
NO RESTART-RECOVERY CANARY: this feature is [pure routing | pure compute |
in-process derived state | etc.]. No state crosses the broker process boundary.
Reviewer must verify: grep the diff for `fs::write`, `OpenOptions`, `tokio::fs`,
`object_store::`, segment writes, manifest writes, secrets writes, IAM persistence,
or any sink that survives a process exit. If grep returns hits, this declaration
is wrong and option (a) applies.
```

The reviewer-grep is the gate. If the implementation later adds a write that crosses the process boundary, the canary becomes mandatory and the PRD is amended.

### (c) Persistence is in scope BUT depends on a yet-unbuilt primitive — file the dependency

If the canary requires a primitive that doesn't exist yet (e.g., harness-level data-dir parameterization, multi-process subprocess spawn, replicated quorum that requires 2+ brokers), file the dependency story explicitly here AND ship a stub canary that compiles + passes today against the in-process scenario you CAN test, with `// PARTIAL: full canary blocked by <primitive>` documented inline. The story files immediately as a sibling PRD; the partial canary closes the loop the day the primitive lands.

### Anti-patterns this section makes impossible

- ❌ "Tests pass; ship it" without ever exercising restart.
- ❌ Documenting the persistence contract in a PRD without an executable canary that asserts the contract.
- ❌ Asserting via internal probes (`wal.virtual_index.bitsets.contains(...)`) — passes even when the customer-facing surface returns empty.
- ❌ Sampling assertion ("verify the first record is back") — silent N-1 persistence loss.
- ❌ Skipping the section because "this is just a small change" — the 2-week incident was a small change to the index format that didn't carry through to a canary.

## § OTLP span-assertion bar (REQUIRED — every NEW canary post-i23-otlp-test-substrate)

> **Naming (Luke, 2026-09-11):** every span this PRD names or mints follows
> [`docs/eng/observability-conventions.md`](../observability-conventions.md) — `rafka.<component>.<entity>.<action>.<reason>`,
> the entity being its REGISTERED kind (never a neighbouring entity's name), the verb on the whitelist, `via-` direct /
> `from-` cascade; and every test this PRD names follows
> [`docs/eng/testing-naming-conventions.md`](../testing-naming-conventions.md) — `<actor>_<action>_<expected_outcome>`,
> behavior-only, surface at the module boundary. A span or test name outside those two documents is a defect in the PRD.

**Rule (CLAUDE.md / `docs/eng/test-otlp-capture.md`):** every NEW canary merged after `i23-otlp-test-substrate` ships MUST:

1. Add `.with_otlp_capture()` to the `BaseTree::provisioned_with(...)` builder.
2. Call `assert_span_emitted` (or other helpers from `tests/e2e/src/span_assert.rs`) to verify at least one gateway-emitted OTLP span was captured with the expected attributes.
3. Leave a `tests/artifacts/<feature>/<test>.spans.jsonl` artifact on disk after each run.
4. **Jaeger Flame-Graph Trace Proof Link:** Output and record the direct Jaeger trace URL (`http://<jaeger-host>:PORT/trace/<trace_id>` (the Jaeger UI port, 16687)) proving the unbroken cross-process span hierarchy.

**Produce/fetch roundtrip success alone does NOT satisfy the canary bar.** A canary that only proves "the records came back" without asserting a corresponding span was emitted and without producing an unbroken Jaeger flame graph is incomplete.

### Engineering Acceptance Bar: Distributed Trace & Flame Graph

Every E2E test must satisfy the distributed tracing acceptance contract:
1. **W3C Trace Context Propagation:** Spans must correlate across process boundaries (Gateway $\rightarrow$ Broker $\rightarrow$ Compute) via `DataFrame` trace context (`TraceId` / `parent_span_id`).
2. **Hierarchical Flame-Graph Integrity:** When inspected in Jaeger (`http://<jaeger-host>:PORT/trace/<trace_id>` (the Jaeger UI port, 16687)), the trace must render a single unbroken causal flame graph from the ingress operation (e.g. `rafka.gateway.auth.create.via-http-control-dispatch` / `produce.dispatched`) down to internal execution and persistence sinks.
3. **Telemetry Attribute Verification:** Each span in the flame graph must carry its mandatory domain attributes (`org_id`, `job_id`, `claim_epoch`, `target_id`, etc.) per `docs/eng/observability-conventions.md`.
4. **Execution Evidence:** Test output logs must print the direct link:  
   `Jaeger Trace: http://192.168.68.99:16687/trace/<trace_id>`

### What this PRD's spans should look like

[Enumerate the spans this feature emits + the attributes each carries. Example:]

| Span name | Emitted from | Required attributes | Asserted by |
|---|---|---|---|
| `rafka.gateway.<feature>.<event>` | gateway | `org_id`, `topic_id`, `partition`, `<feature-specific>` | `via_tcp::<behavior>_test` |
| `rafka.broker.<feature>.<event>` | broker | `partition`, `offset`, `<feature-specific>` | `rest::<behavior>_test` |

If this PRD's feature emits no new spans (because it's purely structural / config-only), state explicitly: "NO NEW SPANS — feature is [structural / config / parser / ...]; no production code path emits a new span. Existing per-message observability spans cover the behavior. Reviewer-grep for `.in_scope(` / `tracing::info_span!` / `Span::current()` across the diff returns no new emit sites." The reviewer-grep is the gate; if it returns hits, the explicit-no-spans declaration is wrong and the table above is mandatory.

### Forbidden anti-patterns (from `docs/eng/test-otlp-capture.md`)

- ❌ Fault-proxy-as-assertion (the proxy fires; the canary asserts the proxy fired; nothing about real spans).
- ❌ Fabricated harness primitives (the canary calls `harness.expect_span(...)` where `expect_span` doesn't exist or doesn't do anything).
- ❌ Internal-state DashMap reads (`COMPILED_ACLS.contains_key(...)` instead of "did the gateway emit a span saying it permitted this?").
- ❌ Roundtrip-only assertion ("the produce returned 200; the fetch returned the records; ship it") — see CLAUDE.md.

## § Per-sprint seed-data obligation (REQUIRED — when sprint introduces a new entity)

**Rule (`feedback_seed_data_per_sprint_obligation.md`):** if this sprint / story introduces a new entity type (new topic kind, new schema, new record type — JobRecord, MetricRecord, projection VT, sub-section, etc.), the PRD's verification block below MUST include all three of:

1. **YAML schema extension.** The new entity type gets a section in `tests/e2e/fixtures/themes/<theme>.yaml`. Existing sections grow new fields where appropriate.
2. **Per-phase Rust seeder.** A seeder under `tests/e2e/src/scaffold/` (or wherever the harness's seeder pipeline lives) consumes the new YAML section and provisions instances per themed org during BaseTree setup.
3. **Canary proof.** At least one e2e canary in `tests/e2e/tests/<feature>.rs` uses `BaseTree::provisioned_with(theme)` and proves the entity is seeded correctly.

If this PRD's feature introduces NO new entity type (it's a parser / validator / config change with no record shape change), state explicitly: "NO NEW ENTITY — feature is [parser / validator / config / refactor]. Seed YAML schema unchanged. Existing fixtures continue to work." Reviewer verifies by `git diff tests/e2e/fixtures/themes/`; if there's a non-trivial diff, the explicit-no-entity declaration is wrong and the three-item obligation applies.

## § Test tier coverage

[Tick the tiers whose tests are part of this story's verification. Leave unticked tiers blank — don't add "N/A" filler. A story targeting only unit correctness ticks only `unit`. A story that boots rafka ticks at least `e2e`. Each ticked tier must have at least one file registered under this feature's slug in `docs/features/feature-registry.json` (`tests.unit`, `tests.e2e`, `tests.load`, `tests.chaos`).]

- [ ] unit — inline `#[cfg(test)]` tests in `src/*.rs`.
- [ ] e2e — `tests/e2e/<feature>.rs` behavior tests (boot rafka in-process).
- [ ] load — `tests/load/<feature>.rs` throughput / sustained scenarios.
- [ ] chaos — `tests/chaos/<feature>.rs` fault-injection scenarios.

## § Pre-implementation thoughts (YYYY-MM-DD)

[Brain-dump before touching implementation code. Minutes, not hours. Bullets fine.]

- **What I think is the hard part:** [your best guess]
- **What I'd bet goes wrong:** [most-likely failure mode]
- **Persistence path** (per `§ Restart-recovery canary` above): [what state crosses the process boundary; which on-disk file holds it; what rebuilds it on boot. If "none," say so explicitly so the reviewer-grep gate above has a target.]
- **Dependencies on other work:** [upstream tickets, unshipped primitives]
- **Architectural risks:** [golden-principle concerns, collision with concurrent work]
- **What I don't know yet:** [open questions that impl will answer]

## Implementation plan

[Step-by-step plan. Keep it mechanical — the design work already happened when you wrote the seed.]

1. [Step 1]
2. [Step 2]
3. [Step 3]
N. **Documentation (ALWAYS the final epic — user directive 2026-07-16).** The last epic of every initiative is writing the 3 per-slug docs (see § Per-feature documentation deliverable), authored by the agent(s) who just built it, while the context is fresh — NOT deferred to an end-of-project documentation phase. Rationale: the arcv sweep proved deferred docs rot (only ~4 of 47 architecture docs survived contact with code, precisely because they were written separately/late and never reconciled); docs written by the builder at finish are code-grounded and accurate. An initiative is not signed off until its docs land alongside its code + its Jaeger-link proof. (Only genuinely cross-cutting/architecture docs that span multiple initiatives defer to a light final reconciliation pass.)

### Epic block (REQUIRED — one per epic; a seat's brief is this section pasted VERBATIM)

Every epic in the plan carries ALL of the following, filled in — not sketched. The bar: dev or QA
copies the epic's section into a dispatch brief unchanged and the seat can start. (User directive
2026-09-09.)

#### e<N> — [epic title, customer-frame]

- **Goal → gap.** One sentence naming the final-state architecture doc section this epic delivers
  (`docs/architecture/<doc>.md § …`) and one sentence naming what the tree lacks today.
- **Detailed requirements, with `file:line`.** Each requirement that touches existing code names the
  file and line in the CURRENT tree (verified by reading, not recalled). A requirement without a
  coordinate is not finished.
- **A path this PRD stipulates but the tree does not yet hold is written `planned:<path>`** — e.g.
  a test stem this epic is to author, ``planned:crates/rafka-<crate>/tests/<topic>.rs``.
  Naming the suites that must be created is what a PRD is for, so `scripts/check-doc-coordinates.sh`
  does not report a marked path as dead. It does the converse: once the file EXISTS the gate fails
  until the marker is dropped, so a `planned:` cannot outlive its plan. A path the PRD asserts is
  ABSENT — a requirement whose subject is that nothing is there — is written `absent:<path>` and is
  checked the same way. **Everything else must resolve in the tree as written**; the marker is not a
  way to quiet a coordinate you have not verified, and nothing in the gate checks that a marked
  destination is a legitimate one (the opening review does).
- **Spans.** Every span the epic emits and every span its cells assert, named per
  `docs/eng/observability-conventions.md` (`rafka.<component>.<entity>.<action>.<reason>`, verb
  whitelist, `via-` direct / `from-` cascade). Absence assertions name their CONTROL span.
- **Testing requirements, by tier.** Unit / functional / e2e per CLAUDE.md § Test architecture:
  which stem file, which registry slug, `.with_otlp_capture()` + `.with_otlp_test_name()` +
  `.with_otlp_feature_name()` and a `/// CONTRACT:` on every e2e cell, the trace as the deliverable.
- **Acceptance tests, by name.** `<actor>_<action>_<expected_outcome>` for each; RED-first where the
  behavior predates the implementation.
- **Mesh plan — built into the epic.** The exact invocation and the `mesh-plan.json` it writes.

  **🔴 THERE ARE EXACTLY THREE SHAPES AND NO EXCEPTIONS** (Luke, 2026-09-10; GH#387). The name IS
  the shape decision — there is no size knob, no `--compute-full`, no per-epic tuning, and a cell
  never chooses its own topology. The same three words name the shape, the theme and the test
  label: **one vocabulary everywhere.**

  | name | node-admin | compute | gateway | broker | meshes |
  |---|---|---|---|---|---|
  | `single-node` | 1 | 1 | 1 | 1 | one |
  | `multi-node` | 2 | 2 | 3 | 3 | one |
  | `multi-mesh` | 2 | 2 | 3 | 3 | **two** — mesh A + peer mesh B, each with its own node-admin |

  **The shapes are additive as the themes are** — each shape a superset of the one below — so a stem that
  fits `single-node` runs unchanged on all three.

  **THE RUNG IS DECLARED ONCE, AT THE EPIC — never per story.** An epic declares the HIGHEST rung
  any of its stems requires, and every story in it runs on that one lane without a rebirth. Because
  a shape serves every smaller shape's cells, a `single-node` story runs unchanged on a `multi-mesh` lane, so the
  highest rung already satisfies all of them.

  **A story that needs a HIGHER rung than its epic is in the wrong epic** — move it, or split the
  epic. It is never a reason to rebirth a lane mid-epic: a rebirth is the lead's call, it blocks the
  seat, and an epic that needs two of them is two epics wearing one number.

  The one exception is an epic whose SUBJECT is the shape hierarchy itself — a proof that a stem climbs, or
  that a shape refuses. There the shapes are the experiment, and the epic says so explicitly and
  budgets the rebirths as part of the work (i130.e4 / GH#390 took two).

  **Pick that rung from what the epic's stems DECLARE** in `docs/features/feature-registry.json`,
  and state which declaration decided it: a stem declaring `gateway_count: 3` / `broker_count: 3`
  needs `multi-node`; a stem declaring `peer_mesh: true` or `tags: ["multi-mesh"]` needs
  `multi-mesh`; anything else is `single-node`. **Read the STEM, never a brief describing it** — a
  design doc saying "multi-node" while the stem requires a peer mesh sends a lane to the
  wrong rung and every attach refuses (cost: GH#204, 2026-09-11).

  **`compute_full` is NOT a plan knob.** Every descriptor stamps `compute_full: true`
  unconditionally — full compute ships on every shape (GH#421,
  `scripts/lane-mesh-deploy.sh`). `--compute-full` and the legacy shape knobs are DEAD
  (`scripts/lane-prep.sh`); there is nothing left to pass or resolve per-stem.

  Still per-epic: `--lead-seed=no` when the epic changes the theme or the WAL's end shape;
  `resume_from_snapshot` when its cells need reset-to-baseline between runs. `none` for an epic with
  no mesh; a durable format move adds `rebirth after`.

  ```
  scripts/lane-mesh-plan.sh <lane> --deployment=single-node --lead-seed=yes --lead-snapshot=yes --agent-resume=no
  ```
  ```json
  { "lane": "<lane>", "deployment": "single-node",
    "shape": { "node_admin": 1, "compute": 1, "gateway": 1, "broker": 1, "compute_full": false },
    "tags": [],
    "lead":  { "deploy": true, "seed": true, "snapshot": true },
    "agent": { "rebuild_changed_bins": true, "redeploy_changed_members": true,
               "seed": false, "snapshot": false, "resume_from_snapshot": false } }
  ```
- **Sizing.** One seat or two; which files it touches (the input to § Dispatch ordering).
- **Rows absorbed.** GitHub issue numbers this epic carries, with each row's verified verdict.

### Files likely to touch

- `[path/to/file.rs]` — [why]
- `[path/to/other.rs]` — [why]

## § Per-feature documentation deliverable (REQUIRED — three docs per slug)

> **User directive 2026-04-27:** every registry-slug feature ships THREE standalone docs at `docs/features/<slug>/`. Shipping a feature WITHOUT all 3 docs at the slug folder = sprint **NOT** closed. This section is part of the sign-off bar; it is non-negotiable.

**Folder shape:**

```
docs/features/<slug>/
  overview.md   # Diátaxis Explanation — what the feature is + how it's achieved internally
  how-to.md     # Diátaxis How-to / Tutorial — how a customer USES the feature (per surface)
  runbook.md    # Diátaxis operational How-to — SRE / production failure modes + recovery
```

The folder shape (NOT a flat `<slug>-overview.md`, NOT a single combined file) lets each feature carry future companions (diagrams, ADRs, examples, profile traces) as siblings without polluting a flat namespace. The registry's `docs.{overview,how-to,runbook}` fields point at exact paths for `rafka-feature-gate` doc-completeness checks.

### overview.md — Diátaxis Explanation

**Audience:** an engineer onboarding to rafka who needs to understand what this feature IS, what problem it solves, and how it works internally.

**Required sections:**

- `## What it is` — one-paragraph customer-frame statement. What can a customer DO once this exists?
- `## Why it exists` — the customer / product gap that motivated it. Reference the audit / sprint that filed it if applicable.
- `## How it works` — architectural narrative. Describe the request path, the persistence path, the failure path. Density bar: `docs/architecture/identity-and-scope.md` and `docs/architecture/entity-framework.md` — concrete, citation-rich, no hand-wavy "the system handles X."
- `## Crates involved` — list the crates that contribute (broker / gateway / compute / rafka-rsql / etc.) with one-line rationale for each.
- `## Persistence path` — what state survives broker restart? Gateway restart? Where does it live on disk / in S3 / in IAM tables? Mirror the question §Restart-recovery canary asks.
- `## Surface map` — the operation table from § Surfaces: every operation once, with its wire and REST decoders; a one-surface operation is a named defect, never a skip.
- `## Cross-references` — links to related features in the registry, related architecture docs (`docs/architecture/identity-and-scope.md`, `_archive-docs/docs/_archive_docs/audit/findings.md`, etc.).

### how-to.md — Diátaxis How-to / Tutorial

**Audience:** a customer (developer / operator) who knows what they want to do and needs the concrete commands to do it.

**Required sections:**

- `## Quickstart` — the minimum viable path. 3-5 concrete steps with commands. Persona-framed ("As a developer at Acme Corp, you want to publish events to a topic. Run: `rf produce ...`").
- `## Per-surface walkthroughs` — one subsection per applicable surface (TCP wire via rafka-client, QUIC wire, REST API, `rf` CLI, `rfa` CLI). Each subsection shows the actual command / API call / code snippet that exercises the feature. NO speculative pseudocode; the snippets MUST work against current code.
- `## Common scenarios` — 2-4 named scenarios beyond quickstart. Examples: "Producing with key-based partitioning", "Consuming with a specific group ID", "Inspecting topic metadata via REST".
- `## Troubleshooting` — top 3-5 failure modes a customer is likely to hit (oversized record, invalid partition, topic not found, auth failure, etc.). Concrete error messages + remediation. This is NOT the runbook; it's the "common customer mistakes" pre-emptive doc.
- `## Cross-references` — link to overview.md (for the deeper "why") and runbook.md (for the production failure modes).

### runbook.md — Diátaxis operational How-to (SRE)

**Audience:** an SRE on-call investigating a production incident touching this feature.

**Required sections:**

- `## What this feature does in production` — one-paragraph orientation for an SRE who has never touched the feature before.
- `## Health signals` — what metrics / logs / traces this feature emits. Where to find them in the dashboard / `/metrics`. What "healthy" looks like.
- `## Failure modes` — enumerate 3-7 production failure modes specific to this feature. For each: detection signature (alert / metric / log line), customer impact, root-cause hypothesis ladder, recovery procedure.
- `## Recovery procedures` — concrete commands (CLI / REST / restart sequences) to recover from each failure mode. Include the broker restart procedure (per §Restart-recovery canary persistence guarantees) and gateway restart procedure where applicable.
- `## Escalation` — when to page whom. Which on-call rotation owns this feature.
- `## Related runbooks` — cross-references to other features whose runbooks an SRE will likely read in the same incident (e.g., produce → fetch → storage all link to one another).

### Density bar

The 3 docs together MUST be substantive enough that an engineer reading them cold could (a) understand what the feature is, (b) use it as a customer, (c) recover it as an SRE — without reading the source. Models for the density bar:

- `docs/architecture/identity-and-scope.md` — explanation density.
- `docs/architecture/entity-framework.md` — citation-rich, concrete-example-driven explanation density (absorbed the prior `entity-lifecycle-events-cascade.md` model on 2026-07-24).
- `docs/overview/storage-disk.md` — combined tutorial + how-to density (the prior single-file pattern; the new 3-doc folder pattern decomposes this).

Anti-patterns that fail the bar:

- Stub file with one sentence + "TODO: write this." (Violates no-defer rule.)
- Doc that just restates the registry entry's `description` field.
- Doc that names internal modules without explaining what they do or why.
- Doc that describes surfaces in the abstract without showing the actual command/code.

## Verification

- [ ] Seed assertion (above §Seed spec) passes.
- [ ] **Surface coverage GREEN** (above § Surfaces) — both mod blocks (`mod via_tcp` / `mod rest`) passes against LIVE records under `cargo test --test <slug> -- --ignored`. Every `// no mod <surface>` skip cites a justified reason. Workspace grep across `tests/e2e/tests/<slug>.rs` for mock markers (`with_test_*_mock`, `fake_auth`, `MockBroker`, `stub_broker`, `with_test_subscriber_index`) returns zero hits.
- [ ] **Restart-recovery canary** (above §Restart-recovery canary) — broker AND gateway restart canaries land per the two-boundary rule. Either (a) both `<feature>_state_survives_broker_restart_at_same_data_dir` AND `<feature>_state_survives_gateway_restart` pass, OR (b) the explicit "NO RESTART-RECOVERY CANARY" declaration is present per process boundary AND the reviewer-grep for persistence sinks returns no hits per boundary. There is no third option that ships.
- [ ] **`docs/features/<slug>/overview.md` landed** (Diátaxis Explanation per § Per-feature documentation deliverable). Density meets the bar; cross-references valid.
- [ ] **`docs/features/<slug>/how-to.md` landed** (Diátaxis How-to per § Per-feature documentation deliverable). Per-surface walkthroughs cover every surface from §Surface coverage matrix; commands work against current code.
- [ ] **`docs/features/<slug>/runbook.md` landed** (Diátaxis operational How-to per § Per-feature documentation deliverable). Failure modes + recovery procedures concrete.
- [ ] `docs/features/feature-registry.json` `<slug>.docs` field populated with the three real paths.
- [ ] `cargo check --workspace --tests` — zero errors, zero warnings.
- [ ] `cargo run -p rafka-feature-gate -- check-tags` — clean.
- [ ] If §Restart-recovery canary chose option (a), the file carries `// @requires-test-categories: error-injection` (this becomes a hard-fail check-tags requirement for any file that touches persistence).
- [ ] Per-message observability assertion (golden principle §7) — span chain intact for at least one record.
- [ ] **Jaeger flame-graph trace proof captured and verified** — every E2E test outputs an explicit Jaeger trace URL (`http://<jaeger-host>:PORT/trace/<trace_id>` (the Jaeger UI port, 16687)) proving unbroken cross-process span hierarchy and attribute propagation.
- [ ] Every tier checked above in `§ Test tier coverage` has at least one entry in `docs/features/feature-registry.json` under this story's feature slug(s) — `tests.unit`, `tests.e2e`, `tests.load`, or `tests.chaos` as appropriate. `cargo run -p rafka-feature-gate -- check-tags` exits 0.

### Source-doc propagation (per `feedback_source_docs_update_per_epic.md`)

Epic retro is not complete until every doc that references this epic's scope is updated. `crates/rafka-rsql/docs/architecture.md` + `crates/rafka-rsql/docs/{architecture,timeline}.md` is the canonical model — every RSQL feature lands in all three with SHA citation.

- [ ] Per-crate `timeline.md` bullet added for each crate whose code changed (SHA + one-line behavioral summary).
- [ ] Per-crate `architecture.md` updated if this epic changed the architectural narrative (new primitive, new boundary, new invariant). Skip if purely additive-test-coverage.
- [ ] Product-level spec updated if this epic touches a product surface with its own root doc (e.g., `crates/rafka-rsql/docs/architecture.md`, `docs/overview/virtual-topics.md`, `docs/overview/storage-disk.md`, `docs/overview/callback.md`, `docs/overview/secrets.md`). Add shipped-feature section with SHA; DO NOT leave planned-language stale.
- [ ] Root authoritative docs (`docs/acl-compiler-principal.md`, `docs/schema-registry.md`, `docs/data-quality.md`, etc.) — find the section that described this epic as "planned" / "proposed" and flip to past-tense with SHA citation. When the LAST implementing epic for that doc ships, flip `status: planned` → `status: live` in frontmatter.
- [ ] Cross-reference docs — if any other doc flagged a "gap" / "TODO" / "O(N) scan" / "not yet shipped" that this epic closes, flip the note (e.g., `docs/parent-child-schemas.md §2.4` O(N) note → "FIXED in i4.e2 SHA `<sha>`").
- [ ] Sprint close-out entry in `docs/plans/sprint-<N>.md` includes this epic's SHA + test count + wall-clock in the retro table.
- [ ] If this epic touches a lineage axis (Structural / Execution / Transformational), flip the relevant row in `docs/data-lineage.md` from planned to shipped with SHA citation. `data-lineage.md` is a living doc; every lineage-touching epic updates it.

## § Dispatch ordering — maximum parallelization, 4 seats (REQUIRED — the last substantive section)

The plan the lead dispatches from, decided once here and reviewed, the way `mesh-plan.json` is the
plan the deploy runs from. (User directive 2026-09-09.)

- **Waves.** Up to four epics/stories per wave that run CONCURRENTLY because their file surfaces are
  DISJOINT — name the files per story; disjointness is verified from the tree, never from titles.
- **Dependency edges.** What forces a story into a later wave: a seam it consumes, a ruling it needs,
  a durable move it must land behind. Name the edge, not just the order.
- **Shared files.** Stories that touch one file are ONE seat or sequenced; say which.
- **Durable format moves** are a wave on their own; every live mesh is reborn (down, then deploy —
  never a redeploy in place) after the wave lands, before anything attaches.
- **Critical path.** The longest chain, so the lead knows what to seat first.

| wave | stories (epic.story) | files each touches | why this wave |
|---|---|---|---|
| 1 | | | |

## § Retro (YYYY-MM-DD, on ship)

[Fill in on ship. Required; not optional. This is where the loop closes and future reviewers learn.]

- **What surprised me:** [things the pre-thoughts missed]
- **What I predicted correctly:** [wins from the pre-thoughts]
- **What the seed missed:** [behaviors the implementation added that the initial seed didn't cover — and whether the seed was amended]
- **What I'd do differently:** [process, not code]
- **Shipped SHAs:** [`<sha1>`, `<sha2>`, ...]

## Cross-references

- Upstream dependencies: [list plan docs]
- Downstream consumers: [list plan docs]
