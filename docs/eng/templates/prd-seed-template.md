# [PLAN TITLE] — [one-line statement of what an RDM consumer can do]

> **Template notice:** RDM Seed-First PRD template. Copy to
> `docs/plans/i<N>-<slug>.md` or an initiative's epic/story file. Replace every bracketed field,
> delete this notice, and retain every required section. Use this repository's current APIs,
> scripts and acceptance registry. Verify references before handing back the PRD.

**Initiative / Epic / Story ID:** `[iN / iN.eM / iN.eM.sK]`
**Sprint:** [number or initiative delivery increment]
**Status:** opened [YYYY-MM-DD]
**Architecture doc:** `docs/architecture/[name].md` §[section] — the final contract this work implements
**Signed off:** qa — pending · dev — pending; each lead records their own review and UTC timestamp
**Approved:** pending Luke's approval; replace with Luke's UTC timestamp and the explicit approval evidence when given
**Owner:** [lead and implementing owner]
**Feature:** [behavioral feature name used in estate ownership and acceptance jobs]
**Baseline:** [verified full source SHA; identify any accompanying uncommitted contract changes]
**Record:** [this initiative's epic rows; name the parent PRD whose dependency row must also be updated]

## Context

[Two to four sentences: which embedding application developer or fabric maintainer is affected,
what fails today, and the observable behavior this work delivers. Separate generic fabric/mesh/node
behavior from application-specific policy.]

## Requirements

A PRD is the gap list and build list. Every requirement resolves to **HOLDS** (current code and
proof, with a verified file:line) or **BUILD** (current source anchor, the missing behavior and its
owning epic). A gap without an implementing epic is an incomplete plan. Do not use a disposition
such as “not built,” “not now” or “future work” to preserve an unassigned gap.

Architecture pages state the final contract; code conforms to them. Obtain Luke's approval for
exact architecture changes under the repository rules. Do not present planned behavior as
implemented, passing tests as run, or approval as given. Each epic records its evidence at landing.

| ID | Requirement, in consumer terms | HOLDS / BUILD | Verified current evidence or source anchor | Owning epic |
|---|---|---|---|---|
| R1 | [When the caller does X, the fabric does Y] | [HOLDS or BUILD] | [file:line and what it establishes] | [eN] |
| R2 | [Durability/restart behavior] | [HOLDS or BUILD] | [file:line] | [eN] |
| R3 | [Named refusal and unchanged-state guarantee] | [HOLDS or BUILD] | [file:line] | [eN] |

Use `planned:<path>` for a file an epic will create and `absent:<path>` for an explicitly checked
absence. Every unmarked path must exist. Recheck source coordinates when the baseline changes.
Any newly required harness seam, decoder, backend primitive or test-registration work gets an epic.

## § Open forks — evidence before a ruling

A fork is a consumer-visible decision the current contract does not settle. First read the positive
surface, architecture, relevant rulings in `docs/plans/rulings/`, and completed initiative records.
An already answered question becomes a HOLDS or BUILD requirement instead of another ruling.

| Fork | Positive surface opened and what it does | Architecture search and exact passage | Outlier document and exact conflicting or missing passage | Why the existing contract does not answer |
|---|---|---|---|---|
| [F1: question] | [Node RPC request/handler, client method, storage trait or lifecycle step; file:line] | [`rg -n '<terms>' docs/architecture/...`; findings] | [one document and section the reviewer can open directly] | [specific unresolved choice] |

After the evidence, give the alternatives, consumer cost, recommendation and epic owning the
implementation for each answer. Follow the repository's ruling-packet workflow when a ruling is
needed. If none remains, state why the architecture/ruling already settles the choices; omit empty
fork rows. Never infer absence merely from an unsuccessful keyword search.

## § Seed spec

**Ordering:** for behavior crossing a process boundary, author and prove the restart-recovery
canary RED first, then this happy-path seed RED, before implementation. A RED result must reach the
behavioral assertion; a missing binary, unsupported fixture or compilation failure is not its proof.
For a purely in-process change, give the explicit applicability analysis in the canary section.

### Estate fixture and request

[Name the existing estate fixture to extend or the planned fixture to create, the bootstrap and
Build calls it uses, executable bindings, backend registrations, deployment provider, required
meshes and nodes, and owned artifact/data-dir paths.]

Use `crates/rdm-test/src/estate.rs` (`Estate`, `Owner`, `artifact`, `spans`) and a matching existing
stem as the starting point for live acceptance. Verify each helper against source. Small unit or
functional proofs use the owning crate's test setup; they do not claim to be estate acceptance.

RDM uses Rust estate fixtures and actual Node RPC/client request types. Supply a complete request
or clearly label a fragment and identify the complete request that contains it. Decode examples
against the actual structs before handback. Declare every referenced provider, executable and node;
verify the baseline with `git cat-file -e`. Do not assume an application theme loader or introduce
YAML fixtures without a real loader and an epic that supplies it.

```text
Fixture: [existing path or planned:path]
Request type and decoder: [crate::Type, file:line]
Consumer operation: [complete validated request, or a labeled extension to a named fixture request]
Expected public result: [values and named refusals]
```

### Suite assertion

[Name the exact behavioral test and its file, test target and module registration. Outline setup,
public action, expected values/refusal, restart or fault, complete readback and span assertions.]

A feature stem normally lives under `crates/rdm-test/tests/` and is registered in that crate's
existing test main, unless the current Cargo test target declares another arrangement. Verify
both discovery and execution. Use a behavior-only name such as
`a_caller_[action]_[expected_outcome]`; copy the actual test syntax from the neighboring stem.
Do not ship ignored cells, empty assertions or a passing placeholder as coverage.

### What a passing seed proves

[One paragraph naming the consumer persona, the full workflow and the externally visible
invariants established. State which failure behaviors require additional cells.]

## § Surfaces — one operation and one shared implementation

Specify each operation once. Its Node RPC decoder, typed `rdm-client` method and any shipped demo
adapter use the same behavior, inputs, completion and named refusal semantics. An embedding
application's product API is a consumer of RDM; identify that dependency explicitly instead of
inventing product-specific decoders inside RDM.

| Operation | Node RPC op/request and direction | Typed client or embedding hook | Shared implementation | Eligibility, exact target and refusal |
|---|---|---|---|---|
| [entity.action] | [existing or planned request, caller → fabric-primary → mesh-primary → node as applicable] | [existing or planned method] | [crate/module] | [writer, exact birth/claim, named outcomes] |
| [read] | [read request and serving node] | [client method] | [same view/store read] | [filters, missing result, bounds/completeness] |

- Commands follow the defined fabric-primary → mesh-primary → node path; justify any exception
  from the lifecycle contract. Do not add a second execution engine or application policy to RDM.
- Entity reads and Build evidence remain distinct from execution and resumption. State which
  result proves completion and when that result is durable.
- Transport variation is proved in the transport suite. Do not multiply identical feature tests
  merely by transport. Deployment-provider or storage-backend variations are named explicitly when
  their namespaces, durability or lifecycle behavior matters.
- Live acceptance exercises real processes and public calls. Internal-state inspection belongs in
  functional tests; estate evidence can corroborate a public outcome, not substitute for it.
- Assert exact refusal reasons and unchanged state, not merely “the call failed.”

## § Restart-recovery canary — required applicability analysis, first test

Answer: if the fabric-primary, mesh-primary or ordinary node exits during the workflow, what
consumer-visible state survives, what remains owed, and which process resumes it? Cover clean exit
and forced termination at the relevant persistence boundaries. A thread restart is not a process
restart, and a fresh data dir is not recovery of the previous one.

| Process boundary | State that must survive / be reconstructed | Durable location and owner | Exact canary and public assertion |
|---|---|---|---|
| fabric-primary | [accepted Build, attempt/decision/receipt or applicable fact] | [provider/log/data-dir file] | [test name] |
| mesh-primary | [mesh and node records, in-flight work or applicable fact] | [provider and owner] | [test name] |
| ordinary node | [connections, local identity, pending work or applicable fact] | [provider/local file] | [test name] |

For each affected boundary, retain its data dir, storage provider and keys as the contract requires;
capture the complete public result, terminate the real process, start recovery, then compare the
entire result. Also test successor recovery where the feature depends on another birth continuing
an attempt. Distinguish the two cases explicitly.

Give each boundary one of these answers:

1. **Persistence/recovery required:** a named RED-first canary and complete post-restart assertions.
2. **No durable effect at this boundary:** explain the pure computation or derived-state behavior,
   verify persistence sinks and owned state in the diff, and name the reconnect/reconstruction test
   if consumer behavior still crosses restart. A newly introduced write invalidates this answer.
3. **Required harness primitive missing:** assign that primitive to an epic here, write the real
   canary as soon as it exists, and keep feature acceptance open until it passes. A passing partial
   or stub test never establishes restart durability.

Canary names include `survives_[process]_restart` and identify retained-storage requirements.
If 100 records were acknowledged, verify all 100 and their ordering/values as promised. Assertions
through public reads accompany functional persistence evidence; sampling cannot prove durability.

## § OTLP span-assertion bar

Each live acceptance cell captures the production `rdm.*` spans, asserts the required attributes and
causal parentage, retains JSONL evidence and prints a direct Jaeger trace link. Use the actual estate
`spans()` and `artifact()` helpers and evidence directory handling; do not invent capture APIs.
Collector/Jaeger addresses come from the environment and repository configuration, not a hardcoded
machine address in the PRD. The same trace id connects the request, downstream steps and persistence.

Names follow the repository's vocabulary and name checks: entity, action, `via-` direct reason or
`from-` consequence as appropriate. Verify against neighboring production spans and
`scripts/name-check.py`; do not copy another product's span prefix or assume missing style guides.
A test name describes behavior; the target/module provides its location.

| Span | Emitting process/path | Mandatory attributes | Cell and observable control |
|---|---|---|---|
| [rdm.component.entity.action.via-reason] | [verified existing site or planned site] | [build/attempt/subject/birth/provider as applicable] | [test; control span proves the request reached the path] |

A claimed absence needs a positive control: no stale mutation is proved by an attempted stale call,
its named refusal and the preserved public value. A quiet log or a fault firing alone proves none
of those. Keep credentials, secret values and protected application payloads out of diagnostics.

For structural changes that add no emit site, explicitly state **NO NEW SPANS**, name the existing
spans proving behavior, and check the diff for emit sites before accepting that declaration.

## § Per-increment seed-data obligation

A new record kind, provider, field or command needs a fixture that actually creates it. Record its
schema/decoder, fixture setup, seeding owner and public assertion in the implementing epic. Include
historical decode cases where compatibility is promised. The fixture must load on every deployment
provider that the PRD claims; unsupported infrastructure gives a named failure, never a green skip.

If no entity or record shape changes, state **NO NEW ENTITY**, identify the changed behavior, and
verify the diff supports that claim. Do not add a separate fixture language or registry solely to
satisfy a section heading; use the repository's existing estate and acceptance registration.

## § Test tier coverage

Tick the required tiers and fill every selected row. Selection is a delivery commitment, not a
claim that the tests have already passed. Name planned files with `planned:` until they exist.

| Selected | Tier | Exact test file / target / module filter | Proof |
|---|---|---|---|
| [ ] | unit | [owning crate inline/module test] | [pure rule/decoder] |
| [ ] | functional | [owning crate test] | [real local persistence or adapter contract] |
| [ ] | e2e | [estate stem and test-main registration] | [public workflow over real processes] |
| [ ] | load | [registered sustained-activity cell] | [bounded resources and ongoing activity] |
| [ ] | chaos | [registered delayed/lost-request or process-death cell] | [specific recovery invariant] |

### Fast smoke, release and scheduled coverage

Name a short smoke covering the main public workflow and register it in the fast gate. Put long
failure, production-cadence, container and sustained tests in the release jobs according to the
existing gate's rules. Changing coverage must preserve existing checks; the smoke complements the
full suite. Record the actual nightly configuration and verify its active schedule and discovery.
If no nightly runner exists, assign the gap to an epic and resolve it; do not claim it was retained.

RDM's runners are:

```sh
# Run from the implementation checkout; replace every argument with its verified value.
scripts/cells.sh <package> <target-or-lib> -- <module-filter>
scripts/rdm-gate-at.sh <checkout-name> fast
scripts/rdm-gate-at.sh <checkout-name> release
scripts/i143-acceptance-gate.sh <registered-job-id>
```

`tools/mesh-audit/i143-acceptance-jobs.json` registers acceptance commands and evidence. Use the
existing runner/receipt schema; do not add another generic runner. Acceptance evidence includes
source SHA, test discovery, result and estate manifest where applicable. A zero-test selection,
ignored cell or absent required provider cannot count as a passing run. Build through the existing
scripts; do not introduce a different per-package build selection or export CARGO_TARGET_DIR.

## § Pre-implementation thoughts ([YYYY-MM-DD])

- **Hard part:** [the contract boundary most likely to fail]
- **Likely failure:** [specific ordering, ownership, restart or durability defect]
- **Persistence path:** [fact → writer → provider/log → acknowledgement → restart reconstruction]
- **Dependencies:** [landed or BUILD primitives, exact plans/epics and interfaces consumed]
- **Architectural risks:** [single writer, claim exclusivity, exact birth, cache ownership, retries]
- **Uncertainty to resolve:** [source audit or executable experiment and its owning epic]

## Implementation plan

[Number the epics in dependency order. The final epic supplies the feature documentation and
reconciles source docs, with implementation knowledge fresh. Contract text and its implementing
code land together under the repository rules; a final documentation epic is not permission to
leave an earlier code landing inconsistent with its contract.]

### e[N] — [consumer-visible outcome]

- **Goal → gap:** [architecture section delivered; exact behavior absent in current code]
- **Detailed requirements:** [requirement IDs; verified file:line for existing code; planned paths
  for additions; explicit ownership, ordering, completion, durability and named refusal rules]
- **Spans:** [each emitted/asserted span and mandatory attributes; control for absence assertions]
- **Testing by tier:** [files, test target/module registration, fixture, RED proof and GREEN proof]
- **Acceptance tests by name:** [behavior-only names and complete expected outcomes]
- **Estate plan:** [topology, participating kinds, exact executable bindings/backend registrations,
  deployment provider and reason each resource is required; existing setup helpers and invocation]
- **Persistence/restart:** [affected processes, retained data dirs, fault instant, expected resumption]
- **Sizing/file ownership:** [one or more implementers and exact files; shared edits sequenced]
- **Rows absorbed:** [requirements and verified linked issues; do not invent issue numbers]
- **State/evidence:** [BUILD initially; RED/GREEN SHAs, test count, elapsed time, artifacts and trace
  at landing; every remaining requirement still has an owner]

Declare the highest topology requirement once per epic from the actual cells. Reuse its fixture
for smaller cases. Record the realized shape, bindings and provider in estate artifacts. A test
whose subject is topology variation may explicitly exercise several shapes; otherwise do not
change fixtures mid-epic without the lead resolving the resulting dependency. Use existing RDM
estate setup and its rung vocabulary; do not copy an external product's fixed broker/compute
counts, deployment flags or mesh-plan generator.

### Files likely to touch

- `[existing path]` — [why; verified source coordinate where relevant]
- `planned:[new path]` — [creating epic and reason]

## § Per-feature documentation deliverable — final epic

Three standalone documents, written by the implementer with working examples and actual evidence:

```text
docs/features/<feature>/overview.md
docs/features/<feature>/how-to.md
docs/features/<feature>/runbook.md
```

Use `planned:` for those paths in a new PRD until created. Record the paths in the initiative and
applicable acceptance metadata; do not assume a feature registry that the repository does not use.

- **overview.md:** what the feature enables, why, request and persistence paths, failure/recovery
  behavior, participating crates, public API map and architecture cross-references.
- **how-to.md:** executable setup and client/Node RPC examples; prerequisites, expected readbacks,
  named refusals and common consumer mistakes. Run examples against the landing revision.
- **runbook.md:** health spans, how to obtain the trace, failure signatures, exact fact/decision to
  inspect, restart/adoption/resubmission/delete procedures as applicable, and escalation ownership.

Together the docs let a consumer use the feature and a maintainer recover it without reading the
implementation. No placeholder procedures, imaginary methods or unverified addresses. Point to
existing `docs/eng/` runbooks when they provide the shared procedure and explain the feature's part.

## Verification

- [ ] Every requirement is HOLDS with proof or BUILD with an implementing epic.
- [ ] Every fork includes positive source evidence, the outlier passage, arms and recommendation.
- [ ] Fixture/request types and examples checked against actual source; baseline SHA resolves.
- [ ] Required process-recovery canary RED first; seed RED before implementation; both GREEN at acceptance.
- [ ] All affected restart boundaries covered; any inapplicability has a verified reason.
- [ ] Public calls use one shared implementation and identical outcomes through shipped adapters.
- [ ] Precise refusal reason and unchanged-state assertions accompany failure coverage.
- [ ] Selected tiers have executable registration; test discovery proves the named cells run.
- [ ] Fast smoke passes; full release coverage passes; nightly configuration/schedule/discovery verified.
- [ ] Process/container or backend variants promised by this PRD pass without green skips.
- [ ] Complete history/current-state/retry guarantees survive the specified process failures.
- [ ] Production span assertions and causal Jaeger trace links retained with JSONL artifacts.
- [ ] Appropriate workspace compile checks, repository name checks, fast/release and acceptance jobs pass at the recorded SHA.
- [ ] All three feature docs land; examples run and local links resolve.
- [ ] Architecture and code agree; durable/wire-format changes follow the marker and compatibility rules.
- [ ] QA/dev each record their completed adversarial review; approval is recorded only when given.

### Source-doc propagation

- [ ] Update the owning architecture page and lifecycle pages/index for changed behavior.
- [ ] Update `docs/eng/consumer-guide.md`, applicable runbooks and CLAUDE.md env table.
- [ ] Regenerate affected wire/span references through the existing generator.
- [ ] Update dependent proposals/rulings/initiative rows with explicit state and landing SHA.
- [ ] Search other docs referring to the changed API or gap; reconcile those passages.
- [ ] Record test counts, duration, failure investigation, traces and scheduled-run evidence in the epic.
- [ ] Update existing per-crate docs where affected; do not assume nonexistent timeline or product docs.

## § Dispatch ordering — last substantive section

Plan up to four concurrent implementers only where file surfaces are disjoint. This section is a
plan, not permission to start tasks. Identify dependency interfaces, shared-file sequencing,
format-change waves and the critical path. Follow the existing checkout/landing workflow in
CLAUDE.md. Never create a new worktree merely because this template mentions parallel work.

| Wave | Epics/stories | Exact files owned by each | Dependency / why disjoint or sequenced |
|---|---|---|---|
| [1] | [eN.sN] | [paths] | [reason] |

**Critical path:** [epic dependency chain and blocking interfaces]
**Format changes:** [marker/compatibility decision, fresh-estate requirements and subsequent checks]

## § Retro ([YYYY-MM-DD], on ship)

- **What surprised us:** [actual defects and evidence]
- **What we predicted correctly:** [failure cases the original plan caught]
- **What the seed missed:** [added behavior/cell and whether the seed was strengthened]
- **What we would change:** [specific process improvement]
- **Shipped SHAs:** [exact commits]
- **Verification:** [counts, duration, artifacts, Jaeger links and nightly run evidence]

## Cross-references

- Contract: [owning architecture and lifecycle sections]
- Rulings: [applicable numbered rulings]
- Dependencies: [initiative/epic documents]
- Consumers: [embedding-application integration record, if affected]
- Repository workflow: [CLAUDE.md](../../../CLAUDE.md)
- Public API guidance: [consumer guide](../consumer-guide.md)
