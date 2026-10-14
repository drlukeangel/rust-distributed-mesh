# i141 / i143 convergence audit — 2026-10-07

Node RPC codes and schemas are defined in the [envelope ledger](../architecture/node-rpc-envelope.md#the-op-ledger).

## 1. Executive verdict and evidence boundary

**Repo fact:** initiative order is **i143 -> i141 -> i142**, not i141 -> i143.
`i143-node-rpc-pos-on-RDM.md`, i141 PRD §0 and i142 PRD §0 agree: RDM builds/proves/exports;
Rafka imports/binds; product families migrate afterward. i143 is already implemented in part.
The request's literal “141 exit / 143 entry” gate contradicts that dependency graph.
This audit does not reverse it or start i143 implementation.

**Executive verdict:** structurally yes; implementation and proof are not yet complete.
**Architectural inference:** the ownership structure supports finishing the Ops substrate,
but current i141 cannot claim the 2026-10-07 lock complete. Its existing pin predates the open
status/drain/storage/type lock stories. The meaningful gate is the **next i143 export / i141
import completion**, followed by the i142 product migration entry gate.

Inspected baseline: Rafka `dc31f73b9e`, RDM `d73d021`; both matched fetched origin/main at
session start. Rafka Cargo.toml pins eight packages to
`a213a6a25d0e6e32a046a5b1850b08239b669314`; Cargo.lock resolves that source. RDM's subsequent
two commits relocate documentation, not the pending contracts. GitHub issue bodies/states were
read on 2026-10-07 using `gh`, including #2880, #2911 and #2923–#2927. Closed issues and older
progress-table GREEN claims are historical evidence, not proof of later requirements.

References below are repository-relative. RDM paths are prefixed `RDM:`; other paths are in
Rafka. The upstream issues actually live in `drlukeangel/rafka-v2` (the RDM issue listing returned
no rows). No external issue was created or edited.

## 2. Accepted architecture already present

| Locked decision | Verified implementation or normative owner | Qualification |
|---|---|---|
| Fence/context/body postcard request; typed replies | RDM: `crates/rafka-node-rpc-contract/src/framing.rs::{Fence, RequestHeader, encode_request, parse_request_target}`; `dispatch.rs::RequestAssembly` | Fence has only target NodeId string and Op byte; FIN required before dispatch. |
| Context is observability only | RDM: `context.rs::CallContext::sanitized`; `crates/rafka-node-rpc/src/server.rs::{apply_context, invocation}` | Encoded sections are bounded; malformed individual values are dropped. Context is separate from the request. |
| Op family semantics | Fence `op`, core ledger and sealed catalog in RDM contract | Rust public names still use TAG/TagOwner/TagState; #2927 owns convergence. |
| Ledger vs catalog | RDM: `catalog.rs::{TagState, EntryKind, CatalogBuilder::seal, SealedCatalog}` | Live/Retired allocation is separate from Canonical/Transitional serving. Retired 0x11 and 0x16 cannot seal. |
| One effective Rafka catalog and process client | `crates/rafka-node-base/src/{catalog.rs, node_rpc.rs}::{LEGACY_ADAPTERS, server, client}` | Canonical Ping/Forward coexist with explicit i142 adapters. Rafka resolver remains incomplete, below. |
| Ping replaces Echo | RDM: `ping.rs`, `catalog.rs::core_ledger`; Rafka: `node_rpc.rs::ping` | 0x01 live, 0x11 retired. Old Echo doc/test labels are not current reachability policy. |
| Forward has no second context | RDM: contract `forward.rs::ForwardRequest`; runtime `forward.rs::carry_once` | Copies the received, sanitized outer context into inner CallOptions; valid context passes unchanged. |
| Forward preserves final target and certainty | RDM: `forward.rs::{call_via, carry_once}`; `outcome.rs::carried` | One direct inner invocation; Forward itself is non-forwardable. |
| Typed identity vocabulary exists | RDM: `crates/rafka-mesh-entity/src` NodeId/MeshId/FabricId/IncarnationId, NodeKind | Status does not yet use those identity types; no new wrappers needed. |
| Exact runtime identity/provider domain | RDM: `runtime.rs::{RuntimeFact, RuntimeLocator, validate}`; provider `adopt` | PID+start or immutable container id plus domain; fact bound to birth by membership, not redundant NodeId fields inside fact. |
| Operational metadata distinct from identity | RDM: pipeline `Publication`, model `Node::data_dir`, current digest projection | Operational data_dir exists beside RuntimeFact; named RuntimeMeta owner is pending migration. |
| Provider executes resolved operations | RDM: `deployment/provider.rs::{ResolvedNodeLaunch, DeploymentProvider, adopt}` | No desired NodeMeta/Tag policy API. Storage release currently happens directly in pipeline; future concrete storage action still pending. |
| Build is accepted topology owner | RDM: `accepted.rs::{FabricTopology, MeshTopology, AcceptedStore, compile}`; `build_state.rs::BuildAccepted`; `fabric_storage.rs::FabricRecord` | Current topology is exact path set, not explicit per-path NodeMeta yet. |
| Lifecycle routing overlay is separate | RDM: `build_state.rs::in_flight_ops`, mesh entity lifecycle/departure facts; `fabric-mesh-ops.md` §§5–7 | Lifecycle operation in_flight is an overlay list; it is not the drain work counter. |
| Certainty and natural status idempotency | RDM: contract `outcome.rs`, `status.rs::transition`; admin `status_rpc.rs`; `tests/status_declarations.rs` | Existing declaration matrix and reply-loss tests exist; new downward operations still absent. |

The desired storage model and successful typed RPC drain are **architecture targets**, not
implemented facts. The documents now say so explicitly.

## 3. Actual remaining gaps and owners

“Blocks” below means the next locked export/import completion, not stopping an already-running
i143 initiative.

| Gap / exact location | Why it fails the lock | Existing owner / blocks |
|---|---|---|
| RDM contract `status.rs::StatusRequest` has four variants with String identities; StatusReply has generic String stale/transition evidence | No ApplyNodeState, ProbeNodeState or NodeDrainingApplied; narrow typed stale evidence absent | [#2923](https://github.com/drlukeangel/rafka-v2/issues/2923), upstream i143; yes |
| RDM `admin.rs` offline `kick_of` and testkit `node.rs` status handler use DeclareNodeState downward | Violates the upward-only declaration contract; grep found sender + testkit receive arm, with status declarations and status_declare callers requiring coordinated updates | #2923 / #2805; yes |
| RDM `protocol.rs::NodeProtocol`, `catalog.rs::CatalogEntry`, server invocation have no status-specific draining-admission mechanism | Applying drain would otherwise prevent future status observation/mutation; server guard currently generic | #2923 with #2927 ratchets; yes; test full/coalesced and fragmented requests; API name remains an implementation choice |
| RDM `deployment/pipeline.rs::retire`, MarkDraining signals provider; WaitForDrain errors on deadline before termination | No typed RPC-first drain; a wedged node can prevent bounded authorized retirement | [#2924](https://github.com/drlukeangel/rafka-v2/issues/2924); yes |
| RDM `admin.rs::drained` reads `d.extra["in_flight"]`, missing value treated as zero | Missing/untyped counter is not established typed drain evidence | #2924/#2926; yes |
| RDM `build_state.rs::BuildStepReceipt` outcome/data representation and pipeline receipts lack typed DrainOutcome | Completed/failed step or free-form data does not identify the arm permitting termination | #2924; yes; durable receipt migration must be specified |
| RDM `accepted.rs::MeshTopology` only name + path set; pipeline Create/Retire and BuildOperation use old storage behavior | No per-path NodeMeta or explicit storage policy; `permanent` currently gates directory deletion | [#2925](https://github.com/drlukeangel/rafka-v2/issues/2925) then [#2926](https://github.com/drlukeangel/rafka-v2/issues/2926); yes |
| Rafka `node-admin/src/main.rs::{reconcile_stateful, authoritative_stateful}` and launch fields; RDM model/digest strings | Runtime/launcher state and booleans still decide behavior instead of canonical typed desired owners | #2925/#2926, i141 #2918/#2929/#2920 bindings; yes at import cut |
| Rafka `node_rpc.rs::HeardBirths::resolve` sets incarnation to empty String | Same-endpoint restart cannot supply truthful current-birth pool identity; this is explicitly admitted in its source comment | i141 #2929/#2879, existing restart canaries #2871/#2872 and PRD §15; yes |
| RDM contract/runtime TAG names; normative old wire statements in long-form docs | Op semantics implemented partially, public vocabulary and authoritative text diverge | [#2927](https://github.com/drlukeangel/rafka-v2/issues/2927); yes for normative lock ratchets; historical text exempt |
| Old i143 handoff points to removed docs/i143; progress reports open election work despite closed #2840 | Relocation/progress lag can send implementation to obsolete authority | #2789/#2790 documentation/export; references fixed here; stale progress is not a new epic |

The catalog's legacy handlers, Rafka stateful launch behavior and 0x12 codec are real current
implementation, but migration-owned. Their presence before the owning cut is not by itself proof
of architectural failure. Completion claims must identify which cut was actually reached.

## 4. Conflicts requiring explicit reconciliation

1. **Initiative order:** the prompt's 141-before-143 sequence conflicts with all three PRDs.
   Repo ownership is authoritative; this patch preserves it and supplies the corrected gate.
2. **Wire authority:** `node-rpc-rdm-ownership.md` claimed precedence while its §5 still put
   target_slot/target_freshness on wire. `node-rpc.md` §§9–10/38 and handoff §1 also retain older
   wire text. The envelope is now explicitly the wire authority; local freshness remains valid.
   Notices preserve older migration/historical text pending #2927 rather than renaming blindly.
3. **Status reseal:** envelope requires ProbeNodeState plus separate incarnation/mesh/fabric stale
   rejections. #2923's body still names RejectedStaleBirth and does not explicitly include Probe.
   Align that existing story before it lands; this audit does not edit GitHub or silently keep
   both schemas normative. No evidence found that the new schema is already frozen in code.
4. **Versioning:** `node-rpc.md` §9 generally requires a new family code for incompatible changes;
   #2923 explicitly requires one coordinated 0x1B reseal before the next pin. Treat this as that
   named pre-pin cut, with both endpoints upgraded together, not general permission for future
   incompatible edits under 0x1B. Deployment compatibility must be evidenced on landing.
5. **Storage retirement:** old `ReleaseStorage if permanent` conflicts with Preserve retirement.
   #2925 explicitly settles StorageMeta and ingress defaults. Pipeline docs now name the typed
   disposition while reporting implementation pending. No new per-kind default was invented.
6. **Pin evidence:** #2880 is closed and a pin exists, while later lock B–F issues are open.
   This is not proof that the pin is bad for its historical gate; it is proof it cannot implement
   variants absent from its source. Current final export eligibility is not established here.

PlacementMeta's exact fields and a version identifier for the per-kind default table were not
established from the inspected definitions. #2925 must supply them; this patch invents neither.
FabricEvent.initiated_by / NotAuthority sender fields must be classified by actual meaning
(path, endpoint, or logical id), not mechanically changed to NodeId.

The product `MeshOpRequest::OP_CODE` / DataFrame operation byte is a broker operation *within*
the 0x12 family. `ops-protocol-and-usage.md` owns that existing payload vocabulary. It must not
be mechanically renamed into the outer Fence Op or given a second family allocation.

The envelope previously said baggage contained “allowlisted keys only,” while `context.rs`
and its functional tests propagate the whole valid value and allowlist only span attributes.
That bounded observability wording is corrected in this patch; no context codec change is needed.

## 5. Ordered document finalization

This is a dependency order for the next export/pin. Independent documents can be reviewed together;
the ordering is not a claim that implementation must stop until every prose edit lands.

| Order / document | Exact changes, kind and why before dependents | Implementation/tests depending on it |
|---|---|---|
| 1. `node-rpc-envelope.md` | Normative wire/status schema, drain success vs refusal, status admissible while draining, Op family and control-plane metadata cross-reference; resolve #2923 mismatch and freeze positional order first | contract/status codec, both endpoints, testkit, postcard fixtures, bounded sections |
| 2. `node-rpc-rdm-ownership.md` + `node-rpc.md` | Normative precedence and family ownership first, then fold §§3/8–10/21/38/49 legacy wire/header/catalog descriptions under #2927; preserve migration-source examples | sealed catalog, pool fences, Forward/context, direction/auth and certainty tests |
| 3. `build.md` and `deployment-provider.md` | Normative deployment (deployment-provider.md §2) / accepted Build (build.md §§1–4): explicit NodeMeta, ingress-only normalization, provider concrete storage action, typed drain receipt and bounded failure arms; resolves policy before lifecycle recipes | #2925 normalization/cardinality/journal tests, #2924 pipeline tests |
| 4. `fabric-mesh-ops.md` | Normative §§3–4/9: RPC drain -> typed evidence -> bounded wait -> termination/inspection; counter and lifecycle overlay are separate; uses the prior contracts | node delete/restart/replacement cells, exact-birth races and all drain arms |
| 5. `fabric-authority.md` | Normative §12 status cross-reference/result names; retain §§2/8/13 topology and hydration rules, which need no new architecture | Pending gate, authority movement and recovery tests |
| 6. `data-plane-transport.md` | Migration documentation §§2–3: label legacy data-plane framing and retired Echo, separate canonical ALPN from adapters; no redesign of DataFrame | i141 catalog/coexistence tests; i142 U6 data migration |
| 7. `i143-prd.md`, `i143-design.md`, export index, then `i141-node-rpc-substrate-prd.md` | Migration planning: fold lock B–F and exact export SHA/evidence first; then import bindings/gates. Do not derive eligibility from stale progress | full upstream gate, single-revision pin, downstream canaries |

No wholesale lifecycle hierarchy, election, connection-route, provider-locality or data-plane
redesign is required by this audit. Full terminology fold remains #2927 after typed owners.
The patch makes authority/boundaries explicit now; it does not claim every older example is folded.

### Proposed status freeze, not a code reorder

Envelope request discriminants 0–5: DeclareNodeState, DeclareMeshState, ApplyNodeState,
ProbeNodeState, ApplyMeshState, ApplyFabricEvent.

Reply discriminants 0–15: Applied, AlreadyApplied, NodeDrainingApplied, Current,
RejectedStaleIncarnation, RejectedStaleMesh, RejectedStaleFabric, RejectedNotAuthority,
RejectedInvalidNodeTransition, RejectedInvalidMeshTransition, PeerUnresolved, NotReady, Busy,
Draining, Malformed, Unauthorized.

**Repo fact:** current code is 4 requests/12 replies, Current last. **Recommendation:** retain the
envelope's proposed grouping for the single coordinated reseal, because it separates directions
and typed success/evidence; byte fixtures must lock nested enum order and field order too. No
existing code enum is changed by this documentation pass. After the new pin, append variants only.

The actual compatibility mechanism found is an exact git revision for all eight imported crates,
with positional postcard enums, manually declared REQUEST_VARIANTS/REPLY_VARIANTS and codec
round-trip tests. No separate negotiated 0x1B version, schema digest pin or comprehensive status
byte-golden suite was found. Round trips alone cannot detect coordinated accidental reorderings.

## 6. Additional i141 issues: none justified

Existing issues already own the gaps. Do not create duplicate epics. Extend acceptance in the
existing owners when they are reconciled; the rows below are recommendations, not new filed work.

| Candidate | Disposition and required acceptance |
|---|---|
| 0x1B reseal, Probe kick migration, typed identity/evidence, direction matrix | i143 #2923/#2805; include first/repeat drain counts, every new variant/refusal, exact incarnation, wrong sender/receiver/seat, authority movement, no side effects on refusal |
| Drain evidence / failure arms | i143 #2924; Established/NotSent/Indeterminate/Deadline in durable receipt before terminate; no forever wait; exact-runtime inspect required; silence-only desired Running negative control |
| Exact-birth races | Existing i143 pool/replacement/status proof + i141 #2871/#2872/#2873/#2874; stale dial/pooled birth and reused provider locator must never act on replacement |
| Forward context | Already implemented; RDM context.rs functional test `a_carried_call_keeps_the_origins_trace_and_caller_system_through_the_hop`; #2927 ratchet, downstream consumes it |
| Section bounds/resource safety | Existing framing/dispatch/admission tests plus #2927 postcard_section_bounds ratchet; exercise boundary sizes, overflow, incomplete FIN, coalesced and fragmented reads; no new i141 epic |
| Authorization/idempotency/certainty | Existing `status_declarations.rs` covers old upward contract and reply loss; expand under #2923 for new directions, drain repeats and stale typed refusals |
| Build per-path NodeMeta/provider boundary | i143 #2925/#2926 plus Rafka shape adapter and i141 #2918/#2920; exact path/meta key coverage, explicit values post-acceptance, durable additive format and normalization precedence tests |
| 0x12 product codec/forward/retry | i142 U6; no generic data-plane implementation in i141 |
| Legacy vocabulary/type sweeps | #2926/#2927; scope excludes historical artifacts and separately owned product adapters |
| Generated catalog manifest | No required generated-manifest mechanism found. Existing core ledger, seal tests, export manifest/parity #2790 suffice; do not invent a generator as a gate |

Dependencies: #2923 before #2924; #2925 before #2926; typed owners before #2927; all relevant
upstream lock evidence before a new import pin. Tests touch the contract, RPC server/client,
testkit, admin core, accepted topology/journals and Rafka adapter/canary code already named above.
Out of scope: algorithm copies in Rafka, new transport/auth stacks, a second desired topology,
new data-frame format, generic retry of Indeterminate mutations, unrelated feature cleanup.

## 7. 0x12 data-plane assessment

**Determination C:** transitional product work belongs to **i142 U6**, and must not become a
precondition for starting i143 or an extra i141 data-plane epic. This is not an assertion that
0x12 already meets every canonical RPC safety/compatibility guarantee. Import coexistence must
remain safe and explicit, and U6 must close the product gaps before claiming its own completion.

| Question | Actual current answer and evidence |
|---|---|
| Framing | `data_rpc` / `bi_rpc_send` writes byte 0x12 then raw encoded DataFrame, finishes request; reply is raw operation-specific bytes to FIN. This is not the three-section postcard envelope. `run_bi_reader` buffers to FIN and feeds broker channel; broker's generic handler loops over length-described DataFrames, so “one request” is sender convention, not a one-frame cardinality validator. |
| Schema | `crates/rafka-mesh-ops/src/frame.rs::DataFrame::{encode, decode}`: org UTF-8 u16 LE length + bytes; 97 fixed bytes: topic id 16 bytes, partition u32, operation u8, flags u8, trace id u128, idem-key length u16, payload length u32, timestamp i64, writer id 16 bytes, writer incarnation u32, partition sequence u64, parent span u64, trace flags u8, record count u32, WAL shard slot u32; idem-key bytes then payload; optional u16-length baggage trailer. Integers LE. Minimum header 99 bytes. Payload/reply schemas are per-op in mesh-ops/src/ops. |
| Version/compatibility | No general frame version field/negotiation found. `rafka-mesh-v1` ALPN and adapter op allocation identify transport/family, not a independently negotiated DataFrame schema. Codec and product deployment history establish current shape; old missing-shard-slot shape rejected by a dedicated test. U6's format move/full rebirth rule is in node-rpc.md §9 and i142 §9. |
| Bounds | node-base MAX_MESH_FRAME_BYTES = 64 MiB; 0x12 body read capped at that minus the tag byte. Reply cap 16 MiB. Broker checks idem+payload against 64 MiB before tail allocation; that check alone excludes header/trailer, unlike the outer total cap. Org/key length u16; baggage max 512 bytes. These are actual different measurement points, not one universal full-frame bound. |
| Malformed input | Outer TooLong stops/resets 413; absent handler and unserved family have named resets. Broker header/tail short reads and oversized declared tail break the loop; decode failure writes status byte 1 then stops parsing. broker/main.rs flushes any response and FIN even on handler error. Some malformed/truncated inputs therefore produce empty FIN rather than typed Malformed. |
| Admission/backpressure | Per-connection read semaphore defaults 16, configured by RAFKA_MESH_FEED_CONCURRENCY. Tasks are spawned before awaiting permit; permit ends after channel handoff. DATA_FRAME channel is unbounded and broker spawns a task per frame. This bounds concurrent body reads, not total queued tasks, bytes, or handler execution. No canonical per-protocol Busy/draining accounting here. QUIC flow control is not equivalent to those limits. |
| Termination | Sender FIN defines complete request; reset/incomplete read never feeds channel. Broker flushes response/FIN. Its parser can loop over multiple frames; no canonical streaming terminal-frame algebra on the adapter. |
| Certainty/retry | node-base `DataRpcError` distinguishes ConnectFailed, IoError, NotDispatched, SlotAcquireWait and EndpointSuperseded. `data_rpc_dispatch`, unfinished-send reset 499 and writer split budgets exist. No silent general replay after request commit; open_bi recovery is bounded. Reader/writer domain settlement remains its own policy (broker-write-execution.md and i142 U6). This legacy algebra is not automatically the canonical typed RpcOutcome. |
| Tests | frame.rs round trips all fields, malformed/truncated input, old missing WAL shard slot, baggage and identity-byte invariants, byte-equivalence encode matrix; node-base data_rpc_wire and unfinished-send/cap/reset tests; broker mesh-manager oversized-tail tests; individual ops have byte goldens. No complete whole-DataFrame version/golden compatibility matrix or dedicated frame fuzz target found; tests/fuzz targets do not establish that coverage. |
| Migration owner | `catalog.rs::LEGACY_ADAPTERS`: 0x12, 0x17 and 0x18 -> i142 U6; 0x19 -> U6 §9.4 audit. Generic RDM's proof product broker_data at 0x20 is a proof family, not the migrated Rafka 0x12 codec. |

**Recommendation to the existing U6 owner:** preserve operation semantics, test exact boundary
sizes and malformed empty-FIN behavior, bound queued bytes/tasks and executing handlers, document
format-move/deployment compatibility, preserve split writer certainty, and compare canonical
request/reply caps with the current 64/16 MiB product contract. No new data-plane architecture or
new 141 issue is proposed here.

## 8. Testable exit / entry gates

### Literal requested gate

“143 may begin when 141 exits” cannot be made truthful: i143 already precedes i141. The following
are the actual dependency gates, not a renumbering of the initiatives.

### Next i143 export / i141 locked import may complete when

- [ ] #2923's merged request/reply types match one reconciled envelope schema (including Probe,
  narrow typed stale variants and status served during drain); every positional variant/field
  and nested enum has reviewed byte fixtures on that revision.
- [ ] All current offline kick send/receive paths use ProbeNodeState; Declare remains upward;
  direction/auth, stale exact-birth, repeat drain count and generic Draining refusal tests pass.
- [ ] #2924 merged pipeline attempts RPC drain before terminate for believed-live targets;
  all four typed drain arms persist before terminate, and tests prove bounded progress plus
  exact provider terminal inspection and the silence-only drift negative control.
- [ ] #2925/#2926 accepted topology carries explicit per-path NodeMeta; normalization precedence,
  selected/versioned kind defaults, cardinality coverage, restart/retire storage dispositions,
  durable journal changes and policy-blind provider tests pass on both sides of the binding.
- [ ] #2927's planted-violation ratchets pass on that merged revision; normative wire text
  contains no competing slot/freshness/tag-byte or Echo contract. Historical/product adapters
  remain clearly scoped to their owners.
- [ ] Existing export/parity/manifest gate #2790 has logs tied to the same exact 40-hex SHA;
  all eight Rafka packages and Cargo.lock use that SHA. Package version 0.1.0 alone is not proof.
- [ ] Existing i141 RuntimeFact/incarnation/provider/hydration bindings and #2871–#2874 canaries
  pass; HeardBirths no longer substitutes an empty incarnation for the exact current birth;
  #2920 removes/delegates the duplicate generic decision paths named by the PRD.
- [ ] Catalog coexistence tests show every surviving Rafka legacy adapter has its explicit i142
  migration owner and absent/retired ops refuse by name, with no uncatalogued fallthrough.

### i142 product migration may enter its canonical integration cut when

- [ ] Its existing i141 import prerequisite is met with the pin/binding evidence above; a U6
  product format move and retained-op deployment compatibility plan are explicit before traffic.

143 does NOT need to wait for:

- i142 U6 migration of 0x12/0x17/0x18, or U6's 0x19 deletion/retention audit;
- i142 U1/U2 credential/snapshot/control or U3 compute provisioning cleanup;
- rewriting archived story transcripts, old `TAG_*` compatibility examples or historical tests;
- a newly invented generated catalog/schema manifest or unrelated application feature cleanup.

These exemptions do not relax existing i143 export dependencies or i141's accepted PRD.

## 9. Search classification

Searches covered active architecture/plans and both implementations for TAG_, protocol_tag,
protocol.tag, UNSERVED_TAG, tagged_bi_rpc, target_slot, target_freshness, STALE_SLOT, stateful,
permanent, d.extra, in_flight, DeclareNodeState, peer-tickle and data-frame. Classify by owner,
not spelling alone:

| Match class | Examples / disposition |
|---|---|
| Current normative | envelope Fence/Op, separate context; lifecycle Build/authority/proof invariants; typed desired resource lock |
| Historical | i143-progress old Echo/incumbency/DesiredTopology landing rows, archived docs/prompts; retain as history, correct active status separately |
| Transitional, owner present | Rafka LEGACY_ADAPTERS, tagged_bi_rpc 0x12/0x19, node-admin stateful source before #2925/#2926 cut; i142 or existing lock owner |
| Compatibility boundary | literal 0x12 byte, 413 legacy reset, per-op payload codecs, former frame-shape rejection tests; preserve until owning format cut |
| Actual stale implementation/doc debt | authoritative wire target_slot/STALE_SLOT claims, active downward Declare, d.extra drain count, unconditional permanent storage release, empty resolver incarnation; owners in §3 |

`in_flight` is not one concept: runtime work count, stats/admission counters and lifecycle
operation overlay list have distinct owners. “stateful” in comments about proof stores and
“permanent” in retired-op reservations are not desired storage defects. Descriptive Tags never
replace execution metadata. No mechanical global rename is justified.

## 10. Patch and verification record

Documentation only: seven architecture files, i141 PRD, i143 export index and this audit.
No enum, pin, provider, protocol handler or external issue changed. Relevant authority/status/
storage/drain passages are reconciled; the full #2927 terminology sweep remains its own work.

Files changed:

- `docs/architecture/node-rpc-envelope.md`: wire authority, pending status freeze, normative status
  admission during drain without a fixed API name, control-plane metadata cross-reference,
  ledger call list and whole-value baggage propagation.
- `docs/architecture/node-rpc.md`: explicit envelope precedence; defer long-form fold to #2927.
- `docs/architecture/node-rpc-envelope.md`: current Fence/context boundary and status schema authority.
- `docs/architecture/deployment-provider.md`: canonical resource vocabulary, storage defaults and
  normalization ownership, storage disposition and bounded typed RPC-first retirement.
- `docs/architecture/fabric-mesh-ops.md`: direction matrix and typed drain evidence before termination.
- `docs/architecture/fabric-authority.md`: status schema cross-reference and downward operation names.
- `docs/architecture/data-plane-transport.md`: current adapter/migration boundary and retired Echo notice.
- `docs/plans/i141-node-rpc-substrate-prd.md`: convergence checkpoint and upstream ownership.
- `docs/plans/i143-node-rpc-pos-on-RDM.md`: relocated record references and current wire fence.
- `docs/plans/i141-i143-convergence-audit.md`: this evidence, finalization plan, owner matrix and gates.

Tests and patch validation are recorded below after the audit run. They prove only the tested
baseline contracts, not pending B–F implementation or final export eligibility.

| Command | Result |
|---|---|
| RDM `cargo test -p rafka-node-rpc-contract --lib --offline` | 50 passed |
| RDM `cargo test -p rafka-node-rpc --test context --test certainty --test forward --offline` | 21 passed (6 context, 9 certainty, 6 Forward) |
| Rafka `cargo test -p rafka-mesh-ops --lib frame::tests --offline` | 51 passed; filter also includes control_frame::tests |
| RDM `cargo test -p rafka-node-rpc --test pool --offline` | 6 passed |
| RDM `cargo test -p rafka-node-admin-core --test status_declarations --offline` | 4 passed |

Total: 132 passed, zero failures. `git diff --check` and new-document whitespace validation
passed. The complete review patch includes the new audit file, which ordinary `git diff`
omits while it is untracked. Changes are left uncommitted for review; RDM has no source changes.

Full process/container scenarios, soak, planted-violation ratchets and export eligibility were
not rerun. No test result above certifies the absent new status/storage/drain contracts.
