# Transport parity ledger (i143)

This ledger records every Rafka transport/pool/reachability change that RDM must disposition before i143 can publish an import-eligible revision.

**Fixed audit base:** `8704558070`  
**Review-through tip for this reconciliation:** `f92173b9c85dea2945cd784ebf22759f99a9c1a5`  
**Canonical ownership:** `docs/architecture/node-rpc-rdm-ownership.md`

The fixed base does not move. A newer Rafka tip extends the audit range.

## Boundary

A Rafka commit requires a disposition when it touches:

```text
crates/rafka-mesh-transport/**
crates/rafka-node-base/** transport-facing code:
  bi-stream dispatch/framing
  request-send certainty/reset codes
  dial/endpoint freshness
  connection pool/slot ownership
  mesh protocol tags
  transport identity/admission seams
crates/rafka-reach-reroute/**
crates/rafka-mesh-ops/** framing/transport protocol contracts
```

Downstream-only still gets a row. "Not mirrored" is a disposition, not permission to omit the change.

## Dispositions

| disposition | meaning |
|---|---|
| **MIRROR** | generic behavior is present in RDM; row names RDM SHA + carrying test |
| **MIRROR pending** | generic behavior not yet proven upstream; blocks import |
| **RAFKA DOMAIN** | product routing/lifecycle/attempt semantics remain downstream |
| **RAFKA AUTH ONLY** | Rafka trust/SVID/issuing-chain policy stays downstream behind generic verifier interfaces |

## Ancestry rule

Do not call a commit "pre-base" because its issue/date is older. Tooling asks whether it is an ancestor of `8704558070`. A non-ancestor containing missing boundary behavior is a **divergence input** and is dispositioned explicitly.

This corrects the earlier treatment of `3644b2e5b9` and `9e8401e6aa`; neither is an ancestor of the fixed base.

## Divergence inputs

| Rafka SHA | issue | change | disposition / proof |
|---|---|---|---|
| `3644b2e5b9` | #2598 B | late dial to superseded endpoint/slot is typed supersession, never current reachability evidence | **MIRROR**. RDM `8fc9f8a416` (rust-distributed-mesh#28): a dial that connects after its exact `(incarnation, slot, freshness)` target moved is re-checked before pooling, closed and answered `NotSent(Superseded)` (`rafka.node_rpc.connection.evict.via-slot-superseded`, `outcome=late-connect-dropped`); carrying test `crates/rafka-node-rpc/tests/pool.rs::late_superseded_dial_never_pools` (deterministic failpoint after connect; pool empty), with `old_freshness_token_never_reenters` |
| `9e8401e6aa` | #2469 | `TAG_FORWARD_TICKLE` / `serve_peer_tickle`, one-hop offline-proxy ladder | **RAFKA DOMAIN**. `peer_tickle` stays Rafka liveness semantics and `FORWARDABLE=false` |

## Post-base ledger

| Rafka SHA | issue | change | disposition / proof |
|---|---|---|---|
| `8b66038ed8` | #2616 | root -> mesh issuer -> member cert chain | **RAFKA AUTH ONLY**. Policy moves to `rafka-internal-mesh-auth`; RDM consumes generic verifier contract |
| `9d87fbb2e2` | #2615 | named pre-dispatch unknown/uninstalled-tag refusal; healthy refusing connection stays pooled | **MIRROR pending**. Canonical destination is sealed catalog + 421 unknown tag + typed `NotReady`; 501/503 are transition-only/reserved |
| `65d17cdc1b` | #2613 | endpoint move immediately cancels in-flight dials whose exact slot target moved | **MIRROR**. RDM `8fc9f8a416` (rust-distributed-mesh#28): an in-flight dial races the resolver's change ticks and is cancelled the moment its exact slot target moves (`…evict.via-slot-superseded`, `outcome=cancelled`); carrying tests `crates/rafka-node-rpc/tests/pool.rs::changed_slot_cancels_only_stale_inflight_dials` (stale waiter released as `Superseded` < 1.5 s; the sibling slot's dial runs to its own deadline) and `unchanged_sibling_slot_stays_usable` (the sibling's pooled connection, same `stable_id`, is reused) |
| `bb891eeaa8` | #2666 | write leg separates connect+complete-send bound from reply bound | **MIRROR**. RDM `a886ea808c` (rust-distributed-mesh#19): `rafka_node_rpc::Budget::Split { send, reply }` — connect + complete-send bound, reply budget starting at the commit cut; carrying test `crates/rafka-node-rpc/tests/certainty.rs::reply_loss_after_a_complete_send_is_indeterminate` |
| `f4a8732be5` | #2666 | unfinished request resets with code 499 so partial frame is never dispatched | **MIRROR**. RDM `a886ea808c` (rust-distributed-mesh#19): the client resets an unfinished request with `499 FRAME_NOT_SENT` (`NotSent`) and the server dispatches only a complete, finished frame; carrying test `crates/rafka-node-rpc/tests/certainty.rs::an_unfinished_send_resets_with_499_is_not_sent_and_is_never_dispatched` (receiver: dropped 1, dispatched 0) |
| `acf84fb8f1` | auth follow-up | spawned node-admin receives mesh issuing key, never fabric root | **RAFKA AUTH ONLY**. Generic deployment may pass opaque auth material, not own Rafka root/issuer semantics |
| `d925c67b5b` | GH2574 | own-mesh reader preference and `mesh_of_node_name` helper | **RAFKA DOMAIN**. Generic reachability does not choose replica order or interpret Rafka placement policy |
| `fb4100fae4` | #2671 | retired `<path.name>.old` resolves to mesh of the slot it held | **RAFKA DOMAIN**. RDM does not own Rafka `.old` naming semantics |

A pending row becomes MIRROR only after it records the RDM SHA, carrying test, and proof artifact.

## Boundary commits through the review-through tip

Every other non-merge commit in `8704558070..f92173b9c8` that touches a boundary path. Most of the
node-base and mesh-ops boundary is Rafka product code living in a transport-adjacent crate; each
row says why.

| Rafka SHA | issue | change | disposition / proof |
|---|---|---|---|
| `c09424c2bf` | #2599, #2568 | a drained or keyed-snapshot install holds the record's OrderKey | **RAFKA DOMAIN**. Application EF / OrderKey ordering and index install semantics; no transport change. |
| `3751834202` | #1174 | a promotion is claimed on the mesh it was enqueued on, so a pinned peer create's promotion runs | **RAFKA DOMAIN**. Rafka job kinds (`rafka-mesh-ops::jobs`); product orchestration, not a transport contract. |
| `b609024d72` | #2599 | the ordered op-65 read honours the armed prefix-read hold | **RAFKA DOMAIN**. Application EF / OrderKey ordering and index install semantics; no transport change. |
| `d6ac804117` | #2599, #2568 | the entity drain reads op-67 windows, so every drained record carries its row's OrderKey | **RAFKA DOMAIN**. Reader product semantics (replica coverage, read order, op-67 drain); generic exact-node reachability is not changed. |
| `b33e65878c` | #2599 | an insert-and-fail win and the offset-only seams install under the record's OrderKey | **RAFKA DOMAIN**. Application EF / OrderKey ordering and index install semantics; no transport change. |
| `287d2196f5` | #2599 | an index claim's win and the resident index apply order by the claimed row's OrderKey | **RAFKA DOMAIN**. Application EF / OrderKey ordering and index install semantics; no transport change. |
| `c0902d2b60` | #1311 | one helper owns the audit child's address; every adt_ reader takes both forms | **RAFKA DOMAIN**. Rafka org/topic/audit/system-topic addressing in node-base; product naming, not transport. |
| `b76ca0d81d` | #2599 | an index write's ACK carries the row's OrderKey to its resident install | **RAFKA DOMAIN**. Application EF / OrderKey ordering and index install semantics; no transport change. |
| `109bfd86ee` | — | read_targets drain cell asserts the op-67 dial failure on the registered target | **RAFKA DOMAIN**. Reader product semantics (replica coverage, read order, op-67 drain); generic exact-node reachability is not changed. |
| `2431977d2b` | #2622 | dial_targets_of cell takes its own fabric ids; readers-and-writers states the unresolved-member rule | **RAFKA DOMAIN**. Reader product semantics (replica coverage, read order, op-67 drain); generic exact-node reachability is not changed. |
| `9c18f5bbc6` | #2622 | a replica-set member that resolves to no node counts as not answered in the coverage read | **RAFKA DOMAIN**. Reader product semantics (replica coverage, read order, op-67 drain); generic exact-node reachability is not changed. |
| `3774ab09eb` | #2600 | remove fault door and mock from shipped lib; guard test broker registration (GH#2600) | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `41b77ad493` | #2600 | wait for durable departure rows, reject on failure, no string guessing on node type (GH#2600) | **RAFKA DOMAIN**. Rafka node lifecycle / departure / termination semantics; RDM builds its own generic lifecycle pipeline (i143 e2/e3). |
| `47c25bc1ae` | — | remove broker connection registration from platform_bootstrap seed | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `4a92e3d38c` | #2600 | address lead rework items for authoritative departure rows (GH#2600) | **RAFKA DOMAIN**. Rafka node lifecycle / departure / termination semantics; RDM builds its own generic lifecycle pipeline (i143 e2/e3). |
| `4c9f7e2e7a` | #2600 | serialize held mesh test with gossip loop (GH#2600) | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `2554e16150` | #2630 | a write in the writer's incarnation gap waits for the successor, bounded by the fence ACK, instead of refusing | **RAFKA DOMAIN**. Writer-ingress product semantics (write legs, writer incarnation, route choice); no generic byte movement, certainty or pool change. |
| `16d875306f` | #2563 | presence index, DemoteOrgFromMesh job kind, and demotion execution | **RAFKA DOMAIN**. Rafka job kinds (`rafka-mesh-ops::jobs`); product orchestration, not a transport contract. |
| `8a55e1e51e` | — | remove extraneous closing brace in jobs.rs | **RAFKA DOMAIN**. Rafka job kinds (`rafka-mesh-ops::jobs`); product orchestration, not a transport contract. |
| `c74ce17bb6` | — | accept KeyedSnapshot RPC failure in read_targets drain dial test | **RAFKA DOMAIN**. Reader product semantics (replica coverage, read order, op-67 drain); generic exact-node reachability is not changed. |
| `2a9078c558` | #2617 | a broker re-sync is a liveness obligation; the stale view reads only a replica in page service | **RAFKA DOMAIN**. Reader product semantics (replica coverage, read order, op-67 drain); generic exact-node reachability is not changed. |
| `b514d48c20` | #2624 | #2624 op 64 INSERT_IF_ABSENT answers STATUS_OK only for the broker's retained current row (Luke ruling A) | **RAFKA DOMAIN**. Rafka protocol family payload (`broker_data` ops / fence / keyed batch) in `rafka-mesh-ops`; payload semantics stay downstream, framing is unchanged. |
| `58c80b40b5` | #1311 | a tenant org's audit child is <source>.adt_<...>.<mesh>; a promotion builds the joining mesh's children (durable format moved) | **RAFKA DOMAIN**. Rafka org/topic/audit/system-topic addressing in node-base; product naming, not transport. |
| `bfdb9fb450` | #2565 | durable format moved: rename the Mesh status ready-for-writes → ready-for-writing (i138 #2565, Luke ruling (a): a Mesh readiness, never a... | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `a04814c8ae` | #2492 | the share-group projection's six cells plus the enrolment pin; docs list the projected kinds | **RAFKA DOMAIN**. Rafka Application EF birth set / entity manifest; product projection ownership. |
| `6adf06f6ad` | #2626 | a name-claim withdraw's resident tombstone installs under the withdraw's stamped OrderKey and the answering replica's offset | **RAFKA DOMAIN**. Application EF / OrderKey ordering and index install semantics; no transport change. |
| `5944930890` | #2617 | the growers' filer stamps the Node row's id; a re-called cancel after successor admission leaves the successor alone | **RAFKA DOMAIN**. Rafka job kinds (`rafka-mesh-ops::jobs`); product orchestration, not a transport contract. |
| `d92b458009` | #2617 | one shared exact-instance BrokerResync cancel; filings stamp the broker instance | **RAFKA DOMAIN**. Rafka job kinds (`rafka-mesh-ops::jobs`); product orchestration, not a transport contract. |
| `99346a1231` | #2582 | op 64 STATUS_SUPERSEDED = 4; the writer counts a superseded leg as decided contention | **RAFKA DOMAIN**. Rafka protocol family payload (`broker_data` ops / fence / keyed batch) in `rafka-mesh-ops`; payload semantics stay downstream, framing is unchanged. |
| `a7dfdaf60e` | #2617 | a joining mesh's broker re-sync takes its instance from the broker's live digest when no Node row exists yet | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `2d0e88b1d3` | #2631 | compute's node-admin reports resolve the current node-admin per request | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `33904a0a7e` | #2631 | fence and open resolve the current node-admin from topology + its Node row | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `d6811964e4` | #2631 | the one resolver is NodeAdminControl::current_node_admin_api_base | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `02c7b5cda0` | #2594 | a node process never runs on the deterministic test bootstrap — an unseeded platform_bootstrap() read refuses by name, naming its caller | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `37c1fdaa90` | #2574 | the walk order and the point read's coverage order state the own-mesh preference | **RAFKA DOMAIN**. Reader product semantics (replica coverage, read order, op-67 drain); generic exact-node reachability is not changed. |
| `ce350c1b9c` | #2652 | the Split prose says what Split means: not decided for this call, any copy it placed withdrawn, claim again | **RAFKA DOMAIN**. Application EF / OrderKey ordering and index install semantics; no transport change. |
| `daec1712ed` | #2560 | a duplicate replays the original ack from a durable BatchReceipt | **RAFKA DOMAIN**. Rafka Application EF birth set / entity manifest; product projection ownership. |
| `c228ce0b25` | #2567 | a tenant org is born on the creating gateway's mesh by ordinary topic placement | **RAFKA DOMAIN**. Rafka org/topic/audit/system-topic addressing in node-base; product naming, not transport. |
| `b486cc8074` | #2208 | silent fabric seat re-election and exclusion inputs in FabricElection | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `8a9ec31e52` | #2649 | on a projection miss the writer gets the topic first; a bootstrap-carried leaf takes the carried set only when no Topic row exists | **RAFKA DOMAIN**. Writer-ingress product semantics (write legs, writer incarnation, route choice); no generic byte movement, certainty or pool change. |
| `6026c31bf6` | #2659 | a node-admin row on GET /api/nodes carries admin_api_port, published under its path.name | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `4b7c3be715` | #2657 | the create door takes over a stale path.name claim whose holder has no record | **RAFKA DOMAIN**. Writer-ingress product semantics (write legs, writer incarnation, route choice); no generic byte movement, certainty or pool change. |
| `eeab48a439` | #2608 | compute is born with path_config_override | **RAFKA DOMAIN**. Rafka Application EF birth set / entity manifest; product projection ownership. |
| `d58f01cd3f` | #2658 | a declared-index write whose slot state cannot be read refuses naming the read, not a missing slot | **RAFKA DOMAIN**. Application EF / OrderKey ordering and index install semantics; no transport change. |
| `5605cadd0f` | #2651 | a keyed re-sync copy the target's newer cell dominates is superseded, never refused below the prefix | **RAFKA DOMAIN**. Rafka protocol family payload (`broker_data` ops / fence / keyed batch) in `rafka-mesh-ops`; payload semantics stay downstream, framing is unchanged. |
| `01c164172c` | #2660 | delete the entry snapshot's elected-projection apply; projections ride ProjectionPull | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `ddc4a74c84` | #2576 | internal node work starts at the node's own ready-for-traffic — the entry carries the peer-mesh directories, node-admin refuses a short e... | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `fc57a8e3a4` | #2670 | one upgrade-framework job per (build, mesh); a compute declines another mesh's job and the job skips off-mesh orgs | **RAFKA DOMAIN**. Rafka job kinds (`rafka-mesh-ops::jobs`); product orchestration, not a transport contract. |
| `de506fce9e` | #2656 | WAL reset tombstones every record in every topology topic | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `196cad0d4a` | #2666 | a write dispatch cut off mid-frame reaches the node's own broker reader as a 499 reset, and no frame reaches the drain | **MIRROR**. RDM `a886ea808c` (rust-distributed-mesh#19): the same cut over real Iroh streams; carrying test `crates/rafka-node-rpc/tests/certainty.rs::an_unfinished_send_resets_with_499_is_not_sent_and_is_never_dispatched` |
| `00fcd79f85` | — | check-id-form compliance and test isolation in topology | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `fb25c6fc84` | #2663 | #2663 fabric entity (fab_, ops.fabrics), in-memory minting, single write, memory-served | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `0ca43b1df8` | #2591 | an entry snapshot carries rafka-time as of the answer's completion | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `bc01931a36` | — | the unanswered-send cell drives the two-phase dispatch the writer bounds since the write-leg split | **RAFKA DOMAIN**. Writer-ingress product semantics (write legs, writer incarnation, route choice); no generic byte movement, certainty or pool change. |
| `385f51f05f` | — | every row hydrated at birth carries OrderKey — durable format moved | **RAFKA DOMAIN**. Application EF / OrderKey ordering and index install semantics; no transport change. |
| `b559ebe971` | #2495 | #2495 resolve own node type from OWN_NODE_TYPE single source per Lead 0005 | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |
| `d77d2058a9` | #2495 | a broker, gateway, or compute completes its own termination on its permanent delete | **RAFKA DOMAIN**. Rafka node lifecycle / departure / termination semantics; RDM builds its own generic lifecycle pipeline (i143 e2/e3). |
| `22f5e18609` | #2668 | the broker gains the claim cutover's capabilities: op-64 resolve-claim-absent, the holder's OrderKey on TAKEN, and durable claim provenance | **RAFKA DOMAIN**. Rafka protocol family payload (`broker_data` ops / fence / keyed batch) in `rafka-mesh-ops`; payload semantics stay downstream, framing is unchanged. |
| `65d28fb35d` | #2668 | the claim cutover's broker capabilities are proven functional: resolve-claim-absent's three outcomes, the restart and re-registered-sourc... | **RAFKA DOMAIN**. Rafka protocol family payload (`broker_data` ops / fence / keyed batch) in `rafka-mesh-ops`; payload semantics stay downstream, framing is unchanged. |
| `241b3ad867` | #2669 | a fence's admission is phase-bearing and its phase reaches the copier | **RAFKA DOMAIN**. Rafka protocol family payload (`broker_data` ops / fence / keyed batch) in `rafka-mesh-ops`; payload semantics stay downstream, framing is unchanged. |
| `45f367ca02` | #2603 | an empty served VT page read emits via-page-read at debug | **RAFKA DOMAIN**. Reader product semantics (replica coverage, read order, op-67 drain); generic exact-node reachability is not changed. |
| `cac997ece2` | #2696 | Node.incarnation_id — the row names the process serving a node | **MIRROR pending**. Generic part: an opaque per-process-birth incarnation id, supersession by equality (ownership §5.1, §8.1). The durable `Node` row field is Rafka's adapter; RDM owns `RuntimeIncarnationId` in the Mesh EF (owner i143.e4) and incarnation supersession in the pool (owner i143.e6). |
| `a40022fe0e` | #2685 | the jobs-audit plane has one form per mesh, placed on that mesh's brokers by the audit-child rule; every writer writes its own mesh's form | **RAFKA DOMAIN**. Rafka org/topic/audit/system-topic addressing in node-base; product naming, not transport. |
| `f586bf68ab` | #2603 | the gossip receive's routine spans go to debug; refusals and declines stay info | **RAFKA DOMAIN**. Rafka Application EF gossip receive instrumentation; gossip itself is not changed. |
| `13d177f9d4` | #2706 | plane_presence — the plane resolver's answers kept apart (registered, unregistered, org planes absent, unreadable) | **RAFKA DOMAIN**. Rafka org/topic/audit/system-topic addressing in node-base; product naming, not transport. |
| `bfaf184da0` | #2706 | JobKind::writes_tenant_legs is the one tenant-leg predicate | **RAFKA DOMAIN**. Rafka job kinds (`rafka-mesh-ops::jobs`); product orchestration, not a transport contract. |
| `04672ccca3` | #2640 | the route ACK row on <system_org>.route-acks, keyed (topic, broker, writer incarnation) | **RAFKA DOMAIN**. Writer route-epoch lease / route ACK in `rafka-reach-reroute`: product routing and replica-catch-up policy, not generic reachability. |
| `be6d71489f` | #2640 | the route lease drops its two unread accessors and the route-ACK prefix read is a reviewed keep-alive | **RAFKA DOMAIN**. Writer route-epoch lease / route ACK in `rafka-reach-reroute`: product routing and replica-catch-up policy, not generic reachability. |
| `fe185b556b` | #2640 | a write leases its topic's route epoch before it decides its write set and gives it back where its legs are originated | **RAFKA DOMAIN**. Writer route-epoch lease / route ACK in `rafka-reach-reroute`: product routing and replica-catch-up policy, not generic reachability. |
| `b06477e9a6` | #2640 | the 60 s settle is the route barrier's fallback, wherever the watermark is described | **RAFKA DOMAIN**. Rafka job kinds (`rafka-mesh-ops::jobs`); product orchestration, not a transport contract. |
| `675bf9ca38` | #2640 | a topic's route lease entry is removed once its epoch is retired and no lease is out | **RAFKA DOMAIN**. Writer route-epoch lease / route ACK in `rafka-reach-reroute`: product routing and replica-catch-up policy, not generic reachability. |
| `735b543cf3` | #2681 | the in-process fault door compiles only under test-fault-injection | **RAFKA DOMAIN**. Rafka in-process gossip fault door and its build gating; RDM failpoints are i143 e8, not this door. |
| `f653cb2111` | #2681 | test-fault-injection feature; debug node bins carry it, release bins never | **RAFKA DOMAIN**. Build/test-harness gating of a Rafka feature flag; no runtime transport behavior. |
| `b226608aad` | #2721 | the Fabric carries its own status, authored only by the fabric-primary | **RAFKA DOMAIN**. Rafka node-admin control/product state (Node/Mesh/Fabric rows, control-endpoint resolution, fabric election, bootstrap). The generic control equivalent is built in i143 e1–e4 from the PRD, not mirrored from this change. |

## Connections parity (i66.e3 → RDM)

The connections subsystem rafka builds first in i66.e3 (#2722 a/b/c, rafka-v2 `docs/architecture/connections.md`)
is the R3 parity source. RDM imports it; it does not redesign it. Each row maps one i66.e3 acceptance clause
(connections.md §13 A–L, plus the model and route-precedence clauses) to the RDM story that imports it and the
test that will carry the proof. The RDM owners add no parallel carrier-evidence store (`CarrierObserved` /
`Withdrawn`) and no second reconnect scheduler (PRD §1.25, §22).

`parity-scan` reads connections.md §13 and fails while any clause has no row, or a row has no owner (an
`i143.eN.sM` story for MIRROR / MIRROR pending), no carrying proof, or is MIRROR without the RDM SHA.

| clause | i66.e3 source | RDM owner | carrying proof | disposition |
|---|---|---|---|---|
| `CONN-FACTS` | connections.md §1–§2; #2722a | i143.e4.s2 (#2755) | functional: `NodeConnection` kind `Direct`/`Proxy`, state `Connected`/`Disconnected`/`Failed`, source/destination/carrier incarnations, recovery epoch + attempt ordinal on Direct Failed | **MIRROR pending** |
| `CONN-ROUTE` | connections.md §5–§6; #2722b | i143.e4.s2 (#2755) | functional: effective route precedence valid Proxy → `ViaPeer(carrier)`, else active Direct → `Direct`, else `NoActiveRoute`, under the protocol carrier-policy hook; carrier spread by `(source, destination, destination_incarnation)` | **MIRROR pending** |
| `CONN-A` | connections.md §13 A; #2722b | i143.e6.s5 (#2771) | functional: the first proven Proxy is reused by the next call to the same destination with no direct dial ladder | **MIRROR pending** |
| `CONN-B` | connections.md §13 B; #2722c | i143.e4.s3 (#2756) | functional: a source restart hydrates the latest Proxy row and the first call uses it without rediscovery | **MIRROR pending** |
| `CONN-C` | connections.md §13 C; #2722c | i143.e4.s3 (#2756) | functional: restart reconstructs `next_due` from the latest Direct Failed row; no attempt before it, ordinal not reset | **MIRROR pending** |
| `CONN-D` | connections.md §13 D; #2722a | i143.e4.s2 (#2755) | functional: many failed attempts leave the raw log growing and the current index at one Direct + one Proxy per pair | **MIRROR pending** |
| `CONN-E` | connections.md §13 E; #2722a | i143.e4.s2 (#2755) | functional: peers' held projection holds active Direct edges only, never Failed/Dead history | **MIRROR pending** |
| `CONN-F` | connections.md §13 F; #2722c | i143.e4.s3 (#2756) | functional: Direct Connected → Proxy still effective → Proxy Disconnected(direct-restored) → Direct effective, in that order | **MIRROR pending** |
| `CONN-G` | connections.md §13 G; #2722c | i143.e4.s3 (#2756) | functional: an accepted inbound Direct Connected creates the same retirement obligation | **MIRROR pending** |
| `CONN-H` | connections.md §13 H; #2722c | i143.e4.s3 (#2756) | functional: a failed retirement write keeps traffic on Proxy and is retried until it lands | **MIRROR pending** |
| `CONN-I` | connections.md §13 I; #2722b | i143.e4.s2 (#2755) | functional: a destination or carrier incarnation change fences the old Direct/Proxy evidence | **MIRROR pending** |
| `CONN-J` | connections.md §13 J; #2722b | i143.e4.s2 (#2755) | functional: losing the carrier's Direct edge to the destination invalidates the Proxy | **MIRROR pending** |
| `CONN-K` | connections.md §13 K; #2722b | i143.e6.s5 (#2771) | functional: a post-commit timeout is `Indeterminate`, never resent on another route, and does not invalidate a working Proxy | **MIRROR pending** |
| `CONN-L` | connections.md §13 L; #2722b | rafka-v2 (stays downstream) | RDM has no semantic route, route barrier or watermark: `orgs.topics` and #2640 are Rafka application state, and `dep-rules` rule 1 keeps rafka-v2 domain crates out of RDM generic packages | **RAFKA DOMAIN** |

## Mechanical completeness gate

The rows above are not the definition of completeness.

At i143 e0 and again before import manifest generation:

1. compute the Rafka range from fixed base through proposed parity-through SHA;
2. include declared non-ancestor divergence inputs;
3. identify every commit touching the boundary;
4. require exactly one disposition for every such commit;
5. reject duplicate/ambiguous rows;
6. reject import eligibility while any applicable row is `MIRROR pending`;
7. record parity-through SHA and ledger digest in the import manifest.

If main advances, rerun the scan.

The gate is `parity-scan` in RDM (`tools/mesh-audit`, `rafka_mesh_audit::parity`). It reads this
ledger (fixed base, review-through tip, boundary paths, every row), computes the range by ancestry
(`git log --no-merges base..through`, `git merge-base --is-ancestor`, never dates) and exits non-zero
on an unclassified boundary commit, a duplicate or ambiguous row, an ancestry violation or any
`MIRROR pending` row. Its JSON report carries the import-handoff fields below.

```text
cargo run -p rafka-mesh-audit --bin parity-scan -- --repo <rafka-v2 checkout> [--through <sha>] [--json <out>]
```

## Classification

```text
byte movement, request certainty, dial/pool freshness,
generic exact-node reachability, dispatcher/catalog mechanics, generic admission
  -> MIRROR / MIRROR pending

Rafka routing, replica selection, path.name lifecycle,
node-admin offline semantics, product retry/attempt policy
  -> RAFKA DOMAIN

Rafka SVID/root/issuing-chain/trust policy behind generic verifier seam
  -> RAFKA AUTH ONLY
```

File location alone never decides ownership.

## Import handoff

```json
{
  "rdm_revision": "...",
  "rafka_parity_base": "8704558070",
  "rafka_parity_through": "...",
  "parity_ledger_digest": "...",
  "mirror_pending": 0
}
```

Any nonzero `mirror_pending` means `eligible=false`.
