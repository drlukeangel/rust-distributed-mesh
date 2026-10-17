# i145 — the cert chain, its verification at a connection and its renewal are RDM's

**Status:** approved 2026-10-16; **held as its own phase; it lands only after the full regression of the Node RPC and gossip work** (Luke, 2026-10-17). Not dispatched. Approved 2026-10-16 (Luke, Q1–Q6 of `docs/plans/proposal/cert-verification-ownership.md`;
ruling `docs/plans/rulings/i145-rulings-2026-10-16-cert-ownership.md`, R-CT1). Page set applied
2026-10-16. Nothing built.

**Record:** this file. Every state or remaining-work update goes in an epic's row below.

## Context

A node-admin issues a member cert to every birth it admits, and Rafka's data plane checks that cert on
every inbound `rafka-mesh-v1` connection before serving it. Today those two halves live in two repos
and two binaries: Rafka's own `node-admin/` mints (outside RDM's `CertSigner`) and delivers by env at
spawn; Rafka's `rafka-node-base` accept loop multiplexes RDM's Node RPC and gossip ALPNs beside its
own and runs the "SVID belt" on its own arm; RDM's node-admin issues through `CertSigner` with no Rafka
implementation behind it. The pages described a merge in neither binary, and three standing rulings
(R-SV1, R-RX1's text, e12.s69) said the belt should be deleted. Luke ruled instead that the connection,
like the rest of the mesh, is RDM's: RDM owns the cert format, mints, signs, verifies at admission and
renews; Rafka passes in the root once. Renewal did not exist anywhere: a member cert lived a day and a
node up longer than that was refused on its next new dial.

The customer is whoever runs a fabric: a node admitted once stays admitted for as long as it is a
member, its credential renewed before it expires, with no reconnect and no gap; a connection without a
valid credential of this fabric is refused by name and recorded; and none of this touches Node RPC or
gossip.

## Requirements

| # | Requirement | State | Evidence today | Epic |
|---|---|---|---|---|
| R1 | the cert format, sign and verify live in `rdm_core::certs`; `CertChoice` is `Ca` / `Issuer` / `NoCerts`; `CertSigner` is gone | BUILD | `crates/rdm-admin/src/certs.rs:29` the trait; rafka `rafka-node-transport/src/cert.rs` the format | e1 |
| R2 | the accept loop is rdm-transport's: every connection on the endpoint, dispatched by ALPN; Rafka's loop deleted | BUILD | rafka `rafka-node-base/src/lib.rs:10748 start_accept_loop` | e2 |
| R3 | `node.connections.admit` on every inbound connection of a registered product ALPN: the exchange on the first bi-stream, the verification, the verdict written back, admit or drop | BUILD | rafka `lib.rs:2817 dialer_send_svid`, `:3056 acceptor_verify_svid` | e2 |
| R4 | registration is per ALPN at `bind`; RDM's own two ALPNs are refused by name | BUILD | `crates/rdm-transport/src/endpoint.rs:30` `bind` takes no admission | e2 |
| R5 | a refusal is the connection fact `Refused { reason }`; an admission the ordinary `Connected`; both spanned | BUILD | connections.md §1.2 had three states | e2 |
| R6 | `node.cert.create` at JoinNode writes the birth's cert row (`NodeCertRow`, `node-cert/1`); `node.get` surfaces `cert { … }` | BUILD | `certs.rs:122` issues, writes no row; `NodeView` has no cert | e3 |
| R7 | `node.cert.renew`: the issuing authority's tick renews at `RDM_CERT_RENEW_AT` + jitter; installed on the running node; presented on new connections; terminates nothing; retried to expiry | BUILD | nothing renews | e3 |
| R8 | `mesh.issuer.create` at the mesh's acceptance, `mesh.issuer.renew` on the fabric-primary's tick, `node.issuer.update` per admin; a member never outlives its issuer | BUILD | rafka `node-admin/src/main.rs:20707` mints an issuer once; nothing rotates | e3 |
| R9 | the TTLs and renewal point are fabric policy, carried in the entry pull | BUILD | `MESH_SPAWN_TYPE` is the pattern | e3 |
| R10 | the fabric-primary watches every cert row; a mesh whose renewals stall has its row surfaced on `fabric.get` | BUILD | Q23 retires Concern; stalled renewal is surfaced on `fabric.get` | e3 |
| R11 | Rafka passes the root in once (`CertChoice::Ca`), signs and verifies nothing, reads none of `RAFKA_CA_PUBKEY` / `RAFKA_NODE_CERT` / `RAFKA_NO_CERT`; its node-admin's minting and the belt are deleted | BUILD | rafka `node-admin/src/main.rs:4713`, `lib.rs:5806`, `rafka-node-base/src/lib.rs:2724` | e4 |
| R12 | no cert is checked on Node RPC or gossip | HOLDS | nothing checks today; the control cell keeps it so | every epic |
| R13 | two levels above the issuers: the fabric root (the app's anchor, signs only the CA) and RDM's CA (minted on day 0 under the root, `fabric.ca.create`); the chain is member → issuer → CA → root (Luke 2026-10-16: "I thought we were using the caller root to issue a new root that I then passed on and we can rotate internally") | BUILD | one level: the root signs issuers | e5 |
| R13a | `fabric.ca.renew`: the CA rotates internally on policy (`RDM_CA_CERT_TTL_SECS`, `RDM_CERT_RENEW_AT`) under the same anchor; every issuer re-minted; no `node.roots.update`; an issuer never outlives its CA, the CA never its root | BUILD | nothing rotates | e5 |
| R13b | `fabric.root.update { secret }`: a caller-supplied anchor rotates through five durable phases (`minted`, `distributed`, `ca-rotated`, `retired`); `node.roots.update` installs the set on every node (the secret to admins); a retire before every chain moved is refused by name; refused in flight or for a trusted root; a launched node is written `RDM_ROOTS`; `fabric.get` carries `roots { phase, expires_at }` and `ca { expires_at }` | BUILD | no caller op; verifiers hold one key | e5 |
| R14 | custody: every node-admin holds the one root (written at launch as `RDM_CA_SECRET`, refreshed on rotation by `node.roots.update`); an elected successor mints and rotates from its own copy; the root is never in a row, frame or digest | BUILD (RDM) / HOLDS (rafka's node-admin already hands every admin the CA secret: `lane-mesh-deploy.sh:1262`) | `Launch` writes no root; `CertChoice::Issuer` carries none | e5 |

## § Open forks

- *Root rotation inside a running fabric?* In i145 (Luke, 2026-10-16: "no deferral"): e5.
- *Does the dialer re-dial on a refusal?* No new rule: the dialer reads the verdict and its existing
  reconnect rule (connections.md §9) decides; a refused dial is a `Failed` row on the dialer's side with
  the named reason.
- *The data-plane ALPN's name.* It stays Rafka's (`rafka-mesh-v1`); RDM registers the step for whatever
  ALPN the app names at `bind`.

## § Surfaces — ONE capability, IDENTICAL on every surface

| Capability | process provider | container provider | wire | page |
|---|---|---|---|---|
| a member cert issued at JoinNode, renewed before expiry, presented on every new product-ALPN connection, verified at admission | the same (the cert is the node's, not the runtime's) | the same | `RenewCert` / `CertRenewed`, `UpdateIssuer` / `IssuerUpdated` on 0x1B; the admission exchange on the first bi-stream; `NodeView.cert` | fabric-certs.md, node-cert-renew.md, node-issuer-update.md, mesh-issuer-renew.md, node-connections-admit.md |

## § OTLP span-assertion bar

| Span | Owner | Attributes | Cells |
|---|---|---|---|
| `rdm.node.connections.create.via-admitted` | the acceptor | source, destination, connection, alpn, cert_expires_at, clock | every e2 cell |
| `rdm.node.connections.reject.via-refused` | the acceptor | the same + `reason` ∈ {`no-credential`, `bad-signature`, `issuer-bad-signature`, `issuer-mesh-mismatch`, `expired`, `issuer-expired`} | e2 refusal cells, one per reason |
| `rdm.node.connections.update.via-presented` | the dialer | destination, alpn, cert_expires_at | e2 |
| `rdm.node_admin.cert.create.via-join` | the accepting authority | node, node_id, incarnation_id, issued_at, expires_at, issuer_expires_at | e3 |
| `rdm.node_admin.cert.update.via-renewal` | the issuing authority | node, node_id, incarnation_id, issued_at, expires_at, previous_expires_at, outcome ∈ {`installed`, `not-sent`, `refused`} | e3 |
| `rdm.node.cert.update.via-installed` | the node | node, node_id, incarnation_id, expires_at | e3 |
| `rdm.fabric.issuer.update.via-renewal` | the fabric-primary | mesh, issuer_expires_at, previous_issuer_expires_at, admins, outcome | e3 |
| `rdm.node_admin.issuer.update.via-installed` | the admin | node, mesh, issuer_expires_at | e3 |
| `rdm.fabric.ca.update.via-renewal` | the fabric-primary | ca_expires_at, previous_ca_expires_at, meshes, outcome | e5 |
| `rdm.fabric.root.update.via-caller` | the fabric-primary | phase, root_expires_at, previous_root_expires_at, nodes, outcome | e5 |
| `rdm.node.roots.update.via-installed` | each node | node, roots, phase | e5 |
| `rdm.node_admin.cert.create.via-no-certs-configured` (exists) | a `NoCerts` admin | unchanged | control |
| control: no admission span on a Node RPC or gossip connection | — | — | every e2 cell |

## § Per-sprint seed-data obligation

NO NEW ENTITY: the cert row is a new status-row kind of the existing birth (`node-cert/1`, the e3
landing carries `durable format moved`); `Refused` is a new value of the connections log's state (the e2
landing carries the marker); `NodeView.cert`, `RenewCert` / `CertRenewed`, `UpdateIssuer` /
`IssuerUpdated` are wire additions (`wire format moved`, mesh-audit reseal, goldens extended by hand).
No new gossip frame and no new `MemberStatus`.

## § Test tier coverage

| Tier | What | Where |
|---|---|---|
| unit | the format: sign, verify, each refusal by name; `MemberOutlivesIssuer`; the skew window; the renew-at computation with jitter | `rdm-core/src/certs.rs` |
| component | the admission exchange over a loopback endpoint: admitted, each refusal, no-cert within the floor, a reset stream; a `NoCerts` acceptor | `rdm-transport` |
| live (test kit mesh) | a member renewed with a seconds-long TTL dials past its first expiry and is admitted; the row and both spans; a successor mesh-primary continues the schedule; an issuer rotated and a member signed by the new issuer admitted by a peer that still holds the old; a stalled mesh's row is surfaced on `fabric.get` | `rdm-test` |
| rafka | the Rafka lane with Rafka's node-admin and belt deleted: every data-plane dial admitted through RDM's step; no `rafka.mesh.auth.*` span | rafka-v2 gate |

## Implementation plan

#### e1 — the format moves into RDM; `CertChoice` is the material, not a signer

- **Goal → gap.** fabric-certs.md §"How RDM is instantiated" (`CertChoice::Ca` / `Issuer` / `NoCerts`).
  The tree has `CertSigner` and no format.
- **Requirements, with coordinates.** R1: `certs.rs:29` (the trait, deleted), rafka `cert.rs` (the
  types, moved to `rdm_core::certs` with their tests), `CertChoice` (`certs.rs:58`).
- **Spans.** None new; `via-no-certs-configured` unchanged.
- **Acceptance cells.** `a_member_cert_verifies_to_its_issuer_and_the_issuer_to_the_root`;
  `each_refusal_is_named` (one per reason); `a_member_cert_never_outlives_its_issuer`;
  `a_node_admin_started_with_no_cert_choice_is_refused_at_start` (exists, kept);
  `a_launched_admin_signs_only_its_own_mesh`. RED first.
- **Page text.** None beyond the set applied 2026-10-16.
- **Sizing.** One seat; `rdm-core/src/certs.rs` (new), `rdm-admin/src/certs.rs`, `wiring.rs`.
- **State.** not started.

#### e2 — the accept loop and `node.connections.admit` are rdm-transport's

- **Goal → gap.** fabric-certs.md §"On a connection"; node-connections-admit.md; connections.md §1.2
  `Refused`. rdm-transport binds the endpoint but accepts only Node RPC; Rafka's loop does the rest.
- **Requirements, with coordinates.** R2 rafka `lib.rs:10748`; R3 rafka `lib.rs:2817`, `:3056`; R4
  `endpoint.rs:30`; R5 connections.md §1.2, §12.
- **Spans.** `via-admitted`, `via-refused`, `via-presented`; the control.
- **Acceptance cells.** `an_inbound_connection_on_a_registered_alpn_is_admitted_on_a_valid_cert`;
  `…_is_refused_by_name_on_<reason>` ×6; `a_dial_that_presents_nothing_within_the_floor_is_no_credential`;
  `a_refusal_is_a_refused_fact_on_the_acceptor_and_a_failed_row_on_the_dialer`;
  `registering_the_step_for_node_rpc_or_gossip_is_refused_at_bind`;
  `a_node_rpc_and_a_gossip_connection_carry_no_admission_span` (control);
  `a_no_certs_acceptor_admits_by_name`. RED first.
- **Page text.** None beyond the set.
- **Sizing.** Two seats (the loop's move; the admission step). `rdm-transport/src/endpoint.rs`,
  `server.rs`, `admission.rs` (new), `membership.rs` (the gossip arm).
- **State.** not started.

#### e3 — the cert row, `node.cert.renew`, `mesh.issuer.renew`, `node.issuer.update`, policy, the watch

- **Goal → gap.** node-cert-renew.md, mesh-issuer-renew.md, node-issuer-update.md, fabric-certs.md
  §Tracking. Nothing renews, no row, no policy.
- **Requirements, with coordinates.** R6 `certs.rs:122` (the issue site), `status_storage.rs:171`
  (`NodeHeldRunningRow`, the row pattern), `rdm-core/src/views.rs` (`NodeView`); R7 the mesh-primary's
  tick in `admin/boot.rs` (the recovery tick is the pattern), `status.rs` (`RenewCert`, `CertRenewed`
  appended), the node's install in `rdm-node`; R8 the fabric-primary's tick, `UpdateIssuer` /
  `IssuerUpdated`, the mesh's issuer row; R9 the entry pull's policy fields (`MESH_SPAWN_TYPE` is the
  pattern); R10 the fabric-level projection surfaced on `fabric.get`.
- **Spans.** the five e3 spans in the bar.
- **Acceptance cells.** `a_join_writes_the_births_cert_row_and_node_get_shows_it`;
  `a_member_is_renewed_at_the_policys_point_and_dials_past_its_first_expiry_admitted`;
  `a_renewal_terminates_nothing_and_publishes_nothing` (control: no connection transition, no frame);
  `a_renewal_that_is_not_sent_is_retried_each_tick_and_marks_nothing`;
  `a_cert_for_another_birth_is_refused_by_name`;
  `a_successor_mesh_primary_continues_the_schedule_from_the_rows`;
  `an_issuer_is_rotated_and_a_member_signed_by_the_new_issuer_is_admitted_by_a_peer_holding_the_old`;
  `a_member_due_near_the_issuers_expiry_is_cut_to_it`;
  `two_admins_read_the_same_policy_from_the_entry_pull`;
  `a_mesh_whose_renewals_stall_has_its_row_surfaced_on_fabric_get`. RED first.
- **Page text.** None beyond the set.
- **Sizing.** Two seats (member renewal + row + policy; issuer renewal + watch).
- **State.** not started.

#### e5 — the CA under the root: `fabric.ca.create`, `fabric.ca.renew`, `fabric.root.update`; every admin holds the root

- **Goal → gap.** fabric-ca-renew.md; fabric-certs.md §chain, §Custody. The root signs issuers directly;
  there is no CA level; verifiers hold one key; a launched admin is not written the root.
- **Requirements, with coordinates.** R13 the fabric-primary's tick (e3's issuer renewal is the
  pattern), `status.rs` (`UpdateRoots` / `RootsUpdated` appended), the node's root set in
  `rdm-transport/src/admission.rs`, the launch env (`RDM_ROOTS`, `pipeline.rs` writes it like
  `RDM_MESH_ID`), the fabric's roots row; R13b the caller op on the fabric-primary's door
  (`fabric.update` is the pattern); R14 the launch env (`RDM_CA_SECRET` written for a launched admin
  beside `RDM_ISSUING_KEY`), `node.roots.update`'s admin arm carrying the secret.
- **Wire note.** `RenewCert`, `UpdateIssuer` and `UpdateRoots` are single-reply installs on Status 0x1B, the `ApplyNodeConfig` shape; they are not progress streams, so 0x22 NodeCommand (the streaming mesh-primary→node command hop) is not theirs. `fabric.root.update` is a caller op to the fabric-primary like `fabric.update`.
- **Spans.** `rdm.fabric.ca.update.via-renewal`, `rdm.fabric.root.update.via-caller`,
  `rdm.node.roots.update.via-installed` (with `secret` ∈ {`carried`, `public-only`}).
- **Acceptance cells.** `the_chain_verifies_member_issuer_ca_root_and_each_level_never_outlives_the_one_above`;
  `an_internal_ca_renewal_re_mints_every_issuer_and_pushes_no_roots_update` (control);
  `a_root_update_runs_through_its_five_phases_with_a_dial_admitted_at_each`;
  `a_retire_before_every_chain_moved_is_refused_by_name`;
  `a_node_born_mid_rotation_trusts_both_roots`;
  `a_successor_fabric_primary_continues_the_rotation_from_the_roots_row`;
  `a_second_root_update_in_flight_is_refused_by_name`;
  `an_elected_successor_mints_and_rotates_from_its_own_copy_of_the_root`;
  `a_launched_admin_is_written_the_root_and_a_launched_rpc_node_is_not`;
  `a_rotation_not_retired_by_the_old_roots_expiry_is_the_fabrics_concern`. RED first.
- **Page text.** fabric-ca-renew.md and fabric-certs.md §Custody applied 2026-10-16 (Q7: A).
- **Sizing.** One seat.
- **State.** not started.

#### e4 — Rafka passes the root in; its node-admin minting, its accept loop and the belt go

- **Goal → gap.** fabric-certs.md §"Where a node-admin's signing material comes from";
  internal-mesh-auth.md's table. Rafka's `node-admin/` still mints and its base still accepts.
- **Requirements, with coordinates.** R11 rafka `node-admin/src/lib.rs:5806`, `main.rs:4713`,
  `:5593-5624`, `:12761`, `rafka-node-base/src/lib.rs:2681-2760`, `:2817`, `:3056`, `:10748`,
  `scripts/lane-mesh-deploy.sh:1262-1265` (passes `RDM_CA_SECRET`; `RAFKA_CA_PUBKEY` unread).
- **Dependency edge.** Rafka's node-admin adopts RDM's (the i141/i142 absorption) or, until then,
  Rafka's node-admin is started with `CertChoice::Ca` and delegates issuance to RDM's `certs` module:
  the lead decides at dispatch with the Rafka engineer.
- **Acceptance cells.** the Rafka gate: `every_data_plane_dial_is_admitted_through_rdms_step`;
  `no_rafka_mesh_auth_span_remains` (control); the e12.s69 criteria as rewritten.
- **Page text.** The two rafka lines applied 2026-10-16; `data-plane-transport.md:147` (the
  `IrohMeshTransport` row) when the loop goes.
- **Sizing.** One seat, Rafka engineer, coordinated with the dev lead.
- **State.** not started.

### Files likely to touch

RDM: `crates/rdm-core/src/certs.rs` (new), `status.rs`, `views.rs`; `crates/rdm-transport/src/endpoint.rs`,
`server.rs`, `admission.rs` (new), `membership.rs`, `wire.rs`; `crates/rdm-admin/src/certs.rs`,
`wiring.rs`, `status_storage.rs`, `admin/boot.rs`, `admin/rows_writer.rs`, `join.rs`;
`crates/rdm-node/src/node.rs`, `wire.rs`; `crates/rdm-test/src/test_certs.rs`. Rafka: `node-admin/src/`,
`crates/rafka-node-base/src/lib.rs`, `crates/rafka-node-transport/src/cert.rs` (deleted),
`scripts/lane-mesh-deploy.sh`.

## § Per-feature documentation deliverable

Applied 2026-10-16: fabric-certs.md (six lines and the workflows table), connections.md (§1.2, §12),
fabric-mesh-node-lifecycles.md (five rows), node-rpc-envelope.md (four typed-object rows),
fabric-create.md step 1, mesh-create.md step 2, node-mesh-join.md step 5, the four new lifecycle pages,
CLAUDE.md's five env rows, rafka's internal-mesh-auth.md and data-plane-transport.md, i143-prd.md
e12.s69, the ruling page. Each landing updates `docs/reference/rdm/spans.md` and `wire.md` with its
spans and frames.

## Verification

- [ ] e1–e3 green in the RDM gate (`rdm-gate-at.sh <lane> fast`, then `release`); each landing's flame
  cited in its row.
- [ ] e4 green in the Rafka gate; a Rafka lane's data-plane dial shows `via-admitted` and no
  `rafka.mesh.auth.*` span.
- [ ] The control holds in every landing: no admission span on a Node RPC or gossip connection.

## § Dispatch ordering

| wave | unit | why |
|---|---|---|
| 0 | e1 | the format every later epic signs and verifies with |
| 1 | e2 | the admission step; consumes e1 |
| 1 | e3 | the row, renewal, policy, watch; consumes e1; independent of e2's loop (its cells admit through e2's step once both land) |
| 2 | e5 | root rotation and custody; consumes e3's issuer renewal and the row pattern |
| 2 | e4 | Rafka's deletion; after e2 and e3 land and the pin moves |

Critical path: e1 → e2 → e4; e3 runs beside e2; e5 after e3.

## § Retro (YYYY-MM-DD, on ship)

## Cross-references

- `docs/plans/proposal/cert-verification-ownership.md` (Q1–Q6, the delta and the vocabulary)
- `docs/plans/rulings/i145-rulings-2026-10-16-cert-ownership.md` (R-CT1)
- `docs/plans/i143-prd.md` e12.s69 (rewritten), i141 PRD line 61 (R-SV1, superseded)
- `docs/architecture/fabric-certs.md`, `connections.md`, the four lifecycle pages
