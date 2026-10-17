# Q34 appendix — reviewed binding, cost and page lines

**Proposal only; no envelope code or architecture edits before Q34 is ruled.**

## Binding frozen by arm A

One gossip message is postcard `(Vec<(String, String)>, Vec<u8>)`: a metadata list followed by
opaque body bytes. The attributes are literal standard names, with no RDM-named attribute. This
is the reviewed binary-mode protocol binding: **no JSON binding on the gossip wire**. Metadata is
inspectable without decoding data. Encoding order is specversion, id, source, type, subject,
sequence when present, time, datacontenttype; decoding is independent of pair order. Duplicate
attribute names are refused. Validate type before selecting or decoding a data schema.

Each string is varint byte length + UTF-8 bytes; each vector has its count/length prefix. The
second vector contains the kind's postcard data, without an additional outer membership-kind
discriminant. The codec still rejects trailing bytes. The body keeps each existing variant's fields in their current declaration order (without the outer enum tag), except snapshot_id's reviewed birth/counter shape below; authority revision is added only in status sequence. Existing WireDigest and ProjectionEntry payload schemas remain data. Any duplicated context must agree with the header. No handler
reconstructs author birth, event identity or sequence from a name or transport sender.

| Attribute | Exact encoding/value |
|---|---|
| specversion | String `1.0` |
| id | Nonempty deterministic String derived from the frame's declared identity, namespaced by its literal type, using the reviewed escaped tuple grammar. A resend keeps the same id; no delivery layer or new delivery counter. Content/identity comparisons and data-validity checks preserve e2's inconsistency outcomes, rather than suppressing conflicting data solely because an id repeats. |
| source | String `rdm://<fabric_id>/<node_id>/<incarnation>`; canonical typed identifiers; always the original author, including an empty projection. |
| type | One of the 22 exact literals below. |
| subject | String with the reviewed percent-escaped subject grammar. Ordinary operation subject names its full exact-birth operation; transfer subject is exactly `handover/<build_id>/<attempt>/<operation>`. |
| sequence | Optional String: scalar `P(u64)` for Digest, Members, MembersDelta and Seated; composite `P(seat_revision) + "." + P(changed_at_rafka_ms)` for statuses; absent on operation facts and reserved kinds. P has exactly 20 zero-padded decimal digits. sequencetype is absent. |
| time | RFC3339 UTC with millisecond precision from the authored rafka-time; resends retain it. It is never an authority revision or an unrelated-operation ordering. |
| datacontenttype | String `application/octet-stream`. |
| data | Opaque postcard bytes selected by type; no JSON/base64 representation on this wire. |

Subject components use UTF-8 percent encoding, unreserved ASCII unchanged and escapes uppercase.
Birth: `birth/<fabric_id>/<node_id>/<incarnation>`; projection:
`mesh/<mesh>/publisher/<fabric_id>/<node_id>/<incarnation>`; mesh status: `mesh/<mesh>/status`;
fabric status: `fabric/<fabric_id>/status`; cohort: `cohort/<mesh>/<node_type>` or
`fabric/<fabric_id>/seat`; lifecycle: `operation/<build_id>/<attempt>/<operation>/birth/<node_id>/<incarnation>`;
round: `round/<fabric_id>/<build_id>/<attempt>/<operation>/birth/<node_id>/<incarnation>`.
Node-admin aliases normalize to the complete mesh-primary cohort as in e2.

Ids use `type + '/' + escaped identity tuple components` in the reviewed mapping. They identify
facts, not transport sends. Snapshot identity includes (source projection, topology_version,
publication owner's exact birth, snapshot counter, chunk_index); the publication owner is the
original source for its full and the forwarding birth for its forwarded full. snapshot_id in data
therefore becomes the postcard tuple (fabric_id, node_id, incarnation, counter), with fabric_id/node_id/incarnation encoded as their canonical postcard strings, and counter the existing postcard u64 snapshot counter of that publishing/forwarding birth. All other snapshot data fields retain their order; no new counter is added. source stays the original author.
A new publication at equal material sequence can supply hearing; an exact repeated publication
cannot. Delta retains base_version in data. Logical operation identity remains stable across
carriers/authors; no unrelated newer sequence can supersede it.

The envelope decides operation admission/dedupe; each handler's existing data-validity check
still decides whether its carried projection or seat records may be used. Apply those checks so
an equal-revision conflict is still via-inconsistent and a stale cohort still refuses the entire
seat publication, without replaying the operation's effect. For NodeDeleting/Restarting/Stopped,
record the operation identity even if its carried version is stale; a repeat can still advance
an admissible carried floor without running the fact twice, exactly as e2 proves. Primary facts
retain per-cohort revisions/stage/content in data; their receipt supplies no seat authority.

## The 22 literal types and sequences

| Page position | Kind | Literal type | sequence / subject |
|---|---|---|---|
| 1 | Digest | `rdm.mesh.digest` | P(digest_seq), birth |
| 2 | Members | `rdm.mesh.members` | P(topology_version), source projection |
| 3 | MembersDelta | `rdm.mesh.members.delta` | P(topology_version), source projection; base_version in data |
| 4 | NodeDeleting | `rdm.node.deleting` | absent, lifecycle operation; carried topology_version in data |
| 5 | NodeDeleted | `rdm.node.deleted` | absent, terminal lifecycle operation |
| 6 | NodeRestarting | `rdm.node.restarting` | absent, lifecycle operation; carried topology_version in data |
| 7 | MeshStatus | `rdm.mesh.status` | P(committed mesh seat revision).P(changed_at_rafka_ms), mesh status |
| 8 | FabricStatus | `rdm.fabric.status` | P(committed fabric seat revision).P(changed_at_rafka_ms), fabric status |
| 9 | Seated | `rdm.seat.seated` | P(committed revision), cohort or fabric seat |
| 10 | Concern | `rdm.mesh.concern` | reserved, refuse before body decode |
| 11 | NodeConnectionsDeleting | `rdm.node.connections.deleting` | reserved, refuse before body decode |
| 12 | NodeLeaving | `rdm.node.leaving` | reserved, refuse before body decode |
| 13 | NodeLeft | `rdm.node.left` | reserved, refuse before body decode |
| 14 | NodeConnectionsDeleted | `rdm.node.connections.deleted` | reserved, refuse before body decode |
| 15 | NewFabricPrimary | `rdm.fabric.primary.new` | reserved, refuse before body decode |
| 16 | StateCommitting | `rdm.fabric.state.committing` | absent, round operation |
| 17 | StateCommitted | `rdm.fabric.state.committed` | absent, round operation |
| 18 | TrafficOpening | `rdm.fabric.traffic.opening` | absent, round operation |
| 19 | TrafficOpened | `rdm.fabric.traffic.opened` | absent, round operation |
| 20 | NodeStopped | `rdm.node.stopped` | absent, lifecycle operation; carried topology_version in data |
| 21 | PrimaryUpdated | `rdm.node.primary.updated` | absent, handover operation, settled stage in type |
| 22 | PrimaryUpdating | `rdm.node.primary.updating` | absent, handover operation, intent stage in type |

All six reserved types emit `rdm.mesh.membership.reject.via-reserved` with type and historical
page position before any body decode, hold, notification or carrier. An unknown nonempty type is
refused distinctly (`rdm.mesh.membership.reject.via-unknown-type`), also without decoding data.
Malformed required context remains an undecodable-frame outcome, distinct from a valid reserved
or unknown type. The reserved and unknown-type cells each send an invalid opaque body to prove
that the type outcome precedes payload deserialization.

## Cost and complete-message ceilings

The review accepted string attributes as is and estimated approximately 150 B metadata per digest;
that is an estimate, not an encoded-frame measurement. This packet also exposes the exact byte
accounting for the chosen literal binding, so full-message goldens can check it rather than hiding
attribute names or body-length prefixes. No measured performance or new cost optimization is claimed.

Let L(s) be varint byte length of UTF-8(s) plus UTF-8 byte length, and V(n) the postcard unsigned
varint length. For n metadata pairs and B body bytes, overhead above the raw body is exactly:

```text
V(n) + sum(L(attribute-name) + L(attribute-value)) + V(B)
```

For canonical 12-character fabric/node/incarnation IDs, source is 44 B; RFC3339 ms time is 24 B.
After those fixed values and datacontenttype are counted, the following per-kind constant K gives
`K + L(id) + L(subject) + V(B)` bytes above the body. Sequence-bearing events have 8 pairs,
operations have 7; no sequencetype pair is emitted. This is exact schema arithmetic, not a
measurement of the unimplemented envelope. The actual golden reports measured complete bytes.

| Kind | K (B) |
|---|---:|
| Digest | 202 |
| Members | 203 |
| MembersDelta | 209 |
| NodeDeleting | 174 |
| NodeDeleted | 173 |
| NodeRestarting | 176 |
| MeshStatus | 223 |
| FabricStatus | 225 |
| Seated | 202 |
| StateCommitting | 184 |
| StateCommitted | 183 |
| TrafficOpening | 183 |
| TrafficOpened | 182 |
| NodeStopped | 173 |
| PrimaryUpdated | 181 |
| PrimaryUpdating | 182 |

For example, a Digest with sequence 1, those three 12-character IDs and id built from the flat
(type, birth components, digest_seq) tuple has a 56 B id and 44 B subject: overhead is
304 + V(B) B (306 B for a 128 B body). This explicit encoding cost is larger than the
review's approximate figure; it introduces no alternative encoding or recommendation. All full
messages, including metadata and both prefixes, must fit the unchanged MAX_FRAME of 4032 B (4096 B transport limit minus its existing 64 B reserve). Pack/chunk
measurements use the complete encoded message. Reserved kinds have no publisher cadence/cost.

## Proposed exact page lines — §3.1 only, not applied

### Page replacement 1

Old (verbatim):

```text
| frame | category | subject | ordering | identity |
|---|---|---|---|---|
| `Digest` | replacement state | the exact birth | `digest_seq`, higher is newer | `(birth, digest_seq)` |
| `Members` (source publication and forwarded full) | snapshot | `(mesh, source_publisher)` | `topology_version` against the installed-version floor: within one publisher revision a lower version never overwrites a higher installed one whatever its `published_at_rafka_ms`, including a version installed by a topology read | `(mesh, source_publisher, topology_version, snapshot_id, chunk_index)` |
| `MembersDelta` | delta | `(mesh, source_publisher)` | `base_version` = installed: apply; `topology_version` ≤ installed: stale (duplicate if already applied); `base_version` > installed: gap, repair | `(mesh, source_publisher, base_version, topology_version)` |
| `NodeDeleting`, `NodeRestarting`, `NodeStopped` | operation fact, carrying the publisher's `topology_version` as the projection it rides | `(build_id, attempt, operation, node_id, incarnation)` | the fact: reconciled against the operation's completion and the birth's later lifecycle (a `NodeRestarting` after that restart's `node.started` reopens nothing); the carried `topology_version`: replacement state for the projection | the operation identity |
| `NodeDeleted` | operation fact, terminal | `(build_id, attempt, operation, node_id, incarnation)` | stands after any unrelated frame; never discarded because other traffic arrived first | the operation identity |
| `MeshStatus`, `FabricStatus` | replacement state | `(mesh \| fabric, publisher)` | `changed_at_rafka_ms`, higher is newer. **Gap:** they carry a publisher string and a timestamp, not the publisher's revision, so a delayed predecessor's frame is not refused by revision; closing it is an i147 packet (a field, or the status riding the publisher's projection) | `(subject, publisher, changed_at_rafka_ms)` |
| `Seated` | replacement state | the cohort `(mesh, node_type)`, or the fabric seat | the committed `revision` | `(cohort, revision)` |
| `PrimaryUpdating`, `PrimaryUpdated` | replacement state | each named cohort `(mesh, node_type)` or the fabric seat | the committed `revision` per cohort; at one revision `updated` is later than `updating`; a frame naming two cohorts is ignored when either holds a newer key | `(cohort, revision, stage)` |
| `Concern` (position 10) | reserved | — | — | refused by name (`rdm.mesh.membership.reject.via-reserved`), nothing applied. Retired (Luke, Q23, 2026-10-16): a receiver that started an investigation because a frame told it to was deciding on gossip (§10); the fabric-primary's silence is measured by each admin itself and acted on with the directed tickle |
| `StateCommitting`, `StateCommitted`, `TrafficOpening`, `TrafficOpened` | operation fact | `(kind, fabric_id, build_id, attempt, operation, node_id, incarnation)` | none: a hook drives no checklist and reopens nothing by construction (gossip receipt never replaces the directed completion). Its only effects are its span and, on the mesh-admin holding an open round, carrying an ordinary node's check-in onto the backbone; a hook of a closed or superseded round is carried nowhere (`via-round-frame` with `carried = closed-round`) | the identity |
| `NodeConnectionsDeleting`, `NodeConnectionsDeleted`, `NodeLeaving`, `NodeLeft`, `NewFabricPrimary` | reserved | — | — | refused by name, nothing applied |
```

New:

```text
| frame / type | category | source / subject | sequence / data validity | id / identity |
|---|---|---|---|---|
| `Digest` (`rdm.mesh.digest`) | replacement state | the author birth / that exact birth | `P(digest_seq)`, higher is newer | birth and digest sequence |
| `Members` (`rdm.mesh.members`, source publication and forwarded full) | snapshot | the original author birth / `(mesh, source_publisher)` | `P(topology_version)` against the installed floor, including a topology read; equal material version admits a new publication's hearing and distinct unfinished chunks | source projection, topology version, publication owner's exact birth and counter, chunk index; a resend keeps id |
| `MembersDelta` (`rdm.mesh.members.delta`) | delta | the original author birth / `(mesh, source_publisher)` | `P(topology_version)`; `base_version` in data equals installed to apply, ahead requests repair as via-gap, lower version is stale | source projection, base version and topology version |
| `NodeDeleting`, `NodeRestarting`, `NodeStopped` (`rdm.node.deleting`, `rdm.node.restarting`, `rdm.node.stopped`) | operation fact | the author birth / `(build_id, attempt, operation, node_id, incarnation)` | absent; the operation cannot reopen after completion or the exact birth's later lifecycle. The carried `topology_version` is data, checked on the projection's own subject by its existing per-publisher floor rule, even for a repeated fact | kind and the complete operation identity; a repeat runs no effect twice, including when the carried version is refused |
| `NodeDeleted` (`rdm.node.deleted`) | operation fact, terminal | the author birth / `(build_id, attempt, operation, node_id, incarnation)` | absent; stands after unrelated traffic | kind and the complete operation identity |
| `MeshStatus`, `FabricStatus` (`rdm.mesh.status`, `rdm.fabric.status`) | replacement state | the author birth / stable mesh or fabric status scope | `P(committed seat revision) + "." + P(changed_at_rafka_ms)`, lexical order; the publisher takes its revision from its own held committed seat row. A predecessor's later timestamp loses to the successor's higher revision; within one holder its change time orders changes | subject and composite sequence; equal sequence is duplicate with identical content, inconsistent with different content |
| `Seated` (`rdm.seat.seated`) | replacement state | the announcing birth / complete cohort `(mesh, node_type)` or fabric seat | `P(committed revision)` against the installed cohort record | cohort and committed revision |
| `PrimaryUpdating`, `PrimaryUpdated` (`rdm.node.primary.updating`, `rdm.node.primary.updated`) | operation fact: announcement of one committed transfer | the announcing birth / `handover/<build_id>/<attempt>/<operation>` | absent; type names the stage. Per-cohort revisions are data: every cohort is checked atomically against the committed rows, the whole frame is refused when either cohort is stale, and updated is later than updating at one revision. Equal-revision content conflicts remain inconsistent. Gossip announces, never supplies authority | type/stage and transfer operation; a repeat runs no effect twice |
| `Concern` (`rdm.mesh.concern`, position 10) | reserved | not read to act | absent; type refused before decoding data | via-reserved, nothing applied |
| `StateCommitting`, `StateCommitted`, `TrafficOpening`, `TrafficOpened` (`rdm.fabric.state.committing`, `rdm.fabric.state.committed`, `rdm.fabric.traffic.opening`, `rdm.fabric.traffic.opened`) | operation fact | the original author birth / `(fabric_id, build_id, attempt, operation, node_id, incarnation)` | absent; Q21 stands: no checklist or reopening, ordinary span and the existing open-round carrier only; a closed or superseded round is carried nowhere | kind and complete round operation identity; a repeat invokes no carrier |
| `NodeConnectionsDeleting`, `NodeLeaving`, `NodeLeft`, `NodeConnectionsDeleted`, `NewFabricPrimary` (`rdm.node.connections.deleting`, `rdm.node.leaving`, `rdm.node.left`, `rdm.node.connections.deleted`, `rdm.fabric.primary.new`) | reserved | not read to act | absent; type refused before decoding data | via-reserved, nothing applied |
```

### Page replacement 2

Old (verbatim):

```text
A publisher change: a delayed frame from a predecessor publisher cannot overwrite the successor's
projection where the frame carries the publisher's committed revision (`Members`, `Seated`,
`PrimaryUpdated`); for `MeshStatus` and `FabricStatus` this is the gap above until closed.
```

New:

```text
A publisher change: a delayed predecessor cannot replace the successor's status: the author's
committed seat revision is the first component of status sequence, before change time. The other
source-projection and cohort floors remain as specified above.
```

### Page replacement 3

Old (verbatim):

```text
The frame enum keeps the positions of NodeConnectionsDeleting, NodeLeaving, NodeLeft, NodeConnectionsDeleted and
NewFabricPrimary. None of the five is published.
```

New:

```text
The six reserved type names in the receive table retain their historical page positions; their
old numeric wire discriminants are not reused. None is published. Each type is refused before data
decode as via-reserved; an unknown type is refused distinctly.
```

Before the receive table insert: `P(n) is the 20-digit zero-padded decimal representation of the existing u64 version. Context is CloudEvents §3.4; sequencetype is unset. Operation facts omit sequence. Identity admission is in the envelope; carried projection and committed-seat data retain their existing validity rules. A receiver decides nothing on a frame.`

No sentence or table in §3.4 changes. The earlier catalog continues to describe each kind's domain data, channels, author and retained state; the receive table above states its standard context and type literals.


## Acceptance and the one envelope reseal

Arm A lands the exact approved page lines and implementation together, named **wire format moved**.
Reseal mesh-audit and every membership byte golden/full-message ceiling once. Build-topic and
Node RPC payloads remain separately owned; the already-landed PublisherId prerequisite is not
repeated as another envelope schema cut. No dual decoder, acknowledgements or new cadence.

- Every e2 cell passes on this binding with assertions, sequencing and bounds unchanged; only
  encoding fixtures/API arguments supply the real node's own digest birth or the probe's minted
  node_id/incarnation. Never derive author birth from path.name or endpoint.
- R10: successor revision 2 at time 1000 installs; predecessor revision 1 at time 9000 is stale;
  the successor stands. Also retain within-holder timestamp order, duplicate and inconsistency
  controls. The status publisher receives its own held committed-seat revision, never a fresh
  lookup by publisher name; reinforcing sends retain the original composite sequence.
- Envelope-only fact admission and independent data validity preserve lifecycle completed-birth
  evidence, atomic all-cohort checks, stage order, closed-round carrier behavior, and no authority
  from gossip. Repeat identity suppression cannot mask an inconsistent data control.
- Reserved and unknown type tests prove refusal before data decode. Attribute-order, duplicate
  names, malformed URI/sequence/time, empty required values and trailing-byte controls cover the
  binding; semantic body/context disagreements are refused before installation.
- Reordered complete chunks, unfinished equal-sequence chunks, two forwarders at one material
  version, new hearing versus exact repeats, delta exact base and update.via-gap repair, read floors
  and cross-channel floors all remain green. R10 stays open until its code and cells pass.
- The real topology-read recovery cell supplies the live post-envelope flame. Retrieve that trace,
  regenerate spans/frame/wire references and run rebase-gate-land.sh for the one envelope landing.
