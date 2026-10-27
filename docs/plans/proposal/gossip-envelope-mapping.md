# Gossip envelope mapping for a1 review — i147 Q34 preparation

**Reviewed by a1, approved for the Q34 packet, 2026-10-17. No envelope implementation authorized before Q34 is ruled.**
Luke accepted e2 at `7d00bb2e`. The gap outcome correction is a separate landing.
No envelope code, architecture text, envelope seal or envelope golden has changed. Q34's packet will freeze this reviewed mapping against gossip.md §3.4 and the PRD's e2 envelope notes.

## Contract and ownership

[gossip.md §3.4](../../architecture/gossip.md#34-the-envelope) chooses CloudEvents 1.0 plus the
Sequence extension. An event's source is its author, never its forwarder. The author is the
node for a heartbeat or ordinary round check-in, the directing authority for a round intent,
the mesh executor for a lifecycle fact, the source mesh-primary for a projection, and the
corresponding committed seat holder for a status. Forwarders preserve authorship.

The receiving node retains e2's category rules and the actual installed floors, including
floors installed by a topology read. Gossip drives no checklist, investigation, vacancy or
seat move. Q21's existing open-round book alone decides carrying an ordinary check-in.

Primary sources consulted:

- [CloudEvents 1.0.2](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md): context and data are independently inspectable; binary mode puts attributes in message metadata and event data in the message body. Postcard is the proposed gossip binding, not a standardized CloudEvents event format.
- [Sequence extension at v1.0.2](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/extensions/sequence.md): sequence is a nonempty, lexicographically orderable String. Its optional Integer sequencetype means a contiguous, wrapping signed 32-bit counter; that does not fit the existing u64 versions. Do not emit sequencetype=Integer.
- [Postcard wire specification](https://postcard.jamesmunns.com/wire-format.html): strings and sequences carry a length prefix; unsigned lengths use base-128 varints; tuples concatenate their fields. Schema compatibility belongs to this binding's single reseal.

## Proposed exact binary binding

One gossip message is the postcard encoding of a two-field tuple:

```text
(Vec<(String, String)>, Vec<u8>)
  message metadata       body
```

This is notation for the proposal, not a Rust implementation. Metadata is decoded independently
of the opaque body. Metadata names are literal standard attribute strings; no RDM extension
attribute, Rust enum ordinal, author field or forwarded_by attribute is added. The second vector
contains the postcard payload schema selected by type, without a second membership-kind enum tag.
There is no JSON on the gossip wire, base64 wrapper or second codec. JSON-format support required
by CloudEvents must be accounted for in the review without changing that wire choice.

The encoder emits metadata in this order: specversion, id, source, type, subject, sequence (when
applicable as decided below), time, datacontenttype. Each pair is two postcard strings: a varint
UTF-8 byte length followed by bytes, twice. The outer metadata vector begins with its pair count;
the body vector begins with its byte count. No byte follows the body. The decoder accepts
attribute order independently, rejects duplicate required attributes, validates standard value
types, and selects/refuses type before deserializing a domain body. Reserved types must reach
via-reserved even when their uninterpreted body has no valid old reserved payload.

| Attribute | Proposed exact representation and invariant |
|---|---|
| specversion | String `1.0` |
| id | Nonempty String; deterministic from the existing declared frame identity, namespaced by type; retained on resend. Concrete grammar below needs a1 review for transformed publications. Content is not hashed into an operation id: changed content must not invent a new operation. |
| source | String `rdm://<fabric_id>/<node_id>/<incarnation>`; each identifier is its canonical typed Crockford representation. No path.name, endpoint id or inferred current holder in place of the actual author birth. |
| type | One literal from the 22-row table below. |
| subject | Nonempty canonical String; grammar below. It describes the receiver's held subject, not the hop. |
| sequence | String. Scalar u64 versions use exactly 20 decimal digits, zero-padded, including zero; lexical and numeric order then agree. No invented counter. Composite or absent values remain review items below. sequencetype is omitted; semantics are the reviewed per-type binding. |
| time | RFC3339 UTC String with millisecond precision, converted from the original fact's rafka-time; e.g. `1970-01-01T00:00:00.001Z` for 1 ms. Resends preserve it. Never a replacement-order fallback. |
| datacontenttype | String `application/octet-stream`; data is the kind's postcard payload. |
| data | Raw body bytes, outside the metadata vector. |

A source is retained with the authored value through receipt, read installation, replay and
forwarding. An empty publication must still name its author. PublisherId currently lacks node_id;
a lookup of path.name at forwarding time could select a successor and is insufficient. The
future publisher API must receive its exact author birth when it creates the event, and retained
values must keep that source. An old incarnation is not earlier by lexical URI order; existing
exact-birth/lineage evidence and committed authority revisions decide succession.

## Subject and delivery-identity grammar for review

Path components below use UTF-8 percent encoding: only ASCII unreserved characters pass through,
hex digits in escapes are uppercase. Separators are literal `/`; a fabric/node/incarnation uses
its canonical typed representation. P(n) is the padded 20-digit sequence representation above.

- Birth subject: `birth/<fabric_id>/<node_id>/<incarnation>`.
- Projection subject: `mesh/<mesh>/publisher/<fabric_id>/<node_id>/<incarnation>`.
- Mesh status subject: `mesh/<mesh>/status`; fabric status: `fabric/<fabric_id>/status`.
  Authority succession must be compared for this stable scope, not hidden behind separate
  per-source holds that would let a predecessor install after its successor.
- Seat subject: `cohort/<mesh>/<node_type>` (canonical node-admin alias agrees with mesh-primary)
  or `fabric/<fabric_id>/seat`.
- Lifecycle operation subject: `operation/<build_id>/<attempt>/<operation>/birth/<node_id>/<incarnation>`.
- Round subject: `round/<fabric_id>/<build_id>/<attempt>/<operation>/birth/<node_id>/<incarnation>`.
  The kind remains in the identity. A hook's subject is the commanded/reporting birth; its source
  need not be that birth for a command intent.

Proposed ids use `type + '/' + escaped identity tuple components`, with canonical decimal
integers and no transport or forwarding fields. Digest identity is (birth, digest_seq); MeshTopology
is (source projection, topology_version, snapshot_id, chunk_index); delta is (source projection,
base_version, topology_version); a lifecycle fact is (kind, build_id, attempt, operation, node_id,
incarnation); a round hook additionally includes fabric_id. Identity suppression for operations
must retain the full logical tuple across author changes, rather than relying only on source+id.
A seat identity includes cohort/revision/stage; content comparison still detects inconsistent
claims. Do not discard a new snapshot publication just because sequence equals the material floor.

## All 22 membership kinds: proposed type and required sequence meaning

Position here is the page's one-based catalog position, not a retained binary discriminant.
Literal types not already exemplified on the page are proposals for a1, not an existing registry.
D means the padded digest_seq; V means padded topology_version; R means padded seat revision.
O and M are the unresolved operation/composite mappings described after the table.

| Position | Current kind | Proposed literal type | Source / subject / sequence |
|---|---|---|---|
| 1 | Digest | `rdm.mesh.digest` | birth / birth / D |
| 2 | MeshTopology | `rdm.mesh.members` | source primary / projection / V; publication and chunk identity remain distinct from V |
| 3 | MeshTopologyDelta | `rdm.mesh.members.delta` | source primary / projection / V; base_version stays an explicit dependency |
| 4 | NodeDeleting | `rdm.node.deleting` | executor / lifecycle operation / O, plus the carried projection's V |
| 5 | NodeDeleted | `rdm.node.deleted` | executor / lifecycle operation / O, terminal |
| 6 | NodeRestarting | `rdm.node.restarting` | executor / lifecycle operation / O, plus carried projection V |
| 7 | MeshStatus | `rdm.mesh.status` | mesh-primary / mesh status / author's committed authority revision (R10), with the within-author change order still to freeze |
| 8 | FabricStatus | `rdm.fabric.status` | fabric-primary / fabric status / author's committed authority revision (R10), with the within-author change order still to freeze |
| 9 | Seated | `rdm.seat.seated` | announcing holder / cohort or fabric seat / R |
| 10 | Concern | `rdm.mesh.concern` | reserved; refuse by type, no subject/body/sequence effect |
| 11 | NodeConnectionsDeleting | `rdm.node.connections.deleting` | reserved; refuse by type |
| 12 | NodeLeaving | `rdm.node.leaving` | reserved; refuse by type |
| 13 | NodeLeft | `rdm.node.left` | reserved; refuse by type |
| 14 | NodeConnectionsDeleted | `rdm.node.connections.deleted` | reserved; refuse by type |
| 15 | NewFabricPrimary | `rdm.fabric.primary.new` | reserved; refuse by type |
| 16 | StateCommitting | `rdm.fabric.state.committing` | directing authority / round subject / O; never checklist evidence |
| 17 | StateCommitted | `rdm.fabric.state.committed` | reporting birth / round subject / O; Q21 carrier only |
| 18 | TrafficOpening | `rdm.fabric.traffic.opening` | directing authority / round subject / O; never checklist evidence |
| 19 | TrafficOpened | `rdm.fabric.traffic.opened` | reporting birth / round subject / O; Q21 carrier only |
| 20 | NodeStopped | `rdm.node.stopped` | executor / lifecycle operation / O, plus carried projection V |
| 21 | PrimaryUpdated | `rdm.node.primary.updated` | announcing incumbent / all named seats / M, settled stage |
| 22 | PrimaryUpdating | `rdm.node.primary.updating` | incumbent / all named seats / M, intent stage |

Reserved types emit rdm.mesh.membership.reject.via-reserved with their historical page position;
none is published. Removing their old payload decoders is not permission to reuse their names.
An unknown type is distinct from one of these six reserved types and must be refused without
installing a body. Type validation and undecodable-envelope outcomes need explicit acceptance cells.

## a1 review must resolve these mappings before the packet freezes bytes

These are parts of Q34 preparation, not new independent rulings or implementation choices.

1. **Operation sequence (O).** e2 has identity, not an ordered counter, for lifecycle facts and
   round hooks. Should these events omit Sequence (the extension is optional at the event level),
   or does the page intend an existing operation-stage value in sequence? The latter needs exact
   values and a rule preserving Q21: passive hooks drive no checklist and install no completion.
   Never use build attempt, wall time or unrelated traffic as a universal operation version.
2. **Two independent orders on a lifecycle fact.** NodeDeleting/Restarting/Stopped must record
   operation identity even when the projection's topology_version is stale; a repeated operation
   can advance its publisher's carried floor without replaying the fact. One generic scalar
   high-water check cannot perform both. Review a standard-attribute encoding that preserves the
   operation subject plus projection subject/version; keeping V in data preserves behavior but
   fails a literal requirement that the classifier inspect only the envelope.
3. **Multi-seat atomicity (M).** A Primary frame can carry mesh revision 4 and fabric revision 9;
   it is refused in full if either is stale. Updating is earlier than updated at the same revision.
   A single scalar cannot encode this partial order. Review exact subject/sequence strings for
   the sorted cohort/revision/stage vector, or clarify the envelope-only requirement. Taking max,
   splitting into independently installed events, or adding a frame-wide counter breaks e2.
4. **R10 revision provenance and status changes.** The status publisher currently accepts no
   committed-revision input. Identify the exact existing committed revision and how it reaches
   creation of each status. A seat's revision orders authority succession but stays unchanged
   through multiple different statuses under the same holder. Freeze how sequence also preserves
   those changes, repeated identical changes and equal-version inconsistencies without a new
   counter or silently treating time as authority. The R10 cell must install a successor status,
   deliver a predecessor with a later timestamp, and retain the successor by committed revision.
5. **Forwarded publication identity.** The forwarder currently strips loads, generates a new full
   and snapshot_id, and derives deltas from retained projections. Specify how these publication
   identities are unique under the original source, preserved on resend, and distinct for new
   equal-material-version hearing, including two forwarders producing publications concurrently.
   Never switch source to the forwarder or dedupe all equal V publications.
6. **Unchanged e2 cells and valid authorship.** Existing cells call Frame::encode directly on
   frames whose publisher is only a name/incarnation or a status string. They do not supply a
   complete author birth for an empty projection. Review how the harness supplies that birth
   without changing any cell's assertions, sequencing or bounds; do not invent a node_id from a
   path.name or endpoint. If “unchanged” includes those constructors, the encoding API seam must
   be resolved explicitly before code.

## Evidence and code for the subsequent packet

Code baseline: accepted e2 `7d00bb2e`; the gap correction only renames its outcome. Line references
below are that baseline (verify/rebase them again when a1's review arrives).

- [code] membership.rs:332 defines the 22-variant Frame; :615–622 encodes that enum through wire.rs.
- [code] wire.rs:35–50 has the one postcard codec and rejects trailing bytes.
- [code] rdm-core/src/publisher.rs:10 lacks node_id; lifecycle.rs:42 uses a path-name hook publisher.
- [code] membership.rs:792–825 constructs statuses from scope/name/time with no committed revision.
- [code] snapshot.rs:773–790 generates a forwarder's full/snapshot_id, :815 derives its delta.
- [code] receive_order.rs module doc enumerates all e2 invariants; seat receipts compare each cohort
  before installing any record, and lifecycle receipt and projection ordering are independent.
- [code] tools/mesh-audit/src/lock.rs:724–740 seals the one codec and existing frame golden cells.
- [flame] [read-recovery MeshStatus refusal](http://localhost:16686/trace/c8d4b6bbaae1f6eb18fce611c5dde949)
  names changed_at_rafka_ms=99 vs held=100; it does not demonstrate a committed-author revision.
- [flame] [same-version fresh publication](http://localhost:16686/trace/80277ebfb8e059ff71f759b87acbf073)
  versus [exact repeat ignored](http://localhost:16686/trace/0e5a602e6588596d2c738324b259a8f5)
  is the e2 guard against collapsing material sequence and publication identity.

The Q34 packet will paste the two or three original span records verbatim, identify the reviewed
commit, give the exact old → new page lines, and put the delta before arms. No envelope flame or
R10 GREEN is claimed before implementation.

## One reseal and proof after review and Luke's ruling

One implementation landing declares **wire format moved**, replaces all 22 old enum wire forms
with this reviewed binding in one cut, and reseals mesh-audit, every membership golden and the
encoded-size ceilings together. Existing Build-topic and Node RPC schemas are separately owned;
this is no permission to change them. No dual decoder or staged second reseal is proposed.

The proof is all e2 cells GREEN on the envelope with assertions, sequencing and bounds preserved,
plus R10's delayed-predecessor cell, per-type author/source mapping, reserved-type refusal before
data decode, malformed header/type/source/sequence controls, byte goldens and full-message size
checks. Keep atomic chunks, cross-channel/read floors, fresh publication hearing and exact-repeat
suppression. Re-run the real topology-read recovery cell as the live flame. R10 stays open until
that revision-carrying status path and its refusal cell actually pass.

## a1 disposition supplied by Luke, 2026-10-17

The binding and the 22 type strings above are accepted for Q34. This section resolves the review
items above; those questions remain as the record of what was reviewed, not open alternatives.

- Literal standard attributes in the postcard metadata/body tuple are a conformant binary-mode
  protocol binding. Record **no JSON binding on the gossip wire**. Attribute decode is independent
  of order, duplicate attributes refused, type validated before data.
- String metadata cost is accepted as is: approximately 150 B per digest frame against a body of
  comparable size. This is a1's estimate, not a new measurement. The packet states actual per-kind
  encoded overhead and goldens cover the complete message ceiling; revisit only on measured cost.
- An id derived from the frame's declared identity is conformant; source+id must be unique and a
  resend retains its id. Gossip has no delivery layer. The declared fact supplies dedupe.
- PublisherId first gains node_id in its own landing. The publishing API takes the author's exact
  birth and retains it through receipt, install, replay and forwarding, with no lookup by name.
- O: operation events omit sequence. Their subject is the operation; id repeats are duplicate;
  local completion/later exact-birth lifecycle evidence prevents reopening. No synthetic counter.
- The lifecycle event's carried topology_version remains data. The envelope classifies the fact;
  the handler applies the independent per-projection floor check as its data-validity rule (R-RX1).
- M: PrimaryUpdating and PrimaryUpdated are operation facts. Their subject is
  handover/<build_id>/<attempt>/<operation>; type carries stage; sequence is absent. Cohort revisions
  stay in data; the handler checks every cohort atomically, preserving no partial install, stale
  refusal, same-revision stage ordering, duplicate and inconsistent-content behavior. Q34's exact
  page lines change the catalog category accordingly; gossip is an announcement, never authority.
- R10: status sequence is P(committed seat revision) + "." + P(changed_at_rafka_ms), sequencetype
  absent. The holder supplies the revision of its own held seat row to its status publisher.
  A predecessor with a later timestamp loses to a successor's higher revision; within a holder,
  change time orders changes. Equal sequence is duplicate or, for different content, inconsistent.
- A forwarded publication retains the original author as source. Its snapshot identity in data is
  the forwarder's exact birth plus its counter; concurrent forwarders differ, resends retain id,
  and new publications at equal material sequence remain admissible as hearing.
- A real-node cell supplies its author birth from its own digest. A synthetic probe mints its own
  node id/incarnation; neither is derived from a path or endpoint. Assertions, sequencing and bounds
  stay unchanged. Reserved types are refused before decoding bodies, unknown types distinctly;
  both have cells. specversion, birth URI, subject grammar, RFC3339 ms time and octet-stream content
  type stand as drafted.

After the publisher-birth prerequisite, Q34 carries exact page lines, the one envelope wire-format
move/reseal, all unchanged e2 behavioral cells, R10's delayed-predecessor cell, byte goldens and full
size ceilings, and the real read-recovery flame. No dual decoder. No envelope code before Q34 ruling.
