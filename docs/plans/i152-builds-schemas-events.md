# i152: Builds add and delete nodes; one schema per entity; one events store

**Source:** Q342 (Luke, 2026-11-02: "A, back it out"), and the design settled with Luke in the
same conversation ("ok i think we're good"), recorded in [INDEX Q342](rulings/INDEX.md).
**Owner:** the lead, in one lane, no agents (Luke, 2026-11-02). **Repo:** this one; Rafka's part
is i150 e6 in rafka-v2.

## Why

The Build was the topology mutation engine and became the executor of everything: stop, start,
connections delete, node recovery, re-adoption, drift and primary moves all ran as attempts of the
current Build (build.md §1 since R-BI1, 2026-10-12; R-BI7 put the mesh-primary's self-heal in a
per-mesh lane). Every later mechanism referees that collision: the in-flight row and attempt claim,
BuildInProgress answered to a repair, the owed recovery (Q291), the hand-back (F6), the drift
guards, the R-D1 exemptions. And a Build restated the whole topology, so a submitter composing it
from a read put back values a node had changed since (Q202).

## The contract

1. **What a Build is.** Every external requested change to the topology that results in the
   addition or a removal of a node is its own Build (Luke's words). Stop, start and connections
   delete change no shape: they are node commands the primary above the node runs, recorded on
   the node record. A repair (delete, then create at the same path.name) is not external: it is
   the primary's own workflow, recorded on the node record. Work on one node is serialized at its
   mesh-primary.
2. **The request is a batch** (Q280; node-rpc-envelope.md "Batch call"), one op, `build.create`:
   - `Create { meshes: [Meshes], nodes: [Nodes] }`: a node naming a mesh not yet live creates it
     with its first node-admin; refused by name: a new mesh with no node-admin, a node naming a
     mesh neither in the request nor live, a path.name already held;
   - `Delete { meshes: [mesh name], node_ids: [node.id] }`: by node.id, never a path.name; a
     node.id already deleted or replaced is refused by name in its own result;
   - `DeleteFabric`.
   The caller states only what it adds or removes, never a node it does not touch. A result per
   node. `build.get` and `build.delete` stay.
3. **The Build record is written once** by the fabric-primary: `build_id`, `change` exactly as it
   came in, `topology` (the whole shape the fabric-primary composed from its records at acceptance,
   each node's type defaults filled in), `source`, `time`. Nothing updates it as the system runs;
   the topology of the moment is the records.
4. **One schema per entity, named for it:** Fabrics, Meshes, Nodes, Builds, Events. A schema is
   the object as stored and the superset of what a Build passes. Each field is settable (a Build
   may state it) or read-only (only RDM sets it; a Build stating one is refused by name).

   | Schema | A Build sets | Only RDM sets |
   |---|---|---|
   | Fabrics | name, provider, topology_storage_provider, types | fabric_id, state (`pending`, `ready`) |
   | Meshes | name | mesh_id, state (`pending`, `ready`, `deleting`, `deleted`) |
   | Nodes | path, name, mesh, type, storage_persistence, executable, tags, config | node_id, incarnation, state, endpoint, is_primary, is_fabric_primary, cert dates |
   | Builds | the request | build_id, topology, source, time |
   | Events | nothing | every field |

   - `path` and `name` are two strings (R-NR1); `PathName` is only the composite lookup index key.
   - A node is referred to by node.id everywhere (an exact birth by node.id and incarnation): every
     command, step, event, record key, operation identity and page reference. `path` and `name`
     appear only as the node's two properties: in a Create's Nodes schema, in the lookup that
     resolves a caller's location to a node.id once, and as the values a repair re-creates the
     node with. The joined `path.name` token leaves the pages (125 uses on 24 pages, s6).
   - `type`: RDM ships `node-admin` and `node`; the application declares its own types on
     Fabrics, each overriding the node's defaults (`storage_persistence`, `executable`). `NodeKind`,
     its Rafka kinds and `StorageMeta::for_kind` go. `placement` (an empty type) goes.
   - Meshes and Fabrics hold one `state`; `MeshRecord.dead`, `MeshRecord.states` and
     `FabricRecord.events` go, the history to Events; `MeshState` takes the page's names.
   - The three primaries stay on the node record: `is_primary` (its type's primary in its mesh; a
     node-admin's is the mesh-primary) and `is_fabric_primary`.
   - Postcard stays the wire; the Rust types are the schema (node-rpc-envelope.md "# Schemas").
5. **Events:** one append-only store, `events.jsonl` (Rafka: an entity), of CloudEvents 1.0
   envelopes with gossip.md §3.4's attributes; `id` a time-ordered UUIDv7; `datacontenttype`
   `application/json`; never gossiped. Two lookups: (build.id, event.id) from `data.build_id` and
   (node.id, event.id) from `subject`; ordered maps in the file store, two declared index slots in
   Rafka. Types: `rdm.build.started`, `rdm.build.dispatched`, `rdm.build.step`, `rdm.build.ended`,
   `rdm.node.investigated` (an investigation is a function; its verdict is an event, never an
   entity), and the mesh and fabric state moves. `on_build_started`, `on_build_ended` and
   `on_node_investigated` are sent from the event's write.
6. **Claims and locks:** `locks/in-flight` and the attempt claim go (one writer of Builds, the
   fabric-primary, which is re-born, never elected); `locks/name` goes (a lookup by the one
   writer); `locks/commit` (the storage copy's commit marker) stays. The fabric name above the
   per-fabric roots has no writer above it: held open (s5).
7. **The "proof" wording goes:** the provider's inspect answer (node.runtime.inspect) is the one
   mechanism; the page sentences that say what is not proof collapse into one line on
   node-runtime-inspect.md; `RuntimeProof`, `StopProof`, `DepartureProof` and the testkit
   `ProofStore` are renamed for what they are; `proof` / `prove` / `proven` join the name check.

## Implementation plan

The lead builds every story in lane 5, one at a time, each through the lane verbs: RED cell first,
the page text and the code in one landing, the fast gate (`post-land-gate.sh`), one flame read for
any story that moves a runtime path, `lane-land.sh`. Release and chaos run once at the tip after s7
(land on fast, run long once). Every story's removals are listed by symbol in its landing and in
the removal debt. The events store lands before the Build record loses its attempt and receipt
lines, so a new mesh-primary always has a source for the Build work its mesh owes in flight
(elections §4.1).

| Story | What lands | Issue | Depends on | Evidence |
|---|---|---|---|---|
| s1 | The page text: every sentence that puts a repair or a node command in a Build, quoted old and new (`rust-distributed-mesh-deploy/reports/q342-packet.md`), for Luke's yes. Applied per story, never ahead of its code. | — | none | Luke's yes |
| s2 | Repairs and node commands leave the Build. node.stop, node.start, node.connections.delete: node commands of the primary above, recorded on the node record. Node recovery and re-adoption: the primary's workflow, recorded on the node record. Fabric recovery acts on the primary watch's verdict and writes no Build record (Q337, rdm#291, folded). Goes: every `AttemptReason` but `Requested` and `Steps` (`Steps` goes in s4); the drift pass; `PrimaryMoved`; the per-mesh lane; the owed recovery; the hand-back; the drift guards; the R-D1 exemptions; repair claims and their in-flight rows; the Build carrier paths of the node commands; `BuildChange::{Stop, Start, DeleteConnections, Recover, Terminate}`. Stays: Q335's "no node-admin left" guard (F7/F5, noted in Q341); Q250's dispatch of each mesh's section by the fabric-primary. | rdm#296 | s1 | stop, start and connections-delete cells by node command; a recovery during a caller's Build runs at once (Q291's case); a hung mesh-primary recovered by the fabric-primary's workflow (rdm#294's case); a flame of each |
| s3 | The events store. `events.jsonl` (the `EventsStorage` trait: append, by event.id, by build.id, by node.id), the CloudEvents envelope, UUIDv7 ids, the two ordered lookups; `rdm.build.*` and `rdm.node.investigated`; the three hooks sent from the write. Each event is its own record with one writer: the fabric-primary for Build-level events, the mesh-primary for its steps, the primary above for an investigation. A new mesh-primary reads the current Build's events for work its mesh owes. | rdm#297 | s2 | the events of one Build and of one node read back in order; a hook sent once per event; a new mesh-primary resumes its mesh's owed step from the events |
| s4 | The request and the record. `BuildChange` = `Create`, `Delete` (by node.id), `DeleteFabric`; the fabric-primary composes the topology at acceptance; the Build record written once; the dispatch recorded as `rdm.build.dispatched`. Goes: `Reconcile`, `RemoveNode`, `RemoveMesh`, `Steps` and `Step`; the attempt and receipt lines of the Build record; the replace as a Build (node-replace.md: node commands, then one Build that adds). Q202 and Q236 amended: a Build still states each node it creates whole, and never states a node it does not touch. Nodes and Meshes schemas land here, `path`/`name` flat in them. | rdm#298 | s3 | a Create of several meshes and nodes; a Delete by node.id; a delete of a replaced node.id refused by name; a node.config.update between the caller's read and the Build survives it |
| s5 | Claims and locks. Goes: `LockKind::InFlight`, `claim_attempt`, `adopt_claim`, `StaleClaim`, the held/won claim fold; `LockKind::Name` and the node name claim, replaced by the fabric-primary's lookup. Stays: `LockKind::Commit`. | rdm#299 | s4 | a second Build refused build-in-progress with no lock; a held path.name refused by lookup |
| s6 | Schemas for the rest. Fabrics with `types` and `state`; Meshes `state` (`dead`, `states` go); `FabricRecord.events` goes; `MeshState` takes the page's names; `type` replaces `NodeKind` (RDM's two types, `node-admin` and `node`; the application's declared); `placement` goes; i150 e1.s2's remaining `PathName` fields flat (D16). The pages' joined `path.name` token goes: each reference becomes node.id, or `path` and `name` where the two properties are meant. A durable-format move, with its commit marker. | rdm#300 | s5 | a declared type's defaults applied at acceptance; an undeclared type refused by name; RDM's two types not overridable |
| s7 | The proof wording. Renames by symbol, the page sentences collapsed into node-runtime-inspect.md, the name check. | rdm#301 | s6 | the name check refuses the words |
| s8 | Rafka (i150 e6, rafka-v2): one pin; the replace cells removed (in e6.s2); deletes look up node.id with `node.get`; Events as an entity with two declared index slots; NodesStorage's two parts. | e6 | s7 | e6's acceptance |

**Decided before their stories (Luke, 2026-11-02):**
- s5, the fabric name: the claim at `<store_dir>/fabrics/<name>` stays, the one name claim, since
  two `fabric.create` on one store root have no fabric-primary above them to look it up ("yes").
- s6, RDM's two types: `node` is the base and `node-admin` the other ("yes node is base,
  node-admin other"). `node` is the plain node that serves the core protocols only (today's
  `NodeKind::RpcNode`); every type extends it: `node-admin` is RDM's, and each type the
  application declares on Fabrics overrides `node`'s defaults. `rpc-node` is not a type name.

**Re-read after s2:** rdm#295 (a reached mesh-primary re-marked every ~30 s and commanded back
through node.mesh.join during a primary move): the repair path it ran through is rewritten by s2;
its cell runs once with spans kept after s2 lands, and the root is fixed if it still happens.

## Review of open work (2026-11-02)

**Issues** (milestone i152 holds s2–s7)

| Issue | Disposition |
|---|---|
| rdm#239 | Closed into s2 (rdm#296): its red was a node.stop refused through `build.create`; under s2 a stop is no Build. 58c826f08's atlas fix stays. |
| rdm#291 (Q337) | Closed into s2 (rdm#296). |
| rdm#294 | Closed into s2's evidence (a hung mesh-primary recovered by the fabric-primary's workflow); the `node_replace` cell goes with s4. |
| rdm#249, #251, #252, #254, #257, #259 | Closed as landed with i150: #257 at fba5b0d03, #259 at 9564dd5a4, #249's removals have no symbol left (`owe_push`, `SentBy::Own`, `Hop::Push`), #251/#252/#254's cells are in the tree, none ignored, and no red tree is recorded for the i150 landing. |
| rdm#295 | Open: re-read after s2. |
| rdm#293 | Open: the collector's retry on .99 (rafka-v2's collector config), not RDM code. |
| rdm#187 | Open: load testing; "load tests before soaks" stands. |

**Rulings superseded by i152 (Q342)**, marked in INDEX with their text intact: Q104 (the drift
pass's hand-back), Q118 (drift attempt receipts), Q211's Steps (the word stands; the Build of steps
goes), Q221's recovery exemption (gate 6 at acceptance stays), Q239 (the next attempt's claim after
an executor's loss), Q253 (when the next claim is taken), Q258 (the mesh-primary runs the next
attempt of a completed Build), Q291 (the owed recovery), Q305 (a later attempt re-applying the
entry's config to a replacement), R-BI1's second bullet, R-BI7's placement in the Build. Amended,
not superseded: Q202 and Q236 (s4). Referenced, not superseded: Q250, Q280, Q335.

**Rulings still owed, outside i152:** Q140 (rafka-v2's two cells), Q262 (docgen), Q268 #2
(take_snapshot), Q271 (`incumbency_of`'s copy), Q273 (the investigation's step order, code to
check against node-investigation.md), Q295 and Q296 (the demo). Q90, Q93, Q101 and Q304 are landed
or cancelled as their rows say; Q276's read is superseded by Q292 as its row says.

## State

s1 in writing (2026-11-02).
