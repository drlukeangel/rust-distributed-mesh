# Node records: node-owned properties and lifecycle records

**Status:** approved design direction; implementation and acceptance are tracked in
[i150](../i150-node-writes-lifecyle-updates.md). This proposal describes the target contract,
not a claim that the complete implementation has landed. The architecture pages change with
the code after approval of their exact old and new text.

Luke, 2026-10-31, with the lead, as ideation ("simplify this whole process down to make it so the
node writes its own [properties], anyone can write, anyone can read; if it can wait it goes through
gossip, if it can't wait it's an op"). Luke, 2026-10-31, after the architect-1 review and its answer: "make the design". This page is the
design, RDM and rafka together, with its red team at the end; the one question it had is decided
below ("Decided"). The architect-1 review of 2026-10-31 (five
findings, two clarifications) is folded into the steps it names, and the Build sentence (Luke's direction, wording by architect 1) is R-NR5's first paragraph. The storage provider is not part of it: every
requirement is stated at the storage traits (`NodesStorage`, `ConnectionsStorage`, storage.md §2), and
holds for the `file` provider and the application's alike.

## Why

A node's own facts take a trip up through two primaries and back to land on its own record, and can still
lose to the Build:

- **Q235.** node-config-update.md:31-39: a node's own map goes up to the mesh-primary its view
  names, owed until answered and retargeted on a primary change; the mesh-primary writes the record,
  stepping its revision, answers the node with the row it already had, and owes the row to the
  fabric-primary, keyed by node and revision, until acknowledged, a successor owing it again from the
  row. A caller's goes the other way down, `ApplyNodeConfig` to a live node (:19-27). The gateway's
  own ports take the first route: three processes and two owed deliveries for a fact one process
  knows.
- **Q305.** A replacement's successor is seeded with its predecessor's config at the claim
  (node-replace.md:19, `row-seeded`), so its row names the dead birth's ports; and a later attempt of
  the same Build re-applies the entry's config over the successor's own update (`accepted.rs`
  `properties_owed`).
- **Q308** (rdm#243). A deleted birth, resumed after its delete, wrote its row at the path; the index
  entry named it, and the atlas row read answered Deleted for a path whose live successor was Ready.
  Fixed by a fold in one more reader. The index entry is the defect.
- **rdm#239.** Acceptance refused `node-not-live` on a path node recovery had just re-created, because
  acceptance decides from a view that had not heard yet, and never from the record.
- **R-OW1 and its machinery.** The mesh-primary is the one writer of its mesh's rows
  (`node_rows.rs:7-9`, `not-mesh-primary`), with held copies, a writer-side fold, owed index moves and
  a revision stepped on every write (`node_rows.rs:296-311`), all enforcing a property the record's key
  gives for nothing once the key is the node.id.
- **`PathName`.** One type parsed five ways, `{ mesh, kind, ordinal, label, old }` (path.rs:91-102),
  serialized as one string, against node-identity-and-state.md:66: "path and name are the node's two
  fields ... both are free text ... never an ordinal; the node's type is its own field, `node_type`,
  never read from the path".

## The rules it rests on

- **Q109.** Storage is persistence only: "storage stores and returns what it is given, and decides
  nothing"; "every reader folds the stored rows the same way, and a row that does not follow the fold
  is ignored by every reader, refused by no storage".
- **Q197.** Nothing starts, stops or does any action through gossip; gossip is the side effect of an
  op, never the reverse; nothing waits for gossip.
- **Q54.** A write is durable before it answers.
- **Q237 A.** A node may read another node's record from the store at runtime; only the reads at
  create, start and join were ruled out, because the mesh-primary syncs the node then.

## Requirements

**R-NR1. Path and name are separate properties with separate meanings.** `path` says where
the node is in the tree; `name` is the node's name. They are stored, passed and read separately.
There is no combined `path.name` identifier, display format or string to split. A name holds no
dot; a path may hold dots. Neither field encodes the node's mesh, type, ordinal or predecessor:
`mesh` and `node_type` remain explicit metadata.

The existing `PathName` Rust type may carry the two fields as a structured argument. That does
not turn them into one value: references and commands retain separate `path` and `name` fields,
and no caller concatenates or parses them. The ordinary claim enforces uniqueness of a name at
its path using both fields, not a concatenated index string.

A node is tied to its immutable `path`. Replacement renames the predecessor's `name` to
`<name>-old` at that same path. The successor claims the same path with the original name.
The Build identifies the predecessor by node ID, never by parsing a name or path.
`node.name.update` carries separate `path` and `name` fields and refuses a different path.
Launches carry `RDM_NODE_PATH` and `RDM_NODE_NAME` separately. Reads filter on `path`, `name`, or
both as separate properties. This supersedes the earlier proposed `.old` path move.
Durable format moved: node and connection records, digest names, JoinNode and
launch inputs retain the two fields separately.

**R-NR2. The node record is keyed by node.id; the lifecycle record by node.id and its writer's
birth; the claim is the only record keyed by the pair.** A node id follows the instance
(node-identity-and-state.md:132), so a record is a birth's record. A lifecycle record of node X is
keyed `(X's node.id, the writing primary's node.id and incarnation)`, so the predecessor's and the
successor's writes of one node are two records, and a predecessor's delayed write changes only its
own; it cannot destroy the successor's value. No node or lifecycle-write counter is introduced (Luke, 2026-10-31:
"is_primary is 0 or 1, not a counter"): `is_primary` and `is_fabric_primary` are flags, and the role
is the election's, which the record follows and never makes (node-primary-update.md:32). A reader
resolves the mesh's current primary as RDM resolves it today, from the election's result it holds
(the recorded primaries, `boot.rs:1188`; the view), never by comparing records, then reads
`(node.id, that primary's birth)`; a node with no record under the current primary is
**unavailable**, never the predecessor's value and never an invented status. The same birth losing
and regaining the role reuses its lifecycle keys. Its takeover writes follow all earlier writes
through the same serialized write path (R-NR6). The takeover cannot complete while an earlier
write's outcome remains unresolved. An expired client wait does not prove that the provider can
no longer commit that write. Election timing supplies no storage-ordering guarantee. The node
record carries no revision. The existing election result retains `Primary.revision`, independently
of the lifecycle flags; readers restore that election result rather than reconstructing its order
from a lifecycle record. Path to
node.id resolves through the claim (`name_claim`, storage.rs:625) and nowhere else; node.id to record
is a point read (`node`, storage.rs:605). The `path.name` index entry (`index_path`, storage.rs:617)
and its reader `node_at` (storage.rs:606-609, "the node whose record was last written at it, a
Deleted one included") are deleted with every caller (about twenty: `atlas.rs:732`, `:877`,
`accepted.rs:1119`, `runner.rs:301`, `:1661`, `node_config_update.rs:44`, `boot.rs:2070`,
`node_rows.rs:1145`). Node-property and lifecycle records and their completion replies carry no
record revision: the latest write at a key is the record, each key has
one writer by construction, and `node.config.updated` carries none. A late write by a deleted birth lands on its own record, which
the claim no longer names, and moves nothing.

The identity model, stated: the property record is keyed by node.id, the lifecycle record by the
subject and writer birth, and the claim by the path/name pair. The pair is never a node-record key and
the withdrawn Q309 shape (`path.name` plus a record revision) is not this proposal's. A stop and a
start keep the node.id and the incarnation (CLAUDE.md: "a node is stopped then started (same node.id
and incarnation)"), so the same two records continue and the node rewrites its ports at the start. A
recovery or a replace is a re-birth: a new node.id, two new records, the pair claimed anew; the
predecessor's records stay at its node.id, Deleted at a recovery, under name `<name>-old` at the unchanged path after a replace. The claim's write is `claim_node_name` as it stands
today (storage.rs:628, `NameHeld` when another node holds the pair; `take_name`, node_rows.rs:1201):
its writers are the attempts, and a mesh runs one attempt at a time whichever admin runs it
(build.md:173, Q291 "nothing overlaps"), so a Build's create and a recovery's create at one pair never
write it together. Whether that primitive sits with Q109's "no insert-and-fail" is a standing
question of today's code (storage.md:109 names the same primitive for a provider change), not one
this proposal opens or closes.

**R-NR3. The node writes properties; the primary writes lifecycle facts; readers use the log.**
Providers add no writer admission policy. The old `not-mesh-primary` refusal, the held copies
(`WrittenRows`, `fold_read`), the owed index moves and the revision stepping go. A node.id has two
records, and each has one writer because only one party knows its facts, which is Q109's premise
("how would you have a conflict in this write") kept without a rule: the **node record**, the node's
own, and the **lifecycle record**, the primary's. Every field of today's row (`NodeRow` and
`BirthFacts`, `rdm-node/src/nodes_storage.rs:82-220`) assigned, with its source, its first write and
what happens to it at a start (same node.id) and at a re-birth (new node.id):

| field | record | written by | source and first write | at a start | at a re-birth |
|---|---|---|---|---|---|
| `path`, `name` | lifecycle | the lifecycle writer, at the claim; `node.name.update` after | the create spec | unchanged | the successor's from its spec; predecessor path unchanged and name renamed to `<name>-old` at replacement |
| `node_id`, `mesh_id`, `fabric_id`, `incarnation_id` | lifecycle | the lifecycle writer, using the launching admin's allocation | minted at the claim and launch | unchanged | new |
| `status`, `declared`, `operations`, `silence`, `data_dir_lost`, `data_dir_unreadable` | lifecycle | the node's mesh-primary (states.md:21-36) | `starting` at the launch | `starting` again on the start command | `starting` on the new record |
| `cert`, `signing_cert` | lifecycle | the mesh-primary as issuer (fabric-certs.md) | at the join | renewed, not reissued | issued anew |
| `is_primary`, `is_fabric_primary` | lifecycle | the lifecycle writer, using the primary-change operation's result | the election; a flag, 0 or 1 (R-NR2) | unchanged | none until elected |
| `kind`, `node_type`, `executable`, `storage_persistence`, `placement`, `topology_storage_provider`, `allocated`, `predecessor`, `renamed_from`, `inherited_by`, `initial_properties` | lifecycle | the lifecycle writer, at the claim or the appropriate operation | explicit entry metadata and the exact claim's immutable launch snapshot; no ownership inferred from path/name | retained unless the operation changes the field | entry-owned fields from the entry at replacement; recovery retains predecessor evidence and mints the new birth's allocation |
| `endpoint_id`, `transport_addr`, `listeners` | node | the node, at its bind | the operating system picks them | rewritten at every start | new |
| `app_types` (`types`, `held`) | node | the node, at its join | its declaration | re-declared at the join | declared anew |
| `config`, `tags`, `applied` | node | the node, through one serialized property writer | entry properties and application recognition at first write; config/tags plus recognition in one durable put for an existing node's Build apply | preserve the durable properties and applied identities; startup does not replay an older environment over them | entry at replacement; predecessor's acknowledged properties and applied identities at recovery |
| owed property obligation | lifecycle | the node's mesh-primary | identified config/tag effect durably accepted while application is deferred (R-NR5) | clear only the exact obligation acknowledged as applied; generic Ready clears none | resolve accepted obligations explicitly; do not report durable acceptance as application |
| `revision` | — | gone | | | |
| `writer` | both | the writer's birth, on every write, for the span | | | |

The **lifecycle writer** is the mesh-primary whose birth is in the record's key.
The launching admin and the primary-change operation supply facts to that writer; they do not
independently put the whole lifecycle record. This includes a node-admin's own fabric-primary flag.
When the writer is the same process, it uses the same serialized write path. The existing
fabric-recovery writer exception remains; it must use its own ownership identity rather than
share an active mesh-primary's lifecycle key. Successive mesh-primaries already write separate
keys (R-NR2). This is the proposed ownership contract, not a claim that concurrent writes were
observed in the current implementation.

The join report still carries the ports and the declared types to the mesh-primary (the join's
check of the assigned listener names, CLAUDE.md `RDM_LISTENERS`); the record of them is the node's.
`node.get` answers the two records as one node, as it answers the row today. A mesh-primary that has not yet learned it was superseded writes under its own birth, a record no
reader resolves to once the election names the successor (R-NR2, R-NR8).

**R-NR4. How a write reaches the primaries.** A primary learns a fact one of three ways, and the
fact picks the way:

1. **Not at all**, when nobody has to act on it. The node writes; whoever needs the record reads it
   at the moment of need (Q237 A). `config`, `tags`, the ports, the connection rows.
2. **Gossip**, when a primary acts on it eventually and one interval is soon enough. The digest
   already carries `node_state` and the budgets; the backbone already carries each mesh to the
   fabric-primary. Nothing is added to gossip, nothing acts on it (Q197): a primary's tick reads the
   digest as node-recovery.md:19 says, and when it decides to act it reads the record.
3. **An op**, when a caller is owed a completion, a lifecycle decision gates on it, or it changes who
   may act. Every one of these is an op already: the join report, `node.started`,
   `ConnectionsDeleting`, the stopped frames, `node.primary.update`, a caller's commands and their
   completions up the stream.

| fact | reaches the mesh-primary by | reaches the fabric-primary by |
|---|---|---|
| `config`, `tags`, ports | nothing; it reads the record | `node.get` and acceptance read the record |
| `types`, `held` | the join op | the backbone |
| join, started, connections-deleted, stopped | the report ops on the command streams | the mesh-primary's status write, and the backbone |
| liveness, load | the digest | the backbone |
| `status`, the silence marks | it wrote them | the backbone for the view; the record when it decides |
| primary changes, claims, in-flight | the ops that make them | the records |
| a caller's `node.config.update` | down to the node, which writes; the completion up the stream | the same stream |
| the node's own `node.config.update` | nothing; a local write | nothing |

Acceptance (rdm#239) reads the pair's current lifecycle record through the claim at the moment it
decides, never a view; it resolves the current primary and its lifecycle record as R-NR2 and R-NR8
require. `node.get` is answered from the records, not a held atlas of node rows;
node-rpc-envelope.md:346's "never from the node store" inverts. The atlas keeps what is the
fabric-primary's own: the Builds, the accepted topology, the in-flight rows.

**R-NR5. Builds, starts and re-births.** Luke's direction, wording by architect 1, 2026-10-31: "A Build applies its entry's properties
to both new and existing nodes. For an existing node, the entry supersedes its current properties
through the appropriate operations. Once applied, later attempts of that same Build do not reapply
them over subsequent updates. A later Build can override them again. This covers config, tags,
executable binding/location, storage policy and placement. Changes requiring replacement still use
replacement."

- **The appropriate operations.** An existing node's Build entry applies config and tags together
  through `ApplyNodeProperties` and the same property writer used by local and caller updates.
  Executable binding/location, storage policy and placement belong to lifecycle state. Apply them
  through their existing operations; only a change whose lifecycle requires replacement creates
  a replacement. Storage policy or placement metadata alone does not imply an immediate re-birth.
- **Applied once.** The node persists the entry's config, tags and
  `PropertyApplication(build_id, semantic entry operation)` in one put. The existing target receipt
  fixes the exact birth; attempts are not part of the application identity. A replay recognizes the
  durable application before changing properties, even if the first reply or later step receipt was
  lost. Step receipts still record workflow and metadata completion. `properties_owed` uses both
  application recognition and the relevant receipts; it does not infer non-application from a missing
  reply. True recovery preserves applied identities with the predecessor's acknowledged properties.
  A later Build has a new `build_id` and can override them again.
- **A stopped node.** A stopped process retains its Node RPC endpoint; it is not an exited or
  OS-paused process. When property application is deferred, the mesh-primary durably records the
  identified obligation and reports durable acceptance separately from application. Startup receives
  obligation A by the existing startup operation. If update B is accepted meanwhile, acknowledgement
  of A clears only A: B remains owed until B is acknowledged as applied. A generic Ready report and
  an environment snapshot are not application evidence. The exact obligation representation and
  startup A/B runtime proof remain acceptance work in i150.e4.
- **The environment hand-off.** `RDM_NODE_PROPERTIES` is one JSON document, the form
  `RDM_DECLARED_SECRETS` already uses so a value holding a separator or nothing survives: the create
  spec's map and tags at a create; the entry's at a replace; the predecessor's current at a recovery
  or a start over an inherited data dir; the identified owed effect when one is selected. An absent
  own record is initialized from that snapshot. An existing durable own record and its applied
  identities survive startup; an older environment never overwrites them. The property writer may
  read its own retained record to preserve that state. Ordinary startup does not need to reconstruct
  another node's lifecycle state: the existing startup operations supply it. The corresponding
  Q227/Q242 wording is included in the coordinated architecture review.
- **Gone.** `row-seeded` and node-replace.md:19's seeding of a successor from the predecessor's row
  (the entry's properties are the successor's, Q305 B), and the node's own update going up (Q235).

**R-NR6. The storage contract**, provider-neutral, at the traits: put a whole record at its key,
durable before it answers (Q54); point read by key; the records of a mesh; the claim. The store
stores what it is given and decides nothing (Q109). `NodesStorage` is rdm-admin's (storage.rs:596)
and a node holds only `ConnectionsStorage` (rdm-node/src/storage.rs:18), so the node record gets a
trait of its own beside `ConnectionsStorage` in rdm-node, bound by the same providers through the
same handle (unit 4). What is no longer called: `index_path`, `node_at`.

Order is the writer's: one issued put per key at a time, with the retained task owning its lock,
backend operation and publication through terminal completion. Caller cancellation cannot release
that lifetime. On every returned outcome, including an error, the issued mutation can no longer
commit later. An error can still mean that an already committed write lost its acknowledgement;
the next operation resolves the exact durable value. An adapter whose append remains pending after
a transport deadline retains that original append until its outcome resolves. It does not retry it
or treat the deadline as absence. Providers do not select primaries or order competing writers.
Different primary births write different lifecycle keys; reuse by the same birth follows R-NR2.
Build replay safety is the atomic application evidence in R-NR5, not merely replaying a whole map.

**R-NR8. Takeover.** Compute the candidate from the existing election, then prepare its lifecycle
state before installing or publishing the role or opening command admission. Freeze the accepted
takeover evidence once; finish the candidate birth's earlier issued writes and persist each required
lifecycle record under that writer birth through the same retained serialized path. Retry uses the
frozen evidence rather than re-reading a mutable predecessor value and promoting it as current.
Only completed preparation exposes the new installed role and its lifecycle flags. An incomplete
preparation exposes no new acting primary. The existing election result and its revision are
persisted independently from those flags. Missing current-writer lifecycle evidence is unavailable,
never a fallback to another writer. The exact stable evidence boundary and interrupted-takeover
proof remain required i150.e3 acceptance work. A read begun before a primary change can still be stale: the
command it produces keeps its exact target and the admission checks every command has at its
execution, as today (build.md:177, workflow input carries the exact birth). This design prevents
the destructive overwrite; it does not make a read and a later action one atomic operation.

**R-NR7. What goes.** `index_path`, `node_at`, `record_at` and its rename guard, the `renamed()`
second lookups (`runner.rs:304`, `node_rows.rs:1321`), `IndexMoves::path`, the Q308 fold in
`Atlas::row`, R-OW1's refusal and copies, the revision field and its stepping (Q235 P1), the route of a
node's own `node.config.update` up through its mesh-primary and the row answered back to it (Q235;
`ApplyNodeConfig` stays as the down command of a caller's or a Build's map), Q305's `claimed` test in
`properties_owed` (the function stays, reading atomic application evidence and workflow receipts,
R-NR5), `row-seeded`, the atlas's held node rows, the `old` flag and
`ordinal`/`label`/`kind` parsing. The hooks stay as Q175 and Q256 left them.

## The record it moves

Each is a page or ruling line this proposal changes, for Luke's yes on the exact text when it is ruled:

| record | today | under this proposal |
|---|---|---|
| node-identity-and-state.md:66, §2.2 | "joined only as the path.name index key"; a reference stores the `path.name` | code references use globally unique `node.id` first; location lookup may index separate `(path, name)` fields |
| node-rpc-envelope.md:346 | `node.get` "by path.name ... never from the node store" | filters on `path` and `name`; answered from the records |
| node-rpc-envelope.md:392-393 | `tags`, `config` "inherited ... from the Deleted row at the claim"; `config` "a node's own goes up through its mesh-primary" | handed in the environment at launch (R-NR5); a node's own is its own write |
| node-config-update.md:11-12, :31-39 | "A node's own goes up"; the own-update sequence | a node's own is a local write; a caller's and a Build's go down to the node |
| node-replace.md:19 | the successor seeded from the predecessor's row | the successor writes its own record from its environment |
| storage.md R14 / R-OW1 | one writer per key, the mesh-primary | node-owned properties; one lifecycle writer per primary-change key (R-NR3) |
| Q235 | the trip up and back | gone; a node's own update completes locally |
| Q235 P1 | the revision stepped by the writer | no revision |
| Q234 | the hydration sync hands the node its own whole row | the topology still; its properties it writes itself |
| Q227, Q242 | no node reads at create, start, join | the own-property writer preserves its retained record; startup operations supply lifecycle/topology context rather than requiring ordinary nodes to reconstruct it |
| Q305 B | the Build writes its entry's config at the claim | kept in effect: the entry's properties are the successor's, handed in its environment; applied to an existing node by the operations, once per Build (R-NR5) |
| node-config-update.md:23 | a stopped node's update completes after the row write and arrives in its hydration sync | durable acceptance records an identified obligation; startup applies it through existing operations and clears only its actual acknowledgement, never generic Ready (R-NR5) |
| Q308 A | `Atlas::row` folds the index read | the index read is gone |
| R-FA1 (fabric-atlas.md) | acceptance from the atlas over the view | acceptance reads the node record through the claim; the atlas keeps Builds, topology, in-flight |
| node-primary-update.md:32, :57, :61; nodes_storage.rs:120 | `is_primary` "at the next revision on the successor's row", `Option<u64>` | `is_primary` a flag on the successor's lifecycle record under its own birth; no revision (R-NR2) |
| CLAUDE.md env table, `RDM_NODE_NAME` | the `path.name` (`mesh1.rpc.2`) | `RDM_NODE_PATH` (`mesh1.rpc`) and `RDM_NODE_NAME` (`2`) |
| node-replace.md:17, :23, :32, :34, :40; node-identity-and-state.md:100, :127, :143 | the predecessor renamed `<name>.old` | the predecessor's path unchanged, its name updated to `<name>-old` |
| node-rpc-envelope.md:388-390; node-name-update.md:5 | `path` immutable, `node.name.update` carries the name only | `node.name.update` validates the unchanged path and changes only name; a different path is refused |
| CLAUDE.md env table | — | `RDM_NODE_PROPERTIES`: the launched node's config and tags, a JSON document written by the launching admin (R-NR5) |
| `node.get [path, name]` of a deleted node | the index entry names the last writer at the pair, a Deleted one included | unchanged in effect: `node.get` is a filter over the mesh's records (storage.rs:610), so a Deleted record still matches its `path` and `name`; only acceptance resolves a pair through the claim, which a delete released |

## RDM: the units, in order

1. **R-NR1**, the pair. The type, the serde, the environment, the wire, the claim key, `node.get`'s
   fields, predecessor name suffix `-old`, immutable-path validation and page text. Durable format moved. Its cell: a create at
   `path = mesh1.rpc, name = north`, renamed to `name = north-old` at the same path, read back using separate path and name filters,
   `[name]` and the pair; a create and a rename with a dot in the name refused by name.
2. **R-NR2 + R-NR7's index part.** Records by node.id, the claim the only pair key, `index_path` and
   `node_at` deleted. Its cells: Q308's (a deleted birth's late write hides nothing) with the fold in
   `Atlas::row` removed, and rdm#239's (acceptance on a path recovery just re-created).
3. **R-NR3 + R-NR8**, the two records and the takeover. The refusal, copies and revision come out
   together with their replacement: the lifecycle key by writer birth and the takeover writes. R-OW1's refusal, copies and revision removed; the node record and the
   lifecycle record at one node.id, `node.get` answering them as one node.
   Its cells: a node's own write and its primary's write at one node.id, read back as one node by
   `node.get [id]`, neither write refused; and a predecessor paused mid-write, a successor elected and
   taken over, the predecessor resumed and its write landed, every reader answering the successor's
   value, the predecessor's record standing under its own birth. Also delay an old put while that
   same birth becomes reachable and regains the role, then release the put: the takeover write must
   finish afterward and remain the value read. Include expiry of the old put's client wait while
   its provider outcome remains unresolved; takeover must not bypass it.
4. **R-NR5 + R-NR4's node part.** The node-side trait for the node record (R-NR6), bound beside
   `ConnectionsStorage`; the node writes its own record at start and on its own `node.config.update`;
   `properties_owed` on atomic application evidence and workflow receipts; identified stopped-node
   obligations on the lifecycle record; the launching admin hands initial config and tags in the
   environment; a caller's update goes down to the node; `row-seeded` is removed. Its cells: Q305's two
   (a successor's own ports; a later attempt does not overwrite the node's map) and Q235's trip up and back
   gone (one span, no fabric-primary call).
5. **R-NR4's primary part.** Acceptance and `node.get` read the records; the atlas's held node rows
   removed. Its cell: rdm#239 again at the fabric-primary with no gossip of the new birth yet.

Each unit lands on a fast GREEN with its page text; the release suite once at the tip.

## Rafka

Rafka pins RDM and consumes the pair and the node's own write; nothing rafka-specific moves into
RDM.

- **The node's own config.** `rafka-node-base` puts the gateway's ports on its own row through RDM's
  `node.config.update` and waits for `node.config.updated` carrying the revision (`rdm_node.rs:528-532`,
  `rdm_topology.rs:43`). It becomes the node's own write through its RDM handle, the node-side trait of R-NR6; the
  completion carries no revision. Nothing in rafka waits on a primary for its own ports.
- **References use node.id first.** Code references use globally unique `node.id`. Where a location
  lookup is required, carry `path` and `name` as separate fields; their composite index is valid because
  names are unique within a path. No reference joins or splits these fields. Rafka's entity framework already carries them apart (`identity.rs:252`, `name: String,
  path: EntityPath`), so its own records do not move; the RDM-facing references do.
- **`node.get [id = x]`** (`rdm_deletions.rs:54`, Q167) reads the deleted node's record as today; the
  answer carries `path` and `name`.
- **Hooks.** None added, none changed. A node's own properties are known to the application that
  wrote them; lifecycle it learns as today.
- **The environment.** Rafka's launch wiring reads `RDM_NODE_PATH` beside `RDM_NODE_NAME`.

## Proofs

Flames first (CLAUDE.md: no change to the mesh without a Jaeger flame): Q305's two traces on laneG8
(3281ce5542afc8de74aff606b410ef6b), Q308's S2 soak at 187fe558a (its 40th repetition), rdm#239's
authority-process proof at 27201508f step 39. Each unit's cell is RED on main at the cited line and
GREEN on the unit. The release suite and the soaks run once at the tip, not per unit.

## Red team

Each attack, and where the requirements answer it. An attack the requirements do not answer is in
"Open", with arms.

1. **An old mesh-primary writes after the primary moved.** Paused past the staleness floor, a
   successor elected and writing node X's lifecycle record, the old one resumes and writes a stale
   status; with no revision and a provider that keeps only the latest value, a shared key would lose
   the good value, not hide it. Answered by R-NR2 and R-NR8: the two write two records, keyed by each
   writer's birth, and a reader resolves the current primary from the election first. The pre-check (a
   writer reads "I am primary", pauses, writes after the change) and the per-node conditional put
   (it checks a field that changes only after the takeover write) were both rejected for leaving
   that window; see "Decided".
2. **A deleted node keeps writing.** A birth resumed after its delete writes its own record for as
   long as it runs. Answered by R-NR2 and R-NR3: its record is its own and the claim names the successor;
   a current-pair lookup does not select it. `node.get [id]` of it shows a Deleted status from its primary and whatever
   it wrote, which is what a deleted node's record is. No fold is needed for it, which is why the Q308
   fold goes.
3. **Two creates at one pair.** A Build's create and a recovery's create at the same pair, from two
   admins (rdm#238: the page has the mesh-primary open the recovery on its own rows with no call to
   the fabric-primary). Answered by R-NR2: both are attempts of the one Build, a mesh runs one attempt
   at a time whichever admin runs it (build.md:173, Q291), and the claim is `claim_node_name` as
   today, including the existing insert-and-fail claim primitive. Attempts serialize execution;
   the claim still arbitrates the pair at persistence. If a recovery stops
   being an attempt of the Build (the direction of the 2026-10-31 thread on node-recovery.md:22),
   the claim's serialization needs its own answer, and it is not in this proposal.
4. **Anyone can write means a node can write another node's record.** It can today: the store
   refuses nothing (Q109) and every node opens the fabric's store root for its connection rows. The
   surface does not move, and the proposal adds no reader rule for it. One span when a lifecycle
   write's writer is not the node's primary, or a node record's writer is not the node, makes a stray
   write visible in a soak before any rule is written for it (Q197's rule for machinery: telemetry
   first).
5. **Reads at acceptance and `node.get` cost a store read each.** Q227's reason was the whole-file
   fold on every open of one provider. A point read by key at the moment of a decision is the read
   Q237 A allows, and acceptance is one pair per command. How a provider answers a point read is the
   provider's (R-NR6); the proposal adds no read on any hot path: gossip's tick reads no record until
   it decides to act.
6. **A caller's config update and a re-birth.** A replacement intentionally uses its Build entry's
   whole config and tags. True recovery instead hands the predecessor's acknowledged properties
   and applied identities to the new birth. Neither replays the predecessor's bound ports: the new
   process publishes its actual bound metadata. An accepted but unapplied obligation remains a
   separate obligation until its disposition is resolved; it is not silently called applied.
7. **The node writes before it is anything.** A node's first write lands before its join is
   accepted; a reader sees a node record newer than a lifecycle record that still says `starting`
   (states.md:21: the mesh-primary writes `starting` at a create's launch, so the lifecycle record
   is there first). Answered by R-NR3's table and R-NR4: status is the primary's and moves with the
   join's acceptance; `starting` beside a node record is a node that has not joined, which is what
   `node.get` answers today. No reader acts on a node record alone (Q197: nothing acts on an observation).
8. **The split environment breaks every launcher.** `RDM_NODE_NAME` is written by node-admin for
   every node it launches and read by the adoption launch (Q156). Both sides change in unit 1, the
   durable format moved; an admin given one without the other refuses its start by name, as a
   non-canonical id does today.
9. **Separate path and name must never be confused by concatenation.** They are stored and
   passed independently; no display or parsing contract joins them. Name validation rejects dots.
   Replacement keeps `path = mesh1.rpc` and renames `name = north` to `name = north-old`.
   The ordinary composite claim uses both fields without changing the path.
10. **Two records read as one node.** `node.get` joins a node record and a lifecycle record written
    by two parties at two times; a reader can see a new node record beside an old lifecycle record.
    Answered by R-NR3 and R-NR4: each record is true of its own facts at its own write, and nothing
    in RDM reads the two together to decide anything (acceptance reads the lifecycle record;
    `node.get` answers both as they stand). The row today is no better: its config and its status
    are written by different ops at different times under one key.
11. **Q109 and insert-and-fail.** The requirements add no check to any storage method and keep the
    claim's write as it is; the rejected conditional put was the one arm that would have added one. That `claim_node_name` is an insert-and-fail against Q109's "no
    insert-and-fail either" is today's code, named in R-NR2 and left to its own question.

12. **A stopped node's update has nowhere to go** (architect-1, finding 3). The node cannot receive
    or write, and its launch environment is a snapshot. Answered by R-NR5: the mesh-primary writes
    identified owed effect on the lifecycle record, with durable acceptance separate from application.
    Startup acknowledgement clears only that effect. A newer obligation B remains owed when the
    startup snapshot and acknowledgement concern A. This requires the i150.e4 startup-race proof.
13. **Two writes from one process complete out of order** (architect-1, finding 4). Answered by
    R-NR6: a retained task serializes each issued put through terminal completion, including after
    cancellation or a transport deadline. A returned error may hide a committed value but cannot
    leave the same mutation pending. R-NR5 supplies durable Build duplicate recognition; replaying
    an old whole map alone would overwrite a later update and is not the proof.
14. **The two records do not cover the row** (architect-1, finding 5). Answered by R-NR3's table:
    every field of `NodeRow` and `BirthFacts` with its record, writer, source, first write, and what
    happens at a start and at a re-birth; the executable, storage, placement and allocation are the
    lifecycle record's, written through the lifecycle writer from the entry by the appropriate
    operations; only changes requiring replacement cause replacement.
15. **A Build's entry must win over a node's current properties** (architect-1, finding 2; the pasted
    Build sentence). Answered by R-NR5: the entry is applied to new and existing nodes by the operations,
    once per Build using atomic property application recognition, with receipts recording workflow
    completion. Loss of the first reply does not make the effect owed again; a later Build applies again.

## Decided

**The superseded mesh-primary's late lifecycle write.** Decided X by the lead with architect 1,
2026-10-31, Luke: "make the design". Lifecycle records are separate per writing primary's birth (R-NR2,
R-NR8). The ceiling that forced it: an overwritable shared value, a stale writer no one refuses, and
no ordering or atomic check cannot be correct together; one had to change, and separating the writes
is the one that adds no revision to ordinary writes and no conditional operation to any store. The
cost, stated plainly: lifecycle records keep an ownership identity, their writer's birth, already a
field on every write today; no number is minted (Luke: "is_primary is 0 or 1, not a counter"), and
the same birth, primary again later, reuses its key only under the serialized-write ordering in R-NR2, never an assumption about resume or election timing (Luke approved, 2026-10-31). Rejected: A, a writer's check before it writes (it can read "I am
primary", pause and write after the change; the takeover write can be overwritten after it lands;
a span records the destruction and prevents nothing); B, a conditional put on a field of the node's
record (it checks a value that changes only after the takeover write reaches that node, so it leaves
the window it was for); C, a revision on lifecycle writes (Luke: no).

## Startup ordering correction

After the hook, satisfy required Build facts first. Fetch topology once, at the end of startup,
and take current Rafka-time from that same reply before reporting started. Do not fetch topology
before the Build check or fetch it again solely for time. This is the required target ordering;
the existing shared startup implementation still needs the corresponding change and proof in i150.
