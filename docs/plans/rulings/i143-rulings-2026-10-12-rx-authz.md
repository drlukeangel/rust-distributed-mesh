# R-RX1 — no receiver authorization inside the mesh: audit and remove (Luke 2026-10-12)

**Ruling (Luke 2026-10-12): "you continue to ask about a receiver authorization; this means you built it into the system. Do an audit and
remove it."**

Two asks are answered by it:
- R-BR1: a backbone receiver refusing a not-seated publisher, with or without a repair.
- R-UC1a: the Node RPC fence target blocking a start's join back.

Neither is a design question. Both are receiver-side checks on who sent a frame or call, and R-SV1 ("we are all homies") and
node-rpc.md §14 ("nothing inside the mesh checks who is calling") already rule those out.

- Every place a receiver (gossip receiver, Node RPC server or door, status/declaration/join/start/stop handler) refuses, ignores or fences
  an input because of WHO sent it is removed.
- Data validity stays: decode and size bounds, version ordering of the same source, duplicate receipt keys, backward transitions of the
  same subject.
- A receiver saying it is not the one to answer (a direction refusal) is listed separately, and stays only where it keys on the receiver,
  never on the sender.
- The audit (RDM main, read-only) lists each sender gate with file:line, its cells, the doc line that encodes it, and which data-validity
  rule covers the hazard it stood in for. The removal units follow from it.

# R-PB1 — no seat moves before the initial fabric is ready-for-traffic (Luke 2026-10-12)

**Ruling (Luke 2026-10-12, asked whether the fabric-primary seat can move before the fabric's first state-commit; the window covers both
pending and state-sync): "no seat move before RFT".**
- No fabric-primary or mesh-primary seat moves while the initial fabric is pending, state-sync or state-commit.
- A Build or update that would move a seat before ready-for-traffic is refused by name, then admitted as usual once the fabric is
  ready-for-traffic.
- Why: until state-commit the fabric's records (the Fabric row with `Fabric.build_id`, each mesh's Mesh row and members' Node rows) live
  only in their holders' scratchpads (states.md: "Before state-commit, RDM holds its rows in the scratchpad"). A seat that moved before
  then left the successor unable to read them:
  - gate red 980d26049ed3c613ecb01b061535b554: the re-submit was refused `not-accepted`;
  - s56 lane p5c: a second Node row, then a 404.
- Docs of record to carry it: `lifecycles/fabric-primary-update.md`, `build.md` (seat-moving admission), `lifecycles/states.md` (the
  initial fabric's states).

# R-PC1 — mesh.delete and caller workflows are refused before the initial fabric is ready-for-traffic (A) (Luke 2026-10-12)

**Ruling (Luke 2026-10-12: "a").**
- Until the initial fabric is ready-for-traffic, the following are refused by name:
  - mesh.delete;
  - every node workflow a caller submits that writes a row: node.stop, node.start, node.restart, node.delete, node.update, and the
    replace recipe.
- The initial accepted Build, which creates the fabric's nodes and brings it to ready-for-traffic, runs as usual.
- Once the fabric is ready-for-traffic these are admitted as usual.
- Why: the same window as R-PB1. Before state-commit a row lives only in its holder's scratchpad. A mesh.delete started then wrote the
  mesh's Deleting row only to its primary's scratchpad. When that primary was lost, the surviving admin published the mesh as pending,
  backward from deleting (s49 lane).
- One admission rule covers R-PB1 and R-PC1: nothing that moves a seat or writes caller state runs before the initial fabric is
  ready-for-traffic.

# R-BI1 — a node joining or leaving is a topology change and mints its own Build; workflows that leave the topology unchanged are attempts of the accepted Build (Luke 2026-10-12)

**Ruling (Luke 2026-10-12, on build.md:156 "every workflow gets its own build_id" vs :169/:279 "never mint a Build for restart, authority
movement or drift against unchanged topology": "a node leaving is a topology change").**
- Its own Build (its own build_id; `Fabric.build_id` moves when it is accepted):
  - node.create;
  - node.delete (a node leaving);
  - the replace recipe, which adds `<name>.new` and removes `<name>.old` (node-replace.md: "submitted as one Build");
  - mesh.delete;
  - fabric.delete.
- An attempt of the Build that `Fabric.build_id` names, never a new Build: node.stop, node.start, node.restart, and node recovery (the same
  path.name put back over the accepted topology), as build.md:169 and :279 state.
- Code today (RDM main): build_op.rs:112-129 routes the `.old` delete and the replace recipe through http.rs `open_with` as attempts of
  the accepted Build, and s60's last-admin node.delete fix (4a1f49f9) does the same. Each of these is a topology change and mints its
  own Build.
- Doc: build.md:156 lists the topology-changing workflows only, and :169/:279 stand.

# Direction (Luke 2026-10-12): the Build is its own entity; the Fabric row never points at the newest Build

- "any change to the system is a build"; "build is its own entity outside of fabric".
- "fabric should not create a link to the build, or if anything it can link to the build that created it, but never to the newest build;
  that would require continuous updates."
- Open, asked as R-BI3: how a node finds the current Build without that pointer.
- Reverted on this direction: s42 part 3 item 1, the fabric-primary writing every learned pointer to fabric/.

# Direction (Luke 2026-10-12): stopped and deleted are the only out-of-traffic states; there is no leaving and no departed

- "leaving and departed are not real states, they are the same state: deleted."
- "you started with leaving, so the assumption being you wanted the node to not be in the system but maintained, which means stopped."
- "in the system, not receiving traffic, not gossiping, not moving forward, is stopped."
- So:
  - a node still in the system with its identity, but out of traffic, silent on gossip and not advancing, is stopped (or stopping);
  - a node no longer in the system is deleted;
  - the code's Leaving status maps to stopping/stopped;
  - the departure book, `departed`, and `departed_retention` map to deleted.
- Lands in: the status cut (#3083), the renames (#3106), and the docs (states.md "departed" prose; gossip.md `departed` field and
  retention).

# R-BI3 — the known state is the newest completed Build, found by reading the Builds newest first; the Fabric row carries no pointer (Luke 2026-10-12)

**Ruling (Luke 2026-10-12): "it reads the builds till it finds the last committed/completed build id and then uses that as known state,
because other builds could have occurred and failed or be in progress"; on "in which case it should try and resume that?" (answered
from build.md:365, :559).**
- Each accepted Build is its own entity in the builds store, written once.
- A reader (the fabric-primary accepting a change, a mesh-primary recovering a member, a successor fabric-primary) reads the Builds
  last in, first out. The current Build is the first one that is in progress or completed (Luke: "if we're LIFO it would be
  in-progress || completed"):
  - in progress: it is the Build being driven, and it is resumed. The successor claims the next attempt after the executor's departure
    proof, and its completed steps are replayed, never re-run;
  - completed: it is the known state;
  - failed Builds are skipped. A failed Build is never current, and nothing resumes it on its own; a caller resubmits it.
- The Fabric row carries no pointer to the newest Build. At most it names the Build that created it.

# R-RX2 — only the mesh-primary publishes a mesh to the backbone: there is no second publisher to order, so the gossip sender gates go with no replacement (Luke 2026-10-12)

**Ruling (Luke 2026-10-12, asked how a receiver chooses between two publishers' Members snapshots of one mesh once the sender gates go:
"that's not possible, only MP publishes to the backbone").**
- One mesh has one publisher on the backbone: its mesh-primary. Receivers take its Members and MembersDelta as they arrive.
- No cross-publisher ordering rule and no seat-epoch field are added.
- The audit's A1 gates are removed outright: `View::predecessor` / `refuse_predecessor`, `Fence.publisher_left`, the unseated arm of
  `refuses_status_event`, and `departed_publisher`.
- Version ordering within the publisher stays (data validity).

# R-RX3 — a gossip message received out of order, with an earlier version than the receiver holds for its source, is ignored, once, in the gossip framework (Luke 2026-10-12)

**Ruling (Luke 2026-10-12): the out-of-sync case "should be a gossip framework level change, not a node level implementation change. It
should read: a message received out of order with a previous, earlier version is ignored".**
- One check in the gossip receive path, for every frame kind. A frame whose order key is earlier than the newest the receiver holds for
  the same source is ignored (Luke's words: "a previous, earlier version"). An equal key passes, because every chunk of one publication
  shares its key. Frames with no order key (MeshStatus, FabricStatus, SeatedTogether, NewFabricPrimary, NodeDeleted, round hooks) are
  taken as they arrive.
- Order keys already on the wire, so no new field:
  - Members and MembersDelta: `published_at_rafka_ms` then `topology_version`, per mesh;
  - Digest: `digest_seq`, per birth;
  - lifecycle facts: `topology_version`.
- Handler-level ordering and every sender-identity gate (R-RX1 audit A1) are removed in the same change.

# R-DG1 — degraded is set on both the mesh and the fabric whenever a mesh is given a new node-admin by the fabric-primary (Luke 2026-10-12)

**Ruling (Luke 2026-10-12, asked whether the fully crashed arm also sets FabricStatus degraded; recommendation A: yes, as at a
carrier-edge-lost verdict: "both mesh and fabric").**
- When the fabric-primary creates a node-admin into a mesh that lost its node-admins, both the mesh and the fabric carry the degraded health
  marker, on either arm: the carrier-edge-lost verdict and the fully crashed arm on exact provider exit proof.
- The new mesh-primary's report clears both.
- Degraded stays a health marker, not a lifecycle status, on the mesh as on the fabric (states.md).
- New: the mesh carries the marker. Today only FabricStatus does (states.md:11).
- Code: the investigation arms (PRD row e12.s56). Docs: states.md mesh and fabric lines, mesh-investigation.md steps 5-6.

# R-TG2 — tags are the node's own property, with node.tags.get / node.tags.create / node.tags.delete, mirroring node.connections (Luke 2026-10-12)

**Ruling (Luke 2026-10-12): "as long as it can be set by the calling app I am fine with it, it should be its own property then with get
and set"; "node.tags.get, like connections, it should mimic this"; "in our terminology it should be node.tags.create; set is a programming
word, not the CRUD nomenclature we have adopted".**
- A node's tags are its own property, set and read by the calling app:
  - `node.tags.get` answers the node's labels;
  - `node.tags.create { label | [labels] }` adds one label or a list (Luke 2026-10-12: "node.tags.create { label, [labels] }"), one
    keyed row per (label, node id) in the tags index, a blind put each;
  - `node.tags.delete { All | [labels] }` removes all or the named labels, tombstoning their rows (the same scope shape as
    `node.connections.delete`).
- `node.update` no longer carries tags.
- `node.get` with a tag filter stays, read through the tags index one label at a time.
- Connections take the same shape (Luke 2026-10-12: "connections should be the same way"): `node.connections.delete { All | connection
  | [connections] }`. The wire keeps its list, one id being a list of one, so only the typed call changes.
- Uniform shape (Luke 2026-10-12: "a list of one is good, both are correct, just keep it uniform"): every such call takes one item or a
  list, and a single item is the same as a list of one.
- No duplicates (Luke: "it should check if it exists already and ignore if it does, don't insert duplicates"): a label is its own keyed
  row (label, node id), so creating an existing label writes the same key and changes nothing. It is idempotent by construction, never a
  read-then-write. The reply names the labels that were already present (AlreadyApplied for those).
- R-TG2a, label grammar (Luke 2026-10-12: "I would go so far as to make it url safe, but definitely don't mess up the system safe"): a
  label is 1-64 characters, at most 32 per node, made only of URL-unreserved characters without uppercase: lowercase letters, digits,
  `-`, `_`, `.`, `~`.
  - No leading `.`, so no label is `.`, `..` or a hidden file name.
  - No uppercase, so no two labels collide on a case-insensitive filesystem.
  - No comma, since the app's comma-delimited list splits on it.
  - A label outside the grammar is refused by name, with nothing written.

# R-TG3 — any entity can have tags, in one shape: <entity>.tags.get / create / delete (Luke 2026-10-12: "we agreed to follow connections"; "any entity can have tags")

- R-TG2 applies to every entity, not only nodes: `<entity>.tags.get`, `<entity>.tags.create { label | [labels] }` and
  `<entity>.tags.delete { All | [labels] }`. They use the R-TG2a grammar and write no duplicates (a keyed row per (label, entity id) in that
  entity store's tags index).
- A mesh's tags are its own property: `mesh.tags.get`, `mesh.tags.create { label | [labels] }` and `mesh.tags.delete { All | [labels] }`.
  They take one item or a list, use the R-TG2a grammar, and write no duplicates (a keyed row per label).
- Mesh config no longer holds tags. Doc lines: node-rpc-envelope.md:247 (the mesh.config row) and node-identity-and-state.md:363.

# R-RX4 — nobody publishes a mesh on its behalf: a mesh with no primary follows mesh investigation, and its new mesh-primary re-gossips once established (Luke 2026-10-12)

**Ruling (Luke 2026-10-12, on the claim "a mesh with no primary still gets published, by the fabric-primary on its behalf": "no, that's
not true; it follows mesh-investigation.md, and the MP re-gossips once established").**
- Only a mesh's own mesh-primary publishes that mesh on the backbone. The fabric-primary never publishes a mesh as its acting primary.
- A mesh with no primary goes unheard. Mesh investigation gives it a node-admin (carrier-edge-lost, or the fully crashed arm on exact
  provider exit proof), and that new mesh-primary publishes once it is established.
- Code that contradicts this goes:
  - rafka-mesh-transport membership.rs `via-acting-primary` / `acting_full` (~3818-3846);
  - its cell `tests/acting_aggregate.rs`;
  - the acting assertions in `mesh_lifecycle__fabric_primary_handover.rs` (~414).
- Docs that contradict this: gossip.md ~:118 and the Members author column ~:154 ("or the fabric-primary as acting primary of a mesh with no
  primary").
- Unchanged: R-MD1's durable row write in mesh.delete (the fabric-primary writes the mesh row Dead only when no primary survived;
  mesh-delete.md:52, states.md:37). That is a store write in a workflow, not a gossip publication.

# R-BI4 — an attempt lost with its executor leaves the Build in progress; only a named terminal failure makes a Build failed (B) (Luke 2026-10-12)

**Ruling (Luke 2026-10-12: "b").**
- A Build whose attempt ended because its executor was lost (no terminal receipt, executor departure proven) is still in progress. The
  LIFO read (R-BI3) returns it as the current Build, and the next attempt resumes from its receipts (mesh-delete.md "Resume after the
  primary's loss"; R11).
- Only an attempt that ended with a named terminal failure (Failed { step, reason }) makes the Build failed. A failed Build is skipped by
  the LIFO read, and nothing resumes it on its own; a caller resubmits it.

# Direction (Luke 2026-10-12): a node's path is free text; path and name are two fields; node_type is the node's own field

- "path is a free text field, not a declared value. We create them, so we create `<mesh>.<kind>.<name>`, NOT ordinal."
- "path.name is not a field; it's the path and the name field." `path.name` is only the key of the path.name index.
- "type will be a first class field set by the caller?" / "yes, immutable is good." The node's type is `node_type` (Rafka
  `crates/rafka-entity-topology/src/record.rs:21` `Node.node_type`). The caller sets it at node.create. node.update refuses a change to
  it by name, and replace and recovery copy it.
- Nothing is derived from a path segment: not a kind, an ordinal or a role. Grouping beyond the type is tags (R-TG2, R-TG3).
- Code: RDM `PathName { mesh, kind: NodeKind, ordinal: u32, old, new }` (rafka-mesh-entity/src/path.rs:71) is replaced (PRD block
  e12.s41). Docs that contradict this: node-identity-and-state.md :65 and :87-89 ("name the ordinal").

# R-WF1 — the Build-topic message carries no Build id: `WireFabric.build_id` is removed (Luke 2026-10-12, "q2 yes remove it")

- After R-BI3 no receiver reads the field. The s58 branch filled it from `current_build()` (`wire.rs` ~686 `WireFabric { fabric_id, name,
  build_id }`, inside `WireMessage.fabric`). It is removed from the frame.
- The frame is positional (postcard), so the landing carries `durable format moved` and its Jaeger flame from a live mesh (a gossip frame
  change). It lands with the R-BI3 code (PRD block e12.s58).

# R-UC1a — answered by the "we're all homies" canon: no receiver refuses a Node RPC call for the node id it names (Luke 2026-10-12)

**Luke 2026-10-12:** "I have answered this question already as well; you're building authorization into the framework. It goes back to the
we're all homies canon" (R-SV1; R-RX1; feedback 2026-10-09 "we're all homies here, you can trust everybody").
- The receiver-side target check (node-rpc-envelope.md §0: a `target_node_id` that is not this process's minted id resets `425
  STALE_TARGET`; rafka-node-rpc `server.rs` `check_fence` ~494) is a receiver deciding whether a call was meant for it. It goes, with no
  replacement. The R-RX1 audit filed it under B (receiver role, keep); that was wrong.
- A stopped node told to start joins back over the endpoint and path of the call that commanded it (node-start.md step 2), with no record
  of the commander and no node id to know; the admin that answers says who it is in the join answer.
- The code that joins the mesh-primary of the node's old view instead of the commander (the e12.s56 gate red, trace
  c36e7eb6dec804801cefa81a828edf29) is the defect, PRD block e12.s42.
- Docs: envelope §0's 425 row and node-rpc.md :480/:595 (RejectedStale as a receiver answer) change on Luke's yes to the exact text.

# R-RX5 — a receiver that is not the writer of what a call asks answers "not me, ask X"; that stays (A) (Luke 2026-10-12)

**Cross-reference (Luke 2026-10-16, R24 / i144 Q11):** R-RX5 licenses `ReceiverNotPrimary` only where the receiver is not the writer of what the call asks. It never licenses it for a request the receiver has no handler for: that is `RequestNotHandled { request }`, decided by dispatch alone, never by node kind, seat or sender (R24).

**Luke 2026-10-12:** "yes, that status report could come from the fabric-primary if the MP is not responding, so 100%" / "a".
- Kept: `ReceiverNotPrimary { needed: mesh-primary of X | fabric-primary | ... }` where a status declaration or row write reaches a node that
  is not its writer under the writer ladder (R14, R-OW1) and the mesh-primary's sole authorship of status. The receiver checks its own
  role, never who called; the answer names who is needed, and the caller (a member, or the fabric-primary when the mesh-primary is not
  responding) re-routes. RDM main: status_rpc.rs :389, :409, :437, :449, :756; fabric_rounds.rs ~882-948; node_config.rs:109;
  node_update.rs:125; node_self.rs.
- Removed with the 425 target check (R-UC1a): `ReceiverNotPrimary { needed: "the subject node itself" }` (status_rpc.rs :423, :641, :805),
  a downward call refused because it names another node; the same class as the address check.

# R-MT1 — node.primary.update: one primitive that moves the primary of any node type's cohort, commanded downward and gossiped by the current primary (Luke 2026-10-12)

**Luke 2026-10-12:** "a node-admin in the mesh shouldn't issue a command to the mesh-primary"; "only a FP should issue commands to a MP"; "the
MP determines who is the next MP and it should issue a follow-on command to do the transfer"; "this is the primitive for any type:
node.primary.update"; "NA1 > node.primary.update > NA2 ---> NA2.update(is_primary = true) > node.primary.updated --> NA1 gossips MP updated
NA2 ... same for NA as it is for RPC node"; "this same primitive should be called on MP stop or delete. Caveat: delete is skipped if it's the
only node on the mesh. Additionally it should be rejected if it's the only NA in the mesh."
- One primitive, `node.primary.update`, for the primary of any (mesh, node type) cohort: node-admins and RPC nodes alike.
- Sequence: the current primary NA1, commanded from above (a mesh-primary by the fabric-primary), computes its successor NA2 in its cohort; it
  gossips that the primary is updating to NA2; it calls `node.primary.update` on NA2; NA2 updates itself (`is_primary = true`) and answers
  `node.primary.updated`; NA1 gossips that the primary updated to NA2. Commands only go down: no node of a cohort commands its primary.
- Three branches: the happy path; rejected; no other node of the type to take it.
- It runs on every stop and delete of a primary (restart and the replace recipe compose them; a seat holder's `node.connections.delete
  { scope: All }` needs the seat moved first, states.md "Leadership"). The delete of a mesh's only node skips it; it is refused when the
  primary is the only node of its type in the mesh.
- The fabric-primary is its own property with its own op, `node.fabric-primary.update`, run by the same logic (Luke 2026-10-12: "it's a
  separate property, same logic, just node.fabric-primary.update"). Only the successor math differs: elections §6 for the fabric seat,
  the cohort election (§4.1) for a cohort primary.
- Replaces R-EL1a's direction (the executor N sending TakeSeats to the holder P).
- Docs on Luke's yes to the exact text; code: PRD block e12.s23.

# R-MT1 Q3 — the mesh-primary commands the primary updates of its mesh's other node types (A) (Luke 2026-10-12, "MP", "a")

For a cohort that is not `node-admin`, the mesh-primary of that mesh commands the holder's `node.primary.update`, writes the cohort's seat row as the mesh's one row writer (R-OW1, R14), and announces "primary updating" / "primary updated" as the sole author of member status. Commands go one level down: the fabric-primary commands mesh-primaries; a mesh-primary commands its own mesh's nodes. Luke's refinement in the same exchange: a command for a mesh's node may originate at the fabric-primary "for some reason", but it reaches the node as a proxy command through its mesh-primary (fabric-primary → mesh-primary → node); the mesh-primary is the owner of its nodes, always. The fabric-primary never commands a mesh's node directly. Proposal: `docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md` C1 roles and Q3.

# The node.primary.update proposal is approved (Luke 2026-10-12: "yes")

`docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md` (as of 7685ccb5b9) is the design of record for node_type, leadership at
the node_type level, node.primary.update and node.fabric-primary.update; Luke's yes is the yes to each doc edit it lists (Parts A2, B2, C3).
Held out at Luke's request while he works it: how the commands-go-down canon (fabric-authority.md:66, "the fabric-primary never acts on a
mesh's nodes in its place") reconciles with R-FS1 step 2, states.md's "the fabric-primary for a mesh with no primary" rows, R-MD1's Dead
write and mesh-delete.md step 7's deleted-mesh aggregate.

# R-NP1 — node.update goes; every node property has its own command; a rename may take any name (A) (Luke 2026-10-12)

Proposal `docs/plans/proposal/node-properties-own-commands.md`, Luke's yes 2026-10-12 to the proposal and its §5 doc edits; §9 = A ("you can rename"): a rename may take any name within the grammar, unique within its path (R-MN1), under the precondition (the node admits no connection, R-CR1) and the insert-and-fail claim; `.old` and `.new` are the replace recipe's conventions, not a validator's list. `node.name.update`, `node.tags.create/delete`, `node.config.update`; `node.update`, `NodeMeta`, the Meta-group table and `RejectedMetaFieldIsRdmOwned` deleted; `rafka_mesh_entity::meta::NodeMeta` → `PathConfig` (defaults set at a path that nodes created there inherit; `node_config` is the app's own bag). OTel debug rides `CallContext` as baggage, never a command body or the record; R-BI8 superseded. Build on PRD e12.s41, behind s53.

# R-CR6 — a data-plane connection may ride Node RPC framing (A) (Luke 2026-10-12)

A data-plane connection may use Node RPC framing when a second Node RPC client inside the node originates it and the node logs it as its Direct entry; `node.connections.delete { All }` deletes it. The node's control-plane client and pool stay unlogged, and `All` never touches them (R-CR5). What makes a connection data plane is who originated it for traffic and that it is logged, not the framing. connections.md's control-plane sentence names "the node's control-plane Node RPC pool". Evidence: s53 @ f96ebdb7 — the control client attaches no observer (admin.rs:5107, testkit node.rs:376), the originate door's client does (testkit node.rs:379); two dials to the admin, two accepts, one Direct entry. Rejected: B, a separate data transport in the test kit (a second protocol for a stand-in).

# Q2a — the Seated frame's order key is the holder record's revision, per seat key (A) (Luke 2026-10-13)

Seated { seat, holder } carries its order key in the body already: the holder record's revision, per seat key `(mesh, node_type)` for a cohort and `fabric` for the fabric seat. It is listed in gossip.md's order-key list beside Members' and Digest's (R-RX3); an older record is ignored in the one gossip receive path with the ignore span, and no handler compares. membership.rs's own `refused-not-newer` check goes with the handler-level ordering R-RX3 removed. Records for different seats never suppress each other.

# Q4a — in fabric recovery the launched admin is the only node, so its time is rafka-time (Luke 2026-10-13, "then its time is rafka time")

There is one clock, rafka-time, served by the fabric-primary. Fabric recovery is the case where nothing is alive: no channel with an active listener, the store naming nodes that are all dead, the old process on the inherited data dir included, each confirmed gone by the provider and never by a wait. The node-admin launched on the fabric-primary's inherited data dir is then the only node, the fabric-primary, and its own clock is rafka-time from the moment it starts. If any node answers, it is not this case: the launched admin joins the live fabric and takes rafka-time from the fabric-primary that exists. Doc line: origin-ordering.md:73 and fabric-recovery.md "Nothing alive" (Q3's bullet), on Luke's yes to the exact text with the R-SB1 landing.

# R-MJ1 — node.mesh.join and node.mesh.leave are primitives; the workflows compose them (Luke 2026-10-13)

**Luke 2026-10-13:** a mesh is a channel on the one fabric network; joining it is a join, never a "subscribe" (the library's verb stays in code comments). "This should really be primitives: node.mesh.join, joining, joined, and node.mesh.leave, leaving, left. Joining occurs on start, restart, and post-tickle after the ping; leave occurs on stop and delete. Node-admins join two channels: their own mesh and the backbone." Spans to match, "that will give us more insight into the wedges". Then: "start with the rulings and the new primitives, then review the primitives and build the workflows; the workflows should in reality just be links to the primitives … mesh investigation is just a node investigation conducted by the FP; it is really just who initiated the primitive and at what level."

- `node.mesh.join`: the node's JoinNode report on 0x1D (the op stays as it is) and its channel join, `f(FabricId, MeshId)`; a node-admin joins its own mesh and the backbone, and the primitive completes when both hold. Progress `joining`, completion `node.mesh.joined`. Run inside node.start and node.restart, standalone when a tickle reaches a node that is off its channel (the mesh-primary commands it back), and by a node-admin entering an existing mesh.
- `node.mesh.leave`: the node leaves its channels, the step stop and delete call "its own transport cut" (R-CR5), after `node.connections.delete { All }` and never before. Progress `leaving`, completion `node.mesh.left`. A leaving node gossips nothing (R-ON1); its mesh-primary publishes NodeStopped or NodeDeleted as today.
- No new gossip frame: `NodeLeaving` / `NodeLeft` stay reserved and unpublished; the completions are stream frames and receipts. The join's visible effect on gossip is the node's first Digest and the mesh-primary's one Pending publish; the leave's is the mesh-primary's NodeStopped.
- The command half is new: `JoinMesh` and `LeaveMesh` command pairs on 0x1B (appended variants, `durable format moved`), retiring the legacy `Rejoin` control frame; wire change with the flame: tickle, command, JoinNode, the channel joined span, the Pending publish.
- Spans: `rdm.node.mesh.update.via-joining` / `via-joined` / `via-leaving` / `via-left` at the node, each naming mesh, MeshId and, for a node-admin, which channel.
- Member statuses are unchanged (`joining` / `leaving` are the primitive's progress, not NodeState variants).

**Primitives the same day (Luke: "understood", "yes"):** `node.tickle` (the reach ladder: direct, then both via-peer kicks at once; reached / unreached; unreached never means dead; R-SW1) and `node.runtime.inspect` (the provider's answer about the exact runtime handle; the only exit proof; deployment-provider §2.4) are primitives with pages. Mesh investigation is `node.tickle` run by the fabric-primary over a mesh's node-admins, ending in `node.mesh.join` or `node.create`; node recovery is the mesh-primary's tickle, then inspect, then delete and create. The stopped-node collector (R-GC1) is a fabric workflow page. Workflow pages compose primitives by link and restate nothing.
