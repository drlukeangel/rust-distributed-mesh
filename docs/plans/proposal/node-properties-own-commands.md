# Proposal: node.update goes; every node property has its own command

## Problem

`node.update` is documented as "a blind full-entity put, never PATCH" (node-rpc-envelope.md:266) and validated against a three-group Meta table (:272-278). The frame it sends carries two settable fields: `UpdateNode { node_id, incarnation, name, tags }` (`rafka-node-rpc-contract/src/status.rs:362`; the envelope's :628 shows it without `tags`), a rename and a full-set tag put in one frame. Every other property already has its own path:

| property | how it changes today |
|---|---|
| tags | `node.tags.create { label \| [labels] }`, `node.tags.delete { All \| [labels] }` (node-rpc-envelope.md:244); each label its own keyed row |
| config | `node.config.update { map }`, the whole map (:245) |
| `otel_debug` | not a property: OTel debug is call context (Luke 2026-10-12). It rides the envelope's context section, `CallContext { caller_system, traceparent, tracestate, baggage }`, beside the trace context, as the baggage entry `otel_debug=1`; per call, read by the receiver's tracing layer, propagated across a carried hop. Never a field of a command body and never on the node record |
| `is_primary`, `is_fabric_primary` | projections of the committed seat record; moved only by node.primary.update / node.fabric-primary.update |
| status | node status only, never a put |
| `node_type`, `path`, `mesh` | the create spec; immutable for the node's life |

So today's `node.update` is a rename plus a full-set tag put; after this proposal the rename alone, under its own name. Today it sits under a name that promises a general put and a table that guards fields no command can carry. The client's `NodeMeta::check_update` (rafka-node-admin-client `objects.rs:230-260`) compares eleven RDM-owned fields the caller cannot change; the only decisions that matter are made on the server in `node_update.rs`: the exact birth (`RejectedStaleIncarnation`), the rename precondition (`traffic_of`, :90-100), the name claim (`NameHeld`), and who writes (`ReceiverNotPrimary`).

The row has one writer, the node's mesh-primary, which holds the whole record and serializes every write (R-OW1; R14: a sole writer's whole-record write is not read-modify-write). One command per property under that writer is one defined storage operation each, with no second writer anywhere.

## Docs of record

- R-CR1: a node never takes a new name while it holds a connection; `ApplyNodeName` answers `Busy` naming the connections.
- R12 / R13: node.update is one write path: validate, adapter insert-and-fail claim, put, `ApplyNodeName`; the mesh-primary writes its mesh's rows; `stopped`, never `parked`.
- R-OW1, R14: the writer ladder; the sole writer's whole-record write.
- R-TG1: config is a property bag riding the digest; tags are labels in the node's meta with their own index.
- R-MN1: a name is unique within its path; only delete releases it.
- Luke 2026-10-12: a path is free text and a name is what we create; `node_type` is the node's own immutable field; OTel debug is call context.
- Pages: node-rpc-envelope.md (typed-object table :242-245; "Meta and config" :266-282; the 0x1B frames :625-678), lifecycles/node-update.md, lifecycles/node-replace.md (:22, :32, :35), lifecycles/node-recovery.md (:24-25, :71-72, :82, :107), lifecycles/node-connections-delete.md, fabric-mesh-node-lifecycles.md (:88, :91).
- Evidence is rafka-v2 main and RDM main `f5ca6ab4`; dev re-pins at landing.

## 1. The node's properties and their commands

Each command goes to the node's mesh-primary as the row writer (Forward `0x1A` when not directly reachable), is refused `fabric-not-ready` until the initial fabric has first reached ready, and runs through that writer's defined storage operation.

| property | set by | changed by | precondition | completion |
|---|---|---|---|---|
| `node.id`, `incarnation`, `endpoint`, `transport_addr`, the runtime handle | RDM at birth | nothing | — | — |
| `status` | RDM | [node status](../../architecture/lifecycles/node-status.md) only | — | that status's own completion |
| `path`, `mesh`, `node_type` | the create spec | nothing: immutable for the node's life. `node.name.update` carries the name only, so the path cannot move by construction | — | — |
| `name` | the create spec | **`node.name.update { node_id, incarnation, name }`** (§2) | the node admits no connection: stopped, or connections-deleted after an `All` with zero held; never ready (R-CR1) | `node.name.updated` (§2.3) |
| `tags` | the create spec (may be empty); inherited with the config on recover (from the Deleted row at the claim) and on replace (from the retained predecessor row at the cutover rename) (§8) | `node.tags.create { label \| [labels] }` adds; `node.tags.delete { All \| [labels] }` removes. Additive and removal commands on keyed label rows, not a replacement of a tags property (unchanged, :244) | none | `node.tags.created` / `node.tags.deleted` |
| `config` | the create spec | `node.config.update { map }`, the whole map replaced (unchanged, :245) | none | `node.config.updated` |
| `is_primary` | the committed seat record | `node.primary.update` ([node.primary.update proposal](node-primary-update-fabric-mesh-and-node-type.md) Part C); never a put | that proposal's Part C | `node.primary.updated` |
| `is_fabric_primary` | the committed seat record | `node.fabric-primary.update` (same proposal, C2) | same | `node.fabric-primary.updated` |
| connections | — (data plane, never a property) | `node.connections.delete { All \| [ids] }` | — | `node.connections.deleted` |

`node.get` still answers the whole node. Nothing above is PATCH: a rename replaces the name, a tag command adds or removes whole labels on their own rows, a config update replaces the whole map, and each runs once through the one writer.

## 2. node.name.update

### 2.1 Contract

| Field | Contract |
|---|---|
| Caller / executor | Caller to the node's mesh-primary, the only writer of its mesh's rows (Forward `0x1A` when not directly reachable). Before a mesh has a primary, the fabric-primary acts as its writer. The replace recipe calls it as a step; a caller may call it alone |
| Entry | The exact `(node.id, incarnation)` and the new name. Nothing else: no path, no mesh, no type, no tags |
| Precondition | The node admits no connection: its status is stopped, or this mesh-primary holds the birth's `node.connections.deleted` of an `All` made while the node was not ready and it holds zero connections. A ready node, or one still holding a connection, is refused. No new connection is admitted from the row write through `ApplyNodeName`, retries after an interruption included |
| Validation | the exact birth matches the held record, else `RejectedStaleIncarnation { held }`; the receiver is the node's writer, else `ReceiverNotPrimary { needed }` (R-RX5); the precondition holds, else `RejectedAdmittingConnections { status, held_connections }`; the name is within the name grammar, else `RejectedName { reason }` (§9 decides which names) |
| Claim and move | The new name is claimed by insert-and-fail on the kind's `path.name` index inside the store adapter; a held name answers `NameHeld { by: node.id }` and nothing is written. The move is two writes (`node_rows.rs:668` `move_row`): the row is put at the new key carrying `renamed_from: <old path.name>`, then the old key is released. Between them the node holds both keys, and the destination row says which is which: **a row whose `renamed_from` key still holds the same node id has a pending release, and the writer releases it.** The rule needs no clearing: once the source no longer holds the id it is false. It runs wherever a writer meets the row: a retry of the command, the Build attempt's resume of a replace or recovery step, and a mesh-primary's rebuild from its mesh's rows after a seat change (R13), which already reads every row of the mesh. A same-process retry is one case of it, not the mechanism |
| Order | Row first, node second |
| Completion | §2.3 |
| Idempotency | Row-held and node-applied are two facts. A repeat naming the name the row already holds finishes any pending release and then, for a live node, resends `ApplyNodeName` and completes on `Applied` or `AlreadyApplied`; for a stopped node it completes on the row. A repeat with a different name is a new rename |
| Wire | `UpdateName { node_id, incarnation, name }` and `ApplyNodeName { node_id, incarnation, name }` on 0x1B; replies `NameUpdated`, `NameHeld`, `RejectedAdmittingConnections`, `RejectedName`, `RejectedStaleIncarnation`, `Busy { connections }` |

### 2.2 Sequence

1. The caller sends `node.name.update` to the node's mesh-primary.
2. The mesh-primary validates from the record it holds: exact birth, its own writer role, the precondition from the node's status and its `node.connections.deleted` receipt (`traffic_of`), the name grammar.
3. The new name is claimed by insert-and-fail in the store adapter; a held name refuses `NameHeld { by }` with nothing written.
4. The mesh-primary writes the row with the new name. The old name's key is released. This is the durable name move.
5. A stopped node is sent nothing: it reads its name in its join answer at its next start, and at any join the row wins.
6. A live connections-deleted node is sent `ApplyNodeName`. It publishes its digest with the new name only on that apply and answers `Applied`, `AlreadyApplied` for a name it already holds, or `Busy { connections }` naming each connection it still holds; `Busy` after the row is the interrupted case of §2.4.
7. `node.name.updated` is emitted per §2.3.

The node owns its name: it is the authority on the name it publishes, and what it publishes is the row's name. A node never renames itself.

### 2.3 Completion, by case

- **Stopped node:** `node.name.updated` after both writes of the move, the put at the new key and the release of the old (step 4). Nothing is sent to the node; its next join reads the name.
- **Live connections-deleted node:** `node.name.updated` after `Applied` or `AlreadyApplied` (step 6).
- **Row written, apply unresolved** (no answer, or `Busy`): the command ends incomplete, naming the row's new name and the apply's state. The name has moved durably and the node will publish it at its next apply or join; the caller resubmits, the resubmit resends `ApplyNodeName`, and completes on `Applied` or `AlreadyApplied` from the node, never from the row. A live node is never reported renamed on the row alone.

### 2.4 Failure contract

- **Refused before the claim** (stale birth, wrong receiver, admitting connections, bad name): nothing written, nothing changed.
- **Claim refused** (`NameHeld`): nothing written.
- **The put at the new key fails**: a named storage error, nothing written, the old name stands.
- **The put succeeded and the release of the old key fails or is interrupted**: the node holds both keys, and the destination row's `renamed_from` names the source. The command ends incomplete naming the new key as claimed and the old key's release as pending; the release is finished by whichever writer next meets the row (a retry, the next attempt, or a successor mesh-primary's rebuild) and the move continues to the apply. Nothing is rolled back and no second row is written. The writer that wrote the put may be gone; the row carries everything the next one needs.
- **Apply fails or is interrupted after the row**: §2.3's incomplete case. The admission block stays in force on the node until the apply resolves, so no connection is opened under the old name while the row carries the new one; a retry of the same command resends `ApplyNodeName` and completes on the node's answer.
- **Connection opened between the precondition check and the apply**: impossible by the admission block; a node that reports `Busy` is a defect in that block, named by the connections it lists.

## 3. Tags and config: unchanged, stated precisely

`node.tags.create` and `node.tags.delete` add and remove labels; each label is its own keyed row under the writer and feeds the tags index (R-TG1). `node.config.update` replaces the whole config map in one write; the map rides the digest. Neither has a precondition beyond the fabric being ready, neither touches the name, and neither is a Build step. They are not changed by this proposal; they are the pattern it brings the name under.

## 4. What is deleted, and what is kept from it

Deleted: `node.update`; the "meta entity" and "Meta groups" vocabulary and table; the frames `UpdateNode` and `NodeUpdated`; the refusal `RejectedMetaFieldIsRdmOwned`; the client's `NodeMeta` (`rafka_node_admin_client::NodeMeta`, the put entity with `otel_debug`) and its `check_update`; the server's `meta_of`. Nothing else reads that type once `node.update` and recovery's step 6 are gone.

Not this type: `rafka_mesh_entity::meta::NodeMeta { storage: StorageMeta, placement: PlacementMeta }` is the accepted Build's per-path policy (what survives a restart, where a node may be placed; `accepted.rs` `node_meta_by_path`, `build.rs:447`), Build input under build.md's "explicit desired Meta policy" seam, never a node property anyone puts. It stays, and takes the name for what it is (Luke 2026-10-12): `path_config`, the defaults set at a path that every node created there inherits (here storage and placement), as distinct from `node_config`, the application's own bag on the node. `PathConfig` in code, on the same s41 row.

Kept, moved into `node.name.update`'s validator before anything is deleted:

- the exact-birth check (`RejectedStaleIncarnation`);
- the writer check (`ReceiverNotPrimary`);
- the rename precondition (`traffic_of`), now answered by its own refusal, `RejectedAdmittingConnections { status, held_connections }`, instead of a generic one;
- path and mesh immutability, now by construction: the command carries the name only;
- the name claim (`NameHeld`);
- the permitted-name rule of §9, as `RejectedName { reason }`.

Nothing that `check_update` refused today becomes allowed by deletion; the one rule that may change is which names are permitted, and §9 states it as a decision, not a side effect.

## 5. Doc edits (Luke's yes to this proposal is the yes to each)

1. **node-rpc-envelope.md :180** (op list): `node.update` → `node.name.update`.
2. **node-rpc-envelope.md :242** (typed-object row): "node.name.update | Rename, to the node's mesh-primary; frames `UpdateName` and `ApplyNodeName` on 0x1B; the node admits no connection | node.name.updated; `NameHeld { by }`, `RejectedAdmittingConnections`, `RejectedName`, `RejectedStaleIncarnation`; `fabric-not-ready`".
3. **node-rpc-envelope.md :266-282** ("Meta and config", the three-group table, the `otel_debug` sentences) → "Node properties", the §1 table, and the kept sentence "Status moves only through node status, never a put. A new exe requires a new node."
4. **node-rpc-envelope.md :628** `UpdateNode { node_id, incarnation, name, tags }` → `UpdateName { node_id, incarnation, name }` (the full-set tag put leaves the frame; tags change only through `node.tags.create` / `node.tags.delete`); **:676** `NodeUpdated` → `NameUpdated`; **:677** `RejectedMetaFieldIsRdmOwned { field }` → `RejectedAdmittingConnections { status, held_connections }` and `RejectedName { reason }`. Variants renamed and replaced on 0x1B: `durable format moved`.
5. **lifecycles/node-update.md** → **lifecycles/node-name-update.md**: the §2 contract, sequence, completion and failure contract; the put vocabulary gone; its "Config update" section becomes **lifecycles/node-config-update.md** on its own. Every link to `node-update.md` moves (node-replace.md, node-connections-delete.md, node-start.md, states.md, the envelope, the index; `grep -rn node-update.md docs`).
6. **fabric-mesh-node-lifecycles.md :88** → two rows, `node.name.update` → node-name-update.md and `node.tags.get/create/delete` → the envelope's typed-object table; **:91** `node.config.get/update` → node-config-update.md.
7. **lifecycles/node-replace.md :22, :32, :35**: `node.update` → `node.name.update`; the sequence unchanged.
8. **lifecycles/node-recovery.md**: step 6 and its arrows (:71-72, :82, :107) deleted; the Steps row (:24) reads "node.delete of the exited instance, node.create at the same path.name over the inherited data dir; the Build's steps are keyed `node.delete:<path.name>` and `node.create:<path.name>`"; Retained (:25) keeps "config (seeded from the deleted row)".
9. **node-primary-update-fabric-mesh-and-node-type.md A2 item 3** (the Meta table row for `node_type`) is superseded by the §1 table; its C1/C2 completion for the fabric seat reads `node.fabric-primary.updated` (corrected in that proposal in the same landing).
10. **Rulings page**: one cross-reference line under R12/R13 and R-CR1: "the sequence is node.name.update's (proposal node-properties-own-commands.md)"; no rewrite.

## 6. Code (PRD e12.s41 row)

- rafka-node-admin-client: `NodeUpdate` typed object → `NameUpdate { node_id, incarnation, name }`; `NodeMeta`, `check_update` and the `otel_debug` field deleted; the typed replies renamed.
- rafka-node-admin-core `node_update.rs` → `node_name_update.rs`: the validator of §4 (exact birth, writer, `traffic_of` as `RejectedAdmittingConnections`, name grammar as `RejectedName`, the adapter claim), `move_row` as it is (two writes, `AlreadyMoved` on retry) with the incomplete outcome naming the pending release, `ApplyNodeName` resent on a same-name retry for a live node, completion per §2.3; `meta_of` deleted.
- `storage.rs` claim over a Deleted row, and `node_rows.rs` cutover move for replace: seed tags with config and write the successor's tags index entries (§8); `NodeRow` gains `renamed_from` (§2.1), written by the put and read by the finishing rule.
- 0x1B frames and replies per §5 item 4; the mesh-audit reseal.
- The replace recipe's two renames call `NameUpdate`; the recovery pipeline's step 6 deleted.
- `durable format moved`.

## 7. Cells

Each asserts the row, the node's published name and the completion:

- stopped node renamed: claim, row, no apply, `node.name.updated` after the row; its next join publishes the new name;
- running node, connections-deleted after `All`, zero held: claim, row, `ApplyNodeName` → `Applied`, `node.name.updated`;
- ready node: refused `RejectedAdmittingConnections`, nothing written;
- connections-deleted node still holding one connection: refused `RejectedAdmittingConnections { held_connections: [id] }`, nothing written;
- held name: `NameHeld { by }`, nothing written;
- the put at the new key fails: nothing written, old name stands;
- failure or interruption between the put and the release: both keys held, the command ends incomplete naming the pending release, the retry finishes it and continues to the apply;
- the writer lost between the put and the release: a successor mesh-primary's rebuild finds the row's `renamed_from` still held by the same node id and releases it; the next attempt of the step continues to the apply;
- same-name retry on a live node after an unresolved apply: `ApplyNodeName` resent, completion on the node's answer, never on the row;
- apply interrupted after the row: the command ends incomplete naming the new name; the node admits no connection meanwhile; the retry answers `AlreadyApplied` and completes;
- a name outside §9's rule: `RejectedName`, nothing written;
- tags and config commands unchanged and green;
- a recovery with no step 6: delete, create with config and tags seeded and the tags index rebuilt under the new id, no rename, no copy; interruption after the claim leaves them on the row;
- replace: both renames through `node.name.update`, the successor's tags and config inherited from the retained predecessor at cutover, the recipe green.

## 8. R-BI8 is superseded

With `otel_debug` out of the entity there is no caller-owned value to carry between node.delete and node.create. R-BI8's arms, the widened Build-topic output and the seeded meta group, are both obsolete: nobody implements the row extension or the receipt extension. The build proposal's Part D and the two s57 PRD rows are closed with that note.

One property still needs a rule, and it is a create-time rule, not a copy step: **tags**. The Deleted row keeps them (`storage.rs:280`), and today a successor's claim takes none (`storage.rs:316-321`: "a Deleted row a new node takes over changes nothing: its labels left when it turned Deleted"). Two sources, because the two recipes leave the predecessor in two states:

- **recovery:** the predecessor's row is Deleted at the same key; the successor's claim over it seeds config **and tags** from that Deleted row and writes the tags index entries under the successor's id, in the same write that seeds config;
- **replace:** the predecessor is retained, moved to `<name>.old` and never Deleted (node-replace.md:27-38); the successor, born `<name>.new`, inherits config **and tags** from the retained predecessor row at the cutover rename, the write that puts it at `<name>`, and the index entries are written under the successor's id then.

Replace's caller-meta copy step goes with recovery's. The guarantee is the row and the index under the new id; nothing is copied later, and a `node.tags.*` command afterwards changes the tags like any other. Cells: recovery interrupted after the successor's claim, tags on the row and in the index under the new id; replace with the predecessor retained at `<name>.old`, the successor carrying its tags at `<name>` and the predecessor's index entries gone.

## 9. Which names may a rename take — ruled A (Luke 2026-10-12, "you can rename", "a")

node-update.md's "Renames" row lists two transitions: a stopped predecessor to or from `<name>.old`, and a stopped successor born `<name>.new` to or from `<name>`. Those are the replace recipe's conventions, and today they are enforced: `check_update` (`objects.rs:258-272`) refuses a changed path and refuses a name change that is not one `.old` / `.new` toggle on the same base name, and the server calls it. A deliberately removes that restriction; the path check stays by construction (the command carries the name only).

- **A (ruled):** a rename may take any name within the name grammar (the tag-label grammar of the node_type proposal A1: 1-64 of `a-z 0-9 - _ . ~`, no leading `.`, whitespace refused), unique within its path (R-MN1), under the precondition and the claim. Names are free text we create (Luke 2026-10-12); `.old` and `.new` are what the replace recipe chooses, not a validator's list. `RejectedName` covers grammar only, and the `.old` / `.new` toggle check is deleted with `check_update`. Cell: a stopped node renamed to an arbitrary name within the same path succeeds.
- **B:** only the replace recipe's transitions; anything else `RejectedName { reason: "not a replace transition" }`.
- **x:** something else.
