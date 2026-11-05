# Lead decisions, 2026-11-04 (delegated)

Luke 2026-11-04: "I think you have everything you need from me ... you're approved to move forward on
the refactor, all text, whatever else you have; don't come back to me with additional questions."
Each row is a decision the lead made under that order, with its reason.

| # | decision | why |
|---|---|---|
| D1 | The gossip refactor (docs/plans/proposal/gossip-by-kind.md) changes no wire: channel ids keep their bytes, status is one Rust type over today's five frames, the Q375 renames are Rust names only | red team H6: build-deployment.md §3 and Q378 make mixed executables the upgrade path, so a new channel id or frame shape splits a mesh during its own upgrade |
| D2 | `rdm-gossip` is its own crate, depending on `rdm-transport`; `chunking` stays in `rdm-transport` | no Node RPC module reads gossip; rdm-client's `read_frames` uses `chunking` (red team H7) |
| D3 | The full and delta frames' Rust names are `TopologyFull` and `TopologyDelta` | gossip.md §3.3's words; no collision with rdm-admin's or rdm-core's `Topology` |
| D4 | `rejoin`'s filter stays; refeed re-asks by clearing the asked set | the same dials as today through the one join |
| D5 | The two chunk methods stay | different jobs (items into whole frames; one message split), and O4's single method would be a wire change (D1) |
| D6 | "Status frame" replaces "lifecycle event" in gossip.md | Q374: they are status; §3.2 already says "status frame" |
| D7 | `Channels`, one per node, holds the iroh-gossip handle, the endpoint and the view; `Channels::join(id, peers)` is Q376's `Channel.join(id, peers)` | red team H8: test cells run several nodes in one process; the receive path comes from the view the channel fills |
| D8 | The seed (Q376): the mesh channel never names a launcher or commander of another mesh; a commanded start or join adds the commander to every other join; the post-read joins, repair, refeed, reopen and the recovery starts keep today's peers | red team H1, H2, H3: the fabric-primary commands a node-admin's start; the post-read joins and Q88's reopen are dials the seed alone would remove |
| D9 | The node-admin's role flags stay on `Backbone`, set by the election as today | red team H4: reading them from the view would change the election (Q335) |
| D10 | The executable replacement (executable-replacement.md) covers nodes that are not node-admins; a node-admin at a held path stays `name-held` | red team H2, H3: a node-admin's replacement meets the fabric-primary rule (Q370) and a mesh's only node-admin (`LastAdmin`); the use case is a gateway's or a broker's binary, and Q380 asks for the minimum |
| D11 | Acceptance pins the held birth as a `Delete` does; the plan runs the primary role, the delete and the create per replaced path | red team H1: `plan_with` counts a live birth as satisfying its path, so relaxing `created()` alone runs nothing, and the pin tells the held birth from a recovery successor (Q372) |
| D12 | A section runs its replacements one at a time, the node-type primary's last, never in the batch of 8 creates | red team H4: a batch would delete every broker of a type at once |
| D13 | The new node's data dir follows the submitted entry's persistence, needs no `exited` reading after the replacement's own delete, and survives a resubmit; the launch uses the entry's snapshot | red team M1; Q381 |
| D14 | "Another executable" is compared with the executable the node's row records, `launch_id` aside | red team M2: the view and the atlas hold no executable |
| D15 | The fabric-primary checks at a section's end that every pinned birth is deleted, else the section ends `incomplete` by name | red team M4: a mesh-primary on an older RDM would plan "live means satisfied" and report complete |
| D16 | gossip-by-kind.md's steps 3 and 4 land in three parts: the one join (`TopologyView::new`, `ChannelId`, `Channels::join`, every join site through it); then the own record (config, tags, row and name) to rdm-node's node record; then the one `Channel` of any kind (gossip.rs's `Channel` and `Hub` into `rdm_gossip::channel`), the `app` module, and `Channels::join` returning the `Channel` | each is its own body of code: the join touches every start and entry, the own record every property writer, the application channel its delivery (bounded queue, loss generation, chunk assembly); one landing for all three would leave one red between three causes |
| D19 | The dial span's caller in the channel's own spawned loops is the loop's existing span (`rdm.mesh.topology.update.via-resubscribe` for the reopen, `rdm.mesh.connection.update.via-refeed` for the repair and the refeed), not the proposal's `topology.reopen` / `topology.repair` / `topology.refeed` | every span RDM exports is `rdm.`-prefixed (the `rdm_spans_are_rdm_prefixed` ratchet; each cell's `assert_own_story`), and the loops already open those spans |
| D17 | A join on a held channel that is stopped names its peers and dials nothing, as before; a commanded start reopens the channel (`Channels::start`) and then joins it through the commander and the peers it knows | the node-admin's turn loop joins the backbone every interval, also while the admin is stopped: a join that started a stopped channel would put a stopped admin back on the backbone |
| D18 | `mesh_seeds` drops the launcher from a created node's mesh-channel peers only when the launcher is of another mesh; the seeds a launch from another mesh adds (that mesh's Ready births, the runner's `template`) stay | they are nodes of the node's own mesh: on its channel |
| D20 | gossip-by-kind.md §4.3 lands in two parts. First the record: config to `NodePropertyWriter::config`, tags to the own row the writer holds, the row to `NodePropertyWriter::set_own_row` / `own_row`, `OwnConfig` to `node_records::OwnRecord`. Then the name: `set_name`, `held_name`, and "the `Digest` publisher is handed the name it publishes" | the name is the view's own identity in its receive paths (`View::current_name`, the digest view, the location view), a different change from the record's copy; observed while moving the row: the start writes its record (`bound`) before its topology read, so a start's record takes the launch's config and tags, and the row the read carries is held for the writer |
| D21 | §5.3's one Rust `Status` type waits for Q374's one wire frame (D1); this refactor folds no status shape | over today's five frames it is five shapes under one name (four order keys, three recovery paths, two send counts: §5.3's table) kept in step with `Frame`, an adapter between spellings (Q331); `NodeDeleted` carries no publisher or version, so building it with `StatusEvent`'s other two node frames needs an `Option` the frame does not have |
| D22 | The node's name stays on its topology view, its one holder (`set_name`, `held_name`); §4.3's move of the name is not made | the view's receive rules name the node by it (`View::current_name`, the digest view, the location view), and rdm-node's `HeldName`, the connections writer and the property writer already read it from the view; a holder in rdm-node would hand the view a closure back, a second path for the one name |
| D23 | The one `Channel` of any kind (§4.2's `channel` row) is not built: `Channels::join` is the one join over the topology channel and the application channel; gossip.rs, the application channels' delivery, is §4.2's `app` module | the two share a sender and a stop only; one struct would carry the other kind's parts as options (repair, refeed and replay; the bounded queue, loss generation and chunk assembly), and their stop and reopen differ (a reopen through the held members; a resubscribe through the peers the application named) |
| D24 | D21 and D22 are withdrawn: §5.3's one `Status` type (960d83863) and §4.3's move of the name to the node's own record (4ca97c0ac) are built | Luke's order was to build the proposal without coming back; a Q331 flag is told to him, never a reason to leave an approved step out |
| D25 | D23 is revised: §7 step 4's deletions are built, the application channels' `Channel` and `Hub` (6fed93b6e: `Channels` holds the subscriptions, `Channels::app` hands the publisher and the receiver); the receive loop stays per kind | §6: step 4 changes no behaviour and no dial; one loop would have an application channel re-subscribe on `Lagged` and refeed its known peers, which it does not do today |

## The red team's 19 findings (gossip-by-kind.md), as folded into the proposal

| finding | folded into |
|---|---|
| H1 the start/join seed is not always of the node's own mesh | §2.5 (the commander can be the fabric-primary); §5.2 table; D8 |
| H2 the first-listener rule has no input for most node-admin starts | §5.2 rows Day-0, recovery, inherited or lost root, adoption ("as today"); D8 |
| H3 `vec![seed]` removes dials | §5.2 rows after the read, repair, refeed, reopen; §6 marks the dial; D8 |
| H4 role flags from the view change the election | §4.2 `backbone` row; D9 |
| H5 §5.3 misstates the receive rules | §5.3 table, each type keeps its own rule |
| H6 one RDM version contradicts the upgrade path | D1 |
| H7 `chunking` serves Node RPC | D2, D5; §3.7 |
| H8 one iroh-gossip per process is false | D7; §5.1 (one `Channels` per node) |
| M1 the shared pieces | §3.1 |
| M2 step 2's callers | §7 step 2 |
| M3 repair and refeed have no named span | §5.1; D19 |
| M4 the seed credited to Q376 | §2.5 |
| M5 one derivation merges namespaces | D1 (channel ids keep their bytes) |
| M6 the §8 text leaves the next sentence wrong | §8 |
| L1 §3.2 counts | §3.2 |
| L2 build.md's rows | §3.1 |
| L3 §3.6 misses Day-0 and recovery seeds | §3.6 |
| L4 §2.2 the delta's author | §2.2 |
| L5 §4.2 `location` row | §4.2 |

## 2026-11-05, Luke away ("annotate your decisions in a decision log i can review later; do not stop and ask me; consult the advisor")

| # | decision | why |
|---|---|---|
| D26 | 402f01758 (Q382's first text: a launch's seeds carry the launcher's live births of the node's mesh) is withdrawn and not landed; the launch seed stays the launcher alone | Luke, after seeing the build: "the launch seed is the MP or the FP if its launching the MP" (Q382 amended); Q376: the node-admin passes itself into the launch as the seed; a seed list is a special part a node-admin or a primary must not have (Luke: "no special parts in it minus getting a signing key") |
