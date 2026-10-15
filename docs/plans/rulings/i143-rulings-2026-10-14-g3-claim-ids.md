# R-G3 — peers resolve a seat holder's contact addresses from the digest; `NewFabricPrimary` is a reserved frame position (Luke 2026-10-14, "yes to all", arm A)

**Docs of record:** `gossip.md` §3.1 (frame catalog, reserved lifecycle frame positions); `lifecycles/node-primary-update.md`.

**Ruling (Luke 2026-10-14, on the R-G3 packet: arm A).**
- A seat update is announced by "primary updating" and "primary updated" only (node-primary-update.md). Neither carries a contact address, and nothing else announces one: every birth's `endpoint_id` and `transport_addr` ride its `Digest` and its mesh-primary's `Members` projection, and a Node RPC caller resolves a holder through the live resolver fed from the digest book (`node_rpc.rs` `feed_once`, `live.rs` `LiveNodeResolver`).
- `Frame::NewFabricPrimary { holder, endpoint_id, transport_addr, admin_api_base }` keeps its position (14) as a reserved frame position, never published, with its byte fixture, like the four R-CR4 positions. A receiver refuses it by name and applies nothing. Positions 15-21 keep their tags: nothing moves on the wire.
- The dead sender (`Membership::announce_new_fabric_primary`), the dead receiver (`note_new_fabric_primary`) and the `SeatBook` contacts side-table (`FabricContacts`, `attach_fabric_contacts`, `fabric_contacts`) go. gossip.md loses the `new-fabric-primary` catalog row, its decoded example and its "New-fabric-primary and contact addresses" section; the reserved-positions row and section name position 14.
- Flame (Jaeger 192.168.68.99:16687, lane e4s3 at a2448aac): same-mesh hand-over `0e8028bd75dde6f1fa7a9805b1fa08b7` (row written), `eaa335e5935a3af0beeb581f5f85076e` (heard on the backbone), `9171f3aca1237368a47452e4a68d98f9` (recompute); cross-mesh `01d16476139da2d8c64a88852eae5c8a`, `33cf0bfa84389031b4123798469eb8f7`, `7f2c45aaff44122ff1aa07187f107808`. No `via-new-fabric-primary` span in any of them.
- Not covered by this yes: `build.md:554` and `node-rpc-envelope.md:1419, :1423` still link the deleted anchor and name the announcement; their exact old and new text is the next ask.

# R-BC1 — 0x1C has one seat refusal, `NotClaimant` (Luke 2026-10-14, "yes to all", option 1)

**Docs of record:** `node-rpc-envelope.md` BuildClaim 0x1C.

- `BuildClaimReply::NotClaimant { mesh, fabric_primary, mesh_primary }` is the one refusal of a receiver that may not claim in the lane: it names who may, as the receiver sees it. The `RejectedNotAuthority { reason: String }` line that 8fc5be4c added to the 0x1C block without a ruling is struck; the code never had it. No wire change.

# R-BI5a — `bid_xxxx-xxxx-xxxx` is a log rendering only (Luke 2026-10-14, "i agree")

**Docs of record:** `node-identity-and-state.md` §2; `build.md` §3; `i143-prd.md` `e12.s58`.

**Ruling (Luke 2026-10-14: "bid_xxxx-xxxx-xxxx but only in logs otherwise we just use the raw-crockford as its a bigint storage value at 12"; agreed 2026-10-14 to build).**
- On the wire, in every store row and in every page example a Build id is the bare 12-character Crockford60 (R-BI5): a 60-bit integer at 12 characters (`BuildId`'s `Display` in `rafka-mesh-entity/src/ids.rs:151` stays bare).
- A log line or `build_id` span field renders it `bid_xxxx-xxxx-xxxx` (the same 12 characters in three groups of four, `BuildId::log()` -> `BuildIdLog` in `rafka-mesh-entity/src/ids.rs:193-212`) for the eye. Nothing parses that form (`BuildId::parse("bid_...")` refuses by name with `IdParseReason::WrongWidth`), and nothing stores it.
- Cell: `ids_crockford::a_dashed_bid_build_id_is_refused_by_parse_and_emitted_only_in_build_id_spans` (RED `8f0e64d0`, landed `55131ee4`, gate 46 s). Flame (Jaeger 192.168.68.99:16687): trace `a592b745aaea8819ba14a5777842e24b` (`rdm.node_admin.build.create.via-accepted` span `c696c67b348c3a5e` carries `"build_id": "bid_293h-reeg-et79"` while the Build row key `node.primary.update:293hreeget79:1:node.stop:mesh1.admin.1`, stored `BuildFact` JSON and the `0x20` `BuildReply::Started` wire carrier stay bare `293hreeget79`).

# R-RX6 — the four A' attribution items in `e12.s53` stay as landed (Luke 2026-10-14, "i agree")

**Docs of record:** `node-rpc.md` §14; `gossip.md` §3.1; `i143-prd.md` `e12.s53` (landed `f1109974`, gate 44 s).

- The four places that read the transport-authenticated sender or the seat book to attribute an incoming message or sample are not receiver authorization and stay as landed:
  1. round `heard_from` keyed on the sender (`status_rpc.rs:582`);
  2. the `ReadyForTraffic` round report recorded under `Reporter::Birth(incarnation)` or `Reporter::Endpoint(peer)` (`status_rpc.rs:671`);
  3. the probe wake `wake.addressed_by(sender, peer)` (`status_rpc.rs:1028`);
  4. rafka-time lineage: `from_mesh_primary_digest` and `from_fabric_primary_members` admit a stamp only from the digest or aggregate whose own payload birth is the seat book's holder and `ReadyForTraffic` (`rafka-mesh-transport/src/discipline.rs:42-78`). That is content against committed record, not sender attribution.

# R-SF1 — the staleness floor stays process-wide (Luke 2026-10-14, "i agree", option A)

**Docs of record:** `i143-prd.md` `e12.s82` / `e12.s83` (landed `f1109974`, gate 44 s).

- `cadence::staleness_floor()` (`rafka-mesh-entity/src/cadence.rs:56`) stays read once per process.
- The four transport cells that need another floor keep running in their own subprocess with `RDM_STALENESS_MS` set through `own_process::delegated_with_env`: `channel_stopped.rs:69`, `gossip_stats.rs:77`, `peer_location_book.rs:148` (`60000 ms`), and `heartbeat_spans.rs:63` (`2000 ms`).
- No floor parameter is added to `Membership::join`.

# e12.s55 — the PRD row is the record (Luke 2026-10-14, "i agree")

**Docs of record:** `i143-prd.md` `e12.s55` (landed `f1109974`, gate 44 s).

- Leave the `e12.s55` PRD row as the record for parked post-beta `R-HB3` (`digest_seq` as evidence on four `0x1B` replies) and file no issue.
- Replace the dead `#3085` tracking reference in the `e12.s55` header with `Tracking issue: none (the #3085 reference never resolved; parked post-beta, this row is the record)`.

