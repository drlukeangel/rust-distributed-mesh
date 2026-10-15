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

# R-BI5a — `bid_xxxx-xxxx-xxxx` is a log rendering only (Luke 2026-10-14)

**Ruling (Luke 2026-10-14: "bid_xxxx-xxxx-xxxx but only in logs otherwise we just use the raw-crockford as its a bigint storage value at 12").**
- On the wire, in every store row and in every page example a Build id is the bare 12-character Crockford60 (R-BI5): a 60-bit integer at 12 characters.
- A log line or span may render it `bid_xxxx-xxxx-xxxx` (the same 12 characters in three groups of four) for the eye. Nothing parses that form, and nothing stores it.
- Recorded, not built: no code renders the log form today; its PRD row sits under e12.s58 / e12.s76.
