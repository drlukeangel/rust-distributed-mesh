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
