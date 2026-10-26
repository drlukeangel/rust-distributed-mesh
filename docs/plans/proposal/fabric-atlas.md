# FabricAtlas

**Origin:** Luke, 2026-10-26 (Q194), from Q189 (a start right after a cross-mesh stop refused
`node-not-live`). **State:** proposed; built after i149 lands. **Name:** `FabricAtlas`, read through
`fabrics.atlas.get`.

## The requirement (Luke's words)

**What it is (Luke, 2026-10-26):** "fabric atlas is the union of what ops are currently executing and the current
topology state." An external client sends `build.create` to the fabric-primary; the fabric-primary accepts or
denies it, deciding from the `FabricAtlas`, never from gossip.

**`FabricAtlas` combines command admission, execution progress and gossip observations, and is exposed
through `fabrics.atlas.get`.** It updates immediately when commands are accepted and before completion is
returned; delayed events cannot move it backward.

**The rule (Luke, 2026-10-26, Q197):** nothing starts, stops or does any action through gossip; gossip is the
side effect of an op, never the reverse. Nothing waits for gossip. Not every op comes through the fabric-primary: a mesh-primary's
self-healing (node recovery, re-create; R-BI7) is an op it runs itself, and the fabric-primary learns its result
through gossip and the store's records; the atlas takes those results in (Luke, same day).

## What it replaces

The fabric-primary accepts a Build from a projection rebuilt per request from the `DigestView` (the gossip
view and everything in flight) and the store rows (`control_plane.rs` accept or deny, `boot.rs` `view_now`,
`projection.rs` `project_with`). A completed stop is already applied before `Complete`, but through the gossip view, which Q197 rules out: `SeatGate::attempt_completed`
(`admin/reconcile.rs:139-152`, awaited at `build_drive.rs:1306`) takes each completed `node.stop:` receipt into
the fabric-primary's view (span `rdm.mesh.topology.remove.via-node-stopped`, `publisher=` empty). No other
receipt is: a completed start leaves the node `leaving` in that view until gossip says otherwise, so a stop sent
the moment a start completes is refused `node-not-live` (red 5/5 on mesh2s node-admins, fabric-atlas-red
8d4768946). `DigestView` keeps its name and meaning (Luke, 2026-10-26: \"digest-view is supposed to be the
gossip-view and everything that is inflight\").

## Requirements

- **R-FA1.** The fabric-primary holds one `FabricAtlas`. The fabric-primary accepts or denies every request (Stop, Start,
  Delete, Reconcile, the rest) reads it, and nothing else. Its accept and progress entries are the
  fabric-primary's own ops (what it accepted, the receipts its commands returned, directly or through
  `build.attempt.run` for a peer mesh), never a gossip frame.
- **R-FA2.** Accepting a request writes it; execution progress writes it from each attempts
  receipts as they stream up `build.attempt.run`, before the caller is answered `Complete`.
- **R-FA3.** Gossip observations are context in the atlas and decide no accept or deny; they merge in only what the fabric-primary did not do itself, each under the
  receiver rules ordering (gossip.md §3.1: a frame older than the newest held for its key is ignored; a
  births `digest_seq`; a Builds attempt number), so a delayed event never moves it backward.
- **R-FA4.** `fabrics.atlas.get` returns it (Q167s read naming); `fabrics.get` is unchanged.
- **R-FA5.** Both directions are accepted: a `node.start` sent the moment the nodes `node.stop` streams
  `Complete` (passes today, through the stop receipt) and a `node.stop` sent the moment its `node.start`
  streams `Complete` (red today, `a_stop_of_another_meshs_node_sent_the_moment_its_start_completes_is_accepted`).

## Open

- Its page: node-identity-and-state.md beside the `DigestView` text, or build.md §4 accept or deny; the exact
  text goes to Luke before landing.
- The existing `FabricState` (`fabric_state.rs`, the fabrics pending / ready-for-traffic state) keeps
  its name.
