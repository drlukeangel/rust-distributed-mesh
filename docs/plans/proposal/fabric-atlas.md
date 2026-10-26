# FabricAtlas

**Origin:** Luke, 2026-10-26 (Q194), from Q189 (a start right after a cross-mesh stop refused
`node-not-live`). **State:** proposed; built after i149 lands. **Name:** `FabricAtlas`, read through
`fabrics.atlas.get`.

## The requirement (Lukes words)

**`FabricAtlas` combines command admission, execution progress and gossip observations, and is exposed
through `fabrics.atlas.get`.** It updates immediately when commands are admitted and before completion is
returned; delayed events cannot move it backward.

## What it replaces

The fabric-primary admits a Build from a projection rebuilt per request from the `DigestView` (the gossip
view and everything in flight) and the store rows (`control_plane.rs` admission, `boot.rs` `view_now`,
`projection.rs` `project_with`); a nodes `stopped` comes from the `DigestView` alone
(`projection.rs:370-393`), so a stop the fabric-primarys own Build recorded is not admissible state until
gossip echoes it (Q189). `DigestView` keeps its name and meaning (Luke, 2026-10-26: "digest-view is supposed
to be the gossip-view and everything that is inflight").

## Requirements

- **R-FA1.** The fabric-primary holds one `FabricAtlas`. Build admission for every change (Stop, Start,
  Delete, Reconcile, the rest) reads it, and nothing else.
- **R-FA2.** Command admission writes it at acceptance; execution progress writes it from each attempts
  receipts as they stream up `build.attempt.run`, before the caller is answered `Complete`.
- **R-FA3.** Gossip observations merge in only what the fabric-primary did not do itself, each under the
  receiver rules ordering (gossip.md §3.1: a frame older than the newest held for its key is ignored; a
  births `digest_seq`; a Builds attempt number), so a delayed event never moves it backward.
- **R-FA4.** `fabrics.atlas.get` returns it (Q167s read naming); `fabrics.get` is unchanged.
- **R-FA5.** First RED cell: a `node.start` of another meshs node sent the moment its `node.stop` streams
  `Complete` is admitted (Q189).

## Open

- Its page: node-identity-and-state.md beside the `DigestView` text, or build.md §4 admission; the exact
  text goes to Luke before landing.
- The existing `FabricState` (`fabric_state.rs`, the fabrics pending / ready-for-traffic state) keeps
  its name.
