# CLAUDE.md — RDM agent instructions

## Nothing lands without these two (Luke, 2026-11-08)

1. **An adversarial code review of the work, once, on the lead lane.** Units land into the lead lane's branch, not
   one by one onto main. When the batch is in, the lead reads the merged diff against what each ruling says must be
   true when it is done, and checks that in the code itself. For example, for a "one copy" ruling, every copy of
   that state is shown gone or shown to be the one. The lead then runs one review or audit there (the `code-review`
   or `code-audit` skill) on the lead lane against origin/main, never one per branch, and reads every finding. Each
   finding is fixed on the lead lane or answered before it goes to main. A green gate is not the review, and neither
   is a seat's report or a diff summary.
2. **Jaeger flames that prove it works.** Every behaviour fix carries a flame: the trace link from an exported run
   (http://192.168.68.99:16687/trace/<id>) and the span lines that show the fix working. The lead reads it before
   the landing. Local span files and a green cell are not the proof. A gossip or mesh change carries a flame from
   before the change and one from after it. When telemetry is down, the unit waits; it does not land without its
   flames.

RDM (rust-distributed-mesh) is the generic Mesh product: the fabric, mesh and node runtime
(node-admin, Node RPC, gossip, Build, deployment providers) that rafka-v2 pins as a dependency and
consumes. Everything rafka-specific (gateways, brokers, orgs, topics, ACLs) lives in rafka-v2 and
is never built here.

## The spec and the record

- **The spec is `docs/architecture/`** and its `lifecycles/` pages. A page is the contract; code
  conforms to the page, never the other way round. `docs/architecture/fabric-mesh-node-lifecycles.md`
  is the index. No agent changes an architecture page without Luke's yes on the exact old and new
  text.
- **The record is `docs/plans/i143-prd.md`**, the only source of truth for remaining work: every
  state or remaining-work update goes in its row. Rulings are in `docs/plans/rulings/`, proposals
  in `docs/plans/proposal/`. Issues and the i143 milestone are tracked in this repo, `drlukeangel/rust-distributed-mesh`.
- **The canon in `/home/admin/rafka-v2/CLAUDE.md` applies here in full**: no change to gossip or the
  mesh without Luke's permission and a Jaeger flame; fix the root, never a coping layer; commands go
  down (fabric-primary → mesh-primary → node); the vocabulary bans; the durable-format rule and its
  commit marker; no Claude attribution in commit messages.

## Vocabulary

Name things what they are. Never make up a word, and never use a name that hides what the thing
does or calls it something it is not. Rule of thumb: if you have to describe what something does to
understand it, its name uses a word from that description (`TimeLimit`, not `Budget`; `Send`, not
`Owed` or `Resend`; `copy`, not `snapshot`).

The primaries are the fabric-primary, each mesh's mesh-primary and each node type's node-type primary in its mesh; a primary changes by `node.primary.update` or, unheard past the staleness floor, by the leadership election. `id` / `index` / `name` are three things; `key` is none of them. `target`, never fence. `revision`,
never epoch. Connections are deleted (`node.connections.delete`), never drained. `forward` is the
control plane's word (Forward 0x1A, `build.section.run`); `proxy` is the data plane's (a `Proxy`
connection). Banned everywhere: `relay`, `cluster`, `tailer`, `sidecar`, `child mesh`, `viewer`,
`operator`, `sysadmin`, `book` / `books` (it is a view: the topology view, `DigestView`), `tree`, `partition`, `reborn`, `drain` / `draining` / `drained` (the state is
`connections-deleting`, the bound `RDM_CONNECTIONS_DELETE_DEADLINE_MS`: [node-connections-delete](docs/architecture/lifecycles/node-connections-delete.md)),
`park` / `parked`, `fence`, `epoch`, `relaunch`, `departed`, `retire` / `retired`, `seat` / `seated` / `unseat`, `holder` (of a primary; a name's holder in `NameHeld` is another sense), `authority` / `authorize` / `authoritative`, `cohort` (it is `node_type`: the nodes of one type in one mesh, whose primary is the node-type primary; `cohort_primary` is `node_type_primary`), `lane`, `cut-off` / `cut off`, `committed seat`, `exit proof` / `proven exit`, `held-running`, `hung hold`, `LeadershipView` / leadership `resolver`, `sticky`, `roll` / `rolls` / `rolled` / `rolling` (the operation is a node's executable replacement, a Build; `docs/architecture/build-deployment.md`), `climb` / `climbs` / `climbed` / `climbing`, `recipe` / `recipes` (the Build's ordered list is steps, one item a step, Q211), `round` / `rounds` (the fabric has states, pending and ready; a state is set fabric-wide and is set when every member has checked in; the whole movement from pending to ready is the fabric's change) (the name check's set,
`scripts/name-check.py`). A node is stopped then started (same node.id and
incarnation; node.restart is removed, Q183) or re-born (same path and name, new node.id); a mesh is never reborn.

## Rulings

A ruling is asked one at a time, with a recommendation, and is produced with the `ruling-packet`
skill (`/home/admin/rafka-v2/.claude/skills/ruling-packet`): spec, issue, flame, code, delta, arms. A
packet without a section 5 delta is returned, not reviewed. Every question and ruling carries a number.

## Lanes, gate, landing

- A lane is a folder with a source tree: `/home/admin/rdm-lanes/<n>`, numbered `1`–`7`. Reuse it;
  never a new worktree, never `.claude/worktrees`, never delete one. `/home/admin/rust-distributed-mesh`
  holds `main` and is not a lane.
- **Never stop a working agent (Luke 2026-11-09: "you never kill a fuckknig anget").** A mistake the lead or a
  seat made is corrected forward: a `lane-note.sh`, the next unit's dispatch, the right type next time. Only a
  seat that has handed off and is finished is stopped; one at work is never TaskStopped, whatever went wrong.
- Every unit has one issue in this repo, one lane, one seat and one agent at a time, and runs through the
  lane verbs in `scripts/` (adapted from rafka-v2's `docs/eng/testing-lane-scripts.md`), never by hand:
  `lane-status.sh [lane…]` (free?); `lane-prep.sh <lane> <branch>` (claim on a fresh branch from
  `origin/main`); `lane-dispatch.sh --lane <n> --issue <N> --name … --title … --design <file>` (writes
  the plan and the seat's agent type `i143-<issue>`, comments the issue); the Agent tool with
  `subagent_type` = its `SEAT TYPE` line, launched only after the dispatch returns (a `NEW` type after
  Claude Code's new-agent-types notice); `lane-note.sh <lane> <file|->` (the one way an update reaches
  a seat); `post-land-gate.sh <lane>` (proves the tip in the lane; its GREEN line is the landing's
  proof); `lane-land.sh <lane> <branch> <issue> <close-comment-file>` (refuses a tip without a standing
  GREEN, never gates, lands, closes the issue with `@SHA@` filled). A lane's plan, inbox and seat are under `$HOME/lane-queue/rdm/<n>/`, never in its tree.
- Never export `CARGO_TARGET_DIR`; never `cargo test -p` in a lane (each `-p` shape rebuilds
  admin-core). One stem: `scripts/cells.sh`; the whole suite: `scripts/rdm-gate-at.sh <lane>
  fast|release`; landing: `rebase-gate-land.sh <lane> <branch>` (rebase, name check, gate,
  fast-forward push). Every script, with the helpers it calls (`name-check.py`, `timings_row.py`),
  lives in this repo's one `scripts/` folder: the land script runs main's copy, the gate runs the
  lane's own copy, and `cells.sh` builds exactly what the gate builds. The two files in
  `/home/admin/lane-queue/i143-lead/` are four-line shims, not sources: `rebase-gate-land.sh` runs
  origin/main's `scripts/rebase-gate-land.sh` and `rdm-gate-at.sh` runs the lane's
  `scripts/rdm-gate-at.sh`; a change to either goes in `scripts/`. Gate state (the red-tree list,
  the timings ledger) and the gate logs are in `/home/admin/rust-distributed-mesh-deploy/`.
- A red gate is read, never re-rolled. A landing that adds a retired word to an identifier or a
  file name, or an issue-named test file, is refused by the name check.
- Every landing that changes a page's statement lands the page text and the code together.

## Env vars

Every env var a node binary reads, with its default: [docs/eng/env-vars.md](docs/eng/env-vars.md). No other configuration mechanism exists. Never export `CARGO_TARGET_DIR` by hand in a lane (see Lanes).


---
