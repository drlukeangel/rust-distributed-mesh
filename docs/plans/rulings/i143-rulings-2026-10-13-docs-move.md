# R-DM1 — the spec and the i143 record live in RDM (Luke 2026-10-13, "move")

**Ruling (Luke 2026-10-13, asked whether the lifecycle and architecture pages should move into RDM "so the agents don't get confused with rafka": "move").**
- The 13 architecture pages that specify RDM's code (node-rpc-envelope, node-identity-and-state, build, gossip, node-admin-lifecycle, fabric-authority, fabric-mesh-node-lifecycles, fabric-node-hooks, fabric-node-lifecycle-elections, fabric-certs, node-rpc, deployment-provider, connections) and the 27 lifecycle pages live in `docs/architecture/` here. The i143 PRD, design, parity and audit files, the rulings pages and the proposals live in `docs/plans/` here. The lead handover `next-agent.md` lives at the repository root.
- A move, never a copy: one source of truth. rafka-v2 keeps an index page (`docs/architecture/rdm.md`) and links by absolute path. GitHub issues and the i143 milestone stay in rafka-v2.
- The May docs tree of this repository (mesh-v1 plans, sprints, the admin-ui design canvas, soak evidence, the feature triples, the historical mesh.md) described a build that no longer exists and is deleted; the chaos-kit overview is kept.
- This reverses the docs half of the 2026-10-07 rule ("nothing of i143's record in the RDM repo"); the issues half stands.
- Landed: RDM `9527d75b` (move), `0bd29e30` (renames), `df378f7a` (CLAUDE.md, README, next-agent.md); rafka-v2 `77d91badc4`.

# R-FP1 — forward is the control plane's word, proxy the data plane's (Luke 2026-10-13)

**Ruling (Luke 2026-10-13: "so we use forward for management and proxy for data"; "if they're doing the same thing seems like we should call them the same").**
- **Forward**: a node hands a command on to the node that must execute it, keeps the stream for the answer, and never commands the final node itself. Two forms: a peer carrying one call verbatim to a node the caller cannot reach (the Forward op `0x1A`, answered `Forwarded { inner }`), and the fabric-primary handing a mesh its attempt of a Build (`build.attempt.run`, `0x20`) for the mesh-primary to execute on the mesh's nodes. A fabric-primary-originated command for a mesh's node is a forwarded command through its mesh-primary (CLAUDE.md, fabric-authority.md §1, node-primary-update.md). A mesh-primary healing its own mesh (R-BI7) forwards nothing.
- **Proxy**: a data-plane connection through one exact carrier, the `Proxy` kind of connections.md, with its own minted id; never a Node RPC call.
- They are two mechanisms (R-CR5: the control plane is never a connection), so two words; `connections.md` §1.1 states both.
- The Forward reply `Relayed` is `Forwarded` (relay is banned); the code rename is a row of e12.s75 (position unchanged, no format move).

# R-DW1 — the moved pages carry no retired word (Luke 2026-10-13, "do all it and the renames")

- `fence` → target (the envelope's first frame) or, where it meant a refusal, block / refuse by name; the Build dispatch refusal `Fenced { node, by }` is `SeatYielded { node, by }`.
- `epoch` → revision (a seat record), round (a connection's reconnect series: recovery round), `claim_revision` (a durable job's claim).
- `drain` → connections are deleted (`node.connections.delete`); a writer finishes its issued writes at its cut-off; the runtime refusal `Draining` is `ConnectionsDeleting { reason }` and node-rpc.md §35 is "Connections-deleting".
- `relay` names nothing; the one sentence that remains says the transport's relay servers are disabled.
- The code renames are rows of e12.s75. The env var `RDM_DRAIN_DEADLINE_MS` is a durable name and is not renamed by this ruling.

# R-TK1 — the tickle's lifeline is the node (Luke 2026-10-13, "why is exact birth an actor")

- A sequence diagram's participants are nodes and authorities; a qualifier (which node.id and incarnation a probe names) rides the arrow, never a lifeline. `node-tickle.md` draws Caller, Node, Peer 1, Peer 2; "exact birth" is gone from the page and the probe reads `ProbeNodeState (node_id, incarnation)`.

# R-LD1 — a docs-only tree lands as a fast-forward when the gate's red is main's (Luke 2026-10-13, "given what red? we didn't update any code")

- A tree that changes no code cannot turn a cell red. When the gate on such a tree is red, the red is main's: it is recorded as a story with its trace (e12.s83, the PrepareStorage name claim that never returns), the red tree stays in the red-trees ledger, and the docs branch is pushed to main as a fast-forward without a second gate.
- This is not a licence to re-roll a gate: a tree that changes code is gated once and a red is read, never re-gated.
