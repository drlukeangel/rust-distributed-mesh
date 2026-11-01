# i151 proposal: one `Resend` for every Node RPC call, and the `on_node_investigated` hook

Status: ruled by Luke 2026-11-01 (Q311, Q312). A new initiative, i151: nothing is broken today, so it
starts after i150 (rdm#245) lands and never rides inside i150.
Origin: Luke, 2026-11-01: "we shouldn't have multiple ways to do the same thing; timelimit should be
inside resend; resend should have attempts"; `Budget` becomes `TimeLimit`, `Owed` becomes `Resend`
(CLAUDE.md naming canon, a63f75756); Rafka's broker routing needs a notice when a node is found down.

## Rulings

- **Q311 (Luke):** one `Resend { attempts, time_limit }` on every call; **3 attempts**; **one rule for every
  caller**: a call whose attempts end unanswered starts node investigation on that birth, the
  mesh-primary itself or, for any other caller, the mesh-primary it reports the birth to; the lead picks
  the time limit (§3).
- **Q312 (Luke):** yes to the hook, `on_node_investigated` (§6).

## 1. What is built today

A call is "reach the node, send, get its answer, try again". Today that is five words (`Budget`,
`Overall`, `Split`, `Stream`, `Owed`), one flag (`once`) and eight loops:

| # | Where | What it repeats or bounds |
|---|---|---|
| 1 | `rdm-transport/src/client.rs:33` `Budget` | four ways to bound one call's time: `Overall(d)` (dial, send and reply in one deadline; the default, 10 s), `Split { send, reply }`, `Stream { send }` (no reply limit), `Owed` (no limit at all) |
| 2 | `client.rs:80` `CallOptions.once` | "asked once": a request whose stream is lost is not resent |
| 3 | `client.rs:1302` attempt loop | resends an unsent or unacked request on the resend backoff; re-dials when no connection is held; checks `receiver_gone()` between attempts (`client.rs:1323`) |
| 4 | `client.rs:877` | an `Owed` request whose receiver this view does not hold yet: reads the view again on the backoff |
| 5 | `client.rs:1495` | acked, response incomplete, stream gone: keeps a connection so the responder can resend |
| 6 | carried attempts (`client.rs` ~1399, `RoutePolicy`) | the next attempt through a carrier (Forward) |
| 7 | `rdm-admin/src/node_config.rs:479-560` `owed()` | a workflow loop on top of #3: re-sends to the primary the view names next on `RejectedNotPrimary` or no reply |
| 8 | `rdm-admin/src/admin/recovery.rs:750` | another workflow loop with its own pacing |

The responder (`delivery.rs:1082`, `server.rs:771`) resends its response until the caller acks: the
same rule in the other direction.

Non-test call sites: `Overall` 17, `Owed` 14, `Split` 11, `Stream` 16 (rdm-admin investigate.rs 6,
node_config.rs 4, node_commands.rs 3, status_rpc.rs 2, status_declare.rs 2, offline.rs 2,
fabric_handover.rs 2, recovery.rs 2, storage_change.rs, reenter.rs, build_op.rs; rdm-node node.rs 2,
topology_read.rs, join.rs; rdm-client build_stream.rs 2; rdm-transport stream.rs, forward.rs; rdm-test
probe 2, rig, conformance; demo replica.rs, consumer_example.rs).

## 2. The one mechanism

```rust
/// How a call is resent: how many attempts it makes and how long each attempt may take.
pub struct Resend {
    /// Attempts in all, the first send included: 3.
    pub attempts: u8,
    /// How long one attempt may take.
    pub time_limit: TimeLimit,
}

/// How long one attempt may take: dial, send and, for a unary op, its reply.
pub struct TimeLimit(pub Duration);
```

- `CallOptions.budget` and `CallOptions.once` are replaced by `CallOptions.resend`.
- An attempt is one send of the request: direct, or through a carrier when the route policy names one
  (#6), each counted. A receiver this view does not hold yet ends that attempt `NotSent` (#4 removed).
- Between attempts the call waits on the resend backoff (first at the publish cadence, doubling with
  jitter) or until the resolver changes, and checks `receiver_gone()` as today.
- A stream op's frames after its first live as long as its connection: a property of the op, not a
  time limit, so `Stream` needs no variant. `Split`'s separate reply limit becomes the attempt's one
  `TimeLimit`.
- When the attempts are spent, the call ends to its owner by name with the last attempt's outcome
  (`NotSent`, `Indeterminate`) and starts node investigation (§4).
- The owner never loops: #7 and #8 are removed. A command for "the mesh-primary of mesh M" resolves its
  receiver at each attempt from the view (the route policy already chooses per attempt).
- The response direction resends under the call's `Resend`.

## 3. The time limit (lead's pick, Q311)

One rule: **an attempt's `TimeLimit` is how long the receiver takes to answer, plus 10 s for the
transport.**

- Every call whose handler answers at once (a status read, a declaration, a config apply): **10 s**, the
  default today (`client.rs:116`).
- A call whose handler waits on its own bound before answering takes that bound plus 10 s:
  `node.connections.delete { All }` and node.stop wait `RDM_CONNECTIONS_DELETE_DEADLINE_MS` (5 s
  default), so 15 s; a Build step call waits its step's own bound.
- A stream op: 10 s to dial, send and receive its first frame; its later frames live as long as its
  connection.

With 3 attempts on the backoff (`c`, then `2c`; `c` the publish cadence, 2 s in production), an
unanswered 10 s call ends in about 36 s, inside one staleness floor (30 s) plus a cadence.

## 4. When the attempts end: node investigation (one rule, Q311)

A call whose attempts end unanswered starts node investigation on its receiving birth:

- the caller is that node's mesh-primary: it starts the investigation itself;
- any other caller (a node, another admin, a data-plane call): it reports the birth to that node's
  mesh-primary, which starts it. One new Status request on the upward path declarations use, carrying
  `{ node_id, incarnation }`; the mesh-primary answers it at once and investigates once per birth.

A data-plane call is no different from a command. Investigation itself is unchanged
(node-investigation.md: connections index, runtime inspect, tickle, tickle through peers, one verdict).

## 5. Page text (exact; for Luke's yes on the build)

node-rpc.md §43, title and first paragraph. Old:

> ## 43. Owed messages: delivery is the runtime's
>
> Retry automatically in Node RPC. Send immediately. Without an ack, resend the same message with the
> same delivery id: first retry at the publish cadence, then exponential backoff with jitter, capped at
> the staleness floor; one attempt in flight per message. [...] Retry stops on the ack, on the
> receiver's re-creation or accepted removal, or on explicit cancellation or retargeting by the owning
> workflow; never on a timer, never on absence from a view, never on a newer message of its kind.

New:

> ## 43. Resend: delivery is the runtime's
>
> Every call carries one `Resend`: three attempts, the first send included, and the `TimeLimit` of each
> attempt (dial, send and, for a unary op, the reply). Send immediately. Without an ack within the time
> limit, resend the same message with the same delivery id: the next attempt at the publish cadence,
> then doubling with jitter; one attempt in flight per message. [...] Resending stops on the ack, on the
> receiver's re-creation or accepted removal, on explicit cancellation by the owning workflow, or when
> the attempts are spent: the call then ends to its owner by name, with its last attempt's outcome, and
> starts node investigation on its receiving birth ([node investigation](lifecycles/node-investigation.md)).
> A call's receiver is resolved again at each attempt; its owner never resends it.

(the elided `[...]` sentences on duplicate suppression stay.)

node-investigation.md:16-18. Old:

> A node that does not answer a command is silent, whatever it gossips (Q182): an owed command it
> leaves unanswered past the staleness floor starts node investigation on its birth as silence does,

New:

> A node that does not answer a call is silent, whatever it gossips (Q182): a call whose attempts end
> unanswered starts node investigation on its birth as silence does (the caller, when it is the node's
> mesh-primary; otherwise the mesh-primary the caller reports the birth to),

The word `owed` (14 architecture pages) is replaced where it names this mechanism ("owed until acked"
becomes "resent until acked").

## 6. The hook: `on_node_investigated` (Q312)

fabric-node-hooks.md, a new row in the hooks table:

> | [`on_node_investigated`](#on_node_investigated) | each mesh-primary | after it records a node investigation's verdict on a birth of its mesh | nothing; it is a notice |

and a new section:

> ## on_node_investigated
>
> Once it records a node investigation's verdict ([node investigation](lifecycles/node-investigation.md)),
> the mesh-primary calls `on_node_investigated` once, with `NodeInvestigatedCtx { fabric_id, mesh_id,
> node_id, incarnation, path, name, verdict, next, context }`, on a task of its own. `verdict` is the
> investigation's (`reached`, `exited`, `os-stopped`, `running`, `unproven`); `next` is what RDM does with
> the birth: `rejoin` (commanded back through node.mesh.join) or `recreate` (re-created at the same
> path.name with a new node.id). Rafka takes the broker out of its topics' write sets on `recreate`. The
> notice gates nothing in RDM.

## 7. RDM build

1. `rdm-transport`: `Resend`, `TimeLimit`; `Budget`, `once` removed; the attempt loop counts attempts;
   #4 folded in; responses resend under the call's `Resend`. Cells: an unanswered call ends after 3
   attempts by name; a receiver gone between attempts ends `ReceiverGone`; a primary change inside the
   attempts is followed; a stream lives past its time limit while its connection does.
2. `rdm-admin`: `node_config::owed` and `recovery.rs`'s loop removed; every call site given its
   `Resend` (§3); a mesh-primary's unanswered call starts node investigation; the upward report and its
   handler; `on_node_investigated` called after each verdict, with its ctx in `rdm_admin::wiring`.
3. Pages: §43, node-investigation.md, fabric-node-hooks.md, the `owed` sweep; rdm-docgen regenerated.
4. A Jaeger flame of a call that ends unanswered, the investigation it starts and the notice, at the tip.

## 8. Rafka build

- Rafka's 33 `Budget` uses (write_hooks.rs 7, broker_data_op.rs 6, writer_target_op.rs 3, context.rs 2,
  topic_traffic_close_op.rs 2, broker_resync_step_op.rs 2, wal_maint_op.rs 1, dial_bound.rs 1, tests)
  move to `Resend` with the RDM pin. Its own retry around `Budget::Split { once: true }` in
  writer-ingress (`context.rs` ~80-313, `write_hooks.rs` ~818) is removed: the one `Resend` is its
  retry, and a call that ends unanswered is answered to the writer by name.
- node-admin implements `on_node_investigated`: on `next = recreate` it writes the broker's
  `write -> none` rows (replica-routing.md §6 step 1), taking it out of its topics' write sets; the
  broker's re-sync stays on `on_node_ready` of its new birth.
