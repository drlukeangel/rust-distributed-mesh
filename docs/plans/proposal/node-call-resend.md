# Proposal: one `Resend` for every Node RPC call

Status: proposal for Luke's ruling (2026-11-01). Built in i150 (rdm#245) once ruled.
Origin: Luke, 2026-11-01: "we shouldn't have multiple ways to do the same thing; timelimit should be
inside resend; resend should have attempts, I would say no more than three"; `Budget` becomes
`TimeLimit`, `Owed` becomes `Resend` (CLAUDE.md naming canon, a63f75756).

## 1. What is built today

A call is "reach the node, send, get its answer, try again". Today that is spread over five words
(`Budget`, `Overall`, `Split`, `Stream`, `Owed`), one flag (`once`) and eight loops:

| # | Where | What it repeats or bounds |
|---|---|---|
| 1 | `rdm-transport/src/client.rs:33` `Budget` | four ways to bound one call's time: `Overall(d)` (dial, send and reply in one deadline; the default, 10 s), `Split { send, reply }`, `Stream { send }` (no reply limit), `Owed` (no limit at all) |
| 2 | `client.rs:80` `CallOptions.once` | "asked once": a request whose stream is lost is not resent |
| 3 | `client.rs:1302` attempt loop | resends an unsent or unacked request on the resend backoff; re-dials when no connection is held; checks `receiver_gone()` between attempts (`client.rs:1323`) |
| 4 | `client.rs:877` | an `Owed` request whose receiver this view does not hold yet: reads the view again on the backoff |
| 5 | `client.rs:1495` | acked, response incomplete, stream gone: keeps a connection so the responder can resend |
| 6 | carried attempts (`client.rs` ~1399, `RoutePolicy`) | the next attempt through a carrier (Forward) |
| 7 | `rdm-admin/src/node_config.rs:479-560` `owed()` | a workflow loop on top of #3: re-sends to the primary the view names next on `RejectedNotPrimary` or no reply, on its own backoff |
| 8 | `rdm-admin/src/admin/recovery.rs:750` | another workflow loop with its own pacing |

The responder side (`delivery.rs:1082`, `server.rs:771`) resends its response until the caller acks:
the same rule in the other direction.

Call sites (non-test): `Overall` 17, `Owed` 14, `Split` 11, `Stream` 16, in rdm-admin (investigate.rs 6,
node_config.rs 4, node_commands.rs 3, status_rpc.rs 2, status_declare.rs 2, offline.rs 2,
fabric_handover.rs 2, recovery.rs 2, storage_change.rs, reenter.rs, build_op.rs), rdm-node (node.rs 2,
topology_read.rs, join.rs), rdm-client (build_stream.rs 2), rdm-transport (stream.rs, forward.rs),
rdm-test (probe 2, rig, conformance), demo (replica.rs, consumer_example.rs).

## 2. The one mechanism

```rust
/// How a call is resent: how many attempts it makes and how long each attempt may take.
pub struct Resend {
    /// Attempts in all, the first send included: 1 to 3.
    pub attempts: u8,
    /// How long one attempt may take: dial, send and, for a unary op, its reply.
    pub time_limit: TimeLimit,
}

/// How long one attempt may take.
pub struct TimeLimit(pub Duration);
```

- `CallOptions.budget` and `CallOptions.once` are replaced by `CallOptions.resend`.
- An attempt is one send of the request: direct, or through a carrier when the route policy names one
  (#6), each counted. A receiver this view does not hold yet ends that attempt `NotSent` (#4 removed).
- Between attempts the call waits on the resend backoff (first at the publish cadence, doubling with
  jitter) or until the resolver changes, and checks `receiver_gone()` as today.
- A stream op's reply after its first frame lives as long as its connection: that is the op's own
  property, not a time limit, so `Stream` needs no variant. `Split`'s separate reply limit becomes the
  one `TimeLimit` of the attempt.
- When the attempts are spent, the call ends to its owner by name with the last attempt's outcome
  (`NotSent`, `Indeterminate`), and (§4) starts node investigation where the caller may.
- The owner never loops: #7 and #8 are removed. A command sent to "the mesh-primary of mesh M"
  resolves its receiver at each attempt from the view (the route policy already chooses per attempt),
  so a primary change is followed inside the attempts, not by a second loop.
- The response direction uses the same `Resend` the call carries.

## 3. Attempts: 2 or 3

The backoff starts at the publish cadence `c` and doubles: 2 attempts end after about `c` (2 s in
production, 0.5 s in tests), 3 after about `3c` (6 s, 1.5 s). Both end long before the staleness
floor (30 s). A resend to a node that does not answer proves nothing new; node investigation (inspect,
tickle, tickle through peers) does. With investigation started when the attempts end (§4), 2 is
enough. Open choice for Luke: **2 or 3**, one constant for every call.

## 4. When the attempts end: node investigation

node-investigation.md:16-18 (Q182) already says an owed command left unanswered past the staleness
floor starts node investigation on its birth. With `Resend` the call ends long before the floor, so
the end of the call is the event. Only a mesh-primary investigates, and only its own mesh's nodes
(node-investigation.md, Q111). Open choice for Luke:

- **A (dev recommendation):** one rule for every caller. A call that ends unanswered: the target's
  mesh-primary starts node investigation on that birth itself; any other caller sends the failed birth
  up to that mesh-primary (one new Status request on the upward path declarations use), which does the
  same. A data-plane call is no different from a command.
- **B:** only the mesh-primary's own calls start investigation; every other caller's failure stays the
  `Failed` connection fact it already writes (`rdm-node/src/connections_writer.rs:662`), read at the
  next investigation.

## 5. Page text (exact; for Luke's yes)

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
> Every call carries one `Resend`: its attempts (the first send included, at most N) and the
> `TimeLimit` of each attempt (dial, send and, for a unary op, the reply). Send immediately. Without an
> ack within the time limit, resend the same message with the same delivery id: the next attempt at the
> publish cadence, then doubling with jitter; one attempt in flight per message. [...] Resending stops
> on the ack, on the receiver's re-creation or accepted removal, on explicit cancellation by the owning
> workflow, or when the attempts are spent: the call then ends to its owner by name, with its last
> attempt's outcome, and starts node investigation as node-investigation.md says. A call's receiver is
> resolved again at each attempt; its owner never resends it.

(`N` = the ruled 2 or 3; the elided `[...]` sentences on duplicate suppression stay.)

node-investigation.md:16-18. Old:

> A node that does not answer a command is silent, whatever it gossips (Q182): an owed command it
> leaves unanswered past the staleness floor starts node investigation on its birth as silence does,

New (arm A):

> A node that does not answer a call is silent, whatever it gossips (Q182): a call whose attempts end
> unanswered starts node investigation on its birth as silence does (the caller, when it is the node's
> mesh-primary; otherwise the mesh-primary the caller reports the birth to),

The word `owed` (14 architecture pages) is replaced where it names this mechanism ("owed until acked"
becomes "resent until acked"); the sweep is listed with the build.

## 6. RDM build

1. `rdm-transport`: `Resend`, `TimeLimit`; `Budget`, `once` removed; the attempt loop counts attempts;
   #4 folded into it; responses resend under the call's `Resend`. Cells: an unanswered call ends after
   N attempts by name; a receiver gone between attempts ends `ReceiverGone`; a primary change inside the
   attempts is followed; a stream lives past its time limit while its connection does.
2. `rdm-admin`: `node_config::owed` and `recovery.rs`'s loop removed; every call site given its
   `Resend`; the end of a mesh-primary's call raises node investigation (arm A also: the upward report).
3. Pages: §43, node-investigation.md, the `owed` sweep; rdm-docgen regenerated.
4. A Jaeger flame of a call that ends unanswered and the investigation it starts, at the tip.

## 7. Rafka build

Rafka's 33 `Budget` uses (write_hooks.rs 7, broker_data_op.rs 6, writer_target_op.rs 3, context.rs 2,
topic_traffic_close_op.rs 2, broker_resync_step_op.rs 2, wal_maint_op.rs 1, dial_bound.rs 1, tests)
move to `Resend` with the RDM pin. Its own retry around `Budget::Split { once: true }` in
writer-ingress (`context.rs` ~80-313, `write_hooks.rs` ~818) is removed: the one `Resend` is its
retry, and a call that ends unanswered is answered to the writer by name.
