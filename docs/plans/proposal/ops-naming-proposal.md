# Ops naming proposal (node-RPC)

**Archived.** Every rule here is stated in the architecture docs, which own it now: start at [the lifecycle index](../../architecture/fabric-mesh-node-lifecycles.md); rollout and acceptance live in [the i143 PRD](../i143-prd.md). This file is kept for history and is not edited.

Agreed with Luke in the ops-naming thread, 2026-10-10. This is a design proposal. Nothing is implemented.

## RDM has no REST (target)

RDM's only surface is node-RPC: the `node`, `mesh`, `fabric` and `build` op families and the typed objects over them. At the target, no RDM crate serves HTTP, depends on an HTTP server, or defines a route. Every `/api/...` path named in this document belongs to the **demo**, an application over the same typed objects that Rafka uses, and the demo may expose or drop routes without any change to RDM. Where this document says "demo: `/api/x`", read "the demo route this call replaces".

**Today is not the target.** `rafka-node-admin-core/src/http.rs` serves routes, the crate depends on `axum`, and `admin.rs`, four test files in that crate and the testkit's `admin_faults.rs` use it. Rollout step 0 below moves those to the demo. The no-REST CI guard turns on only after that step lands; until then, a PR that adds a *new* route to an RDM crate is refused, and the existing ones are a migration debt, not a precedent.

Build creation, execution, reads and deletion ride the additive `build` family on op `0x20` (R-ON8 = A′, 2026-10-10); the fabric-primary is its sole driver, and [the envelope](../../architecture/node-rpc-envelope.md#build-op-0x20-rdm) owns its schema and open codec questions.

## Summary

Node-RPC does everything. Every operation is an RPC call on a node-RPC op family, one family per noun: `node`, `mesh`, `fabric` and additive `build` (0x20). The remaining families split the `0x1B` status calls and the ad hoc words such as `drain-node`, `retire-node`, `shutdown-mesh` and `LeaveMesh`. Code calls typed objects such as `node.create(spec)`. Each object builds the RPC request, sends it, and gets back the typed reply. A change completes as an event named with the noun and a past-tense verb, such as `node.created`.

RDM provides the primitives and the transaction that runs them. Rafka composes its own workflows from those primitives and names them however it likes.

## How an op works on node-RPC

An op is a family, and its request enum's variants are its calls (`docs/architecture/node-rpc-envelope.md`, op ledger). So each noun becomes one op family:

```text
node.create(spec)                                  what code writes
  → NodeRequest::Create { spec }  on op "node"     what goes on the wire (fence: target node id + op)
  ← NodeReply::Created { id }                      the typed reply
  ⇢ node.created                                   the event, published when the change has happened
```

- **The ops still exist.** The op code plus its request and reply variants is the wire contract. Code never builds a request by hand. It calls the typed object, and the object builds the op.
- **One shared core.** The Build runner, the tests and Rafka all call the same `node.create`. Every surface is a thin caller over it, the shape CLAUDE.md asks for with `create_role_binding_core`.
- **One canonical name, derived everywhere.** `node.create` is the only name anyone writes. Each surface renders it mechanically:
  - the Rust variants: `NodeRequest::Create` and `NodeReply::Created`
  - the Build step key: `node.create:<path.name>`
  - the span: `rdm.node_admin.node.create.via-…`, which is already the span grammar

  Today each surface was named by hand, and that is where the drift came from. Mesh shutdown has six names today: `shutdown-mesh`, `leave-mesh`/`LeaveMesh`, `MeshLeave`, `MeshLeft`, `mesh-leaving` and `mesh-leave.md`. Node removal has four: `retire-node`, `delete-node`, "decommission" and `DELETE ?permanent=true`.
- **Dots are allowed in op names.** `node.create:mesh1.rpc.1` works. The Build step key is an opaque `String` (`BuildStepReceipt.operation`, `node-admin/tests/functional/build_recovery.rs::an_all_admin_restart_recovers_the_build_the_fabric_names_and_continues_it`). The colon separates the op from the target, so dots before it don't collide with the dotted path.name after it.

## Naming rules

- **Command and completion:** a command is `noun.verb` and completes as `noun.verb-past`. For example, `node.create` completes as `node.created` and `node.drain` as `node.drained`.
- **Every step is visible.** A workflow that contains `start` or `stop` publishes every event those workflows publish, each with its own span (`rdm.node_admin.node.<verb>.via-…`) and its own Build step receipt. Composition changes where a step is defined, never whether it is seen.
- **A long-running change publishes a start event and an end event.** For example, `node.draining` is published when the drain starts and `node.drained` when the in-flight work is finished. Workflows wait on the end event.
- **The reply stream is the execution interface.** Spans are for people; events are what the system acts on. Every call that changes something — a single call or a workflow — is a streamed reply (the envelope already has this: `ReplySink`, `Started`, then frames). Each step event (`node.draining`, `node.drained`, …) is a reply frame on that stream, delivered to the caller by the node or mesh that performed the step, so the caller sees what completed, where it stopped and why, without a Build. Build adds durability, not visibility. The caller waits on the stream, never on gossip. Gossip carries only what *peers* need to act on (`NodeRestarting`, `Leaving`, departures); it is not how the caller learns a step completed. Spans mirror the same events.
- **A failure names its step.** A success event is emitted only after its step completes. An application failure ends the stream with a typed terminal frame that names the step, and nothing downstream of it is emitted:
  ```text
  node.start
    ← Started
    ← Failed { step: "node.join", reason: "…" }     no node.joined, no node.started
  ```
  The caller knows the call was accepted and the join failed. Every Build step receipt is `Complete` or `Failed { step, reason }`; those are the only durable step outcomes.
- **A broken stream is not a failed step.** Missing frames prove nothing about the step in flight. The *call* ends `NotSent` (before its commit cut) or `Indeterminate` (after it), under the existing commit-cut rule, and the caller treats it as exactly that. For a Build-backed call, the caller resubmits by `build_id` to the fabric-primary: a completed Build answers `AlreadyApplied` with its terminal frame, a running one with a live holder returns that holder's stream, and a failed one, or one whose holder is proven lost, gets its next attempt. `build.get` is a read for re-attaching, never a resume. A step the receipts show `Complete` is never replayed; an `Indeterminate` call whose step is idempotent by natural key is settled by re-issuing it and reading `AlreadyApplied`, never by assuming either outcome.
- **A read is `noun.<thing>.get`.** The reply comes back immediately on the same call, and no event is published. A read changes nothing, so there is no fact to announce, and nobody else needs to hear that someone looked. Events are kept for changes only, so the event stream is a log of state changes. This replaces the `.send` / `.sent` pairs from the first draft.
- **No "birth" or "re-birth" in RDM.** A new node is `node.created`. Birth and re-birth are Rafka's names for its own workflows.
- **"State" is not used.** The word is too loaded. What RDM stores about a node is its **meta**. What the app stores on a node is its **config**.

## Layer split: RDM primitives, Rafka workflows

RDM provides two things:
- **The primitives:** node create, start, stop, update, delete, drain, connections and config. Restart is a composed workflow (`stop` then `start`), not a primitive.
- **The transaction that runs them:** the Build, with its attempts, step receipts and resume after an admin dies (`rafka-node-admin-core/src/build_state.rs`). A Build is **durable, resumable execution**: it resumes from its receipts and completes or fails typed. It does not roll back. A recipe that needs a rollback path writes it as its own steps, as replace does.

Rafka composes workflows from those primitives, submits each workflow as one Build, and names it however it likes: birth, re-birth, replace, broker resync. RDM never knows what a broker or an org is, and it never knows that four steps add up to a replace.

The Build is what keeps a Rafka recipe safe. As four separate calls, a crash after `node[1].update(name + ".old")` would leave a renamed old node with no successor, and nothing would know to finish the job. As one Build with four steps, RDM finishes it or resumes it.

## Node data: meta, config and exe

| | what it is | how it changes |
|---|---|---|
| **meta** | everything RDM stores about the node, including `otel_debug` | `node.update` with the full entity, as a blind put |
| **config** | the app's own key/values on the node: tags, `admin_api_base`, anything else. Opaque to RDM | `node.config.update`, a single call through the fabric-primary |
| **exe** | bin, sha, cpu/ram budgets, cpu_set | only through a new node (the replace recipe) |

- **Meta has three groups, and a put touches only one.** *RDM-owned, never copied and never put:* `node.id`, `incarnation`, `endpoint`, `transport_addr`, `exe`, the runtime handle, and `status`. *RDM-owned, set by the create spec and never copied:* `path`, `name`, `mesh`, `kind`. *Caller-owned, copied on recover and replace and settable by put:* `otel_debug` and whatever else is added here. `node.update` is a blind put of the full entity, but a body whose RDM-owned fields differ from what RDM holds is refused typed (`MetaFieldIsRdmOwned { field }`); `name` may differ only with no traffic: the node is stopped, or it is a successor born `<name>.new` that nothing has routed to. Status never moves through a put: it moves only through the actions and workflows, each with the typed transition refusals. The recover and replace recipes' `node[2].update` carries the caller-owned group only; the successor's path comes from its own create spec and its final name from the replace recipe's rename, never from the predecessor's meta.
- **No partial update anywhere.** node-admin doesn't know the values, so it can't merge a partial body. A partial update would force node-admin to read the row, merge it and write it back, which is the read-modify-write CLAUDE.md bans. So the caller gets the full entity and sends the full entity back, and node-admin does a blind put.
- **Config is a field of the node record.** The node record (meta and config) is written to the nodes store: in Rafka its nodes topic, in RDM's default store memory. The mesh-primary of the node's mesh is the only writer of that mesh's rows; the fabric-primary writes a row only as the acting primary of a mesh that has no primary yet, the first admin it creates. Writers never share a key: path is `<mesh>.<kind>`, so every row and every path.name index key is mesh-scoped by construction. The mesh-primary holds the whole record of every live node of its mesh and writes the whole row from that held record, a blind put; it never reads the store on the write path. The store is read only to rebuild, after a seat change or a recovery, before anything is written, each mesh-primary from its own mesh's rows. The node's digest carries the live copy (the existing `extra` map, renamed `config`), so every node holds every node's config through the topology, which stays ephemeral. RDM never reads config to decide anything. It is a property bag of `name: value` pairs: a name is unique by the map's own shape and nothing checks it, so a body that names a key twice keeps the last value, `admin_api_base` included; last one wins. It is capped at fixed numbers, 32 keys and 2048 bytes (`MAX_CONFIG_KEYS`, `MAX_CONFIG_BYTES` in `rafka-mesh-entity/src/digest.rs`), refused by name past either with the measured size and the bound (`ConfigOverCap`), because it rides every heartbeat. The numbers are set, not computed: the usable gossip message is 4032 bytes, and the code comment above the constants states the breakdown as a share of it (config about half, the digest's own fields the rest), so a change to the frame limit is made against that comment, never by a percentage at runtime. A mesh's primary claims the mesh's name by insert-and-fail within its path and writes the mesh row both when the mesh is created and during the fabric's state-commit, then every status; the fabric-primary does the same for the fabric's name and row, at Day 0 and at state-commit. At Day 0 the fabric-primary creates the fabric row and its own mesh's row, holding both seats.
- **A config update is a single call, not a Build.** Caller → the node's mesh-primary (Forward `0x1A` when not directly reachable), which writes the record → the node (`ApplyNodeConfig`), which applies it and carries it in its next digest → `node.config.updated` back. Row first, mesh second. It is never refused because a Build is running: every change to a node record goes through its mesh-primary's one held record, one at a time, so any interleaving with a Build is a serial order of the two. Two topology changes, by contrast, never run together: one Build is in flight, and a second is refused `BuildInProgress { build_id }` until it completes (`rafka-node-admin-core/src/http.rs-135`).
- **A rename is the same write.** `node.update` with `name` changed is caller → the node's mesh-primary, which validates from what it already holds: the exact birth against its record; RDM-owned fields unchanged except `name`; the name changes only with no traffic, that is the held status is stopped, or the node's current name is `<name>.new` and nothing has ever routed to it. It then claims the new name by insert-and-fail inside the store adapter, so Rafka's nodes topic and RDM's memory store each run their own check (`NameHeld { by: node.id }` refuses a held name, and nothing is written), and writes the row. Row first, mesh second: a stopped node is sent nothing and reads its name in its join answer at its next start; a live successor is sent `ApplyNodeName` and publishes its digest with the name only on that apply; `node.updated` completes the step only after the apply answered. The node owns its name: it is the authority on the name it publishes, and what it publishes is the row's name. Only the mesh's authorized row writer claims and writes its Node rows. A member cannot write its row merely because it is the subject. The mesh-primary writes every row in its mesh, including its own. Before a mesh has a primary, the fabric-primary acts as its writer. At Day 0 the bootstrap admin, as mesh1's primary, claims its name and writes its own row through this same path, the first write of its birth; that is storage authority only, never permission to originate its own rename. Config on the row, `admin_api_base` included, is stored and gossiped as given; nothing checks it.
- **Config belongs to the role, not the process.** A node created at a `path.name`, or a successor taking one by rename, seeds its config from the previous row at that `path.name` (a Deleted row, or the `<name>.old` row), from its mesh-primary's held record or a point read through the `path.name` index. Recover and replace need no config copy step. A node that rejoins gets its config back in its join answer, a pull from its authority.
- **Tags are config, and they stay.** Rafka sets them with `node.config.update`. `node.get` takes a tag filter (all of these tags, or any of them), which replaces the demo's `tag:{tag}` selector and its `?tags=` and `?tags_any=` filters.
- **The exe belongs to the node.** Bin, sha, cpu and ram budgets and cpu_set are fixed when the node is created. A restart can't change them. A new bin takes a new node, with a new id and a new incarnation, and that new node replaces the old one through the replace recipe.
- **Storage is out for now.** It was proposed as the place Rafka hangs things on a node, such as the projections it inherits or a Rafka vault for ephemeral data. It comes back as `node.storage` when there is a real consumer. If it does, a write to it carries a declaration (which kind, its TTL, its backing store), never projection rows, because a projection fills only through the read path.
- **`otel_debug` is a meta field.** It turns one node's OpenTelemetry debug level up or down without a restart, so it is a property of the node, not an action. Today it is demo: `POST /api/nodes/{node}/otel-debug` with `{"on": bool}` (demo: `node-admin/src/main.rs`).

## Ops

Single calls answer or change one thing. Workflows are one call that RDM runs as several steps inside one Build. In the step lists, `<` is an event the node publishes, `>` is something sent to the node, and `<<` is an event the executor publishes from proof it holds (the node cannot report its own exit).

```text
NODE  (op family "node"; replaces the node calls on 0x1B, plus 0x1D join and 0x1E topology)
====
SINGLE CALLS (one call, one reply or one event)
verb          rpc call -> reply / event        replaces (demo route / old wire op)

read          node.get -> reply                by id, or filtered by mesh or by tags (all / any);
                                               old ProbeNodeState, tag:{tag}, ?tags=, ?tags_any=
-------------------
update        node.update -> node.updated      the full meta entity, blind put; only the
                                               caller-owned group may differ from what RDM
                                               holds (see "Node data"); old otel-debug,
                                               POST /nodes/state. NOT a lifecycle op.
-------------------
topology      node.topology.get -> reply       the node's held topology; old GetTopology (0x1E),
                                               demo: /api/topology-cache, demo: /api/mesh/listening
-------------------
connections   node.connections.get -> reply    old /dials
              node.connections.delete
                -> node.connections.deleted    hard cut, not graceful
-------------------
config        ## the app (Rafka) sets its own attributes, e.g. tags
              node.config.get -> reply
              node.config.update
                -> node.config.updated         full map; the mesh-primary writes the record, the
                                               node applies it (see "Node data"); old tag:{tag},
                                               ?tags=, ?tags_any=; demo: admin_api_base

~~~~~~~~~~~~~~~~~
ACTIONS (single calls that change the node)
~~~~~~~~~~~~~~~~~
drain         node.drain
                < node.draining                refuse new work, finish in-flight work
                < node.drained                 graceful; old drain-node / DrainNode / NodeDrained

WORKFLOWS (one call; RDM runs the steps as one Build)
==============
restart       node.restart                     node.stop then node.start on the stopped process;
                                               same node.id, same incarnation, same bin, the
                                               process never exits; a new bin is a new node
                                               (replace); old restart-node, demo: /api/nodes/{node}/restart
                < NodeRestarting (gossip)      from the mesh-primary: open no new connections
                                               to this node, expect it back, not "gone"
              node.stop                        the whole stop workflow; inside a restart its
                                               stopped is published by the mesh primary as
                                               NodeRestarting (expect it back), not
                                               NodeStopped, so peers hold the node rather
                                               than drop it
              node.start                       the whole start workflow
                < node.restarted
              restart = restarting + stop + start + restarted
==============
create        node.create(spec)                spec is today's SpawnRequest; old create-node,
                                               demo: POST /api/nodes, demo: /api/nodes/spawn
              mint and launch                  id, path.name, endpoint minted, data dir prepared,
                                               process launched, port bound
              node.start                       the whole start workflow, as defined below, with
                                               start_kind = FirstStart (old JoinNode 0x1D,
                                               FetchBuildFacts 0x1F)
                < node.created                 the node exists and is started; create ends here
              create = mint + start + created; nothing in start is repeated here
==============
stop          node.stop                        old stop-node / StopNode, and a plain
                                               demo: DELETE /api/nodes/{node} without permanent
                < node.draining                status Draining: live, not routable
                < node.drained                 receipt Established { in_flight } or, at the
                                               drain bound, Deadline { last_in_flight }
                < node.connections.deleted     the hard cut: gossip neighbours and pools.
                                               The cut never closes the stop call's own
                                               stream from the mesh primary; stopped rides it
                < node.stopped                 a reply frame to the mesh primary, never a
                                               gossip frame from the node (nobody is connected
                                               to hear one). The mesh primary publishes the
                                               departure as a NodeStopped lifecycle op in its
                                               in-flight overlay, the overlay NodeRestarting and
                                               NodeDeleting already ride in. Status Leaving:
                                               not live, still present.
              process stays alive and stopped, waiting for start or delete; stopped IS Leaving
              with a live process, and start is the only transition out of Leaving
==============
start         node.start                       the stopped (or just-created) process joins;
                                               nothing is created
              > join                           the accepting authority answers and the mesh
                                               channel is joined; the answer carries rafka-time
                                               and the cert (the hook needs the clock: broker
                                               resync stamps against it)
                < node.joined                  topology now follows the hook: the hook
                                               subscribes its kind topics through the joined
                                               channel and reads no topology snapshot. This
                                               moves fabric-node-hooks.md (:12, :38) and
                                               node-create.md step 6, which install GetTopology
                                               before the hook; they change when step 1 lands
                  (the app's hydrate_before_ready runs here, with start_kind in its context:
                  FirstStart or AfterStop, and the spec saying whether the data dir was
                  inherited. Broker resync, EF topics, projections. May take a while; Ready
                  waits on it. Blocked / Failed as in the hook contract.)
              > topology, and rafka-time again last, so both are current the moment the node
                re-enters; build facts when the node is below the authority's floor
                < node.started                 the node reports: hydrated, nothing pending
              status ReadyForTraffic           the node.started frame is the node's report; the
                                               mesh primary sets the status on it (old
                                               DeclareNodeState / ApplyNodeState, same
                                               exact-birth and transition rules); never set by
                                               a caller
==============
delete        node.delete                      old retire-node, delete-node, decommission,
                                               DELETE ?permanent=true
              node.stop                        the whole stop workflow, as defined above
                << node.deleted                the stopped process exits and id and name are
                                               released; published by the executor from the
                                               provider's exact-exit proof.
                                               Delete releases IDENTITY, not storage: the data
                                               dir (and the node-key in it) belongs to the spec
                                               and outlives the record until no spec names it
                                               or the app removes it. The node's mesh-primary
                                               writes the node's row with status Deleted; the
                                               record and its config stay
              delete = stop + deleted; nothing in stop is repeated here
              On a node whose process already exited (a crash), the stop steps are trivially
              done; the executor proves the exit and publishes deleted.
==============
recover       not an op: RDM drift reconciliation, run by the mesh-primary for its own
              members (lifecycles/node-recovery.md). The mesh-primary's tick reads
              PendingReconnect members, tickles, proves the exit through the provider, and
              submits this as one Build to the fabric-primary, which dispatches it back to
              the mesh-primary as executor. The app takes part only through the start hook
              <mesh primary proves the exit>   RDM: the tickle policy, then provider proof
              node[1].delete                   identity released; storage stays (above)
              node.create                      same name; spec points at node[1]'s data dir, so
                                               the same endpoint key; new node.id, incarnation
              node[2].update                   caller-owned meta only (see "Node data"); config
                                               is seeded by node.create from node[1]'s row
              the create's start runs the app hook with start_kind = FirstStart and the spec
              showing an inherited data dir: that is where Rafka runs broker resync
==============
replace       not an RDM op; a Rafka recipe, run as one Build
              forward
              node.create                      the successor, born as <name>.new: new node.id, new
                                               incarnation, same bin or a new one; Ready, and
                                               nothing routes to <name>.new
              node[1].stop                     stopped; incarnation and data kept
              node[1].update                   name -> <name>.old, if the data is needed; else
                                               node[1].delete. The mesh-primary claims the name
                                               in the store adapter and writes the row; the
                                               stopped node is sent nothing
              node[2].update                   name <name>.new -> <name>: the mesh-primary claims
                                               <name> (free since the row above), seeds config
                                               from the <name>.old row, writes the row, sends
                                               ApplyNodeName; node[2] publishes as <name>: cutover
              node[2].update                   caller-owned meta only
              <Rafka moves the data>           Rafka's step, not RDM's
              rollback (while node[1] is still stopped and its data is still there)
              node[2].stop                     no traffic before its name can move
              node[2].update                   <name> -> <name>.new, or node[2].delete after
                                               exit proof (any wanted successor data is Rafka's
                                               to copy out before this)
              node[1].update                   <name>.old -> <name>
              node[1].start                    the stopped process rejoins; same incarnation
                                               (start, not restart: the process never exited)
              Rollback ends the moment the data transfer runs, unless Rafka copies rather
              than moves and reclaims on commit. Rafka's recipe states which.

BUILD  (op family "build", op 0x20, RDM)
=====
create        build.create -> Started {build_id}, step frames, Complete / Failed {step, reason}
              build.create {preset: "demo"} is a preset Build
              node.create/stop/start/restart/delete are submitted through build.create
attempt.run   build.attempt.run (build_id, attempt, Won claim)
              fabric-primary -> executor_for's executor; adopt_claim before execution
              one contiguous same-executor run in plan order
get           build.get: read accepted Build and receipts for reattach after a cut
              never progress polling
delete        build.delete: history deletion; current Fabric.build_id refuses deletion
              no update to accepted intent; attempts and receipts append under it
              0x1C claim and 0x1F fact read stay unchanged

MESH  (op family "mesh"; replaces the mesh calls on 0x1B)
====
SINGLE CALLS (one call, one reply or one event)
verb          rpc call -> reply / event        replaces (demo route / old wire op)

read          mesh.get -> reply                by id, or every mesh in a fabric; demo: GET
                                               /api/meshes, /api/meshes/{id}, /api/mesh/summary
-------------------
update        mesh.update -> mesh.updated      full meta, blind put; never status
-------------------
nodes         mesh.nodes.get -> reply          the mesh's members
-------------------
config        ## the app (Rafka) sets its own attributes, e.g. tags
              mesh.config.get -> reply
              mesh.config.update
                -> mesh.config.updated

WORKFLOWS (one call; RDM runs the steps as one Build)
==============
create        no mesh.create op                old create-mesh, demo: POST /api/meshes, provider
                                               mesh-create
              node.create                      a mesh comes into being with its first node-admin.
                                               node.create names its mesh; the fabric-primary,
                                               which drives every node.create and holds the
                                               complete accepted topology, decides: mesh absent
                                               -> this Build mints the mesh id (never reissued)
                                               and the node-admin opens the mesh channel; mesh
                                               present -> the node joins like any other node.
                                               Never a "look whether the channel exists, then
                                               create it" read-then-write: that races
              the mesh-primary is elected (lowest Ready node-admin, sticky seat); the first
              node-admin wins because it is the only candidate, never by appointing itself.
              The fabric-primary sets the mesh's status ReadyForTraffic once every member is
              (old DeclareMeshState / ApplyMeshState).
              A node-admin left alive after its mesh is deleted is node.delete'd; a new
              node.create brings the mesh back under a new mesh id.
==============
delete        mesh.delete                      old shutdown-mesh, leave-mesh / LeaveMesh, demo:
                                               DELETE /api/meshes/{id}; run by the fabric-primary
              > fabric.primary.update          first, if this mesh holds the fabric-primary seat
                < mesh.deleting                the mesh-primary publishes its mesh's state
                                               (Leaving) in its own aggregate; peers learn it
                                               only there; replaces the fabric-primary's old
                                               mesh-leaving gossip (backbone + own mesh)
              > node.delete                    every member except the primary node-admin:
                                               ordinary nodes, then the other node-admins
                < mesh.deleted                 reported by the mesh-primary, still alive, from
                                               its members' exit proofs; mesh is dead, id kept
                                               retired; old MeshLeave / MeshLeft
              then a separate call: someone has to report the end of the thing, so the
              mesh-primary is not deleted inside its own mesh.delete. Until that call it keeps
              publishing its aggregate, itself the only member, mesh status Dead, so the dead
              mesh and its last admin stay in every topology; stopped or deleted, the
              fabric-primary publishes that mesh's own aggregate (itself the publisher, no
              digests, the birth in in_flight or departed) until a start or departed retention
              node.delete                      the mesh-primary node-admin; the fabric-primary is
                                               the executor and proves its exit

no verb       mesh recovery (the reconciler rebuilds a mesh under its existing mesh id)
              mesh investigation (Ping, then node.get forwarded through a peer; old Forward
              carrying ProbeNodeState)

FABRIC  (op family "fabric"; replaces the fabric calls on 0x1B and 0x21)
======
SINGLE CALLS (one call, one reply or one event)
verb          rpc call -> reply / event        replaces (demo route / old wire op)

read          fabric.get -> reply              demo: GET /api/fabric
-------------------
update        fabric.update -> fabric.updated  full meta, blind put; never status;
                                               demo: PATCH /api/fabric
-------------------
meshes        fabric.meshes.get -> reply       every mesh in the fabric
-------------------
nodes         fabric.nodes.get -> reply        every node in the fabric (the topology);
                                               demo: GET /api/topology
-------------------
config        ## the app (Rafka) sets its own attributes
              fabric.config.get -> reply
              fabric.config.update
                -> fabric.config.updated

~~~~~~~~~~~~~~~~~
ACTIONS (single calls that change the fabric)
~~~~~~~~~~~~~~~~~
primary       fabric.primary.update            hand the fabric-primary seat to another mesh-primary;
                                               old TakeFabricPrimary (0x21)
                < fabric.primary.updated       committed transfer at the next epoch;
                                               old FabricPrimaryTaken, new-fabric-primary gossip

WORKFLOWS (one call; RDM runs the steps as one Build)
==============
create        fabric.create(spec)              none today: a fabric is born with its first mesh
              mint                             fabric id minted; status pending
              > node.create                    each mesh's first node-admin (which creates the mesh);
                                               the first mesh's primary is fabric-primary
              > the status changes below, once every mesh is ready-for-traffic
                < fabric.created               the fabric is ready-for-traffic; create ends here
==============
status        pending -> state-sync -> state-commit -> ready-for-traffic
              the fabric's status, set only by the fabric-primary (old ApplyFabricEvent)
              and carried down in rounds, fabric-primary -> mesh-primaries -> nodes; each
              member checks in with its primary, and a primary reports only once all of its
              members have (states.md, the round checklist). These are
              status changes, not calls; the hooks run on the status change.
              a resize or roll returns it to pending (states.md:9, :67)

              state-sync                       every mesh is ready-for-traffic. Hook: the app
                                               (Rafka) does its day-0 work, including the system
                                               org and the store RDM's rows go into; it answers
                                               when its work ends (old sync-state / state-synced)
              state-commit                     each node writes its scratchpad (its RDM rows) into
                                               that store: a separate status because the store
                                               does not exist until state-sync ends (old
                                               CommitState / StateCommitted)
              ready-for-traffic                every node checked in (old OpenTraffic /
                                               TrafficOpened). Hook: one notice to the app, which
                                               opens client traffic
==============
delete        fabric.delete                    old fabric shutdown (FabricShutdown, leaf-inward,
                                               fabric-mesh-lifecycle.md §11.1)
              > mesh.delete                    each mesh, the fabric-primary's own mesh last;
                                               on its own mesh the seat stays put (no other
                                               mesh is left to take it), so that mesh.delete
                                               skips its fabric.primary.update step
              > node.delete                    each other mesh's mesh-primary, after its
                                               mesh.deleted; the fabric-primary is the executor
                                               and proves its exit
                < fabric.deleted               reported by the fabric-primary, still alive
              then a separate call, for the same reason:
              node.delete                      the fabric-primary, called from outside the fabric
                < node.deleted                 the fabric-primary's last reply; the caller takes
                                               its word. The one exception to exit proof: nobody
                                               is left in the fabric to prove it

no verb       fabric recovery (recovered primaries resume the same Build from the durable map)
```

## Identity: what each op keeps

Four identity terms, used exactly this way throughout:

- **`path`** — the node's position in the fabric (`<mesh>.<kind>`). Minted at create, never changes.
- **`name`** — the node's name within its path. The only mutable identity field, changed only by `node.update` with no traffic, and only in the two renames CLAUDE.md allows: a stopped predecessor to or from `<name>.old`, and a successor born `<name>.new` to or from `<name>`. `path.name` in this document means the composite.
- **`node.id`** — the logical node. New at create, same through stop/start/restart, released at delete.
- **`incarnation`** — the exact runtime. New at create, same through stop/start/restart (the process never exits), ended at delete. `(node.id, incarnation)` is the exact-birth fence every authority check, proof and lineage rule keys on; retiring the *word* "birth" from op names retires nothing about the fence.

- **`endpoint`** — the iroh key, which lives in the data dir (`node-key`). **The endpoint key belongs to the data dir:** whoever runs on that storage has that key. A new node over an inherited data dir keeps the key and binds a fresh port; a new node with its own data dir mints a new key.

| op | node.id | path | name | endpoint key | port | incarnation | process |
|---|---|---|---|---|---|---|---|
| create | new | new | new | new | new | new | new |
| stop | same | same | same | same | same | same | stopped |
| start | same | same | same | same | same | same | the stopped process rejoins |
| restart | same | same | same | same | same | same | stopped, then rejoins; never exits |
| update | same | same | may change, stopped only | same | same | same | unchanged |
| delete | released | — | released | stays with the data dir | — | ended | exits, with proof |
| recover (Rafka) | new | same | same | same (inherited data dir) | new | new | new |
| replace (Rafka) | new | same | taken over | new, unless the recipe copies node-key | new | new | new |

Status through the lifecycle (R-SC11 preserved, no new state): `draining` → **Draining** (live, not routable); `drained` carries the drain receipt, `Established { in_flight }` or at the bound `Deadline { last_in_flight }`; `stopped` → **Leaving** (not live, still present). Stopped is Leaving with a live process. `node.start` is the only transition out of Leaving, and only for a stopped process; every other refusal of a Leaving node (`build.rs`) stands. Under `delete`, Leaving continues to `deleted`.

Unique ownership of a name is by insert-and-fail at every step of replace and rollback: a name is held by exactly one node (stopped or live) until that node is renamed or deleted, and a `create` or rename into a held name is refused typed (`NameHeld { by: node.id }`).

- **Restart is not a re-birth.** A restart is `node.stop` then `node.start` on the stopped process: the process never exits, so it keeps the node.id and the incarnation, and it can't change the bin. A new bin takes a new node, with a new id and a new incarnation, which replaces the old one. Anything with a new node.id is a create. The node-RPC fence already says this: "A restarted node keeps its id; a replacement is a new id" (`node-rpc-envelope.md`, Section 0). One code consequence for rollout step 1: the mesh transport's book today completes a restart only when a digest with a *different* incarnation arrives (`rust-distributed-mesh` `membership.rs`, `restarting` / `restarted`); under this model it completes on `node.started` of the *same* incarnation, and the exit-and-relaunch case that rule was written for is now Rafka's `recover` recipe — a new node over the old data dir. `supersedes` lineage does not apply to it: `supersedes` chains incarnations of one node.id, and recover mints a new node.id. Peers learn the replacement from `node.deleted` for the old instance and the new instance's own join. The peer-location book (`register_location`, `rafka-mesh-transport` `membership.rs-99`) is keyed by endpoint key and refuses only the incarnation the held entry `supersedes`; recover sets none, so a late digest from the old instance would read as fresher and point the key back at the old port. So `node.deleted` also fences the old `(node.id, incarnation)` in that book, and a digest from a fenced incarnation is refused by name.
- **Recover and replace are a create followed by a meta copy.** The copy step copies only caller-owned fields. The successor gets `path.name` and its identity from its own create spec; it never copies the predecessor's name, and the predecessor's RDM-owned identity and runtime fields (`node.id`, `incarnation`, `endpoint`, `transport_addr`, `exe`, runtime handle, `status`) never carry over. Recover still runs on the same endpoint key, because the key comes from the inherited data dir, not from the copy.
- **Stop stops a live process.** It drains, drops connections, reports `node.stopped` to its mesh primary on the stop call's stream (the mesh primary publishes the departure; the node gossips nothing) and waits for further instructions. Start makes that same process go through the rejoin lifecycle, minus the creation part. So start keeps the incarnation and the endpoint, and nothing is created.
- **A stopped node stays reachable.** `node.connections.delete` cuts the node's *mesh* connections: gossip neighbours and its pooled connections to peers. It never unbinds the node's own endpoint. A stopped node keeps its endpoint bound and answers node-RPC from anyone who reaches it; peers drop it from their books and pools on the published departure, so its authority is the only caller left, which is how `start`, `update` and `delete` reach it. Nothing checks who is calling: there is no auth layer inside the mesh, and the fence stays target node id + op. `node.get` on a stopped node answers `Current { parked }`.
- **Stop/start cycles are already distinct.** Stop and start keep the incarnation, but nothing needs a new counter to tell cycles apart: each step's receipt is keyed by its Build id and attempt, and each workflow's events arrive on that call's own reply stream, so a late `node.stopped` from an earlier call can't be read as part of a later one. On the gossip side the process never exits, so `digest_seq` continues across the cycle and the book's newer-wins rule drops the older digest.
- **A name is unique within its path, for every entity kind, and only a delete releases it.** Fabric, mesh, node, virtual topic alike: `mesh1.broker.1` and `mesh2.broker.1` coexist because their paths differ; two live `mesh2.broker.1` cannot. A node's name is released by node.delete (its row Deleted); a mesh's by its removal (its row Dead), and a mesh removal deletes every roster birth, so a mesh created again under the same name claims its node names over Deleted rows with a new mesh id. A stopped node, a Dead mesh's live-status row or a silent one still holds its name. A path is a path: dotted segments, minted once, immutable, with no structural meaning of their own. The segments we use are a naming convention, `mesh1.broker` for a node named `1`, `fabric1.meshes` for a mesh named `mesh1`, and nothing reads structure out of them; the link between a node and its mesh is the MeshId on the node's row. Where a row is stored (`ops.meshes` in the system org) says nothing about the entity's path.
- **The name is claimed by insert-and-fail, inside the store adapter, by the mesh-primary.** The path is minted once and never changes; the node's `name` within it can. The new node can take the name over only after the old node lets go, by being renamed to `<name>.old` or deleted. The two renames are `<name>` to `<name>.old` on a stopped predecessor and `<name>.new` to `<name>` on a successor nothing has routed to; both run back on rollback.
- **A name changes only with no traffic, and never by the node itself.** `node.update` refuses a `name` change on a node that routes traffic. The mesh-primary writes the row first; the node publishes the name only when told (`ApplyNodeName`), or reads it at its next join. A node renaming itself mid-gossip is the same class of fault as a same-key restart: peers hold the old name against the same key and incarnation.

## Decisions and why

- **Stopped nodes are garbage after a ttl, by config.** The contract is [the stopped-node garbage collector](../../architecture/fabric-node-lifecycle.md#stopped-node-garbage-collector), fabric-node-lifecycle.md §6.9. `stopped_node_ttl`: a node stopped longer than it is collected. The fabric-primary, the sole Build driver, sweeps every ttl and submits an ordinary node.delete Build for each such node (exit proof, row Deleted, name released, NodeDeleted in the aggregate, departed retention); the sweep deletes nothing itself. A stopped birth an open Build still references is skipped, so a replace mid-transfer keeps its rollback point. The age is the current stopped period's own stop receipt in the Build log, never an earlier stop before a later start; the receipt is durable, so it survives a fabric-primary handover, and a birth whose receipt cannot be read is not expired, it is skipped by name. The sweep only selects: the stopped and open-Build checks run again at the delete Build's admission, serialized with it, so a node started or referenced after the scan is refused there, never deleted from a stale decision. One topology Build runs at a time, so the sweep submits one delete and the rest wait for the next sweep; a `BuildInProgress` refusal drops nothing, the candidate is simply seen again. Timing: a node is eligible after one ttl and is normally collected between one and two ttl, plus Build contention. No frame gains a field. Blank means forever: no sweep, the node stays until its own start or delete. RDM's shipped config sets 30 minutes; Rafka's deployment sets 1 hour.

- **CRUD alone isn't enough.** Drain, stop and start change a node that keeps existing, so they aren't create or delete. Treating stop as delete is already causing confusion, because today the demo's plain `DELETE /api/nodes/<name>` is really a stop that keeps the row and the data.
- **Drain and connections.delete both stay.** `node.drain` is graceful: it refuses new work and lets in-flight work finish. `node.connections.delete` is a hard cut. Stop runs drain, waits for `drained`, then deletes the connections and reports `node.stopped` on the stream; the mesh primary publishes the departure to peers (R-ON1).
- **Stop is not decommission.** Today's decommission is drain, then stop, then retire the identity. In the new set, that is `node.delete`. Stop is only the middle step, and the node still exists afterwards.

  | | stop | delete (old decommission) |
  |---|---|---|
  | drains first | yes | yes |
  | process | stopped, alive | exits, with proof |
  | node.id, name | kept | released |
  | storage (data dir, node-key) | kept | kept: it belongs to the spec, not the record |
  | can come back as the same node | yes, with start | no; a new create gets a new id (over the same storage, if the spec says so) |

- **`permanent` goes away, because delete is always permanent.** Today `?permanent=true` changes four things:
  - It releases the data dir and `node-identity.json` on exit (`should_release_on_exit`, `node-admin/src/lib.rs::should_release_on_exit`).
  - It writes broker departure rows as `PermanentRemoval` (`node-admin/src/main.rs::kill_one_keeping_held_mint`).
  - It reaps the spawn registry entry (`node-admin/src/main.rs::nodes_topology_json`).
  - It flags the gossiped pending-delete as permanent (`node-admin/src/main.rs::kill_one_keeping_held_mint`).

  Under the new verbs, stop keeps everything and delete releases the identity, so the flag has nothing left to say. The data dir is not released by delete at all, and `node-identity.json` — the endpoint key — is part of it and stays with it; removing the data dir is the app's step.
- **Decommission moves no data.** Survivors rebuild missing replicas from the other replicas, not from the leaving node. Only today's replace moves data: it renames the predecessor's partition dir into the successor's, on the same filesystem (`node-admin/src/lib.rs`). In the new model, keeping the old node around for data transfer is Rafka's call. Rafka does it with the `.old` rename in its replace recipe.
- **Stop and start are the primitives; the workflows contain them.** Create is the whole `start` then `created`; delete is the whole `stop` then `deleted`; restart is `restarting`, `stop`, `start`, `restarted`. Create's own step (AllocateIdentity, PrepareStorage, launch) already runs in the Build pipeline. Nothing in `start` or `stop` is restated inside a workflow, so there is one definition of each and they can't drift. Every workflow is built from about eight step events, which keeps the event vocabulary small.
- **`SpawnRequest` is not a chaos mechanism.** It supplies the spec of node.create through build.create.
- **`restart-broadcast` doesn't exist in the model.** The route only announces `NodeRestarting` for a node in a peer mesh, and it restarts nothing (`node-admin/src/main.rs`). In the model, `NodeRestarting` comes only from the restart itself. It sets the node's status to restarting until the node comes back, which tells every node to start draining its connections to it, because the node's next events are draining and then stopped.
- **Broker resync moves to Rafka, and runs inside `start`'s hook.** `resync-steps` walks a broker through re-syncing its replicas after a departure (`node-admin/src/resync.rs`). Brokers and replicas are Rafka's concepts. Rafka runs resync inside `hydrate_before_ready` when the start's context says the data dir was inherited (the recover recipe) — that is why the join answer carries rafka-time before the hook, so resync stamps against a current clock — and the node is not Ready until it finishes. Broker departure rows on delete are also Rafka's step.
- **Builds are an entity.** A build has create, get and delete, and no update. Bootstrap is a preset build, not its own call.
- **System-org is Rafka's.** It is app data created during state-sync, the fabric status in which Rafka does its day-0 work. RDM only reports the fabric status.

## Rollout and cost

Four moves, each its own landing, none bundled into another. Step 1 lands before the i143 soak; steps 0, 2 and 3 land after the i141 pin, behind the typed objects, so they change no Rafka caller (R-ON7, 2026-10-10; it moves R-ON1's `NodeStopped` from step 2 to step 1). Step 0 is code placement; step 1 adds the Build 0x20 wire carrier and node behavior plus one gossip variant (`durable format moved`); step 2 is a durable-format move; step 3 is a wire-contract move.

0. **REST out of RDM.** `rafka-node-admin-core/src/http.rs`, the `axum`/`tower` dependencies in that crate, and their users (`admin.rs`, `planned:tests/control_routes.rs`, `planned:tests/client_contract.rs`, `planned:tests/drift_convergence.rs`, `planned:tests/provider_policy.rs`, `rafka-node-rpc-testkit/src/admin_faults.rs`) move to the demo crate as thin callers over the typed objects. So does `admin_api_base`, the REST base URL `NewFabricPrimary` carries in its contact addresses (`gossip.md`): after this step it is the demo's field, not RDM's. When this lands, `grep -rn 'Router\|axum\|"/api/' crates/rafka-node-admin*` returns nothing, and that grep becomes the CI guard. Not before.
1. **Typed objects, the node lifecycle and Build 0x20.** The additive build family and fabric-primary sole driver land here; 0x1B stays unchanged. Node workflows are submitted through build.create, then mapped onto the existing node ops. For example, `node.drain` sends the existing `DrainNode`; callers stop seeing the ad hoc names straight away. The behaviours that need each other land together here: stop stops and start rejoins; restart keeps the incarnation, and the mesh transport's restart completion rule moves to the same incarnation (see "Identity"); delete proves the exit; `node.deleted` fences the old incarnation in the peer-location book; and the mesh primary publishes `NodeStopped` from the stop call's `stopped` frame. `NodeStopped` is a postcard enum variant, so this landing carries `durable format moved`; a stop that stops has no other publisher for its departure, so the variant cannot wait for step 2.
2. **Build step keys next** (`node.create:<path.name>` replacing `create-node:<path.name>`). The keys are stored in accepted Builds and receipts, so this is a `durable format moved` change. They are built and parsed in rust-distributed-mesh, which has to be checked first for anything that splits them on `-`.
3. **Split the remaining 0x1B calls.** Splitting them into the `node`, `mesh` and `fabric` op families is a wire contract change, hidden behind the typed objects, so only the objects change.
After step 0, as its own landing: **config in the digest.** `admin_api_base` leaves its typed `MeshDigest` field (`rafka-mesh-entity/src/digest.rs`) and `NewFabricPrimary`'s contact addresses and becomes a config key the demo owns; `extra` is renamed `config` (a name only; postcard is positional); the cap is enforced. Removing the two fields moves positional rows, so this landing carries `durable format moved`.

4. **Gossip frames** (`NodeDeleting`, `NodeDeleted`, `mesh-leaving`, `NodeRestarting`) follow the same naming only with Luke's go-ahead. One addition is already ruled (R-ON1, 2026-10-10): a `NodeStopped` lifecycle op in the in-flight overlay, authored by the mesh primary from the stop call's `stopped` frame. It is a postcard enum variant — `durable format moved` — and lands with step 1, because a stopped node's departure has no other publisher.

## Acceptance

Three tests make the contract real. Each ships with its Jaeger trace.

1. **A failure names its step.** `node.start` on a node whose join fails: the stream carries `Started`, then `Failed { step: "node.join", reason }`, and neither `node.joined` nor `node.started` is ever emitted. The node's status never reaches ready-for-traffic.
2. **A broken stream is not a failure.** Cut the caller's stream mid-workflow. The caller's outcome is `NotSent` or `Indeterminate`, never an inferred step failure. For a Build-backed call, `build.get` returns the receipts of exactly the steps that completed, and resubmitting by `build_id` re-runs none of them.
3. **Stop stops, start rejoins.** `node.stop` then `node.start`: the same `node.id`, `incarnation` and endpoint before and after; `node.get` answers `Current { parked }` in between; the mesh primary's `start` reaches the stopped node over node-RPC at the same endpoint (a fresh dial, since `connections.delete` cut the old ones); peers' books show the same birth throughout.

### Build carrier acceptance (R-ON8)

Each case ships its Jaeger flame from a live mesh.

1. node.restart over 0x20 is one call with Started, all step frames and Complete;
   no build.get polling and no Build-topic wait before the first frame.
2. Fabric-primary death after claim and before dispatch: the successor's
   build.attempt.run starts the claimed work; resubmission by build_id streams
   through Complete, with no completed step executed twice.
3. Executor loss ends the inner stream Indeterminate. A successor attempt is claimed
   only after departure proof; the caller receives the successor's frames.
4. Cutting the outer stream leaves the Build running. Resubmitting build_id after
   completion returns AlreadyApplied with the terminal frame.
5. A mid-Build seat handover ends the outer stream with the typed new-seat frame,
   not Indeterminate. Old-seat dispatch is refused after the fence and its span
   records Fenced.
6. build.attempt.run sent to a recipient not named by the claim, or with a stale
   folded claim, receives a typed named refusal.

## Open

- [ ] Mesh and fabric stop and start: neither exists today. Add them only if something needs to park a whole mesh.
