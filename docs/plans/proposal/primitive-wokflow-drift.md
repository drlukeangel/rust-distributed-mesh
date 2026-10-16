# Primitive and workflow drift: how far the pages and the code have drifted from "the node owns its state"

Report, 2026-10-15, on main `4de221c3`. Read-only; nothing here is changed yet. Every architecture line below is quoted
verbatim with its line number at that sha. Four readers covered the 36 pages in full; the lead sample-verified 14 cited
lines (all exact) and read the code sites himself.

## 1. The contract (Luke, 2026-10-15, verbatim)

> A node has primitive commands that do one thing: join a mesh, leave a mesh, delete connections, and so on. Each command
> takes an input, executes on the node, and returns its progress and result.
> A workflow composes one or more primitives. node.start, node.stop and node.restart are workflows. Their steps use the
> preceding commands' results.
> The node changes its own state as it executes. Its replies tell the MP what happened and what state it reached. The MP
> records and gossips that reported state. Sending a command does not let the MP assume it happened.
> An event is not a state. node.joined says a join completed. pending and ready-for-traffic describe the node's state. Do
> not turn command names or completion events into additional states.
> If a command never reaches the node, that command has changed nothing on the node. The MP must not publish an invented
> state transition.
> The provider separately reports runtime observations and proves exit when the node cannot report for itself.

MP = the node-admin mesh-primary. Three things stay distinct:

| | what it is | who originates it | examples |
|---|---|---|---|
| lifecycle state | what the node reports it is in | the node | `pending`, `ready-for-traffic`, `connections-deleting`, `connections-deleted`, `stopping`, `stopped` |
| workflow intent | an overlay on an operation the executing authority holds | the executing authority | `starting`, `restarting` (`NodeRestarting`), `deleting` (`NodeDeleting`) |
| external observation | what the MP or the provider sees, never the node's word | the MP, the provider | `PendingReconnect`, `Dead`, the durable `deleted` row from exit proof, `held-running`, `os-stopped`, `data-dir-lost`, `data-dir-unreadable` |

## 2. How much drift

| | findings | anchors (lines that already state the contract) |
|---|---|---|
| lifecycles/ node-* pages and states.md (18 pages) | 56 | 60 |
| lifecycles/ mesh-* and fabric-* pages, fabric-authority, fabric-mesh-node-lifecycles, elections, node-admin-lifecycle (13 pages) | 57 | 53 |
| gossip.md, node-identity-and-state.md | 47 | 30 |
| node-rpc.md, node-rpc-envelope.md | 24 | 29 |
| **total, 36 pages, 8,848 lines** | **184** | **172** |

Not evenly spread. The anchors show the pages already know the rule where it is about proof: silence is never death,
exit proof is the provider's, a reply is not a completion, a refusal proves the handler never ran. The drift is
concentrated where the pages describe who moves a node's status: there the vocabulary is "set by the mesh-primary", the
source is a command's admission, and two overlays and one durable row are listed as states. Nine patterns carry
almost all of the 184 lines.

## 3. The nine patterns

**P1. "Set by the mesh-primary" (D1), about 40 lines.** states.md's table has a column headed `set by` and every node
row says the MP (states.md:23-33); node-status.md:26 "A node reports to its mesh's primary, which sets its status";
gossip.md:153-154 "the status authored by the mesh-primary alone"; gossip.md:157 and node-primary-update.md:39 "the
sole author of member status"; node-identity-and-state.md:21 "Its mesh-primary sets a node's status"; node-start.md:46,
:64, :94 "the mesh-primary sets ReadyForTraffic"; node-connections-delete.md:18, :67, :75 "set by / sets";
node-mesh-join.md:213; node-rpc-envelope.md:571 "Every node's status ... is set by its mesh's primary". The source
column is often right (the node's report); the verb is wrong, and the code took the verb literally (§5).
Fix: one wording everywhere: the node reports; the MP records and gossips.

**P2. A state sourced from a command's admission or send (D2), 9 lines.** states.md:30 "node stopping | ... | the stop
call's admission"; states.md:28 and node-connections-delete.md:21, :67 "connections-deleting ... set at the call's
admission"; node-stop.md:33 "The stop's visible status is stopping" while the child call runs; node-mesh-leave.md:18;
mesh-create.md:23, :41, :81, :95 (the mesh's `pending` from a Ping answer and an `ApplyMeshState` request sent to it).
This is the pattern behind C17: a stop that never reached the node was recorded as `stopping`.
Fix: the state is what the node (or the mesh's authority) reports in its reply; admission of a command is not a state.

**P3. Events and intent as states (D3), about 45 lines.** `starting` and `deleting` sit in the node state list
(states.md:13, :25, :32, :92-93; node-identity-and-state.md:269, :465-466; fabric-node-lifecycle-elections.md:45);
`Restarting` is a `NodeState` row (node-identity-and-state.md:481, :497-498, :515; and in code, `NodeStatus::Restarting`,
model.rs:33); `Leaving` is a state "set by the mesh-primary on node.stopped" (node-identity-and-state.md:482, :512);
node-restart.md:16, :27, :39, :45, :57 publish the node's reported `stopped` as `NodeRestarting` instead of as
`stopped`; the mesh analog `MeshStatus deleting` and the event name `mesh.deleting` used as a status
(mesh-delete.md:19, :25, :38, :53, :88, :93, :122, :160, :183, :210, :229; gossip.md:345; node-rpc-envelope.md:253, :593,
:817); `joining` / `leaving` named as "that state" (node-mesh-join.md:29, node-mesh-leave.md:21).
Fix: the state lists hold the six reported states; `starting`, `restarting`, `deleting` are overlays beside them;
`stopped` is gossiped as `stopped` with the restart overlay kept alongside.

**P4. A downward status apply that the code refuses (D1), 12 lines.** node-status.md:5, :30-43 describes "Status apply:
the node's mesh-primary, downward to the exact accepted node"; node-rpc-envelope.md:571 "downward for applies", :581
"ApplyNodeState carries the permitted transitions", :609 the `ApplyNodeState` request, :697
`RejectedInvalidNodeTransition`; states.md:40 "the report and apply that move a node's status". On the wire every
node and every admin refuses `ApplyNodeState` (`rdm-node/src/node.rs:1267`, `rdm-admin/src/status_rpc.rs:983,1032`:
"ApplyNodeState is not served"). The pages describe a mechanism that does not exist and must not: the contract has no
downward state write.
Fix: the "Status apply" section goes; the envelope keeps the variant only as a reserved position if its removal would
move the 0x1B discriminants (durable format), and says it is never served.

**P5. Three state vocabularies (D6).** The contract names six node states. The envelope's `NodeState` and
`MemberStatus` (node-rpc-envelope.md:586-592, :942-948) carry four: `Pending`, `ReadyForTraffic`, `ConnectionsDeleting`,
`Stopped`. The code's `NodeState`, `MemberStatus` and `NodeStatus` (rdm-core `status.rs:46-52`, `digest.rs:35-41`,
`model.rs:18-24`) carry a different four: `Pending`, `ReadyForTraffic`, `Draining`, `Leaving`, with wire names
`draining` and `leaving` (`rename_all = "kebab-case"`), while the pages say `connections-deleting`, `stopping`, `stopped`
(the R-ST1 rename reached the 0x1B reply names, `status.rs:1439,1443`, and the pages, not the state enums). The node
itself declares only three states in its life: `Draining` when it drains (`node.rs:151`), `Leaving` when it stops
(`node.rs:204`), `ReadyForTraffic` after its start (`node.rs:1072`). It never declares `pending`; `stopped` is not a
state it can report, only the `Stopped { receipt }` reply to `StopNode`; `connections-deleted` and `stopping` exist
nowhere in code. Every place the MP holds `pending` or `stopped` for a node, it put it there itself (P7, §5).
Fix: one `NodeState` with the six names on the wire and in the code, declared by the node at each transition it makes;
a durable format move on 0x1B and on the Members projection (a gossip change: Luke's permission and a flame).

**P6. `stopped` carried as an in-flight overlay, not as the member's status (D3), a design question.** gossip.md:153,
:156, :207, :261, :274-279 carry `NodeStopped` in the Members `in_flight` overlay beside `NodeRestarting` and
`NodeDeleting`, "derived from Build facts each round", and node.start "clears it at node.joined". states.md:119 says
"status `stopped`, overlay". Under the contract `stopped` is a state the node reports, so it belongs in the member's
status like `connections-deleting` does (gossip.md:230-231 already says so for the other three), and it is replaced only
by the node's next reported state. The restart and delete overlays stay overlays.
Fix: Luke's call (§7, question 2). Either way the pages must agree with themselves.

**P7. Observations written as statuses (D5), about 20 lines.** The durable `deleted` row described as "the node's row
with status deleted" written by the MP (node-delete.md:32, node-identity-and-state.md:158-159, :649, mesh-delete.md:26);
`Dead` "written as status Dead on the node's row" (node-recovery.md:50); the fabric-primary's `degraded` mark published
as mesh-x's `MeshStatus` (mesh-investigation.md:59, :68, :86, :115, :164) against the same page's line 18 "This
investigation does not author mesh-x's MeshStatus"; "the mesh-primary proved its exit" where the provider proved it and
the MP recorded (fabric-delete.md:18, :34, :53; fabric-mesh-node-lifecycles.md:89; mesh-delete.md:55);
node-delete.md:16, :27 "for a crashed process those stop steps are already done" (nothing stopped; nothing is recorded as
stopped; node-recovery.md:85 already says "no stop").
Fix: observations are named as observations: the exit-proof row, the silence marks, the degraded health mark; none is a
node's or a mesh's lifecycle status.

**P8. "parked" (D6), 8 lines.** `CurrentParked` (node-rpc-envelope.md:245, :709; node-tickle.md:27, :72, :112;
node-recovery.md:43; states.md:133; node-replace.md:16). R-DW1 retired the word; a stopped node answers
`Current { state: stopped }`.
Fix: one reply shape; the variant becomes a reserved position if its removal moves 0x1B discriminants.

**P9. The same rule one level up (mesh and fabric), about 25 lines, Luke's call whether it applies.** The mesh's
`pending` set from a Ping and an `ApplyMeshState(Pending)` request (mesh-create.md:23, :41, :64, :81, :155, :188); the
mesh's `ready-for-traffic` "set by the fabric-primary" (mesh-create.md:23, :46, :54, :157, :164, :190); "Build +
Mesh/Fabric lifecycle transition" rows (fabric-authority.md:329-332, :345); the fabric rounds' `-ing` frames published
on the send (fabric-state-commit.md:17, :63-64; fabric-ready-for-traffic.md:6, :48-49; gossip.md:305-307) where the
pages themselves call them "command intent, never completion" (gossip.md fabric round hooks), which is the overlay the
contract allows. If the rule applies up, the `-ing` frames are intent overlays and the mesh's and fabric's states come
only from the check-ins; the wording moves the same way as P1.

## 4. Internal contradictions the readers found (one page against itself or its neighbour)

- mesh-investigation.md:18 ("does not author mesh-x's MeshStatus") against :59, :68, :86, :164 (publishes `MeshStatus`
  degraded).
- gossip.md:164 (the restart overlay is open for the whole restart) against :274 (the stop's `stopped` is published as
  `NodeRestarting`); and node-restart.md:16 against states.md:119 (status `stopped`, overlay).
- node-delete.md:16, :27 ("stop steps are already done" for a crashed target) against node-recovery.md:85 ("no stop").
- node-rpc-envelope.md:581 forbids `ApplyNodeState(ConnectionsDeleting)` and permits the other transitions, while the
  code serves none (P4).
- node-rpc-envelope.md:1267 "RecipeStep = Create | Stop | Update | Start | Delete: one node primitive per step" against
  :239 and node-restart.md:5 (stop and start are workflows).

## 5. The code side (the lead's reads, rdm-admin and rdm-node at 4de221c3)

The code did what P1 told it: the executor writes the node's status when it decides to command.

| site | writes | the node's report it stands in for |
|---|---|---|
| `deployment/pipeline.rs:1683` | `Pending` at the publish of the birth's record | none: the node never declares `pending` |
| `deployment/pipeline.rs:1918` | `Pending` at the top of the StartNode step, before the send | none |
| `deployment/pipeline.rs:2249` | `Leaving` (= `stopping`) at the top of the StopNode step, before the send | the node's `Leaving` declaration at `node.rs:204`, which the MP also receives on 0x1B; C17: the send failed and the write stayed |
| `deployment/pipeline.rs:1976`, `:1279` | `ReadyForTraffic` after the readiness wait and at create Complete | the node's `ReadyForTraffic` declaration, already applied at `status_rpc.rs:893-905`: a second writer of the same fact |
| `admin/runner.rs:143`, `:151` | member status `Draining`, `Leaving` on the executor's step | the node's own declarations |
| `admin/runner.rs:148` | publishes the reserved `NodeLeaving` frame (gossip.md:160, R-CR4: never published) | nothing: a command hook, not a report |
| `admin/runner.rs:734` | `Leaving` for a parked birth record | the node's `stopped` reply |
| `model.rs:33` `NodeStatus::Restarting`; `views.rs:36`; `projection.rs:329` | a workflow overlay as a view status | none |

Correct under the contract and kept: the one report path at `status_rpc.rs:893-905` (the node's `DeclareNodeState`
applied, `book.started` / `book.reported`); `recovery.rs:529` and `projection.rs:481` (`PendingReconnect`, `Dead` as
observer marks); the exit-proof row; the refusal of `ApplyNodeState` by every receiver.

The underlying defect, as the a1 architect put it: one fact with four writers (pipeline assignments, the node's
report, the status apply path, catch-up reconciliation). The fix is one report-processing path that everything else
consumes, and a cell proving a failed command cannot invent or replay a transition.

## 6. The full finding table

Format: `file:line | category | quote | what the contract says`. Categories: D1 MP sets/authors; D2 state from
admission or send; D3 event or intent as a state; D4 published before reported; D5 observation as status; D6 vocabulary.

### 6.1 lifecycles/ node pages and states.md (56)

```text
states.md:5 | D1 | "This page owns the fabric, mesh and node lifecycle states, who sets each, the status rounds" | the page owns which node reply reports each state and who records and gossips it
states.md:13 | D3/D5 | "**Node:** starting → pending → ready-for-traffic → connections-deleting → connections-deleted → stopping → stopped → deleting → deleted" | node states are pending … stopped only; starting/deleting are workflow-intent overlays; deleted is the provider-proved durable row, listed separately
states.md:21 | D1 | "## Who sets each state" | "Who reports and who records each state": the node reports, the MP records and gossips
states.md:23 | D1 | "| state | set by | from |" | columns: reported by (the node) / recorded and gossiped by (the MP) / carried in which node reply
states.md:25 | D3/D2 | "| node starting | the node's mesh-primary | the accepted node.start execution (inside node.create, or on a stopped node) |" | starting is the executor's intent overlay for the start workflow, not a node state; move it out of the state table
states.md:26 | D1 | "| node pending | the node's mesh-primary | the node's node.joined report on the start call |" | pending is the state the node reports reaching; the MP records it and does not set it
states.md:27 | D1 | "| node ReadyForTraffic | the node's mesh-primary | the node's node.started report, after every start gate |" | the node reports ready-for-traffic with node.started; the MP records and gossips it
states.md:28 | D1/D2 | "| node connections-deleting | the node's mesh-primary | the admission of `node.connections.delete { scope: All }`; inside node.stop the visible status is stopping |" | the node reports connections-deleting when it enters the closure, inside node.stop too; admission sets no state
states.md:29 | D1 | "| node connections-deleted | the node's mesh-primary | the node's node.connections.deleted completion carrying `Established`" | the source is right, but the MP records the reported connections-deleted; it does not set it
states.md:30 | D2 | "| node stopping | the node's mesh-primary | the stop call's admission (node.stop) |" | stopping comes only from the node's report that it is stopping; the stop call's admission changes nothing on the node
states.md:31 | D1 | "| node stopped | the node's mesh-primary | the matching stopped frame on the stop call's stream |" | the node reports stopped in its reply; the MP records and gossips it
states.md:32 | D3/D4 | "| node deleting | the node's mesh-primary (the fabric-primary for a mesh with no primary) | node.delete's adopted claim, published as NodeDeleting |" | deleting is the delete workflow's intent overlay (NodeDeleting) from the claim, never a node state
states.md:33 | D5 | "| node deleted (the durable row) | the node's mesh-primary (the fabric-primary for a mesh with no primary) | node.delete's exact provider exit proof |" | the deleted row is an external observation from the provider's exit proof; list it apart from node lifecycle states
states.md:40 | D1 | "The internal report and apply that move a node's status are [node status](node-status.md)." | the node's reply reports its state and the MP records it; no apply moves a node's state
states.md:92 | D3 | "A node that is starting, pending," | a node is never "starting"; that is the start workflow's intent overlay
states.md:93 | D3 | "connections-deleting, connections-deleted, stopping, stopped or deleting, a NodeStopped," | a node is never "deleting"; NodeDeleting is the delete workflow's intent overlay
states.md:133 | D6 | "node.get answers Current { parked }." | node.get answers the node's reported state, stopped
states.md:138 | D3/D5 | "node.delete proceeds from stopped, as deleting, to exact provider exit proof" | the node stays stopped under the delete's deleting overlay; deleted is the durable row from the provider's exit proof
node-start.md:17 | D1 | "| Completion | Hook Ok, complete topology, current time, required Build floor, `node.started`, and the mesh-primary's ReadyForTraffic on it |" | completion is node.started reporting the node's ready-for-traffic, which the MP then records
node-start.md:46 | D1 | "That frame is the node's report: the mesh-primary sets" | the node's report says it reached ready-for-traffic; the MP records and gossips it, with no status apply
node-start.md:49 | D1 | "refusal of that apply completes no readiness." | readiness is the node's reported state; no MP apply grants or withholds it
node-start.md:64 | D1 | "[mesh-primary sets ReadyForTraffic]" | "[mesh-primary records and gossips the reported ready-for-traffic]"
node-start.md:94 | D1 | "Note over M: sets ReadyForTraffic" | "Note over M: records reported ready-for-traffic"
node-stop.md:33 | D2 | "The stop's visible status is stopping; the child call keeps its events," | during the child call the status is what the node reports (connections-deleting); stopping only once the node reports it
node-stop.md:68 | D3 | "Inside [node.restart](node-restart.md), the mesh-primary publishes that stop's stopped as" | the MP gossips the node's reported stopped; NodeRestarting may sit beside it as intent, never replace it
node-restart.md:16 | D3 | "Mesh-primary authors NodeRestarting, including from stop's stopped; never NodeStopped for this restart" | the reported stopped is gossiped as stopped; NodeRestarting is only an intent overlay on top of it
node-restart.md:27 | D3 | "workflow's stopped the mesh-primary publishes NodeRestarting instead of NodeStopped." | the MP publishes the reported stopped, with the restart intent overlay kept alongside it
node-restart.md:39 | D3 | "[stop's stopped published as NodeRestarting]" | "[stop's reported stopped published; NodeRestarting overlay retained]"
node-restart.md:45 | D3 | "NodeRestarting for the whole restart, stop's stopped included;" | NodeRestarting is intent for the whole restart; the node's reported states, stopped included, are still gossiped
node-restart.md:57 | D3 | "Note over M: stop's stopped published as NodeRestarting" | "publishes the reported stopped under the NodeRestarting overlay"
node-delete.md:16 | D4 | "| Crashed target | Stop steps are already done; no call is required from an exited process |" | for a crashed target no stop ran and none is recorded; the delete rests only on the provider's exit proof
node-delete.md:27 | D4 | "For a crashed process those stop steps are already done;" | for a crashed process no stop step is run or assumed; nothing is recorded as stopped
node-delete.md:32 | D5 | "writes the node's row with status deleted;" | the durable deleted row records the provider's exit proof, an external observation, not a node-reported status
node-mesh-join.md:29 | D3 | "the node publishes nothing in that state" | joining is the primitive's progress, not a node state; the node's state stays what it last reported
node-mesh-join.md:67 | D3 | "that held it stopped or restarting hears the rejoin." | peers held it stopped (possibly under a NodeRestarting intent overlay); restarting is not a state
node-mesh-join.md:213 | D1 | "ReadyForTraffic is set by the mesh-primary after `node.started` ([node status](node-status.md))," | the node reports ready-for-traffic with node.started; the MP records and gossips it
node-mesh-leave.md:18 | D2 | "connections-deleted: `node.connections.delete { All }` completed with its receipt (`Established`, or `Deadline` with the held ids recorded)" | entry needs the node's reported connections-deleted (Established); a Deadline receipt leaves the node connections-deleting
node-mesh-leave.md:21 | D3/D6 | "`leaving` from the command until the mesh is left (and the backbone, a node-admin); the node publishes nothing in that state" | leaving is the primitive's progress, not a node state
node-connections-delete.md:18 | D1 | "Status connections-deleting, then connections-deleted, set by the node's mesh-primary" | the node reports each state as it executes; the MP records and gossips them
node-connections-delete.md:21 | D2 | "| Entry (`All`) | The [connections-deleting](states.md#who-sets-each-state) status, set at the call's admission |" | connections-deleting begins when the node reports entering the closure; admission is not a state
node-connections-delete.md:67 | D1/D2 | "The node's mesh-primary sets connections-deleting when it accepts the call." | the node enters connections-deleting as it executes and reports it; the MP records the report
node-connections-delete.md:75 | D1 | "The node's mesh-primary sets connections-deleted from that completion." | the MP records the connections-deleted that the node reports in that completion
node-connections-delete.md:81 | D4 | "attempt, or [node.stop](node-stop.md), leaves that state;" | the node leaves connections-deleting only when it reports a new state
node-status.md:5 | D1/D2 | "The internal status report and apply; not a caller op. Status moves only through the" | status changes only as the node executes; the node reports it and the MP records it, with no apply
node-status.md:26 | D1 | "A node reports to its mesh's primary, which sets its status." | "… which records and gossips the state the node reported"
node-status.md:34 | D1 | "| Caller / receiver | The node's mesh-primary, downward to the exact accepted node |" | there is no downward status apply; state flows up in the node's replies
node-status.md:36 | D2 | "connections-deleting is entered only by [node.connections.delete](node-connections-delete.md) with `{ scope: All }`" | the node enters connections-deleting while executing the All closure and reports it
node-status.md:40 | D1 | "Workflow actions run the status apply; how a node leaves the stopped status is" | workflows send commands; the node's replies report the state it reached
node-status.md:43 | D1 | "On the applied status the node adjusts its lifecycle admission." | the node changes its own state and admission as it executes and reports the result
node-recovery.md:43 | D6 | "any answer from the exact birth (`Current`, `CurrentParked` or" | a stopped node answers Current with its reported state, stopped
node-recovery.md:50 | D5 | "least one floor later is TRUE OFFLINE, written as status Dead on the node's row." | Dead is the MP's silence mark on the row, never a node status
node-tickle.md:27 | D6 | "the node answers `Current { state }`, `CurrentParked` or `Blocked { reason, retry_on }`" | a stopped node answers Current { state: stopped }
node-tickle.md:72 | D6 | "A stopped node answers `CurrentParked { node_id, incarnation }`;" | a stopped node answers Current carrying its state, stopped
node-tickle.md:112 | D6 | "N-->>C: Current / CurrentParked / Blocked — reached" | "Current { state } / Blocked — reached"
node-replace.md:16 | D6 | "Ready, and nothing routes to it; it is stopped before it takes the name and started under it" | name the reported state (pending or ready-for-traffic) and say how the successor is held out of routing
node-primary-update.md:39 | D1 | "The writer, as the mesh-primary and sole author of member status, publishes \"primary updating\":" | the mesh-primary is the sole recorder and publisher of the member status each node reports, not its author
```

### 6.2 lifecycles/ mesh and fabric pages, fabric-authority, fabric-mesh-node-lifecycles, elections, node-admin-lifecycle (57)

```text
node-admin-lifecycle.md:129 | D1 | "The Pending hand-off to a mesh's first admin is a row of [States: who sets each state]" | the first admin reports Pending itself; States lists who records and gossips each reported state
fabric-mesh-node-lifecycles.md:13 | D1 | "fabric, mesh and node lifecycle states, who sets each" | node states come only from the node's own report; the page owns who records and gossips each
fabric-mesh-node-lifecycles.md:89 | D5 | "Replacing a member whose exit its mesh-primary proved" | a member whose exit the provider proved and its mesh-primary recorded
fabric-authority.md:66 | D1 | "A mesh-primary commands its own mesh's nodes, writes their rows and announces their status" | it records and gossips the status each node reports (minor)
fabric-authority.md:190 | D1 | "The ordinary lifecycle transition machinery owns advancement out of `Pending`." | leaving Pending is what the mesh's authority reports, not something machinery performs (minor)
fabric-authority.md:329 | D2 | "Mesh join | yes | yes/new authority may appear | yes | Build + Mesh/Fabric lifecycle transition" | the Build sends commands; mesh/fabric status changes only by what its authority reports
fabric-authority.md:330 | D2 | "Mesh remove | yes | yes as estate changes | yes | Build + Mesh/Fabric lifecycle transition" | same
fabric-authority.md:332 | D2 | "Mesh replacement (mesh.delete, then a new mesh) | yes | yes | yes | Build + Mesh/Fabric lifecycle transition" | same
fabric-authority.md:345 | D2 | "it gives exact-target transport and certainty for one requested transition" | it carries a command; the transition is what the target reports it reached
fabric-authority.md:345 | D2 | "A reply never decides when a Mesh or Fabric may change state" | the authority's reply or report is what says what happened and which state it reached
fabric-node-lifecycle-elections.md:43 | D1 | "before lifecycle makes it Ready" | before the node-admin reports itself ready-for-traffic
fabric-node-lifecycle-elections.md:45 | D3 | "A node that is starting, pending, connections-deleting, connections-deleted, stopping, stopped or deleting" | starting and deleting are workflow-intent overlays; list only reported states and name the overlays separately
fabric-node-lifecycle-elections.md:149 | D5 | "watching admins record `held-running { since, verdicts }` on the holder's status row" | held-running is the watchers' external observation, kept apart from the holder's reported lifecycle state
mesh-create.md:23 | D1 | "`ready-for-traffic`, set by the fabric-primary on its mesh-primary's report" | the mesh's ready-for-traffic is what its mesh-primary reports; the fabric-primary records it
mesh-create.md:23 | D2 | "`pending`, set by the node-admin that launched the mesh's first admin, one `ApplyMeshState(Pending)` after the launched birth answers Ping" | mesh pending is what the mesh's own authority reports, not a Ping answer or an apply sent to it
mesh-create.md:41 | D1 | "sends one bounded Ping to the launched birth and, on the answer, applies the mesh's first state" | ApplyMeshState is a command; the mesh's first state is what the node-admin replies it reached
mesh-create.md:46 | D1 | "The fabric-primary, as writer of a mesh with no primary, sets the node-admin's ReadyForTraffic." | the node-admin reports ReadyForTraffic; the fabric-primary records and gossips it
mesh-create.md:54 | D1 | "it reports to the fabric-primary, which applies `ApplyMeshState(ReadyForTraffic)`" | the mesh-primary's report is the mesh's ready-for-traffic; the fabric-primary records it
mesh-create.md:64 | D1 | "The one call this arm adds is the mesh's first state." | the call is a command; the mesh's first state is the node-admin's reply
mesh-create.md:81 | D2 | "\"state\": \"Pending\"" | a request from the fabric-primary cannot be the source of the mesh's state; the state comes back in the reply
mesh-create.md:95 | D2 | "At step 8 the same request carries `\"state\": \"ReadyForTraffic\"`." | readiness arrives in the mesh-primary's report, not in a request sent to it
mesh-create.md:155 | D1 | "F->>A: 5. ApplyMeshState(Pending)" | the diagram should show A reporting Pending and F recording it
mesh-create.md:157 | D1 | "Note over A: 6. hook, topology read, node.started, fabric-primary sets ReadyForTraffic" | A reports ReadyForTraffic; F records it
mesh-create.md:164 | D1 | "F->>A: ApplyMeshState(ReadyForTraffic)" | the mesh-primary reports ready-for-traffic; F records it
mesh-create.md:188 | D2 | "Mesh Pending | fabric-primary | the Ping, then `ApplyMeshState(Pending)`'s call span" | the evidence is the node-admin's reported Pending, not the Ping or the call span
mesh-create.md:190 | D1 | "Mesh ReadyForTraffic | fabric-primary, node-admin | `ApplyMeshState(ReadyForTraffic)`" | the reporting actor is the mesh-primary; the fabric-primary only records
mesh-delete.md:19 | D3 | "Mesh state | `mesh.deleting`: the mesh-primary publishes its mesh's status, deleting" | mesh.deleting is a step/event name and deleting is a workflow-intent overlay, not a mesh state
mesh-delete.md:25 | D3 | "its final aggregate, the mesh deleted and itself deleting, is its last gossip" | deleting is an overlay; the aggregate carries the admin's reported state plus the intent overlay
mesh-delete.md:26 | D5 | "A mesh's deleted and a node's deleted release names as [States](states.md) states" | a node's deleted is the durable row from the provider's exit proof, an observation, not a node state
mesh-delete.md:38 | D3 | "`mesh.deleting`: the mesh-primary publishes its mesh's status, deleting, as its MeshStatus frame." | deleting is a workflow-intent overlay, not a published mesh status
mesh-delete.md:53 | D3 | "its final aggregate, the mesh deleted and itself deleting, is its last gossip: the node.delete's NodeDeleting in aggregate form" | NodeDeleting is an event and deleting an overlay; neither is the member's state
mesh-delete.md:55 | D5 | "fabric-primary stops it, proves its exit and writes its row deleted" | it records the provider's exit proof and writes the deleted row from that proof
mesh-delete.md:88 | D3 | "← mesh.deleting the mesh-primary's aggregate carries the mesh deleting" | the aggregate carries the deleting intent overlay, not a mesh state
mesh-delete.md:93 | D3 | "← the final aggregate from the mesh-primary: the mesh deleted, itself deleting" | "itself deleting" is an overlay, not the admin's lifecycle state
mesh-delete.md:122 | D3 | "the mesh-primary then publishes `MeshStatus` deleting" | deleting is a workflow-intent overlay, not a MeshStatus value
mesh-delete.md:160 | D3 | "\"status\": \"deleting\"," | a MeshStatus frame should not carry the deleting overlay as a status
mesh-delete.md:183 | D3 | "`mesh2.admin.1` deleting under `operation` `node.delete:mesh2.admin.1`" | the member digest carries its reported state; deleting is the overlay
mesh-delete.md:210 | D3 | "M->>B: aggregate: mesh deleting (mesh.deleting)" | the event name and the overlay are drawn as the mesh's status
mesh-delete.md:211 | D3 | "B-->>R: hold the mesh deleting and each member's own state" | peers hold the deleting overlay, not a mesh state (the second half is correct)
mesh-delete.md:215 | D4 | "M->>B: aggregate: NodeStopped" | gossip NodeStopped only after the member's node.stopped reply, which the diagram leaves out (compare 230-231)
mesh-delete.md:229 | D3 | "M->>B: final aggregate: mesh deleted, itself deleting (its last gossip)" | "itself deleting" is an overlay, not a state
mesh-investigation.md:59 | D5 | "both the mesh (`MeshStatus` degraded) and the fabric (`FabricStatus` degraded) are marked degraded (R-DG1)" | degraded is the fabric-primary's own observation, published as an observation, never as mesh-x's MeshStatus
mesh-investigation.md:68 | D5 | "The fabric-primary never authors mesh-x's MeshStatus outside the investigation arms (R-DG1)." | it never authors mesh-x's MeshStatus at all (contradicts the anchor at line 18)
mesh-investigation.md:86 | D5 | "MeshStatus and FabricStatus degraded (both arms, R-DG1)" | a degraded health observation, not a MeshStatus/FabricStatus transition
mesh-investigation.md:115 | D5 | "FP->>FP: author FabricStatus degraded" | record a degraded observation, not a fabric status
mesh-investigation.md:164 | D5 | "both the mesh (`MeshStatus` degraded) and the fabric (`FabricStatus` degraded) are marked degraded" | the observation travels as an observation, not as mesh-x's or the fabric's status
fabric-create.md:40 | D1 | "The statuses are set only by the fabric-primary and carried down in rounds, fabric-primary to mesh-primaries to nodes" | the fabric-primary authors only the fabric status; mesh and node states are what they report in their check-ins
fabric-delete.md:18 | D5 | "The fabric-primary executes each other mesh-primary's node.delete and proves its exit" | it executes the delete and records the provider's exit proof
fabric-delete.md:34 | D5 | "The fabric-primary is the executor and proves its exit." | it records the provider's exit proof
fabric-delete.md:53 | D5 | "fabric-primary is the executor and proves its exit" | it records the provider's exit proof
fabric-recovery.md:47 | D5 | "opt ladder proves no admin" | the reach ladder gives only reached/unreached; only the provider's exit proof proves absence
fabric-state-commit.md:17 | D2 | "The fabric-primary sets the fabric's status to state-commit and carries it down in a round" | CommitState is a command; each member's state comes from its own StateCommitted check-in
fabric-state-commit.md:63 | D4 | "M->>G: state-committing" | publish only after N's Applied reply (or the mesh-primary's own report); here it follows the bare command
fabric-state-commit.md:64 | D4 | "M->>B: state-committing" | same
fabric-ready-for-traffic.md:6 | D1 | "the fabric-primary carries the ready-for-traffic status down in a round: fabric-primary → mesh-primaries → nodes" | it sends OpenTraffic commands; each node reports its own ready-for-traffic via TrafficOpened
fabric-ready-for-traffic.md:48 | D4 | "M->>G: traffic-opening" | publish only after N's Applied reply
fabric-ready-for-traffic.md:49 | D4 | "M->>B: traffic-opening" | same
```

### 6.3 gossip.md and node-identity-and-state.md (47)

```text
gossip.md:6 | D1 | "Fabric, mesh and node lifecycle states and who sets them are defined only in [States](lifecycles/states.md#states)." | States should define who reports node states (the node) and who records them (the MP)
gossip.md:152 | D1 | "a heartbeat never carries or moves a status, and a member's status comes only from its mesh-primary" | a member's status is the node's own report, which its mesh-primary records and gossips
gossip.md:153 | D1 | "In `digests`, each member is a projection entry `{ digest, status }`, the status authored by the mesh-primary alone." | the status is the state the node reported, and the mesh-primary is its only publisher
gossip.md:153 | D3 | "and NodeStopped until the next lifecycle op on that exact birth" | stopped is a state the node reports; carry it as the member's status, not in in_flight next to intent overlays (§3 P6)
gossip.md:153 | D3 | "NodeStopped, NodeDeleting and NodeRestarting stay immediate events." | NodeStopped republishes a reported state; only NodeRestarting and NodeDeleting are intent overlays; do not group the three
gossip.md:154 | D1 | "In `changed`, each member is a projection entry `{ digest, status }`, the status authored by the mesh-primary alone." | same as :153
gossip.md:154 | D3 | "NodeStopped, NodeDeleting and NodeRestarting stay immediate events." | same as :153
gossip.md:156 | D3 | "the lifecycle in-flight overlay" | stopped is the member's reported status in the projection, not an in-flight (intent) overlay (P6)
gossip.md:157 | D1 | "the mesh-primary (the holder itself for the `node-admin` cohort), the sole author of member status" | the only recorder and publisher of the status its members report
gossip.md:164 | D4 | "`topology_version` the version of the subject's mesh at which the subject changed" | NodeRestarting records the authority's intent; the subject's state changes only when the node reports it
gossip.md:165 | D4 | "`topology_version` the version of the subject's mesh at which the subject changed" | NodeDeleting is intent published when the claim is adopted; no subject state change is implied
gossip.md:207 | D3 | "lifecycle facts (`NodeStopped`, `NodeDeleting`, `NodeRestarting`): `topology_version`, per publisher and subject mesh" | the reported stopped state is separate from the two intent overlays
gossip.md:261 | D3 | "operation and exact subject in the lifecycle in-flight overlay." | the node's reported stopped state is its status, not an intent overlay (P6)
gossip.md:274 | D3,D4 | "Inside node.restart, the stop's stopped is published as NodeRestarting, not NodeStopped; peers hold the instance and expect it back." | the reported stopped is gossiped as stopped with the restarting overlay beside it; conflicts with :164 (the overlay is open for the whole restart)
gossip.md:276 | D3 | "it rides the same in-flight mechanism as NodeRestarting and NodeDeleting" | the stopped state travels as status, not through the intent-overlay mechanism (P6)
gossip.md:278-279 | D2 | "node.start clears it at node.joined;" | stopped is replaced only by the node's next reported state (pending), not cleared by the workflow at a completion event
gossip.md:289-290 | D1 | "The announcer is the mesh-primary (the holder itself for the `node-admin` cohort), the sole author of member status;" | the only recorder and publisher of the status its members report
gossip.md:305,307 | D4 | "| commit-state | StateCommitting | commanding authority |" | an -ing frame published on the send is workflow intent; the subject's state comes only from its check-in (fabric round scope, P9)
gossip.md:345 | D3 | "at `mesh.deleting` the mesh-primary publishes its mesh's status, deleting, as its `MeshStatus`" | "deleting" is workflow intent, not a status; mesh.deleting is an event name (mesh scope, P9)
gossip.md:364-365 | D1,D2 | "each member's own state as its node.delete moves it" | each member's state as that member reports it while node.delete runs
gossip.md:367-368 | D5 | "A member's NodeDeleted moves it to `deleted`, where it resolves Gone for the deleted retention." | the deleted row comes from the provider's exit proof; NodeDeleted spreads that observation and moves no node state
gossip.md:680 | D5 | "A known exact deletion is membership state and a lifecycle event, never a second gossip protocol." | a deletion is the provider's exit-proof observation spread by an event, not a node lifecycle state
gossip.md:807 | D1 | "[States](lifecycles/states.md#states) owns the lifecycle states, who sets them, the rounds and" | who reports the states (the node) and who records them (the MP)
node-identity-and-state.md:21-22 | D1 | "Its mesh-primary sets a node's status from the node's reports on its workflow's reply stream" | the source is right but "sets" is wrong: the mesh-primary records the status the node reports
node-identity-and-state.md:158-159 | D5 | "The node's mesh-primary writes the row with status deleted, keeping the record and its config;" | the deleted row is written from the provider's exit proof, an external observation, not a reported status
node-identity-and-state.md:203-205 | D1,D5 | "a member that died, was deleted, or moved. That is a membership event in its own mesh's rows, authored by that mesh's own primary" | death and deletion are observations or provider proof; state changes are the node's reports; the MP records each kind separately
node-identity-and-state.md:269 | D3,D5 | "starting → pending → ready-for-traffic → connections-deleting → connections-deleted → stopping → stopped → deleting → deleted" | the states are pending through stopped; starting and deleting are intent overlays; deleted is the provider-proof row
node-identity-and-state.md:269-270 | D5 | "happens on a REAL membership, reachability or role event" | a status changes only when the node reports it; reachability is an observation
node-identity-and-state.md:315 | D2 | "It writes `ops.nodes` when a node workflow step or a node.update reaches it" | a status write follows the node's reply, not the arrival of a workflow step
node-identity-and-state.md:315-316 | D5 | "when a status change happens INORGANICALLY — a node lost, a role moved, anything the mesh observes rather than was asked for" | things the mesh observes are recorded as observations (PendingReconnect, Dead, provider proof), not as status changes
node-identity-and-state.md:465-466 | D3 | "starting, pending, ready-for-traffic, connections-deleting, connections-deleted, stopping, stopped, deleting and deleted" | remove starting and deleting (intent overlays) and deleted (the provider-proof row) from the status list
node-identity-and-state.md:466 | D1,D2 | "each set by its workflow as" | each state is reached by the node and reported; the MP records it
node-identity-and-state.md:481 | D3 | "Alive → Restarting                    the mesh-primary's NodeRestarting overlay for an open node.restart" | Restarting is an intent overlay, not a NodeState
node-identity-and-state.md:482 | D1,D3,D6 | "*     → Leaving                       set by the mesh-primary on node.stopped; published as NodeStopped" | the node's own reported stopped is recorded and gossiped as NodeStopped; there is no Leaving state
node-identity-and-state.md:486-487 | D1,D3,D5 | "inferred by an observer from absence, or set from its mesh-primary's lifecycle overlays (`NodeStopped`, `NodeRestarting`)" | NodeState is only the liveness the node publishes about itself; observations and overlays are separate things
node-identity-and-state.md:497-498 | D3 | "A node holding `Restarting` is EXEMPT from the staleness sweep's flip to `PendingReconnect`" | a node under the restarting overlay, not a node holding a Restarting state
node-identity-and-state.md:512 | D1,D3,D6 | "`Leaving` | set by the mesh-primary on `node.stopped` (overlay `NodeStopped`)" | this row is stopped, as the node reports it and the MP records and gossips it
node-identity-and-state.md:515 | D3 | "`Restarting` | the mesh-primary's `NodeRestarting` overlay for an open node.restart" | an overlay, not a row in the state table
node-identity-and-state.md:573 | D1 | "its mesh-primary sets ReadyForTraffic on that report" | the node reaches ready-for-traffic and reports it; its mesh-primary records that state
node-identity-and-state.md:600-601 | D2 | "Admission, not a socket or an unanswered stream, establishes completion." | completion is the node's reply that the admitted join ran; admission alone does not (borderline)
node-identity-and-state.md:633 | D3 | "| **Stopped** | the mesh-primary's `NodeStopped` overlay | node.stop's `node.stopped` |" | the node's reported stopped state, gossiped by the MP as NodeStopped, not an overlay
node-identity-and-state.md:645 | D3,D5 | "It goes `Restarting` or `PendingReconnect` and stays in the projection's kind index" | the row holds a restarting overlay or a PendingReconnect observation; neither is the node's state
node-identity-and-state.md:649 | D5 | "the row stays with status deleted while the WAL stays in the data dir" | the deleted row is the provider's exit-proof observation, not a status
node-identity-and-state.md:660 | D1 | "present in every topology, marked stopped" | the node holds the stopped state it reported
node-identity-and-state.md:819 | D6 | "`rafka.mesh.connection.resolve.via-node-pending-delete-received`" | deleting-overlay vocabulary; "pending-delete" is not a name in the contract
node-identity-and-state.md:820 | D1,D3 | "`rafka.mesh.connection.update.via-node-restarting-marked`" | a restarting overlay was received, not a node marked restarting
node-identity-and-state.md:822 | D6 | "`rafka.mesh.connection.delete.via-gossip-pending-delete-broadcast`" | deleting-overlay vocabulary; "pending-delete" is not a name in the contract
```

### 6.4 node-rpc.md and node-rpc-envelope.md (24)

```text
node-rpc.md:625 | D6 | "The protocol supplies constructors for shared states and a classifier for decoded replies." | shared refusal or reply classes, not "states"; lifecycle states are only the node's reported states
node-rpc.md:908 | D3/D6 | "Lifecycle vocabulary is fixed:" (:911-914 map not initialized=NotReady, overloaded=Busy, op not served=Unserved) | runtime refusal or reply classes, not lifecycle vocabulary; envelope :321 says NotReady "is never a lifecycle fact"
node-rpc-envelope.md:245 | D6 | "Current; stopped answers CurrentParked; no event" | a stopped node answers Current { state: stopped }; drop "parked"
node-rpc-envelope.md:253 | D3 (mesh) | "mesh.deleting, mesh.deleted" | "deleting" is a workflow-intent word; as a published event it reads as a state (P9)
node-rpc-envelope.md:278 | D1 | "| `status` | RDM | [node status](lifecycles/node-status.md) only | — | that status's own completion |" | status comes from the node's own report; the MP records and gossips it; RDM does not set it
node-rpc-envelope.md:302 | D3 | "Mesh and fabric workflows compose node.stop and node.delete as [mesh.delete] and [fabric.delete] state" | mesh.delete and fabric.delete are workflows that compose node workflows, not "state"
node-rpc-envelope.md:571 | D1 | "Direction: upward for declarations; downward for applies and probes." | node state goes upward as the node's report; downward calls are commands or probes, none applies a state (P4)
node-rpc-envelope.md:571 | D1 | "Every node's status, node-admins included, is set by its mesh's primary on the node's report, and the mesh-primary sets its own" | each node reports its own status; its mesh-primary records and gossips that report
node-rpc-envelope.md:571 | D1 | "Authority applies/probes the exact node birth; fabric-primary applies mesh state at its bootstrap admin" | the authority commands or probes a birth; state comes only from the reply or declaration
node-rpc-envelope.md:573 | D1 | "Typed stale, authority and invalid-transition refusals are separate from runtime refusals." | an invalid-transition refusal assumes the receiver moves node state; the MP records what the node reports
node-rpc-envelope.md:581 | D1 | "Lifecycle callers MUST NOT use ApplyNodeState(ConnectionsDeleting); ApplyNodeState carries the permitted transitions other than connections-deleting." | no caller applies a node transition; a command runs on the node and the node reports the state it reached (P4)
node-rpc-envelope.md:581 | D1/D2 | "Backward transitions are refused by name." | applied to DeclareNodeState this refuses to record what the node reported; the node's report is authoritative for its own state
node-rpc-envelope.md:586 | D6 | "pub enum NodeState {" (Pending, ReadyForTraffic, ConnectionsDeleting, Stopped) | the contract's six states include connections-deleted and stopping; four cannot carry the full report (P5)
node-rpc-envelope.md:593 | D3 (mesh) | "Positional postcard discriminants: Pending=0, ReadyForTraffic=1, Deleting=2, Deleted=3." | MeshState::Deleting puts the intent word among states (P9)
node-rpc-envelope.md:609 | D1 | "ApplyNodeState { node_id: NodeId, incarnation: IncarnationId, state: NodeState }," | a downward request carrying a target state moves the node's state from outside; replace with commands whose replies report the state reached (P4)
node-rpc-envelope.md:697 | D1 | "RejectedInvalidNodeTransition { current: NodeState }," | refusals name a command the node cannot execute, with its reported current state; not an invalid authority-applied transition
node-rpc-envelope.md:707 | D2/D3 | "Started," (with :708 "StartFailed { step: String, reason: String },") | the StartNode reply should say what happened and the state the node reached (pending or ready-for-traffic); otherwise any state recorded comes from the command completing
node-rpc-envelope.md:709 | D6 | "CurrentParked { node_id: NodeId, incarnation: IncarnationId }," | a stopped node answers Current { state: stopped }; "parked" is not a contract state name
node-rpc-envelope.md:738 | D2 | "MeshJoined { node_id: NodeId, incarnation: IncarnationId, joined: Vec<WhichMesh> }," | the JoinMesh reply should also carry the state the node reached
node-rpc-envelope.md:739 | D2 | "MeshLeft { node_id: NodeId, incarnation: IncarnationId, left: Vec<WhichMesh> }," | the LeaveMesh reply should also carry the state the node reached
node-rpc-envelope.md:758 | D1 (fabric) | "The fabric statuses state-commit and ready-for-traffic are carried down in rounds on op" (:759 "CommitState / OpenTraffic carry the status down") | CommitState and OpenTraffic are commands; status is what the subjects report back in StateCommitted and TrafficOpened (P9)
node-rpc-envelope.md:817 | D2 (mesh) | "its admission starts mesh.deleting." | admitting DeleteMesh starts only the workflow; no state or event follows from admission until the mesh-primary reports (P9)
node-rpc-envelope.md:942 | D6 | "pub enum MemberStatus {" (Pending, ReadyForTraffic, ConnectionsDeleting, Stopped) | the gossiped member status omits connections-deleted and stopping (P5)
node-rpc-envelope.md:1267 | D3 | "// RecipeStep = Create | Stop | Update | Start | Delete: one node primitive per step." | node.stop and node.start are workflows that compose primitives, not node primitives
```

## 7. Anchors to keep (172 lines that already say it right)

By page, line numbers only; the readers' quotes are in their transcripts and every one is on main at `4de221c3`.

- states.md 14, 110, 118, 119, 139, 152, 167 · node-start.md 6, 75 · node-stop.md 8, 20, 39 · node-restart.md 5, 19, 30 · node-create.md 52, 64, 67, 89 · node-delete.md 17, 19, 22, 27, 35, 55, 58 · node-mesh-join.md 65, 66, 73, 127, 256 · node-mesh-leave.md 23 (twice) · node-connections-delete.md 43, 50, 77, 83 · node-status.md 24, 44 · node-name-update.md 52 · node-config-update.md 15 · node-recovery.md 22, 37, 51, 57, 85, 109 · node-tickle.md 8, 44, 69, 133 · node-runtime-inspect.md 8, 27, 29 · node-topology-get.md 15, 69 · node-replace.md 84 · node-primary-update.md 41, 73, 113.
- node-admin-lifecycle.md 31, 36, 77, 125, 170 · fabric-mesh-node-lifecycles.md 83, 84, 146, 150 · fabric-authority.md 17, 106, 129, 184, 186, 192, 208, 256, 294 · fabric-node-lifecycle-elections.md 29, 93, 147, 198, 216 · mesh-create.md 100 · mesh-delete.md 7, 23, 28, 67, 95, 218, 242 · mesh-investigation.md 9, 18, 32, 40, 76, 159, 162, 167, 193, 203, 231, 273, 275, 279 · fabric-delete.md 22, 65 · fabric-recovery.md 74, 75, 79, 99 · fabric-state-sync.md 76 · fabric-ready-for-traffic.md 8.
- gossip.md 94, 156, 165, 166, 201, 230-231, 232-233, 233-234, 272, 332-333, 354-355, 407-408, 620, 658, 689, 693-695, 810, 812, 869, 882 · node-identity-and-state.md 22-24, 29-30, 362-364, 491-493, 546-547, 580, 639, 674, 732, 806.
- node-rpc.md 133, 149, 151, 153, 384, 557, 589, 900 · node-rpc-envelope.md 199, 217, 238, 239, 243, 244, 321, 579, 581, 768, 771, 803, 846, 853, 886, 985, 1157, 1181, 1200, 1218, 1410-1412.

The anchors are the pages' own statement of the contract. The repair is to make the rest of each page agree with them.

## 8. What needs Luke, one at a time (none of these is decided here)

1. **states.md's table**, rewritten under the three-way split (the lead's draft of 2026-10-15: the node's six reported states; `starting`, `restarting`, `deleting` as overlays; `deleted`, `PendingReconnect`, `Dead` and the provider facts as the MP's observations; the six sentences of §1 above the table). R-HB1's "authors and publishes" becomes "records and publishes" in the same landing.
2. **`stopped` as the member's status or as an in-flight overlay** (P6). The contract says state; gossip.md carries it as an overlay derived from Build facts. One answer, then gossip.md, states.md:119, node-stop.md:20 and node-restart.md agree.
3. **One state vocabulary** (P5): the six names on the wire and in code, declared by the node at each transition, replacing `Draining`/`Leaving`/`ConnectionsDeleting`/`Stopped` across `NodeState`, `MemberStatus`, `NodeStatus` and the Members projection. A durable format move and a gossip change: permission and a flame.
4. **`ApplyNodeState` and `RejectedInvalidNodeTransition`** (P4): remove from the contract, keep the 0x1B positions reserved. Same for `CurrentParked` (P8).
5. **Does the rule apply one level up** (P9): mesh and fabric states only from their authority's check-in; the rounds' `-ing` frames stay intent overlays.
6. **The restart's stopped** (node-restart.md:16, :27): gossiped as `stopped` with the overlay beside it, or kept as today. Falls out of question 2.

## 9. Order of repair

1. The page edits, one page per landing, each with Luke's yes on exact text, states.md first (it is the table every other page cites), then node-status.md (the "Status apply" section goes), then node-stop/restart/connections-delete, then gossip.md and node-identity-and-state.md, then the envelope, then the mesh and fabric pages once question 5 is answered.
2. The code, one landing: one report-processing path (`status_rpc.rs:893-905`), the pipeline's five writes and runner.rs's `admitted`/`NodeLeaving` deleted, `NodeStatus::Restarting` moved to the overlay; a cell that a `NotSent` command invents no transition and a replay restores none. This is i143 e12.s89's D2/D2b row grown to its real size.
3. The vocabulary move (question 3), its own landing with the marker and the flame, after 1 and 2.
