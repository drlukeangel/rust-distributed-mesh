Q47: A (Luke, 2026-10-17). Q49: A folds into Q47: applied operation effects survive cycle begins. Exact page replacements await Luke’s yes.

# Q47 — move node.stop and node.start onto the NodeCommand 0x22 stream

### 1. Spec

`docs/architecture/lifecycles/node-stop.md`, “Command and completion messages”:

```text
node.stop
  ← Started
  ← node.connections.deleted ConnectionsReceipt [Established { in_flight, closed } | Deadline { last_in_flight, closed, held }]
  ← node.stopped
```

“These are reply frames on one call”; steps 2–5 put the whole closure, transport cut and stopped report on that stop's execution. `node-start.md`, “Command, completion and gossip messages”, is a decoded caller progress view:

```text
node.start
  ← Started
  ← node.joined
  [hydrate_before_ready]
  [node.topology.get, missing Build floor, rafka-time last]
  ← node.started
  [mesh-primary sets ReadyForTraffic]
  ← Complete
```

Its step 5: “That frame is the node's report”; “The node sends no separate status report after node.started.” `lifecycles/states.md`, “Who sets each state”: the mesh-primary sets pending from the node.joined report, ReadyForTraffic from node.started, stopping from stop admission, stopped from the matching stopped frame. Owner: the exact node executes; its mesh-primary records the node's reports.

Q6/Q7 in `docs/plans/i144-provider-v2-prd.md`: “Moving stop and start onto it is a drift item”; NodeCommand 0x22 is server-streaming, never forwarded. The current pattern is `node-connections-delete.md:28–44`: mesh-primary → exact node, `CommandAccepted`, progress, terminal; admission is not completion. `gossip.md:231`: “node.connections.delete and node.stop are calls, not gossip”. The envelope's typed-object rows still name Status for stop/start; those wire mappings must move with this drift correction.

Q36 remains binding: `node-stop.md` steps 4–5 sends the owed Stopped declaration naming the operation whether or not the stop stream is open; matching frame or declaration records/publishes once, the other is AlreadyApplied. It supplies the stopped report if the command stream is lost.

### 2. Issue

Stop/start still use one unary Status 0x1B completion and obtain states through other calls instead of their command's progress frames; observed once each in the latest successful same-birth live stop/start control on `ba3c7124`, and structurally in both protocol handlers. Tracking: [rafka-v2#3129](https://github.com/drlukeangel/rafka-v2/issues/3129), the i144 Q6/Q7 drift.

### 3. Flame

**Observations.** Live cell: `node_restart::a_fresh_restart_of_a_stopped_node_is_refused_by_name_and_node_start_is_served`, green in `gates-e7s3-ba3c7124.log.d/node_restart.log`. It stops and starts `mesh1.rpc.2` with the same node id and incarnation. [Stop](http://localhost:16686/trace/3a92a5fac5162d9b0f34ddd888419c8e) and [start](http://localhost:16686/trace/16a226cdbe8968f020962e9db247c3f1) show `protocol=status`, `op=27`, one final outcome (`stopped` / `started`). The [start's own declaration record](http://localhost:16686/trace/8caaf1608894be4608bda26f865f5b46) says `operation=node.start:mesh1.rpc.2 state=ReadyForTraffic outcome=applied`; its separate [receiver call](http://localhost:16686/trace/dad572f36ea932bd89e20573069fef78) records `declare-node-state`, not a NodeCommand stream frame.

The three span records below are pasted verbatim from the retained JSONL:

```json
{"attributes":{"busy_ns":"1601131","caller_system":"rdm","code":"OK","code.filepath":"crates/rdm-transport/src/server.rs","code.lineno":"119","code.namespace":"rdm_transport::server","detail":"stopped","idle_ns":"204198834","op":"27","peer":"ecd73147c53e55759a5aead0886006c76acce2b04c4e96a6d6e979a906fae9f8","protocol":"status","thread.id":"7","thread.name":"tokio-rt-worker"},"end_unix_nano":1792265885692609226,"events":[],"name":"rdm.node_rpc.request.serve.via-direct","parent_span_id":"6b5e52d7f5071e03","service":"rafka-rpc-node","span_id":"2d3977d2081ef7f7","start_unix_nano":1792265885486810717,"trace_id":"3a92a5fac5162d9b0f34ddd888419c8e"}
{"attributes":{"busy_ns":"14300569","caller_system":"rdm","code":"OK","code.filepath":"crates/rdm-transport/src/server.rs","code.lineno":"119","code.namespace":"rdm_transport::server","detail":"started","idle_ns":"446052021","op":"27","peer":"ecd73147c53e55759a5aead0886006c76acce2b04c4e96a6d6e979a906fae9f8","protocol":"status","thread.id":"12","thread.name":"tokio-rt-worker"},"end_unix_nano":1792265887130873786,"events":[],"name":"rdm.node_rpc.request.serve.via-direct","parent_span_id":"237d099155b3acf1","service":"rafka-rpc-node","span_id":"f6f2ea6c76b55abd","start_unix_nano":1792265886670523212,"trace_id":"16a226cdbe8968f020962e9db247c3f1"}
{"attributes":{"busy_ns":"10736","code.filepath":"crates/rdm-node/src/node.rs","code.lineno":"1205","code.namespace":"rdm_node::node","definitive":"true","idle_ns":"4896","node":"mesh1.rpc.2","operation":"node.start:mesh1.rpc.2","outcome":"applied","route":"direct","state":"ReadyForTraffic","thread.id":"7","thread.name":"tokio-rt-worker","to":"mesh1.admin.1"},"end_unix_nano":1792265887501602975,"events":[{"attributes":{"code.filepath":"crates/rdm-node/src/node.rs","code.lineno":"1206","code.namespace":"rdm_node::node","level":"INFO","target":"rdm_node::node"},"name":"declared own state to the holder of the mesh's primary seat","time_unix_nano":1792265887501600223}],"name":"rdm.node_rpc.status.update.via-declare-own","parent_span_id":"","service":"rafka-rpc-node","span_id":"0d8413d51acb0435","start_unix_nano":1792265887501588079,"trace_id":"8caaf1608894be4608bda26f865f5b46"}
```

Canonical evidence, including the complete selected traces and import payload: `/home/admin/rust-distributed-mesh-deploy/kept/i146-q47-stop-start-ba3c7124-member/`. All four trace links were read back from Jaeger. The declaration's sender record is a separate root; operation, subject and the controlled cell correlate it, not a fabricated parent relationship.

### 4. Observations (commit ba3c7124)

- [code] `crates/rdm-client/src/calls.rs:286,299`: stop/start construct StatusRequest::StopNode / StartNode and call the unary `status` path. `crates/rdm-admin/src/node_commands.rs:215,223` uses those calls for the mesh-primary's exact birth command, with the existing owner bounds.
- [code] `crates/rdm-admin/src/status_rpc.rs:581,928`: the node-admin's Status handler dispatches both commands to `own.serve` and returns a single StatusReply (:943).
- [code] `crates/rdm-node/src/node_self.rs:796–821`: both operations enter that same Status execution path. Stop runs the closure and transport cut (:973–1047), then returns `StatusReply::Stopped { receipt }` (:968), with no intermediate wire frame. Its Q36 declaration is sent at :1053–1054.
- [code] `crates/rdm-node/src/node_self.rs:1140,1150`: start awaits session.rejoin and returns `StatusReply::Started`. `crates/rdm-node/src/node.rs:1089–1095` queues the owed ReadyForTraffic state and spawns its declaration; :1208 calls `Status` separately.
- [code] `crates/rdm-admin/src/admin/runner.rs:721,861–865`: standalone stop and start use the existing pipeline, preserving the same birth and step receipts. `admin/session.rs:215–218` begins the new cycle before the start command is sent; the stream correction must decide the state from received node frames.
- [code] `crates/rdm-core/src/node_command.rs:194,254–259`: 0x22 has one request kind, DeleteConnections. Its existing ConnectionsDeleted is Terminal; it cannot become a Data frame for stop without breaking the already-landed connections.delete contract.
- [code] `docs/reference/rdm/wire.md:211,265`: Status requests StopNode and StartNode occupy positions 8 and 16. Their reserved positions must not be deleted or reused. Existing Status reply positions 17–19 stand too.
- [flame] The live stop/start server spans are unary Status op 27; the node separately declares the start operation's ReadyForTraffic state as applied.

#### Inferred

- [inferred] The state declaration and start completion belong to the same start because the one controlled cell commands that exact birth once and the declaration names that start's operation. The detached declaration span does not prove a parent/child trace edge.

### 5. Delta

Spec says stop/start progress facts are carried by one command reply stream; code has no such path for those progress facts, returning unary Status completions and separately declaring state instead.

### 6. Arms

Every arm assumes the settled lifecycle sequences, exact-birth ownership, Q36 stopped report, existing bounds and positional wire rules stand.

**A. Move the mesh-primary → node stop/start hop onto 0x22.** Append StopNode and StartNode requests carrying their existing exact birth and Build/attempt/operation identity. Append typed progress and terminal replies; reuse the header's invocation/sequence and existing stream transport:

```text
StopNode:  CommandAccepted → StopConnectionsDeleted { receipt } → NodeStopped
StartNode: CommandAccepted → NodeJoined → NodeStarted
failure after acceptance: Failed { step, reason, code } (terminal)
```

CommandAccepted is admission, not node.start's completion. StopConnectionsDeleted is a new Data reply for the page's node.connections.deleted; the existing ConnectionsDeleted stays Terminal for connections.delete. NodeJoined is Data and NodeStarted is Terminal; the outer Build emits Complete after the mesh-primary records the final report. No caller/Forward streaming extension.

The mesh-primary applies only reports it receives: stop admission → stopping, start execution → starting, NodeJoined → pending, NodeStarted → ReadyForTraffic, matching NodeStopped → stopped/overlay. Remove pre-send state changes and the open-start stream's separate status declaration; retain its owed state for recovery/seat movement and Q36's stopped report when the stream is unavailable (including its existing once-only reconciliation). The target's closure, start gates, operation/cycle idempotence, typed refusal/code constructors and Build receipts stay the execution authority. A refusal or NotSent establishes no admitted state; broken after admission is incomplete.

Reserve and refuse Status request positions 8 and 16 by name; retain every existing request/reply position and all earlier 0x22 bytes. Update the envelope's two typed-object rows, Status reserved schemas and 0x22 schemas with the code. **Wire format moved**; Status and NodeCommand reseals, new goldens by hand, no dual decoder. No durable format addition is proposed.

RED commits first: both frame sequences (and same-frame replay once); the old Status positions refused with nothing executed; never-sent controls; no state written before admission; node.started only after all gates and ReadyForTraffic recorded before outer Complete; failed join/hook/topology/floor/time produces no later success; stop visible as stopping through its closure; Q36 lost-stream declaration and duplicate publication control; live stop/start retains the birth/process/endpoint/port. Existing e1/e2 cells, operation receipts and bounds stand. One landing through the land script with a live flame; this packet contains no implementation.

**x. Another mapping within the settled one-stream contract:** specify which wire or owner choice changes.
