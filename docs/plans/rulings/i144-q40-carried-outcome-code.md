**Ruled B — Luke, 2026-10-17. Typed cause beside explanation; Forward-only wire change, own landing and reseal. Exact page replacement awaits Luke.**

# Q40 — the carried outcome loses its typed cause before code classification

## 1. Spec

Luke's e5 assignment (2026-10-17), i144 PRD e5 mapping: `DEADLINE_EXCEEDED` for any lapse of the owner's bound; `UNKNOWN` only for a lost reply or a reset after the commit cut; `UNAVAILABLE`, detail `receiver-gone`, for a receiving birth proven exited or removed. No wire bytes change. node-rpc.md §25: “It never infers meaning from reply bytes or strings”. Forward §36.1: the carrier performs the inner call, then hands its certainty to the origin; the origin decodes the final protocol's reply. The carrier owns the inner attempt; the origin owns the outer invocation.

## 2. Issue

The existing carried-exit cell passes 1/1 but its typed `ReceiverGone` becomes a free-form `InnerIndeterminate.reason: String`, leaving the origin no typed discriminator to derive the required code apart from lost reply or deadline.

## 3. Flame

### Observations

[Live carried exit](http://localhost:16686/trace/9414e27de4444937f1a0eedd357dd570), retrieved from Jaeger (9 spans), complete JSON under `target/evidence/outcome-codes/carried-exit.json`.

```json
{"attributes":{"attempts":"1","busy_ns":"9536","code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"1161","code.namespace":"rdm_transport::client","delivery_id":"5pgn97ze04wa/1m0acqyy5yz3#1","idle_ns":"3072","reason":"incarnation yhhzfmmvzy4m exited or removed","thread.id":"2","thread.name":"forward::a_target_whose_exit_is_proven_after_it_applied_answers_the_origin_carried_receiver_gone"},"end_unix_nano":1792244041733416543,"events":[{"attributes":{"code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"1162","code.namespace":"rdm_transport::client","level":"INFO","target":"rdm_transport::client"},"name":"the receiving birth ended: the request is owed no longer","time_unix_nano":1792244041733415263}],"name":"rdm.node_rpc.delivery.remove.via-receiver-gone","parent_span_id":"2fe6e516b7f73c24","service":"rdm-outcome-carried","span_id":"e5af103bb17adda1","start_unix_nano":1792244041733404735,"trace_id":"9414e27de4444937f1a0eedd357dd570"}
{"attributes":{"attempts":"1","busy_ns":"230335","code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"687","code.namespace":"rdm_transport::client","delivery":"receiver-gone","delivery_id":"5pgn97ze04wa/1m0acqyy5yz3#1","elapsed_ms":"2","idle_ns":"2185869","op":"94","outcome":"Indeterminate","reason":"ReceiverGone(\"incarnation yhhzfmmvzy4m exited or removed\")","reused":"false","target":"ExactNode(NodeId(\"0pkk9w7xy9m1\"))","thread.id":"2","thread.name":"forward::a_target_whose_exit_is_proven_after_it_applied_answers_the_origin_carried_receiver_gone"},"end_unix_nano":1792244041733533007,"events":[{"attributes":{"addr":"127.0.0.1:35102","code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"472","code.namespace":"rdm_transport::client","incarnation":"yhhzfmmvzy4m","level":"INFO","node":"mesh1.rpc.3","node_id":"0pkk9w7xy9m1","peer":"0721d79873","step":"resolved","target":"rdm_transport::client"},"name":"the target resolved to one birth","time_unix_nano":1792244041731129939},{"attributes":{"addr":"127.0.0.1:35102","code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"472","code.namespace":"rdm_transport::client","incarnation":"yhhzfmmvzy4m","level":"INFO","node":"mesh1.rpc.3","node_id":"0pkk9w7xy9m1","peer":"0721d79873","step":"resolved","target":"rdm_transport::client"},"name":"the target resolved to one birth","time_unix_nano":1792244041731173187},{"attributes":{"code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"519","code.namespace":"rdm_transport::client","level":"INFO","step":"dialed","target":"rdm_transport::client","waited_ms":"1"},"name":"a connection to the target is held","time_unix_nano":1792244041732232177},{"attributes":{"code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"567","code.namespace":"rdm_transport::client","level":"INFO","step":"stream-opened","target":"rdm_transport::client","waited_ms":"0"},"name":"a request stream is open","time_unix_nano":1792244041732238225},{"attributes":{"addr":"127.0.0.1:35102","code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"472","code.namespace":"rdm_transport::client","incarnation":"yhhzfmmvzy4m","level":"INFO","node":"mesh1.rpc.3","node_id":"0pkk9w7xy9m1","peer":"0721d79873","step":"resolved","target":"rdm_transport::client"},"name":"the target resolved to one birth","time_unix_nano":1792244041733202783},{"attributes":{"code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"714","code.namespace":"rdm_transport::client","level":"INFO","target":"rdm_transport::client"},"name":"node rpc call finished","time_unix_nano":1792244041733531839}],"name":"rdm.node_rpc.request.update.via-call","parent_span_id":"c197d0ce7f590153","service":"rdm-outcome-carried","span_id":"2fe6e516b7f73c24","start_unix_nano":1792244041731117379,"trace_id":"9414e27de4444937f1a0eedd357dd570"}
{"attributes":{"attempts":"1","busy_ns":"309951","code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"687","code.namespace":"rdm_transport::client","delivery":"acked","delivery_id":"5pgn97ze04wa/1m0acqyy5yz3#1","elapsed_ms":"4","idle_ns":"4538122","op":"26","outcome":"Reply","reason":"","reused":"false","target":"ExactNode(NodeId(\"8f6x3bw386jh\"))","thread.id":"2","thread.name":"forward::a_target_whose_exit_is_proven_after_it_applied_answers_the_origin_carried_receiver_gone"},"end_unix_nano":1792244041733750415,"events":[{"attributes":{"addr":"127.0.0.1:55900","code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"472","code.namespace":"rdm_transport::client","incarnation":"kyze11tmc65b","level":"INFO","node":"mesh1.rpc.2","node_id":"8f6x3bw386jh","peer":"d600dc6089","step":"resolved","target":"rdm_transport::client"},"name":"the target resolved to one birth","time_unix_nano":1792244041728930150},{"attributes":{"addr":"127.0.0.1:55900","code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"472","code.namespace":"rdm_transport::client","incarnation":"kyze11tmc65b","level":"INFO","node":"mesh1.rpc.2","node_id":"8f6x3bw386jh","peer":"d600dc6089","step":"resolved","target":"rdm_transport::client"},"name":"the target resolved to one birth","time_unix_nano":1792244041729008966},{"attributes":{"code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"519","code.namespace":"rdm_transport::client","level":"INFO","step":"dialed","target":"rdm_transport::client","waited_ms":"1"},"name":"a connection to the target is held","time_unix_nano":1792244041730462964},{"attributes":{"code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"567","code.namespace":"rdm_transport::client","level":"INFO","step":"stream-opened","target":"rdm_transport::client","waited_ms":"0"},"name":"a request stream is open","time_unix_nano":1792244041730471924},{"attributes":{"code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"591","code.namespace":"rdm_transport::client","level":"INFO","remaining_ms":"9998","step":"remaining-measured","target":"rdm_transport::client"},"name":"the remaining budget at the request write","time_unix_nano":1792244041730477188},{"attributes":{"code.filepath":"crates/rdm-transport/src/client.rs","code.lineno":"714","code.namespace":"rdm_transport::client","level":"INFO","target":"rdm_transport::client"},"name":"node rpc call finished","time_unix_nano":1792244041733749247}],"name":"rdm.node_rpc.request.update.via-call","parent_span_id":"375d72af35e8754d","service":"rdm-outcome-carried","span_id":"331dd529037bdee6","start_unix_nano":1792244041728904134,"trace_id":"9414e27de4444937f1a0eedd357dd570"}
```

## 4. Code

### Observations

Base `bebcb1dc`; RED `1554b763`; the flame instrumentation is the working candidate.

- [code] crates/rdm-core/src/forward.rs:72-78: `InnerIndeterminate` has only `reason: String`; `InnerNotSent` likewise has only free-form text.
- [code] crates/rdm-transport/src/forward.rs:191: every inner `IndeterminateReason` becomes `format!("{:?}", i.reason())` in that one variant.
- [code] crates/rdm-core/src/outcome.rs:448 (base): the origin rebuilds `IndeterminateReason::Carried(reason)`, not `ReceiverGone`, `ReplyDeadline` or `ReplyLost`.
- [flame] the inner call ends `Indeterminate`, delivery `receiver-gone`; the outer Forward call ends `Reply`, delivery `acked`.
- [code] the classifier takes only `&ForwardReply`; it has neither the original typed reason nor the inner op for an opaque `Forwarded` body.

### Inferred

- [inferred] without a wire change or interpreting reason strings, a classifier can describe the carrier's answered operation, but cannot recover the inner cause's required code from the current Forward variant.

## 5. Delta

Spec says the receiver-gone fact is carried as an outcome distinguishing UNAVAILABLE from DEADLINE_EXCEEDED and UNKNOWN; code has no typed path for that distinction across Forward.

## 6. Arms

Every arm assumes the e5 codes retain the distinction between certainty and failure cause.

A. The Forward call's code describes the carrier's own answered operation (`OK` for a carried result); its typed detail identifies InnerNotSent/InnerIndeterminate/etc. The final call's code requires a separate ruling for carrying its typed cause before e5.2 uses that code. Wire unchanged; same carrier; same timer and tiers; separates outer and inner outcomes explicitly.

B. Carry a typed reason (or the derived standard code) beside the existing explanation. Wire changes and is resealed by an explicit exception; same publisher, timer and tier; the origin then derives the exact inner code without reading strings.

No string-prefix classifier is proposed: it contradicts §25 and would assign semantics to human explanations.
