# Q46 — probe exit without a span file: the first-flush hypothesis is not reproduced

### 1. Spec

Q42, recorded in `docs/plans/i146-owed-messages-prd.md` (e2's telemetry history): “The drain flushes the evidence processor first and on its own”, before collector shutdown, under one 2 s deadline. Luke's follow-up: “the flush must create and write the file when any span is pending, whether or not the export thread's first export has opened it.” Owner: the process telemetry drain. Sequence: end spans → flush the evidence queue (including its first create/write) → collector shutdown → exit, or name the bounded loss.

### 2. Issue

Gate `55c015ab` failed at `probe_root_span.rs:101`: the first probe returned JSON `Reply`, but no new span file was found (0, expected 1); one observed failure, run 0 of the planned 40, and the gate is held, not rerun. Tracking: [rafka-v2#3129](https://github.com/drlukeangel/rafka-v2/issues/3129).

### 3. Flame

**Observations.** [Trace 9905221ef1e627287573a3a7d8c7812e](http://localhost:16686/trace/9905221ef1e627287573a3a7d8c7812e) contains the server's successful `get`/`absent` response. These are the retained server spans verbatim; this is server evidence, not a recovered probe export:

```json
{"attributes":{"busy_ns":"84655","code.filepath":"crates/rdm-test/src/proof_store.rs","code.lineno":"503","code.namespace":"rdm_test::proof_store","idle_ns":"1282091","incarnation_id":"5wkgev4khen0","node":"mesh1.rpc.1","node_id":"zq0dvccbkwn5","op":"get","outcome":"absent","thread.id":"683","thread.name":"tokio-rt-worker"},"end_unix_nano":1792264735326927465,"events":[],"name":"rdm.node_rpc.proof_store.serve.via-request","parent_span_id":"82cc07f9aea57160","service":"testkit","span_id":"2e1b6b741ff97c92","start_unix_nano":1792264735325562607,"trace_id":"9905221ef1e627287573a3a7d8c7812e"}
{"attributes":{"busy_ns":"129759","caller_system":"rdm","code":"OK","code.filepath":"crates/rdm-transport/src/server.rs","code.lineno":"119","code.namespace":"rdm_transport::server","detail":"absent","idle_ns":"1407802","op":"112","peer":"ac797fa457230b62d6a2493996705481c075cb6a294b00f93fd2bcddaf17cd92","protocol":"proof-store","thread.id":"683","thread.name":"tokio-rt-worker"},"end_unix_nano":1792264735326987289,"events":[],"name":"rdm.node_rpc.request.serve.via-direct","parent_span_id":"71b4ad9d55d7eb40","service":"testkit","span_id":"82cc07f9aea57160","start_unix_nano":1792264735325454848,"trace_id":"9905221ef1e627287573a3a7d8c7812e"}
```

The gate read is `run 0: one invocation leaves one span file; left: 0; right: 1`. Retained evidence: `/home/admin/rust-distributed-mesh-deploy/kept/i146-position-55c015ab-red/` (gate output, server JSONL and probe transport diagnostic). Neither the child's stderr nor its process exit status was preserved in that failure report. A completed RPC is not evidence that the process exited normally or reached its drain.

The new immediate-exit control is green: `telemetry_exit::a_process_that_emits_one_span_and_exits_immediately_leaves_its_span_file`, 1 passed, 0 failed, 0.01 s. It emits one application span and immediately calls the existing exit drain; it leaves the span file. Log: `/home/admin/rust-distributed-mesh-deploy/i146-immediate-exit-read.log`. No production telemetry change precedes this result.

### 4. Observations (commit d5f02515; production telemetry identical to main d64f6ae2)

- [code] `crates/rdm-transport/src/telemetry.rs:59`: the drain force-flushes `SharedEvidence` with `skip_when_collector_down = false`, before provider shutdown (:82).
- [code] `crates/rdm-transport/src/evidence_capture.rs:47` → `pipeline.rs:120`: that flush reaches `BatchSpanProcessor::force_flush`; no check for an already-open file bypasses it.
- [code] SDK `opentelemetry_sdk-0.27.1/src/trace/span_processor.rs:270`: flush queues `BatchMessage::Flush` and waits for its result; :429 drains pending spans via export (:446).
- [code] `crates/rdm-transport/src/telemetry.rs:490`: export calls `file()` then `write_all`; :432 creates the directory and opens a still-unopened file. It already handles the proposed first-flush case.
- [code] `crates/rdm-test/src/bin/rafka_rpc_probe.rs:357`: the probe drops its runtime, then its telemetry guard (:358), then returns the exit code. This says where a normal exit drains, not whether the failed invocation reached it.
- [code] `crates/rdm-test/tests/probe_root_span.rs:98`: only a JSON parse error includes stderr. A valid `Reply` reaches the file-count assertion (:101) without checking `out.status` or retaining stderr. The missing-file assertion preserves neither.
- [code] `crates/rdm-transport/tests/telemetry_exit.rs:173`: the new process control runs the existing init/drain with no production telemetry edits; test-only commit `d5f02515` is green, not a claimed RED or a fix.
- [flame] The retained server trace records the call as `OK` / `absent`; the gate records no probe span file.

#### Inferred

- [inferred] “Its first export never ran, and the drain found nothing to flush” is not established: the code flushes queued spans even before a first export, and the immediate-exit control passes. An abnormal process exit, a named bounded loss, an exporter failure or spans not yet ended cannot be distinguished from the retained evidence. None is asserted as the cause.

### 5. Delta

Spec says the process flushes its evidence file then exits; the observed probe run ends without that file, while its retained evidence does not establish whether its normal drain ran.

### 6. Arms

Every arm assumes Q42's existing first-flush path and its 2 s bound stay intact until a failing process's exit and flush evidence identify a defect.

- A: Keep the probe's complete-file contract; preserve child status, stdout and stderr before the existing assertions, then read a failing invocation and fix its demonstrated cause with a RED commit. Wire: unchanged; publisher: unchanged; timer: unchanged; tier: process telemetry / test evidence; fork: no new drain path. This is my proposed next action; no production fix or gate rerun has been made on the unconfirmed hypothesis.
- B: Make the successful-probe cell accept Q42's explicitly named bounded loss, as the existing collector-down and blocked-open cells do. Wire: unchanged; publisher: unchanged; timer: unchanged; tier: the cell's exit-evidence contract; fork: Luke would be changing the unconditional file assertion, and this still requires the missing child status/stderr to distinguish named loss from abnormal exit.

Stop/start is renumbered **Q47**. The first follow-up and status-string landing remain held; no stop/start implementation or replacement drain has begun.
