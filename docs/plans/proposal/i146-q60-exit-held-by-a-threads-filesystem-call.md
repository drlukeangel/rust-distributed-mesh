## Q60: A process exit is held by its evidence thread's file creation: what does Q42's bound hold?

### 1. Spec

Q42 (Luke, 2026-10-17, recorded in `docs/plans/i146-owed-messages-prd.md`): "the bound is the contract; a named, bounded loss is right, an exit that waits on the disk past its bound is not." Owner: the node process's telemetry exit drain (`rdm-transport` `telemetry.rs` `Exporters::drain`, `flush_before_exit`). Sequence: the exit line → the evidence flush, the collector's shutdown and the log sinks' flush, all within one 2 s deadline (`export::DRAIN_BOUND`) → `std::process::exit`.

### 2. Issue

`telemetry_exit`'s exit cells (and `collector_outage`, `probe_root_span`'s refusal cell, the same exit) see the process end 2.5–2.9 s after the exit line, against the 2 s bound plus 500 ms. Red under the gate's load: gates `103`, `305`, `508` and the whole rdm-transport binary looped beside the gate (lane e4s2); never on a quiet box, never with one CPU starved alone (6 of 6 green).

### 3. Flame

The child's spans are not written in these runs (the evidence file is the thing being created), so the flame is the cell's own measurement: the child's exit times on stderr and a parent thread sampling the child's threads every 20 ms from `/proc/<pid>/task/*/{stat,wchan}`.

Gate `508` (lane e4s2, `4ce8e282`), `telemetry_exit::a_process_that_emits_one_span_and_exits_immediately_leaves_its_span_file`, verbatim:

```
the exit flush returned after 2004 ms, the exit was called at +2005 ms, the process's end was seen at +2746 ms;
the child's threads from its exit line on, sampled every 20 ms (thread:state:wait channel x samples):
main-af90a9fac1:S:-x1 main-af90a9fac1:S:futex_do_waitx89 main-af90a9fac1:Z:-x35 rdm-log:S:futex_do_waitx180
rdm-telemetry-d:S:futex_do_waitx89 rdm-telemetry-e:D:filename_createx123 rdm-telemetry-e:R:-x2
telemetry_exit::R:-x2 telemetry_exit::S:futex_do_waitx88
telemetry: the evidence flush ended at its bound (2000 ms): 9 of 9 spans not confirmed written to the evidence file
```

Runs 34 and 35 of the whole-binary loop show the same with the log sinks: `rdm-log:D:filename_create`, `rdm-log:D:__jbd2_log_wait_for_space`.

### 4. Observations (commit `4ce8e282`)

- [flame] gate 508: the drain returned at its bound (2004 ms) and the exit was called at +2005 ms; nothing in the node's code waited past the bound.
- [flame] gate 508: the evidence thread `rdm-telemetry-e` was in `D` (uninterruptible) in `filename_create` for 123 samples (about 2.46 s), from inside the drain until after the exit was called; the main thread was a zombie (`Z`) for 35 samples (about 700 ms), the thread group's end waiting on that thread.
- [code] `telemetry.rs` `Exporters::drain`: the evidence flush runs under `export::bounded_until` with one deadline; at the bound it names the loss and returns; it cannot cancel a system call a thread is in.
- [code] the evidence file is created on the export thread at its first export (i146 Q33: init waits for nothing); in the immediate-exit shape that first export is the drain's flush. The log sinks create their files on their own threads (`log_sink.rs` `LogSink::start_opening`).
- [code] `std::process::exit` returns nothing to wait on; Linux ends a process when every thread has left the kernel, and a thread in `filename_create` or `__jbd2_log_wait_for_space` leaves only when the call returns.

#### Inferred

- [inferred] `__jbd2_log_wait_for_space` is the ext4 journal out of free space; what fills it is the fsync count of every estate on the box (each durable write: a temp file's fsync, a rename, the directory's fsync). The same disk wait is under `fabric_primary.rs:204`'s lane reads and plausibly `accepted_topology`'s created admin missing its `runtime.json` within 10 s.

### 5. Delta

Spec says an exit never waits on the disk past its bound; the kernel holds a process's end until each of its threads' in-flight filesystem calls returns, and at the bound the evidence thread's file creation is such a call.

### 6. Arms

Every arm keeps the drain's one 2 s bound and its named loss.

- A: the evidence and log files are created and written by a process of the node's own, whose writes the node's exit does not wait on — changes the telemetry writer (a second process per node).
- B: the node's evidence and log files are created by its provider before the node runs, so no file creation is in flight at the exit (a write still can be) — changes the provider and the telemetry open path.
- C: Q42's bound holds what the node's code does: the cells hold the drain and the exit's call to the bound (the child's own times) and report the kernel's teardown with the threads' states, unbounded — changes the cells only.
- x: something else.
