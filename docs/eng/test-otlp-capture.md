# Test trace capture: spans as a cell's evidence

A cell proves a mechanism from the spans the code emits on its normal path, never from a probe
built for the test alone. RDM has two capture paths, and both write the same JSONL span record:

| path | used by | where the spans land |
|---|---|---|
| **in-process**: `rdm_transport::EvidenceCapture` | a cell that runs RDM code inside the test binary (`rdm-admin`, `rdm-node`, `rdm-transport` and `rdm-test` cells that use the rig) | the capture's own file, and the collector when one is named |
| **estate**: `RDM_EVIDENCE_DIR` on every process | a cell that starts real processes (`rdm_test::estate::Estate`) | one file per process under the estate's evidence directory, and the collector when one is named |

How spans leave a process and how one trace crosses processes is
[docs/architecture/telemetry.md](../architecture/telemetry.md); span names are
[telemetry-patterns.md](telemetry-patterns.md) §"Span names".

## 1. Environment

| variable | effect | source |
|---|---|---|
| `RDM_EVIDENCE_DIR` | every RDM binary writes every exported span as JSONL to `<dir>/<service>.<pid>-<pid namespace inode>.spans.jsonl` | `crates/rdm-transport/src/telemetry.rs::init_evidence_telemetry`; CLAUDE.md env table |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | spans also go over OTLP/HTTP to `<url>/v1/traces`, log records to `<url>/v1/logs` | `telemetry.rs::signal_url`; CLAUDE.md |
| `OTEL_SERVICE_NAME` | overrides the binary's own service name | `telemetry.rs::init_evidence` |
| `RDM_OTEL_DEBUG=1` | span export carries DEBUG from RDM's own targets instead of INFO | `telemetry.rs::span_export_filter` |
| `RDM_OTLP_LOG_LEVEL` | the level the OTLP log bridge exports from; default WARN | `telemetry.rs::otlp_log_export_level` |
| `RDM_DEBUG_GOSSIP` | tests only: an `EvidenceCapture` adds a stderr layer of iroh-gossip's own DEBUG lines, on one clock with RDM's spans | `crates/rdm-transport/src/evidence_capture.rs` (the `open` function); CLAUDE.md |
| `RDM_ARTIFACTS_DIR` | an estate's artifact root; default `crates/rdm-test/tests/artifacts`; a relative value is taken from the workspace root | `crates/rdm-test/src/estate.rs::artifacts_root` |
| `JAEGER_QUERY_URL` | the Jaeger base an estate writes into `trace-url.txt`; default `http://localhost:16686` | `estate.rs::Estate::record_trace_url` |

`RUST_LOG` drives stdout only. Span export is a fixed filter: everything off, then INFO (DEBUG with
`RDM_OTEL_DEBUG=1`) from RDM's own targets (`rafka*`, `rshape*`, `rdm*` and the one test
executable per crate, `main`), and every third-party crate (iroh, iroh-gossip, noq, netwatch,
the HTTP and OTLP stacks) off whatever `RUST_LOG` says (`telemetry.rs::span_export_filter`).

## 2. The span record

One JSON object per line, written when the span ends (`telemetry.rs`, the JSONL exporter):
`trace_id`, `span_id`, `parent_span_id`, `name`, `service`, `start_unix_nano`, `end_unix_nano`,
`attributes`, `links` and `events`. Causality is `parent_span_id`; a reader never infers it from
timestamps.

Because a line is written only at the span's end, a span still open when the file is read is not
in it. An absence means something only once the work that would have emitted the span has
finished: wait for the completion first (the reply, the Build's terminal, the process's exit),
then read.

## 3. In-process: `EvidenceCapture`

One test binary is one process, so a cell never installs process-wide telemetry: no global
subscriber, no global propagator, no `init_*` call. `EvidenceCapture` is the calling thread's
default dispatcher (`set_default`) for as long as it lives (`evidence_capture.rs`, module doc).

| call | what it does |
|---|---|
| `EvidenceCapture::start(dir, service)` | the cell's capture: its own file `<service>.<pid>-<pid namespace inode>.<n>.spans.jsonl` under `dir`, and the collector named by `OTEL_EXPORTER_OTLP_ENDPOINT` when the process has one |
| `EvidenceCapture::start_with_collector(dir, service, Some(url))` | the same, with the collector given explicitly |
| `EvidenceCapture::start_held(dir, service)` | for a cell that makes more calls than its evidence can carry: each trace's spans wait until the cell calls `keep(trace)` (sent on) or `forget(trace)` (dropped); a trace never decided reaches neither |
| `capture.run(worker_threads, future)` | runs the cell's future on a multi-thread runtime **every one of whose threads** has this capture as its default dispatcher |
| `capture.spans()` | every span ended before the call, as the file holds it; `Err` names what failed (the flush bound of 10 s, a span that never reached the file, an unreadable line) |
| `capture.path()` | the capture's own file |

**The thread constraint.** `set_default` is thread-local. A span opened on a thread that never had
the capture as its default does not reach it, silently. A `#[tokio::test(flavor = "multi_thread")]`
runtime's worker threads are such threads. Run the cell's work through `capture.run(...)`, which
builds the runtime with the capture on every worker; never rely on a plain `multi_thread` test
runtime to carry it.

Each crate's test binary sets its process environment once, in its `tests/main.rs` `#[ctor]`,
before any thread exists; a cell never sets or removes a variable
(`crates/rdm-admin/tests/main.rs`, `crates/rdm-transport/tests/main.rs`,
`crates/rdm-test/tests/main.rs`). `rdm-transport`'s sets `RDM_EVIDENCE_DIR` to
`I143_ACCEPTANCE_DIR` when the acceptance runner exports one, else
`target/evidence/rdm-transport`.

Inside `rdm-transport`'s own unit tests, a cell installs its layer through the crate's one door,
`test_span_capture::install` (`crates/rdm-transport/src/test_span_capture.rs`), which enables
every callsite first.

**Own story only.** `rdm-test`'s `assert_own_story(names)` (`crates/rdm-test/tests/main.rs`)
fails a cell whose capture holds any span not named `rdm.*`: a transport-internal or foreign span
in a cell's trace is a defect of the capture.

## 4. Estate cells: one file per process

An estate (`crates/rdm-test/src/estate.rs`) hands its evidence directory to every node-admin it
starts as `RDM_EVIDENCE_DIR`; each admin passes it through to every node it launches
(`crates/rdm-admin/src/admin/records.rs::PASSTHROUGH`), and each process writes its own file
there. A container estate mounts the directory into every container
(`crates/rdm-admin/src/deployment/container.rs`).

| call | what it does |
|---|---|
| `estate.spans()` | every `*.spans.jsonl` line under the estate's evidence directory, sorted by `start_unix_nano`. A `build_id` attribute rendered `bid_xxxx-xxxx-xxxx` is returned as its 12-character bare id; the files keep the raw form |
| `rdm_test::estate::named(&spans, name)` | the spans of one name |
| `rdm_test::estate::descends_from(&spans, child, ancestor)` | whether `child` descends from `ancestor` by `parent_span_id` links: assert the parent edge, never only that one span's interval contains another's |
| `rdm_test::estate::claim_decider(&spans, build_id, attempt)` | the node-admin whose `via-claim-decision` span decided an attempt's claim |
| `estate.record_trace_url(trace_id)` | writes `<JAEGER_QUERY_URL>/trace/<trace_id>` to the estate's `trace-url.txt` |
| `estate.stop().await` | stops every process of the estate and waits until each has exited, its evidence flushed: read `spans()` after it |

A process flushes its spans at exit within a bound (`EXIT_FLUSH_BOUND`, 2 s,
`crates/rdm-transport/src/export.rs`); a flush that ends at its bound names what it lost on stderr
(`the evidence flush ended at its bound`). `crates/rdm-test/tests/common/mod.rs::named_loss`
reads that line, so a cell can account for every span a process handed to its exporter.

## 5. Reading a trace

| endpoint | what | source |
|---|---|---|
| collector, OTLP/HTTP | `http://192.168.68.99:5318` (the gate's and the test runner's export target) | `scripts/rdm-gate-at.sh` line 42; `tools/test-runner/src/lib.rs` (`OTLP`) |
| Jaeger UI | `http://192.168.68.99:16687`, a trace at `/trace/<trace_id>` | `tools/test-runner/src/lib.rs` (`JAEGER_UI`); `scripts/demo-estate.sh` |
| ClickHouse | `http://192.168.68.99:18123`, table `otel.otel_traces`: span counts by name or attribute across a whole run | `observability-conventions.md` in `rafka-v2`, §"Telemetry endpoints" |

- The local defaults (`http://localhost:4318`, `http://localhost:16686`, CLAUDE.md's env table)
  are another store: a gate's or a test runner's spans are on the `.99` endpoints.
- Look a trace up by its id, taken from the cell's own spans (`trace_id`). A trace link cited in a
  report must resolve.
- `scripts/cells.sh` exports no `OTEL_EXPORTER_OTLP_ENDPOINT`; set it in the caller's
  environment when a cell's trace should reach the collector. The gate exports it for every job.
- The gate keeps one snapshot of the scenario artifacts after a red, under
  `$HOME/rust-distributed-mesh-deploy/kept/` (`scripts/keep-gate-evidence.py`).
