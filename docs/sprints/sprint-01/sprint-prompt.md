# Sprint 01 — Telemetry-first gateway

**Sprint config:** `docs/sprints/sprint-01/sprint-config.json`
**Branch:** `sprint-01-telemetry-first-gateway` (off main)
**Size:** Tiny. 1-3 days. Single deliverable. Do NOT scope-creep.

---

## What you ship

ONE thing: a telemetry pipeline + ONE bare gateway binary that emits a complete boot-span chain visible in Jaeger.

This is the foundation sprint. Telemetry is the SUBSTRATE (Golden Principle #6). The gateway is just the demo that proves the telemetry pipeline works end-to-end.

## What you do NOT ship

- No broker, compute, schema binaries — those are Sprints 02, 04, 05
- No peer-to-peer connection — Sprint 03
- No topology UI — Sprint 06+
- No rfa CLI — Sprint 09+
- No tests — pilot phase skips formal coverage per Principle #6 (telemetry IS the verification)
- No HTTP routes anywhere
- No app logic

If you find yourself writing code outside the `in_scope` list in `sprint-config.json`, STOP and SendMessage team-lead. Scope creep kills sprints.

## Execution order (do this exactly)

1. **Worktree:** `git -C E:/dev/rafka-V2-new-mesh worktree add E:/worktrees/sprint-01-telemetry-first-gateway -b sprint-01-telemetry-first-gateway origin/main` then `cd E:/worktrees/sprint-01-telemetry-first-gateway`
2. **Cargo target:** Always prefix cargo commands with `CARGO_TARGET_DIR=E:/cargo-target-sprint-01`
3. **Pre-reads first.** Read CLAUDE.md, sprint-config.json, decisions log. Confirm understanding via SendMessage to team-lead BEFORE writing code. Format: "Pre-reads complete. Confirming: building OTLP + telemetry crate + gateway binary, 1-3 day scope. Starting now."
4. **OTLP collector first.** Write `deployment/dev/docker-compose.otlp.yml`. Verify `docker-compose up -d` brings up Jaeger on `http://localhost:16686`. If you can't load the UI in a browser, do not proceed.
5. **rafka-telemetry crate.** `crates/rafka-telemetry/` with one public function `init_telemetry(service_name: &str) -> TelemetryGuard`. Uses `tracing-opentelemetry` + `opentelemetry-otlp`. Use SimpleSpanProcessor for sub-process visibility (BatchSpanProcessor can swallow spans on quick exits).
6. **rafka-mesh-transport crate.** `MeshTransport` trait + `IrohMeshTransport` struct. Minimal — just wraps `iroh::Endpoint` creation and a no-op accept loop. Every async fn is `#[instrument]`.
7. **gateway binary.** `gateway/src/main.rs`. main() does in order:
   - `let _guard = rafka_telemetry::init_telemetry("rafka-gateway");`
   - Boot sequence under one `rafka.mesh.node.ready` parent span:
     - Load or mint identity → emit `rafka.mesh.boot.identity_loaded` or `.identity_minted`
     - Create iroh Endpoint → emit `rafka.mesh.boot.endpoint_created`
     - Register ALPN → emit `rafka.mesh.boot.alpn_registered`
     - Start gossip discovery → emit `rafka.mesh.boot.gossip_started`
     - Start accept loop → emit `rafka.mesh.boot.accept_loop_started`
   - Spawn heartbeat task: every 5s emit `rafka.mesh.heartbeat`
   - Signal handler: on SIGINT/SIGTERM, emit `rafka.mesh.node.stopping`, then exit
8. **Boot it.** `CARGO_TARGET_DIR=E:/cargo-target-sprint-01 cargo run -p rafka-gateway`. Wait 15 seconds. Open Jaeger. Find your trace.
9. **Workspace gate.** `CARGO_TARGET_DIR=E:/cargo-target-sprint-01 cargo check --workspace --tests --no-default-features` — must be zero/zero.
10. **Commit + push.** Single commit. Flip sprint-config.json `status: "closed"` + set `closes` date in the SAME commit. Push to `origin/sprint-01-telemetry-first-gateway`.
11. **Close-out SendMessage.** Format below, INCLUDING the Jaeger URL or sprint is not closed.

## Close-out SendMessage format (required)

```
DONE. Sprint 01 telemetry-first gateway closed.

Branch tip: <SHA>
Worktree: E:/worktrees/sprint-01-telemetry-first-gateway

Jaeger URL (open in browser to validate):
http://localhost:16686/search?service=rafka-gateway&operation=rafka.mesh.node.ready&lookback=1h

What you'll see:
- One trace per process start
- Root span: rafka.mesh.node.ready
- 5 child spans: identity_loaded/minted, endpoint_created, alpn_registered, gossip_started, accept_loop_started
- Heartbeat spans every 5s while running
- Final rafka.mesh.node.stopping span on Ctrl+C

Standing by for audit.
```

## Discipline (re-read CLAUDE.md before commit)

- Sonnet only for any subagent you spawn (you likely don't need to spawn any)
- No defer language ("pass 1 / pass 2 later" forbidden)
- No Claude attribution in commit messages
- Workspace gate zero/zero before every commit
- 80/20 verify your own work via `git diff` before claiming done
- Every async fn `#[instrument]`. No silent code paths.

## When you're stuck

- 4 hours on the same problem → SendMessage team-lead with diagnostic + ask for advisor consult
- iroh has a show-stopper → SendMessage immediately, do NOT fall back to custom QUIC (banned)
- Sprint config seems wrong → SendMessage, do NOT modify it unilaterally
