# Sprint 01 — Agent Dispatch Prompt (team-lead use only)

This is the prompt to paste into `Agent` tool when spawning the Sprint 01 Sonnet engineer. Self-contained, no inferred context.

---

## Tool invocation parameters

```
Agent({
  description: "Sprint 01 substrate spike — iroh + UI + rfa",
  subagent_type: "general-purpose",
  model: "sonnet",
  name: "mesh-substrate-eng",
  prompt: <see below>,
  run_in_background: true
})
```

## Prompt body (paste into `prompt` field)

```
You are executing rafkav2 Sprint 01 — the substrate spike + day-1 topology UI + day-1 rfa CLI.

WORKING ENVIRONMENT
- Working tree: E:/worktrees/sprint-01-substrate-spike/ — create via `git -C E:/dev/rafka-V2-new-mesh worktree add E:/worktrees/sprint-01-substrate-spike -b sprint-01-substrate-spike origin/main`
- Cargo target: ALWAYS use `CARGO_TARGET_DIR=E:/cargo-target-sprint-01` for every cargo command. Do NOT use the default ./target — it'll conflict with other concurrent workspaces.
- OTLP collector: deployment/dev/docker-compose.otlp.yml (start with `podman compose -f deployment/dev/docker-compose.otlp.yml up -d` — Jaeger UI at http://localhost:16686)
- Remote: https://github.com/drlukeangel/rafkav2 — push directly to `sprint-01-substrate-spike` branch, NO PR

MANDATORY PRE-READS (read in this order before touching any code)
1. E:/dev/rafka-V2-new-mesh/CLAUDE.md — the 5 non-negotiable architectural locks + discipline rules
2. E:/dev/rafka-V2-new-mesh/docs/sprints/sprint-01/sprint-prd.md — north-star PRD
3. E:/dev/rafka-V2-new-mesh/docs/sprints/sprint-01/sprint-config.json — exit criteria + in/out scope
4. E:/dev/rafka-V2-new-mesh/docs/sprints/sprint-01/sprint-prompt.md — engineer-facing detail brief
5. E:/dev/rafka-V2-new-mesh/docs/plans/mesh-v1/06-decisions.md — 18 locked decisions
6. E:/dev/rafka-V2-new-mesh/docs/plans/mesh-v1/01-substrate-prd.md — iroh substrate spec
7. E:/dev/rafka-V2-new-mesh/docs/plans/mesh-v1/02-topology-ui-prd.md — UI requirements
8. E:/dev/rafka-V2-new-mesh/docs/plans/mesh-v1/03-rfa-cli-prd.md — CLI requirements

If any sprint brief contradicts a locked decision in `06-decisions.md`, the decision wins; flag to team-lead via SendMessage before proceeding.

WHAT YOU SHIP

In order:
1. Workspace skeleton — Cargo.toml workspace + 6 crates (rafka-mesh-transport, rfa, gateway, broker, compute, schema, topology-ui)
2. `crates/rafka-mesh-transport/` — `MeshTransport` trait + `IrohMeshTransport` impl. Uses iroh::Endpoint, ALPN `rafka-mesh-v1`, gossip via iroh discovery. NO custom gossip protocol.
3. 4 bare node binaries (rafka-gateway, rafka-broker, rafka-compute, rafka-schema) — each boots, mints `EndpointId` to $RAFKA_DATA_DIR/node-identity.json, joins mesh, emits substrate spans, accepts InternalMeshFrame on the mesh ALPN. Zero app logic. Zero HTTP routes.
4. `topology-ui/` binary — axum server on http://localhost:19090. Joins mesh as view-only participant on ALPN `rafka-topology-v1`. Serves plain HTML+JS page with vis-network graph + spawn/kill buttons. Subprocess management for spawned nodes. WebSocket for real-time delta updates.
5. `crates/rfa/` CLI binary — thin REST client targeting http://localhost:19090. Commands: mesh node add/remove/list/describe/logs/spans, mesh topology show/watch, mesh status, mesh wait-converged. Every command supports --format json.
6. OTLP wiring — all 14 substrate spans (listed in sprint-config.json::spans_to_emit) land in tests/artifacts/mesh-substrate/

EXIT CRITERIA (see sprint-config.json for full list — these are the headlines)

- `cargo run -p rafka-gateway` boots a node, joins mesh, emits rafka.mesh.node.started
- Same for rafka-broker, rafka-compute, rafka-schema
- `cargo run -p rafka-topology-ui` starts UI on http://localhost:19090
- Browser shows live topology graph; spawn buttons work; kill via UI works
- `rfa mesh node add --type <type>`: node appears in UI within 5s
- `rfa mesh node remove <name>`: disappears within 10s
- `rfa mesh wait-converged --timeout 30s` exits 0 after spawning 4 nodes
- All 14 substrate spans in tests/artifacts/mesh-substrate/*.spans.jsonl with non-empty content
- `CARGO_TARGET_DIR=E:/cargo-target-sprint-01 cargo check --workspace --tests --no-default-features` = 0 errors, 0 new warnings
- Grep tests: zero hand-rolled mesh primitives, zero custom QUIC code (see sprint-config.json::exit_criteria for exact grep patterns)

DISCIPLINE (mandatory)
- Sonnet only for ANY subagent you spawn (don't spawn agents for simple work; do it directly)
- No defer language. Full scope in one merge. No "pass 1 / pass 2 later."
- OTLP artifact evidence before sprint close. Code gates alone are insufficient.
- stash → pull --rebase origin/main → commit → push between fix batches
- No Claude attribution in commit messages (no Co-Authored-By, no 🤖 trailer)
- No `cargo clean` as debugging shortcut — diagnose with `cargo check -p <crate>` first
- Workspace gate zero/zero before EVERY commit
- 80/20 verify your own work via git diff before claiming done. Lying about scope = killed.
- No HTTP routes on any node binary except rafka-topology-ui (per Golden Principle #2 in CLAUDE.md)

REPORTING CADENCE
- Report progress via SendMessage to team-lead every ~30 minutes OR at each commit, whichever is more frequent
- If you hit an architectural ambiguity, surface BEFORE coding around it — don't invent
- If iroh has a show-stopper, surface to team-lead — do NOT silently fall back to custom QUIC (banned)

WHEN DONE
1. Flip sprint-config.json status to "closed", set closes date
2. Final commit + push to origin/sprint-01-substrate-spike
3. SendMessage to team-lead: branch tip SHA + OTLP artifact path + brief all-green summary
4. Stand by for audit — team-lead will independently verify (80/20 rule) before merging to main

If you can't crack a sub-deliverable in 4 hours, surface a diagnostic message with what you tried + ask for advisor consult. Don't go silent.

Begin by:
1. Running the worktree-add command
2. Reading the 8 mandatory pre-reads in order
3. Posting a single SendMessage with "Pre-reads complete, starting workspace skeleton" so team-lead knows you're alive

Then execute. Go.
```

## Post-spawn checklist (team-lead)

After spawning:
- Note the agent_id returned by Agent tool
- Add a task: `Sprint 01 in flight, agent <name>, branch sprint-01-substrate-spike, worktree E:/worktrees/sprint-01-substrate-spike`
- Probe via SendMessage every 30 min if no spontaneous report
- Verify any reported deliverable via `git -C E:/worktrees/sprint-01-substrate-spike diff --stat` before trusting

## When agent reports done

1. Independent audit per the sprint-config.json exit_criteria (run each one yourself)
2. If audit passes: merge to main via `cd E:/dev/rafka-V2-new-mesh && git fetch origin && git merge --no-ff origin/sprint-01-substrate-spike -m "merge(sprint-01): mesh substrate spike + day-1 UI + day-1 rfa CLI"` then push
3. If audit fails: SendMessage with specific exit-criterion that didn't pass, ask for fix
4. After merge: shutdown_request to agent, TeamDelete (if solo agent) or keep team alive for Sprint 02
