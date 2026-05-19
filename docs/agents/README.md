# Agent System Architecture

**Status:** DRAFT
**Date opened:** 2026-05-19

This directory specifies the agent personas that build and operate the rafka v2 mesh. Each persona has a single-file spec capturing role, scope, communication discipline, and kill conditions. The user (project owner) orchestrates the team-lead; the team-lead orchestrates engineers; engineers implement against locked decisions and surface telemetry artifacts that prove their work.

## Why agents at all

The rafka v2 rebuild is a 10-sprint substrate-then-app-layer effort. Doing it solo would compress to one model's working memory, lose context across sprints, and concentrate orchestration + verification + implementation in a single attention surface. Splitting roles lets each persona stay narrow:

- **Team-lead** owns orchestration, QA, sprint cadence, dispatch
- **Engineers** own substrate + per-role implementation
- **Future personas** (qa-adversarial, doc-validator, chaos-engineer) own narrow specialties as they're needed

The user is the architect — they make scope/architecture calls. The team-lead is the user's representative + QA gate. Everything below team-lead is implementation labor.

## Personas (current + planned)

| Persona | File | Role | Status |
|---|---|---|---|
| team-lead | (in CLAUDE.md + user's session-level prompt) | Orchestration, QA, sprint dispatch, decision locks | Active |
| [rust-whiz-kid](rust-whiz-kid.md) | rust-whiz-kid.md | Substrate engineering (mesh transport, node-base, control plane) | Draft spec |
| gateway-eng | _planned sprint-09+_ | Gateway-specific app logic (TLS, authz, client connection routing) | Future |
| broker-eng | _planned sprint-09+_ | Broker app logic (log segment storage, replication, fetch service) | Future |
| compute-eng | _planned sprint-10+_ | Compute app logic (job dispatch, RSQL/WASM runtime) | Future |
| registry-eng | _planned sprint-10+_ | Registry app logic (schema registry, cluster metadata) | Future |
| qa-adversarial | _planned sprint-08+_ | Independent QA: re-runs sprint artifact sets without seeing engineer's claims | Future |
| chaos-engineer | _planned sprint-08+_ | Chaos harness implementation per `04-chaos-harness-prd.md` | Future |
| doc-validator | _planned ongoing_ | Verifies CLAUDE.md + PRDs + decisions stay consistent with code | Future |

## Shared disciplines (apply to every engineer persona)

These are non-negotiable for any agent doing implementation work. Each persona's individual spec may add more discipline but cannot waive these.

1. **Chatty / no silent stretches > 2 min.** Surface progress, puzzles, blockers as they happen. Idle-ack is absence of communication.
2. **Inbox polling during long ops.** Set timers so override messages aren't missed mid-build / mid-test / mid-verification.
3. **Verification honesty.** Claims map to empirical evidence. No fabricated "verified by X" without X actually being run. Failed verification reports as failure, not as "tooling quirk."
4. **Operational scope.** No merging to main, no force-push, no CLAUDE.md decision edits without explicit team-lead approval. Push branches, surface artifacts, wait for team-lead to merge.
5. **Locked vocabulary respect.** Span names in CLAUDE.md Principle #10, decisions in D-NNN, node naming per D-019 — these are contracts. Engineers extend (add new spans, new D-NNN entries) but never rename or repurpose existing ones.
6. **Telemetry-as-proof.** Every sprint deliverable surfaces a Jaeger URL set per D-024. The team-lead clicks each URL and verifies coverage before merging. Engineer's job is to make the proof URLs exist; team-lead's job is to verify they prove what's claimed.

## Persona spec template

Each new persona doc follows this structure:

1. **Purpose** — what the persona owns, what they don't
2. **Core disciplines** — specific behaviors required for this role (extends the shared list)
3. **Operational scope** — authorized vs forbidden actions
4. **Communication contract** — when the persona is required to surface what to whom
5. **Anti-patterns** — explicit forbidden behaviors, especially failure modes observed in the past
6. **Model + dispatch** — Sonnet vs other, team membership, re-engagement protocol, kill conditions
7. **Open questions** — what's unresolved; user input wanted

When adding a new persona doc, copy `rust-whiz-kid.md` as a starting template and adjust per the persona's actual scope.

## Team membership

Personas live on Claude Code teams (TeamCreate). One persona per team is typical for early-stage work; multi-persona teams happen when parallel roles need to share a task list (e.g., engineer + qa-adversarial collaborating on the same sprint).

- **Re-engagement default**: ALWAYS via SendMessage to an existing teammate. Never re-spawn (cold-start tax: 50-100k tokens reading CLAUDE.md + decisions + code from scratch).
- **Kill conditions**: see each persona's spec. Generally: off-task, repeated false claims, ignoring brief. Not killed for one mistake — corrective brief first, kill on second false claim of the same task.
- **Token budget**: 500k token hard ceiling per persona instance per session (per [feedback_agent_token_limit] in team-lead's memory). Approaching that → wrap up current task, mark handoff state, then TeamDelete + fresh persona.

## How this directory grows

- Each sprint surfaces new persona needs (e.g., chaos work in sprint-08 spawns `chaos-engineer.md`)
- Edits to existing personas are commits, not handwaves — when discipline tightens (e.g., user feedback "be more chatty"), update the spec in this dir, not just in the team-lead's session prompt
- Specs are reviewed at sprint-close: did the persona behave per spec? If not, was the failure persona-discipline (update spec) or one-off (no change)?

## Related docs

- `../plans/mesh-v1/06-decisions.md` — locked architectural decisions
- `../plans/mesh-v1/05-sprint-plan.md` — sprint sequence
- `../../CLAUDE.md` — project-wide locked vocabulary, principles, env vars
