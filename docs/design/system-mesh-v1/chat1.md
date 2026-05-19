# Redpanda UI Design Study

_Started 2026-05-10 22:38 UTC_

---

## User

i am building a modern day event streaming platform that is based on kakfa called Rafak (its built in rust) i don't curerntly hvae a fornted but i need to do everytyhing that conleuent and redpanda's ui are doing today. the design needs to be crip deveoloper firendly and mondern i like this dies but devs usignaly like dark mode but we can have both. canyou start with doing a deign sudy on redpnad and cluents ui and hten come backa with some suggestions

## Assistant

_[tool: questions_v2]_

I'd love to dig in on this. Before I start the design study and propose directions, a few questions to make sure I build the right thing. Quick note up front: I won't recreate Confluent or Redpanda's UIs directly (that's their IP), but I'll absolutely study what categories of screens, IA patterns, and developer affordances they use as industry-standard patterns for Kafka tooling — then propose an original Rafka design system on top of that.

## User

Questions answered:
- scope: Just the design study and a moodboard / visual direction first
- primary_user: all of these and data governence , AI builders , quite a large user base. in adtion we have a combined kakfa connetors and flink into the sytem but i am thinking we start by focusing on the basic kafka features to start. 
- vibe: 2, 0
- vibe_label: Data-dense engineering blue, Terminal-native dark, Decide for me
- default_mode: Dark first, light available
- density: Balanced (Linear / Vercel style)
- name_brand: Rafka
- rust_personality: A lot — lean into rust-orange palette, ferris-ish energy, perf numbers everywhere
- killer_features: Cluster overview + broker health, Topic browser + message inspector (search, filter, replay), Schema registry + schema evolution, Consumer groups + lag monitoring, ACLs / security / audit log
- differentiators: confluent feels like salesforce box hell it functional but horrible.. for his round its all about mangemen t of the infra. orginzantion > environment > cluster> topics. i really like claudes astetic fons aspcing but rafka's vibe needs that relaxed feel but with a edge. like a robot wearing a motorcycle jacket but the patches are all top p99 ,  10ms club. funny event driven stuff. edgy but fun and home
- tweaks: Decide for me

## Assistant

_[tool: set_project_title]_

_[tool: update_todos]_

_[tool: copy_starter_component]_

Great brief. "Robot in a motorcycle jacket with p99 patches" is exactly the kind of constraint I can work with. Let me set this up.

_[tool: read_file]_

_[tool: snip]_

_[tool: write_file]_

Building the moodboard now. I'll structure it as a long-scroll design document with the field study, principles, tokens, voice, primitives, and a micro-peek at the end.

_[tool: write_file]_

_[tool: write_file]_

_[tool: write_file]_

_[tool: write_file]_

_[tool: str_replace_edit]_

_[tool: str_replace_edit]_

_[tool: snip]_

_[tool: done]_

_[tool: snip]_

_[tool: fork_verifier_agent]_

Dropped **Rafka — Design Study v0.1** in your tab. It's a long-scroll moodboard, dark by default, with a Tweaks panel for theme / accent / density.

What's in it:

1. **Masthead** — wordmark, tagline, the crab line
2. **The brief** — audience + scope for v0.1 (basic Kafka first; connectors/Flink deferred)
3. **Field study** — honest two-column read on the managed-enterprise vs developer-native schools, plus a borrow/kill synthesis
4. **Six principles** — including "Streams are live or they are lies" and "Funny is fine; cute is not"
5. **Palette** — warm-tinted surfaces, rust as the single load-bearing accent, jade/amber/crimson/ice for status-with-meaning
6. **Type** — Geist + JetBrains Mono, mono showing up earlier than usual
7. **Voice & patches** — the patch wall (`p99 < 10ms or your money back`, `consumer lag: skill issue`, `ZK who?`…), plus yes/no microcopy
8. **Primitives** — buttons, status pills, inputs, a topic-row table, a sparkline card, terminal block
9. **IA** — Org → Env → Cluster → Topic three-pane visualization with the rules that keep it from becoming nested-tab hell
10. **First peek** — cluster overview card + message inspector composed from the primitives
11. **Next** — the six hero screens I'd build next

Things I want your call on before I go further:

- **Accent direction** — try cycling rust / ember / crimson in the Tweaks panel. Rust is the obvious pick but I want you to feel it.
- **Voice ceiling** — the patches are dialed to ~7/10. Want it louder, quieter, or about right?
- **What's missing from the field study** — anything I should call out about the consoles you've used that I didn't?

Once you sign off on the vibe, the natural next step is the **cluster overview + topic browser** hero screens in a design canvas so we can iterate on layouts side-by-side.

## User

IS THE BACKGOURND BROWN OR BLACK?

## Assistant

_[tool: snip]_

It's neither, technically — it's a **near-black with a tiny warm tint**, plus a subtle rust-colored glow at the top of the page that might be what's reading as brown.

The exact value is `oklch(0.155 0.005 50)`:
- **0.155 lightness** → very dark (pure black is 0, pure white is 1)
- **0.005 chroma** → almost zero saturation; basically neutral
- **50 hue** → the trace of color it does have is in the orange family

So on a calibrated screen it should look like "warm black" — the same kind of off-black a Leica camera body is. Not brown. But the page also has a faint **rust-colored radial gradient** behind the masthead (to add depth and reinforce the brand), and depending on your monitor that can bleed into looking brownish at the top.

Three options — tell me which:

1. **Keep it warm-black** (current) — the leather jacket reading
2. **Push to pure neutral** — `oklch(0.155 0 0)`, true dark gray, no warmth
3. **Push warmer** — `oklch(0.16 0.012 50)`, actually a touch brown, like fired clay

I'd default to **#1** because pure black with a rust accent reads cold; the warm tint glues the accent to the surface. But if it's reading "muddy" to you, **#2** is the safer bet. Which?

## User

THATS COOL I ILIKE WHAT S NEXT

## Assistant

_[tool: snip]_

Sweet. Now we move from moodboard → actual screens. Here's the proposed sequence, in priority order:

**Next pass — Hero screens (design canvas, side-by-side):**

1. **Cluster overview** — throughput, broker matrix, ISR health, top partitions, hot consumers, audit feed. The "first thing you see when you log in" screen.
2. **Topic browser + message inspector** — the screen devs actually live in. Topic list → partitions → message peek → replay.
3. **Consumer groups + lag** — group → member → assignment tree, lag-with-ETA, rebalance history.
4. **Schema registry** — subjects, compatibility matrix, evolution diff.
5. **ACLs & audit** — subject × resource grid, live audit stream.
6. **⌘K palette** — the connective tissue, demoed last.

I'd build these as artboards inside one design canvas so you can pan/zoom and compare. Each will support the Tweaks panel (theme / accent / density / cluster-state).

**A few decisions before I start cutting screens:**

_[tool: questions_v2]_

## User

Questions answered:
- next_screens: 1. Cluster overview, 2. Topic browser + message inspector
- variants_per_screen: One per screen, but a couple of explorations on the home screen
- data_realism: side collapses into icons
- layout_shell: 1
- scale: Medium (9 brokers, 142 topics, ~1.4M msg/s) — like the moodboard peek

## Assistant

_[tool: snip]_

_[tool: update_todos]_

_[tool: copy_starter_component]_

_[tool: write_file]_

_[tool: snip]_

_[tool: write_file]_

_[tool: snip]_

_[tool: write_file]_

_[tool: snip]_

_[tool: snip]_

_[tool: write_file]_

_[tool: snip]_

_[tool: write_file]_

_[tool: write_file]_

_[tool: write_file]_

_[tool: snip]_

_[tool: done]_

_[tool: update_todos]_

_[tool: fork_verifier_agent]_

`screens.html` is open in your tab — three artboards live on a single design canvas:

1. **Cluster overview · Variant A** — data-dense, sidebar expanded, every KPI on the surface
2. **Cluster overview · Variant B** — chart-led, sidebar collapsed to a rail, more breathing room
3. **Topic browser + message inspector** — list ↔ inspector with replay, schema, config tabs

Tweaks panel (bottom-right) lets you cycle the accent through **rust / ember / crimson** to feel which one lands.

Suggested next moves:
- Pick a winner between A and B (or call out what you want stolen from each) and I'll converge
- Pick an accent
- Then the natural follow-ons are **consumer groups + lag detail**, **schema registry**, and the **light theme pass**

## User

a, its hard to not go with rust i like embr too. proceeed iwth next

## Assistant

_[tool: update_todos]_

_[tool: read_file]_

_[tool: read_file]_

On it. Building **consumer groups + lag detail**, **schema registry**, and a **light theme pass** — all added as new artboards on the same canvas so you can compare side-by-side.

_[tool: read_file]_

_[tool: snip]_

_[tool: str_replace_edit]_

_[tool: snip]_

_[tool: write_file]_

_[tool: snip]_

_[tool: write_file]_

_[tool: str_replace_edit]_

_[tool: str_replace_edit]_

_[tool: done]_

_[tool: update_todos]_

_[tool: snip]_

_[tool: fork_verifier_agent]_

Three new artboards are live on `screens.html`:

1. **Consumer groups + lag detail** — group list with lag pills, per-group KPIs, lag chart with SLO line, 24-partition assignment matrix colored by load, member table with hosts/clients/heartbeats, reset-offsets CLI footer
2. **Schema registry** — subject list filterable by format, version timeline with a rolled-back v4 marker, side-by-side v4→v5 diff with `+` gutter for the added `promo_code` field, compatibility-mode picker
3. **Light theme pass** — cluster overview wrapped in `data-theme="light"` so you can A/B against the dark version right on the canvas

The light pass uses the same density and components — just retoned: warmer paper-white surfaces, deeper rust, lower-saturation status colors so they don't shout.

When you've had a look, three good next moves:
- **Pick favorites** between the cluster overview variants (A vs B) and I'll converge on a single hero
- **Connectors** screen — source/sink catalog + instance detail
- **ACLs / audit log** — tabular permissions matrix + filterable audit stream

## User

Apply drawing

## Assistant

_[tool: snip]_

_[tool: str_replace_edit]_

_[tool: done]_

_[tool: snip]_

Fixed. The ⌘K search and the "Add broker" / "Create topic" buttons were wrapping because nothing was telling them to stay on one line — fixed by pinning `white-space: nowrap` on `.btn`, the topbar search, and the page-head action row, and giving the search a fixed 320px width with ellipsis on overflow.

## User

Pick favorites between the cluster overview variants (A vs B) and I'll converge on a single hero
Connectors screen — source/sink catalog + instance detail
ACLs / audit log — tabular permissions matrix + filterable audit stream

## Assistant

_[tool: snip]_

_[tool: update_todos]_

_[tool: str_replace_edit]_

_[tool: str_replace_edit]_

_[tool: str_replace_edit]_

_[tool: write_file]_

_[tool: write_file]_

_[tool: str_replace_edit]_

_[tool: snip]_

_[tool: str_replace_edit]_

_[tool: str_replace_edit]_

_[tool: update_todos]_

_[tool: done]_

_[tool: snip]_

_[tool: fork_verifier_agent]_

Three new things landed:

1. **Cluster overview hero converged** — Variant A is the canonical one; B is removed from the canvas
2. **Connectors** — 12-type catalog (Postgres CDC, S3, Snowflake, Mongo, ES, BigQuery, MySQL, Kinesis, Redis, Webhook, Mirror, Dynamo), instance table with per-row state pills, 8-up task pool with throughput bars and error counts, and a config.yaml + CLI footer
3. **ACLs & audit** — two artboards: permissions matrix (principal × CREATE/READ/WRITE/DELETE/ALTER/DESCRIBE/CLUSTER_ACTION with allow/deny/inherited/none glyphs) and a filterable audit feed with event-kind pills and expandable detail rows

Natural next moves: a **stream-processing workbench** (SQL editor + live result tape), an **onboarding/empty-state** pass, or a **mobile/responsive** look at the overview.

