// Rafka — Design Study v0.1 (continued)
// Type, voice, primitives, IA, peek, closer.

function TypeSpecimen() {
  return (
    <section className="section shell">
      <SectionHead
        index="05"
        label="type"
        title="Geist for ink. JetBrains Mono for truth."
        lead="Sans for prose and UI; mono for anything a developer would copy. Mono shows up earlier than usual — in eyebrows, status, metrics, and most labels — because Rafka rewards the eye that scans for numbers."
      />

      <div className="card card-pad-lg">
        <div className="row gap-8" style={{ alignItems: 'flex-end', flexWrap: 'wrap' }}>
          <div style={{ flex: '1 1 320px' }}>
            <div className="card-sub mono" style={{ marginBottom: 4 }}>DISPLAY · GEIST 600 · -0.03em</div>
            <div style={{ fontSize: 56, lineHeight: 1.02, letterSpacing: '-0.028em', fontWeight: 600 }}>Streams that don't lie about being live.</div>
          </div>
          <div style={{ flex: '0 0 180px', textAlign: 'right', color: 'var(--ink-3)', fontFamily: 'JetBrains Mono, monospace', fontSize: 11, letterSpacing: '0.06em' }}>
            56 / 32 / 20 / 15 / 13 / 11
          </div>
        </div>

        <hr className="hr" />

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 28 }}>
          <div>
            <div className="card-sub mono" style={{ marginBottom: 8 }}>H2 · 32 / 600 / -0.022em</div>
            <div style={{ fontSize: 32, fontWeight: 600, letterSpacing: '-0.022em', lineHeight: 1.1 }}>Cluster overview</div>

            <div className="card-sub mono" style={{ margin: '20px 0 8px' }}>H3 · 20 / 600 / -0.015em</div>
            <div style={{ fontSize: 20, fontWeight: 600, letterSpacing: '-0.015em' }}>Top partitions by write throughput</div>

            <div className="card-sub mono" style={{ margin: '20px 0 8px' }}>BODY · 15 / 400</div>
            <p style={{ fontSize: 15, color: 'var(--ink-2)', margin: 0, lineHeight: 1.55, maxWidth: '52ch' }}>
              When a broker drops out of the ISR, Rafka surfaces the why before the what.
              We trade five extra characters of copy for a saved hour of pager-duty roulette.
            </p>

            <div className="card-sub mono" style={{ margin: '20px 0 8px' }}>SMALL · 11 / 500 · 0.08em uppercase</div>
            <div style={{ fontSize: 11, fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: 'var(--ink-3)' }}>Last refreshed 2s ago · live</div>
          </div>

          <div>
            <div className="card-sub mono" style={{ marginBottom: 8 }}>MONO · METRICS</div>
            <div className="mono num" style={{ fontSize: 32, fontWeight: 600, letterSpacing: '-0.015em' }}>
              <span style={{ color: 'var(--rust)' }}>1.42M</span> <span style={{ color: 'var(--ink-3)', fontSize: 14 }}>msg/s</span>
            </div>
            <div className="mono" style={{ fontSize: 11, color: 'var(--jade)', marginTop: 4 }}>▲ 4.1% vs 1h</div>

            <div className="card-sub mono" style={{ margin: '20px 0 8px' }}>MONO · CODE</div>
            <pre className="mono" style={{ margin: 0, background: 'var(--bg-2)', padding: 12, borderRadius: 8, fontSize: 12.5, lineHeight: 1.55, border: '1px solid var(--line-1)' }}>
{`pub async fn append(
    &self,
    records: &[Record],
) -> Result<Offset, AppendError> {
    self.log.append(records).await
}`}
            </pre>

            <div className="card-sub mono" style={{ margin: '20px 0 8px' }}>NUMERIC · TABULAR · OKLCH-TICK</div>
            <div className="mono num" style={{ display: 'flex', gap: 14, fontSize: 16, fontWeight: 500 }}>
              <span><span style={{ color: 'var(--ink-3)' }}>p50</span> 2.1ms</span>
              <span><span style={{ color: 'var(--ink-3)' }}>p95</span> 6.8ms</span>
              <span><span style={{ color: 'var(--rust)' }}>p99</span> 9.4ms</span>
              <span><span style={{ color: 'var(--ink-3)' }}>p99.9</span> 18.2ms</span>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

// ─── Voice & Patches ────────────────────────────────────────────────────────
function Voice() {
  const patches = [
    { c: 'rust',    t: 'p99 < 10ms or your money back' },
    { c: 'jade',    t: 'GC pauses: 0' },
    { c: '',        t: '🦀 unsafe-free since v0.1' },
    { c: 'ice',     t: 'ack=all gang' },
    { c: 'amber',   t: 'consumer lag: skill issue' },
    { c: 'rust',    t: 'no JVM was harmed' },
    { c: '',        t: '$ tail -f /your/business' },
    { c: 'jade',    t: '12 nines club' },
    { c: '',        t: 'ZK who?' },
    { c: 'crimson', t: 'exactly-once (for real this time)' },
    { c: 'ice',     t: 'log-structured baby' },
    { c: '',        t: 'rebalance? hardly knew her' },
    { c: 'rust',    t: 'compaction enjoyer' },
    { c: '',        t: 'wal? more like w-yall' },
    { c: 'jade',    t: 'schema evolution > schema revolution' },
    { c: 'amber',   t: 'produces at line rate' },
  ];

  return (
    <section className="section shell">
      <SectionHead
        index="06"
        label="voice & patches"
        title="A robot in a motorcycle jacket. The patches mean things."
        lead="Rafka talks like the senior engineer who's seen it all and still ships. Dry, specific, occasionally funny — never cute. The patch system gives marketing surfaces and empty states a personality without infecting the data UI."
      />

      <div className="card card-pad-lg">
        <div className="card-sub mono" style={{ marginBottom: 16 }}>PATCH WALL · USE ON: ONBOARDING · EMPTY STATES · MARKETING · STATUS PAGE</div>
        <div className="patch-wall">
          {patches.map((p, i) => (
            <span key={i} className={`patch ${p.c}`}>{p.t}</span>
          ))}
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginTop: 16 }}>
        <div className="card card-pad-lg">
          <div className="card-sub mono" style={{ marginBottom: 12, color: 'var(--jade)' }}>YES, MICROCOPY</div>
          <ul className="list-tight">
            <li><span className="mark good">→</span><span><b>Empty state, topics:</b> "no topics yet. that's not a bug, it's a clean slate. <span className="mono" style={{ color: 'var(--rust)' }}>rafka topic create orders</span>"</span></li>
            <li><span className="mark good">→</span><span><b>Successful publish:</b> "shipped. offset <span className="mono">+13,421</span>."</span></li>
            <li><span className="mark good">→</span><span><b>ISR shrink:</b> "broker-3 fell out of the in-sync replica set. not yet an incident — but worth a glance."</span></li>
            <li><span className="mark good">→</span><span><b>Loading:</b> "pulling 1.4M offsets. won't be a minute." <span className="subtle">(real ETA in mono)</span></span></li>
          </ul>
        </div>
        <div className="card card-pad-lg">
          <div className="card-sub mono" style={{ marginBottom: 12, color: 'var(--crimson)' }}>NO, MICROCOPY</div>
          <ul className="list-tight">
            <li><span className="mark bad">×</span><span><b>"Oopsie!"</b> · ever, for any reason. We're operating production infrastructure.</span></li>
            <li><span className="mark bad">×</span><span><b>Exclamation marks in errors.</b> Errors should describe and link to a fix, not yell.</span></li>
            <li><span className="mark bad">×</span><span><b>Cartoon mascots.</b> The crab is a wink in patches; never a screen actor.</span></li>
            <li><span className="mark bad">×</span><span><b>"We're working on it"</b> · in production telemetry. Say what we're working on.</span></li>
          </ul>
        </div>
      </div>
    </section>
  );
}

// ─── Primitives ─────────────────────────────────────────────────────────────
function Primitives() {
  const Spark = () => (
    <svg className="spark" width="120" height="32" viewBox="0 0 120 32" preserveAspectRatio="none">
      <path className="fill" d="M0,28 L8,22 16,24 24,18 32,20 40,14 48,16 56,10 64,12 72,8 80,11 88,6 96,9 104,5 112,7 120,4 L120,32 L0,32 Z" />
      <path className="line" d="M0,28 L8,22 16,24 24,18 32,20 40,14 48,16 56,10 64,12 72,8 80,11 88,6 96,9 104,5 112,7 120,4" />
    </svg>
  );

  return (
    <section className="section shell">
      <SectionHead
        index="07"
        label="primitives"
        title="The smallest pieces. Everything else is built from these."
        lead="If a component isn't here, it's a remix of two that are. Buttons, pills, status, table rows, sparklines, terminal blocks."
      />

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
        {/* Buttons */}
        <div className="card card-pad-lg">
          <div className="card-sub mono" style={{ marginBottom: 14 }}>BUTTONS</div>
          <div className="row gap-3" style={{ flexWrap: 'wrap', alignItems: 'center' }}>
            <button className="btn primary">Create topic</button>
            <button className="btn">Connect cluster</button>
            <button className="btn ghost">Cancel</button>
            <button className="btn"><span className="kbd">⌘ K</span></button>
          </div>
        </div>

        {/* Pills */}
        <div className="card card-pad-lg">
          <div className="card-sub mono" style={{ marginBottom: 14 }}>STATUS PILLS</div>
          <div className="row gap-2" style={{ flexWrap: 'wrap' }}>
            <span className="pill jade"><span className="dot" />healthy</span>
            <span className="pill amber"><span className="dot" />degraded</span>
            <span className="pill crimson"><span className="dot" />isr drop</span>
            <span className="pill ice"><span className="dot" />syncing</span>
            <span className="pill rust"><span className="dot" />live</span>
            <span className="pill">offline</span>
          </div>
        </div>

        {/* Inputs */}
        <div className="card card-pad-lg">
          <div className="card-sub mono" style={{ marginBottom: 14 }}>INPUTS</div>
          <div className="row gap-3" style={{ flexWrap: 'wrap' }}>
            <input className="input" placeholder="search topics…" style={{ minWidth: 220 }} />
            <input className="input mono" defaultValue="orders.v2" style={{ minWidth: 160 }} />
          </div>
          <div style={{ marginTop: 10, color: 'var(--ink-3)', fontSize: 12 }}>
            Inputs use mono when the value is identifier-shaped; sans when it's prose.
          </div>
        </div>

        {/* Tags */}
        <div className="card card-pad-lg">
          <div className="card-sub mono" style={{ marginBottom: 14 }}>TAGS · KBD · CHIPS</div>
          <div className="row gap-2" style={{ flexWrap: 'wrap', alignItems: 'center' }}>
            <span className="tag">env: prod</span>
            <span className="tag">region: us-east-2</span>
            <span className="tag">partitions: 24</span>
            <span className="kbd">/</span>
            <span className="kbd">j</span>
            <span className="kbd">k</span>
            <span className="kbd">⌘ K</span>
          </div>
        </div>

        {/* Topic row peek */}
        <div className="card" style={{ gridColumn: '1 / -1', padding: 0 }}>
          <div className="card-head">
            <div className="card-title">Topic row · table primitive</div>
            <span className="card-sub mono">live · 2s ago</span>
          </div>
          <div>
            <div className="t-row head mono">
              <div>name</div><div>partitions</div><div>msg/s</div><div>p99</div><div>status</div>
            </div>
            {[
              { n: 'orders.v2',        p: 24, r: '12.4k', l: '8.2ms', s: 'healthy', c: 'jade' },
              { n: 'inventory.updates', p: 12, r: '4.8k',  l: '6.1ms', s: 'healthy', c: 'jade' },
              { n: 'payments.dlq',      p: 6,  r: '0',     l: '—',     s: 'idle',    c: '' },
              { n: 'clickstream.raw',   p: 64, r: '184k',  l: '11.3ms',s: 'degraded',c: 'amber' },
              { n: 'risk.signals',      p: 12, r: '2.1k',  l: '4.7ms', s: 'live',    c: 'rust' },
            ].map((r, i) => (
              <div key={i} className="t-row">
                <div className="name"><span className="ico">T</span><span className="mono">{r.n}</span></div>
                <div className="mono num subtle">{r.p}</div>
                <div className="mono num">{r.r}</div>
                <div className="mono num">{r.l}</div>
                <div><span className={`pill ${r.c}`}>{r.c && <span className="dot" />}{r.s}</span></div>
              </div>
            ))}
          </div>
        </div>

        {/* Sparkline card */}
        <div className="card card-pad-lg">
          <div className="card-sub mono" style={{ marginBottom: 8 }}>SPARKLINE · 5M WINDOW</div>
          <div className="row gap-4" style={{ alignItems: 'flex-end' }}>
            <div className="mono num" style={{ fontSize: 28, fontWeight: 600 }}>
              <span style={{ color: 'var(--rust)' }}>184k</span> <span className="subtle" style={{ fontSize: 13, fontWeight: 400 }}>msg/s</span>
            </div>
            <Spark />
          </div>
          <div className="mono" style={{ marginTop: 6, fontSize: 11, color: 'var(--jade)' }}>▲ 12% · last 5m</div>
        </div>

        {/* Terminal */}
        <div className="term">
          <div className="term-head">
            <span className="lights"><i /><i /><i /></span>
            <span>~/rafka</span>
          </div>
          <div className="term-body">
            <div><span className="prompt">$</span> rafka <span className="arg">topic create</span> orders.v2 <span className="flag">--partitions</span> <span className="num">24</span> <span className="flag">--replicas</span> <span className="num">3</span></div>
            <div className="ok">✓ topic created · 24 partitions · rf=3</div>
            <div><span className="prompt">$</span> rafka <span className="arg">produce</span> orders.v2 <span className="flag">--from</span> <span className="arg">file.jsonl</span></div>
            <div className="dim">streaming 412,318 records…</div>
            <div className="ok">✓ shipped · last offset <span className="num">412317</span> · 9.1ms p99</div>
            <div><span className="prompt">$</span> rafka <span className="arg">tail</span> orders.v2 <span className="flag">--since</span> <span className="num">5m</span></div>
            <div className="dim">tail -f /your/business · ^C to stop</div>
          </div>
        </div>
      </div>
    </section>
  );
}

// ─── IA ─────────────────────────────────────────────────────────────────────
function IA() {
  return (
    <section className="section shell">
      <SectionHead
        index="08"
        label="information architecture"
        title="Org · Env · Cluster · Topic. Two clicks to anywhere."
        lead="The hierarchy borrowed from the enterprise school — without the modal labyrinth on top. Top breadcrumb is always live; cluster switcher lives in ⌘K; everything below cluster is two clicks away."
      />

      <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
        <div className="ia-tree">
          <div className="ia-col">
            <div className="label">Organization</div>
            <div className="ia-item active"><span className="mono glyph">◆</span>acme<span className="meta">3 envs</span></div>
            <div className="ia-item"><span className="mono glyph">◇</span>acme-labs<span className="meta">1 env</span></div>
            <div className="ia-item"><span className="mono glyph">◇</span>acme-eu<span className="meta">2 envs</span></div>
          </div>
          <div className="ia-col">
            <div className="label">Environment</div>
            <div className="ia-item"><span className="mono glyph">◯</span>dev<span className="meta">2 clusters</span></div>
            <div className="ia-item"><span className="mono glyph">◯</span>staging<span className="meta">1 cluster</span></div>
            <div className="ia-item active"><span className="mono glyph">●</span>prod<span className="meta">3 clusters</span></div>
          </div>
          <div className="ia-col">
            <div className="label">Cluster · prod / us-east-2</div>
            <div className="ia-item"><span className="mono glyph">▤</span>Overview<span className="meta">⌘ 1</span></div>
            <div className="ia-item active"><span className="mono glyph">▦</span>Topics<span className="meta">⌘ 2</span></div>
            <div className="ia-item"><span className="mono glyph">▣</span>Consumer groups<span className="meta">⌘ 3</span></div>
            <div className="ia-item"><span className="mono glyph">▥</span>Schema registry<span className="meta">⌘ 4</span></div>
            <div className="ia-item"><span className="mono glyph">⛬</span>Connectors<span className="meta">⌘ 5</span></div>
            <div className="ia-item"><span className="mono glyph">⌬</span>Flink jobs<span className="meta">⌘ 6</span></div>
            <div className="ia-item"><span className="mono glyph">◫</span>ACLs &amp; audit<span className="meta">⌘ 7</span></div>
            <div className="ia-item"><span className="mono glyph">⚙</span>Settings<span className="meta">⌘ ,</span></div>
          </div>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16, marginTop: 16 }}>
        <div className="card card-pad-lg">
          <div className="card-sub mono" style={{ marginBottom: 8 }}>RULE 01</div>
          <div style={{ fontWeight: 600, fontSize: 15, marginBottom: 6 }}>Breadcrumb is the truth</div>
          <p style={{ color: 'var(--ink-2)', margin: 0, fontSize: 13.5 }}>Always shows org / env / cluster / current resource. Each segment is a switcher. No "back" button needed.</p>
        </div>
        <div className="card card-pad-lg">
          <div className="card-sub mono" style={{ marginBottom: 8 }}>RULE 02</div>
          <div style={{ fontWeight: 600, fontSize: 15, marginBottom: 6 }}>⌘K is the highway</div>
          <p style={{ color: 'var(--ink-2)', margin: 0, fontSize: 13.5 }}>Switch cluster, jump to topic by name, run any CLI command, search messages. One palette, fuzzy, scoped by current cluster.</p>
        </div>
        <div className="card card-pad-lg">
          <div className="card-sub mono" style={{ marginBottom: 8 }}>RULE 03</div>
          <div style={{ fontWeight: 600, fontSize: 15, marginBottom: 6 }}>Detail is a side-sheet</div>
          <p style={{ color: 'var(--ink-2)', margin: 0, fontSize: 13.5 }}>Clicking a topic, a partition, a consumer group opens a 480px right-side sheet — preserves table context, supports j/k to walk neighbors.</p>
        </div>
      </div>
    </section>
  );
}

// ─── Peek ───────────────────────────────────────────────────────────────────
function Peek() {
  return (
    <section className="section shell">
      <SectionHead
        index="09"
        label="first peek"
        title="What this all adds up to."
        lead="Not a screen yet — a vibe check. Three primitives composed in the order you'd actually see them on the cluster overview."
      />

      <div className="peek">
        {/* Cluster card */}
        <div className="card peek-cluster-card">
          <div className="card-head">
            <div className="row gap-3" style={{ alignItems: 'center' }}>
              <span className="mono" style={{ color: 'var(--ink-3)', fontSize: 11 }}>acme / prod /</span>
              <span style={{ fontWeight: 600 }}>us-east-2</span>
              <span className="pill rust"><span className="dot" />live</span>
            </div>
            <div className="row gap-2">
              <span className="patch jade square">12 nines · 87d</span>
              <span className="patch rust square">p99 9.4ms</span>
            </div>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)' }}>
            <div className="peek-stat">
              <span className="lbl">throughput</span>
              <span className="val num"><span style={{ color: 'var(--rust)' }}>1.42M</span><span className="unit">msg/s</span></span>
              <span className="delta up mono num">▲ 4.1% · 1h</span>
            </div>
            <div className="peek-stat">
              <span className="lbl">brokers</span>
              <span className="val num">9<span className="unit">/ 9</span></span>
              <span className="delta mono" style={{ color: 'var(--jade)' }}>all in-sync</span>
            </div>
            <div className="peek-stat">
              <span className="lbl">topics</span>
              <span className="val num">142</span>
              <span className="delta mono subtle">3 new today</span>
            </div>
            <div className="peek-stat">
              <span className="lbl">consumer lag</span>
              <span className="val num"><span style={{ color: 'var(--amber)' }}>318k</span></span>
              <span className="delta down mono num">▼ from 612k · 10m</span>
            </div>
          </div>
        </div>

        {/* Message inspector preview */}
        <div className="card" style={{ padding: 0 }}>
          <div className="card-head">
            <div className="row gap-2" style={{ alignItems: 'center' }}>
              <span className="mono" style={{ fontWeight: 600 }}>orders.v2</span>
              <span className="tag">partition 7</span>
              <span className="tag">offset 412,317</span>
            </div>
            <span className="card-sub mono">json · avro-decoded</span>
          </div>
          <div style={{ padding: 14, fontFamily: 'JetBrains Mono, monospace', fontSize: 12.5, lineHeight: 1.6 }}>
            <div><span style={{ color: 'var(--ink-3)' }}>{'{'}</span></div>
            <div style={{ paddingLeft: 16 }}>
              <span style={{ color: 'var(--ember)' }}>"order_id"</span>: <span style={{ color: 'var(--jade)' }}>"ord_01HQX9K2N"</span>,
            </div>
            <div style={{ paddingLeft: 16 }}>
              <span style={{ color: 'var(--ember)' }}>"customer"</span>: <span style={{ color: 'var(--jade)' }}>"cus_8F2Q3"</span>,
            </div>
            <div style={{ paddingLeft: 16 }}>
              <span style={{ color: 'var(--ember)' }}>"total_cents"</span>: <span style={{ color: 'var(--violet)' }}>4299</span>,
            </div>
            <div style={{ paddingLeft: 16 }}>
              <span style={{ color: 'var(--ember)' }}>"ts"</span>: <span style={{ color: 'var(--jade)' }}>"2026-05-10T18:42:11Z"</span>
            </div>
            <div><span style={{ color: 'var(--ink-3)' }}>{'}'}</span></div>
            <hr className="hr" style={{ margin: '14px 0' }} />
            <div className="row gap-2" style={{ flexWrap: 'wrap' }}>
              <button className="btn ghost">← prev offset</button>
              <button className="btn ghost">next →</button>
              <button className="btn">Replay from here</button>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

// ─── Closer ─────────────────────────────────────────────────────────────────
function Closer() {
  return (
    <section className="section shell">
      <SectionHead
        index="10"
        label="next"
        title="Where this goes from here."
        lead="If the vibe lands, the next pass is hero screens — cluster overview, topic browser + message inspector, consumer-group lag detail, schema evolution, ACL editor. Each will be a fullscreen artboard inside one canvas so you can compare side-by-side."
      />

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12 }}>
        {[
          { n: '01', t: 'Cluster overview', d: 'Throughput, broker matrix, ISR health, top partitions, recent audit.' },
          { n: '02', t: 'Topic browser + message inspector', d: 'Search · filter · replay · decode (JSON, Avro, Proto) · jump by offset/time.' },
          { n: '03', t: 'Consumer groups + lag', d: 'Group → member → assignment tree. Lag with ETA-to-zero. Rebalance feed.' },
          { n: '04', t: 'Schema registry', d: 'Subject list, compatibility matrix, evolution diff, who-broke-what.' },
          { n: '05', t: 'ACLs &  audit', d: 'Subject × resource grid. Audit log streaming. Diff between snapshots.' },
          { n: '06', t: '⌘K command palette', d: 'The thing that ties it together. Demo this last; it will sell the room.' },
        ].map((s) => (
          <div key={s.n} className="card card-pad" style={{ display: 'flex', flexDirection: 'column', gap: 6, minHeight: 110 }}>
            <span className="card-sub mono">{s.n}</span>
            <div style={{ fontWeight: 600, fontSize: 14.5 }}>{s.t}</div>
            <div style={{ color: 'var(--ink-2)', fontSize: 13, lineHeight: 1.5 }}>{s.d}</div>
          </div>
        ))}
      </div>

      <div className="foot">
        <div className="row gap-3" style={{ alignItems: 'baseline', flexWrap: 'wrap' }}>
          <span className="mono" style={{ color: 'var(--rust)' }}>rafka</span>
          <span className="mono">·</span>
          <span>design study v0.1</span>
          <span className="mono">·</span>
          <span>built in the same week we wrote the log compaction routine</span>
          <span className="mono">·</span>
          <span>🦀</span>
        </div>
      </div>
    </section>
  );
}

Object.assign(window, { TypeSpecimen, Voice, Primitives, IA, Peek, Closer });
