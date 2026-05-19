// Rafka — Design Study v0.1
// Sections: masthead, brief, field study, principles, palette, type, voice, primitives, IA, peek, closer.

const Eyebrow = ({ index, label }) => (
  <div className="section-eyebrow">
    <span className="dot">●</span>&nbsp;{index} &nbsp;·&nbsp; {label}
  </div>
);

const SectionHead = ({ index, label, title, lead, right }) => (
  <div className="section-head">
    <div>
      <Eyebrow index={index} label={label} />
      <h2 className="section-title">{title}</h2>
      {lead && <p className="section-lead">{lead}</p>}
    </div>
    {right}
  </div>
);

// ─── Masthead ───────────────────────────────────────────────────────────────
function Masthead() {
  return (
    <header className="masthead shell">
      <div className="meta-row">
        <span>RAFKA</span>
        <span className="sep">/</span>
        <span>DESIGN STUDY v0.1</span>
        <span className="sep">/</span>
        <span>MAY 2026</span>
        <span className="sep">/</span>
        <span style={{ color: 'var(--rust)' }}>● DRAFT</span>
      </div>
      <h1>
        rafka<span className="accent">.</span>
      </h1>
      <div className="tagline">
        an event-streaming console for people who'd rather be <em>shipping</em>.
        kafka-compatible, written in rust, allergic to enterprise sprawl.
      </div>
      <div className="crab-line">
        <span className="crab">🦀</span>
        <span>$ rafka --version</span>
        <span style={{ color: 'var(--ink-2)' }}>0.1.0-prelude</span>
        <span className="sep" style={{ color: 'var(--ink-4)' }}>·</span>
        <span style={{ color: 'var(--jade)' }}>no JVM was harmed</span>
      </div>
      <div className="cta-row">
        <button className="btn primary">Read the study ↓</button>
        <button className="btn ghost"><span className="kbd">⌘ K</span>&nbsp;jump to section</button>
      </div>
    </header>
  );
}

// ─── Section: The Brief ─────────────────────────────────────────────────────
function Brief() {
  return (
    <section className="section shell">
      <SectionHead
        index="01"
        label="the brief"
        title="A console that feels like a great CLI, not a CRM."
        lead="Rafka is Kafka-wire-compatible, written in Rust. It collapses brokers, connectors, and Flink-style stream processing into one runtime — so the console should collapse them into one home. The goal of v0.1 isn't to ship every screen; it's to lock the voice, the tokens, and the IA so every future screen feels like the same product."
      />
      <div className="row gap-3" style={{ flexWrap: 'wrap' }}>
        <span className="tag">audience: app devs · SREs · data eng · governance · AI builders</span>
        <span className="tag">scope v0.1: cluster · topics · consumer groups · schema · ACLs</span>
        <span className="tag">deferred: connectors UI · flink job graph · billing</span>
        <span className="tag">platforms: web (primary) · CLI parity</span>
      </div>
    </section>
  );
}

// ─── Section: Field Study ───────────────────────────────────────────────────
function FieldStudy() {
  return (
    <section className="section shell">
      <SectionHead
        index="02"
        label="field study"
        title="What category leaders get right — and where they hurt."
        lead="Two consoles dominate this space. Both solved real problems; both carry scars from doing so. Pattern observations, not screenshots — Rafka's UI is its own."
      />

      <div className="two-col">
        <div className="card card-pad-lg">
          <div className="card-sub mono" style={{ marginBottom: 6 }}>OBSERVATION SET A</div>
          <h3 style={{ margin: '0 0 14px', fontSize: 18, letterSpacing: '-0.01em' }}>The "managed-enterprise" school</h3>
          <ul className="list-tight">
            <li><span className="mark good">+</span><span><b>Org → Env → Cluster hierarchy</b> is genuinely useful at scale; teams expect it.</span></li>
            <li><span className="mark good">+</span><span><b>RBAC + audit log</b> are deep and battle-tested. Compliance teams trust it.</span></li>
            <li><span className="mark good">+</span><span><b>Connector catalog</b> is a real moat — discoverability of source/sink integrations.</span></li>
            <li><span className="mark bad">−</span><span><b>Tabs nested in tabs.</b> The IA is a sales-org diagram. Three clicks to reach a topic.</span></li>
            <li><span className="mark bad">−</span><span><b>Modal heaviness.</b> Every interaction launches a modal. No keyboard escape velocity.</span></li>
            <li><span className="mark bad">−</span><span><b>No first-class CLI parity.</b> The web UI and CLI feel like different products.</span></li>
            <li><span className="mark bad">−</span><span><b>Stale data, generously displayed.</b> Charts update every 60s; "live" is a lie.</span></li>
          </ul>
        </div>

        <div className="card card-pad-lg">
          <div className="card-sub mono" style={{ marginBottom: 6 }}>OBSERVATION SET B</div>
          <h3 style={{ margin: '0 0 14px', fontSize: 18, letterSpacing: '-0.01em' }}>The "developer-native" school</h3>
          <ul className="list-tight">
            <li><span className="mark good">+</span><span><b>Clean topic browser.</b> Topics list → partitions → messages, no detour.</span></li>
            <li><span className="mark good">+</span><span><b>Message viewer with replay.</b> Pick offset, jump, decode JSON/Avro/Proto inline.</span></li>
            <li><span className="mark good">+</span><span><b>Honest empty states.</b> Tells you the shell command that would do the same thing.</span></li>
            <li><span className="mark bad">−</span><span><b>Cluster overview is anemic.</b> Throughput is shown; saturation, hot partitions, ISR drift aren't.</span></li>
            <li><span className="mark bad">−</span><span><b>Schema registry is an afterthought.</b> Lives off in a side panel; evolution rules are hidden.</span></li>
            <li><span className="mark bad">−</span><span><b>Multi-cluster is awkward.</b> Switching clusters refreshes the world.</span></li>
            <li><span className="mark bad">−</span><span><b>Visual identity is muted.</b> Functional, but forgettable. No motorcycle jacket.</span></li>
          </ul>
        </div>
      </div>

      <hr className="hr" />

      <div className="card card-pad-lg">
        <div className="card-sub mono" style={{ marginBottom: 8 }}>SYNTHESIS</div>
        <h3 style={{ margin: '0 0 12px', fontSize: 20, letterSpacing: '-0.015em' }}>What Rafka borrows, what Rafka kills.</h3>
        <div className="row gap-6" style={{ flexWrap: 'wrap' }}>
          <div style={{ flex: '1 1 280px' }}>
            <div className="card-sub mono" style={{ marginBottom: 8, color: 'var(--jade)' }}>BORROW</div>
            <ul className="list-tight">
              <li><span className="mark good">✓</span><span>Org → Env → Cluster → Topic spine.</span></li>
              <li><span className="mark good">✓</span><span>RBAC + immutable audit log out of the box.</span></li>
              <li><span className="mark good">✓</span><span>First-class message inspector with replay.</span></li>
              <li><span className="mark good">✓</span><span>Schema registry treated as a peer of topics, not an island.</span></li>
            </ul>
          </div>
          <div style={{ flex: '1 1 280px' }}>
            <div className="card-sub mono" style={{ marginBottom: 8, color: 'var(--crimson)' }}>KILL</div>
            <ul className="list-tight">
              <li><span className="mark bad">×</span><span>Nested tab labyrinths. Max two levels of nav.</span></li>
              <li><span className="mark bad">×</span><span>Modals for routine reads. Side-sheets &amp; inline edit instead.</span></li>
              <li><span className="mark bad">×</span><span>Polling pretending to be streaming. If it's live, it's a WebSocket.</span></li>
              <li><span className="mark bad">×</span><span>Beige professionalism. We are a Rust thing in a leather jacket.</span></li>
            </ul>
          </div>
        </div>
      </div>
    </section>
  );
}

// ─── Section: Principles ────────────────────────────────────────────────────
function Principles() {
  const items = [
    { t: 'Streams are live or they are lies', d: 'Every metric carries a freshness ticker. WebSocket-first; polling only when the user opted out. If we are showing a number, we mean it as of right now.' },
    { t: 'Keyboard before mouse', d: '⌘K palette, j/k row nav, gg to top, / to filter. Every action has a hotkey, every hotkey has a discovery affordance. Mouse is a fallback.' },
    { t: 'Show the shell command', d: 'Anything you can do in the UI shows the equivalent rafka CLI invocation. Copy-paste friendly. The console is a teaching surface, not a moat.' },
    { t: 'Confidence over decoration', d: 'No gradients-for-the-sake-of. No cards with shadows that pretend to be tactile. Hairlines, type, and color do the work.' },
    { t: 'Edges, not blobs', d: 'Crisp 1px lines, square corners on data surfaces, generous radius only on inputs and pills. Rust accent earns its keep.' },
    { t: 'Funny is fine; cute is not', d: 'Patches and copy can wink at the dev. Microcopy can have a heartbeat. No cartoon mascots, no "oopsie" empty states.' },
  ];
  return (
    <section className="section shell">
      <SectionHead
        index="03"
        label="principles"
        title="Six rules we'll use to argue with each other."
        lead="When two designs feel close, these decide. When a design violates one, we either change the design or change the principle — never both at once."
      />
      <div className="principles">
        {items.map((p, i) => (
          <div key={i} className="principle">
            <span className="num">{String(i + 1).padStart(2, '0')}</span>
            <h3>{p.t}</h3>
            <p>{p.d}</p>
          </div>
        ))}
      </div>
    </section>
  );
}

// ─── Section: Palette ───────────────────────────────────────────────────────
function Palette() {
  const surfaces = [
    { n: 'bg-0', v: '--bg-0', note: 'page' },
    { n: 'bg-1', v: '--bg-1', note: 'card' },
    { n: 'bg-2', v: '--bg-2', note: 'hover' },
    { n: 'bg-3', v: '--bg-3', note: 'active' },
  ];
  const ink = [
    { n: 'ink-1', v: '--ink-1', note: 'primary' },
    { n: 'ink-2', v: '--ink-2', note: 'body' },
    { n: 'ink-3', v: '--ink-3', note: 'muted' },
    { n: 'ink-4', v: '--ink-4', note: 'disabled' },
  ];
  const accents = [
    { n: 'rust', v: '--rust', note: 'primary brand · action' },
    { n: 'rust-2', v: '--rust-2', note: 'rust hover/active' },
    { n: 'ember', v: '--ember', note: 'warm support' },
    { n: 'jade', v: '--jade', note: 'healthy · 200' },
    { n: 'amber', v: '--amber', note: 'warn · degraded' },
    { n: 'crimson', v: '--crimson', note: 'error · 5xx · ISR drop' },
    { n: 'ice', v: '--ice', note: 'info · neutral metric' },
    { n: 'violet', v: '--violet', note: 'numbers · code keyword' },
  ];

  const Sw = ({ s }) => (
    <div className="swatch">
      <div className="chip" style={{ background: `var(${s.v})` }} />
      <div className="meta">
        <div className="name">{s.n}</div>
        <div className="val">{s.note}</div>
      </div>
    </div>
  );

  return (
    <section className="section shell">
      <SectionHead
        index="04"
        label="palette"
        title="Warm-tinted neutrals. Rust as the only voice."
        lead="Dark-first because devs live there. Backgrounds carry a tiny warm chroma (hue 50) so the dark mode reads like fired clay, not slate. Rust is the single load-bearing accent; jade/amber/crimson exist only to mean something."
      />

      <div className="col gap-6">
        <div>
          <div className="card-sub mono" style={{ marginBottom: 10 }}>SURFACES</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
            {surfaces.map((s) => <Sw key={s.n} s={s} />)}
          </div>
        </div>
        <div>
          <div className="card-sub mono" style={{ marginBottom: 10 }}>INK</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
            {ink.map((s) => <Sw key={s.n} s={s} />)}
          </div>
        </div>
        <div>
          <div className="card-sub mono" style={{ marginBottom: 10 }}>ACCENTS · USE WITH INTENT</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
            {accents.map((s) => <Sw key={s.n} s={s} />)}
          </div>
        </div>
      </div>

      <hr className="hr" />

      <div className="card card-pad-lg">
        <div className="card-sub mono" style={{ marginBottom: 12 }}>USAGE GUARDRAILS</div>
        <div className="row gap-6" style={{ flexWrap: 'wrap' }}>
          <div style={{ flex: '1 1 240px' }}>
            <div style={{ fontWeight: 600, marginBottom: 6 }}>Rust is sparing.</div>
            <p style={{ color: 'var(--ink-2)', margin: 0, fontSize: 13.5 }}>One rust element in view at a time, ideally. Reserved for: primary action, brand mark, "live now" indicator, selected row.</p>
          </div>
          <div style={{ flex: '1 1 240px' }}>
            <div style={{ fontWeight: 600, marginBottom: 6 }}>Status colors mean status.</div>
            <p style={{ color: 'var(--ink-2)', margin: 0, fontSize: 13.5 }}>Jade is healthy, amber is degraded, crimson is incident. They never carry brand or decoration weight.</p>
          </div>
          <div style={{ flex: '1 1 240px' }}>
            <div style={{ fontWeight: 600, marginBottom: 6 }}>No raw black, no raw white.</div>
            <p style={{ color: 'var(--ink-2)', margin: 0, fontSize: 13.5 }}>The dark surface is oklch(0.155 0.005 50). White text is oklch(0.96). Pure values feel cheap; the warmth carries the brand.</p>
          </div>
        </div>
      </div>
    </section>
  );
}

Object.assign(window, { Eyebrow, SectionHead, Masthead, Brief, FieldStudy, Principles, Palette });
