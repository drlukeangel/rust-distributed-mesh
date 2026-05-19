// Rafka — System (platform / SRE view)
// Three artboards in one file: mesh overview → component type → single node.

/* ──────────────────────────────────────────────────────────────
   shared bits
   ──────────────────────────────────────────────────────────── */
function Spark({ data, color = 'var(--rust)', h = 22, w = 60 }) {
  const max = Math.max(...data), min = Math.min(...data);
  const path = data.map((v, i) =>
    `${i ? 'L' : 'M'}${(i / (data.length - 1)) * w},${h - ((v - min) / Math.max(max - min, 0.001)) * (h - 2) - 1}`
  ).join(' ');
  return (
    <svg width={w} height={h} style={{ display: 'block' }}>
      <path d={path} fill="none" stroke={color} strokeWidth="1.2" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

function genSeries(n, base, jitter, trend = 0, seed = 1) {
  const out = [];
  let v = base;
  let s = seed;
  for (let i = 0; i < n; i++) {
    s = (s * 9301 + 49297) % 233280;
    const r = s / 233280;
    v = v + (r - 0.5) * jitter + trend;
    out.push(v);
  }
  return out;
}

/* ──────────────────────────────────────────────────────────────
   Artboard 1 — System mesh overview
   ──────────────────────────────────────────────────────────── */
function SystemMesh() {
  // node coordinates within a 1280×620 stage
  const dpg = [
    { id: 'dpg-1', x: 60,  y: 70,  state: 'ok',   rps: 18.4, p99: 4.2 },
    { id: 'dpg-2', x: 60,  y: 170, state: 'ok',   rps: 17.9, p99: 4.0 },
    { id: 'dpg-3', x: 60,  y: 270, state: 'warn', rps: 21.2, p99: 11.8 },
    { id: 'dpg-4', x: 60,  y: 370, state: 'ok',   rps: 16.1, p99: 3.9 },
    { id: 'dpg-5', x: 60,  y: 470, state: 'ok',   rps: 17.4, p99: 4.4 },
  ];
  const sr = [
    { id: 'sr-1', x: 350, y: 120, state: 'ok' },
    { id: 'sr-2', x: 350, y: 220, state: 'ok' },
    { id: 'sr-3', x: 350, y: 320, state: 'ok' },
  ];
  const cpg = [
    { id: 'cpg-1', x: 350, y: 430, state: 'ok',  jobs: 142, fuel: 0.42 },
    { id: 'cpg-2', x: 350, y: 530, state: 'fail', jobs: 138, fuel: 0.99 },
  ];
  const brokers = [
    { id: 'br-1', x: 700, y: 60,  state: 'ok', lead: 18, isr: '3/3' },
    { id: 'br-2', x: 700, y: 140, state: 'ok', lead: 21, isr: '3/3' },
    { id: 'br-3', x: 700, y: 220, state: 'ok', lead: 19, isr: '3/3' },
    { id: 'br-4', x: 700, y: 300, state: 'ok', lead: 20, isr: '3/3' },
    { id: 'br-5', x: 700, y: 380, state: 'warn', lead: 26, isr: '2/3' },
    { id: 'br-6', x: 700, y: 460, state: 'ok', lead: 18, isr: '3/3' },
    { id: 'br-7', x: 700, y: 540, state: 'ok', lead: 22, isr: '3/3' },
  ];
  const storage = [
    { id: 's3-hot',  x: 1040, y: 180, label: 's3 · hot',  meta: '482 GB / 1 TB' },
    { id: 's3-cold', x: 1040, y: 320, label: 's3 · cold', meta: '14.2 TB tiered' },
    { id: 's3-bk',   x: 1040, y: 460, label: 's3 · meta', meta: 'compacted + WAL' },
  ];

  // edges — keep a tidy subset rather than O(n²) spaghetti
  const edges = [];
  // dpg → schema-registry (any-of)
  dpg.forEach((d, i) => sr.forEach((s, j) => {
    if ((i + j) % 2 === 0) edges.push([d, s, 'sr', d.state === 'warn' ? 'warn' : 'mute']);
  }));
  // dpg → broker (every dpg fans out to every broker, but we draw a sample)
  dpg.forEach((d, i) => brokers.forEach((b, j) => {
    if ((i + j) % 3 === 0) edges.push([d, b, 'kafka', d.state === 'warn' && b.state === 'warn' ? 'warn' : 'lite']);
  }));
  // cpg → broker (job topics)
  cpg.forEach((c, i) => brokers.forEach((b, j) => {
    if ((i * 2 + j) % 3 === 0) edges.push([c, b, 'jobs', c.state === 'fail' ? 'fail' : 'lite']);
  }));
  // broker mesh (replication)
  for (let i = 0; i < brokers.length - 1; i++) edges.push([brokers[i], brokers[i + 1], 'repl', 'repl']);
  // broker → s3
  brokers.forEach((b, j) => {
    const t = storage[j % storage.length];
    if (j % 2 === 0) edges.push([b, t, 's3', 'mute']);
  });

  // dpg gossip mesh (curve between sibling dpgs)
  const gossip = [];
  for (let i = 0; i < dpg.length - 1; i++) gossip.push([dpg[i], dpg[i + 1]]);

  const edgeColor = (k) => ({
    sr:    'oklch(0.78 0.14 250 / 0.45)',
    kafka: 'oklch(0.74 0.18 50 / 0.4)',
    jobs:  'oklch(0.74 0.18 50 / 0.45)',
    repl:  'oklch(0.65 0.13 160 / 0.5)',
    s3:    'oklch(0.7 0.05 250 / 0.35)',
    lite:  'oklch(0.65 0.04 60 / 0.25)',
    warn:  'oklch(0.84 0.16 70 / 0.7)',
    fail:  'oklch(0.7 0.22 25 / 0.8)',
    mute:  'oklch(0.5 0.03 60 / 0.3)',
  }[k] || 'oklch(0.5 0.03 60 / 0.3)');

  const path = (a, b) => {
    const dx = b.x - a.x;
    const c1x = a.x + dx * 0.55;
    const c2x = a.x + dx * 0.45;
    return `M ${a.x + 50} ${a.y + 20} C ${c1x} ${a.y + 20}, ${c2x} ${b.y + 20}, ${b.x} ${b.y + 20}`;
  };

  return (
    <Shell active="system" breadcrumb={['acme', 'prod', 'us-east-2', 'system']}
      title="platform · system mesh"
      sub="data-plane-gateway · schema-registry · compute-gateway · broker io-pump · 17 nodes · OTLP nominal"
      actions={<>
        <button className="btn ghost">Topology snapshot</button>
        <button className="btn ghost">Time range · 1h</button>
        <button className="btn primary">Open incident</button>
      </>}>

      {/* Top KPI strip */}
      <div className="panel" style={{ padding: 0, marginBottom: 14 }}>
        <div className="sys-kpis">
          <SysKpi lbl="Nodes" val="17" sub="dpg 5 · sr 3 · cpg 2 · br 7" />
          <SysKpi lbl="OTLP spans / s" val="84.2k" sub="rafka.* surface · nominal" spark={genSeries(36, 80, 12, 0.1, 7)} />
          <SysKpi lbl="REST p99" val="11.8" unit="ms" sub="dpg-3 above SLO" warn spark={genSeries(36, 6, 2, 0.18, 11)} />
          <SysKpi lbl="Kafka produce p99" val="4.6" unit="ms" sub="acks=all, all partitions" spark={genSeries(36, 4.4, 0.8, 0, 17)} />
          <SysKpi lbl="ACL deny rate" val="0.38" unit="%" sub="security-ok" spark={genSeries(36, 0.4, 0.1, -0.001, 23)} />
          <SysKpi lbl="Active jobs" val="280" sub="142 · 138 on cpg" />
          <SysKpi lbl="Cluster fuel" val="71" unit="%" sub="WASM jobs · 30s avg" warn={false} spark={genSeries(36, 70, 5, 0, 31)} />
          <SysKpi lbl="Open alerts" val="3" sub="1 high · 2 warn" warn />
        </div>
      </div>

      <div className="sys-grid">
        {/* ── Mesh stage ── */}
        <div className="panel sys-mesh">
          <div className="sys-mesh-h">
            <div>
              <h3>Live topology</h3>
              <div className="dim mono">17 nodes · 84 edges · gossip RTT 1.4ms p50 · mesh fully convergent</div>
            </div>
            <div className="sys-legend mono">
              <span><i style={{ background: 'oklch(0.74 0.18 50)' }} />kafka wire</span>
              <span><i style={{ background: 'oklch(0.65 0.13 160)' }} />replication</span>
              <span><i style={{ background: 'oklch(0.78 0.14 250)' }} />schema lookup</span>
              <span><i style={{ background: 'oklch(0.7 0.05 250)' }} />tiered s3</span>
              <span><i style={{ background: 'oklch(0.84 0.16 70)' }} />degraded</span>
            </div>
          </div>

          <div className="sys-stage">
            <svg viewBox="0 0 1280 620" preserveAspectRatio="xMidYMid meet" style={{ width: '100%', height: 620 }}>
              <defs>
                <marker id="msh-arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto">
                  <path d="M0,0 L10,5 L0,10 z" fill="var(--ink-3)" />
                </marker>
                <linearGradient id="flow" x1="0" x2="1">
                  <stop offset="0" stopColor="oklch(0.74 0.18 50)" stopOpacity="0" />
                  <stop offset="0.5" stopColor="oklch(0.74 0.18 50)" stopOpacity="1" />
                  <stop offset="1" stopColor="oklch(0.74 0.18 50)" stopOpacity="0" />
                </linearGradient>
              </defs>

              {/* column labels */}
              <text x="100" y="30" className="sys-lane">data-plane-gateway · 5</text>
              <text x="380" y="30" className="sys-lane">schema-registry · 3</text>
              <text x="380" y="408" className="sys-lane">compute-gateway · 2</text>
              <text x="720" y="30" className="sys-lane">broker io-pump · 7</text>
              <text x="1058" y="30" className="sys-lane">storage</text>

              {/* dpg gossip — curved dashed */}
              {gossip.map(([a, b], i) => (
                <path key={'g' + i}
                  d={`M ${a.x + 12} ${a.y + 20} C ${a.x - 20} ${(a.y + b.y) / 2 + 20}, ${a.x - 20} ${(a.y + b.y) / 2 + 20}, ${b.x + 12} ${b.y + 20}`}
                  fill="none" stroke="oklch(0.74 0.18 50 / 0.35)" strokeWidth="1" strokeDasharray="3 3" />
              ))}
              <text x="14" y="540" className="sys-edge-lbl">QUIC gossip mesh · entity caches</text>

              {/* edges */}
              {edges.map(([a, b, k, st], i) => (
                <path key={'e' + i} d={path(a, b)} fill="none"
                  stroke={edgeColor(st === 'warn' ? 'warn' : st === 'fail' ? 'fail' : k)}
                  strokeWidth={st === 'warn' || st === 'fail' ? 1.6 : 1}
                  strokeDasharray={k === 's3' ? '2 3' : undefined} />
              ))}

              {/* broker ring (replication mesh hint) */}
              <path d={`M ${brokers[0].x + 25} ${brokers[0].y + 20} Q ${brokers[0].x + 80} ${(brokers[0].y + brokers[brokers.length - 1].y) / 2 + 20} ${brokers[brokers.length - 1].x + 25} ${brokers[brokers.length - 1].y + 20}`}
                fill="none" stroke="oklch(0.65 0.13 160 / 0.5)" strokeWidth="1" />

              {/* nodes */}
              {dpg.map((n, i) => <MeshNode key={n.id} {...n} kind="dpg"  label={`gw-${i + 1}`} sub={`${n.rps}k rps · p99 ${n.p99}ms`} />)}
              {sr.map((n, i)  => <MeshNode key={n.id} {...n} kind="sr"   label={`sr-${i + 1}`} sub="bindings ok · 0 reject" />)}
              {cpg.map((n, i) => <MeshNode key={n.id} {...n} kind="cpg"  label={`cpg-${i + 1}`} sub={`${n.jobs} jobs · fuel ${Math.round(n.fuel * 100)}%`} />)}
              {brokers.map((n, i) => <MeshNode key={n.id} {...n} kind="br" label={`br-${i + 1}`} sub={`leads ${n.lead} · ISR ${n.isr}`} />)}
              {storage.map((n) => <MeshNode key={n.id} x={n.x} y={n.y} state="ok" kind="s3" label={n.label} sub={n.meta} />)}

              {/* animated flow dot on kafka edge dpg-1 → br-3 */}
              <circle r="3" fill="oklch(0.95 0.16 50)">
                <animateMotion dur="2.4s" repeatCount="indefinite"
                  path={path(dpg[0], brokers[2])} />
              </circle>
              <circle r="3" fill="oklch(0.95 0.16 50)">
                <animateMotion dur="2.4s" begin="0.8s" repeatCount="indefinite"
                  path={path(dpg[1], brokers[0])} />
              </circle>
              <circle r="3" fill="oklch(0.84 0.16 70)">
                <animateMotion dur="1.8s" repeatCount="indefinite"
                  path={path(cpg[1], brokers[4])} />
              </circle>
            </svg>
          </div>
        </div>

        {/* ── Side rail: alerts + OTLP heartbeat ── */}
        <div className="sys-side">
          <div className="panel sys-alerts">
            <div className="panel-h">
              <div>
                <div className="title">Active alerts</div>
                <div className="sub mono">3 open · last fired 14:21:08</div>
              </div>
              <span className="pill amber" style={{ height: 20 }}><span className="dot" />1 high</span>
            </div>
            <div className="sys-alert hi">
              <div className="bar" />
              <div className="row">
                <span className="mono lvl">high</span>
                <span className="mono nm">cpg-2 · fuel saturation</span>
                <span className="mono dim age">2m</span>
              </div>
              <div className="msg">WASM fuel consumption at 99% for 90s. Job-claim CAS pressure rising; expect heartbeat drift.</div>
              <div className="tgt mono">page · oncall-platform · acked by m.ortiz</div>
            </div>
            <div className="sys-alert wa">
              <div className="bar" />
              <div className="row">
                <span className="mono lvl">warn</span>
                <span className="mono nm">dpg-3 · REST p99 over SLO</span>
                <span className="mono dim age">8m</span>
              </div>
              <div className="msg">/topics route p99 = 11.8ms (SLO 8ms). Tail-offset lag from broker on `schema_bindings` 4.2s.</div>
              <div className="tgt mono">notify · #rafka-prod</div>
            </div>
            <div className="sys-alert wa">
              <div className="bar" />
              <div className="row">
                <span className="mono lvl">warn</span>
                <span className="mono nm">br-5 · ISR shrink</span>
                <span className="mono dim age">17m</span>
              </div>
              <div className="msg">Partition `payments.events:14` ISR 2/3 after follower disconnect. Replication QUIC reconnect rate ↑.</div>
              <div className="tgt mono">notify · #rafka-prod</div>
            </div>
          </div>

          <div className="panel sys-otlp">
            <div className="panel-h">
              <div>
                <div className="title">OTLP heartbeat</div>
                <div className="sub mono">span volume per rafka.* name — early-warning floor</div>
              </div>
              <span className="pill jade" style={{ height: 20 }}><span className="dot" />all firing</span>
            </div>
            {[
              { n: 'rafka.gateway.acl.compile',          v: '0.34 /s', s: 'green', spark: genSeries(40, 0.34, 0.06, 0, 41) },
              { n: 'rafka.gateway.rest.request',         v: '84.2k /s', s: 'green', spark: genSeries(40, 84, 8, 0, 43) },
              { n: 'rafka.gateway.cache.tail.apply',     v: '12.4 /s', s: 'green', spark: genSeries(40, 12, 2, 0, 47) },
              { n: 'rafka.schema.compatibility.check',   v: '142 /s', s: 'green', spark: genSeries(40, 140, 18, 0, 53) },
              { n: 'rafka.compute.job.claim.cas',        v: '8.1 /s', s: 'amber', spark: genSeries(40, 6, 1.5, 0.06, 59) },
              { n: 'rafka.compute.silent.double.claim',  v: '0 /s', s: 'green', spark: Array(40).fill(0) },
              { n: 'rafka.broker.segment.append.via-wal',v: '142.4k /s', s: 'green', spark: genSeries(40, 140, 14, 0, 61) },
              { n: 'rafka.broker.fsync.window.close',    v: '24 /s', s: 'green', spark: genSeries(40, 24, 2, 0, 67) },
            ].map((r) => (
              <div key={r.n} className="otlp-row">
                <span className={'dot ' + r.s} />
                <span className="mono nm">{r.n}</span>
                <Spark data={r.spark} h={18} w={80} color={r.s === 'amber' ? 'var(--amber)' : r.s === 'green' ? 'var(--jade)' : 'var(--ink-3)'} />
                <span className="mono v">{r.v}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* per-type cards (entry to drill-down) */}
      <div className="sys-types">
        <TypeCard kind="dpg" name="data-plane-gateway" count={5} state="warn"
          desc="Customer-facing. Kafka wire + REST mgmt plane. ACL compile. QUIC gossip mesh of entity caches."
          metrics={[
            ['REST p99', '11.8ms', 'warn'],
            ['auth fail/s', '0.4', 'ok'],
            ['cache apply lag', '4.2s', 'warn'],
            ['gossip RTT p50', '1.4ms', 'ok'],
          ]}
          spark={genSeries(48, 80, 10, 0.1, 71)} />
        <TypeCard kind="sr" name="schema-registry" count={3} state="ok"
          desc="Avro / Protobuf / JSON schemas + bindings. Compatibility gates at register and produce time."
          metrics={[
            ['register/s', '4.1', 'ok'],
            ['reject %', '0.02', 'ok'],
            ['bound cache hit', '99.8%', 'ok'],
            ['version count', '847', 'ok'],
          ]}
          spark={genSeries(48, 4, 0.6, 0, 73)} />
        <TypeCard kind="cpg" name="compute-gateway" count={2} state="fail"
          desc="WASM jobs runtime · dispatcher state machine · RSQL · connectors · DQ rules · org tailers."
          metrics={[
            ['job ttf p99', '420ms', 'ok'],
            ['fuel max', '99%', 'fail'],
            ['stale reclaim/s', '0.3', 'warn'],
            ['silent_double_claim', '0', 'ok'],
          ]}
          spark={genSeries(48, 60, 8, 0.5, 79)} />
        <TypeCard kind="br" name="broker io-pump" count={7} state="warn"
          desc="The log itself. WAL · segments · compaction · tiered s3 · replication · partition leadership."
          metrics={[
            ['WAL append p99', '1.8ms', 'ok'],
            ['repl lag p99', '38ms', 'ok'],
            ['cold-tier hit', '4.1%', 'ok'],
            ['ISR shrink/h', '2', 'warn'],
          ]}
          spark={genSeries(48, 140, 12, 0, 83)} />
      </div>
    </Shell>
  );
}

function SysKpi({ lbl, val, unit, sub, spark, warn }) {
  return (
    <div className={'sys-kpi' + (warn ? ' warn' : '')}>
      <div className="lbl mono">{lbl}</div>
      <div className="val">{val}{unit && <span className="u">{unit}</span>}</div>
      <div className="sub mono">{sub}</div>
      {spark && <Spark data={spark} h={20} w={120} color={warn ? 'var(--amber)' : 'var(--rust)'} />}
    </div>
  );
}

function MeshNode({ x, y, state, kind, label, sub }) {
  const w = kind === 'br' || kind === 'sr' ? 200 : 220;
  const h = 40;
  return (
    <g transform={`translate(${x}, ${y})`} className={'sys-node ' + kind + ' st-' + state}>
      <rect width={w} height={h} rx="8" />
      <circle cx="13" cy="20" r="4.5" className="state-dot" />
      <text x="26" y="17" className="nm">{label}</text>
      <text x="26" y="31" className="sub">{sub}</text>
      <text x={w - 12} y="17" className="kd" textAnchor="end">{kind.toUpperCase()}</text>
    </g>
  );
}

function TypeCard({ kind, name, count, state, desc, metrics, spark }) {
  return (
    <div className={'sys-type ' + kind + ' st-' + state}>
      <div className="hd">
        <span className={'kd ' + kind}>{kind.toUpperCase()}</span>
        <span className="nm mono">{name}</span>
        <span className="ct mono">×{count}</span>
        <span className={'pill ' + (state === 'ok' ? 'jade' : state === 'fail' ? 'crimson' : 'amber')} style={{ marginLeft: 'auto', height: 18, fontSize: 10 }}>
          <span className="dot" />{state === 'ok' ? 'healthy' : state === 'fail' ? '1 failing' : 'degraded'}
        </span>
      </div>
      <div className="desc">{desc}</div>
      <div className="mtx">
        {metrics.map(([k, v, s]) => (
          <div key={k} className={'m s-' + s}>
            <div className="k mono">{k}</div>
            <div className="v">{v}</div>
          </div>
        ))}
      </div>
      <div className="ft">
        <Spark data={spark} h={28} w={200} color="var(--rust)" />
        <button className="btn ghost" style={{ height: 24, padding: '0 10px', fontSize: 11, marginLeft: 'auto' }}>Open type →</button>
      </div>
    </div>
  );
}

/* ──────────────────────────────────────────────────────────────
   Artboard 2 — Component type page · data-plane-gateway
   ──────────────────────────────────────────────────────────── */
function SystemType() {
  const gws = [
    { id: 'gw-1', host: 'dpg-1.us-east-2.acme', az: 'use2-a', v: '2.18.4', state: 'ok',   rps: 18.4, p99: 4.2, auth: 0.40, deny: 0.32, caches: 13, lag: 0.6 },
    { id: 'gw-2', host: 'dpg-2.us-east-2.acme', az: 'use2-b', v: '2.18.4', state: 'ok',   rps: 17.9, p99: 4.0, auth: 0.36, deny: 0.30, caches: 13, lag: 0.5 },
    { id: 'gw-3', host: 'dpg-3.us-east-2.acme', az: 'use2-c', v: '2.18.4', state: 'warn', rps: 21.2, p99: 11.8, auth: 1.18, deny: 0.41, caches: 13, lag: 4.2 },
    { id: 'gw-4', host: 'dpg-4.us-east-2.acme', az: 'use2-a', v: '2.18.4', state: 'ok',   rps: 16.1, p99: 3.9, auth: 0.41, deny: 0.31, caches: 13, lag: 0.4 },
    { id: 'gw-5', host: 'dpg-5.us-east-2.acme', az: 'use2-b', v: '2.18.3', state: 'ok',   rps: 17.4, p99: 4.4, auth: 0.39, deny: 0.34, caches: 13, lag: 0.5, drift: true },
  ];

  const routes = [
    ['/orgs',        18420, 1.8,  6.4, 0.04],
    ['/topics',     108210, 2.1,  8.2, 0.02],
    ['/jobs',        62180, 2.8, 14.1, 0.12],
    ['/groups',      42090, 1.4,  4.2, 0.01],
    ['/acls',         8240, 1.1,  3.6, 0.00],
    ['/schemas',     14280, 4.2, 12.4, 0.04],
    ['/callbacks',    2148, 6.8, 18.6, 0.21],
    ['/metrics',      4012, 0.9,  2.8, 0.00],
  ];

  const caches = [
    ['organization',         '24,180', '14 MB', 0.4, 0.2],
    ['service_account',      ' 8,920', '6.2 MB', 0.3, 0.2],
    ['user_credential',      '11,402', '8.4 MB', 0.5, 0.3],
    ['iam_group',            ' 2,140', '1.8 MB', 0.3, 0.2],
    ['compiled_acls',        '46,820', '28 MB', 0.6, 0.4],
    ['schema_bindings',      ' 1,948', '0.9 MB', 4.2, 4.0],
    ['topic',                '14,280', '12 MB', 0.4, 0.2],
    ['consumer_group',       ' 4,120', '2.6 MB', 0.4, 0.2],
    ['connector',            '   384', '0.4 MB', 0.5, 0.3],
    ['callback',             ' 6,210', '3.1 MB', 0.4, 0.2],
    ['org_settings',         ' 4,180', '2.0 MB', 0.4, 0.2],
    ['dq_rule',              '   942', '0.5 MB', 0.6, 0.3],
    ['service_account_key',  ' 8,920', '4.4 MB', 0.4, 0.2],
  ];

  return (
    <Shell active="system" breadcrumb={['acme', 'prod', 'us-east-2', 'system', 'data-plane-gateway']}
      title="data-plane-gateway · 5 instances"
      sub="customer-facing kafka wire + REST mgmt plane · 13 entity caches per node · QUIC gossip mesh"
      actions={<>
        <button className="btn ghost">Drain</button>
        <button className="btn ghost">Rolling restart</button>
        <button className="btn primary">Open runbook</button>
      </>}>

      {/* type-wide signal */}
      <div className="panel" style={{ padding: 0, marginBottom: 14 }}>
        <div className="sys-kpis">
          <SysKpi lbl="Instances" val="5" sub="3 az · 1 drift v2.18.3" />
          <SysKpi lbl="Aggregate RPS" val="91.0k" spark={genSeries(36, 90, 8, 0.1, 81)} sub="across all 5 nodes" />
          <SysKpi lbl="REST p99 worst" val="11.8" unit="ms" warn sub="dpg-3 · /jobs route" spark={genSeries(36, 6, 2, 0.18, 89)} />
          <SysKpi lbl="Kafka produce p99" val="4.6" unit="ms" sub="acks=all" spark={genSeries(36, 4.4, 0.8, 0, 91)} />
          <SysKpi lbl="Auth fail / s" val="2.8" sub="jwt 0.6 · oauth 1.9 · sa 0.3" spark={genSeries(36, 2.6, 0.6, 0, 97)} />
          <SysKpi lbl="ACL deny rate" val="0.38" unit="%" sub="prod-normal" />
          <SysKpi lbl="Cache apply lag p99" val="4.2" unit="s" warn sub="schema_bindings on dpg-3" />
          <SysKpi lbl="Gossip RTT p50" val="1.4" unit="ms" sub="mesh fully connected" spark={genSeries(36, 1.4, 0.3, 0, 101)} />
        </div>
      </div>

      <div className="sys-typ-grid">
        {/* instances */}
        <div className="panel sys-instances">
          <div className="panel-h">
            <div>
              <div className="title">Instances</div>
              <div className="sub mono">tap a row to drill into the node</div>
            </div>
            <div style={{ display: 'flex', gap: 6 }}>
              <span className="chip">all</span>
              <span className="chip">degraded · 1</span>
              <span className="chip">version drift · 1</span>
            </div>
          </div>
          <div className="sys-inst-head mono">
            <div>host</div><div>az</div><div>ver</div><div className="r">rps</div><div className="r">p99</div>
            <div className="r">auth fail/s</div><div className="r">deny %</div><div className="r">cache lag</div><div></div>
          </div>
          {gws.map((g) => (
            <div key={g.id} className={'sys-inst ' + g.state + (g.id === 'gw-3' ? ' sel' : '')}>
              <div className="mono nm"><span className={'dot ' + g.state} />{g.host}</div>
              <div className="mono">{g.az}</div>
              <div className="mono">
                {g.v}
                {g.drift && <span className="pill amber" style={{ height: 14, fontSize: 9, marginLeft: 6 }}>drift</span>}
              </div>
              <div className="mono r">{g.rps}k</div>
              <div className={'mono r' + (g.p99 > 8 ? ' warn' : '')}>{g.p99} ms</div>
              <div className={'mono r' + (g.auth > 1 ? ' warn' : '')}>{g.auth}</div>
              <div className="mono r">{g.deny}%</div>
              <div className={'mono r' + (g.lag > 2 ? ' warn' : '')}>{g.lag}s</div>
              <div className="r"><Spark data={genSeries(28, g.rps, 2.5, 0, g.host.length * 7)} h={20} w={70} color={g.state === 'warn' ? 'var(--amber)' : 'var(--rust)'} /></div>
            </div>
          ))}
        </div>

        {/* AZ + version map */}
        <div className="panel sys-azmap">
          <div className="panel-h">
            <div>
              <div className="title">AZ &amp; version map</div>
              <div className="sub mono">5 instances · 3 az · 2 versions</div>
            </div>
          </div>
          <div className="sys-az">
            {['use2-a', 'use2-b', 'use2-c'].map((az) => (
              <div key={az} className="sys-az-col">
                <div className="az-h mono">{az}</div>
                {gws.filter((g) => g.az === az).map((g) => (
                  <div key={g.id} className={'az-pod ' + g.state}>
                    <span className={'dot ' + g.state} />
                    <div className="mono nm">{g.id}</div>
                    <div className="mono dim">{g.v}{g.drift ? ' · drift' : ''}</div>
                  </div>
                ))}
              </div>
            ))}
          </div>
          <div className="sys-az-legend mono">
            <span><span className="dot ok" /> healthy 4</span>
            <span><span className="dot warn" /> degraded 1</span>
            <span><span className="dot fail" /> failing 0</span>
          </div>
        </div>
      </div>

      <div className="sys-typ-grid2">
        {/* route grid */}
        <div className="panel sys-routes">
          <div className="panel-h">
            <div>
              <div className="title">REST surface · per &lt;route, status&gt;</div>
              <div className="sub mono">/orgs /topics /jobs /groups /acls /schemas /callbacks /metrics</div>
            </div>
            <span className="mono dim">last 1h</span>
          </div>
          <div className="rt-head mono">
            <div>route</div><div className="r">rps</div><div className="r">p50 ms</div><div className="r">p99 ms</div><div className="r">err %</div>
            <div>2xx</div><div>3xx</div><div>4xx</div><div>5xx</div>
          </div>
          {routes.map(([r, rps, p50, p99, err]) => (
            <div key={r} className="rt-row">
              <div className="mono nm">{r}</div>
              <div className="mono r">{rps.toLocaleString()}</div>
              <div className="mono r">{p50}</div>
              <div className={'mono r' + (p99 > 12 ? ' warn' : '')}>{p99}</div>
              <div className={'mono r' + (err > 0.1 ? ' warn' : '')}>{err}</div>
              <div className="bar"><i className="ok" style={{ width: (100 - err - (err * 2)) + '%' }} /></div>
              <div className="bar"><i className="info" style={{ width: '1%' }} /></div>
              <div className="bar"><i className="warn" style={{ width: Math.max(err, 0.4) + '%' }} /></div>
              <div className="bar"><i className="bad" style={{ width: (err * 0.3) + '%' }} /></div>
            </div>
          ))}
        </div>

        {/* cache table */}
        <div className="panel sys-caches">
          <div className="panel-h">
            <div>
              <div className="title">Entity caches · 13</div>
              <div className="sub mono">tail-offset lag from broker · warm-on-boot via peer rehydrate</div>
            </div>
            <span className="mono dim">aggregate · 5 nodes</span>
          </div>
          <div className="cc-head mono">
            <div>cache</div><div className="r">entries</div><div className="r">ram</div><div className="r">tail lag</div><div className="r">gossip lag</div>
          </div>
          {caches.map(([k, e, m, tl, gl]) => (
            <div key={k} className="cc-row">
              <div className="mono nm">{k}</div>
              <div className="mono r">{e.trim()}</div>
              <div className="mono r">{m}</div>
              <div className={'mono r' + (tl > 2 ? ' warn' : '')}>{tl}s</div>
              <div className={'mono r' + (gl > 2 ? ' warn' : '')}>{gl}s</div>
            </div>
          ))}
        </div>
      </div>
    </Shell>
  );
}

/* ──────────────────────────────────────────────────────────────
   Artboard 3 — Single node detail · dpg-3
   ──────────────────────────────────────────────────────────── */
function SystemNode() {
  return (
    <Shell active="system" breadcrumb={['acme', 'prod', 'us-east-2', 'system', 'data-plane-gateway', 'dpg-3']}
      title="dpg-3.us-east-2.acme"
      sub="data-plane-gateway · use2-c · v2.18.4 · uptime 17d 4h · sprint-96"
      actions={<>
        <button className="btn ghost">SSH</button>
        <button className="btn ghost">Drain</button>
        <button className="btn ghost">Profile (1m)</button>
        <button className="btn primary">Restart</button>
      </>}>

      {/* hero */}
      <div className="panel sys-node-hero">
        <div className="l">
          <div className="hero-st">
            <span className="pill amber" style={{ height: 22 }}><span className="dot" />degraded · /jobs p99 over SLO</span>
            <span className="mono dim">since 14:13:02 · 8m</span>
          </div>
          <div className="hero-meta mono">
            <div><span className="k">host</span><span>dpg-3.us-east-2.acme</span></div>
            <div><span className="k">az</span><span>use2-c</span></div>
            <div><span className="k">version</span><span>2.18.4</span></div>
            <div><span className="k">boot</span><span>peer-rehydrate · ✓ accept-gate 1.4s</span></div>
            <div><span className="k">peers</span><span>4 / 4 reachable</span></div>
            <div><span className="k">cpu</span><span>62% · 8 cores</span></div>
            <div><span className="k">rss</span><span>2.4 GB / 6 GB</span></div>
            <div><span className="k">tcp</span><span>4,128 conn</span></div>
          </div>
        </div>
        <div className="r">
          <div className="rps">
            <div className="big">21.2<span className="u">k</span></div>
            <div className="dim mono">requests / s · all surfaces</div>
            <Spark data={genSeries(60, 19, 2.5, 0.06, 109)} h={48} w={320} color="var(--rust)" />
          </div>
        </div>
      </div>

      {/* signal grid */}
      <div className="sys-node-grid">
        {/* REST p99 chart with SLO line */}
        <div className="panel sys-chart sp2">
          <div className="panel-h">
            <div><div className="title">REST p99 · 1h · by route</div><div className="sub mono">SLO 8ms (dashed). /jobs and /callbacks above.</div></div>
            <div className="ck mono">
              <span><i style={{ background: 'var(--rust)' }} />/jobs</span>
              <span><i style={{ background: 'var(--ember)' }} />/callbacks</span>
              <span><i style={{ background: 'var(--ice)' }} />/topics</span>
              <span><i style={{ background: 'var(--violet)' }} />/schemas</span>
            </div>
          </div>
          <ChartLines
            series={[
              { color: 'var(--rust)',   data: genSeries(80, 8, 1.5, 0.05, 121) },
              { color: 'var(--ember)',  data: genSeries(80, 9, 2, 0.04, 127) },
              { color: 'var(--ice)',    data: genSeries(80, 4.5, 0.8, 0, 131) },
              { color: 'var(--violet)', data: genSeries(80, 5.5, 1, 0, 137) },
            ]}
            slo={8} h={200} />
        </div>

        {/* Auth fail by method */}
        <div className="panel sys-chart">
          <div className="panel-h">
            <div><div className="title">Auth failures / s</div><div className="sub mono">jwt · oauth · sa · refresh</div></div>
          </div>
          <ChartStack
            series={[
              { label: 'jwt',     color: 'var(--rust)',   data: genSeries(80, 0.4, 0.15, 0, 141) },
              { label: 'oauth',   color: 'var(--ember)',  data: genSeries(80, 0.55, 0.18, 0.005, 149) },
              { label: 'sa',      color: 'var(--ice)',    data: genSeries(80, 0.2, 0.08, 0, 151) },
              { label: 'refresh', color: 'var(--violet)', data: genSeries(80, 0.08, 0.04, 0, 157) },
            ]}
            h={200} />
        </div>

        {/* ACL deny / status */}
        <div className="panel sys-chart">
          <div className="panel-h">
            <div><div className="title">Status mix</div><div className="sub mono">401 · 403 · 404 · 409 per minute</div></div>
          </div>
          <ChartStack
            series={[
              { label: '401', color: 'var(--amber)',  data: genSeries(60, 14, 4, 0, 161) },
              { label: '403', color: 'var(--ember)',  data: genSeries(60, 7,  2, 0, 167) },
              { label: '404', color: 'var(--ice)',    data: genSeries(60, 22, 6, 0, 173) },
              { label: '409', color: 'var(--violet)', data: genSeries(60, 3,  1, 0, 179) },
            ]}
            h={200} bar />
        </div>

        {/* Boot timing waterfall */}
        <div className="panel sys-boot sp2">
          <div className="panel-h">
            <div><div className="title">Last boot · waterfall</div><div className="sub mono">peer-readiness → fence-waits → COMPILED_ACLS → accept-gate (sprint-96)</div></div>
            <span className="mono dim">total 1.42s</span>
          </div>
          <div className="boot">
            {[
              ['process init',                  0,    140, 'ok'],
              ['peer-readiness ping',         140,    180, 'ok'],
              ['fence · service_account',     180,    340, 'ok'],
              ['fence · iam_group',           240,    410, 'ok'],
              ['fence · user_credential',     300,    490, 'ok'],
              ['boot_rehydrate_from_peer',    340,    980, 'rehydrate'],
              ['  chunks · 18 / 18',          410,    920, 'rehydrate'],
              ['compile COMPILED_ACLS',       940,   1180, 'ok'],
              ['fence · group',              1010,   1190, 'ok'],
              ['warm ancillary caches',      1100,   1330, 'ok'],
              ['accept-gate ready',          1330,   1420, 'ok'],
            ].map(([nm, s, e, k]) => (
              <div key={nm} className="boot-row">
                <div className="lb mono">{nm}</div>
                <div className="trk"><div className={'bar ' + k} style={{ left: (s / 1420 * 100) + '%', width: ((e - s) / 1420 * 100) + '%' }} /></div>
                <div className="ms mono">{e - s} ms</div>
              </div>
            ))}
            <div className="boot-foot mono">
              <span>rehydrate · 18 chunks · 14.2 MB · 0 fallback-to-broker-tail</span>
              <span className="dim">herd-protected · 2 concurrent peers throttled</span>
            </div>
          </div>
        </div>

        {/* Cache lag heatmap */}
        <div className="panel sys-heat">
          <div className="panel-h">
            <div><div className="title">Entity cache · tail-offset lag (s)</div><div className="sub mono">13 caches × 30 min</div></div>
          </div>
          <Heatmap
            rows={['organization','service_account','user_credential','iam_group','compiled_acls','schema_bindings','topic','consumer_group','connector','callback','org_settings','dq_rule','sa_key']}
            cols={30} seed={181}
            hot={{ 5: [12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29] }} />
        </div>

        {/* Live spans */}
        <div className="panel sys-spans">
          <div className="panel-h">
            <div><div className="title">OTLP spans · live</div><div className="sub mono">rafka.gateway.* on this node</div></div>
            <span className="pill jade" style={{ height: 20 }}><span className="dot" />nominal</span>
          </div>
          {[
            ['rafka.gateway.rest.request',        '21.2k /s', 'green', genSeries(40, 20, 2, 0.06, 191)],
            ['rafka.gateway.kafka.produce',       '142k /s',  'green', genSeries(40, 140, 12, 0, 193)],
            ['rafka.gateway.kafka.fetch',         '98.4k /s', 'green', genSeries(40, 98, 8, 0, 197)],
            ['rafka.gateway.acl.compile',         '0.34 /s',  'green', genSeries(40, 0.3, 0.05, 0, 199)],
            ['rafka.gateway.cache.tail.apply',    '12.4 /s',  'green', genSeries(40, 12, 2, 0, 211)],
            ['rafka.gateway.cache.gossip.apply',  '8.1 /s',   'green', genSeries(40, 8, 1.5, 0, 223)],
            ['rafka.gateway.boot.peer_rehydrate', '0 /s',     'mute',  Array(40).fill(0)],
            ['rafka.gateway.auth.jwt',            '6.2k /s',  'green', genSeries(40, 6.2, 0.5, 0, 227)],
          ].map((r, i) => (
            <div key={i} className="otlp-row">
              <span className={'dot ' + r[2]} />
              <span className="mono nm">{r[0]}</span>
              <Spark data={r[3]} h={18} w={80} color={r[2] === 'green' ? 'var(--jade)' : 'var(--ink-3)'} />
              <span className="mono v">{r[1]}</span>
            </div>
          ))}
        </div>

        {/* QUIC + connection */}
        <div className="panel sys-conn">
          <div className="panel-h"><div><div className="title">Connections</div><div className="sub mono">tcp · quic · gossip mesh</div></div></div>
          <div className="conn-grid">
            <div className="c"><div className="k mono">tcp open</div><div className="v">4,128</div></div>
            <div className="c"><div className="k mono">quic open</div><div className="v">1,842</div></div>
            <div className="c"><div className="k mono">tcp hs err / s</div><div className="v">0.02</div></div>
            <div className="c"><div className="k mono">quic hs err / s</div><div className="v">0.00</div></div>
            <div className="c"><div className="k mono">peer count</div><div className="v">4 / 4</div></div>
            <div className="c"><div className="k mono">gossip RTT p50</div><div className="v">1.4 ms</div></div>
            <div className="c"><div className="k mono">undelivered backlog</div><div className="v">0</div></div>
            <div className="c warn"><div className="k mono">reconnect / m</div><div className="v">3</div></div>
          </div>
        </div>
      </div>

      {/* runbook + recent events */}
      <div className="sys-node-grid2">
        <div className="panel sys-runbook">
          <div className="panel-h">
            <div><div className="title">Runbook · cache apply lag</div><div className="sub mono">matched on `schema_bindings tail lag &gt; 2s`</div></div>
            <span className="pill amber" style={{ height: 20 }}><span className="dot" />triggered 8m ago</span>
          </div>
          <ol className="rb">
            <li>Check `rafka.gateway.cache.tail.apply` cadence on this node — should be ≥10/s. <span className="ok mono">✓ 12.4 /s</span></li>
            <li>Inspect broker partition for `schema_bindings` topic — confirm leader healthy. <span className="ok mono">✓ br-2 leader</span></li>
            <li>Confirm gossip-apply lag is consistent with tail lag — divergence indicates herd. <span className="ok mono">✓ 4.0s vs 4.2s</span></li>
            <li>If lag persists &gt; 30s: trigger peer rehydrate. <span className="dim mono">— skip</span></li>
            <li>Drain &amp; restart this node if condition holds &gt; 5m. <span className="warn mono">→ candidate</span></li>
          </ol>
          <div className="rb-actions">
            <button className="btn ghost" style={{ height: 26 }}>Force tail catch-up</button>
            <button className="btn ghost" style={{ height: 26 }}>Trigger peer rehydrate</button>
            <button className="btn primary" style={{ height: 26 }}>Drain</button>
          </div>
        </div>

        <div className="panel sys-events">
          <div className="panel-h"><div><div className="title">Recent events</div><div className="sub mono">node-local · last 30m</div></div></div>
          {[
            ['14:21:08', 'cache.tail.lag.warn', 'schema_bindings · 4.2s'],
            ['14:18:42', 'gossip.apply.lag',    'topic · 2.1s'],
            ['14:13:02', 'route.p99.over_slo',  '/jobs · 11.8ms · SLO 8ms'],
            ['14:09:18', 'quic.peer.reconnect', 'peer dpg-1 · 1.6s outage'],
            ['13:54:11', 'rest.4xx.spike',      '/topics 404 · 0.4 → 1.1%'],
            ['13:48:02', 'acl.compile',         'org_18a4 · 220ms'],
            ['13:32:14', 'cache.tail.lag.ok',   'all caches < 1s'],
            ['13:14:01', 'boot.complete',       'accept-gate · 1.42s'],
          ].map(([t, k, m]) => (
            <div key={t} className="ev">
              <span className="mono t">{t}</span>
              <span className={'mono k ' + (k.includes('warn') || k.includes('spike') || k.includes('over') ? 'w' : 'g')}>{k}</span>
              <span className="mono m">{m}</span>
            </div>
          ))}
        </div>
      </div>
    </Shell>
  );
}

/* ──────────────────────────────────────────────────────────────
   helper charts
   ──────────────────────────────────────────────────────────── */
function ChartLines({ series, slo, h = 200 }) {
  const allMax = Math.max(...series.flatMap(s => s.data));
  const max = Math.max(allMax, slo ? slo * 1.4 : 0);
  const n = series[0].data.length;
  return (
    <div style={{ padding: '8px 16px 14px' }}>
      <svg viewBox={`0 0 ${n} ${h}`} preserveAspectRatio="none" style={{ width: '100%', height: h }}>
        {[0, 0.25, 0.5, 0.75, 1].map(p => (
          <line key={p} x1="0" x2={n} y1={h - p * h * 0.92 - 8} y2={h - p * h * 0.92 - 8} stroke="var(--line-1)" strokeWidth="0.4" />
        ))}
        {slo && (
          <line x1="0" x2={n} y1={h - (slo / max) * h * 0.92 - 8} y2={h - (slo / max) * h * 0.92 - 8}
            stroke="var(--amber)" strokeWidth="0.8" strokeDasharray="2 2" />
        )}
        {series.map((s, i) => {
          const d = s.data.map((v, k) => `${k ? 'L' : 'M'}${k},${h - (v / max) * h * 0.92 - 8}`).join(' ');
          return <path key={i} d={d} fill="none" stroke={s.color} strokeWidth="1.2" vectorEffect="non-scaling-stroke" />;
        })}
      </svg>
    </div>
  );
}

function ChartStack({ series, h = 200, bar }) {
  const n = series[0].data.length;
  const totals = Array.from({ length: n }, (_, i) => series.reduce((a, s) => a + s.data[i], 0));
  const max = Math.max(...totals);
  return (
    <div style={{ padding: '8px 16px 14px' }}>
      <svg viewBox={`0 0 ${n} ${h}`} preserveAspectRatio="none" style={{ width: '100%', height: h }}>
        {[0, 0.25, 0.5, 0.75, 1].map(p => (
          <line key={p} x1="0" x2={n} y1={h - p * h * 0.92 - 8} y2={h - p * h * 0.92 - 8} stroke="var(--line-1)" strokeWidth="0.4" />
        ))}
        {Array.from({ length: n }).map((_, k) => {
          let yAcc = h - 8;
          return series.map((s, i) => {
            const hh = (s.data[k] / max) * h * 0.92;
            yAcc -= hh;
            const y = yAcc;
            return bar
              ? <rect key={i + '_' + k} x={k} y={y} width={1} height={hh} fill={s.color} opacity="0.9" />
              : <rect key={i + '_' + k} x={k} y={y} width={1.02} height={hh + 0.5} fill={s.color} opacity="0.85" />;
          });
        }).flat()}
      </svg>
    </div>
  );
}

function Heatmap({ rows, cols, seed, hot = {} }) {
  let s = seed;
  return (
    <div className="heat" style={{ padding: '8px 16px 14px' }}>
      {rows.map((r, ri) => (
        <div key={r} className="heat-row">
          <div className="lbl mono">{r}</div>
          <div className="cells">
            {Array.from({ length: cols }).map((_, ci) => {
              s = (s * 9301 + 49297) % 233280;
              let v = (s / 233280) * 0.4;
              if (hot[ri] && hot[ri].includes(ci)) v = 0.7 + ((s / 233280) * 0.3);
              return <div key={ci} className="cell" style={{ background: `oklch(0.72 ${0.05 + v * 0.16} ${v > 0.5 ? 70 : 50} / ${0.15 + v})` }} />;
            })}
          </div>
        </div>
      ))}
      <div className="heat-foot mono">
        <span>now ←</span>
        <span style={{ flex: 1 }} />
        <span>30 min ago →</span>
      </div>
    </div>
  );
}

/* ──────────────────────────────────────────────────────────────
   Artboard 4 — Single node detail · broker io-pump (br-5)
   ──────────────────────────────────────────────────────────── */
function SystemNodeBroker() {
  const partitions = [
    ['orders.v2:0',          'leader', '3/3', '142.4k', '38ms', 'ok'],
    ['orders.v2:1',          'leader', '3/3', '138.1k', '41ms', 'ok'],
    ['payments.events:14',   'leader', '2/3', '24.1k',  '184ms','warn'],
    ['payments.events:7',    'follow', '3/3', '23.8k',  '42ms', 'ok'],
    ['clickstream.raw:22',   'leader', '3/3', '412k',   '28ms', 'ok'],
    ['clickstream.parsed:9', 'follow', '3/3', '402k',   '31ms', 'ok'],
    ['jobs-active:0',        'leader', '3/3', '0.4k',   '12ms', 'ok'],
    ['compiled_acls:0',      'leader', '3/3', '0.1k',   '14ms', 'ok'],
    ['schema_bindings:0',    'follow', '3/3', '0.0k',   '18ms', 'ok'],
  ];
  return (
    <Shell active="system" breadcrumb={['acme','prod','us-east-2','system','broker io-pump','br-5']}
      title="br-5.us-east-2.acme"
      sub="broker io-pump · use2-c · v2.18.4 · uptime 9d 11h · rafka-server (flafka heritage)"
      actions={<>
        <button className="btn ghost">Decommission check</button>
        <button className="btn ghost">Reassign leadership</button>
        <button className="btn primary">Investigate ISR</button>
      </>}>

      <div className="panel sys-node-hero">
        <div className="l">
          <div className="hero-st">
            <span className="pill amber" style={{height:22}}><span className="dot" />degraded · payments.events:14 ISR 2/3</span>
            <span className="mono dim">since 14:05:14 · 17m</span>
          </div>
          <div className="hero-meta mono">
            <div><span className="k">host</span><span>br-5.us-east-2.acme</span></div>
            <div><span className="k">az</span><span>use2-c</span></div>
            <div><span className="k">version</span><span>2.18.4</span></div>
            <div><span className="k">disk</span><span>482 GB / 1 TB · 47%</span></div>
            <div><span className="k">leaders</span><span>26 partitions</span></div>
            <div><span className="k">followers</span><span>54 partitions</span></div>
            <div><span className="k">fsync p99</span><span>2.1 ms</span></div>
            <div><span className="k">tiered</span><span>14.2 TB · s3 cold</span></div>
          </div>
        </div>
        <div className="r">
          <div className="rps">
            <div className="big">142<span className="u">k</span></div>
            <div className="dim mono">records appended / s · WAL</div>
            <Spark data={genSeries(60,140,12,0,229)} h={48} w={320} color="var(--jade)" />
          </div>
        </div>
      </div>

      <div className="sys-node-grid">
        <div className="panel sys-chart sp2">
          <div className="panel-h">
            <div><div className="title">WAL segment-append · latency · 1h</div><div className="sub mono">rafka.broker.segment.append.via-wal-write</div></div>
            <div className="ck mono">
              <span><i style={{background:'var(--rust)'}}/>p50</span>
              <span><i style={{background:'var(--ember)'}}/>p95</span>
              <span><i style={{background:'var(--amber)'}}/>p99</span>
            </div>
          </div>
          <ChartLines
            series={[
              { color:'var(--rust)',  data:genSeries(80, 0.8, 0.2, 0, 241) },
              { color:'var(--ember)', data:genSeries(80, 1.4, 0.3, 0, 251) },
              { color:'var(--amber)', data:genSeries(80, 1.9, 0.4, 0, 257) },
            ]} h={200} />
        </div>

        <div className="panel sys-chart">
          <div className="panel-h"><div><div className="title">Replication lag</div><div className="sub mono">leader → follower · bytes/s</div></div></div>
          <ChartLines
            series={[
              { color:'var(--jade)',  data:genSeries(80, 18, 4, 0, 263) },
              { color:'var(--amber)', data:genSeries(80, 38, 14, 0.4, 269) },
            ]} h={200} />
        </div>

        <div className="panel sys-chart">
          <div className="panel-h"><div><div className="title">Fsync window close · cadence</div><div className="sub mono">24/s · 5.8k records/batch · acks=0 grouping</div></div></div>
          <ChartLines
            series={[{ color:'var(--ice)', data:genSeries(80, 24, 2, 0, 271) }]} h={200} />
        </div>

        <div className="panel sys-chart">
          <div className="panel-h"><div><div className="title">Tiered storage</div><div className="sub mono">offload rate · replay latency · s3 errors</div></div></div>
          <ChartStack
            series={[
              { label:'offload', color:'var(--rust)',  data:genSeries(60, 22, 4, 0, 277) },
              { label:'replay',  color:'var(--ember)', data:genSeries(60, 8, 2, 0, 283) },
              { label:'errors',  color:'var(--crimson)', data:Array(60).fill(0).map((_,i)=>i===34?2:0) },
            ]} h={200} bar />
        </div>

        <div className="panel sys-chart">
          <div className="panel-h"><div><div className="title">Compaction lag</div><div className="sub mono">head − compacted offset · top compacted topics</div></div></div>
          <div style={{padding:'12px 16px',display:'flex',flexDirection:'column',gap:8}}>
            {[['jobs-active', 0.18],['compiled_acls', 0.22],['schema_bindings', 0.08],['org_settings', 0.31],['dq_rule', 0.14]].map(([t,v])=>(
              <div key={t} style={{display:'grid',gridTemplateColumns:'160px 1fr 60px',gap:10,alignItems:'center',fontSize:11.5}}>
                <span className="mono" style={{color:'var(--ink-1)'}}>{t}</span>
                <div style={{height:8,background:'var(--bg-0)',borderRadius:2,overflow:'hidden'}}>
                  <div style={{width:(v*100)+'%',height:'100%',background:v>0.3?'var(--amber)':'var(--jade)'}}/>
                </div>
                <span className="mono dim r" style={{textAlign:'right'}}>{(v*100).toFixed(0)}%</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="sys-node-grid2">
        <div className="panel sys-routes">
          <div className="panel-h"><div><div className="title">Partitions on this broker · 80</div><div className="sub mono">leadership · ISR · throughput · repl lag</div></div></div>
          <div className="rt-head mono" style={{gridTemplateColumns:'1.4fr 0.7fr 0.6fr 0.8fr 0.7fr 0.5fr'}}>
            <div>partition</div><div>role</div><div>isr</div><div className="r">rate</div><div className="r">repl lag p99</div><div></div>
          </div>
          {partitions.map(p => (
            <div key={p[0]} className="rt-row" style={{gridTemplateColumns:'1.4fr 0.7fr 0.6fr 0.8fr 0.7fr 0.5fr'}}>
              <div className="mono nm">{p[0]}</div>
              <div className="mono">{p[1]}</div>
              <div className={'mono ' + (p[5]==='warn'?'warn':'')}>{p[2]}</div>
              <div className="mono r">{p[3]}/s</div>
              <div className={'mono r ' + (p[5]==='warn'?'warn':'')}>{p[4]}</div>
              <div className="r"><span className={'dot ' + p[5]} style={{display:'inline-block',width:8,height:8,borderRadius:'50%',background:p[5]==='warn'?'var(--amber)':'var(--jade)'}}/></div>
            </div>
          ))}
        </div>

        <div className="panel sys-spans">
          <div className="panel-h">
            <div><div className="title">OTLP spans · this broker</div><div className="sub mono">rafka.broker.*</div></div>
            <span className="pill jade" style={{height:20}}><span className="dot"/>nominal</span>
          </div>
          {[
            ['rafka.broker.segment.append.via-wal-write','142k /s','green', genSeries(40,140,12,0,291)],
            ['rafka.broker.fsync.window.close',          '24 /s','green',   genSeries(40,24,2,0,293)],
            ['rafka.broker.compaction.dedup',            '142 /s','green',  genSeries(40,140,16,0,299)],
            ['rafka.broker.tiered.offload',              '22/s','green',    genSeries(40,22,3,0,301)],
            ['rafka.broker.tiered.replay',               '8/s','green',     genSeries(40,8,1.5,0,307)],
            ['rafka.broker.repl.send',                   '94k/s','green',   genSeries(40,94,8,0,311)],
            ['rafka.broker.repl.recv',                   '88k/s','amber',   genSeries(40,86,12,0.2,313)],
            ['rafka.broker.partition.leader.elect',      '0 /s','mute',     Array(40).fill(0)],
          ].map((r,i)=>(
            <div key={i} className="otlp-row">
              <span className={'dot ' + r[2]}/>
              <span className="mono nm">{r[0]}</span>
              <Spark data={r[3]} h={18} w={80} color={r[2]==='green'?'var(--jade)':r[2]==='amber'?'var(--amber)':'var(--ink-3)'}/>
              <span className="mono v">{r[1]}</span>
            </div>
          ))}
        </div>
      </div>
    </Shell>
  );
}

/* ──────────────────────────────────────────────────────────────
   Artboard 5 — Single node detail · compute-gateway (cpg-2, failing)
   ──────────────────────────────────────────────────────────── */
function SystemNodeCompute() {
  const jobs = [
    ['CascadeTombstoneFanout',  'running',   '2m 14s', 0.62, 'org_4a8b'],
    ['ReprovisionPartitions',   'running',   '0m 41s', 0.84, 'org_2188'],
    ['ReprovisionReplicationFactor','queued','—',      0.00, 'org_1119'],
    ['DQ.rule_apply · cust_seg','running',   '0m 12s', 0.18, 'org_3304'],
    ['RSQL.query · orders_sum', 'running',   '0m 04s', 0.08, 'org_0421'],
    ['Connector.webhook · stripe','running', '14m 02s',0.42, 'org_4a8b'],
    ['org_reaper · sweep',      'running',   '0m 09s', 0.04, '—'],
    ['telemetry_federation',    'running',   '1m 32s', 0.22, '—'],
  ];
  return (
    <Shell active="system" breadcrumb={['acme','prod','us-east-2','system','compute-gateway','cpg-2']}
      title="cpg-2.us-east-2.acme"
      sub="compute-gateway · use2-b · v2.18.4 · sa_compute_gateway (sprint-95) · WASM runtime"
      actions={<>
        <button className="btn ghost">Pause dispatcher</button>
        <button className="btn ghost">Drain jobs</button>
        <button className="btn primary">Capture profile</button>
      </>}>

      <div className="panel sys-node-hero">
        <div className="l">
          <div className="hero-st">
            <span className="pill crimson" style={{height:22}}><span className="dot"/>failing · WASM fuel saturation 99% · 90s sustained</span>
            <span className="mono dim">since 14:19:42 · 4m · acked by m.ortiz</span>
          </div>
          <div className="hero-meta mono">
            <div><span className="k">host</span><span>cpg-2.us-east-2.acme</span></div>
            <div><span className="k">az</span><span>use2-b</span></div>
            <div><span className="k">version</span><span>2.18.4</span></div>
            <div><span className="k">platform SA</span><span>sa_compute_gateway</span></div>
            <div><span className="k">active jobs</span><span>138</span></div>
            <div><span className="k">queued</span><span>42</span></div>
            <div><span className="k">silent_double_claim</span><span style={{color:'var(--jade)'}}>0 · sentinel green</span></div>
            <div><span className="k">heartbeat drift</span><span style={{color:'var(--amber)'}}>+1.8s vs 30s</span></div>
          </div>
        </div>
        <div className="r">
          <div className="rps">
            <div className="big">99<span className="u">%</span></div>
            <div className="dim mono">cluster fuel consumption · 30s rolling</div>
            <Spark data={genSeries(60,72,8,0.6,319)} h={48} w={320} color="var(--crimson)"/>
          </div>
        </div>
      </div>

      <div className="sys-node-grid">
        <div className="panel sys-chart sp2">
          <div className="panel-h">
            <div><div className="title">Job state transition · latency</div><div className="sub mono">enqueue → claim → first heartbeat → terminal</div></div>
            <div className="ck mono">
              <span><i style={{background:'var(--ice)'}}/>enqueue→claim</span>
              <span><i style={{background:'var(--rust)'}}/>claim→hb</span>
              <span><i style={{background:'var(--amber)'}}/>hb→terminal</span>
            </div>
          </div>
          <ChartLines
            series={[
              { color:'var(--ice)',   data:genSeries(80, 84, 14, 0, 331) },
              { color:'var(--rust)',  data:genSeries(80, 142, 24, 0.6, 337) },
              { color:'var(--amber)', data:genSeries(80, 380, 80, 1.4, 347) },
            ]} h={200} />
        </div>

        <div className="panel sys-chart">
          <div className="panel-h"><div><div className="title">WASM fuel · per JobKind</div><div className="sub mono">i13 · top 4 by consumption</div></div></div>
          <div style={{padding:'14px 16px',display:'flex',flexDirection:'column',gap:10}}>
            {[['CascadeTombstoneFanout', 0.99,'fail'],['ReprovisionPartitions', 0.84,'warn'],['ReprovisionReplicationFactor', 0.42,'ok'],['DQ.rule_apply', 0.18,'ok']].map(([k,v,s])=>(
              <div key={k} style={{display:'grid',gridTemplateColumns:'180px 1fr 50px',gap:10,alignItems:'center',fontSize:11.5}}>
                <span className="mono" style={{color:'var(--ink-1)'}}>{k}</span>
                <div style={{height:10,background:'var(--bg-0)',borderRadius:3,overflow:'hidden'}}>
                  <div style={{width:(v*100)+'%',height:'100%',background:s==='fail'?'var(--crimson)':s==='warn'?'var(--amber)':'var(--jade)'}}/>
                </div>
                <span className="mono r" style={{textAlign:'right',color:s==='fail'?'var(--crimson)':s==='warn'?'var(--amber)':'var(--ink-2)'}}>{Math.round(v*100)}%</span>
              </div>
            ))}
          </div>
        </div>

        <div className="panel sys-chart">
          <div className="panel-h"><div><div className="title">Dispatcher state machine</div><div className="sub mono">DispatcherEvent broadcast bus · subscriber depth</div></div></div>
          <ChartLines
            series={[
              { color:'var(--rust)',  data:genSeries(80, 14, 4, 0.2, 353) },
              { color:'var(--ember)', data:genSeries(80, 28, 6, 0.6, 359) },
            ]} h={200} />
        </div>

        <div className="panel sys-chart">
          <div className="panel-h"><div><div className="title">Stale-detection &amp; reclaim</div><div className="sub mono">D4 max-reclaims=5 · cluster near cap</div></div></div>
          <ChartStack
            series={[
              { label:'reclaim=1', color:'var(--jade)',   data:genSeries(60, 4, 1, 0, 367) },
              { label:'reclaim=2', color:'var(--ice)',    data:genSeries(60, 2, 0.5, 0, 373) },
              { label:'reclaim=3', color:'var(--amber)',  data:genSeries(60, 1, 0.4, 0.02, 379) },
              { label:'reclaim=4', color:'var(--ember)',  data:genSeries(60, 0.4, 0.2, 0.02, 383) },
              { label:'reclaim=5', color:'var(--crimson)',data:genSeries(60, 0.1, 0.05, 0.005, 389) },
            ]} h={200} bar />
        </div>

        <div className="panel sys-chart">
          <div className="panel-h"><div><div className="title">Wake-scheduler timers</div><div className="sub mono">outstanding · cancel-and-reschedule rate</div></div></div>
          <ChartLines
            series={[
              { color:'var(--ice)',  data:genSeries(80, 240, 30, 0, 397) },
              { color:'var(--rust)', data:genSeries(80, 18, 4, 0.2, 401) },
            ]} h={200} />
        </div>
      </div>

      <div className="sys-node-grid2">
        <div className="panel sys-routes">
          <div className="panel-h"><div><div className="title">Active jobs · 8 of 138</div><div className="sub mono">kind · state · runtime · fuel · org</div></div></div>
          <div className="rt-head mono" style={{gridTemplateColumns:'1.6fr 0.7fr 0.7fr 1fr 0.8fr'}}>
            <div>job kind</div><div>state</div><div className="r">runtime</div><div>fuel</div><div>org</div>
          </div>
          {jobs.map(j => (
            <div key={j[0]} className="rt-row" style={{gridTemplateColumns:'1.6fr 0.7fr 0.7fr 1fr 0.8fr'}}>
              <div className="mono nm">{j[0]}</div>
              <div className="mono">{j[1]}</div>
              <div className="mono r">{j[2]}</div>
              <div>
                <div style={{height:6,background:'var(--bg-0)',borderRadius:2,overflow:'hidden'}}>
                  <div style={{width:(j[3]*100)+'%',height:'100%',background:j[3]>0.8?'var(--crimson)':j[3]>0.5?'var(--amber)':'var(--jade)'}}/>
                </div>
              </div>
              <div className="mono">{j[4]}</div>
            </div>
          ))}
        </div>

        <div className="panel sys-spans">
          <div className="panel-h">
            <div><div className="title">OTLP spans · this node</div><div className="sub mono">rafka.compute.*</div></div>
            <span className="pill amber" style={{height:20}}><span className="dot"/>elevated</span>
          </div>
          {[
            ['rafka.compute.job.claim.cas',        '8.1 /s','amber', genSeries(40,6,1.5,0.06,409)],
            ['rafka.compute.silent.double.claim',  '0 /s','green',   Array(40).fill(0)],
            ['rafka.compute.heartbeat.tick',       '4.6 /s','amber', genSeries(40,4.6,0.4,0,419)],
            ['rafka.compute.wasm.fuel.spent',      '99% sat.','amber',genSeries(40,72,8,0.6,421)],
            ['rafka.compute.rsql.query',           '14 /s','green',  genSeries(40,14,2,0,431)],
            ['rafka.compute.connector.webhook',    '142 /s','green', genSeries(40,140,12,0,433)],
            ['rafka.compute.dq.rule.apply',        '4.2k /s','green',genSeries(40,4200,180,0,439)],
            ['rafka.compute.tailer.org_reaper',    '0.4 /s','green', genSeries(40,0.4,0.05,0,443)],
            ['rafka.compute.mesh.event.bus',       '128 /s','amber', genSeries(40,118,18,0.4,449)],
          ].map((r,i)=>(
            <div key={i} className="otlp-row">
              <span className={'dot ' + r[2]}/>
              <span className="mono nm">{r[0]}</span>
              <Spark data={r[3]} h={18} w={80} color={r[2]==='green'?'var(--jade)':r[2]==='amber'?'var(--amber)':'var(--ink-3)'}/>
              <span className="mono v">{r[1]}</span>
            </div>
          ))}
        </div>
      </div>
    </Shell>
  );
}

Object.assign(window, { SystemMesh, SystemType, SystemNode, SystemNodeBroker, SystemNodeCompute });
