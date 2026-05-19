// Rafka — Cluster Overview · variant A (data-dense)
// 1440x900 artboard. Designed for the "first thing you see" screen.

function MiniSpark({ pts, cls = 'sk r' }) {
  const w = 56, h = 22;
  const max = Math.max(...pts), min = Math.min(...pts);
  const norm = (v) => h - 2 - ((v - min) / (max - min || 1)) * (h - 4);
  const step = w / (pts.length - 1);
  const d = pts.map((v, i) => `${i ? 'L' : 'M'}${(i * step).toFixed(1)},${norm(v).toFixed(1)}`).join(' ');
  return (
    <svg className={"sk " + cls} width={w} height={h} viewBox={`0 0 ${w} ${h}`}>
      <path className="l" d={d} />
    </svg>
  );
}

function ThroughputChart() {
  // sample 60 points across 220×180 plot
  const W = 760, H = 180, PAD_L = 36, PAD_R = 12, PAD_T = 12, PAD_B = 26;
  const innerW = W - PAD_L - PAD_R, innerH = H - PAD_T - PAD_B;
  const produce = [620, 700, 680, 740, 820, 880, 910, 870, 920, 980, 1040, 1100, 1080, 1120, 1180, 1240, 1280, 1320, 1300, 1360, 1410, 1380, 1420, 1480, 1460, 1430, 1410, 1450, 1490, 1500, 1480, 1440, 1420, 1440, 1470, 1500, 1530, 1540, 1500, 1480, 1500, 1530, 1560, 1540, 1510, 1480, 1450, 1430, 1410, 1430, 1460, 1480, 1500, 1490, 1470, 1450, 1430, 1420, 1410, 1420];
  const consume = produce.map((v, i) => Math.max(580, v - 18 - Math.sin(i / 8) * 50));
  const max = 1700, min = 500;
  const xs = (i) => PAD_L + (i / (produce.length - 1)) * innerW;
  const ys = (v) => PAD_T + innerH - ((v - min) / (max - min)) * innerH;

  const path = (arr) => arr.map((v, i) => `${i ? 'L' : 'M'}${xs(i).toFixed(1)},${ys(v).toFixed(1)}`).join(' ');
  const area = `${path(produce)} L${xs(produce.length - 1)},${PAD_T + innerH} L${PAD_L},${PAD_T + innerH} Z`;

  const yTicks = [500, 1000, 1500];
  const xLabels = ['−60m', '−45m', '−30m', '−15m', 'now'];

  return (
    <div>
      <div className="bigchart" style={{ height: 200 }}>
        <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
          <g className="gy">
            {yTicks.map((t) => (
              <g key={t}>
                <line x1={PAD_L} x2={W - PAD_R} y1={ys(t)} y2={ys(t)} />
                <text x={PAD_L - 6} y={ys(t) + 3} textAnchor="end">{(t / 1000).toFixed(1)}k</text>
              </g>
            ))}
          </g>
          <g className="gx">
            {xLabels.map((l, i) => (
              <g key={l}>
                <line x1={PAD_L + (i / (xLabels.length - 1)) * innerW} x2={PAD_L + (i / (xLabels.length - 1)) * innerW} y1={PAD_T + innerH} y2={PAD_T + innerH + 4} />
                <text x={PAD_L + (i / (xLabels.length - 1)) * innerW} y={PAD_T + innerH + 16} textAnchor="middle">{l}</text>
              </g>
            ))}
          </g>
          <path className="area" d={area} />
          <path className="line" d={path(produce)} />
          <path className="line2" d={path(consume)} />
        </svg>
      </div>
      <div className="legend">
        <span><span className="swatch" style={{ background: 'var(--rust)' }} /> produce · msg/s × 1k</span>
        <span><span className="swatch" style={{ background: 'var(--ice)', borderTop: '1px dashed var(--ice)' }} /> consume · msg/s × 1k</span>
        <span style={{ marginLeft: 'auto', color: 'var(--ink-4)' }}>window: 60m · res: 60s · ws</span>
      </div>
    </div>
  );
}

function BrokerCard({ id, role, host, cpu, mem, lag, status, ssd }) {
  const cells = Array.from({ length: 8 }, (_, i) => {
    const c = (id + i * 3) % 8;
    return c < (cpu / 12.5) ? '' : 'dim';
  });
  // mark hotspots
  if (status === 'degraded') cells[2] = 'warn';
  if (status === 'isr-drop') cells[1] = 'bad';
  return (
    <div className="broker">
      <div className="h">
        <span className="id">broker-{id}</span>
        <span className={"pill " + (status === 'healthy' ? 'jade' : status === 'degraded' ? 'amber' : 'crimson')}>
          <span className="dot" />{status === 'healthy' ? 'in-sync' : status === 'degraded' ? 'degraded' : 'isr drop'}
        </span>
      </div>
      <div className="meta">
        <span>role <b>{role}</b></span>
        <span>cpu <b>{cpu}%</b></span>
        <span>host <b>{host}</b></span>
        <span>mem <b>{mem}%</b></span>
        <span>lag <b>{lag}</b></span>
        <span>ssd <b>{ssd}</b></span>
      </div>
      <div className="bars">{cells.map((c, i) => <i key={i} className={c} />)}</div>
    </div>
  );
}

function ClusterOverviewA() {
  const brokers = [
    { id: 1, role: 'controller', host: 'use2-a-01', cpu: 42, mem: 58, lag: '0',    status: 'healthy',  ssd: '62%' },
    { id: 2, role: 'replica',    host: 'use2-a-02', cpu: 48, mem: 61, lag: '0',    status: 'healthy',  ssd: '58%' },
    { id: 3, role: 'replica',    host: 'use2-a-03', cpu: 39, mem: 55, lag: '0',    status: 'healthy',  ssd: '60%' },
    { id: 4, role: 'replica',    host: 'use2-b-01', cpu: 71, mem: 64, lag: '128',  status: 'degraded', ssd: '74%' },
    { id: 5, role: 'replica',    host: 'use2-b-02', cpu: 44, mem: 57, lag: '0',    status: 'healthy',  ssd: '61%' },
    { id: 6, role: 'replica',    host: 'use2-b-03', cpu: 51, mem: 59, lag: '0',    status: 'healthy',  ssd: '63%' },
    { id: 7, role: 'replica',    host: 'use2-c-01', cpu: 47, mem: 60, lag: '0',    status: 'healthy',  ssd: '59%' },
    { id: 8, role: 'replica',    host: 'use2-c-02', cpu: 53, mem: 62, lag: '0',    status: 'healthy',  ssd: '64%' },
    { id: 9, role: 'replica',    host: 'use2-c-03', cpu: 45, mem: 58, lag: '0',    status: 'healthy',  ssd: '60%' },
  ];

  const topPart = [
    { n: 'clickstream.raw [p41]',     v: '184k', pct: 100 },
    { n: 'clickstream.raw [p17]',     v: '142k', pct: 78 },
    { n: 'orders.v2 [p07]',           v: '88k',  pct: 48 },
    { n: 'risk.signals [p03]',        v: '64k',  pct: 35 },
    { n: 'inventory.updates [p11]',   v: '42k',  pct: 23 },
    { n: 'orders.v2 [p12]',           v: '38k',  pct: 21 },
  ];

  const audit = [
    { t: '18:42:11', m: <><span className="who">j.lee</span> created topic <span className="obj">orders.v2</span> (24p, rf=3)</> },
    { t: '18:38:04', m: <><b>ISR shrink</b> on <span className="obj">clickstream.raw</span> — broker-4 fell out at p17, p23</> },
    { t: '18:31:55', m: <><span className="who">m.okafor</span> updated ACL <span className="obj">topic:payments.*</span> grant <b>read</b> to <span className="obj">role:analytics</span></> },
    { t: '18:24:12', m: <>schema <span className="obj">orders-value</span> evolved <b>v4 → v5</b> · backward-compatible</> },
    { t: '18:11:08', m: <><span className="who">svc-loader</span> created connector <span className="obj">snowflake-sink-orders</span></> },
    { t: '17:58:33', m: <>consumer group <span className="obj">risk-engine</span> rebalanced · 3 members · 24 → 24 assignments</> },
  ];

  return (
    <Shell
      active="overview"
      breadcrumb={['acme', 'prod', 'us-east-2']}
      title="us-east-2 overview"
      actions={<>
        <button className="btn ghost">⌘ K</button>
        <button className="btn">Add broker</button>
        <button className="btn primary">Create topic</button>
      </>}
    >
      {/* Patches strip */}
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 16 }}>
        <span className="patch jade square">12 nines · 87d uptime</span>
        <span className="patch rust square">p99 9.4ms</span>
        <span className="patch ice square">ack=all · all topics</span>
        <span className="patch square">🦀 rafka 0.18.2 · all brokers</span>
      </div>

      {/* Stat strip */}
      <div className="stat-strip">
        <div className="stat">
          <span className="lbl">throughput · in</span>
          <span className="val num"><span className="accent">1.42M</span><span className="unit">msg/s</span></span>
          <span className="delta up">▲ 4.1% · 1h</span>
          <span className="micro"><MiniSpark cls="sk r" pts={[60,62,61,64,68,72,74,71,75,79,82,86,88,92,94,96,98,99,100,98,99,102,104,106]} /></span>
        </div>
        <div className="stat">
          <span className="lbl">brokers in-sync</span>
          <span className="val num">8<span className="unit">/ 9</span></span>
          <span className="delta warn">broker-4 degraded</span>
        </div>
        <div className="stat">
          <span className="lbl">partitions</span>
          <span className="val num">3,408</span>
          <span className="delta">142 topics · rf=3</span>
        </div>
        <div className="stat">
          <span className="lbl">consumer lag</span>
          <span className="val num"><span style={{ color: 'var(--amber)' }}>318k</span></span>
          <span className="delta down">▼ from 612k · 10m</span>
          <span className="micro"><MiniSpark cls="sk dn" pts={[88,90,92,89,86,80,72,64,58,52,46,40,38,34,30]} /></span>
        </div>
        <div className="stat">
          <span className="lbl">storage on-disk</span>
          <span className="val num">42.8<span className="unit">TB</span></span>
          <span className="delta">+1.2 TB · 24h</span>
        </div>
      </div>

      {/* Throughput chart + broker matrix */}
      <div className="grid12">
        <div className="col-8 panel">
          <div className="panel-h">
            <div>
              <div className="title">Cluster throughput</div>
              <div className="sub" style={{ marginTop: 2 }}>produce · consume · ws stream</div>
            </div>
            <div className="tabs">
              <span className="tab">1h</span>
              <span className="tab on">6h</span>
              <span className="tab">24h</span>
              <span className="tab">7d</span>
            </div>
          </div>
          <div className="panel-body flush">
            <ThroughputChart />
          </div>
        </div>

        <div className="col-4 panel" style={{ overflow: 'hidden' }}>
          <div className="panel-h">
            <div className="title">Top partitions · 5m</div>
            <span className="sub">by write throughput</span>
          </div>
          <div className="panel-body flush">
            {topPart.map((p, i) => (
              <div key={i} className="kvrow">
                <span className="rank">{String(i + 1).padStart(2, '0')}</span>
                <span className="name">{p.n}</span>
                <span className="bar"><i style={{ width: p.pct + '%' }} /></span>
                <span className="val">{p.v}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="col-12 panel">
          <div className="panel-h">
            <div className="title">Brokers · us-east-2</div>
            <span className="sub">9 nodes · 3 AZs · controller broker-1</span>
          </div>
          <div className="panel-body">
            <div className="brokers" style={{ gridTemplateColumns: 'repeat(3, 1fr)' }}>
              {brokers.map((b) => <BrokerCard key={b.id} {...b} />)}
            </div>
          </div>
        </div>

        <div className="col-7 panel">
          <div className="panel-h">
            <div className="title">Audit · live</div>
            <span className="sub">streamed from /audit/log · last 1h</span>
          </div>
          <div className="panel-body flush" style={{ maxHeight: 260, overflowY: 'auto' }}>
            {audit.map((a, i) => (
              <div key={i} className="audit-row">
                <span className="ts">{a.t}</span>
                <span className="msg">{a.m}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="col-5 panel">
          <div className="panel-h">
            <div className="title">Latency · last 5m</div>
            <span className="sub">end-to-end produce → ack</span>
          </div>
          <div className="panel-body">
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 14 }}>
              {[
                { l: 'p50', v: '2.1', u: 'ms', c: 'var(--ink-1)' },
                { l: 'p95', v: '6.8', u: 'ms', c: 'var(--ink-1)' },
                { l: 'p99', v: '9.4', u: 'ms', c: 'var(--rust)' },
                { l: 'p99.9', v: '18.2', u: 'ms', c: 'var(--amber)' },
              ].map((x) => (
                <div key={x.l} style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                  <span style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 11, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>{x.l}</span>
                  <span className="mono num" style={{ fontSize: 24, fontWeight: 600, color: x.c, letterSpacing: '-0.015em' }}>{x.v}<span style={{ fontSize: 12, color: 'var(--ink-3)', marginLeft: 3, fontWeight: 400 }}>{x.u}</span></span>
                </div>
              ))}
            </div>
            <hr className="hr" style={{ margin: '16px 0 14px' }} />
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: 'var(--ink-3)' }}>
              <span className="mono">target: p99 &lt; 10ms</span>
              <span className="mono" style={{ color: 'var(--jade)' }}>✓ within SLO</span>
            </div>
          </div>
        </div>
      </div>
    </Shell>
  );
}

Object.assign(window, { ClusterOverviewA, MiniSpark });
