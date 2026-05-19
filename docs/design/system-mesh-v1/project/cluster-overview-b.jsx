// Rafka — Cluster Overview · variant B (chart-led, calmer)
// Same data, fewer panels, larger hero chart, broker heatmap instead of cards.

function HeroChart() {
  const W = 1100, H = 280, PAD_L = 44, PAD_R = 16, PAD_T = 16, PAD_B = 32;
  const innerW = W - PAD_L - PAD_R, innerH = H - PAD_T - PAD_B;
  const N = 90;
  const produce = Array.from({ length: N }, (_, i) => {
    const t = i / N;
    return 800 + 600 * Math.sin(t * 6) * 0.5 + 400 * t + 80 * Math.sin(i * 0.6);
  });
  const consume = produce.map((v, i) => v - 80 + Math.sin(i / 5) * 60);
  const max = 1800, min = 400;
  const xs = (i) => PAD_L + (i / (N - 1)) * innerW;
  const ys = (v) => PAD_T + innerH - ((v - min) / (max - min)) * innerH;
  const path = (arr) => arr.map((v, i) => `${i ? 'L' : 'M'}${xs(i).toFixed(1)},${ys(v).toFixed(1)}`).join(' ');
  const area = `${path(produce)} L${xs(N - 1)},${PAD_T + innerH} L${PAD_L},${PAD_T + innerH} Z`;

  return (
    <div className="bigchart" style={{ height: 320 }}>
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
        <defs>
          <linearGradient id="rustG" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="var(--rust)" stopOpacity="0.35" />
            <stop offset="100%" stopColor="var(--rust)" stopOpacity="0.02" />
          </linearGradient>
        </defs>
        <g className="gy">
          {[600, 1000, 1400].map((t) => (
            <g key={t}>
              <line x1={PAD_L} x2={W - PAD_R} y1={ys(t)} y2={ys(t)} />
              <text x={PAD_L - 8} y={ys(t) + 3} textAnchor="end">{(t / 1000).toFixed(1)}M</text>
            </g>
          ))}
        </g>
        <g className="gx">
          {['−6h','−4h30','−3h','−1h30','now'].map((l, i) => (
            <text key={l} x={PAD_L + (i / 4) * innerW} y={PAD_T + innerH + 18} textAnchor="middle">{l}</text>
          ))}
        </g>
        <path d={area} fill="url(#rustG)" />
        <path className="line" d={path(produce)} strokeWidth="2" />
        <path className="line2" d={path(consume)} />
      </svg>
    </div>
  );
}

function BrokerHeatmap() {
  // 9 brokers × 24 hours · color intensity = load
  const brokers = 9, hours = 24;
  const data = Array.from({ length: brokers }, (_, b) =>
    Array.from({ length: hours }, (_, h) => {
      const base = 0.3 + 0.4 * Math.sin((h + b * 2) / 4) + 0.2 * Math.cos(h / 3);
      // broker-4 hotspot around h=18
      if (b === 3 && h >= 17 && h <= 19) return Math.min(1, base + 0.5);
      return Math.max(0.05, Math.min(0.95, base));
    })
  );
  return (
    <div className="panel" style={{ overflow: 'hidden' }}>
      <div className="panel-h">
        <div className="title">Broker load · last 24h</div>
        <span className="sub">heat = avg msg/s · row = broker · col = hour</span>
      </div>
      <div className="panel-body">
        <div style={{ display: 'grid', gridTemplateColumns: '60px 1fr', gap: 8, alignItems: 'center' }}>
          {data.map((row, b) => (
            <React.Fragment key={b}>
              <span className="mono" style={{ fontSize: 11, color: 'var(--ink-3)' }}>
                broker-{b + 1}{b === 0 && <span style={{ color: 'var(--rust)' }}> ●</span>}
              </span>
              <div style={{ display: 'grid', gridTemplateColumns: `repeat(${hours}, 1fr)`, gap: 3 }}>
                {row.map((v, h) => {
                  const isHot = v > 0.75;
                  const bg = isHot
                    ? `oklch(0.72 0.20 38 / ${v})`
                    : `oklch(0.74 0.18 50 / ${v * 0.7})`;
                  return <div key={h} style={{ aspectRatio: '1', borderRadius: 3, background: bg }} title={`broker-${b + 1} · h-${h}`} />;
                })}
              </div>
            </React.Fragment>
          ))}
          <span />
          <div style={{ display: 'grid', gridTemplateColumns: `repeat(${hours}, 1fr)`, gap: 3, fontFamily: 'JetBrains Mono, monospace', fontSize: 9, color: 'var(--ink-4)' }}>
            {Array.from({ length: hours }, (_, h) => (
              <span key={h} style={{ textAlign: 'center' }}>{h % 4 === 0 ? `${h}h` : ''}</span>
            ))}
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 14, fontFamily: 'JetBrains Mono, monospace', fontSize: 11, color: 'var(--ink-3)' }}>
          <span>cool</span>
          <div style={{ display: 'flex', gap: 2 }}>
            {[0.1, 0.25, 0.4, 0.55, 0.7, 0.85].map((v, i) => (
              <i key={i} style={{ width: 18, height: 8, borderRadius: 2, background: v > 0.75 ? `oklch(0.72 0.20 38 / ${v})` : `oklch(0.74 0.18 50 / ${v * 0.7})` }} />
            ))}
          </div>
          <span>hot</span>
          <span style={{ marginLeft: 'auto', color: 'var(--amber)' }}>● broker-4 hotspot @ 18:00</span>
        </div>
      </div>
    </div>
  );
}

function ClusterOverviewB() {
  return (
    <Shell
      active="overview"
      breadcrumb={['acme', 'prod', 'us-east-2']}
      title="us-east-2 overview"
      collapsed={true}
      actions={<>
        <button className="btn ghost">⌘ K</button>
        <button className="btn primary">Create topic</button>
      </>}
    >
      {/* Hero throughput */}
      <div className="panel" style={{ marginBottom: 16 }}>
        <div className="panel-h">
          <div className="row gap-4" style={{ alignItems: 'baseline' }}>
            <div>
              <div className="title">Throughput</div>
              <div className="sub" style={{ marginTop: 2 }}>cluster-wide · produce vs consume · live</div>
            </div>
            <div className="mono num" style={{ fontSize: 38, fontWeight: 600, letterSpacing: '-0.025em', lineHeight: 1 }}>
              <span style={{ color: 'var(--rust)' }}>1.42M</span> <span style={{ fontSize: 14, color: 'var(--ink-3)', fontWeight: 400 }}>msg/s</span>
            </div>
            <div className="mono" style={{ fontSize: 12, color: 'var(--jade)' }}>▲ 4.1% · vs 1h</div>
          </div>
          <div className="tabs">
            <span className="tab">1h</span>
            <span className="tab on">6h</span>
            <span className="tab">24h</span>
            <span className="tab">7d</span>
          </div>
        </div>
        <div className="panel-body flush">
          <HeroChart />
          <div className="legend">
            <span><span className="swatch" style={{ background: 'var(--rust)' }} /> produce</span>
            <span><span className="swatch" style={{ background: 'var(--ice)' }} /> consume</span>
            <span style={{ marginLeft: 'auto', color: 'var(--ink-4)' }}>ws · res 5m · 6h window</span>
          </div>
        </div>
      </div>

      {/* Side-by-side: SLO summary + broker heatmap */}
      <div className="grid12">
        <div className="col-4">
          <div className="panel" style={{ marginBottom: 16 }}>
            <div className="panel-h">
              <div className="title">SLO · this hour</div>
              <span className="sub">target p99 &lt; 10ms</span>
            </div>
            <div className="panel-body" style={{ padding: '20px 22px' }}>
              <div className="mono num" style={{ fontSize: 56, fontWeight: 600, letterSpacing: '-0.03em', color: 'var(--rust)', lineHeight: 0.95 }}>
                9.4<span style={{ fontSize: 18, color: 'var(--ink-3)', marginLeft: 6, fontWeight: 400 }}>ms</span>
              </div>
              <div className="mono" style={{ fontSize: 12, color: 'var(--jade)', marginTop: 8 }}>✓ within SLO · 0.6ms headroom</div>
              <hr className="hr" style={{ margin: '18px 0' }} />
              <div className="row" style={{ justifyContent: 'space-between', fontFamily: 'JetBrains Mono, monospace', fontSize: 12 }}>
                <span style={{ color: 'var(--ink-3)' }}>p50</span><span>2.1ms</span>
              </div>
              <div className="row" style={{ justifyContent: 'space-between', fontFamily: 'JetBrains Mono, monospace', fontSize: 12, marginTop: 6 }}>
                <span style={{ color: 'var(--ink-3)' }}>p95</span><span>6.8ms</span>
              </div>
              <div className="row" style={{ justifyContent: 'space-between', fontFamily: 'JetBrains Mono, monospace', fontSize: 12, marginTop: 6 }}>
                <span style={{ color: 'var(--ink-3)' }}>p99.9</span><span style={{ color: 'var(--amber)' }}>18.2ms</span>
              </div>
            </div>
          </div>

          <div className="panel">
            <div className="panel-h">
              <div className="title">Cluster</div>
              <span className="sub">at a glance</span>
            </div>
            <div className="panel-body" style={{ padding: 0 }}>
              {[
                ['brokers in-sync', '8 / 9',     'warn',  'broker-4 degraded'],
                ['topics',          '142',       '',      '+3 today'],
                ['partitions',      '3,408',     '',      'rf=3 · evenly distributed'],
                ['consumer lag',    '318k',      'warn',  '▼ from 612k · 10m'],
                ['storage',         '42.8 TB',   '',      '+1.2 TB / 24h'],
                ['uptime',          '87d 4h',    'ok',    '12 nines this quarter'],
              ].map(([k, v, c, sub], i) => (
                <div key={k} style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', padding: '12px 22px', borderTop: i ? '1px solid var(--line-1)' : 0 }}>
                  <div>
                    <div className="mono" style={{ fontSize: 11, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>{k}</div>
                    <div className="mono" style={{ fontSize: 10.5, color: c === 'warn' ? 'var(--amber)' : c === 'ok' ? 'var(--jade)' : 'var(--ink-4)', marginTop: 2 }}>{sub}</div>
                  </div>
                  <div className="mono num" style={{ fontSize: 18, fontWeight: 500 }}>{v}</div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="col-8">
          <BrokerHeatmap />

          <div className="panel" style={{ marginTop: 16 }}>
            <div className="panel-h">
              <div className="title">Recent activity</div>
              <span className="sub">audit + system events · last 1h</span>
            </div>
            <div className="panel-body flush">
              {[
                { t: '18:42:11', m: <><span className="who">j.lee</span> created topic <span className="obj">orders.v2</span> · 24p · rf=3</> },
                { t: '18:38:04', m: <><b>ISR shrink</b> on <span className="obj">clickstream.raw</span> — broker-4 fell out at p17, p23</> },
                { t: '18:31:55', m: <><span className="who">m.okafor</span> updated ACL <span className="obj">topic:payments.*</span> · grant <b>read</b> to <span className="obj">role:analytics</span></> },
                { t: '18:24:12', m: <>schema <span className="obj">orders-value</span> evolved <b>v4 → v5</b> · backward-compatible</> },
              ].map((a, i) => (
                <div key={i} className="audit-row">
                  <span className="ts">{a.t}</span>
                  <span className="msg">{a.m}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </Shell>
  );
}

Object.assign(window, { ClusterOverviewB });
