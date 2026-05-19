// Rafka — Consumer groups + lag detail

function ConsumerGroups() {
  const [sel, setSel] = React.useState('orders-fulfillment');

  const groups = [
    { n: 'orders-fulfillment', topics: 'orders.v2, inventory.updates', members: 6, lag: '1.2k',  state: 'rebalancing', cls: 'amber', warn: false },
    { n: 'payments-ledger',    topics: 'payments.events',               members: 3, lag: '0',     state: 'stable',      cls: 'jade',  warn: false },
    { n: 'clickstream-etl',    topics: 'clickstream.raw',               members: 8, lag: '312k',  state: 'lagging',     cls: 'amber', warn: true  },
    { n: 'risk-scorer',        topics: 'risk.signals, orders.v2',       members: 4, lag: '0',     state: 'stable',      cls: 'jade',  warn: false },
    { n: 'analytics-warehouse',topics: 'orders.v2, payments.events',    members: 2, lag: '4.1k',  state: 'stable',      cls: 'jade',  warn: false },
    { n: 'fraud-detect',       topics: 'payments.events, auth.signins', members: 4, lag: '0',     state: 'stable',      cls: 'jade',  warn: false },
    { n: 'audit-stream',       topics: 'payments.audit',                members: 1, lag: '0',     state: 'stable',      cls: 'jade',  warn: false },
    { n: 'shipping-notify',    topics: 'logistics.events',              members: 2, lag: '12',    state: 'stable',      cls: 'jade',  warn: false },
    { n: 'legacy-mirror',      topics: 'orders.v1',                     members: 0, lag: '—',     state: 'empty',       cls: '',      warn: false },
  ];

  const cur = groups.find((g) => g.n === sel) || groups[0];

  // Lag chart data — 60 points
  const lagSeries = Array.from({ length: 60 }, (_, i) => {
    const base = cur.warn ? 200 + i * 4.5 : 30 + Math.sin(i / 6) * 18;
    const spike = cur.warn && i > 36 && i < 44 ? 60 : 0;
    return Math.max(0, base + spike + (Math.sin(i * 0.7) * 10));
  });
  const lagMax = Math.max(...lagSeries) * 1.15;
  const W = 800, H = 200, PADL = 36, PADR = 14, PADT = 10, PADB = 22;
  const innerW = W - PADL - PADR;
  const innerH = H - PADT - PADB;
  const xAt = (i) => PADL + (i / (lagSeries.length - 1)) * innerW;
  const yAt = (v) => PADT + innerH - (v / lagMax) * innerH;
  const linePath = lagSeries.map((v, i) => `${i ? 'L' : 'M'} ${xAt(i).toFixed(1)} ${yAt(v).toFixed(1)}`).join(' ');
  const areaPath = `${linePath} L ${xAt(lagSeries.length - 1).toFixed(1)} ${PADT + innerH} L ${PADL} ${PADT + innerH} Z`;

  // Partition assignment matrix — 24 partitions × N members
  const memberNames = ['fulfill-7df2', 'fulfill-9a14', 'fulfill-c081', 'fulfill-3e6b', 'fulfill-1f2a', 'fulfill-bb09'];
  const parts = 24;
  const assignment = Array.from({ length: parts }, (_, p) => p % memberNames.length);
  const partLag = Array.from({ length: parts }, (_, p) => {
    if (cur.warn) return [3,3,2,3,3,2,3,3,2,3,3,3,2,3,3,2,3,3,2,3,3,2,3,3][p];
    return [1,0,0,1,0,0,1,0,0,0,1,0,0,0,1,0,0,0,1,0,0,0,1,0][p];
  });

  return (
    <Shell
      active="groups"
      breadcrumb={['acme', 'prod', 'us-east-2', 'consumer groups']}
      title="consumer groups"
      actions={<>
        <button className="btn ghost">Export CSV</button>
        <button className="btn">⟲ Reset offsets</button>
      </>}
    >
      <div className="panel" style={{ padding: 0 }}>
        <div className="cg-split">
          {/* ── Left: groups list ── */}
          <div className="cg-list">
            <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--line-1)', display: 'flex', alignItems: 'center', gap: 8 }}>
              <div className="search-i" style={{ flex: 1, display: 'flex', alignItems: 'center', gap: 8, height: 28, padding: '0 10px', background: 'var(--bg-1)', border: '1px solid var(--line-1)', borderRadius: 7, fontSize: 12, color: 'var(--ink-3)' }}>
                <Icon name="search" />
                <input placeholder="filter groups" style={{ background: 'transparent', border: 0, outline: 0, color: 'var(--ink-1)', font: 'inherit', flex: 1, minWidth: 0 }} />
              </div>
              <span className="kbd">9</span>
            </div>
            {groups.map((g) => (
              <div key={g.n} className={'cg-li' + (g.n === sel ? ' sel' : '') + (g.warn ? ' warn' : '')} onClick={() => setSel(g.n)}>
                <div className="nm">{g.n}</div>
                <div className="meta">{g.topics}  ·  {g.members} member{g.members === 1 ? '' : 's'}</div>
                <div className="lag"><b>{g.lag}</b>lag</div>
              </div>
            ))}
          </div>

          {/* ── Right: detail ── */}
          <div className="cg-detail">
            <div className="cg-hero">
              <div className="l">
                <h2>{cur.n}</h2>
                <div className="tags">
                  <span className={"pill " + cur.cls}>{cur.cls && <span className="dot" />}{cur.state}</span>
                  <span className="tag">protocol: range</span>
                  <span className="tag">session 30s</span>
                  <span className="tag">commits: auto · 5s</span>
                  <span className="tag">isolation: read_committed</span>
                </div>
              </div>
              <div className="actions">
                <button className="btn ghost">Pause</button>
                <button className="btn ghost">Restart</button>
                <button className="btn primary">Reset offsets…</button>
              </div>
            </div>

            <div className="cg-kpis">
              <div className="cg-kpi">
                <div className="lbl">total lag</div>
                <div className="val">{cur.warn ? '312' : '1.2'}<span className="unit">{cur.warn ? 'k msgs' : 'k msgs'}</span></div>
                <div className={'delta ' + (cur.warn ? 'warn' : 'up')}>{cur.warn ? '↑ growing · +4.5k/min' : '↓ recovering · −180/min'}</div>
              </div>
              <div className="cg-kpi">
                <div className="lbl">throughput</div>
                <div className="val">{cur.warn ? '184' : '12.4'}<span className="unit">k msg/s</span></div>
                <div className="delta up">↑ 8.2% vs 1h</div>
              </div>
              <div className="cg-kpi">
                <div className="lbl">members</div>
                <div className="val">{cur.warn ? '8' : '6'}<span className="unit">/ {cur.warn ? '8' : '6'} healthy</span></div>
                <div className="delta">last rebalance · 4m ago</div>
              </div>
              <div className="cg-kpi">
                <div className="lbl">p99 commit</div>
                <div className="val">{cur.warn ? '94' : '12'}<span className="unit">ms</span></div>
                <div className={'delta ' + (cur.warn ? 'warn' : 'up')}>{cur.warn ? 'slo: 50ms · breached' : 'slo: 50ms'}</div>
              </div>
            </div>

            {/* Lag chart */}
            <div className="cg-chart">
              <div style={{ display: 'flex', alignItems: 'center', gap: 14, marginBottom: 6, padding: '0 4px' }}>
                <span style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 11, color: 'var(--ink-2)' }}>consumer lag · last 1h</span>
                <span className="pill amber" style={{ height: 18 }}><span className="dot" />warn &gt;5k</span>
                <span style={{ flex: 1 }}></span>
                <span style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 11, color: 'var(--ink-3)' }}>1m · 15m · <span style={{ color: 'var(--ink-1)' }}>1h</span> · 6h · 24h</span>
              </div>
              <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
                {/* y gridlines */}
                {[0, 0.25, 0.5, 0.75, 1].map((p, i) => (
                  <g key={i}>
                    <line className="grid" x1={PADL} x2={W - PADR} y1={PADT + innerH * (1 - p)} y2={PADT + innerH * (1 - p)} />
                    <text className="y" x={PADL - 6} y={PADT + innerH * (1 - p) + 3} textAnchor="end">{Math.round(lagMax * p)}</text>
                  </g>
                ))}
                {/* slo line */}
                <line x1={PADL} x2={W - PADR} y1={yAt(cur.warn ? 5000 : 200)} y2={yAt(cur.warn ? 5000 : 200)} stroke="var(--amber)" strokeDasharray="3 4" strokeWidth="1" opacity="0.6" />
                <path className="lag-area" d={areaPath} />
                <path className="lag-line" d={linePath} />
                {/* x axis labels */}
                {['−60m', '−45m', '−30m', '−15m', 'now'].map((l, i) => (
                  <text key={l} className="y" x={PADL + (innerW * i) / 4} y={H - 6} textAnchor="middle">{l}</text>
                ))}
              </svg>
            </div>

            {/* Partition matrix */}
            <div className="cg-pmat" style={{ borderTop: '1px solid var(--line-1)' }}>
              <div className="head">
                <span>partition assignment · 24 partitions</span>
                <span className="legend">
                  <span><i style={{ background: 'var(--bg-3)' }}></i>idle</span>
                  <span><i style={{ background: 'oklch(from var(--rust) l c h / 0.55)' }}></i>assigned</span>
                  <span><i style={{ background: 'oklch(from var(--rust) l c h / 0.78)' }}></i>busy</span>
                  <span><i style={{ background: 'oklch(from var(--amber) l c h / 0.65)' }}></i>lag</span>
                </span>
              </div>
              <div className="grid" style={{ gridTemplateColumns: `repeat(${parts}, 1fr)` }}>
                {Array.from({ length: parts }, (_, p) => {
                  const lv = partLag[p];
                  const cls = lv === 0 ? 'lv0' : lv === 1 ? 'lv1' : lv === 2 ? 'lv2' : lv === 3 ? (cur.warn ? 'warn' : 'lv3') : 'lv3';
                  return <div key={p} className={'cell ' + cls} title={`p${p} → ${memberNames[assignment[p]]}`}>{p}</div>;
                })}
              </div>
            </div>

            {/* Members */}
            <div style={{ borderTop: '1px solid var(--line-1)' }}>
              <div className="cg-members">
                <div className="th">member id</div>
                <div className="th">host</div>
                <div className="th">client</div>
                <div className="th">partitions</div>
                <div className="th r">lag</div>
                <div className="th">heartbeat</div>

                {memberNames.map((m, i) => {
                  const owned = assignment.map((a, p) => (a === i ? p : -1)).filter((p) => p >= 0);
                  const memberLag = cur.warn ? ['52k', '47k', '58k', '49k', '51k', '55k'][i] : ['0', '120', '0', '380', '0', '720'][i];
                  return (
                    <React.Fragment key={m}>
                      <div className="td mono" style={{ color: 'var(--ink-1)' }}>{m}</div>
                      <div className="td mono" style={{ color: 'var(--ink-2)' }}>ip-10-0-{12 + i}-{42 + i * 3}</div>
                      <div className="td mono" style={{ color: 'var(--ink-2)' }}>rdkafka/2.3</div>
                      <div className="td mono" style={{ color: 'var(--ink-2)' }}>{owned.length ? `p${owned.join(', p')}`.replace(/, p/g, ', p').slice(0, 36) + (owned.join(', p').length > 36 ? '…' : '') : '—'}</div>
                      <div className="td mono r" style={{ color: cur.warn ? 'var(--amber)' : 'var(--ink-2)' }}>{memberLag}</div>
                      <div className="td mono" style={{ color: 'var(--ink-3)' }}>{(i + 1) * 1.2}s ago</div>
                    </React.Fragment>
                  );
                })}
              </div>
            </div>

            {/* CLI footer */}
            <div style={{ padding: '14px 24px 22px' }}>
              <div className="term">
                <div className="term-head">
                  <span className="lights"><i /><i /><i /></span>
                  <span>reset offsets · cli equivalent</span>
                </div>
                <div className="term-body" style={{ padding: '10px 14px' }}>
                  <div><span className="prompt">$</span> rafka <span className="arg">groups reset</span> {cur.n} <span className="flag">--to-timestamp</span> <span className="num">2026-05-10T18:00:00Z</span> <span className="flag">--dry-run</span></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Shell>
  );
}

Object.assign(window, { ConsumerGroups });
