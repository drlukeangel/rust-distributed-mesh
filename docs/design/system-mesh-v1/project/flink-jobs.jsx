// Rafka — Flink jobs

function FlinkJobs() {
  const jobs = [
    { id: 'orders-enrich',        name: 'orders-enrich',                state: 'running',  cls: 'jade',    par: 8,  rate: 142,  lag: '2.1s',  ck: 'ok',   spark: [10,12,11,13,18,16,20,22,19,21,24,22,25,24,26] },
    { id: 'sessionize-click',     name: 'sessionize-click',             state: 'running',  cls: 'jade',    par: 24, rate: 488,  lag: '4.8s',  ck: 'ok',   spark: [40,42,46,48,52,49,54,58,56,60,62,58,64,66,62] },
    { id: 'fraud-score-v3',       name: 'fraud-score-v3',               state: 'degraded', cls: 'amber',   par: 16, rate: 71,   lag: '38s',   ck: 'late', spark: [30,28,32,29,35,38,42,46,52,58,64,68,62,70,72] },
    { id: 'iceberg-cdc-mirror',   name: 'iceberg-cdc-mirror',           state: 'running',  cls: 'jade',    par: 6,  rate: 12,   lag: '1.4s',  ck: 'ok',   spark: [8,9,10,9,11,12,13,12,11,12,14,13,12,14,13] },
    { id: 'payments-window-1m',   name: 'payments-window-1m',           state: 'running',  cls: 'jade',    par: 12, rate: 38,   lag: '6.0s',  ck: 'ok',   spark: [18,20,22,21,24,25,22,26,28,27,30,29,28,32,30] },
    { id: 'inventory-projection', name: 'inventory-projection',         state: 'recover',  cls: 'ice',     par: 4,  rate: 6,    lag: '52m',   ck: '—',    spark: [22,18,14,10,6,4,3,4,5,6,7,8,9,11,12] },
    { id: 'risk-features-rt',     name: 'risk-features-rt',             state: 'failed',   cls: 'crimson', par: 0,  rate: 0,    lag: '—',     ck: 'fail', spark: [40,42,38,36,30,22,12,2,0,0,0,0,0,0,0] },
    { id: 'audit-stitch',         name: 'audit-stitch',                 state: 'canceled', cls: '',        par: 0,  rate: 0,    lag: '—',     ck: '—',    spark: [10,11,9,12,10,8,4,2,0,0,0,0,0,0,0] },
  ];
  const [sel, setSel] = React.useState(0);
  const job = jobs[sel];

  // DAG operators — 4-stage stream pipeline.
  const ops = [
    { id: 'src',   nm: 'kafka-source',    sub: 'orders.v2',                  par: 8,  rate: '142.4k/s', bp: 0.10, cls: 'src'   },
    { id: 'parse', nm: 'parse-json',      sub: 'flat-map · 4 fields',        par: 8,  rate: '142.4k/s', bp: 0.18, cls: 'op'    },
    { id: 'enr',   nm: 'enrich-customer', sub: 'async-io · redis lookup',    par: 8,  rate: '141.9k/s', bp: 0.42, cls: 'op'    },
    { id: 'win',   nm: 'tumbling-1m',     sub: 'keyed by customer_id',       par: 8,  rate: '4.7k/s',   bp: 0.65, cls: 'op-w'  },
    { id: 'sink',  nm: 'iceberg-sink',    sub: 'orders.enriched',            par: 4,  rate: '4.7k/s',   bp: 0.22, cls: 'sink'  },
  ];

  // Checkpoint history — last 10
  const checkpoints = Array.from({ length: 10 }, (_, i) => {
    const idx = 4218 - i;
    const dur = (1.8 + Math.sin(i * 0.9) * 0.6 + (i === 1 ? 4.2 : 0)).toFixed(1);
    const sz = (340 + Math.sin(i * 0.7) * 60).toFixed(0);
    const st = i === 1 ? 'slow' : i === 6 ? 'fail' : 'ok';
    return { idx, dur, sz, st, when: `${(i * 30 + 12)}s ago` };
  });

  // Backpressure spark for hero chart
  const bpSeries = Array.from({ length: 60 }, (_, i) => 22 + Math.sin(i / 3) * 8 + (i > 42 ? (i - 42) * 1.4 : 0));
  const maxBp = Math.max(...bpSeries);
  const bpPath = bpSeries.map((v, i) => `${i === 0 ? 'M' : 'L'}${(i / (bpSeries.length - 1)) * 100},${100 - (v / maxBp) * 100}`).join(' ');
  const bpFill = bpPath + ` L100,100 L0,100 Z`;

  return (
    <Shell
      active="flink"
      breadcrumb={['acme', 'prod', 'us-east-2', 'flink']}
      title="flink jobs"
      actions={<>
        <button className="btn ghost">Job graph viewer</button>
        <button className="btn ghost">Savepoint</button>
        <button className="btn primary">+ Submit job</button>
      </>}
    >
      <div className="panel" style={{ padding: 0 }}>
        <div className="fl-split">
          {/* ── Left: job list ── */}
          <div className="fl-list">
            <div className="fl-summary">
              <div className="cell"><div className="lbl">running</div><div className="val">5</div></div>
              <div className="cell"><div className="lbl">degraded</div><div className="val warn">1</div></div>
              <div className="cell"><div className="lbl">failed</div><div className="val bad">1</div></div>
              <div className="cell"><div className="lbl">slots</div><div className="val">76<span className="u">/96</span></div></div>
            </div>
            <div className="fl-filters">
              <span className="chip on">all</span>
              <span className="chip">running</span>
              <span className="chip">issues</span>
              <span className="chip">canceled</span>
            </div>
            {jobs.map((j, i) => (
              <div key={j.id} className={'fl-li' + (i === sel ? ' sel' : '')} onClick={() => setSel(i)}>
                <div className="nm">
                  <span className={'pill ' + j.cls} style={{ height: 16, padding: '0 6px', fontSize: 10 }}>
                    {j.cls && <span className="dot" />}{j.state}
                  </span>
                  <span>{j.name}</span>
                </div>
                <div className="meta">par {j.par}  ·  lag {j.lag}  ·  ck {j.ck}</div>
                <svg className="spark-mini" viewBox="0 0 60 22" preserveAspectRatio="none">
                  <path
                    d={j.spark.map((v, k) => `${k === 0 ? 'M' : 'L'}${(k / (j.spark.length - 1)) * 60},${22 - (v / Math.max(...j.spark)) * 18 - 2}`).join(' ')}
                    fill="none"
                    stroke={j.cls === 'crimson' ? 'var(--crimson)' : j.cls === 'amber' ? 'var(--amber)' : j.cls === 'ice' ? 'var(--ice)' : j.cls === '' ? 'var(--ink-4)' : 'var(--rust)'}
                    strokeWidth="1.4"
                  />
                </svg>
                <div className="rate">{j.rate ? <><b>{j.rate}</b><span>k/s</span></> : <span className="dim">—</span>}</div>
              </div>
            ))}
          </div>

          {/* ── Right: job detail ── */}
          <div className="fl-detail">
            <div className="fl-hero">
              <div className="l">
                <div className="bcl"><span className="dim">flink ›</span> session-cluster-1 <span className="dim">›</span> jobs</div>
                <h2>{job.name}</h2>
                <div className="tags">
                  <span className={'pill ' + job.cls}>{job.cls && <span className="dot" />}{job.state}</span>
                  <span className="tag">job-id 7c4a91e</span>
                  <span className="tag">par {job.par}</span>
                  <span className="tag">flink 1.18.1</span>
                  <span className="tag">uptime 14d 06:42</span>
                  <span className="tag">savepoint · s3://acme-flink/sp-3812</span>
                </div>
              </div>
              <div className="actions">
                <button className="btn ghost">Savepoint &amp; stop</button>
                <button className="btn ghost">Rescale</button>
                <button className="btn primary">Restart</button>
              </div>
            </div>

            <div className="fl-kpis">
              <div className="fl-kpi">
                <div className="lbl">throughput · in</div>
                <div className="val">142.4<span className="unit">k/s</span></div>
                <div className="delta up">▲ 4.2% vs 1h</div>
              </div>
              <div className="fl-kpi">
                <div className="lbl">watermark lag</div>
                <div className="val">2.1<span className="unit">s</span></div>
                <div className="delta">p99 · 4.8s</div>
              </div>
              <div className="fl-kpi">
                <div className="lbl">checkpoint · last</div>
                <div className="val">1.8<span className="unit">s</span></div>
                <div className="delta up">✓ ck-4218 · 342 MB</div>
              </div>
              <div className="fl-kpi">
                <div className="lbl">max backpressure</div>
                <div className="val warn">65<span className="unit">%</span></div>
                <div className="delta warn">tumbling-1m · rising</div>
              </div>
              <div className="fl-kpi">
                <div className="lbl">task failures · 24h</div>
                <div className="val">2</div>
                <div className="delta">last 38m ago · recovered</div>
              </div>
            </div>

            {/* ── DAG ── */}
            <div className="fl-section">
              <div className="fl-section-h">
                <h3>job graph</h3>
                <div className="tabs">
                  <span className="tab on">live</span>
                  <span className="tab">backpressure</span>
                  <span className="tab">rate</span>
                  <span className="tab">records</span>
                </div>
                <span className="mono dim" style={{ marginLeft: 'auto' }}>5 vertices · 4 edges</span>
              </div>
              <div className="fl-dag">
                {ops.map((o, i) => (
                  <React.Fragment key={o.id}>
                    <div className={'fl-node ' + o.cls}>
                      <div className="fl-node-h">
                        <span className="kind">{o.cls === 'src' ? 'SRC' : o.cls === 'sink' ? 'SNK' : o.cls === 'op-w' ? 'WIN' : 'OP'}</span>
                        <span className="par">×{o.par}</span>
                      </div>
                      <div className="nm">{o.nm}</div>
                      <div className="sub">{o.sub}</div>
                      <div className="rate mono">{o.rate}</div>
                      <div className="bp">
                        <div className="bp-lbl">
                          <span>backpressure</span>
                          <span className={'mono ' + (o.bp > 0.5 ? 'warn' : o.bp > 0.3 ? 'mid' : '')}>{Math.round(o.bp * 100)}%</span>
                        </div>
                        <div className="bp-bar"><i style={{ width: (o.bp * 100) + '%' }} className={o.bp > 0.5 ? 'warn' : o.bp > 0.3 ? 'mid' : ''}></i></div>
                      </div>
                    </div>
                    {i < ops.length - 1 && (
                      <svg className="fl-edge" viewBox="0 0 60 60" preserveAspectRatio="none">
                        <defs>
                          <marker id={`arr-${i}`} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto">
                            <path d="M0,0 L10,5 L0,10 z" fill="var(--ink-3)" />
                          </marker>
                        </defs>
                        <line x1="0" y1="30" x2="54" y2="30" stroke="var(--ink-3)" strokeWidth="1.4" markerEnd={`url(#arr-${i})`} />
                        <text x="30" y="22" textAnchor="middle" fill="var(--ink-3)" fontFamily="JetBrains Mono, monospace" fontSize="9">FORWARD</text>
                      </svg>
                    )}
                  </React.Fragment>
                ))}
              </div>
            </div>

            {/* ── Charts row: backpressure timeline + checkpoint history ── */}
            <div className="fl-row">
              <div className="panel fl-chart">
                <div className="panel-h">
                  <div>
                    <div className="title">backpressure · tumbling-1m · 60min</div>
                    <div className="sub">window operator has been climbing past 60% — stage is fanning out too few records</div>
                  </div>
                  <div className="tabs">
                    <span className="tab on">1h</span>
                    <span className="tab">6h</span>
                    <span className="tab">24h</span>
                  </div>
                </div>
                <div className="panel-body flush" style={{ padding: '14px 16px 4px' }}>
                  <svg viewBox="0 0 100 100" preserveAspectRatio="none" style={{ width: '100%', height: 180 }}>
                    <g>
                      {[20, 40, 60, 80].map((y) => (
                        <line key={y} x1="0" x2="100" y1={y} y2={y} stroke="var(--line-1)" strokeWidth="0.4" />
                      ))}
                      <line x1="0" x2="100" y1="35" y2="35" stroke="var(--amber)" strokeWidth="0.4" strokeDasharray="2 2" opacity="0.6" />
                    </g>
                    <path d={bpFill} fill="oklch(from var(--amber) l c h / 0.18)" />
                    <path d={bpPath} fill="none" stroke="var(--amber)" strokeWidth="1.2" vectorEffect="non-scaling-stroke" />
                  </svg>
                  <div className="legend" style={{ padding: '4px 0 12px' }}>
                    <span><i className="swatch" style={{ background: 'var(--amber)' }}></i> bp%</span>
                    <span><i className="swatch" style={{ background: 'transparent', borderBottom: '1px dashed var(--amber)' }}></i> alert · 65%</span>
                    <span style={{ marginLeft: 'auto' }}>now: <b style={{ color: 'var(--amber)' }}>65%</b></span>
                  </div>
                </div>
              </div>

              <div className="panel fl-ckpt">
                <div className="panel-h">
                  <div>
                    <div className="title">checkpoint history</div>
                    <div className="sub">incremental · rocksdb · s3 backend</div>
                  </div>
                  <span className="mono dim">retain 10</span>
                </div>
                <div className="fl-ckpt-tbl">
                  <div className="th">id</div>
                  <div className="th">state</div>
                  <div className="th r">duration</div>
                  <div className="th r">size</div>
                  <div className="th r">trigger</div>
                  {checkpoints.map((c) => (
                    <React.Fragment key={c.idx}>
                      <div className="td mono">ck-{c.idx}</div>
                      <div className="td">
                        <span className={'pill ' + (c.st === 'fail' ? 'crimson' : c.st === 'slow' ? 'amber' : 'jade')}
                              style={{ height: 16, padding: '0 6px', fontSize: 10 }}>
                          <span className="dot" />{c.st === 'ok' ? 'ok' : c.st}
                        </span>
                      </div>
                      <div className="td mono r">{c.dur}s</div>
                      <div className="td mono r">{c.sz} MB</div>
                      <div className="td mono r dim">{c.when}</div>
                    </React.Fragment>
                  ))}
                </div>
              </div>
            </div>

            {/* ── Config + CLI ── */}
            <div className="fl-row" style={{ padding: '4px 28px 28px' }}>
              <div className="term">
                <div className="term-head"><span className="lights"><i /><i /><i /></span><span>{job.id} · job.yaml</span><span style={{ marginLeft: 'auto', color: 'var(--ink-3)' }}>edit ⌘E</span></div>
                <div className="term-body" style={{ padding: '12px 16px', fontSize: 11.5 }}>
                  <div><span style={{ color: 'var(--ember)' }}>job</span>: <span style={{ color: 'var(--jade)' }}>{job.id}</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>jar</span>: <span style={{ color: 'var(--jade)' }}>"s3://acme-flink/jars/{job.id}-1.4.2.jar"</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>entry</span>: <span style={{ color: 'var(--jade)' }}>"com.acme.stream.OrdersEnrich"</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>parallelism</span>: <span style={{ color: 'var(--violet)' }}>{job.par}</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>checkpoint.interval</span>: <span style={{ color: 'var(--violet)' }}>30s</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>checkpoint.mode</span>: <span style={{ color: 'var(--jade)' }}>exactly_once</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>state.backend</span>: <span style={{ color: 'var(--jade)' }}>rocksdb</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>restart.strategy</span>: <span style={{ color: 'var(--jade)' }}>fixed-delay</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>source.topics</span>: [<span style={{ color: 'var(--jade)' }}>"orders.v2"</span>]</div>
                  <div><span style={{ color: 'var(--ember)' }}>sink.topic</span>: <span style={{ color: 'var(--jade)' }}>"orders.enriched"</span></div>
                </div>
              </div>
              <div className="term">
                <div className="term-head"><span className="lights"><i /><i /><i /></span><span>cli equivalent</span></div>
                <div className="term-body" style={{ padding: '12px 16px', fontSize: 11.5 }}>
                  <div><span className="prompt">$</span> rafka <span className="arg">flink submit</span> <span className="flag">-f</span> <span className="num">./{job.id}.yaml</span></div>
                  <div><span className="dim"># compiling… plan ok · 5 vertices</span></div>
                  <div><span className="ok">✓</span> job <span style={{ color: 'var(--rust)' }}>{job.id}</span> submitted · jobid 7c4a91e</div>
                  <div style={{ marginTop: 8 }}><span className="prompt">$</span> rafka <span className="arg">flink savepoint</span> {job.id} <span className="flag">--target</span> <span className="num">s3://acme-flink/sp</span></div>
                  <div><span className="dim"># triggering savepoint…</span></div>
                  <div><span className="ok">✓</span> savepoint <span style={{ color: 'var(--ember)' }}>sp-3812</span> · 342 MB · 2.1s</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Shell>
  );
}

Object.assign(window, { FlinkJobs });
