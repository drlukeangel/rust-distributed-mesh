// Rafka — Connectors

function Connectors() {
  const [sel, setSel] = React.useState('snowflake');
  const cats = [
    { id: 'postgres',  lg: 'PG', cls: 'pg',  nm: 'PostgreSQL CDC',   meta: 'source · debezium',     kind: 'src',  inst: 4 },
    { id: 's3',        lg: 'S3', cls: 's3',  nm: 'S3 Sink',          meta: 'sink · aws',            kind: 'sink', inst: 6 },
    { id: 'snowflake', lg: 'SF', cls: 'sf',  nm: 'Snowflake',        meta: 'sink · ingest service', kind: 'sink', inst: 3 },
    { id: 'mongo',     lg: 'MG', cls: 'mg',  nm: 'MongoDB CDC',      meta: 'source · change stream',kind: 'src',  inst: 2 },
    { id: 'es',        lg: 'ES', cls: 'es',  nm: 'Elasticsearch',    meta: 'sink · bulk api',       kind: 'sink', inst: 4 },
    { id: 'bq',        lg: 'BQ', cls: 'bq',  nm: 'BigQuery',         meta: 'sink · storage write',  kind: 'sink', inst: 2 },
    { id: 'mysql',     lg: 'MY', cls: 'my',  nm: 'MySQL CDC',        meta: 'source · binlog',       kind: 'src',  inst: 3 },
    { id: 'kinesis',   lg: 'KS', cls: 'ks',  nm: 'Kinesis Bridge',   meta: 'source · aws',          kind: 'src',  inst: 1 },
    { id: 'redis',     lg: 'RD', cls: 'rd',  nm: 'Redis Stream',     meta: 'sink · xadd',           kind: 'sink', inst: 0 },
    { id: 'http',      lg: 'WH', cls: 'wh',  nm: 'HTTP Webhook',     meta: 'sink · generic',        kind: 'sink', inst: 2 },
    { id: 'kafka',     lg: 'KF', cls: 'kf',  nm: 'Kafka Mirror',     meta: 'source · mm2',          kind: 'src',  inst: 1 },
    { id: 'dynamo',    lg: 'DY', cls: 'dy',  nm: 'DynamoDB Streams', meta: 'source · aws',          kind: 'src',  inst: 0 },
  ];
  const cur = cats.find((c) => c.id === sel) || cats[0];

  const instances = {
    snowflake: [
      { id: 'sf-orders-prod',     state: 'running',  tasks: '4/4', src: 'orders.v2 · payments.events', rate: '12.4k', err: '0',  cls: 'jade' },
      { id: 'sf-clickstream-fact',state: 'degraded', tasks: '7/8', src: 'clickstream.parsed',           rate: '178k',  err: '142',cls: 'amber' },
      { id: 'sf-audit-cold',      state: 'paused',   tasks: '0/2', src: 'payments.audit',               rate: '0',     err: '0',  cls: '' },
    ],
    postgres: [
      { id: 'pg-billing-cdc',  state: 'running', tasks: '2/2', src: 'billing.* (8 tables)',  rate: '420',   err: '0',  cls: 'jade' },
      { id: 'pg-catalog-cdc',  state: 'running', tasks: '4/4', src: 'catalog.* (24 tables)', rate: '1.2k',  err: '0',  cls: 'jade' },
      { id: 'pg-fulfill-cdc',  state: 'failed',  tasks: '0/2', src: 'fulfillment.*',         rate: '—',     err: '1',  cls: 'crimson' },
      { id: 'pg-legacy-cdc',   state: 'running', tasks: '2/2', src: 'legacy.*',              rate: '88',    err: '0',  cls: 'jade' },
    ],
  };
  const list = instances[sel] || instances.snowflake;
  const [selInst, setSelInst] = React.useState(0);
  const inst = list[Math.min(selInst, list.length - 1)] || list[0];

  // 8 tasks for selected instance
  const tasks = Array.from({ length: 8 }, (_, i) => {
    const totalRate = 178;
    const r = (totalRate / 8) * (0.85 + Math.sin(i * 1.3) * 0.18);
    const err = i === 5 ? 142 : 0;
    const cls = err > 0 ? 'warn' : '';
    return { i, rate: r.toFixed(1), err, cls };
  });

  return (
    <Shell
      active="connectors"
      breadcrumb={['acme', 'prod', 'us-east-2', 'connectors']}
      title="connectors"
      actions={<>
        <button className="btn ghost">Catalog</button>
        <button className="btn primary">+ New connector</button>
      </>}
    >
      <div className="panel" style={{ padding: 0 }}>
        <div className="cx-split">
          <div className="cx-cat">
            <div className="h"><span>catalog · 32 types</span><span className="count">12 active</span></div>
            <div style={{ padding: '10px 14px', display: 'flex', gap: 6, flexWrap: 'wrap', borderBottom: '1px solid var(--line-1)' }}>
              <span className="chip on">all</span>
              <span className="chip">sources</span>
              <span className="chip">sinks</span>
              <span className="chip">aws</span>
              <span className="chip">db cdc</span>
            </div>
            {cats.map((c) => (
              <div key={c.id} className={'cx-tile' + (c.id === sel ? ' sel' : '')} onClick={() => { setSel(c.id); setSelInst(0); }}>
                <div className="lg">{c.lg}</div>
                <div className="nm">{c.nm}</div>
                <div className="meta">{c.meta}  ·  {c.inst} running</div>
                <div className={'badge ' + (c.kind === 'src' ? 'src' : 'sink')}>{c.kind === 'src' ? 'source' : 'sink'}</div>
              </div>
            ))}
          </div>

          <div className="cx-detail">
            <div className="cx-hero">
              <div className="lg-big">{cur.lg}</div>
              <div style={{ flex: 1 }}>
                <h2>{cur.nm}</h2>
                <div className="desc">{cur.kind === 'src' ? 'Stream changes from this system into Rafka topics. Schema is inferred and registered automatically.' : 'Deliver topic data into this destination with at-least-once or exactly-once semantics depending on plugin support.'}</div>
                <div className="tags">
                  <span className="tag">{cur.kind === 'src' ? 'source' : 'sink'}</span>
                  <span className="tag">official · acme/rafka</span>
                  <span className="tag">v2.4.1</span>
                  <span className="tag">exactly-once</span>
                  <span className="tag">{list.length} instance{list.length === 1 ? '' : 's'}</span>
                </div>
              </div>
              <button className="btn primary">+ New instance</button>
            </div>

            {/* Instances */}
            <div>
              <div className="cx-inst">
                <div className="th l">instance</div>
                <div className="th l">source · topics</div>
                <div className="th">tasks</div>
                <div className="th r">throughput</div>
                <div className="th r">errors · 1h</div>
                <div className="th">state</div>
                <div className="th"></div>

                {list.map((it, i) => (
                  <div key={it.id} className={'row' + (i === selInst ? ' sel' : '')} onClick={() => setSelInst(i)}>
                    <div className="td"><span className="nm"><span style={{ width: 6, height: 6, borderRadius: 50, background: it.cls ? `var(--${it.cls})` : 'var(--ink-4)', display: 'inline-block' }}></span>{it.id}</span></div>
                    <div className="td mono" style={{ color: 'var(--ink-2)', fontSize: 11.5 }}>{it.src}</div>
                    <div className="td mono">{it.tasks}</div>
                    <div className="td mono r">{it.rate}{it.rate !== '—' && it.rate !== '0' && ' /s'}</div>
                    <div className="td mono r" style={{ color: it.err === '0' ? 'var(--ink-3)' : 'var(--amber)' }}>{it.err}</div>
                    <div className="td"><span className={'pill ' + it.cls}>{it.cls && <span className="dot" />}{it.state}</span></div>
                    <div className="td" style={{ justifyContent: 'flex-end' }}>
                      <button className="btn ghost" style={{ height: 26, padding: '0 8px', fontSize: 11 }}>···</button>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Task grid for selected instance */}
            <div className="cx-tasks">
              <div style={{ display: 'flex', alignItems: 'center', gap: 14, marginBottom: 12 }}>
                <h3 style={{ margin: 0 }}>task pool · {inst.id}</h3>
                <span className={'pill ' + inst.cls}>{inst.cls && <span className="dot" />}{inst.state}</span>
                <span style={{ marginLeft: 'auto', fontFamily: 'JetBrains Mono, monospace', fontSize: 11, color: 'var(--ink-3)' }}>{inst.tasks} tasks  ·  rebalance: 4m ago</span>
              </div>
              <div className="cx-task-grid">
                {tasks.map((t) => (
                  <div key={t.i} className={'cx-task ' + t.cls}>
                    <div className="id">task-{t.i.toString().padStart(2, '0')}</div>
                    <div className="v">{t.rate}<span style={{ fontSize: 10, color: 'var(--ink-3)', fontWeight: 400, marginLeft: 4 }}>k/s</span></div>
                    <div className="bar"><i style={{ width: (60 + Math.sin(t.i) * 25) + '%' }}></i></div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontFamily: 'JetBrains Mono, monospace', fontSize: 10, color: 'var(--ink-3)' }}>
                      <span>offset ↑{(412317 + t.i * 2031).toString().slice(-6)}</span>
                      <span style={{ color: t.err > 0 ? 'var(--amber)' : 'var(--ink-4)' }}>err {t.err}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Config preview + CLI */}
            <div style={{ padding: '4px 28px 28px', display: 'grid', gridTemplateColumns: '1.2fr 1fr', gap: 16 }}>
              <div className="term">
                <div className="term-head"><span className="lights"><i /><i /><i /></span><span>{inst.id} · config.yaml</span><span style={{ marginLeft: 'auto', color: 'var(--ink-3)' }}>edit ⌘E</span></div>
                <div className="term-body" style={{ padding: '12px 16px', fontSize: 11.5 }}>
                  <div><span className="key" style={{ color: 'var(--ember)' }}>connector</span>: <span style={{ color: 'var(--jade)' }}>snowflake-sink</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>tasks.max</span>: <span style={{ color: 'var(--violet)' }}>8</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>topics</span>: [<span style={{ color: 'var(--jade)' }}>"clickstream.parsed"</span>]</div>
                  <div><span style={{ color: 'var(--ember)' }}>snowflake.url</span>: <span style={{ color: 'var(--jade)' }}>"acme-prod.snowflakecomputing.com"</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>snowflake.database</span>: <span style={{ color: 'var(--jade)' }}>"events_raw"</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>snowflake.schema</span>: <span style={{ color: 'var(--jade)' }}>"click"</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>buffer.flush.time</span>: <span style={{ color: 'var(--violet)' }}>60</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>buffer.size.bytes</span>: <span style={{ color: 'var(--violet)' }}>5_000_000</span></div>
                  <div><span style={{ color: 'var(--ember)' }}>delivery.guarantee</span>: <span style={{ color: 'var(--jade)' }}>exactly_once</span></div>
                  <div><span className="dim"># key.converter set at worker level</span></div>
                </div>
              </div>
              <div className="term">
                <div className="term-head"><span className="lights"><i /><i /><i /></span><span>cli equivalent</span></div>
                <div className="term-body" style={{ padding: '12px 16px', fontSize: 11.5 }}>
                  <div><span className="prompt">$</span> rafka <span className="arg">connect deploy</span> <span className="flag">-f</span> <span className="num">./{inst.id}.yaml</span></div>
                  <div><span className="dim"># validating… ok</span></div>
                  <div><span className="dim"># reserved 8 task slots on workers w-1, w-2, w-4</span></div>
                  <div><span className="ok">✓</span> connector <span style={{ color: 'var(--rust)' }}>{inst.id}</span> deployed</div>
                  <div style={{ marginTop: 8 }}><span className="prompt">$</span> rafka <span className="arg">connect logs</span> {inst.id} <span className="flag">--task</span> <span className="num">5</span> <span className="flag">--since</span> <span className="num">15m</span></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Shell>
  );
}

Object.assign(window, { Connectors });
