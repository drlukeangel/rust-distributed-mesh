// Rafka — SQL editor (streaming SQL → Flink jobs)

function SqlEditor() {
  const [tab, setTab] = React.useState(0);
  const queryTabs = [
    { id: 'q1', name: 'orders_enrich.sql',        dirty: true,  state: 'running' },
    { id: 'q2', name: 'sessionize_click.sql',     dirty: false, state: 'saved' },
    { id: 'q3', name: 'untitled-3',               dirty: true,  state: 'draft' },
  ];

  // Schema browser
  const [catOpen, setCatOpen] = React.useState({ orders: true, payments: true, click: false });
  const cats = [
    { id: 'orders', label: 'orders', tables: [
      { nm: 'orders.v2',          kind: 'topic', cols: '12 cols · keyed' },
      { nm: 'orders.enriched',    kind: 'view',  cols: '18 cols · materialized' },
      { nm: 'orders.fulfillment', kind: 'topic', cols: '9 cols' },
    ]},
    { id: 'payments', label: 'payments', tables: [
      { nm: 'payments.events',  kind: 'topic',   cols: '14 cols · keyed' },
      { nm: 'payments.audit',   kind: 'topic',   cols: '7 cols' },
      { nm: 'payments.refunds', kind: 'topic',   cols: '8 cols' },
    ]},
    { id: 'click', label: 'click', tables: [
      { nm: 'clickstream.raw',    kind: 'topic', cols: 'json' },
      { nm: 'clickstream.parsed', kind: 'topic', cols: '23 cols' },
    ]},
    { id: 'lookup', label: 'lookup · jdbc', tables: [
      { nm: 'customer',  kind: 'jdbc', cols: 'pg · 42 cols' },
      { nm: 'sku',       kind: 'jdbc', cols: 'pg · 16 cols' },
    ]},
  ];

  // Editor — orders_enrich.sql with line-by-line tokens
  const lines = [
    [{ c: 'com', t: '-- enrich orders.v2 with customer + sku tier, window 1m' }],
    [{ c: 'kw',  t: 'CREATE TEMPORARY VIEW' }, { t: ' ' }, { c: 'id', t: 'orders_enriched' }, { t: ' ' }, { c: 'kw', t: 'AS' }],
    [{ c: 'kw',  t: 'SELECT' }],
    [{ t: '  o.' }, { c: 'col', t: 'order_id' }, { t: ',  o.' }, { c: 'col', t: 'customer_id' }, { t: ',  o.' }, { c: 'col', t: 'sku' }, { t: ',  o.' }, { c: 'col', t: 'qty' }, { t: ',' }],
    [{ t: '  c.' }, { c: 'col', t: 'segment' }, { t: ',   s.' }, { c: 'col', t: 'tier' }, { t: ',          o.' }, { c: 'col', t: 'amount_cents' }, { t: ',' }],
    [{ c: 'fn',  t: '  TUMBLE_START' }, { t: '(o.' }, { c: 'col', t: 'event_time' }, { t: ', ' }, { c: 'fn', t: 'INTERVAL' }, { t: ' ' }, { c: 'str', t: "'1' MINUTE" }, { t: ') ' }, { c: 'kw', t: 'AS' }, { t: ' ' }, { c: 'col', t: 'win_start' }],
    [{ c: 'kw',  t: 'FROM' }, { t: ' ' }, { c: 'id', t: 'orders.v2' }, { t: ' o' }],
    [{ c: 'kw',  t: 'LEFT JOIN' }, { t: ' ' }, { c: 'id', t: 'lookup.customer' }, { c: 'kw', t: ' FOR SYSTEM_TIME AS OF' }, { t: ' o.' }, { c: 'col', t: 'event_time' }, { t: ' c' }],
    [{ t: '  ' }, { c: 'kw', t: 'ON' }, { t: ' o.' }, { c: 'col', t: 'customer_id' }, { t: ' = c.' }, { c: 'col', t: 'id' }],
    [{ c: 'kw',  t: 'LEFT JOIN' }, { t: ' ' }, { c: 'id', t: 'lookup.sku' }, { t: ' s ' }, { c: 'kw', t: 'ON' }, { t: ' o.' }, { c: 'col', t: 'sku' }, { t: ' = s.' }, { c: 'col', t: 'sku' }],
    [{ c: 'kw',  t: 'WHERE' }, { t: ' o.' }, { c: 'col', t: 'amount_cents' }, { t: ' > ' }, { c: 'num', t: '0' }],
    [{ c: 'kw',  t: 'GROUP BY' }, { t: ' ' }, { c: 'fn', t: 'TUMBLE' }, { t: '(o.' }, { c: 'col', t: 'event_time' }, { t: ', ' }, { c: 'fn', t: 'INTERVAL' }, { t: ' ' }, { c: 'str', t: "'1' MINUTE" }, { t: '), o.' }, { c: 'col', t: 'order_id' }, { t: ',' }],
    [{ t: '         o.' }, { c: 'col', t: 'customer_id' }, { t: ', o.' }, { c: 'col', t: 'sku' }, { t: ', o.' }, { c: 'col', t: 'qty' }, { t: ', c.' }, { c: 'col', t: 'segment' }, { t: ', s.' }, { c: 'col', t: 'tier' }, { t: ', o.' }, { c: 'col', t: 'amount_cents' }, { t: ';' }],
    [{ t: '' }],
    [{ c: 'kw',  t: 'INSERT INTO' }, { t: ' ' }, { c: 'id', t: 'orders.enriched' }],
    [{ c: 'kw',  t: 'SELECT' }, { t: ' * ' }, { c: 'kw', t: 'FROM' }, { t: ' ' }, { c: 'id', t: 'orders_enriched' }, { t: ';' }],
  ];

  // Results stream rows
  const rows = [
    { off: '4,128,902', t: '14:22:18.402', oid: 'ord_8f1a', cid: 'cust_2188', sku: 'SKU-2317', qty: 2,  seg: 'premium', tier: 'A', amt: 19_980 },
    { off: '4,128,903', t: '14:22:18.418', oid: 'ord_8f1b', cid: 'cust_0421', sku: 'SKU-1009', qty: 1,  seg: 'core',    tier: 'B', amt:  4_900 },
    { off: '4,128,904', t: '14:22:18.461', oid: 'ord_8f1c', cid: 'cust_3304', sku: 'SKU-2317', qty: 3,  seg: 'premium', tier: 'A', amt: 29_970 },
    { off: '4,128,905', t: '14:22:18.490', oid: 'ord_8f1d', cid: 'cust_7712', sku: 'SKU-0044', qty: 1,  seg: 'core',    tier: 'C', amt:  1_290 },
    { off: '4,128,906', t: '14:22:18.522', oid: 'ord_8f1e', cid: 'cust_1119', sku: 'SKU-0931', qty: 6,  seg: 'whale',   tier: 'A', amt: 71_400 },
    { off: '4,128,907', t: '14:22:18.544', oid: 'ord_8f1f', cid: 'cust_0034', sku: 'SKU-2317', qty: 2,  seg: 'core',    tier: 'B', amt: 19_980 },
    { off: '4,128,908', t: '14:22:18.581', oid: 'ord_8f20', cid: 'cust_4082', sku: 'SKU-1009', qty: 1,  seg: 'core',    tier: 'B', amt:  4_900 },
    { off: '4,128,909', t: '14:22:18.604', oid: 'ord_8f21', cid: 'cust_2188', sku: 'SKU-0044', qty: 4,  seg: 'premium', tier: 'C', amt:  5_160 },
    { off: '4,128,910', t: '14:22:18.638', oid: 'ord_8f22', cid: 'cust_9981', sku: 'SKU-0931', qty: 1,  seg: 'core',    tier: 'A', amt: 11_900 },
    { off: '4,128,911', t: '14:22:18.661', oid: 'ord_8f23', cid: 'cust_3304', sku: 'SKU-2317', qty: 2,  seg: 'premium', tier: 'A', amt: 19_980 },
  ];

  const [resTab, setResTab] = React.useState('results');

  return (
    <Shell
      active="sql"
      breadcrumb={['acme', 'prod', 'us-east-2', 'sql']}
      title="streaming sql"
      actions={<>
        <button className="btn ghost">Catalog</button>
        <button className="btn ghost">Saved · 18</button>
        <button className="btn primary">+ New query</button>
      </>}
    >
      <div className="panel" style={{ padding: 0 }}>
        <div className="sq-grid">
          {/* ── Schema sidebar ── */}
          <div className="sq-schema">
            <div className="sq-schema-h">
              <span>catalog · acme-prod</span>
              <span className="dim mono">42</span>
            </div>
            <div className="sq-schema-search">
              <Icon name="search" />
              <input placeholder="filter tables…" />
            </div>
            {cats.map((cat) => (
              <div key={cat.id} className="sq-cat">
                <div className="sq-cat-h" onClick={() => setCatOpen({ ...catOpen, [cat.id]: !catOpen[cat.id] })}>
                  <span className="caret" style={{ transform: catOpen[cat.id] ? 'rotate(90deg)' : 'none' }}>▸</span>
                  <span className="nm">{cat.label}</span>
                  <span className="ct mono">{cat.tables.length}</span>
                </div>
                {catOpen[cat.id] && cat.tables.map((tb) => (
                  <div key={tb.nm} className="sq-tbl">
                    <span className={'kind ' + tb.kind}>{tb.kind === 'topic' ? 'T' : tb.kind === 'view' ? 'V' : 'J'}</span>
                    <span className="nm">{tb.nm}</span>
                    <span className="meta mono">{tb.cols}</span>
                  </div>
                ))}
              </div>
            ))}
          </div>

          {/* ── Editor + results ── */}
          <div className="sq-main">
            {/* Tab strip */}
            <div className="sq-tabs">
              {queryTabs.map((q, i) => (
                <div key={q.id} className={'sq-tab' + (i === tab ? ' on' : '')} onClick={() => setTab(i)}>
                  <span className={'dot ' + (q.state === 'running' ? 'jade' : q.state === 'draft' ? 'amber' : '')} />
                  <span className="nm">{q.name}</span>
                  {q.dirty && <span className="dirty">●</span>}
                  <span className="x">×</span>
                </div>
              ))}
              <div className="sq-tab add">＋</div>
              <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 8, padding: '0 14px' }}>
                <span className="pill jade"><span className="dot" />job · orders-enrich</span>
                <span className="mono dim" style={{ fontSize: 11 }}>par 8 · ck-4218 ok</span>
              </div>
            </div>

            {/* Editor toolbar */}
            <div className="sq-tools">
              <button className="btn primary" style={{ height: 28, padding: '0 12px', fontSize: 12 }}>▶ Run</button>
              <button className="btn ghost" style={{ height: 28, padding: '0 10px', fontSize: 12 }}>Explain</button>
              <button className="btn ghost" style={{ height: 28, padding: '0 10px', fontSize: 12 }}>Submit as job</button>
              <span className="sep" />
              <div className="seg">
                <span className="seg-i on">streaming</span>
                <span className="seg-i">batch</span>
              </div>
              <span className="sep" />
              <span className="mono dim" style={{ fontSize: 11 }}>flink-sql 1.18 · catalog: acme-prod · format: avro</span>
              <span style={{ marginLeft: 'auto' }} className="mono dim">ln 16 · col 22 · ⌘↵ run</span>
            </div>

            {/* Editor body */}
            <div className="sq-editor">
              <div className="gutter">
                {lines.map((_, i) => (
                  <div key={i} className={'ln' + ((i === 5 || i === 11) ? ' hl' : '')}>{i + 1}</div>
                ))}
              </div>
              <div className="code">
                {lines.map((toks, i) => (
                  <div key={i} className="ln">
                    {toks.map((tk, k) => (
                      <span key={k} className={tk.c ? 'sq-' + tk.c : ''}>{tk.t}</span>
                    ))}
                  </div>
                ))}
                {/* caret */}
                <div className="caret" style={{ top: '15.6em', left: '22ch' }}></div>
                {/* autocomplete popover */}
                <div className="autocomp">
                  <div className="ac-i sel"><span className="ac-k">F</span><span className="ac-n">TUMBLE_END</span><span className="ac-t">window fn</span></div>
                  <div className="ac-i"><span className="ac-k">F</span><span className="ac-n">TUMBLE_ROWTIME</span><span className="ac-t">window fn</span></div>
                  <div className="ac-i"><span className="ac-k">F</span><span className="ac-n">TUMBLE_PROCTIME</span><span className="ac-t">window fn</span></div>
                  <div className="ac-i"><span className="ac-k">C</span><span className="ac-n">customer_id</span><span className="ac-t">o · BIGINT</span></div>
                  <div className="ac-i"><span className="ac-k">C</span><span className="ac-n">cancel_reason</span><span className="ac-t">o · STRING</span></div>
                  <div className="ac-foot">↑↓ navigate · ↵ accept · esc close</div>
                </div>
              </div>
            </div>

            {/* Results pane */}
            <div className="sq-res">
              <div className="sq-res-h">
                <div className="sq-res-tabs">
                  <span className={'tab' + (resTab === 'results' ? ' on' : '')} onClick={() => setResTab('results')}>
                    results <span className="mono dim">· streaming</span>
                  </span>
                  <span className={'tab' + (resTab === 'plan' ? ' on' : '')} onClick={() => setResTab('plan')}>plan</span>
                  <span className={'tab' + (resTab === 'logs' ? ' on' : '')} onClick={() => setResTab('logs')}>logs</span>
                  <span className={'tab' + (resTab === 'job' ? ' on' : '')} onClick={() => setResTab('job')}>job</span>
                  <span className={'tab' + (resTab === 'history' ? ' on' : '')} onClick={() => setResTab('history')}>history</span>
                </div>
                <div className="sq-res-meta">
                  <span className="pill jade"><span className="dot" />live</span>
                  <span className="mono">142.4k rows/s</span>
                  <span className="mono dim">· elapsed 02:14</span>
                  <span className="mono dim">· emitted 19.2M</span>
                  <button className="btn ghost" style={{ height: 24, padding: '0 8px', fontSize: 11 }}>⏸ pause</button>
                  <button className="btn ghost" style={{ height: 24, padding: '0 8px', fontSize: 11 }}>export</button>
                </div>
              </div>

              <div className="sq-res-body">
                <div className="sq-tbl-head">
                  <div className="th mono">#offset</div>
                  <div className="th mono">event_time</div>
                  <div className="th mono">order_id</div>
                  <div className="th mono">customer_id</div>
                  <div className="th mono">sku</div>
                  <div className="th mono r">qty</div>
                  <div className="th mono">segment</div>
                  <div className="th mono">tier</div>
                  <div className="th mono r">amount</div>
                </div>
                {rows.map((r, i) => (
                  <div key={r.off} className={'sq-tbl-row' + (i === 4 ? ' fresh' : '')}>
                    <div className="td mono dim">{r.off}</div>
                    <div className="td mono">{r.t}</div>
                    <div className="td mono">{r.oid}</div>
                    <div className="td mono">{r.cid}</div>
                    <div className="td mono">{r.sku}</div>
                    <div className="td mono r">{r.qty}</div>
                    <div className="td"><span className={'pill ' + (r.seg === 'whale' ? 'rust' : r.seg === 'premium' ? 'ice' : '')}
                                              style={{ height: 16, padding: '0 6px', fontSize: 10 }}>{r.seg}</span></div>
                    <div className="td mono">{r.tier}</div>
                    <div className="td mono r">${(r.amt / 100).toFixed(2)}</div>
                  </div>
                ))}
                <div className="sq-tbl-stream">
                  <span className="live"><i />streaming · new rows append below</span>
                  <span className="mono dim">watermark 14:22:14.802 · lag 3.6s</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Shell>
  );
}

Object.assign(window, { SqlEditor });
