// Rafka — Schema registry

function SchemaRegistry() {
  const [sel, setSel] = React.useState('orders-value');

  const subjects = [
    { n: 'orders-value',        fmt: 'avro',     v: 5,  compat: 'BACKWARD',         updated: '2d',  topics: 1 },
    { n: 'orders-key',          fmt: 'avro',     v: 1,  compat: 'BACKWARD',         updated: '34d', topics: 1 },
    { n: 'payments.events-value', fmt: 'protobuf', v: 8, compat: 'BACKWARD_TRANS',   updated: '6h',  topics: 1 },
    { n: 'payments.events-key', fmt: 'avro',     v: 2,  compat: 'BACKWARD',         updated: '14d', topics: 1 },
    { n: 'inventory.updates-value', fmt: 'avro', v: 3,  compat: 'FORWARD',          updated: '1d',  topics: 1 },
    { n: 'clickstream.raw-value', fmt: 'json',   v: 12, compat: 'NONE',             updated: '1h',  topics: 1 },
    { n: 'risk.signals-value',  fmt: 'protobuf', v: 4,  compat: 'FULL',             updated: '5d',  topics: 1 },
    { n: 'auth.signins-value',  fmt: 'avro',     v: 2,  compat: 'BACKWARD',         updated: '12d', topics: 1 },
    { n: 'logistics.events-value', fmt: 'avro', v: 7,  compat: 'BACKWARD',         updated: '3d',  topics: 1 },
  ];
  const cur = subjects.find((s) => s.n === sel) || subjects[0];

  const versions = [
    { v: 1, when: '2025-09-04', by: 'm.singh',   status: 'ok',    note: 'initial' },
    { v: 2, when: '2025-11-18', by: 'm.singh',   status: 'ok',    note: '+ region' },
    { v: 3, when: '2026-01-22', by: 'j.lee',     status: 'ok',    note: '+ discount_cents' },
    { v: 4, when: '2026-03-11', by: 'p.tanaka',  status: 'break', note: 'rolled back' },
    { v: 5, when: '2026-05-08', by: 'j.lee',     status: 'cur',   note: '+ promo_code · nullable' },
  ];

  // Diff lines
  const prev = [
    { t: '{' },
    { t: '  "type": "record",', k: { 'type': 'record' } },
    { t: '  "name": "OrderEvent",' },
    { t: '  "namespace": "com.acme.orders",' },
    { t: '  "fields": [' },
    { t: '    { "name": "order_id",      "type": "string" },' },
    { t: '    { "name": "customer_id",   "type": "string" },' },
    { t: '    { "name": "total_cents",   "type": "long" },' },
    { t: '    { "name": "currency",      "type": "string" },' },
    { t: '    { "name": "items",         "type": "int" },' },
    { t: '    { "name": "region",        "type": "string" },' },
    { t: '    { "name": "discount_cents","type": "long",   "default": 0 },' },
    { t: '    { "name": "occurred_at",   "type": "long",   "logicalType": "timestamp-millis" }' },
    { t: '  ]' },
    { t: '}' },
  ];
  const next = [
    { t: '{' },
    { t: '  "type": "record",' },
    { t: '  "name": "OrderEvent",' },
    { t: '  "namespace": "com.acme.orders",' },
    { t: '  "fields": [' },
    { t: '    { "name": "order_id",      "type": "string" },' },
    { t: '    { "name": "customer_id",   "type": "string" },' },
    { t: '    { "name": "total_cents",   "type": "long" },' },
    { t: '    { "name": "currency",      "type": "string" },' },
    { t: '    { "name": "items",         "type": "int" },' },
    { t: '    { "name": "region",        "type": "string" },' },
    { t: '    { "name": "discount_cents","type": "long",   "default": 0 },' },
    { t: '    { "name": "promo_code",    "type": ["null","string"], "default": null },', mark: 'add' },
    { t: '    { "name": "occurred_at",   "type": "long",   "logicalType": "timestamp-millis" }' },
    { t: '  ]' },
    { t: '}' },
  ];

  // Tiny tokenizer for json-ish lines
  const tok = (s) => {
    const out = [];
    let i = 0, key = 0;
    const re = /"([^"]+)"|\b(true|false|null)\b|\b(-?\d+(?:\.\d+)?)\b|([{}\[\],:])|(\/\/.*$)|\s+/g;
    let m, last = 0;
    while ((m = re.exec(s))) {
      if (m.index > last) out.push(s.slice(last, m.index));
      if (m[1]) {
        // Distinguish keys vs strings: a key is followed by ':' (after optional whitespace)
        const rest = s.slice(re.lastIndex).trimStart();
        if (rest.startsWith(':')) out.push(<span key={key++} className="key">"{m[1]}"</span>);
        else out.push(<span key={key++} className="str">"{m[1]}"</span>);
      } else if (m[2]) out.push(<span key={key++} className="kw">{m[2]}</span>);
      else if (m[3]) out.push(<span key={key++} className="kw">{m[3]}</span>);
      else if (m[4]) out.push(m[4]);
      else if (m[5]) out.push(<span key={key++} className="com">{m[5]}</span>);
      else out.push(m[0]);
      last = re.lastIndex;
    }
    if (last < s.length) out.push(s.slice(last));
    return out;
  };

  return (
    <Shell
      active="schema"
      breadcrumb={['acme', 'prod', 'us-east-2', 'schema registry']}
      title="schema registry"
      actions={<>
        <button className="btn ghost">Compatibility check</button>
        <button className="btn primary">+ Register schema</button>
      </>}
    >
      <div className="panel" style={{ padding: 0 }}>
        <div className="sr-split">
          {/* ── Subject list ── */}
          <div className="sr-list">
            <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--line-1)', display: 'flex', gap: 8, alignItems: 'center' }}>
              <div style={{ flex: 1, display: 'flex', alignItems: 'center', gap: 8, height: 28, padding: '0 10px', background: 'var(--bg-1)', border: '1px solid var(--line-1)', borderRadius: 7, fontSize: 12, color: 'var(--ink-3)' }}>
                <Icon name="search" />
                <input placeholder="filter subjects" style={{ background: 'transparent', border: 0, outline: 0, color: 'var(--ink-1)', font: 'inherit', flex: 1, minWidth: 0 }} />
              </div>
              <span className="kbd">{subjects.length}</span>
            </div>
            <div style={{ padding: '8px 16px 4px', display: 'flex', gap: 6 }}>
              <span className="chip on">all</span>
              <span className="chip">avro</span>
              <span className="chip">protobuf</span>
              <span className="chip">json</span>
            </div>
            {subjects.map((s) => (
              <div key={s.n} className={'sr-li' + (s.n === sel ? ' sel' : '')} onClick={() => setSel(s.n)}>
                <div className="nm">{s.n}</div>
                <div className="meta">{s.fmt}  ·  {s.compat.toLowerCase()}  ·  {s.updated} ago</div>
                <div className="v"><b>v{s.v}</b>versions</div>
              </div>
            ))}
          </div>

          {/* ── Detail ── */}
          <div className="sr-detail">
            <div className="sr-hero">
              <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
                <h2 style={{ flex: 1, margin: 0 }}>{cur.n}</h2>
                <span className="pill rust" style={{ height: 22 }}><span className="dot" />v{cur.v} · current</span>
              </div>
              <div className="tags" style={{ marginTop: 10 }}>
                <span className="tag">{cur.fmt}</span>
                <span className="tag">id: 100{cur.v}3</span>
                <span className="tag">{cur.compat.toLowerCase()}</span>
                <span className="tag">used by 1 topic · {cur.n.replace(/-(value|key)$/, '')}</span>
                <span className="tag">updated {cur.updated} ago by j.lee</span>
              </div>
            </div>

            {/* Version timeline */}
            <div className="sr-timeline">
              {versions.map((vv) => (
                <div key={vv.v} className={'sr-tn ' + (vv.status === 'cur' ? 'cur' : vv.status === 'break' ? 'break' : 'ok')}>
                  <div className="v">v{vv.v}</div>
                  <div className="dot"></div>
                  <div className="when">{vv.when}</div>
                  <div className="lbl">{vv.note}</div>
                </div>
              ))}
              <div style={{ flex: 1 }}></div>
            </div>

            {/* Diff view */}
            <div className="sr-body">
              <div className="sr-pane">
                <div className="h">
                  <span>previous</span>
                  <span className="ver">v4</span>
                  <span style={{ marginLeft: 'auto', color: 'var(--ink-3)', textTransform: 'none', letterSpacing: 0, fontSize: 11 }}>p.tanaka · 2026-03-11</span>
                </div>
                <div className="sr-code">
                  {prev.map((ln, i) => (
                    <div key={i} className="ln">
                      <span className="num">{i + 1}</span>
                      <span className="gut"></span>
                      <span>{tok(ln.t)}</span>
                    </div>
                  ))}
                </div>
              </div>
              <div className="sr-pane">
                <div className="h">
                  <span>current</span>
                  <span className="ver">v5</span>
                  <span className="pill jade" style={{ height: 18, marginLeft: 'auto' }}><span className="dot" />backward-compatible</span>
                </div>
                <div className="sr-code">
                  {next.map((ln, i) => (
                    <div key={i} className={'ln' + (ln.mark === 'add' ? ' add' : ln.mark === 'rem' ? ' rem' : '')}>
                      <span className="num">{i + 1}</span>
                      <span className="gut">{ln.mark === 'add' ? '+' : ln.mark === 'rem' ? '−' : ''}</span>
                      <span>{tok(ln.t)}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Compatibility */}
            <div className="sr-compat">
              <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                <div className="h" style={{ margin: 0, fontFamily: 'JetBrains Mono, monospace', fontSize: 11, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: '0.1em' }}>compatibility mode</div>
                <span className="pill" style={{ height: 18 }}>subject-level override</span>
                <span style={{ marginLeft: 'auto', fontFamily: 'JetBrains Mono, monospace', fontSize: 11, color: 'var(--ink-3)' }}>cluster default · BACKWARD</span>
              </div>
              <div className="modes">
                <div className="mode"><div className="nm">NONE</div><div className="dx">no checks</div></div>
                <div className="mode on"><div className="nm">BACKWARD</div><div className="dx">new readers · old data</div></div>
                <div className="mode"><div className="nm">FORWARD</div><div className="dx">old readers · new data</div></div>
                <div className="mode"><div className="nm">FULL</div><div className="dx">both directions</div></div>
                <div className="mode"><div className="nm">BACKWARD_TRANS</div><div className="dx">all prior versions</div></div>
                <div className="mode"><div className="nm">FORWARD_TRANS</div><div className="dx">all future versions</div></div>
              </div>
            </div>

            {/* CLI footer */}
            <div style={{ padding: '12px 24px 22px' }}>
              <div className="term">
                <div className="term-head">
                  <span className="lights"><i /><i /><i /></span>
                  <span>register · cli equivalent</span>
                </div>
                <div className="term-body" style={{ padding: '10px 14px' }}>
                  <div><span className="prompt">$</span> rafka <span className="arg">schema register</span> {cur.n} <span className="flag">--file</span> <span className="num">./order-v5.avsc</span> <span className="flag">--compat</span> <span className="num">backward</span></div>
                  <div><span className="dim"># ok · registered as id 100{cur.v}3 (subject {cur.n} v{cur.v})</span></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Shell>
  );
}

Object.assign(window, { SchemaRegistry });
