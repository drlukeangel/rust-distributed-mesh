// Rafka — Topic Browser + Message Inspector

function TopicBrowser() {
  const [sel, setSel] = React.useState('orders.v2');
  const topics = [
    { n: 'orders.v2',          p: 24, r: '12.4k', l: '8.2ms',  lag: '0',    s: 'healthy',  c: 'jade' },
    { n: 'orders.v1',          p: 12, r: '0',     l: '—',      lag: '0',    s: 'deprecated', c: '' },
    { n: 'inventory.updates',  p: 12, r: '4.8k',  l: '6.1ms',  lag: '0',    s: 'healthy',  c: 'jade' },
    { n: 'inventory.snapshot', p: 6,  r: '120',   l: '4.2ms',  lag: '0',    s: 'healthy',  c: 'jade' },
    { n: 'payments.events',    p: 24, r: '8.2k',  l: '9.8ms',  lag: '1.2k', s: 'healthy',  c: 'jade' },
    { n: 'payments.dlq',       p: 6,  r: '0',     l: '—',      lag: '0',    s: 'idle',     c: '' },
    { n: 'payments.audit',     p: 12, r: '1.4k',  l: '5.1ms',  lag: '0',    s: 'healthy',  c: 'jade' },
    { n: 'clickstream.raw',    p: 64, r: '184k',  l: '11.3ms', lag: '312k', s: 'degraded', c: 'amber' },
    { n: 'clickstream.parsed', p: 64, r: '178k',  l: '9.9ms',  lag: '4.1k', s: 'healthy',  c: 'jade' },
    { n: 'risk.signals',       p: 12, r: '2.1k',  l: '4.7ms',  lag: '0',    s: 'live',     c: 'rust' },
    { n: 'risk.flagged',       p: 6,  r: '88',    l: '3.9ms',  lag: '0',    s: 'healthy',  c: 'jade' },
    { n: 'fraud.candidates',   p: 12, r: '412',   l: '5.3ms',  lag: '0',    s: 'healthy',  c: 'jade' },
    { n: 'logistics.events',   p: 24, r: '6.4k',  l: '7.7ms',  lag: '0',    s: 'healthy',  c: 'jade' },
    { n: 'auth.signins',       p: 12, r: '3.1k',  l: '4.4ms',  lag: '0',    s: 'healthy',  c: 'jade' },
    { n: '__consumer_offsets', p: 50, r: '—',     l: '—',      lag: '—',    s: 'internal', c: '' },
  ];

  const messages = [
    { off: 412317, ts: '18:42:11.218', key: 'ord_01HQX9K2N', body: '{"order_id":"ord_01HQX9K2N","customer":"cus_8F2Q3","total_cents":4299,"currency":"USD","items":3,"region":"us-east-2"}' },
    { off: 412316, ts: '18:42:11.214', key: 'ord_01HQX9K2M', body: '{"order_id":"ord_01HQX9K2M","customer":"cus_2R1Y8","total_cents":11900,"currency":"USD","items":1,"region":"us-east-2"}' },
    { off: 412315, ts: '18:42:11.198', key: 'ord_01HQX9K2L', body: '{"order_id":"ord_01HQX9K2L","customer":"cus_PP3M9","total_cents":680,"currency":"EUR","items":2,"region":"eu-west-1"}' },
    { off: 412314, ts: '18:42:11.181', key: 'ord_01HQX9K2K', body: '{"order_id":"ord_01HQX9K2K","customer":"cus_K7B4F","total_cents":24500,"currency":"USD","items":7,"region":"us-east-2"}' },
    { off: 412313, ts: '18:42:11.162', key: 'ord_01HQX9K2J', body: '{"order_id":"ord_01HQX9K2J","customer":"cus_M1Q8P","total_cents":1299,"currency":"USD","items":1,"region":"us-east-2"}' },
    { off: 412312, ts: '18:42:11.145', key: 'ord_01HQX9K2I', body: '{"order_id":"ord_01HQX9K2I","customer":"cus_QQ7T2","total_cents":7820,"currency":"USD","items":4,"region":"us-east-2"}' },
  ];

  // Tiny JSON colorizer
  const renderBody = (s) => {
    const parts = [];
    let i = 0;
    const re = /"([^"]+)"\s*:|"([^"]+)"|\b(-?\d+(?:\.\d+)?)\b|([{}\[\],:])/g;
    let m, last = 0;
    while ((m = re.exec(s))) {
      if (m.index > last) parts.push(s.slice(last, m.index));
      if (m[1]) parts.push(<span key={i++} className="k">"{m[1]}"</span>, ':');
      else if (m[2]) parts.push(<span key={i++} className="s">"{m[2]}"</span>);
      else if (m[3]) parts.push(<span key={i++} className="n">{m[3]}</span>);
      else if (m[4]) parts.push(m[4]);
      last = re.lastIndex;
    }
    if (last < s.length) parts.push(s.slice(last));
    return parts;
  };

  const selTopic = topics.find((t) => t.n === sel) || topics[0];

  return (
    <Shell
      active="topics"
      breadcrumb={['acme', 'prod', 'us-east-2', 'topics']}
      title="topics"
      actions={<>
        <button className="btn ghost">Import schema</button>
        <button className="btn primary">Create topic</button>
      </>}
    >
      <div className="panel" style={{ padding: 0, height: 'calc(100vh - 56px - 24px - 56px - 60px)' }}>
        <div className="filter-bar">
          <div className="search-i">
            <Icon name="search" />
            <input placeholder="filter topics · supports regex (e.g. orders\..*)" defaultValue="" />
            <span className="kbd">/</span>
          </div>
          <div className="chips">
            <span className="chip on">all · 142</span>
            <span className="chip">live · 89</span>
            <span className="chip">degraded · 2</span>
            <span className="chip">idle · 12</span>
            <span className="chip">internal · 4</span>
          </div>
          <div style={{ flex: 1 }} />
          <span className="mono" style={{ fontSize: 11, color: 'var(--ink-3)' }}>sorted by msg/s · ▼</span>
        </div>

        <div className="split">
          <div className="col-list" style={{ overflowY: 'auto' }}>
            <div className="topic-table">
              <div className="th"></div>
              <div className="th">name</div>
              <div className="th r">partitions</div>
              <div className="th r">msg/s</div>
              <div className="th r">p99</div>
              <div className="th r">lag</div>
              <div className="th">trend · 1h</div>
              <div className="th">status</div>

              {topics.map((t) => {
                const trend = Array.from({ length: 20 }, (_, i) => 30 + Math.sin(i / 3 + t.n.length) * 12 + (t.n === 'clickstream.raw' ? i * 1.5 : 0));
                return (
                  <div key={t.n} className={"row" + (t.n === sel ? ' sel' : '')} onClick={() => setSel(t.n)}>
                    <div className="td"><span className="ico">T</span></div>
                    <div className="td name">{t.n}</div>
                    <div className="td mono r">{t.p}</div>
                    <div className="td mono r">{t.r}</div>
                    <div className="td mono r">{t.l}</div>
                    <div className="td mono r" style={{ color: t.lag === '0' ? 'var(--ink-3)' : t.lag.includes('k') ? 'var(--amber)' : 'var(--ink-1)' }}>{t.lag}</div>
                    <div className="td"><MiniSpark cls={t.c === 'amber' ? 'sk dn' : t.c === 'rust' ? 'sk r' : 'sk up'} pts={trend} /></div>
                    <div className="td"><span className={"pill " + t.c}>{t.c && <span className="dot" />}{t.s}</span></div>
                  </div>
                );
              })}
            </div>
          </div>

          <div className="col-inspect">
            <div className="insp-h">
              <div className="name">{selTopic.n}</div>
              <div className="meta">
                <span className="tag">{selTopic.p} partitions</span>
                <span className="tag">rf=3</span>
                <span className="tag">retention 7d</span>
                <span className="tag">compaction: delete</span>
                <span className={"pill " + selTopic.c}>{selTopic.c && <span className="dot" />}{selTopic.s}</span>
              </div>
            </div>

            <div className="insp-tabs">
              <span className="tab on">messages</span>
              <span className="tab">partitions</span>
              <span className="tab">schema</span>
              <span className="tab">config</span>
              <span className="tab">consumers</span>
              <span className="tab">acls</span>
            </div>

            <div className="insp-body">
              <div className="row gap-2" style={{ marginBottom: 12, flexWrap: 'wrap', alignItems: 'center' }}>
                <button className="btn ghost" style={{ height: 28, fontSize: 12 }}>◧ tail</button>
                <button className="btn" style={{ height: 28, fontSize: 12 }}>↺ replay from</button>
                <button className="btn ghost" style={{ height: 28, fontSize: 12 }}>⤓ export</button>
                <span style={{ flex: 1 }} />
                <span className="mono" style={{ fontSize: 11, color: 'var(--ink-3)' }}>partition · all</span>
                <span className="mono" style={{ fontSize: 11, color: 'var(--rust)' }}>● live</span>
              </div>

              <div className="msg-list">
                {messages.map((m) => (
                  <div key={m.off} className="msg">
                    <div className="h">
                      <span className="off">@{m.off}</span>
                      <span>p07</span>
                      <span>key: <span style={{ color: 'var(--ink-1)' }}>{m.key}</span></span>
                      <span className="ts">{m.ts}</span>
                    </div>
                    <div className="body">{renderBody(m.body)}</div>
                  </div>
                ))}
              </div>

              <hr className="hr" style={{ margin: '18px 0 12px' }} />

              <div className="kv">
                <span className="k">schema</span><span className="v">orders-value · avro · v5</span>
                <span className="k">created</span><span className="v">2026-04-12 by j.lee</span>
                <span className="k">avg msg size</span><span className="v">412 bytes</span>
                <span className="k">storage</span><span className="v">142 GB · 7d retention</span>
                <span className="k">consumers</span><span className="v">3 groups · 11 members</span>
              </div>

              <div className="term" style={{ marginTop: 4 }}>
                <div className="term-head">
                  <span className="lights"><i /><i /><i /></span>
                  <span>cli equivalent</span>
                </div>
                <div className="term-body" style={{ padding: '10px 14px' }}>
                  <div><span className="prompt">$</span> rafka <span className="arg">tail</span> {selTopic.n} <span className="flag">--since</span> <span style={{ color: 'var(--violet)' }}>5m</span></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Shell>
  );
}

Object.assign(window, { TopicBrowser });
