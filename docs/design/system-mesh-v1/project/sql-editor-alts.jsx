// Rafka — SQL editor: alternative directions
// Alt A: Notebook (cell-based, like Hex/Jupyter for streaming)
// Alt B: Pipeline canvas (visual node graph ⇄ SQL)
// Alt C: AI prompt-first (NL → SQL → live preview)

/* ──────────────────────────────────────────────────────────────
   Alt A — Notebook
   ──────────────────────────────────────────────────────────── */
function SqlNotebook() {
  const cells = [
    {
      kind: 'md',
      body: '# Orders enrichment\nJoin `orders.v2` with the customer dimension and the SKU tier, then window by minute. Each cell below runs as a temporary view; the final cell **promotes** to a deployed job.',
    },
    {
      kind: 'sql',
      title: 'enrich_orders',
      state: 'ok',
      tokens: [
        [{c:'kw',t:'WITH'},{t:' '},{c:'id',t:'enriched'},{t:' '},{c:'kw',t:'AS'},{t:' ('}],
        [{c:'kw',t:'  SELECT'},{t:' o.*, c.'},{c:'col',t:'segment'},{t:', s.'},{c:'col',t:'tier'}],
        [{c:'kw',t:'  FROM'},{t:' '},{c:'id',t:'orders.v2'},{t:' o'}],
        [{c:'kw',t:'  LEFT JOIN'},{t:' '},{c:'id',t:'lookup.customer'},{t:' c '},{c:'kw',t:'ON'},{t:' o.'},{c:'col',t:'customer_id'},{t:' = c.'},{c:'col',t:'id'}],
        [{c:'kw',t:'  LEFT JOIN'},{t:' '},{c:'id',t:'lookup.sku'},{t:' s '},{c:'kw',t:'ON'},{t:' o.'},{c:'col',t:'sku'},{t:' = s.'},{c:'col',t:'sku'}],
        [{t:') '},{c:'kw',t:'SELECT'},{t:' * '},{c:'kw',t:'FROM'},{t:' '},{c:'id',t:'enriched'},{t:';'}],
      ],
      out: { mode: 'sample', rate: '142.4k/s', rows: 6, cols: ['order_id', 'customer_id', 'sku', 'segment', 'tier', 'amount'] },
    },
    {
      kind: 'md',
      body: '## Window by minute, filter to paid orders',
    },
    {
      kind: 'sql',
      title: 'windowed',
      state: 'ok',
      tokens: [
        [{c:'kw',t:'SELECT'},{t:' '},{c:'fn',t:'TUMBLE_START'},{t:'(event_time, '},{c:'fn',t:'INTERVAL'},{t:' '},{c:'str',t:"'1' MINUTE"},{t:') '},{c:'kw',t:'AS'},{t:' '},{c:'col',t:'win'},{t:','}],
        [{t:'       '},{c:'col',t:'segment'},{t:', '},{c:'fn',t:'COUNT'},{t:'(*) '},{c:'kw',t:'AS'},{t:' '},{c:'col',t:'orders'},{t:', '},{c:'fn',t:'SUM'},{t:'(amount_cents) '},{c:'kw',t:'AS'},{t:' '},{c:'col',t:'gmv'}],
        [{c:'kw',t:'FROM'},{t:' '},{c:'id',t:'enrich_orders'},{t:' '},{c:'kw',t:'WHERE'},{t:' status = '},{c:'str',t:"'paid'"}],
        [{c:'kw',t:'GROUP BY'},{t:' '},{c:'fn',t:'TUMBLE'},{t:'(event_time, '},{c:'fn',t:'INTERVAL'},{t:' '},{c:'str',t:"'1' MINUTE"},{t:'), '},{c:'col',t:'segment'},{t:';'}],
      ],
      out: { mode: 'chart' },
    },
    {
      kind: 'md',
      body: '## Promote to materialized sink\nWrites to `orders.enriched.gmv_1m` with exactly-once semantics.',
    },
    {
      kind: 'sql',
      title: 'sink_promote',
      state: 'promoted',
      tokens: [
        [{c:'kw',t:'CREATE MATERIALIZED VIEW'},{t:' '},{c:'id',t:'orders.enriched.gmv_1m'}],
        [{c:'kw',t:'WITH'},{t:' (connector = '},{c:'str',t:"'kafka'"},{t:', '},{c:'kw',t:'format'},{t:' = '},{c:'str',t:"'avro'"},{t:', '},{c:'kw',t:'mode'},{t:' = '},{c:'str',t:"'exactly_once'"},{t:')'}],
        [{c:'kw',t:'AS SELECT'},{t:' * '},{c:'kw',t:'FROM'},{t:' '},{c:'id',t:'windowed'},{t:';'}],
      ],
      out: { mode: 'job' },
    },
  ];

  return (
    <Shell active="sql" breadcrumb={['acme','prod','us-east-2','sql','notebooks','orders-gmv']}
      title="orders-gmv · notebook"
      actions={<><button className="btn ghost">Restart kernel</button><button className="btn ghost">Run all</button><button className="btn primary">⇧⏎ Run cell</button></>}>
      <div className="panel" style={{padding:0}}>
        <div className="nb-meta">
          <span className="mono">notebook · flink-sql 1.18</span>
          <span className="dim mono">· kernel: session-cluster-1 · ✓ attached</span>
          <span className="dim mono" style={{marginLeft:'auto'}}>autosave · 4s ago · @j.lee</span>
        </div>
        <div className="nb-body">
          {cells.map((cell, i) => (
            <div key={i} className={'nb-cell ' + cell.kind}>
              <div className="nb-gut">
                <div className="nb-idx">{cell.kind === 'sql' ? `[${i}]` : '— —'}</div>
                <div className="nb-run">{cell.kind === 'sql' && <span>▶</span>}</div>
              </div>
              {cell.kind === 'md' && (
                <div className="nb-md">
                  {cell.body.split('\n').map((ln, k) => {
                    if (ln.startsWith('# ')) return <h2 key={k}>{ln.slice(2)}</h2>;
                    if (ln.startsWith('## ')) return <h3 key={k}>{ln.slice(3)}</h3>;
                    return <p key={k} dangerouslySetInnerHTML={{
                      __html: ln.replace(/`([^`]+)`/g, '<code>$1</code>').replace(/\*\*([^*]+)\*\*/g, '<b>$1</b>')
                    }} />;
                  })}
                </div>
              )}
              {cell.kind === 'sql' && (
                <div className="nb-sql">
                  <div className="nb-sql-h">
                    <span className="nb-cell-title mono">{cell.title}</span>
                    <span className={'pill ' + (cell.state === 'promoted' ? 'rust' : 'jade')} style={{height:18,fontSize:10}}>
                      <span className="dot" />{cell.state === 'promoted' ? 'promoted · sink_promote' : 'cached view'}
                    </span>
                    <span style={{marginLeft:'auto'}} className="mono dim">ran 1.4s ago · 0.182s</span>
                  </div>
                  <div className="nb-sql-body">
                    {cell.tokens.map((toks, k) => (
                      <div key={k} className="ln">{toks.map((tk, j) => <span key={j} className={tk.c ? 'sq-' + tk.c : ''}>{tk.t}</span>)}</div>
                    ))}
                  </div>
                  <div className="nb-out">
                    {cell.out.mode === 'sample' && <NbSampleTable />}
                    {cell.out.mode === 'chart' && <NbChart />}
                    {cell.out.mode === 'job' && <NbJobCard />}
                  </div>
                </div>
              )}
            </div>
          ))}
          <div className="nb-add">
            <button className="nb-add-btn"><span>＋</span> SQL cell</button>
            <button className="nb-add-btn"><span>＋</span> Markdown</button>
            <button className="nb-add-btn"><span>＋</span> Chart</button>
            <span className="dim mono" style={{marginLeft:'auto'}}>4 cells · 1 promoted</span>
          </div>
        </div>
      </div>
    </Shell>
  );
}

function NbSampleTable() {
  const rows = [
    ['ord_8f1a','cust_2188','SKU-2317','premium','A','$199.80'],
    ['ord_8f1b','cust_0421','SKU-1009','core','B','$49.00'],
    ['ord_8f1c','cust_3304','SKU-2317','premium','A','$299.70'],
    ['ord_8f1d','cust_7712','SKU-0044','core','C','$12.90'],
    ['ord_8f1e','cust_1119','SKU-0931','whale','A','$714.00'],
    ['ord_8f1f','cust_0034','SKU-2317','core','B','$199.80'],
  ];
  return (
    <div>
      <div className="nb-out-h">
        <span className="pill jade" style={{height:18,fontSize:10}}><span className="dot" />streaming · 142.4k/s</span>
        <span className="mono dim">sample · 6 of 19.2M</span>
        <span className="mono dim" style={{marginLeft:'auto'}}>order_id · customer_id · sku · segment · tier · amount</span>
      </div>
      <div className="nb-tbl">
        <div className="th mono">order_id</div><div className="th mono">customer_id</div><div className="th mono">sku</div><div className="th mono">segment</div><div className="th mono">tier</div><div className="th mono r">amount</div>
        {rows.map((r,i)=>(<React.Fragment key={i}>{r.map((c,k)=><div key={k} className={'td mono' + (k===5?' r':'')}>{c}</div>)}</React.Fragment>))}
      </div>
    </div>
  );
}

function NbChart() {
  const series = Array.from({length:24},(_,i)=>40+Math.sin(i/2)*14+i*1.4);
  const max = Math.max(...series);
  const path = series.map((v,i)=>`${i?'L':'M'}${(i/(series.length-1))*100},${100-(v/max)*92}`).join(' ');
  return (
    <div>
      <div className="nb-out-h">
        <span className="pill jade" style={{height:18,fontSize:10}}><span className="dot" />updating · 1m window</span>
        <span className="mono dim">gmv · last 24 minutes · all segments</span>
        <span className="mono dim" style={{marginLeft:'auto'}}>peak $71,420 · 14:18</span>
      </div>
      <div style={{padding:'10px 16px 14px',background:'var(--bg-0)'}}>
        <svg viewBox="0 0 100 60" preserveAspectRatio="none" style={{width:'100%',height:130}}>
          {[20,40,60,80].map(y => <line key={y} x1="0" x2="100" y1={y*0.6} y2={y*0.6} stroke="var(--line-1)" strokeWidth="0.3"/>)}
          <path d={path+' L100,60 L0,60 Z'} fill="oklch(from var(--rust) l c h / 0.16)"/>
          <path d={path} fill="none" stroke="var(--rust)" strokeWidth="1.2" vectorEffect="non-scaling-stroke"/>
        </svg>
      </div>
    </div>
  );
}

function NbJobCard() {
  return (
    <div style={{padding:'12px 16px',background:'oklch(from var(--rust) l c h / 0.06)',borderTop:'1px solid var(--line-1)',display:'flex',alignItems:'center',gap:14}}>
      <div style={{width:36,height:36,borderRadius:8,background:'linear-gradient(135deg, var(--rust), var(--rust-2))',display:'flex',alignItems:'center',justifyContent:'center',color:'#fff',fontFamily:'JetBrains Mono, monospace',fontWeight:700}}>▶</div>
      <div style={{flex:1}}>
        <div style={{fontFamily:'JetBrains Mono, monospace',fontSize:13,fontWeight:600,color:'var(--rust)'}}>orders-gmv-1m · deployed</div>
        <div className="mono dim" style={{fontSize:11}}>flink job 7c4a91e · par 8 · ck-4218 ok · 02:14 elapsed · 19.2M emitted</div>
      </div>
      <button className="btn ghost" style={{height:26,padding:'0 10px',fontSize:11}}>Open job</button>
      <button className="btn ghost" style={{height:26,padding:'0 10px',fontSize:11}}>Savepoint</button>
    </div>
  );
}

/* ──────────────────────────────────────────────────────────────
   Alt B — Pipeline canvas (visual node graph)
   ──────────────────────────────────────────────────────────── */
function SqlCanvas() {
  const [sel, setSel] = React.useState('window');
  const nodes = [
    { id:'src1',   kind:'src',  x:24,  y:40,   w:160, h:84, title:'orders.v2',         sub:'kafka · 24 part',  rate:'142.4k/s' },
    { id:'src2',   kind:'jdbc', x:24,  y:160,  w:160, h:84, title:'lookup.customer',   sub:'jdbc · postgres',  rate:'temporal' },
    { id:'src3',   kind:'jdbc', x:24,  y:280,  w:160, h:84, title:'lookup.sku',        sub:'jdbc · postgres',  rate:'temporal' },
    { id:'join',   kind:'op',   x:230, y:148, w:170, h:108, title:'JOIN', sub:'temporal · 2 keys', rate:'141.9k/s', bp:0.18 },
    { id:'filter', kind:'op',   x:440, y:148, w:160, h:108, title:'WHERE', sub:'status = "paid"',   rate:'138.2k/s', bp:0.10 },
    { id:'window', kind:'win',  x:640, y:148, w:170, h:108, title:'TUMBLE 1m', sub:'group by segment', rate:'4.7k/s', bp:0.65 },
    { id:'sink',   kind:'sink', x:850, y:148, w:140, h:108, title:'orders.gmv_1m', sub:'kafka · avro · eos', rate:'4.7k/s' },
  ];
  const edges = [
    ['src1','join'], ['src2','join'], ['src3','join'],
    ['join','filter'], ['filter','window'], ['window','sink'],
  ];
  const node = (id) => nodes.find(n => n.id === id);

  const selectedSql = {
    window: [
      [{c:'kw',t:'SELECT'},{t:' '},{c:'fn',t:'TUMBLE_START'},{t:'(event_time, '},{c:'fn',t:'INTERVAL'},{t:' '},{c:'str',t:"'1' MINUTE"},{t:') '},{c:'kw',t:'AS'},{t:' '},{c:'col',t:'win'},{t:','}],
      [{t:'       '},{c:'col',t:'segment'},{t:', '},{c:'fn',t:'SUM'},{t:'(amount_cents) '},{c:'kw',t:'AS'},{t:' '},{c:'col',t:'gmv'}],
      [{c:'kw',t:'FROM'},{t:' '},{c:'id',t:'#filter'}],
      [{c:'kw',t:'GROUP BY'},{t:' '},{c:'fn',t:'TUMBLE'},{t:'(event_time, '},{c:'fn',t:'INTERVAL'},{t:' '},{c:'str',t:"'1' MINUTE"},{t:'), '},{c:'col',t:'segment'},{t:';'}],
    ]
  };

  // Edge paths (orthogonal bezier-ish)
  const edgePath = (a, b) => {
    const ax = a.x + a.w, ay = a.y + a.h/2;
    const bx = b.x,       by = b.y + b.h/2;
    const dx = Math.max((bx-ax)/2, 20);
    return `M ${ax} ${ay} C ${ax+dx} ${ay}, ${bx-dx} ${by}, ${bx} ${by}`;
  };

  return (
    <Shell active="sql" breadcrumb={['acme','prod','us-east-2','sql','pipelines','orders-gmv']}
      title="orders-gmv · pipeline"
      actions={<><button className="btn ghost">SQL view</button><button className="btn ghost">Validate</button><button className="btn primary">Deploy</button></>}>
      <div className="panel" style={{padding:0}}>
        <div className="pc-grid">
          {/* Palette */}
          <div className="pc-palette">
            <div className="pc-pal-h mono">nodes</div>
            <div className="pc-pal-cat mono">sources</div>
            <div className="pc-pal-i"><span className="kind src">T</span><span className="nm">kafka topic</span></div>
            <div className="pc-pal-i"><span className="kind jdbc">J</span><span className="nm">jdbc lookup</span></div>
            <div className="pc-pal-i"><span className="kind src">C</span><span className="nm">cdc stream</span></div>
            <div className="pc-pal-cat mono">transforms</div>
            <div className="pc-pal-i"><span className="kind op">σ</span><span className="nm">filter / where</span></div>
            <div className="pc-pal-i"><span className="kind op">⨝</span><span className="nm">join</span></div>
            <div className="pc-pal-i"><span className="kind op">π</span><span className="nm">project</span></div>
            <div className="pc-pal-i"><span className="kind win">⊞</span><span className="nm">tumbling window</span></div>
            <div className="pc-pal-i"><span className="kind win">⊟</span><span className="nm">sliding window</span></div>
            <div className="pc-pal-i"><span className="kind win">Σ</span><span className="nm">aggregate</span></div>
            <div className="pc-pal-cat mono">sinks</div>
            <div className="pc-pal-i"><span className="kind sink">K</span><span className="nm">kafka topic</span></div>
            <div className="pc-pal-i"><span className="kind sink">I</span><span className="nm">iceberg</span></div>
            <div className="pc-pal-i"><span className="kind sink">S</span><span className="nm">snowflake</span></div>
          </div>

          {/* Canvas */}
          <div className="pc-canvas">
            <div className="pc-cnv-tools mono">
              <span className="seg"><span className="seg-i on">canvas</span><span className="seg-i">sql</span><span className="seg-i">split</span></span>
              <span className="sep" />
              <span className="dim">zoom 100%</span>
              <span className="dim">· 7 nodes · 6 edges</span>
              <span style={{marginLeft:'auto'}} className="pill jade" data-h={18}><span className="dot" />validated · ready to deploy</span>
            </div>
            <div className="pc-stage">
              <svg className="pc-grid-bg">
                <defs>
                  <pattern id="g" width="20" height="20" patternUnits="userSpaceOnUse">
                    <path d="M 20 0 L 0 0 0 20" fill="none" stroke="oklch(from var(--line-1) l c h / 0.5)" strokeWidth="0.5"/>
                  </pattern>
                  <marker id="pcarr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto">
                    <path d="M0,0 L10,5 L0,10 z" fill="var(--ink-3)"/>
                  </marker>
                </defs>
                <rect width="100%" height="100%" fill="url(#g)"/>
                {edges.map(([a,b],i)=>(
                  <path key={i} d={edgePath(node(a), node(b))} fill="none"
                        stroke={node(b).bp > 0.5 ? 'var(--amber)' : 'var(--ink-3)'}
                        strokeWidth="1.6" markerEnd="url(#pcarr)" />
                ))}
                {edges.map(([a,b],i)=>{
                  const n1=node(a), n2=node(b);
                  const mx = (n1.x+n1.w + n2.x)/2;
                  const my = (n1.y+n1.h/2 + n2.y+n2.h/2)/2 - 6;
                  return <text key={'l'+i} x={mx} y={my} textAnchor="middle" fill="var(--ink-3)" fontFamily="JetBrains Mono, monospace" fontSize="10">{n2.rate}</text>;
                })}
              </svg>
              {nodes.map(n => (
                <div key={n.id} className={'pc-node ' + n.kind + (sel===n.id?' sel':'')}
                     style={{left:n.x,top:n.y,width:n.w,height:n.h}}
                     onClick={()=>setSel(n.id)}>
                  <div className="pc-node-h">
                    <span className="kd">{n.kind === 'src' ? 'SRC' : n.kind === 'sink' ? 'SNK' : n.kind === 'win' ? 'WIN' : n.kind === 'jdbc' ? 'JDB' : 'OP'}</span>
                    <span className="rate mono">{n.rate}</span>
                  </div>
                  <div className="pc-node-t">{n.title}</div>
                  <div className="pc-node-s">{n.sub}</div>
                  {typeof n.bp === 'number' && (
                    <div className="pc-bp">
                      <div className="pc-bp-bar"><i style={{width:(n.bp*100)+'%'}} className={n.bp>0.5?'warn':n.bp>0.3?'mid':''}/></div>
                      <span className={'mono ' + (n.bp>0.5?'warn':'')}>{Math.round(n.bp*100)}% bp</span>
                    </div>
                  )}
                  <span className="pc-port l"/><span className="pc-port r"/>
                </div>
              ))}
            </div>
          </div>

          {/* Inspector */}
          <div className="pc-inspect">
            <div className="pc-ins-h mono">inspector · TUMBLE 1m</div>
            <div className="pc-ins-tabs mono">
              <span className="tab on">props</span>
              <span className="tab">sql</span>
              <span className="tab">preview</span>
              <span className="tab">errors</span>
            </div>
            <div className="pc-ins-body">
              <div className="pc-fld"><span className="k mono">node id</span><span className="v mono">window</span></div>
              <div className="pc-fld"><span className="k mono">type</span><span className="v mono">tumbling window</span></div>
              <div className="pc-fld"><span className="k mono">size</span><span className="v mono">1 MINUTE</span></div>
              <div className="pc-fld"><span className="k mono">time attribute</span><span className="v mono">event_time</span></div>
              <div className="pc-fld"><span className="k mono">group by</span><span className="v mono">segment</span></div>
              <div className="pc-fld"><span className="k mono">aggregates</span>
                <span className="v mono">SUM(amount_cents) AS gmv<br/>COUNT(*) AS orders</span>
              </div>
              <div className="pc-fld"><span className="k mono">parallelism</span><span className="v mono">8</span></div>

              <div className="pc-ins-sql mono">
                {selectedSql.window.map((toks,i)=>(
                  <div key={i} className="ln">{toks.map((tk,k)=><span key={k} className={tk.c?'sq-'+tk.c:''}>{tk.t}</span>)}</div>
                ))}
              </div>

              <div className="pc-ins-prev">
                <div className="pc-ins-prev-h mono"><span>preview · 60 last windows</span><span className="dim">live</span></div>
                <svg viewBox="0 0 100 40" preserveAspectRatio="none" style={{width:'100%',height:70}}>
                  {Array.from({length:60}).map((_,i)=>{
                    const h = 8 + Math.abs(Math.sin(i/3))*22 + (i>42?(i-42)*0.4:0);
                    return <rect key={i} x={i*(100/60)} y={40-h} width={100/60-0.2} height={h} fill={h>30?'var(--amber)':'var(--rust)'} opacity="0.85"/>;
                  })}
                </svg>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Shell>
  );
}

/* ──────────────────────────────────────────────────────────────
   Alt C — AI prompt-first
   ──────────────────────────────────────────────────────────── */
function SqlPrompt() {
  const turns = [
    {
      kind: 'user',
      body: 'give me a stream of paid orders enriched with customer segment and sku tier, aggregated to gmv per minute by segment',
    },
    {
      kind: 'ai',
      plan: [
        'Read from `orders.v2` (kafka, 24 partitions)',
        'Temporal join `lookup.customer` for segment',
        'Temporal join `lookup.sku` for tier',
        'Filter to status = "paid"',
        'Tumble 1m, group by segment, sum amount',
      ],
      sql: [
        [{c:'kw',t:'CREATE TEMPORARY VIEW'},{t:' '},{c:'id',t:'gmv_by_segment_1m'},{t:' '},{c:'kw',t:'AS'}],
        [{c:'kw',t:'SELECT'},{t:' '},{c:'fn',t:'TUMBLE_START'},{t:'(o.event_time, '},{c:'fn',t:'INTERVAL'},{t:' '},{c:'str',t:"'1' MINUTE"},{t:') '},{c:'kw',t:'AS'},{t:' '},{c:'col',t:'win'},{t:','}],
        [{t:'       c.'},{c:'col',t:'segment'},{t:', '},{c:'fn',t:'SUM'},{t:'(o.amount_cents) '},{c:'kw',t:'AS'},{t:' '},{c:'col',t:'gmv'}],
        [{c:'kw',t:'FROM'},{t:' '},{c:'id',t:'orders.v2'},{t:' o'}],
        [{c:'kw',t:'LEFT JOIN'},{t:' '},{c:'id',t:'lookup.customer'},{c:'kw',t:' FOR SYSTEM_TIME AS OF'},{t:' o.event_time c '},{c:'kw',t:'ON'},{t:' o.customer_id = c.id'}],
        [{c:'kw',t:'LEFT JOIN'},{t:' '},{c:'id',t:'lookup.sku'},{c:'kw',t:' FOR SYSTEM_TIME AS OF'},{t:' o.event_time s '},{c:'kw',t:'ON'},{t:' o.sku = s.sku'}],
        [{c:'kw',t:'WHERE'},{t:' o.status = '},{c:'str',t:"'paid'"}],
        [{c:'kw',t:'GROUP BY'},{t:' '},{c:'fn',t:'TUMBLE'},{t:'(o.event_time, '},{c:'fn',t:'INTERVAL'},{t:' '},{c:'str',t:"'1' MINUTE"},{t:'), c.'},{c:'col',t:'segment'},{t:';'}],
      ],
      caveats: [
        { kind:'warn', t:'Temporal joins use proc-time of `lookup.*`. Late row updates will not retro-correct windows.' },
        { kind:'info', t:'`sku` table was not used in the SELECT — included join because you mentioned tier. Remove if unused.' },
      ],
    },
    {
      kind: 'user',
      body: 'also break gmv out by sku tier',
    },
    {
      kind: 'ai-typing',
    },
  ];

  return (
    <Shell active="sql" breadcrumb={['acme','prod','us-east-2','sql','assistant']}
      title="ask · streaming sql"
      actions={<><button className="btn ghost">History · 18</button><button className="btn ghost">Saved prompts</button><button className="btn primary">New session</button></>}>
      <div className="panel" style={{padding:0}}>
        <div className="ap-grid">
          {/* Conversation */}
          <div className="ap-conv">
            {turns.map((t,i) => (
              <div key={i} className={'ap-turn ' + t.kind}>
                {t.kind === 'user' && (
                  <>
                    <div className="ap-av usr">JL</div>
                    <div className="ap-bub usr">{t.body}</div>
                  </>
                )}
                {t.kind === 'ai-typing' && (
                  <>
                    <div className="ap-av ai">R</div>
                    <div className="ap-bub ai typing"><span/><span/><span/> drafting plan…</div>
                  </>
                )}
                {t.kind === 'ai' && (
                  <>
                    <div className="ap-av ai">R</div>
                    <div className="ap-bub ai">
                      <div className="ap-section mono">PLAN</div>
                      <ol className="ap-plan">
                        {t.plan.map((p,k)=>(
                          <li key={k} dangerouslySetInnerHTML={{__html:p.replace(/`([^`]+)`/g,'<code>$1</code>')}}/>
                        ))}
                      </ol>
                      <div className="ap-section mono">SQL <span className="dim">· flink-sql 1.18</span></div>
                      <div className="ap-sql mono">
                        {t.sql.map((toks,k)=>(<div key={k} className="ln">{toks.map((tk,j)=><span key={j} className={tk.c?'sq-'+tk.c:''}>{tk.t}</span>)}</div>))}
                      </div>
                      {t.caveats && (
                        <div className="ap-caveats">
                          {t.caveats.map((c,k)=>(
                            <div key={k} className={'ap-cav ' + c.kind}>
                              <span className="gl">{c.kind==='warn'?'⚠':'ℹ'}</span>
                              <span>{c.t}</span>
                            </div>
                          ))}
                        </div>
                      )}
                      <div className="ap-actions">
                        <button className="btn primary" style={{height:26,padding:'0 10px',fontSize:11}}>▶ Run preview</button>
                        <button className="btn ghost" style={{height:26,padding:'0 10px',fontSize:11}}>Deploy as job</button>
                        <button className="btn ghost" style={{height:26,padding:'0 10px',fontSize:11}}>Open in editor</button>
                        <span className="mono dim" style={{marginLeft:'auto',fontSize:10}}>cost · est 4 task slots · 0 backfill</span>
                      </div>
                    </div>
                  </>
                )}
              </div>
            ))}
            {/* Composer */}
            <div className="ap-composer">
              <div className="ap-comp-chips">
                <span className="chip on">orders.v2</span>
                <span className="chip">lookup.customer</span>
                <span className="chip">lookup.sku</span>
                <span className="chip add">＋ add context</span>
                <span className="mono dim" style={{marginLeft:'auto',fontSize:10}}>⌘↵ send</span>
              </div>
              <div className="ap-comp-in">
                <textarea placeholder="ask in plain english, or paste SQL to refactor…" defaultValue="also break gmv out by sku tier"/>
              </div>
              <div className="ap-comp-foot mono">
                <span className="dim">model · rafka-sql 0.4</span>
                <span className="dim">· grounded on schema registry · 142 topics</span>
                <button className="btn primary" style={{marginLeft:'auto',height:26,padding:'0 12px',fontSize:11}}>Send</button>
              </div>
            </div>
          </div>

          {/* Live preview pane */}
          <div className="ap-side">
            <div className="ap-side-h mono">live preview · gmv_by_segment_1m</div>
            <div className="ap-side-kpis">
              <div className="k"><div className="lbl mono">rows / s</div><div className="val">4.7<span className="u">k</span></div></div>
              <div className="k"><div className="lbl mono">windows / m</div><div className="val">240</div></div>
              <div className="k"><div className="lbl mono">elapsed</div><div className="val">02:14</div></div>
            </div>
            <div className="ap-side-chart">
              <div className="hd mono"><span>gmv · 24 windows · stacked by segment</span><span className="dim">live</span></div>
              <svg viewBox="0 0 100 60" preserveAspectRatio="none" style={{width:'100%',height:140}}>
                {Array.from({length:24}).map((_,i)=>{
                  const x = i*(100/24);
                  const w = 100/24 - 0.6;
                  const a = 8 + Math.abs(Math.sin(i/2))*8;
                  const b = 6 + Math.abs(Math.cos(i/3))*6;
                  const c = 4 + Math.abs(Math.sin(i/4+1))*4;
                  let y = 60;
                  return (
                    <g key={i}>
                      <rect x={x} y={y-a} width={w} height={a} fill="var(--rust)" opacity="0.9"/>
                      <rect x={x} y={y-a-b} width={w} height={b} fill="var(--ember)" opacity="0.9"/>
                      <rect x={x} y={y-a-b-c} width={w} height={c} fill="var(--ice)" opacity="0.9"/>
                    </g>
                  );
                })}
              </svg>
              <div className="lg mono">
                <span><i style={{background:'var(--rust)'}}/>core</span>
                <span><i style={{background:'var(--ember)'}}/>premium</span>
                <span><i style={{background:'var(--ice)'}}/>whale</span>
              </div>
            </div>
            <div className="ap-side-tbl">
              <div className="hd mono">sample rows</div>
              <div className="rw mono"><span>14:22</span><span>premium</span><span>$71,420</span></div>
              <div className="rw mono"><span>14:22</span><span>core</span><span>$42,180</span></div>
              <div className="rw mono"><span>14:22</span><span>whale</span><span>$118,940</span></div>
              <div className="rw mono"><span>14:21</span><span>premium</span><span>$66,210</span></div>
              <div className="rw mono"><span>14:21</span><span>core</span><span>$39,800</span></div>
              <div className="rw mono"><span>14:21</span><span>whale</span><span>$112,400</span></div>
            </div>
          </div>
        </div>
      </div>
    </Shell>
  );
}

Object.assign(window, { SqlNotebook, SqlCanvas, SqlPrompt });
