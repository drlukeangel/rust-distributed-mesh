// Rafka — Fuel (WCC) — 4 screens
// 1. PlatformFuel       — SaaS admin: fleet capacity + revenue plumbing health
// 2. PlatformOrgDrill   — SaaS admin → single org (heavy tail / metering anomaly)
// 3. CustomerFuel       — Org's own dashboard: where is my money going
// 4. CustomerJobDrill   — Single expensive job/filter, tax breakdown, fix suggestions

/* ── shared helpers ─────────────────────────────────────────── */
const fuelFmt = (n) => n >= 1e9 ? (n/1e9).toFixed(2)+'B' : n >= 1e6 ? (n/1e6).toFixed(1)+'M' : n >= 1e3 ? (n/1e3).toFixed(1)+'k' : String(n);

/* ──────────────────────────────────────────────────────────────
   1. Platform fuel overview
   ──────────────────────────────────────────────────────────── */
function PlatformFuel() {
  const orgs = [
    ['org_4a8b · stripe-eng',     '142.4M', '12%', 'whale',   '8.4k', 0.42, 'amber',  '$1,842/h'],
    ['org_2188 · acme',           '88.2M',  '47%', 'premium', '4.6k', 0.18, 'jade',   '$1,142/h'],
    ['org_3304 · nimbus',         '64.0M',  '8%',  'whale',   '6.2k', 0.61, 'amber',  '$  928/h'],
    ['org_1119 · figment-labs',   '38.4M',  '92%', 'premium', '3.1k', 0.12, 'jade',   '$  482/h'],
    ['org_0421 · oxide',          '24.1M',  '62%', 'core',    '2.4k', 0.22, 'jade',   '$  302/h'],
    ['org_7712 · linear-pulse',   '18.2M',  '4%',  'core',    '1.9k', 0.84, 'crimson','$  244/h'],
    ['org_0034 · vanta-loop',     '12.0M',  '38%', 'core',    '1.4k', 0.31, 'amber',  '$  148/h'],
    ['org_9981 · ledgerline',     '8.2M',   '71%', 'core',    '0.9k', 0.14, 'jade',   '$  102/h'],
  ];

  return (
    <Shell active="platform" breadcrumb={['rafka','platform','fuel','overview']}
      title="fuel (WCC) · platform"
      sub="capacity protection · revenue plumbing · 142 orgs · fleet 6 regions · plan: enterprise"
      actions={<>
        <button className="btn ghost">Reconcile $/WCC</button>
        <button className="btn ghost">Pricing levers</button>
        <button className="btn primary">Capture state</button>
      </>}>

      {/* status strip */}
      <div className="panel fu-strip">
        <div className="ck">
          <div className="ck-l mono">capacity</div>
          <div className="ck-bg">
            <div className="ck-st"><span className="dot jade"/><span className="mono v">0.47</span><span className="lbl mono">fleet pressure · 30s</span></div>
            <div className="ck-st"><span className="dot jade"/><span className="mono v">0</span><span className="lbl mono">circuit-breaker trips · 24h</span></div>
            <div className="ck-st"><span className="dot amber"/><span className="mono v">14</span><span className="lbl mono">orgs &lt;10% balance</span></div>
            <div className="ck-st"><span className="dot jade"/><span className="mono v">2.4/min</span><span className="lbl mono">FuelExhausted traps</span></div>
          </div>
        </div>
        <div className="ck">
          <div className="ck-l mono">revenue</div>
          <div className="ck-bg">
            <div className="ck-st"><span className="dot jade"/><span className="mono v">0.4s</span><span className="lbl mono">billing tail lag</span></div>
            <div className="ck-st"><span className="dot jade"/><span className="mono v">$24,180/h</span><span className="lbl mono">WCC revenue · run</span></div>
            <div className="ck-st"><span className="dot amber"/><span className="mono v">0.04%</span><span className="lbl mono">$/WCC drift · 24h</span></div>
            <div className="ck-st"><span className="dot crimson"/><span className="mono v">RED</span><span className="lbl mono">i13.e1 metering · no proof</span></div>
          </div>
        </div>
      </div>

      {/* charts grid */}
      <div className="fu-grid">
        <div className="panel fu-chart sp2">
          <div className="panel-h">
            <div><div className="title">Fleet fuel pressure · 1h</div><div className="sub mono">per-broker · 5% reserve for OS / io_uring</div></div>
            <div className="ck mono"><span><i style={{background:'var(--jade)'}}/>p50</span><span><i style={{background:'var(--amber)'}}/>p95</span><span><i style={{background:'var(--crimson)'}}/>0.95 trip line</span></div>
          </div>
          <ChartLines series={[
            { color:'var(--jade)',  data:genSeries(80, 0.42, 0.06, 0, 11) },
            { color:'var(--amber)', data:genSeries(80, 0.72, 0.08, 0, 17) },
          ]} h={200} threshold={0.95} thresholdColor="var(--crimson)"/>
        </div>

        <div className="panel fu-chart">
          <div className="panel-h"><div><div className="title">WCC revenue · $/h</div><div className="sub mono">7-day · current run $24,180/h</div></div></div>
          <ChartLines series={[{ color:'var(--rust)', data:genSeries(80, 22000, 1800, 0, 23) }]} h={200}/>
        </div>

        <div className="panel fu-chart">
          <div className="panel-h">
            <div><div className="title">WCC balance distribution</div><div className="sub mono">remaining fuel · 142 orgs</div></div>
            <span className="pill amber" style={{height:18,fontSize:10}}><span className="dot"/>14 orgs &lt; 10%</span>
          </div>
          <ChartStack series={[
            { label:'<10%',  color:'var(--crimson)', data:Array(20).fill(0).map((_,i)=>i<3?2:0) },
            { label:'10-50%',color:'var(--amber)',   data:Array(20).fill(0).map((_,i)=>i<8?3:0) },
            { label:'>50%',  color:'var(--jade)',    data:Array(20).fill(0).map((_,i)=>i<15?6:0) },
          ]} h={200} bar/>
        </div>

        <div className="panel fu-chart">
          <div className="panel-h"><div><div className="title">Burn rate · heavy-tail</div><div className="sub mono">top-1% orgs vs rest · fleet share</div></div></div>
          <div style={{padding:'14px 16px',display:'flex',flexDirection:'column',gap:10}}>
            {[
              ['top-1% · 2 orgs', 0.46, 'crimson'],
              ['top-5% · 8 orgs', 0.71, 'amber'],
              ['top-20% · 28 orgs', 0.91, 'jade'],
              ['long tail · 114 orgs', 0.09, 'mute'],
            ].map(([k,v,s])=>(
              <div key={k} style={{display:'grid',gridTemplateColumns:'160px 1fr 60px',gap:10,alignItems:'center',fontSize:11.5}}>
                <span className="mono" style={{color:'var(--ink-1)'}}>{k}</span>
                <div style={{height:10,background:'var(--bg-0)',borderRadius:3,overflow:'hidden'}}>
                  <div style={{width:(v*100)+'%',height:'100%',background:s==='crimson'?'var(--crimson)':s==='amber'?'var(--amber)':s==='jade'?'var(--jade)':'var(--ink-3)'}}/>
                </div>
                <span className="mono r" style={{textAlign:'right'}}>{Math.round(v*100)}%</span>
              </div>
            ))}
          </div>
        </div>

        <div className="panel fu-chart">
          <div className="panel-h"><div><div className="title">JIT re-scan rate</div><div className="sub mono">5× penalty · filter-epoch rewinds</div></div></div>
          <ChartLines series={[{ color:'var(--ember)', data:genSeries(80, 142, 38, 0, 31) }]} h={200}/>
        </div>

        <div className="panel fu-chart">
          <div className="panel-h">
            <div><div className="title">Billing pipeline · invariants</div><div className="sub mono">crash-reconciliation canaries</div></div>
          </div>
          <div style={{padding:'12px 16px',display:'flex',flexDirection:'column',gap:8,fontSize:12}}>
            {[
              { t:'billing tail-offset lag',     v:'0.4s · p99 1.2s', s:'ok'   },
              { t:'GasRecord commit p99',         v:'48ms',           s:'ok'   },
              { t:'reconcile $ vs aggregate',     v:'+0.04% drift',   s:'warn' },
              { t:'crash_reconciliation_no_double_charge', v:'#[ignore] — RED', s:'red' },
              { t:'silent_double_charge sentinel',v:'0 fires · ok',   s:'ok'   },
              { t:'/metrics/fuel scrape p99',     v:'182ms',          s:'ok'   },
            ].map(r=>(
              <div key={r.t} style={{display:'grid',gridTemplateColumns:'18px 1fr auto',gap:8,alignItems:'center'}}>
                <span className={'dot ' + (r.s==='ok'?'green':r.s==='warn'?'amber':'red')} style={{width:9,height:9,borderRadius:'50%',display:'inline-block',background:r.s==='ok'?'var(--jade)':r.s==='warn'?'var(--amber)':'var(--crimson)'}}/>
                <span className="mono" style={{color:'var(--ink-1)'}}>{r.t}</span>
                <span className="mono" style={{color:r.s==='red'?'var(--crimson)':r.s==='warn'?'var(--amber)':'var(--ink-2)'}}>{r.v}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* heavy tail orgs */}
      <div className="panel fu-orgs">
        <div className="panel-h">
          <div><div className="title">Heavy-tail orgs · top burn 24h</div><div className="sub mono">click row to drill into org · 142 orgs total · 8 shown</div></div>
          <div className="cms-list-tools mono"><span className="chip on">all tiers</span><span className="chip">whale 4</span><span className="chip">premium 12</span><span className="chip">core 126</span></div>
        </div>
        <div className="rt-head mono" style={{gridTemplateColumns:'1.6fr 0.9fr 0.8fr 0.8fr 0.9fr 1.2fr 0.9fr'}}>
          <div>org · workspace</div><div className="r">burn 24h</div><div className="r">balance</div><div>tier</div><div className="r">orps/s</div><div>pressure share</div><div className="r">$/h</div>
        </div>
        {orgs.map(o => (
          <div key={o[0]} className="rt-row" style={{gridTemplateColumns:'1.6fr 0.9fr 0.8fr 0.8fr 0.9fr 1.2fr 0.9fr'}}>
            <div className="mono nm">{o[0]}</div>
            <div className="mono r">{o[1]}</div>
            <div className="mono r" style={{color:parseFloat(o[2])<10?'var(--crimson)':parseFloat(o[2])<40?'var(--amber)':'var(--ink-1)'}}>{o[2]}</div>
            <div className="mono"><span className={'env-pill ' + (o[3]==='whale'?'prod':o[3]==='premium'?'staging':'dev')}>{o[3]}</span></div>
            <div className="mono r">{o[4]}</div>
            <div>
              <div style={{height:6,background:'var(--bg-0)',borderRadius:2,overflow:'hidden'}}>
                <div style={{width:(o[5]*100)+'%',height:'100%',background:o[6]==='crimson'?'var(--crimson)':o[6]==='amber'?'var(--amber)':'var(--jade)'}}/>
              </div>
            </div>
            <div className="mono r">{o[7]}</div>
          </div>
        ))}
      </div>
    </Shell>
  );
}

/* ──────────────────────────────────────────────────────────────
   2. Platform → single org drilldown
   ──────────────────────────────────────────────────────────── */
function PlatformOrgDrill() {
  return (
    <Shell active="platform" breadcrumb={['rafka','platform','fuel','orgs','org_4a8b']}
      title="org_4a8b · stripe-eng · fuel detail"
      sub="whale tier · 8 envs · JWT fuel_limit 500k (stateful) · contract LTV $1.4M · CSM @d.ng"
      actions={<>
        <button className="btn ghost">Notify CSM</button>
        <button className="btn ghost">Raise fuel_limit</button>
        <button className="btn primary">Open Stripe account</button>
      </>}>

      <div className="panel sys-node-hero">
        <div className="l">
          <div className="hero-st">
            <span className="pill amber" style={{height:22}}><span className="dot"/>burn-rate anomaly · 12× baseline · since 13:48 UTC</span>
            <span className="mono dim">flagged by metering-anomaly detector · not yet attributed</span>
          </div>
          <div className="hero-meta mono">
            <div><span className="k">org</span><span>org_4a8b</span></div>
            <div><span className="k">tier</span><span>whale</span></div>
            <div><span className="k">fuel_limit</span><span>500k stateful</span></div>
            <div><span className="k">balance</span><span style={{color:'var(--crimson)'}}>12% · ~38m to zero</span></div>
            <div><span className="k">fleet share</span><span>42%</span></div>
            <div><span className="k">24h burn</span><span>142.4M WCC · $1,842/h</span></div>
            <div><span className="k">JIT re-scans</span><span style={{color:'var(--amber)'}}>3.4k/h · 5× class</span></div>
            <div><span className="k">FuelExhausted</span><span>0 traps · graceful</span></div>
          </div>
        </div>
        <div className="r">
          <div className="rps">
            <div className="big">42<span className="u">%</span></div>
            <div className="dim mono">fleet pressure share · 30s</div>
            <Spark data={genSeries(60, 0.42, 0.06, 0.4, 47)} h={48} w={320} color="var(--amber)"/>
          </div>
        </div>
      </div>

      <div className="sys-node-grid">
        <div className="panel sys-chart sp2">
          <div className="panel-h">
            <div><div className="title">WCC burn breakdown · last 6h</div><div className="sub mono">tax category · which line items moved</div></div>
            <div className="ck mono">
              <span><i style={{background:'var(--rust)'}}/>WASM filter</span>
              <span><i style={{background:'var(--ember)'}}/>5× JIT</span>
              <span><i style={{background:'var(--ice)'}}/>virtual topic</span>
              <span><i style={{background:'var(--amber)'}}/>webhook latency</span>
              <span><i style={{background:'var(--violet)'}}/>S3 batch</span>
            </div>
          </div>
          <ChartLines series={[
            { color:'var(--rust)',  data:genSeries(80, 28, 4, 0.4, 53) },
            { color:'var(--ember)', data:genSeries(80, 18, 6, 0.6, 59) },
            { color:'var(--ice)',   data:genSeries(80, 12, 3, 0, 61) },
            { color:'var(--amber)', data:genSeries(80, 8, 2, 0, 67) },
            { color:'var(--violet)',data:genSeries(80, 4, 1, 0, 71) },
          ]} h={200}/>
        </div>

        <div className="panel sys-chart">
          <div className="panel-h"><div><div className="title">Run-out forecast</div><div className="sub mono">at current burn · top-up auto-disabled</div></div></div>
          <div style={{padding:'20px 18px',display:'flex',flexDirection:'column',gap:12}}>
            <div style={{display:'flex',alignItems:'baseline',gap:8}}>
              <div style={{fontSize:36,fontWeight:600,letterSpacing:'-0.02em',color:'var(--crimson)'}}>38m</div>
              <div className="mono dim">to zero · 14:58 UTC</div>
            </div>
            <div className="mono dim" style={{fontSize:11}}>balance 60.0M / 500.0M (12%)</div>
            <div style={{height:10,background:'var(--bg-0)',borderRadius:3,overflow:'hidden'}}>
              <div style={{width:'12%',height:'100%',background:'linear-gradient(90deg, var(--crimson), var(--amber))'}}/>
            </div>
            <div className="mono dim" style={{fontSize:11,lineHeight:1.5}}>
              suspension policy · checkpoint to __system_compute_checkpoints, resume on top-up.<br/>
              no data loss; invoice trip only.
            </div>
          </div>
        </div>

        <div className="panel sys-chart sp2">
          <div className="panel-h"><div><div className="title">Per-record fuel cost · 7d trend</div><div className="sub mono">top topics · drift = data shape changed</div></div></div>
          <ChartLines series={[
            { color:'var(--rust)',  data:genSeries(60, 480, 80, 0.6, 79) },
            { color:'var(--ember)', data:genSeries(60, 220, 30, 0.1, 83) },
            { color:'var(--ice)',   data:genSeries(60, 142, 18, 0, 89) },
            { color:'var(--ink-3)', data:genSeries(60, 88, 8, 0, 97) },
          ]} h={200}/>
        </div>

        <div className="panel sys-chart">
          <div className="panel-h"><div><div className="title">Anomaly attribution</div><div className="sub mono">candidate root causes</div></div></div>
          <div style={{padding:'12px 16px',display:'flex',flexDirection:'column',gap:8,fontSize:12}}>
            {[
              ['filter regression · payments.events', 'parse cost +280%', 'crimson', '6h ago'],
              ['reindex event · clickstream', 'VirtualTopicReindex 88M WCC',     'amber',   '4h ago'],
              ['webhook stripe-events slow', '+482ms p95 → latency tax up',      'amber',   '2h ago'],
              ['JIT re-scan from analytics replay', 'filter_epoch +1',           'ember',   '1h ago'],
            ].map((r,i)=>(
              <div key={i} style={{padding:'8px 10px',borderRadius:6,background:'var(--bg-0)',border:'1px solid var(--line-1)',display:'flex',flexDirection:'column',gap:2}}>
                <div className="mono" style={{fontWeight:600,color:'var(--ink-1)',fontSize:11.5}}>{r[0]}</div>
                <div style={{display:'flex',gap:8,alignItems:'center'}}>
                  <span className="mono" style={{color:r[2]==='crimson'?'var(--crimson)':'var(--amber)',fontSize:10.5}}>{r[1]}</span>
                  <span className="mono dim" style={{fontSize:10,marginLeft:'auto'}}>{r[3]}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* per-job table */}
      <div className="panel sys-routes">
        <div className="panel-h"><div><div className="title">Top expensive jobs · this org</div><div className="sub mono">filter / virtual topic / webhook / batch — 24h</div></div></div>
        <div className="rt-head mono" style={{gridTemplateColumns:'0.5fr 1.6fr 0.8fr 0.9fr 0.9fr 0.8fr'}}>
          <div>kind</div><div>name</div><div className="r">24h WCC</div><div className="r">$/h</div><div className="r">Δ vs 7d</div><div></div>
        </div>
        {[
          ['filter', 'payments_pii_redact.wasm',   '42.1M', '$542', '+312%', 'crimson'],
          ['vtopic', 'orders.enriched.gmv_1m',     '28.4M', '$368', '+18%',  'amber'],
          ['webhook','stripe-events → webhooks.acme', '18.2M','$240','+82%', 'amber'],
          ['filter', 'clickstream_geo_enrich.wasm','12.1M', '$162', '+4%',   'jade'],
          ['batch',  'analytics.cold_scan_30d',    '8.4M',  '$108', '+0%',   'jade'],
          ['filter', 'orders_dq_validate.wasm',    '6.2M',  '$ 81', '−14%',  'jade'],
        ].map((r,i)=>(
          <div key={i} className="rt-row" style={{gridTemplateColumns:'0.5fr 1.6fr 0.8fr 0.9fr 0.9fr 0.8fr'}}>
            <div><span className={'env-pill ' + (r[0]==='filter'?'prod':r[0]==='vtopic'?'staging':'dev')}>{r[0]}</span></div>
            <div className="mono nm">{r[1]}</div>
            <div className="mono r">{r[2]}</div>
            <div className="mono r">{r[3]}</div>
            <div className="mono r" style={{color:r[5]==='crimson'?'var(--crimson)':r[5]==='amber'?'var(--amber)':'var(--jade)'}}>{r[4]}</div>
            <div className="mono dim r" style={{fontSize:10}}>open ↗</div>
          </div>
        ))}
      </div>
    </Shell>
  );
}

/* ──────────────────────────────────────────────────────────────
   3. Customer fuel dashboard (org's own view)
   ──────────────────────────────────────────────────────────── */
function CustomerFuel() {
  const taxes = [
    { k:'WASM filter exec',        wcc:'42.1M', pct:38, usd:'$542',  delta:'+312%', tone:'crimson', why:'parse cost regressed on payments_pii_redact.wasm' },
    { k:'5× JIT re-scan',           wcc:'18.4M', pct:16, usd:'$240', delta:'+182%', tone:'amber',   why:'analytics consumer rewinding past filter_epoch' },
    { k:'Virtual topic ops',        wcc:'14.2M', pct:13, usd:'$184', delta:'+18%',  tone:'amber',   why:'VirtualTopicReindex from partition expansion' },
    { k:'Webhook 3-dim',            wcc:'12.0M', pct:11, usd:'$156', delta:'+82%',  tone:'amber',   why:'stripe-events endpoint p95 +482ms' },
    { k:'Stateful Window Tax',      wcc:'8.4M',  pct: 7, usd:'$108', delta:'+4%',   tone:'jade',    why:'gmv_by_segment_1m · MB×s pinned slabs' },
    { k:'Batch S3 Egress',          wcc:'6.2M',  pct: 5, usd:'$ 80', delta:'+0%',   tone:'jade',    why:'analytics.cold_scan_30d nightly' },
    { k:'SIMD JSON parse',          wcc:'4.1M',  pct: 4, usd:'$ 53', delta:'+22%',  tone:'amber',   why:'orders.v2 array depth grew · drift watch' },
    { k:'Assassin Tax · tombstones',wcc:'3.8M',  pct: 3, usd:'$ 48', delta:'+0%',   tone:'jade',    why:'baseline; consider batched deletes' },
    { k:'Batch Spilling',           wcc:'2.4M',  pct: 2, usd:'$ 31', delta:'−12%',  tone:'jade',    why:'merge sort intermediate · ssd io' },
    { k:'Webhook DLQ retry',        wcc:'0.4M',  pct: 1, usd:'$  6', delta:'−4%',   tone:'jade',    why:'2 endpoints triggering retry penalty' },
  ];

  return (
    <Shell active="fuel" breadcrumb={['acme','prod','us-east-2','fuel']}
      title="fuel (WCC) · where is the money going"
      sub="acme · prod · plan: enterprise (whale) · JWT fuel_limit 500k stateful · @d.ng your CSM"
      actions={<>
        <button className="btn ghost">Export invoice (PDF)</button>
        <button className="btn ghost">Pricing docs</button>
        <button className="btn primary">Top-up · auto on</button>
      </>}>

      {/* hero summary */}
      <div className="panel cf-hero">
        <div className="cf-bal">
          <div className="cf-bal-l mono">remaining balance</div>
          <div className="cf-bal-v">60.0<span className="u">M</span> <span className="dim">/ 500M</span></div>
          <div className="cf-bal-bar"><i style={{width:'12%'}}/></div>
          <div className="cf-bal-eta mono">
            <span className="t">run-out</span><span className="v">38m</span><span className="dim">· 14:58 UTC</span>
          </div>
        </div>
        <div className="cf-burn">
          <div className="cf-burn-l mono">burn rate · 30s</div>
          <div className="cf-burn-v">$1,842<span className="u">/h</span></div>
          <Spark data={genSeries(60, 1842, 220, 0.4, 11)} h={56} w={300} color="var(--rust)"/>
          <div className="cf-burn-d mono">+12× baseline · investigation open</div>
        </div>
        <div className="cf-month">
          <div className="cf-month-l mono">this month</div>
          <div className="cf-month-v">$24,182</div>
          <div className="cf-month-row">
            <span className="mono dim">projected EoM</span>
            <span className="mono" style={{color:'var(--amber)'}}>$48,400 · +18% vs Mar</span>
          </div>
          <div className="cf-month-row">
            <span className="mono dim">budget cap</span>
            <span className="mono">$60,000</span>
          </div>
        </div>
      </div>

      {/* trend + tax stack */}
      <div className="cf-grid">
        <div className="panel fu-chart">
          <div className="panel-h"><div><div className="title">Spend · last 30d</div><div className="sub mono">daily · WCC → $ at current rate card</div></div></div>
          <ChartLines series={[{ color:'var(--rust)', data:genSeries(80, 820, 180, 0.4, 17) }]} h={200}/>
        </div>
        <div className="panel fu-chart">
          <div className="panel-h"><div><div className="title">Tax composition · 24h</div><div className="sub mono">stack by category</div></div></div>
          <ChartStack series={[
            { label:'WASM filter', color:'var(--rust)',  data:genSeries(60, 28, 4, 0.2, 23) },
            { label:'JIT 5×',      color:'var(--ember)', data:genSeries(60, 18, 6, 0.4, 29) },
            { label:'V-topic',     color:'var(--ice)',   data:genSeries(60, 12, 3, 0, 31) },
            { label:'Webhook',     color:'var(--amber)', data:genSeries(60, 8, 2, 0, 37) },
            { label:'S3 / window', color:'var(--violet)',data:genSeries(60, 6, 1.5, 0, 41) },
          ]} h={200} bar/>
        </div>
      </div>

      {/* tax breakdown table */}
      <div className="panel cf-tax">
        <div className="panel-h">
          <div><div className="title">Where your fuel went · 24h</div><div className="sub mono">tax categories · click row for drill</div></div>
          <span className="mono dim">112.0M WCC · $1,442</span>
        </div>
        <div className="cf-tax-h mono">
          <div>category</div><div className="r">WCC</div><div></div><div className="r">share</div><div className="r">$ today</div><div className="r">Δ 7d</div><div>why</div>
        </div>
        {taxes.map(t=>(
          <div key={t.k} className="cf-tax-row">
            <div className="mono nm">{t.k}</div>
            <div className="mono r">{t.wcc}</div>
            <div className="bar"><i style={{width:t.pct+'%',background:t.tone==='crimson'?'var(--crimson)':t.tone==='amber'?'var(--amber)':'var(--jade)'}}/></div>
            <div className="mono r dim">{t.pct}%</div>
            <div className="mono r">{t.usd}</div>
            <div className="mono r" style={{color:t.tone==='crimson'?'var(--crimson)':t.tone==='amber'?'var(--amber)':'var(--jade)'}}>{t.delta}</div>
            <div className="mono dim why">{t.why}</div>
          </div>
        ))}
      </div>

      {/* fix-it cards */}
      <div className="panel cf-fixes">
        <div className="panel-h"><div><div className="title">Cost-reduction suggestions</div><div className="sub mono">ranked by potential 30-day savings</div></div></div>
        <div className="cf-fix-grid">
          {[
            { sav:'$4,820/mo', t:'Materialize filter into write-time topic', why:'5× JIT cost on payments.events is driven by analytics consumer rewinding past filter_epoch. Write the filter result into a new topic; consumers read pre-filtered.', kind:'JIT', tone:'crimson' },
            { sav:'$2,140/mo', t:'Profile payments_pii_redact.wasm regression', why:'parse cost up 280% week-over-week. Likely added a nested loop in v0.4.2 — earlier versions ran for 6.2M WCC; this runs for 42.1M WCC at the same volume.', kind:'WASM', tone:'crimson' },
            { sav:'$1,180/mo', t:'Switch stripe-events webhook to async ack', why:'p95 +482ms inflates Latency Tax. Their endpoint accepts queued mode; current SLA is sync.', kind:'WEB', tone:'amber' },
            { sav:'$  920/mo', t:'Batch your tombstone deletes', why:'12k loose tombstones in last 24h = 12k × 200 fuel = 2.4M WCC. Batched deletes at 1k each = 12 ops = 2.4k WCC. Net: 1000× cheaper.', kind:'TOMB', tone:'amber' },
            { sav:'$  640/mo', t:'Cache analytics.cold_scan_30d into hot view', why:'Nightly 8.4M WCC scan against S3 — same query, mostly identical results. Materialized view refreshes on changed partitions only.', kind:'S3', tone:'jade' },
            { sav:'$  340/mo', t:'Reduce gmv_by_segment_1m window state', why:'1m tumbling over 24h held in L3 slabs = MB×s tax. Drop archived segments from key space; pin only active ones.', kind:'WIN', tone:'jade' },
          ].map((f,i)=>(
            <div key={i} className="cf-fix">
              <div className="cf-fix-h">
                <span className={'cf-fix-k ' + f.tone}>{f.kind}</span>
                <span className="cf-fix-sav mono">{f.sav}</span>
              </div>
              <div className="cf-fix-t">{f.t}</div>
              <div className="cf-fix-w">{f.why}</div>
              <div className="cf-fix-act mono">
                <button className="btn ghost" style={{height:24,padding:'0 8px',fontSize:11}}>Apply</button>
                <button className="btn ghost" style={{height:24,padding:'0 8px',fontSize:11}}>Dismiss</button>
                <span className="dim" style={{marginLeft:'auto'}}>est. impact in 2h</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </Shell>
  );
}

/* ──────────────────────────────────────────────────────────────
   4. Customer single-job drilldown
   ──────────────────────────────────────────────────────────── */
function CustomerJobDrill() {
  return (
    <Shell active="fuel" breadcrumb={['acme','prod','us-east-2','fuel','filters','payments_pii_redact']}
      title="payments_pii_redact.wasm"
      sub="WASM filter · v0.4.2 (deployed 6d ago) · 142k invocations/s on payments.events"
      actions={<>
        <button className="btn ghost">Rollback to v0.4.1</button>
        <button className="btn ghost">Compare versions</button>
        <button className="btn primary">Open in editor</button>
      </>}>

      <div className="panel sys-node-hero">
        <div className="l">
          <div className="hero-st">
            <span className="pill crimson" style={{height:22}}><span className="dot"/>+312% WCC vs 7d · regression suspected in v0.4.2</span>
            <span className="mono dim">$542/h burn · 30% of org spend · materialize-into-topic could save $4,820/mo</span>
          </div>
          <div className="hero-meta mono">
            <div><span className="k">filter</span><span>payments_pii_redact.wasm</span></div>
            <div><span className="k">version</span><span>0.4.2 · 6d ago</span></div>
            <div><span className="k">topic</span><span>payments.events</span></div>
            <div><span className="k">invocations</span><span>142k /s</span></div>
            <div><span className="k">fuel / record</span><span style={{color:'var(--crimson)'}}>296 WCC (was 44)</span></div>
            <div><span className="k">wasm fuel limit</span><span>500k stateful</span></div>
            <div><span className="k">trap rate</span><span>0 /min · ok</span></div>
            <div><span className="k">commit</span><span>git:a3f81c2 · @j.lee</span></div>
          </div>
        </div>
        <div className="r">
          <div className="rps">
            <div className="big">$542<span className="u">/h</span></div>
            <div className="dim mono">burn rate · this filter only</div>
            <Spark data={genSeries(60, 360, 80, 0.6, 53)} h={48} w={320} color="var(--crimson)"/>
          </div>
        </div>
      </div>

      <div className="sys-node-grid">
        <div className="panel sys-chart sp2">
          <div className="panel-h">
            <div><div className="title">Fuel per record · 14d</div><div className="sub mono">deploy markers · regression starts at v0.4.2</div></div>
            <div className="ck mono"><span><i style={{background:'var(--rust)'}}/>WCC / record</span><span><i style={{background:'var(--ink-3)'}}/>p95</span></div>
          </div>
          <ChartLines series={[
            { color:'var(--rust)',  data:[...genSeries(40, 44, 4, 0, 61), ...genSeries(40, 296, 28, 0, 67)] },
            { color:'var(--ink-3)',  data:[...genSeries(40, 58, 6, 0, 71), ...genSeries(40, 348, 38, 0, 73)] },
          ]} h={200} markers={[{ at:0.5, label:'v0.4.2', color:'var(--crimson)' }]}/>
        </div>

        <div className="panel sys-chart">
          <div className="panel-h"><div><div className="title">Hottest opcodes</div><div className="sub mono">Wasmtime instruction count · top 6</div></div></div>
          <div style={{padding:'14px 16px',display:'flex',flexDirection:'column',gap:9}}>
            {[
              ['memory.fill · 0x4180', 0.34, 'crimson', 'newly hot in v0.4.2'],
              ['simd.v128.load',       0.22, 'amber',   'json scan inner loop'],
              ['i32.div_s',            0.14, 'amber',   'mask offset compute'],
              ['call (hash_blob)',     0.10, 'mute',    'baseline'],
              ['memory.copy',          0.08, 'mute',    'baseline'],
              ['local.get / set',      0.06, 'mute',    'baseline'],
            ].map(([k,v,s,note])=>(
              <div key={k} style={{display:'grid',gridTemplateColumns:'160px 1fr 38px',gap:8,alignItems:'center',fontSize:11}}>
                <span className="mono" style={{color:'var(--ink-1)'}}>{k}</span>
                <div style={{display:'flex',flexDirection:'column',gap:3}}>
                  <div style={{height:6,background:'var(--bg-0)',borderRadius:2,overflow:'hidden'}}>
                    <div style={{width:(v*100)+'%',height:'100%',background:s==='crimson'?'var(--crimson)':s==='amber'?'var(--amber)':'var(--ink-3)'}}/>
                  </div>
                  <span className="mono dim" style={{fontSize:9.5}}>{note}</span>
                </div>
                <span className="mono r" style={{textAlign:'right',color:s==='crimson'?'var(--crimson)':'var(--ink-2)'}}>{Math.round(v*100)}%</span>
              </div>
            ))}
          </div>
        </div>

        <div className="panel sys-chart sp2">
          <div className="panel-h"><div><div className="title">Source diff · v0.4.1 → v0.4.2</div><div className="sub mono">redact() — added nested array walk inside the hot loop</div></div></div>
          <div style={{padding:'14px 18px',fontFamily:'JetBrains Mono, monospace',fontSize:12,lineHeight:1.6,overflow:'auto'}}>
            <div className="mono dim" style={{fontSize:11,marginBottom:6}}>filter.rs · line 84-104</div>
            <div style={{color:'var(--ink-2)'}}>  for field in PII_FIELDS {'{'}</div>
            <div style={{color:'var(--ink-2)'}}>    if let Some(v) = record.get(field) {'{'}</div>
            <div style={{background:'oklch(from var(--crimson) l c h / 0.10)',color:'var(--crimson)'}}>+     for sub in v.descend_arrays() {'{'} // O(n) per record</div>
            <div style={{background:'oklch(from var(--crimson) l c h / 0.10)',color:'var(--crimson)'}}>+       redact_inplace(sub, &MASK);</div>
            <div style={{background:'oklch(from var(--crimson) l c h / 0.10)',color:'var(--crimson)'}}>+     {'}'}</div>
            <div style={{background:'oklch(from var(--jade) l c h / 0.10)',color:'var(--jade)'}}>−     redact_inplace(v, &MASK);</div>
            <div style={{color:'var(--ink-2)'}}>    {'}'}</div>
            <div style={{color:'var(--ink-2)'}}>  {'}'}</div>
            <div className="mono" style={{color:'var(--ink-3)',marginTop:10,fontSize:11}}>// v0.4.2 walks every nested array — payments.events records contain line_items[] which averages 14 entries. Cost: O(n × 14) vs O(n).</div>
          </div>
        </div>

        <div className="panel sys-chart">
          <div className="panel-h"><div><div className="title">Versions deployed</div><div className="sub mono">recent · click to rollback</div></div></div>
          <div style={{padding:'10px 16px',display:'flex',flexDirection:'column',gap:0}}>
            {[
              ['v0.4.2', 'current', '6d ago', '296 WCC/rec', 'crimson'],
              ['v0.4.1', 'prior',   '21d ago','44 WCC/rec',  'jade'],
              ['v0.4.0', '',        '38d ago','42 WCC/rec',  'jade'],
              ['v0.3.8', '',        '62d ago','48 WCC/rec',  'jade'],
              ['v0.3.7', '',        '94d ago','51 WCC/rec',  'jade'],
            ].map((v,i)=>(
              <div key={i} style={{display:'grid',gridTemplateColumns:'auto 1fr auto',gap:10,padding:'9px 0',borderTop:i?'1px solid var(--line-1)':'0',alignItems:'center'}}>
                <span className="mono" style={{fontWeight:600,color:v[4]==='crimson'?'var(--crimson)':'var(--ink-1)'}}>{v[0]}</span>
                <span className="mono dim" style={{fontSize:11}}>{v[1] || '·'} · {v[2]}</span>
                <span className="mono" style={{fontSize:11,color:v[4]==='crimson'?'var(--crimson)':'var(--jade)'}}>{v[3]}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="panel cf-fixes">
        <div className="panel-h"><div><div className="title">Suggested actions</div><div className="sub mono">for this filter</div></div></div>
        <div className="cf-fix-grid">
          <div className="cf-fix">
            <div className="cf-fix-h"><span className="cf-fix-k jade">ROLLBACK</span><span className="cf-fix-sav mono">$4,820/mo</span></div>
            <div className="cf-fix-t">Rollback to v0.4.1</div>
            <div className="cf-fix-w">v0.4.1 ran at 44 WCC/record across 7 stable weeks. Rolling back returns burn to baseline.</div>
          </div>
          <div className="cf-fix">
            <div className="cf-fix-h"><span className="cf-fix-k amber">PATCH</span><span className="cf-fix-sav mono">$4,200/mo</span></div>
            <div className="cf-fix-t">Hoist descend_arrays() out of hot loop</div>
            <div className="cf-fix-w">Compute the array-paths once per schema, cache, walk by index. Keeps the new redaction semantics without the O(n²) blow-up.</div>
          </div>
          <div className="cf-fix">
            <div className="cf-fix-k-row" style={{display:'flex',gap:8,alignItems:'center'}}><span className="cf-fix-k crimson">MATERIALIZE</span><span className="cf-fix-sav mono">$5,140/mo</span></div>
            <div className="cf-fix-t">Write filter result into payments.events.redacted</div>
            <div className="cf-fix-w">If most consumers read the redacted version anyway, run the filter once at write time and bypass the 5× JIT tax on every replay.</div>
          </div>
        </div>
      </div>
    </Shell>
  );
}

Object.assign(window, { PlatformFuel, PlatformOrgDrill, CustomerFuel, CustomerJobDrill });
