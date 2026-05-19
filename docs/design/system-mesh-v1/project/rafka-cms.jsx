// Rafka — SaaS overview seen as a CMS
// "Content model" = streaming primitives. Topics, Schemas, Connectors, Flink Jobs,
// SQL Views, ACLs, Datasets become content types with drafts, scheduled publishes,
// editorial review, collaborators, an activity feed. Re-framing infra ops as content ops.

function RafkaCms() {
  return (
    <Shell active="home"
      breadcrumb={['acme', 'workspaces', 'rafka-prod', 'overview']}
      title="rafka · prod workspace"
      sub="content workspace · 1 environment · 14 collaborators · last publish 4m ago"
      actions={<>
        <button className="btn ghost">Environments · 3</button>
        <button className="btn ghost">Roles &amp; access</button>
        <button className="btn primary">+ New entry ▾</button>
      </>}>

      {/* Workspace strip */}
      <div className="panel cms-strip">
        <div className="ws">
          <div className="ws-tile">A</div>
          <div className="ws-info">
            <div className="ws-name">Acme · production <span className="ws-tag mono">prod</span></div>
            <div className="ws-meta mono">space_id 8f1a · region us-east-2 · plan Enterprise · 14 seats</div>
          </div>
          <div className="ws-envs mono">
            <span className="env on">prod</span>
            <span className="env">staging</span>
            <span className="env">dev</span>
            <span className="env add">+ env</span>
          </div>
        </div>
        <div className="ws-search mono">
          <span className="ic">⌕</span>
          <input placeholder="search content · topics, schemas, jobs, principals…" defaultValue="" />
          <span className="kbd">⌘K</span>
        </div>
        <div className="ws-quota mono">
          <div className="quota">
            <div className="lbl">entries this month</div>
            <div className="bar"><i style={{width:'62%'}}/></div>
            <div className="val">8,412 <span className="dim">/ 13,500</span></div>
          </div>
        </div>
      </div>

      {/* Body grid: model · main · right rail */}
      <div className="cms-grid">

        {/* ── Content model rail ── */}
        <div className="panel cms-model">
          <div className="cms-rail-h mono">content model</div>
          {[
            { ic:'T', cls:'topic', nm:'Topic',        ct:142, draft:6, k:'streams' },
            { ic:'S', cls:'schema', nm:'Schema',      ct:184, draft:3, k:'streams' },
            { ic:'V', cls:'view',  nm:'SQL view',     ct:38,  draft:2, k:'streams' },
            { ic:'C', cls:'conn',  nm:'Connector',    ct:24,  draft:1, k:'streams' },
            { ic:'F', cls:'flink', nm:'Flink job',    ct:18,  draft:4, k:'streams' },
            { ic:'D', cls:'dataset', nm:'Dataset',    ct:62,  draft:0, k:'taxonomies' },
            { ic:'G', cls:'group', nm:'Consumer grp', ct:96,  draft:0, k:'taxonomies' },
            { ic:'P', cls:'acl',   nm:'Principal',    ct:48,  draft:2, k:'access' },
            { ic:'R', cls:'acl',   nm:'ACL rule',     ct:204, draft:8, k:'access' },
            { ic:'A', cls:'asset', nm:'Asset',        ct:18,  draft:0, k:'media' },
          ].map((m,i,arr) => {
            const showCat = i === 0 || arr[i-1].k !== m.k;
            const active = i === 0;
            return (
              <React.Fragment key={m.nm}>
                {showCat && <div className="cms-rail-cat mono">{m.k}</div>}
                <div className={'cms-rail-i' + (active ? ' on' : '')}>
                  <span className={'mt ' + m.cls}>{m.ic}</span>
                  <span className="nm">{m.nm}</span>
                  <span className="ct mono">{m.ct}</span>
                  {m.draft > 0 && <span className="dr mono">{m.draft}</span>}
                </div>
              </React.Fragment>
            );
          })}
          <div className="cms-rail-foot mono">
            <span className="dim">+ Content type</span>
          </div>
        </div>

        {/* ── Main column ── */}
        <div className="cms-main">

          {/* KPI strip */}
          <div className="panel cms-kpis">
            {[
              { lbl:'total entries',    val:'714',  delta:'+18 this week', tone:'jade' },
              { lbl:'published',        val:'682',  delta:'95.5%',          tone:'jade' },
              { lbl:'drafts',           val:'26',   delta:'+4 yesterday',   tone:'amber' },
              { lbl:'in review',        val:'11',   delta:'avg age 1.4d',   tone:'ice' },
              { lbl:'broken refs',      val:'3',    delta:'orders.v3 missing', tone:'crimson' },
              { lbl:'publish rate · 24h', val:'42', delta:'p95 11s deploy', tone:'jade' },
            ].map(k => (
              <div key={k.lbl} className="kpi">
                <div className="lbl mono">{k.lbl}</div>
                <div className="val">{k.val}</div>
                <div className={'delta mono ' + k.tone}>{k.delta}</div>
              </div>
            ))}
          </div>

          {/* Editorial pipeline kanban */}
          <div className="panel cms-pipe">
            <div className="panel-h">
              <div><div className="title">Editorial pipeline</div><div className="sub mono">contracts, schemas &amp; jobs awaiting publish · drag between columns to advance</div></div>
              <div className="seg mono"><span className="seg-i on">all types</span><span className="seg-i">schemas</span><span className="seg-i">jobs</span><span className="seg-i">acl</span></div>
            </div>
            <div className="kanban">
              {[
                { col:'Draft',     ct:9, tone:'mute', cards:[
                  { t:'orders-value · v6',     k:'schema', who:'j.lee',   age:'12m', tag:'breaking' },
                  { t:'payments.events → s3',  k:'conn',   who:'r.silva', age:'2h',  tag:'sink' },
                  { t:'sessionize_click.sql',  k:'flink',  who:'m.ortiz', age:'8h',  tag:'job' },
                  { t:'sa_etl_batch · ACLs',   k:'acl',    who:'a.park',  age:'1d',  tag:'principal' },
                ]},
                { col:'In review', ct:5, tone:'ice', cards:[
                  { t:'clickstream-value · v3', k:'schema', who:'k.huang', age:'4h',  tag:'2 reviewers', rev:[{n:'JL',ok:1},{n:'MO',ok:0}] },
                  { t:'orders-enrich · par 12', k:'flink',  who:'j.lee',   age:'6h',  tag:'await dba',  rev:[{n:'AP',ok:0}] },
                  { t:'gmv_by_segment_1m view', k:'view',   who:'m.ortiz', age:'1d',  tag:'sql 28L',    rev:[{n:'JL',ok:1}] },
                ]},
                { col:'Scheduled', ct:3, tone:'amber', cards:[
                  { t:'orders-value · v5 → v6', k:'schema', who:'j.lee',   age:'launch 17:00 UTC', tag:'rolling' },
                  { t:'snowflake-sink-prod',    k:'conn',   who:'r.silva', age:'launch tomorrow 09:00', tag:'maint window' },
                ]},
                { col:'Published', ct:14, tone:'jade', cards:[
                  { t:'orders-value · v5',     k:'schema', who:'j.lee',  age:'4m',   tag:'live' },
                  { t:'payments-enrich job',   k:'flink',  who:'a.park', age:'31m',  tag:'live · par 8' },
                  { t:'orders.refunds topic',  k:'topic',  who:'k.huang',age:'2h',   tag:'24 part' },
                  { t:'analyst_ro · read ACL', k:'acl',    who:'m.ortiz',age:'5h',   tag:'29 topics' },
                ]},
              ].map(col => (
                <div key={col.col} className="kcol">
                  <div className="kh">
                    <span className={'dot ' + col.tone}/>
                    <span className="mono nm">{col.col}</span>
                    <span className="mono ct">{col.ct}</span>
                  </div>
                  {col.cards.map((c,i) => (
                    <div key={i} className="kcard">
                      <div className="khd">
                        <span className={'kt ' + c.k}>{c.k}</span>
                        <span className="mono age">{c.age}</span>
                      </div>
                      <div className="kt2">{c.t}</div>
                      <div className="kf">
                        <span className="who mono">{c.who}</span>
                        <span className="tag mono">{c.tag}</span>
                      </div>
                      {c.rev && (
                        <div className="krev">
                          {c.rev.map((r,j) => (
                            <span key={j} className={'rv ' + (r.ok?'ok':'pending')}>
                              <i/>{r.n}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
                  <button className="kadd mono">+ add</button>
                </div>
              ))}
            </div>
          </div>

          {/* Recent entries table (content list) */}
          <div className="panel cms-list">
            <div className="panel-h">
              <div><div className="title">Recent entries</div><div className="sub mono">unified across content types · sort by updated</div></div>
              <div className="cms-list-tools mono">
                <span className="chip on">all</span>
                <span className="chip">topic 142</span>
                <span className="chip">schema 184</span>
                <span className="chip">flink 18</span>
                <span className="chip">conn 24</span>
                <span className="chip">view 38</span>
                <span className="sp"/>
                <span className="seg-i on mono">grid</span>
                <span className="seg-i mono">list</span>
              </div>
            </div>
            <div className="cms-tbl-h mono">
              <div></div>
              <div>name</div>
              <div>type</div>
              <div>status</div>
              <div>environment</div>
              <div>owner</div>
              <div>updated</div>
              <div className="r">refs</div>
            </div>
            {[
              ['T','topic','orders.v2',            'topic', 'published', 'prod',     'j.lee',   '4m ago',  '38 in / 12 out'],
              ['S','schema','orders-value',        'schema','published', 'prod',     'j.lee',   '4m ago',  '142 bindings'],
              ['F','flink','orders-enrich · 1.18', 'job',   'published', 'prod',     'a.park',  '31m ago', '4 topics'],
              ['S','schema','clickstream-value',   'schema','review',    'staging',  'k.huang', '4h ago',  '3 bindings'],
              ['V','view','gmv_by_segment_1m',     'view',  'review',    'staging',  'm.ortiz', '1d ago',  '1 source'],
              ['C','conn','snowflake-sink-prod',   'conn',  'scheduled', 'prod',     'r.silva', '2d ago',  '1 topic'],
              ['S','schema','orders-value',        'schema','draft',     'dev',      'j.lee',   '12m ago', '2 reviewers'],
              ['F','flink','sessionize_click.sql', 'job',   'draft',     'dev',      'm.ortiz', '8h ago',  '1 topic'],
              ['T','topic','jobs-active',          'topic', 'published', 'prod',     'system',  '5h ago',  '12 in / 4 out'],
              ['R','acl','analyst_ro · read',      'acl',   'published', 'prod',     'm.ortiz', '5h ago',  '29 topics'],
            ].map((r,i) => (
              <div key={i} className="cms-tbl-row">
                <div><span className={'mt ' + r[1]}>{r[0]}</span></div>
                <div className="mono nm">{r[2]}</div>
                <div className="mono dim">{r[3]}</div>
                <div><span className={'st ' + r[4]}>{r[4]}</span></div>
                <div className="mono"><span className={'env-pill ' + r[5]}>{r[5]}</span></div>
                <div className="mono">{r[6]}</div>
                <div className="mono dim">{r[7]}</div>
                <div className="mono r dim">{r[8]}</div>
              </div>
            ))}
            <div className="cms-tbl-foot mono">
              <span className="dim">showing 10 of 472</span>
              <span style={{marginLeft:'auto'}} className="dim">page 1 of 48 · ← →</span>
            </div>
          </div>

          {/* Asset library + scheduled bottom row */}
          <div className="cms-bot">
            <div className="panel cms-assets">
              <div className="panel-h">
                <div><div className="title">Asset library</div><div className="sub mono">connector binaries · wasm modules · jars · 18 assets · 412 MB</div></div>
                <button className="btn ghost" style={{height:24,padding:'0 8px',fontSize:11}}>Upload</button>
              </div>
              <div className="assets-grid">
                {[
                  { nm:'snowflake-sink',  v:'2.1.0', sz:'14 MB', kind:'jar',  tone:'rust' },
                  { nm:'debezium-pg',     v:'2.5.1', sz:'22 MB', kind:'jar',  tone:'rust' },
                  { nm:'dq-enrich.wasm',  v:'0.4.2', sz:'1.2 MB',kind:'wasm', tone:'ember' },
                  { nm:'pii-redact.wasm', v:'0.2.0', sz:'0.8 MB',kind:'wasm', tone:'ember' },
                  { nm:'s3-tiered',       v:'1.0.0', sz:'18 MB', kind:'jar',  tone:'rust' },
                  { nm:'kinesis-source',  v:'1.4.0', sz:'12 MB', kind:'jar',  tone:'rust' },
                  { nm:'webhook-fanout',  v:'0.9.1', sz:'0.6 MB',kind:'wasm', tone:'ember' },
                  { nm:'compiled_acls',   v:'binary',sz:'2 MB',  kind:'bin',  tone:'ice'  },
                ].map(a => (
                  <div key={a.nm} className="aset">
                    <div className={'tile ' + a.tone}>
                      <span className="mono">{a.kind}</span>
                    </div>
                    <div className="meta">
                      <div className="mono nm">{a.nm}</div>
                      <div className="mono sub">{a.v} · {a.sz}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="panel cms-sched">
              <div className="panel-h">
                <div><div className="title">Scheduled publishes</div><div className="sub mono">rollouts in the next 48h · click to reschedule</div></div>
                <span className="pill amber" style={{height:18,fontSize:10}}><span className="dot"/>2 today</span>
              </div>
              <div className="sched-tl">
                {[
                  { when:'today · 17:00 UTC', in:'2h 38m', t:'orders-value · v5 → v6', k:'schema',  tone:'amber', mode:'rolling · 24 part' },
                  { when:'today · 22:00 UTC', in:'7h 38m', t:'sa_compute_gateway · key rotation', k:'acl', tone:'amber', mode:'auto' },
                  { when:'tomorrow · 09:00',  in:'18h',    t:'snowflake-sink-prod', k:'conn',     tone:'ice',   mode:'maint window' },
                  { when:'tomorrow · 14:00',  in:'23h',    t:'orders-enrich par 8 → 12', k:'flink', tone:'ice', mode:'savepoint resume' },
                ].map((s,i) => (
                  <div key={i} className="sti">
                    <div className="ax">
                      <div className={'pt ' + s.tone}/>
                      {i < 3 && <div className="ln"/>}
                    </div>
                    <div className="bd">
                      <div className="t1 mono">
                        <span className={'kt ' + s.k}>{s.k}</span>
                        <span className="when">{s.when}</span>
                        <span className="in dim">in {s.in}</span>
                      </div>
                      <div className="t2">{s.t}</div>
                      <div className="t3 mono dim">{s.mode}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* ── Right rail: activity, collaborators, references ── */}
        <div className="cms-side">

          <div className="panel cms-act">
            <div className="panel-h">
              <div><div className="title">Activity</div><div className="sub mono">workspace · last 24h</div></div>
              <span className="mono dim">412 events</span>
            </div>
            {[
              { who:'j.lee',  act:'published',    obj:'orders-value · v5',    k:'schema', t:'4m ago',  tone:'jade' },
              { who:'a.park', act:'published',    obj:'payments-enrich job',  k:'flink',  t:'31m ago', tone:'jade' },
              { who:'m.ortiz',act:'commented',    obj:'gmv_by_segment_1m',    k:'view',   t:'46m ago', tone:'ice', body:'should we partition by segment to keep groupby local?' },
              { who:'k.huang',act:'requested review on', obj:'clickstream-value · v3', k:'schema', t:'4h ago', tone:'amber' },
              { who:'system', act:'scheduled',    obj:'orders-value v6',      k:'schema', t:'5h ago',  tone:'amber' },
              { who:'r.silva',act:'created draft',obj:'snowflake-sink-prod',  k:'conn',   t:'6h ago',  tone:'mute' },
              { who:'j.lee',  act:'archived',     obj:'legacy-orders topic',  k:'topic',  t:'1d ago',  tone:'crimson' },
              { who:'a.park', act:'rolled back',  obj:'orders-enrich v3 → v2',k:'flink',  t:'1d ago',  tone:'crimson' },
            ].map((a,i) => (
              <div key={i} className="act-i">
                <span className="av mono">{a.who.split('.').map(s=>s[0]).join('').toUpperCase()}</span>
                <div className="bd">
                  <div className="ln">
                    <span className="mono who">{a.who}</span>
                    <span className={'act mono ' + a.tone}>{a.act}</span>
                    <span className={'kt ' + a.k}>{a.k}</span>
                  </div>
                  <div className="obj mono">{a.obj}</div>
                  {a.body && <div className="body">{a.body}</div>}
                  <div className="ts mono dim">{a.t}</div>
                </div>
              </div>
            ))}
          </div>

          <div className="panel cms-collab">
            <div className="panel-h">
              <div><div className="title">Online now</div><div className="sub mono">4 of 14 collaborators</div></div>
            </div>
            {[
              { nm:'jamie lee',   role:'Editor · Schemas',   loc:'orders-value v6 (draft)',     on:1 },
              { nm:'aria park',   role:'Reviewer · Flink',   loc:'orders-enrich · pipeline',    on:1 },
              { nm:'kai huang',   role:'Editor · Schemas',   loc:'clickstream-value v3 · diff', on:1 },
              { nm:'maya ortiz',  role:'Editor · SQL',       loc:'gmv_by_segment_1m · editor',  on:1 },
              { nm:'rico silva',  role:'Editor · Connectors',loc:'snowflake-sink-prod',         on:0 },
              { nm:'devon ng',    role:'Admin',              loc:'audit log',                   on:0 },
            ].map(p => (
              <div key={p.nm} className="col-i">
                <span className="av mono">{p.nm.split(' ').map(s=>s[0]).join('').toUpperCase()}{p.on ? <i className="dot"/> : null}</span>
                <div className="bd">
                  <div className="nm">{p.nm}</div>
                  <div className="role mono dim">{p.role}</div>
                  <div className="loc mono">{p.loc}</div>
                </div>
              </div>
            ))}
          </div>

          <div className="panel cms-refs">
            <div className="panel-h">
              <div><div className="title">Reference health</div><div className="sub mono">cross-type links — drafts, breaks &amp; deprecations</div></div>
            </div>
            <div className="refs">
              <div className="ref bad">
                <div className="rh"><span className="t mono">broken reference</span><span className="ct mono">3</span></div>
                <div className="rb">connector <b>orders-cdc</b> targets schema <b>orders.v3</b> which doesn't exist in this env.</div>
              </div>
              <div className="ref warn">
                <div className="rh"><span className="t mono">deprecated, still referenced</span><span className="ct mono">8</span></div>
                <div className="rb">schema <b>orders-value · v3</b> deprecated 21d ago — used by 2 flink jobs, 1 view.</div>
              </div>
              <div className="ref info">
                <div className="rh"><span className="t mono">orphan</span><span className="ct mono">5</span></div>
                <div className="rb">5 topics have no consumers in 7 days — candidates for archival.</div>
              </div>
            </div>
          </div>

        </div>
      </div>
    </Shell>
  );
}

Object.assign(window, { RafkaCms });
