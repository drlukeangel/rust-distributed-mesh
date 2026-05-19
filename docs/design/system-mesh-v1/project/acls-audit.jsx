// Rafka — ACLs + audit log

function AclsAudit({ defaultTab = 'acl' }) {
  const [tab, setTab] = React.useState(defaultTab);

  const principals = [
    { id: 'svc:orders-api',       kind: 'service', av: 'OA', svc: true,  perms: ['allow','allow','none','none','none','allow','none'] },
    { id: 'svc:fulfillment-wrk',  kind: 'service', av: 'FW', svc: true,  perms: ['none','allow','allow','allow','none','allow','none'] },
    { id: 'svc:analytics-rw',     kind: 'service', av: 'AR', svc: true,  perms: ['none','allow','none','allow','none','allow','none'] },
    { id: 'svc:clickstream-etl',  kind: 'service', av: 'CE', svc: true,  perms: ['none','allow','allow','allow','none','allow','none'] },
    { id: 'svc:audit-stream',     kind: 'service', av: 'AS', svc: true,  perms: ['none','allow','none','none','none','allow','none'] },
    { id: 'usr:j.lee',            kind: 'user',    av: 'JL', svc: false, perms: ['allow','allow','allow','allow','allow','allow','allow'] },
    { id: 'usr:m.singh',          kind: 'user',    av: 'MS', svc: false, perms: ['inh','inh','inh','inh','none','allow','none'] },
    { id: 'usr:p.tanaka',         kind: 'user',    av: 'PT', svc: false, perms: ['inh','inh','none','inh','none','allow','none'] },
    { id: 'grp:on-call',          kind: 'group',   av: 'OC', svc: false, perms: ['inh','allow','allow','allow','allow','allow','none'] },
    { id: 'grp:contractors',      kind: 'group',   av: 'CT', svc: false, perms: ['none','allow','none','none','none','none','none'] },
    { id: 'svc:legacy-mirror',    kind: 'service', av: 'LM', svc: true,  perms: ['none','allow','deny','deny','none','allow','none'] },
  ];
  const ops = ['CREATE', 'READ', 'WRITE', 'DELETE', 'ALTER', 'DESCRIBE', 'CLUSTER_ACTION'];

  const events = [
    { ts: '18:42:11', kind: 'acl',    who: 'usr:j.lee',     body: <>granted <span className="obj">WRITE</span> on <span className="obj">orders.v2</span> to <span className="obj">svc:fulfillment-wrk</span></>, det: 'principal=User:fulfillment-wrk  resource=Topic:orders.v2  op=WRITE  permission=ALLOW  host=*' },
    { ts: '18:41:46', kind: 'config', who: 'usr:j.lee',     body: <>updated <b>retention.ms</b> on <span className="obj">clickstream.raw</span> from <b>604800000</b> to <b>1209600000</b></>, det: null },
    { ts: '18:39:12', kind: 'login',  who: 'svc:fulfillment-wrk', body: <>authenticated via <b>OIDC</b> from <span className="obj">ip-10-0-12-44</span></>, det: null },
    { ts: '18:36:02', kind: 'topic',  who: 'usr:m.singh',   body: <>created topic <span className="obj">risk.flagged</span> · partitions=6 · rf=3 · cleanup=delete</>, det: 'configured: retention.ms=86400000  segment.bytes=1073741824' },
    { ts: '18:31:55', kind: 'group',  who: 'svc:analytics-rw',   body: <>committed offsets on <span className="obj">orders.v2</span> · partitions [0..23]</>, det: null },
    { ts: '18:28:41', kind: 'acl',    who: 'usr:j.lee',     body: <>revoked <span className="obj">WRITE</span> on <span className="obj">payments.audit</span> from <span className="obj">grp:contractors</span></>, det: null },
    { ts: '18:24:08', kind: 'schema', who: 'usr:j.lee',     body: <>registered <span className="obj">orders-value</span> <b>v5</b> · backward-compatible</>, det: '+ promo_code: ["null","string"] · default null' },
    { ts: '18:18:30', kind: 'broker', who: 'system',        body: <>broker <span className="obj">broker-4</span> entered <b>degraded</b> state · isr=2/3 on 47 partitions</>, det: 'follower broker-7 (use2-c-01) failed to fetch within replica.lag.time.max.ms=30s' },
    { ts: '18:12:14', kind: 'login',  who: 'usr:p.tanaka',  body: <>authenticated via <b>OIDC + MFA</b> from <span className="obj">203.0.113.42</span> (San Francisco, US)</>, det: null },
    { ts: '17:58:01', kind: 'connect',who: 'usr:j.lee',     body: <>paused connector <span className="obj">sf-audit-cold</span></>, det: null },
    { ts: '17:42:20', kind: 'group',  who: 'svc:clickstream-etl',body: <>rebalanced consumer group · members 7 → 8 · 64 partitions reassigned</>, det: 'trigger: new member joined (host=ip-10-0-14-91)' },
    { ts: '17:35:09', kind: 'acl',    who: 'system',        body: <>denied <span className="obj">CREATE</span> on <span className="obj">__internal.flink_checkpoints</span> for <span className="obj">grp:contractors</span></>, det: 'no matching ALLOW rule  ·  principal not in any allow-list for cluster-action' },
  ];

  const kindColor = { acl: 'rust', config: 'amber', login: 'ice', topic: 'jade', group: 'ice', schema: 'rust', broker: 'crimson', connect: 'amber' };

  return (
    <Shell
      active="acls"
      breadcrumb={['acme', 'prod', 'us-east-2', 'acls & audit']}
      title="acls & audit"
      actions={<>
        <button className="btn ghost">Export</button>
        <button className="btn primary">+ New rule</button>
      </>}
    >
      <div className="panel" style={{ padding: 0 }}>
        <div className="aa-tabs">
          <span className={'tab' + (tab === 'acl' ? ' on' : '')} onClick={() => setTab('acl')}>permissions matrix</span>
          <span className={'tab' + (tab === 'audit' ? ' on' : '')} onClick={() => setTab('audit')}>audit log  ·  342 events / 24h</span>
          <span className="tab">policies</span>
          <span className="tab">api keys</span>
        </div>

        {tab === 'acl' ? (
          <>
            <div className="aa-bar">
              <div className="field"><span className="lbl">resource</span><span className="val">Topic:orders.v2</span><span style={{ marginLeft: 'auto', color: 'var(--ink-4)' }}>▾</span></div>
              <div className="field"><span className="lbl">pattern</span><span className="val">LITERAL</span><span style={{ marginLeft: 'auto', color: 'var(--ink-4)' }}>▾</span></div>
              <span className="chip on">all principals</span>
              <span className="chip">services</span>
              <span className="chip">users</span>
              <span className="chip">groups</span>
              <span style={{ flex: 1 }}></span>
              <span style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 11, color: 'var(--ink-3)' }}>
                <span className="perm allow" style={{ width: 14, height: 14, fontSize: 9, verticalAlign: 'middle', marginRight: 4 }}>✓</span>allow
                <span className="perm deny"  style={{ width: 14, height: 14, fontSize: 9, verticalAlign: 'middle', margin: '0 4px 0 12px' }}>✗</span>deny
                <span className="perm inh"   style={{ width: 14, height: 14, fontSize: 9, verticalAlign: 'middle', margin: '0 4px 0 12px' }}>↳</span>inherited
                <span className="perm none"  style={{ width: 14, height: 14, fontSize: 9, verticalAlign: 'middle', margin: '0 4px 0 12px' }}>·</span>none
              </span>
            </div>

            <div className="aa-matrix">
              <div className="th l">principal</div>
              {ops.map((o) => <div key={o} className="th">{o}</div>)}
              <div className="th"></div>

              {principals.map((p) => (
                <div key={p.id} className="row">
                  <div className="td l princ">
                    <span className={'ico ' + (p.svc ? 'svc' : '')}>{p.av}</span>
                    <span>{p.id}</span>
                    <span className="kind">{p.kind}</span>
                  </div>
                  {p.perms.map((perm, i) => (
                    <div key={i} className="td">
                      <span className={'perm ' + perm}>{perm === 'allow' ? '✓' : perm === 'deny' ? '✗' : perm === 'inh' ? '↳' : '·'}</span>
                    </div>
                  ))}
                  <div className="td"><button className="btn ghost" style={{ height: 24, padding: '0 8px', fontSize: 11 }}>edit</button></div>
                </div>
              ))}
            </div>

            <div style={{ padding: '18px 28px', borderTop: '1px solid var(--line-1)' }}>
              <div className="term">
                <div className="term-head"><span className="lights"><i /><i /><i /></span><span>cli equivalent · grant write to fulfillment</span></div>
                <div className="term-body" style={{ padding: '10px 14px' }}>
                  <div><span className="prompt">$</span> rafka <span className="arg">acl grant</span> <span className="flag">--principal</span> <span style={{ color: 'var(--violet)' }}>svc:fulfillment-wrk</span> <span className="flag">--op</span> <span style={{ color: 'var(--violet)' }}>WRITE</span> <span className="flag">--resource</span> <span style={{ color: 'var(--violet)' }}>topic/orders.v2</span></div>
                  <div><span className="dim"># preview: 1 rule will be created · no deny conflicts</span></div>
                  <div><span className="ok">✓</span> rule <span style={{ color: 'var(--rust)' }}>acl-7f3a91</span> created</div>
                </div>
              </div>
            </div>
          </>
        ) : (
          <>
            <div className="aa-bar">
              <div className="field" style={{ flex: 1, maxWidth: 360 }}>
                <Icon name="search" />
                <input placeholder="actor, resource, action…" style={{ background: 'transparent', border: 0, outline: 0, color: 'var(--ink-1)', font: 'inherit', flex: 1, minWidth: 0 }} />
                <span className="kbd" style={{ marginLeft: 'auto' }}>/</span>
              </div>
              <span className="chip on">all kinds</span>
              <span className="chip">acl</span>
              <span className="chip">login</span>
              <span className="chip">topic</span>
              <span className="chip">schema</span>
              <span className="chip">broker</span>
              <span style={{ flex: 1 }}></span>
              <div className="field"><span className="lbl">window</span><span className="val">last 24h</span><span style={{ marginLeft: 'auto', color: 'var(--ink-4)' }}>▾</span></div>
            </div>

            <div className="aa-feed">
              {events.map((e, i) => (
                <div key={i} className="aa-event">
                  <span className="ts">{e.ts}<span style={{ display: 'block', color: 'var(--ink-4)', marginTop: 2 }}>2026-05-11</span></span>
                  <div className="body">
                    <span className="who">{e.who}</span> {e.body}
                    {e.det && <span className="det">{e.det}</span>}
                  </div>
                  <div className="kind"><span className={'pill ' + (kindColor[e.kind] || '')}>{kindColor[e.kind] && <span className="dot" />}{e.kind}</span></div>
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </Shell>
  );
}

Object.assign(window, { AclsAudit });
