// Rafka — shared app shell, used inside each artboard.

const Icon = ({ name }) => {
  const paths = {
    grid:   <><rect x="2.5" y="2.5" width="4.5" height="4.5" rx="1"/><rect x="9" y="2.5" width="4.5" height="4.5" rx="1"/><rect x="2.5" y="9" width="4.5" height="4.5" rx="1"/><rect x="9" y="9" width="4.5" height="4.5" rx="1"/></>,
    topic:  <><rect x="2" y="3" width="12" height="2" rx="0.5"/><rect x="2" y="7" width="12" height="2" rx="0.5"/><rect x="2" y="11" width="9" height="2" rx="0.5"/></>,
    group:  <><circle cx="6" cy="6" r="2.4"/><circle cx="11" cy="9.5" r="2"/><path d="M2 13c0-2 1.8-3.4 4-3.4s4 1.4 4 3.4" fill="none" stroke="currentColor" strokeWidth="1.2"/></>,
    schema: <><path d="M8 2v12M3 5l5-3 5 3M3 11l5 3 5-3" fill="none" stroke="currentColor" strokeWidth="1.2"/></>,
    plug:   <><path d="M5 2v3M11 2v3M3 5h10v3a5 5 0 0 1-10 0V5zM8 13v1.5" fill="none" stroke="currentColor" strokeWidth="1.2"/></>,
    flow:   <><circle cx="3" cy="4" r="1.5"/><circle cx="13" cy="4" r="1.5"/><circle cx="8" cy="12" r="1.5"/><path d="M3 4l5 8M13 4l-5 8M3 4h10" fill="none" stroke="currentColor" strokeWidth="1.2"/></>,
    sql:    <><ellipse cx="8" cy="4" rx="5" ry="1.8" fill="none" stroke="currentColor" strokeWidth="1.2"/><path d="M3 4v8c0 1 2.2 1.8 5 1.8s5-.8 5-1.8V4" fill="none" stroke="currentColor" strokeWidth="1.2"/><path d="M3 8c0 1 2.2 1.8 5 1.8s5-.8 5-1.8" fill="none" stroke="currentColor" strokeWidth="1.2"/></>,
    lock:   <><rect x="3.5" y="7" width="9" height="6.5" rx="1"/><path d="M5.5 7V5a2.5 2.5 0 0 1 5 0v2" fill="none" stroke="currentColor" strokeWidth="1.2"/></>,
    gear:   <><circle cx="8" cy="8" r="2"/><path d="M8 1.5v2M8 12.5v2M1.5 8h2M12.5 8h2M3.5 3.5l1.4 1.4M11.1 11.1l1.4 1.4M3.5 12.5l1.4-1.4M11.1 4.9l1.4-1.4" stroke="currentColor" strokeWidth="1.2" fill="none"/></>,
    home:   <><path d="M2 8l6-5 6 5v6H2z" fill="none" stroke="currentColor" strokeWidth="1.2"/><path d="M6.5 14V10h3v4" fill="none" stroke="currentColor" strokeWidth="1.2"/></>,
    search: <><circle cx="7" cy="7" r="4" fill="none" stroke="currentColor" strokeWidth="1.4"/><path d="M10 10l3.5 3.5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round"/></>,
    bell:   <><path d="M4 11V7a4 4 0 1 1 8 0v4l1 1.5H3z" fill="none" stroke="currentColor" strokeWidth="1.2"/><path d="M6.5 13.5a1.5 1.5 0 0 0 3 0" fill="none" stroke="currentColor" strokeWidth="1.2"/></>,
    mesh:   <><circle cx="3" cy="3" r="1.6"/><circle cx="13" cy="3" r="1.6"/><circle cx="3" cy="13" r="1.6"/><circle cx="13" cy="13" r="1.6"/><circle cx="8" cy="8" r="1.6"/><path d="M3 3l5 5 5-5M3 13l5-5 5 5M3 3v10M13 3v10" fill="none" stroke="currentColor" strokeWidth="1" opacity="0.6"/></>,
    chevL:  <><path d="M10 3L5 8l5 5" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/></>,
    chevR:  <><path d="M6 3l5 5-5 5" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/></>,
  };
  return <svg viewBox="0 0 16 16" fill="currentColor">{paths[name]}</svg>;
};

function RailItem({ icon, label, meta, active }) {
  return (
    <div className={"rail-item" + (active ? " active" : "")}>
      <span className="ico"><Icon name={icon} /></span>
      <span className="txt">{label}</span>
      {meta && <span className="meta">{meta}</span>}
    </div>
  );
}

function Shell({ active, breadcrumb, title, sub, actions, children, collapsed: collapsedProp }) {
  const [collapsed, setCollapsed] = React.useState(() => {
    if (typeof collapsedProp === 'boolean') return collapsedProp;
    try { return localStorage.getItem('rafka.rail.collapsed') === '1'; } catch (_) { return false; }
  });
  // Cross-shell sync: when one Shell toggles, every other Shell on the page mirrors it.
  React.useEffect(() => {
    const onSync = (e) => setCollapsed(!!e.detail);
    window.addEventListener('rafka:rail-sync', onSync);
    return () => window.removeEventListener('rafka:rail-sync', onSync);
  }, []);
  const toggle = () => {
    const next = !collapsed;
    setCollapsed(next);
    try { localStorage.setItem('rafka.rail.collapsed', next ? '1' : '0'); } catch (_) {}
    window.dispatchEvent(new CustomEvent('rafka:rail-sync', { detail: next }));
  };
  return (
    <div className="app" data-rail={collapsed ? "collapsed" : "expanded"}>
      <aside className="rail">
        <div className="rail-brand">
          <span className="logo">R</span>
          <span className="name">rafka</span>
          <button className="rail-toggle" onClick={toggle} aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'} title={collapsed ? 'Expand' : 'Collapse'}>
            <Icon name={collapsed ? 'chevR' : 'chevL'} />
          </button>
        </div>

        <div className="rail-section">Cluster</div>
        <nav className="rail-nav">
          <RailItem icon="home"   label="Overview"        meta="⌘1" active={active === 'overview'} />
          <RailItem icon="topic"  label="Topics"          meta="⌘2" active={active === 'topics'} />
          <RailItem icon="group"  label="Consumer groups" meta="⌘3" active={active === 'groups'} />
          <RailItem icon="schema" label="Schema registry" meta="⌘4" active={active === 'schema'} />
        </nav>

        <div className="rail-section">Pipelines</div>
        <nav className="rail-nav">
          <RailItem icon="plug" label="Connectors" meta="⌘5" active={active === 'connectors'} />
          <RailItem icon="flow" label="Flink jobs" meta="⌘6" active={active === 'flink'} />
          <RailItem icon="sql"  label="SQL"        meta="⌘7" active={active === 'sql'} />
        </nav>

        <div className="rail-section">Platform</div>
        <nav className="rail-nav">
          <RailItem icon="mesh" label="System"   meta="⌘8" active={active === 'system'} />
        </nav>

        <div className="rail-section">Governance</div>
        <nav className="rail-nav">
          <RailItem icon="lock" label="ACLs &amp; audit" meta="⌘9" active={active === 'acls'} />
          <RailItem icon="gear" label="Settings"         meta="⌘,"  active={active === 'settings'} />
        </nav>

        <div className="rail-spacer" />
        <div className="rail-foot">
          <span className="av">JL</span>
          <span className="who"><div style={{ color: 'var(--ink-1)', fontSize: 12 }}>j.lee</div><div style={{ fontSize: 10.5, color: 'var(--ink-3)' }}>admin</div></span>
        </div>
      </aside>

      <main className="main">
        <div className="topbar">
          <nav className="bcrumb">
            {breadcrumb.map((b, i) => (
              <React.Fragment key={i}>
                {i > 0 && <span className="sl">/</span>}
                <span className={"seg " + (i === breadcrumb.length - 1 ? 'cur' : '')}>{b}</span>
              </React.Fragment>
            ))}
          </nav>
          <div className="grow" />
          <div className="search">
            <Icon name="search" />
            <span>Jump to topic, broker, command…</span>
            <span className="kbd" style={{ marginLeft: 'auto' }}>⌘ K</span>
          </div>
          <button className="btn ghost" aria-label="Notifications"><Icon name="bell" /></button>
        </div>

        <div className="main-inner">
          <div className="page-head">
            <div>
              <h1>{title}</h1>
              <div className="sub">
                <span className="live"><i />live</span>
                <span>·</span>
                <span className="mono">last refresh 2s ago</span>
                <span>·</span>
                <span className="mono">9 brokers · 142 topics · rf=3</span>
              </div>
            </div>
            {actions && <div className="actions">{actions}</div>}
          </div>

          {children}
        </div>
      </main>
    </div>
  );
}

Object.assign(window, { Shell, Icon, RailItem });
