// Rafka — Screens app: design canvas wrapping the 3 artboards + tweaks panel.

const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/{
  "accent": "rust"
}/*EDITMODE-END*/;

const ACCENTS = {
  rust:    { rust: 'oklch(0.74 0.18 50)',  rust2: 'oklch(0.62 0.20 38)',  soft: 'oklch(0.74 0.18 50 / 0.16)' },
  ember:   { rust: 'oklch(0.80 0.16 70)',  rust2: 'oklch(0.66 0.18 58)',  soft: 'oklch(0.80 0.16 70 / 0.16)' },
  crimson: { rust: 'oklch(0.70 0.22 25)',  rust2: 'oklch(0.58 0.24 20)',  soft: 'oklch(0.70 0.22 25 / 0.16)' },
};

function ScreensApp() {
  const [t, setTweak] = useTweaks(TWEAK_DEFAULTS);

  React.useEffect(() => {
    document.documentElement.setAttribute('data-theme', 'dark');
    const a = ACCENTS[t.accent] || ACCENTS.rust;
    const r = document.documentElement.style;
    r.setProperty('--rust', a.rust);
    r.setProperty('--rust-2', a.rust2);
    r.setProperty('--rust-soft', a.soft);
  }, [t.accent]);

  return (
    <>
      <DesignCanvas title="Rafka · v0.1 hero screens" subtitle="cluster overview (2 explorations) + topic browser">
        <DCSection id="cms" title="SaaS overview · CMS perspective" subtitle="Rafka reframed as a content workspace — streaming primitives (topics, schemas, jobs, connectors) become content types with drafts, scheduled publishes, reviewers, and an activity feed.">
          <DCArtboard id="cms-1" label="Rafka workspace · CMS overview" width={1640} height={1620}>
            <RafkaCms />
          </DCArtboard>
        </DCSection>
        <DCSection id="cluster" title="Cluster overview · hero" subtitle="Converged on Variant A — data-dense, sidebar expanded. The first thing you see when you log in.">
          <DCArtboard id="cluster-a" label="Cluster overview · hero" width={1440} height={1100}>
            <ClusterOverviewA />
          </DCArtboard>
        </DCSection>

        <DCSection id="topics" title="Topic browser + message inspector" subtitle="Topic list with live trends · inspector with replay, schema, config tabs.">
          <DCArtboard id="topics-1" label="Topic browser · orders.v2 selected" width={1440} height={900}>
            <TopicBrowser />
          </DCArtboard>
        </DCSection>

        <DCSection id="groups" title="Consumer groups + lag detail" subtitle="Where 3am pages start. Lag, partition assignment, per-member health, reset.">
          <DCArtboard id="groups-1" label="Consumer groups · orders-fulfillment selected" width={1440} height={1100}>
            <ConsumerGroups />
          </DCArtboard>
        </DCSection>

        <DCSection id="schema" title="Schema registry" subtitle="Subject list, version timeline, side-by-side diff, compatibility modes.">
          <DCArtboard id="schema-1" label="Schema registry · orders-value v5" width={1440} height={1000}>
            <SchemaRegistry />
          </DCArtboard>
        </DCSection>

        <DCSection id="system" title="System · platform / SRE view" subtitle="Live mesh of all 4 component types · per-type signal · single-node deep dive. The platform-owner perspective.">
          <DCArtboard id="system-mesh" label="1. System mesh — live topology + alerts + OTLP heartbeat" width={1640} height={1620}>
            <SystemMesh />
          </DCArtboard>
          <DCArtboard id="system-type" label="2. Component type — data-plane-gateway (5 instances)" width={1640} height={1400}>
            <SystemType />
          </DCArtboard>
          <DCArtboard id="system-node" label="3. Node — dpg-3 (data-plane-gateway)" width={1640} height={1640}>
            <SystemNode />
          </DCArtboard>
          <DCArtboard id="system-node-br" label="4. Node — br-5 (broker io-pump)" width={1640} height={1500}>
            <SystemNodeBroker />
          </DCArtboard>
          <DCArtboard id="system-node-cpg" label="5. Node — cpg-2 (compute-gateway, failing)" width={1640} height={1500}>
            <SystemNodeCompute />
          </DCArtboard>
        </DCSection>

        <DCSection id="connectors" title="Connectors" subtitle="Catalog of source/sink types · running instances · task-pool detail.">
          <DCArtboard id="connectors-1" label="Connectors · Snowflake sink selected" width={1440} height={1100}>
            <Connectors />
          </DCArtboard>
        </DCSection>

        <DCSection id="flink" title="Flink jobs" subtitle="Job list with backpressure spark · DAG · checkpoint history · per-operator metrics.">
          <DCArtboard id="flink-1" label="Flink jobs · orders-enrich selected" width={1440} height={1280}>
            <FlinkJobs />
          </DCArtboard>
        </DCSection>

        <DCSection id="sql" title="Streaming SQL editor — variations" subtitle="Four directions, from familiar to ambitious: classic IDE, notebook cells, visual pipeline canvas, AI-first assistant. Mix and match.">
          <DCArtboard id="sql-1" label="1. Classic IDE — schema + editor + live results" width={1440} height={1100}>
            <SqlEditor />
          </DCArtboard>
          <DCArtboard id="sql-2" label="2. Notebook — cells, inline output, promote-to-job" width={1440} height={1280}>
            <SqlNotebook />
          </DCArtboard>
          <DCArtboard id="sql-3" label="3. Pipeline canvas — visual node graph ⇄ SQL" width={1440} height={760}>
            <SqlCanvas />
          </DCArtboard>
          <DCArtboard id="sql-4" label="4. AI prompt-first — natural language → SQL → preview" width={1440} height={1240}>
            <SqlPrompt />
          </DCArtboard>
        </DCSection>

        <DCSection id="fuel" title="Fuel (WCC) · platform + customer" subtitle="SaaS admin views (fleet + per-org) and customer-facing dashboards (where did my money go, single-job drilldown).">
          <DCArtboard id="fuel-platform" label="1. Platform fuel · SaaS admin overview" width={1440} height={1320}>
            <PlatformFuel />
          </DCArtboard>
          <DCArtboard id="fuel-org-drill" label="2. Platform → org drilldown · burn anomaly" width={1440} height={1280}>
            <PlatformOrgDrill />
          </DCArtboard>
          <DCArtboard id="fuel-customer" label="3. Customer dashboard · where is the money going" width={1440} height={1480}>
            <CustomerFuel />
          </DCArtboard>
          <DCArtboard id="fuel-job-drill" label="4. Customer → single filter · regression detective" width={1440} height={1360}>
            <CustomerJobDrill />
          </DCArtboard>
        </DCSection>

        <DCSection id="acls" title="ACLs &amp; audit" subtitle="Permissions matrix (principal × operation) and filterable audit stream.">
          <DCArtboard id="acls-1" label="ACLs · permissions matrix" width={1440} height={900}>
            <AclsAudit />
          </DCArtboard>
          <DCArtboard id="audit-1" label="Audit log · last 24h" width={1440} height={900}>
            <AclsAudit defaultTab="audit" />
          </DCArtboard>
        </DCSection>

        <DCSection id="light" title="Light theme pass" subtitle="Same cluster overview, same density — daylight build for review screens and prod war-rooms.">
          <DCArtboard id="light-1" label="Cluster overview · light" width={1440} height={1100}>
            <div data-theme="light" style={{ width: '100%', height: '100%' }}>
              <ClusterOverviewA />
            </div>
          </DCArtboard>
        </DCSection>
      </DesignCanvas>

      <TweaksPanel title="Rafka">
        <TweakSection label="Accent">
          <TweakRadio label="Color" value={t.accent}
                      options={['rust', 'ember', 'crimson']}
                      onChange={(v) => setTweak('accent', v)} />
        </TweakSection>
      </TweaksPanel>
    </>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(<ScreensApp />);
