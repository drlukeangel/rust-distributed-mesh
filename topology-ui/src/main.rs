use anyhow::Result;
use axum::{
    Router,
    extract::{Json, Path, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
};
use dashmap::DashMap;
use rand::Rng;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{process::Child, sync::Mutex};
use tracing::{info, info_span, Instrument};

const KNOWN_NODE_TYPES: &[&str] = &["gateway", "broker", "compute", "registry", "bridge"];

const HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>rafka mesh — boot waterfall</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: monospace; background: #0d1117; color: #c9d1d9; padding: 1.5rem; }
  header { border-bottom: 1px solid #30363d; padding-bottom: 1rem; margin-bottom: 1.5rem; }
  h1 { font-size: 1.2rem; font-weight: bold; color: #58a6ff; }
  #status-bar { display: flex; align-items: center; gap: 0.75rem; margin-bottom: 1.5rem; font-size: 0.85rem; }
  #status-dot { width: 10px; height: 10px; border-radius: 50%; background: #3fb950; }
  #status-dot.error { background: #f85149; }
  select, button { background: #161b22; color: #c9d1d9; border: 1px solid #30363d; padding: 0.4rem 0.75rem; font-family: monospace; font-size: 0.85rem; cursor: pointer; border-radius: 4px; }
  button:hover { background: #21262d; }
  #spawn-row { display: flex; gap: 0.5rem; margin-bottom: 1rem; flex-wrap: wrap; }
  .spawn-btn { border-color: #3fb950; color: #3fb950; }
  .spawn-btn:hover { background: #0d2a14; }
  .spawn-btn:disabled { opacity: 0.5; cursor: not-allowed; }
  #controls { display: flex; gap: 0.75rem; margin-bottom: 1.5rem; align-items: center; }
  #toast { font-size: 0.8rem; margin-bottom: 1rem; min-height: 1.2em; color: #3fb950; }
  #toast.error { color: #f85149; }
  #waterfall {
    border: 1px solid #30363d;
    border-radius: 6px;
    padding: 1rem;
    min-height: 200px;
  }
  #waterfall-header {
    font-size: 0.8rem;
    color: #8b949e;
    margin-bottom: 0.75rem;
  }
  .wf-row {
    display: flex;
    align-items: center;
    margin-bottom: 0.4rem;
    gap: 0.5rem;
  }
  .wf-label {
    width: 260px;
    min-width: 260px;
    font-size: 0.75rem;
    color: #8b949e;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .wf-track {
    flex: 1;
    position: relative;
    height: 20px;
    background: #161b22;
    border-radius: 3px;
  }
  .wf-bar {
    position: absolute;
    top: 0;
    height: 100%;
    border-radius: 3px;
    min-width: 2px;
    display: flex;
    align-items: center;
    padding: 0 4px;
  }
  .wf-bar-label {
    font-size: 0.65rem;
    color: rgba(255,255,255,0.85);
    white-space: nowrap;
    overflow: hidden;
  }
  .wf-empty {
    color: #8b949e;
    font-size: 0.85rem;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 160px;
  }
  .wf-error {
    color: #f85149;
    font-size: 0.85rem;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 160px;
    text-align: center;
    padding: 1rem;
  }
  /* tabs */
  #tabs { display: flex; gap: 0; margin-bottom: 1rem; border-bottom: 1px solid #30363d; }
  .tab { background: transparent; border: none; border-bottom: 2px solid transparent; padding: 0.5rem 1rem; color: #8b949e; cursor: pointer; font-family: monospace; font-size: 0.85rem; }
  .tab:hover { color: #c9d1d9; }
  .tab.active { color: #58a6ff; border-bottom-color: #58a6ff; }
  .panel { display: none; }
  .panel.active { display: block; }
  /* topology svg */
  #topology-svg { width: 100%; height: 480px; background: #0d1117; border: 1px solid #30363d; border-radius: 6px; }
  .topo-node circle { stroke: #c9d1d9; stroke-width: 1.5; }
  .topo-node text { fill: #c9d1d9; font-size: 11px; text-anchor: middle; pointer-events: none; }
  .topo-edge { stroke: #58a6ff; stroke-width: 1.5; stroke-opacity: 0.5; }
  .topo-edge-label { fill: #8b949e; font-size: 9px; text-anchor: middle; pointer-events: none; }
  .topo-legend { font-size: 0.7rem; color: #8b949e; margin-top: 0.5rem; display: flex; gap: 1rem; flex-wrap: wrap; }
  .topo-legend span::before { content: ''; display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 0.4rem; vertical-align: middle; }
  .topo-legend .lg-gateway::before { background: #58a6ff; }
  .topo-legend .lg-broker::before { background: #f0883e; }
  .topo-legend .lg-compute::before { background: #3fb950; }
  .topo-legend .lg-registry::before { background: #a371f7; }
</style>
</head>
<body>
<header>
  <h1>rafka mesh — boot waterfall</h1>
</header>

<div id="status-bar">
  <div id="status-dot"></div>
  <span id="status-text">connecting…</span>
</div>

<div id="cluster-summary" style="background:#161b22;border:1px solid #30363d;border-radius:6px;padding:0.5rem 0.75rem;margin-bottom:0.5rem;font-size:0.8rem;color:#8b949e;font-family:monospace"></div>

<div id="spawn-row">
  <button class="spawn-btn" data-type="gateway">+ Spawn gateway</button>
  <button class="spawn-btn" data-type="broker">+ Spawn broker</button>
  <button class="spawn-btn" data-type="compute">+ Spawn compute</button>
  <button class="spawn-btn" data-type="registry">+ Spawn registry</button>
  <button class="spawn-btn" data-type="bridge">+ Spawn bridge</button>
</div>

<div id="toast"></div>

<div id="tabs">
  <button class="tab active" data-panel="panel-waterfall">Boot Waterfall</button>
  <button class="tab" data-panel="panel-topology">Topology</button>
  <button class="tab" data-panel="panel-alerts">Alerts</button>
  <button class="tab" data-panel="panel-health">Heartbeat</button>
  <button class="tab" data-panel="panel-chaos">Chaos</button>
  <button class="tab" data-panel="panel-timeline">Timeline</button>
</div>

<div id="panel-waterfall" class="panel active">
  <div id="controls">
    <select id="node-selector">
      <option value="">select a node</option>
    </select>
    <button id="refresh">Refresh</button>
    <button id="kill-btn" disabled style="border-color:#f85149;color:#f85149;display:none">Kill selected</button>
  </div>

  <div id="waterfall">
    <div class="wf-empty">select a node to view its boot waterfall</div>
  </div>
</div>

<div id="panel-topology" class="panel">
  <div id="controls">
    <button id="topology-refresh">Refresh topology</button>
    <span id="topology-status" style="color:#8b949e;font-size:0.8rem;margin-left:0.5rem"></span>
  </div>
  <svg id="topology-svg" viewBox="0 0 800 480" preserveAspectRatio="xMidYMid meet"></svg>
  <div class="topo-legend">
    <span class="lg-gateway">gateway</span>
    <span class="lg-broker">broker</span>
    <span class="lg-compute">compute</span>
    <span class="lg-registry">registry</span>
  </div>
</div>

<div id="panel-alerts" class="panel">
  <div id="controls">
    <button id="alerts-refresh">Refresh alerts</button>
    <span id="alerts-status" style="color:#8b949e;font-size:0.8rem;margin-left:0.5rem"></span>
  </div>
  <div id="alerts-list" style="border:1px solid #30363d;border-radius:6px;padding:1rem;min-height:200px"></div>
</div>

<div id="panel-health" class="panel">
  <div id="controls">
    <button id="health-refresh">Refresh heartbeats</button>
    <span id="health-status" style="color:#8b949e;font-size:0.8rem;margin-left:0.5rem"></span>
  </div>
  <div id="health-cards" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:1rem"></div>
</div>

<div id="panel-chaos" class="panel">
  <div id="controls">
    <button id="chaos-refresh">Refresh chaos events</button>
    <span id="chaos-status" style="color:#8b949e;font-size:0.8rem;margin-left:0.5rem"></span>
  </div>
  <div id="chaos-summary" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:0.5rem;margin-bottom:1rem"></div>
  <div id="chaos-recent" style="border:1px solid #30363d;border-radius:6px;padding:0.5rem"></div>
</div>

<div id="panel-timeline" class="panel">
  <div id="controls">
    <button id="timeline-refresh">Refresh timeline</button>
    <span id="timeline-status" style="color:#8b949e;font-size:0.8rem;margin-left:0.5rem"></span>
  </div>
  <div style="color:#8b949e;font-size:0.75rem;margin-bottom:0.5rem">
    Chronological execute → detect pairs for every chaos primitive in the last 10 min.
    Green ↦ resolved (substrate detected the disturbance); amber ↦ pending or failed.
  </div>
  <div id="timeline-list" style="font-family:monospace;font-size:0.78rem"></div>
</div>

<script>
(function() {
  var dot      = document.getElementById('status-dot');
  var txt      = document.getElementById('status-text');
  var sel      = document.getElementById('node-selector');
  var wf       = document.getElementById('waterfall');
  var toast    = document.getElementById('toast');
  var killBtn  = document.getElementById('kill-btn');

  // node_name → node_type for subprocesses spawned by this UI session
  var uiSpawned = {};

  var COLORS = {
    'rafka.mesh.node.ready':              '#1f6feb',
    'rafka.mesh.boot.identity_':          '#3fb950',
    'rafka.mesh.boot.endpoint_created':   '#e3b341',
    'rafka.mesh.boot.alpn_registered':    '#8957e5',
    'rafka.mesh.boot.gossip_started':     '#39c5cf',
    'rafka.mesh.boot.accept_loop_started':'#f85149',
  };

  function spanColor(opName) {
    for (var prefix in COLORS) {
      if (opName === prefix || opName.indexOf(prefix) === 0) return COLORS[prefix];
    }
    return '#484f58';
  }

  function setStatus(ok, msg) {
    dot.className = ok ? '' : 'error';
    txt.textContent = msg;
  }

  function showToast(ok, msg) {
    toast.className = ok ? '' : 'error';
    toast.textContent = msg;
    setTimeout(function() { if (toast.textContent === msg) toast.textContent = ''; }, 8000);
  }

  function pollHealth() {
    fetch('/api/health')
      .then(function(r) { return r.json(); })
      .then(function(d) { setStatus(true, 'api: ' + d.status); })
      .catch(function() { setStatus(false, 'api unreachable'); });
  }

  function loadNodes() {
    fetch('/api/nodes')
      .then(function(r) { return r.json(); })
      .then(function(d) {
        var nodes = d.nodes || [];
        var prev = sel.value;
        while (sel.options.length > 1) sel.remove(1);
        nodes.forEach(function(n) {
          var opt = document.createElement('option');
          opt.value = n;
          opt.textContent = n;
          sel.appendChild(opt);
        });
        if (prev && nodes.indexOf(prev) !== -1) sel.value = prev;
        setStatus(true, 'nodes: ' + (nodes.length ? nodes.join(', ') : '(none in jaeger yet)'));
      })
      .catch(function() { setStatus(false, 'node list unavailable'); });
  }

  function renderWaterfall(svc, traceData) {
    var spans = traceData.spans || [];
    var rafkaSpans = spans.filter(function(s) {
      return s.operationName && s.operationName.indexOf('rafka.') === 0;
    });

    if (rafkaSpans.length === 0) {
      wf.innerHTML = '<div class="wf-error">no rafka spans found in boot trace for ' + svc + '</div>';
      return;
    }

    rafkaSpans.sort(function(a, b) { return a.startTime - b.startTime; });

    var rootTime = rafkaSpans[0].startTime;
    var endTimes = rafkaSpans.map(function(s) { return s.startTime + s.duration; });
    var maxEnd   = Math.max.apply(null, endTimes);
    var totalUs  = maxEnd - rootTime;
    if (totalUs <= 0) totalUs = 1;

    var rootDate   = new Date(rootTime / 1000);
    var headerText = svc + ' boot @ ' + rootDate.toISOString();
    var html       = '<div id="waterfall-header">' + headerText + '</div>';

    rafkaSpans.forEach(function(sp) {
      var name       = sp.operationName;
      var shortName  = name.replace('rafka.mesh.', '');
      var offsetUs   = sp.startTime - rootTime;
      var leftPct    = (offsetUs / totalUs * 100).toFixed(2);
      var widthPct   = (sp.duration / totalUs * 100).toFixed(2);
      var durationMs = (sp.duration / 1000).toFixed(2);
      var color      = spanColor(name);

      html += '<div class="wf-row">' +
        '<div class="wf-label" title="' + name + '">' + shortName + '</div>' +
        '<div class="wf-track">' +
          '<div class="wf-bar" style="left:' + leftPct + '%;width:max(' + widthPct + '%,2px);background:' + color + '" title="' + name + ' — ' + durationMs + 'ms">' +
            '<span class="wf-bar-label">' + durationMs + 'ms</span>' +
          '</div>' +
        '</div>' +
        '</div>';
    });

    wf.innerHTML = html;
    setStatus(true, svc + ': ' + rafkaSpans.length + ' rafka spans, total ' + (totalUs / 1000).toFixed(2) + 'ms');
  }

  function loadTrace(svc) {
    if (!svc) return;
    setStatus(true, 'fetching boot trace for ' + svc + '…');
    wf.innerHTML = '<div class="wf-empty">loading…</div>';
    fetch('/api/boot-trace?service=' + encodeURIComponent(svc))
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.error) {
          wf.innerHTML = '<div class="wf-error">no boot trace found for <strong>' + svc + '</strong><br>has it run recently? (traces age out after ~10 min)</div>';
          setStatus(false, 'no boot trace: ' + svc);
        } else {
          var trace = d.data && d.data[0];
          if (trace) renderWaterfall(svc, trace);
          else {
            wf.innerHTML = '<div class="wf-error">empty trace data for ' + svc + '</div>';
            setStatus(false, 'empty trace: ' + svc);
          }
        }
      })
      .catch(function() {
        wf.innerHTML = '<div class="wf-error">boot-trace fetch failed</div>';
        setStatus(false, 'boot-trace fetch failed');
      });
  }

  function updateKillBtn() {
    var svc = sel.value;
    if (svc && uiSpawned[svc]) {
      killBtn.style.display = '';
      killBtn.disabled = false;
      killBtn.textContent = 'Kill ' + svc;
    } else {
      killBtn.style.display = 'none';
      killBtn.disabled = true;
    }
  }

  function spawnNode(nodeType, btn) {
    btn.disabled = true;
    fetch('/api/nodes/spawn', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({node_type: nodeType})
    })
      .then(function(r) { return r.json().then(function(d) { return {ok: r.ok, d: d}; }); })
      .then(function(res) {
        btn.disabled = false;
        if (res.ok) {
          uiSpawned[res.d.node_name] = nodeType;
          showToast(true, 'Spawned ' + res.d.node_name + ' (pid=' + res.d.pid + ')');
          setTimeout(loadNodes, 5000);
        } else {
          showToast(false, 'Spawn failed: ' + (res.d.error || 'unknown error'));
        }
      })
      .catch(function() {
        btn.disabled = false;
        showToast(false, 'Spawn request failed');
      });
  }

  function killSelected() {
    var svc = sel.value;
    if (!svc || !uiSpawned[svc]) return;
    killBtn.disabled = true;
    fetch('/api/nodes/' + encodeURIComponent(svc), { method: 'DELETE' })
      .then(function(r) { return r.json().then(function(d) { return {ok: r.ok, d: d}; }); })
      .then(function(res) {
        if (res.ok) {
          delete uiSpawned[res.d.node_name];
          showToast(true, 'Killed ' + res.d.node_name + ' (' + res.d.reason + ')');
          loadNodes();
          updateKillBtn();
          wf.innerHTML = '<div class="wf-empty">select a node to view its boot waterfall</div>';
        } else {
          showToast(false, 'Kill failed: ' + (res.d.error || 'unknown error'));
          killBtn.disabled = false;
        }
      })
      .catch(function() {
        showToast(false, 'Kill request failed');
        killBtn.disabled = false;
      });
  }

  killBtn.addEventListener('click', killSelected);

  document.querySelectorAll('.spawn-btn').forEach(function(btn) {
    btn.addEventListener('click', function() { spawnNode(btn.dataset.type, btn); });
  });

  sel.addEventListener('change', function() { loadTrace(sel.value); updateKillBtn(); });

  document.getElementById('refresh').addEventListener('click', function() {
    pollHealth();
    loadNodes();
    if (sel.value) loadTrace(sel.value);
  });

  // ── topology tab ────────────────────────────────────────────────────────────
  // ── cluster summary banner ───────────────────────────────────────────────
  var clusterBanner = document.getElementById('cluster-summary');
  function loadClusterSummary() {
    fetch('/api/cluster/summary')
      .then(function(r) { return r.json(); })
      .then(function(d) {
        var meshes = (d.meshes || []).join(', ') || '(none)';
        clusterBanner.innerHTML =
          '<span style="color:#3fb950">' + d.spawned_count + ' spawned</span> | ' +
          '<span style="color:#58a6ff">meshes: ' + meshes + '</span> | ' +
          '<span style="color:#e3b341">chaos: ' + d.chaos_events_1m + '/min</span> | ' +
          '<span style="color:#c9d1d9">mean peers: ' + (d.mean_peer_count || 0).toFixed(1) + '</span>';
      })
      .catch(function(e) { clusterBanner.textContent = 'summary fetch failed: ' + e; });
  }
  setInterval(loadClusterSummary, 8000);
  loadClusterSummary();

  var topoSvg = document.getElementById('topology-svg');
  var topoStatus = document.getElementById('topology-status');
  var topoRefresh = document.getElementById('topology-refresh');
  var topoTimer = null;

  var TYPE_COLOR = {gateway:'#58a6ff', broker:'#f0883e', compute:'#3fb950', registry:'#a371f7', bridge:'#e3b341'};

  // Stable per-mesh-id ring color (string → palette index).
  var MESH_RING_PALETTE = ['#58a6ff', '#3fb950', '#f0883e', '#a371f7', '#e3b341', '#ff7b72'];
  function meshRingColor(meshId) {
    if (!meshId || meshId === 'default') return '#30363d';
    var h = 0;
    for (var i = 0; i < meshId.length; i++) h = (h * 31 + meshId.charCodeAt(i)) | 0;
    return MESH_RING_PALETTE[Math.abs(h) % MESH_RING_PALETTE.length];
  }

  function renderTopology(data) {
    var W = 800, H = 480, cx = W/2, cy = H/2, R = 170;
    var nodes = data.nodes || [];
    var edges = data.edges || [];

    if (nodes.length === 0) {
      topoSvg.innerHTML = '<text x="' + cx + '" y="' + cy + '" fill="#8b949e" text-anchor="middle">no nodes — start some via Spawn buttons</text>';
      return;
    }

    // Group nodes by mesh_id so each mesh gets its own arc on the ring + a ring
    // background highlighting the mesh boundary.
    var byMesh = {};
    nodes.forEach(function(n) {
      var m = n.mesh_id || 'default';
      (byMesh[m] = byMesh[m] || []).push(n);
    });
    var meshes = Object.keys(byMesh).sort();
    var pos = {};
    var meshArcs = [];
    var arcStart = -Math.PI/2;
    meshes.forEach(function(m) {
      var members = byMesh[m];
      var span = 2 * Math.PI * (members.length / nodes.length);
      var ringColor = meshRingColor(m);
      // Draw a faint background arc for each mesh — visually groups its members.
      var midAng = arcStart + span/2;
      meshArcs.push('<text x="' + (cx + (R + 30) * Math.cos(midAng)) + '" y="' + (cy + (R + 30) * Math.sin(midAng)) + '" fill="' + ringColor + '" font-size="11" text-anchor="middle">' + m + '</text>');
      members.forEach(function(n, i) {
        var slot = members.length === 1 ? 0.5 : i / (members.length - 1);
        var ang = arcStart + span * slot * 0.85 + span * 0.075; // 0.075 padding each side
        pos[n.id] = { x: cx + R * Math.cos(ang), y: cy + R * Math.sin(ang), mesh: m };
      });
      arcStart += span;
    });

    var svgParts = meshArcs.slice();
    edges.forEach(function(e) {
      var a = pos[e.from], b = pos[e.to];
      if (!a || !b) return;
      var isCross = e.kind === 'cross';
      var style = isCross ? 'stroke-dasharray:5,4;stroke:#e3b341;stroke-opacity:0.7' : '';
      svgParts.push('<line class="topo-edge" x1="' + a.x + '" y1="' + a.y + '" x2="' + b.x + '" y2="' + b.y + '" style="' + style + '"/>');
    });
    nodes.forEach(function(n) {
      var p = pos[n.id];
      var typeColor = TYPE_COLOR[n.type] || '#888';
      var meshColor = meshRingColor(n.mesh_id || 'default');
      svgParts.push('<g class="topo-node">' +
        '<circle cx="' + p.x + '" cy="' + p.y + '" r="26" fill="none" stroke="' + meshColor + '" stroke-width="3" stroke-opacity="0.8"/>' +
        '<circle cx="' + p.x + '" cy="' + p.y + '" r="22" fill="' + typeColor + '" fill-opacity="0.65"/>' +
        '<text x="' + p.x + '" y="' + (p.y + 4) + '">' + (n.id.length > 12 ? n.id.slice(0,10) + '…' : n.id) + '</text>' +
        '<text x="' + p.x + '" y="' + (p.y + 38) + '" style="fill:#8b949e;font-size:9px">' + (n.mesh_id || 'default') + '</text>' +
        (typeof n.frames_per_min === 'number' && n.frames_per_min > 0 ? '<text x="' + p.x + '" y="' + (p.y + 50) + '" style="fill:#3fb950;font-size:9px">' + n.frames_per_min + ' fr/m</text>' : '') +
        '</g>');
    });
    topoSvg.innerHTML = svgParts.join('');
    topoStatus.textContent = nodes.length + ' nodes, ' + edges.length + ' edges, ' + meshes.length + ' mesh' + (meshes.length === 1 ? '' : 'es');
  }

  function loadTopology() {
    fetch('/api/topology')
      .then(function(r) { return r.json(); })
      .then(function(d) { renderTopology(d); })
      .catch(function(e) { topoStatus.textContent = 'fetch failed: ' + e; });
  }

  topoRefresh.addEventListener('click', loadTopology);

  // ── alerts tab ───────────────────────────────────────────────────────────
  var alertsList = document.getElementById('alerts-list');
  var alertsStatus = document.getElementById('alerts-status');
  var alertsRefresh = document.getElementById('alerts-refresh');
  var alertsTimer = null;

  function renderAlerts(alerts) {
    if (!alerts || alerts.length === 0) {
      alertsList.innerHTML = '<div style="color:#3fb950;font-size:0.85rem">no recent alerts (all chaos events passing)</div>';
      alertsStatus.textContent = '0 active alerts';
      return;
    }
    var html = '';
    alerts.forEach(function(a) {
      html += '<div style="border-left:3px solid #f85149;padding:0.5rem 0.75rem;margin-bottom:0.5rem;background:#161b22;border-radius:4px">' +
        '<div style="color:#f85149;font-size:0.85rem;font-weight:bold">' + (a.kind || 'failure') + '</div>' +
        '<div style="color:#c9d1d9;font-size:0.8rem;margin-top:0.25rem">' + (a.message || '') + '</div>' +
        '<div style="color:#8b949e;font-size:0.7rem;margin-top:0.25rem">trace: <a href="http://localhost:16686/trace/' + a.trace_id + '" target="_blank" style="color:#58a6ff">' + (a.trace_id || '').slice(0,16) + '</a></div>' +
      '</div>';
    });
    alertsList.innerHTML = html;
    alertsStatus.textContent = alerts.length + ' alerts';
  }

  function loadAlerts() {
    fetch('/api/alerts')
      .then(function(r) { return r.json(); })
      .then(function(d) { renderAlerts(d.alerts || []); })
      .catch(function(e) { alertsStatus.textContent = 'fetch failed: ' + e; });
  }

  alertsRefresh.addEventListener('click', loadAlerts);

  // ── heartbeat panel ──────────────────────────────────────────────────────
  var healthCards = document.getElementById('health-cards');
  var healthStatus = document.getElementById('health-status');
  var healthRefresh = document.getElementById('health-refresh');
  var healthTimer = null;

  function renderHealth(services) {
    if (!services || services.length === 0) {
      healthCards.innerHTML = '<div style="color:#8b949e">no heartbeat data yet</div>';
      return;
    }
    var html = '';
    services.forEach(function(s) {
      var ageSec = s.age_ms < 0 ? '?' : (s.age_ms / 1000).toFixed(1);
      var ageColor = s.age_ms < 0 ? '#8b949e' :
                     (s.age_ms > 30000 ? '#f85149' : (s.age_ms > 10000 ? '#e3b341' : '#3fb950'));
      var typeColor = TYPE_COLOR[s.node_type || s.service] || '#888';
      html += '<div style="background:#161b22;border:1px solid #30363d;border-radius:6px;padding:1rem">' +
        '<div style="color:' + typeColor + ';font-weight:bold;font-size:0.95rem;margin-bottom:0.3rem">' + s.service + '</div>' +
        '<div style="color:#8b949e;font-size:0.7rem">type: ' + (s.node_type || '?') + ' · mesh: ' + (s.mesh_id || 'default') + '</div>' +
        '<div style="color:#8b949e;font-size:0.7rem">node_id: ' + (s.node_id || '').slice(0,16) + '…</div>' +
        '<div style="font-size:1.4rem;color:#c9d1d9;margin-top:0.5rem">peers: <strong>' + s.peer_count + '</strong></div>' +
        '<div style="color:' + ageColor + ';font-size:0.75rem;margin-top:0.25rem">last beat: ' + ageSec + 's ago</div>' +
        '</div>';
    });
    healthCards.innerHTML = html;
    healthStatus.textContent = services.length + ' services tracked';
  }

  function loadHealth() {
    fetch('/api/heartbeats').then(function(r) { return r.json(); }).then(function(d) {
      // d.heartbeats = [{node_name, node_type, node_id, mesh_id, peer_count, age_ms}]
      var items = (d.heartbeats || []).map(function(h) {
        return {
          service: h.node_name,           // card title shows the spawn name
          node_type: h.node_type,         // for color
          node_id: h.node_id,
          peer_count: h.peer_count,
          age_ms: h.age_ms,
          mesh_id: h.mesh_id,
        };
      });
      renderHealth(items);
    }).catch(function(e) { healthStatus.textContent = 'fetch failed: ' + e; });
  }

  healthRefresh.addEventListener('click', loadHealth);

  // ── chaos events panel ───────────────────────────────────────────────────
  var chaosSummary = document.getElementById('chaos-summary');
  var chaosRecent = document.getElementById('chaos-recent');
  var chaosStatus = document.getElementById('chaos-status');
  var chaosRefresh = document.getElementById('chaos-refresh');
  var chaosTimer = null;

  function renderChaos(d) {
    var counts = d.counts || {};
    var keys = Object.keys(counts).sort();
    if (keys.length === 0) {
      chaosSummary.innerHTML = '<div style="color:#8b949e;font-size:0.85rem">no chaos events in lookback window</div>';
      chaosRecent.innerHTML = '';
      chaosStatus.textContent = '0 events';
      return;
    }
    var html = '';
    keys.forEach(function(k) {
      html += '<div style="background:#161b22;border:1px solid #30363d;border-radius:4px;padding:0.5rem">' +
        '<div style="color:#58a6ff;font-size:0.7rem;text-transform:uppercase">' + k + '</div>' +
        '<div style="font-size:1.4rem;color:#c9d1d9">' + counts[k] + '</div>' +
        '</div>';
    });
    chaosSummary.innerHTML = html;
    var recent = d.recent || [];
    var rhtml = '';
    recent.forEach(function(e) {
      rhtml += '<div style="border-bottom:1px solid #1f2429;padding:0.4rem 0.5rem;font-size:0.8rem">' +
        '<div><span style="color:#3fb950">' + e.name + '</span>' +
        '<span style="color:#8b949e"> on </span>' +
        '<span style="color:#c9d1d9">' + (e.target || '?') + '</span>' +
        '<span style="color:#8b949e;float:right">' + e.when + '</span></div>' +
        (e.description ? '<div style="color:#6e7681;font-size:0.72rem;margin-top:0.15rem">' + e.description + '</div>' : '') +
        '</div>';
    });
    chaosRecent.innerHTML = rhtml;
    var total = keys.reduce(function(a, k) { return a + counts[k]; }, 0);
    chaosStatus.textContent = total + ' events in last 10min';
  }

  function loadChaos() {
    fetch('/api/chaos/recent')
      .then(function(r) { return r.json(); })
      .then(renderChaos)
      .catch(function(e) { chaosStatus.textContent = 'fetch failed: ' + e; });
  }

  chaosRefresh.addEventListener('click', loadChaos);

  // ── timeline tab ─────────────────────────────────────────────────────────
  var timelineList = document.getElementById('timeline-list');
  var timelineStatus = document.getElementById('timeline-status');
  var timelineRefresh = document.getElementById('timeline-refresh');
  var timelineTimer = null;

  function renderTimeline(events) {
    if (!events || events.length === 0) {
      timelineList.innerHTML = '<div style="color:#8b949e">no chaos events in lookback window — soak idle?</div>';
      timelineStatus.textContent = '0 events';
      return;
    }
    var html = '';
    events.forEach(function(e) {
      var resolved = e.detection === 'passed';
      var color = resolved ? '#3fb950' : (e.detection === 'pending' ? '#e3b341' : '#f85149');
      var symbol = resolved ? '✓' : (e.detection === 'pending' ? '…' : '✗');
      var detectionTxt = resolved ? 'resolved in ' + e.resolved_ms + 'ms' :
                         (e.detection === 'pending' ? 'pending detection' : 'failed: ' + (e.detection || 'unknown'));
      html += '<div style="padding:0.4rem 0.6rem;border-bottom:1px solid #1f2429">' +
        '<div style="display:flex;gap:0.75rem;align-items:baseline">' +
          '<span style="color:#8b949e;width:80px">' + e.when + '</span>' +
          '<span style="color:' + color + ';width:18px;text-align:center">' + symbol + '</span>' +
          '<span style="color:#58a6ff;width:160px">' + e.primitive + '</span>' +
          '<span style="color:#c9d1d9;flex:1">' + (e.target || '') + '</span>' +
          '<span style="color:' + color + '">' + detectionTxt + '</span>' +
        '</div>' +
        (e.description ? '<div style="color:#6e7681;font-size:0.72rem;margin-top:0.15rem;margin-left:103px">' + e.description + '</div>' : '') +
        '</div>';
    });
    timelineList.innerHTML = html;
    var resolved = events.filter(function(e) { return e.detection === 'passed'; }).length;
    timelineStatus.textContent = events.length + ' events, ' + resolved + ' resolved';
  }

  function loadTimeline() {
    fetch('/api/chaos/timeline')
      .then(function(r) { return r.json(); })
      .then(function(d) { renderTimeline(d.events || []); })
      .catch(function(e) { timelineStatus.textContent = 'fetch failed: ' + e; });
  }

  timelineRefresh.addEventListener('click', loadTimeline);

  // tab switching
  var tabs = document.querySelectorAll('.tab');
  tabs.forEach(function(t) {
    t.addEventListener('click', function() {
      tabs.forEach(function(x) { x.classList.remove('active'); });
      t.classList.add('active');
      document.querySelectorAll('.panel').forEach(function(p) { p.classList.remove('active'); });
      var target = document.getElementById(t.getAttribute('data-panel'));
      if (target) target.classList.add('active');
      // Clear all auto-poll timers, then activate the right one for the chosen tab
      if (topoTimer) { clearInterval(topoTimer); topoTimer = null; }
      if (alertsTimer) { clearInterval(alertsTimer); alertsTimer = null; }
      if (healthTimer) { clearInterval(healthTimer); healthTimer = null; }
      if (chaosTimer) { clearInterval(chaosTimer); chaosTimer = null; }
      if (timelineTimer) { clearInterval(timelineTimer); timelineTimer = null; }
      var panel = t.getAttribute('data-panel');
      if (panel === 'panel-topology') {
        loadTopology();
        topoTimer = setInterval(loadTopology, 5000);
      } else if (panel === 'panel-alerts') {
        loadAlerts();
        alertsTimer = setInterval(loadAlerts, 10000);
      } else if (panel === 'panel-health') {
        loadHealth();
        healthTimer = setInterval(loadHealth, 5000);
      } else if (panel === 'panel-chaos') {
        loadChaos();
        chaosTimer = setInterval(loadChaos, 10000);
      } else if (panel === 'panel-timeline') {
        loadTimeline();
        timelineTimer = setInterval(loadTimeline, 5000);
      }
    });
  });

  pollHealth();
  loadNodes();
  setInterval(pollHealth, 5000);
  setInterval(loadNodes, 30000);
})();
</script>
</body>
</html>"##;

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    jaeger_url: String,
    cargo_target_dir: String,
    processes: Arc<DashMap<String, Mutex<Child>>>,
}

#[derive(Deserialize)]
struct BootTraceQuery {
    service: String,
}

#[derive(Deserialize)]
struct SpawnRequest {
    node_type: String,
    /// Optional extra env vars to merge into the child process env. Used by chaos
    /// primitives like clock_skew to inject behavior switches at restart.
    #[serde(default)]
    extra_env: Option<std::collections::HashMap<String, String>>,
}

async fn handle_root() -> Html<&'static str> {
    Html(HTML)
}

async fn handle_health() -> impl IntoResponse {
    axum::Json(json!({"status": "ok"}))
}

async fn handle_spawned_list(State(state): State<AppState>) -> impl IntoResponse {
    let names: Vec<String> = state.processes.iter().map(|e| e.key().clone()).collect();
    let span = info_span!(
        "rafka.ui.spawned_list",
        count = names.len() as i64,
        "otel.kind" = "internal",
    );
    span.in_scope(|| info!(count = names.len(), "spawned subprocesses listed"));
    (StatusCode::OK, axum::Json(json!({"spawned": names}))).into_response()
}

/// `GET /api/heartbeats` — per-instance heartbeat data for every spawned
/// subprocess. Returns `[{node_name, node_type, node_id, mesh_id, peer_count,
/// age_ms}]`. Used by the Heartbeat tab to render one card per instance
/// instead of one per node_type.
async fn handle_heartbeats(State(state): State<AppState>) -> impl IntoResponse {
    let spawned: Vec<String> = state.processes.iter().map(|e| e.key().clone()).collect();
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    let mut out: Vec<Value> = Vec::new();
    for name in spawned {
        let node_type = KNOWN_NODE_TYPES
            .iter()
            .find(|t| name.starts_with(*t))
            .copied()
            .unwrap_or("?");
        let tags_json = serde_json::to_string(&serde_json::json!({"node_name": &name}))
            .unwrap_or_else(|_| "{}".into());
        let tags_enc = urlencoding::encode(&tags_json);
        let url = format!(
            "{}/api/traces?service={}&operation=rafka.mesh.heartbeat&limit=1&lookback=2m&tags={}",
            state.jaeger_url, node_type, tags_enc
        );
        let (node_id, mesh_id, peer_count, age_ms) = match state.http.get(&url).send().await {
            Ok(resp) => match resp.json::<Value>().await {
                Ok(body) => {
                    let s = body["data"]
                        .as_array()
                        .and_then(|a| a.first())
                        .and_then(|t| t["spans"].as_array())
                        .and_then(|a| a.first());
                    let tags = s.and_then(|sp| sp["tags"].as_array());
                    let nid = tags
                        .and_then(|tt| {
                            tt.iter()
                                .find(|t| t["key"] == "node_id")
                                .and_then(|t| t["value"].as_str())
                        })
                        .unwrap_or("")
                        .to_string();
                    let m = tags
                        .and_then(|tt| {
                            tt.iter()
                                .find(|t| t["key"] == "mesh_id")
                                .and_then(|t| t["value"].as_str())
                        })
                        .unwrap_or("default")
                        .to_string();
                    let p = tags
                        .and_then(|tt| {
                            tt.iter()
                                .find(|t| t["key"] == "peer_count")
                                .and_then(|t| t["value"].as_i64())
                        })
                        .unwrap_or(0);
                    let start_us = s.and_then(|sp| sp["startTime"].as_i64()).unwrap_or(0);
                    let age = if start_us > 0 {
                        (now_us - start_us).max(0) / 1000
                    } else {
                        -1
                    };
                    (nid, m, p, age)
                }
                Err(_) => (String::new(), "default".into(), 0, -1),
            },
            Err(_) => (String::new(), "default".into(), 0, -1),
        };
        out.push(json!({
            "node_name": name,
            "node_type": node_type,
            "node_id": node_id,
            "mesh_id": mesh_id,
            "peer_count": peer_count,
            "age_ms": age_ms,
        }));
    }
    (StatusCode::OK, axum::Json(json!({"heartbeats": out}))).into_response()
}

/// One-line description per chaos primitive. Returned alongside every chaos
/// event in /api/chaos/recent + /api/chaos/timeline so the operator-facing
/// tabs can show "what does this thing do" without crossing references.
/// Single source of truth — UI just renders.
fn primitive_description(name: &str) -> &'static str {
    match name {
        "kill_node"        => "Terminate one random spawned subprocess (SIGKILL equivalent). Substrate must detect within deadline.",
        "restart_node"     => "Kill + immediately re-spawn the same node_type with a fresh NodeId. Substrate must reconnect.",
        "burst_kill"       => "Kill N random subprocesses back-to-back. Tests substrate-race conditions on the spawn registry.",
        "disk_full"        => "Fill the target's spawn data dir until writes fail (capped). Tests disk-pressure path.",
        "wedge_node"       => "Suspend the OS process via Windows NtSuspendProcess. Process exists but doesn't respond; revert resumes it.",
        "clock_skew"       => "Restart target with RAFKA_CLOCK_SKEW_MS env. node-base adds that offset to wall_time_ms on every heartbeat span.",
        "slow_link"        => "Restart target with RAFKA_LINK_SLOW_MS env. node-base sleeps that many ms before each outbound frame send.",
        "lossy_link"       => "Restart target with RAFKA_LINK_LOSS_PCT env. Per outbound frame, dice roll <pct ⇒ emit drop span and skip the send.",
        "nat_shift"        => "Restart target with new random RAFKA_NODE_BIND_ADDR. iroh must re-discover the NodeId at the new ephemeral port.",
        "partition_pair"   => "ADMIN: Windows firewall block outbound UDP between two named programs. Survivors should detect the partition.",
        "partition_subset" => "ADMIN: Pick K random node_types as the subset; firewall-block every (subset, complement) pair. Tests split-brain.",
        "flap_link"        => "ADMIN: Create+delete partition_pair-style firewall block N times with on/off duty. Tests substrate against churn.",
        "firewall_inbound" => "ADMIN: Block inbound UDP to one named program for duration_ms. Peers can't dial in; existing outbound still works.",
        _                  => "Unknown primitive.",
    }
}

/// `GET /api/chaos/timeline` — chronological execute → detect pairs.
/// Queries Jaeger for both `rafka.chaos.primitive.executed` and
/// `rafka.chaos.primitive.detected` spans in last 10m, matches them by trace_id
/// (each chaos event runs in its own trace), and emits a sorted timeline with
/// "resolved in Xms" or "pending" markers. Used by the Timeline tab to prove
/// the system is actually resolving the disturbances, not just suffering them.
async fn handle_chaos_timeline(State(state): State<AppState>) -> impl IntoResponse {
    let exec_url = format!(
        "{}/api/traces?service=rfa&operation=rafka.chaos.primitive.executed&limit=300&lookback=10m",
        state.jaeger_url
    );
    let detect_url = format!(
        "{}/api/traces?service=rfa&operation=rafka.chaos.primitive.detected&limit=300&lookback=10m",
        state.jaeger_url
    );
    let exec_body: Value = match state.http.get(&exec_url).send().await {
        Ok(r) => r.json::<Value>().await.unwrap_or(json!({"data":[]})),
        Err(_) => return (StatusCode::OK, axum::Json(json!({"events": []}))).into_response(),
    };
    let det_body: Value = match state.http.get(&detect_url).send().await {
        Ok(r) => r.json::<Value>().await.unwrap_or(json!({"data":[]})),
        Err(_) => json!({"data":[]}),
    };

    // Build detected-by-trace_id index: trace_id → (result, waited_ms)
    let mut det_by_trace: std::collections::HashMap<String, (String, i64)> =
        std::collections::HashMap::new();
    if let Some(arr) = det_body["data"].as_array() {
        for trace in arr {
            let tid = trace["traceID"].as_str().unwrap_or("").to_string();
            if let Some(spans) = trace["spans"].as_array() {
                for s in spans {
                    if s["operationName"] != "rafka.chaos.primitive.detected" {
                        continue;
                    }
                    let tags = s["tags"].as_array();
                    let result = tags
                        .and_then(|t| t.iter().find(|x| x["key"] == "result"))
                        .and_then(|x| x["value"].as_str())
                        .unwrap_or("?")
                        .to_string();
                    let waited = tags
                        .and_then(|t| t.iter().find(|x| x["key"] == "waited_ms"))
                        .and_then(|x| x["value"].as_i64())
                        .unwrap_or(0);
                    det_by_trace.insert(tid.clone(), (result, waited));
                }
            }
        }
    }

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);

    // Walk executed spans, join with detected via trace_id.
    let mut rows: Vec<(i64, Value)> = Vec::new();
    if let Some(arr) = exec_body["data"].as_array() {
        for trace in arr {
            let tid = trace["traceID"].as_str().unwrap_or("").to_string();
            if let Some(spans) = trace["spans"].as_array() {
                for s in spans {
                    if s["operationName"] != "rafka.chaos.primitive.executed" {
                        continue;
                    }
                    let start_us = s["startTime"].as_i64().unwrap_or(0);
                    let tags = s["tags"].as_array();
                    let primitive = tags
                        .and_then(|t| t.iter().find(|x| x["key"] == "name"))
                        .and_then(|x| x["value"].as_str())
                        .unwrap_or("?")
                        .to_string();
                    let target = tags
                        .and_then(|t| t.iter().find(|x| x["key"] == "target"))
                        .and_then(|x| x["value"].as_str())
                        .unwrap_or("")
                        .to_string();
                    let (detection, resolved_ms) = match det_by_trace.get(&tid) {
                        Some((res, w)) => (res.clone(), *w),
                        None => ("pending".to_string(), 0),
                    };
                    let age_s = ((now_us - start_us).max(0)) / 1_000_000;
                    let when = if age_s < 60 {
                        format!("{age_s}s ago")
                    } else if age_s < 3600 {
                        format!("{}m{}s ago", age_s / 60, age_s % 60)
                    } else {
                        format!("{}h{}m ago", age_s / 3600, (age_s % 3600) / 60)
                    };
                    rows.push((
                        start_us,
                        json!({
                            "when": when,
                            "primitive": primitive,
                            "description": primitive_description(&primitive),
                            "target": target,
                            "detection": detection,
                            "resolved_ms": resolved_ms,
                        }),
                    ));
                }
            }
        }
    }
    rows.sort_by(|a, b| b.0.cmp(&a.0)); // newest first
    let events: Vec<Value> = rows.into_iter().map(|(_, v)| v).collect();
    (StatusCode::OK, axum::Json(json!({"events": events}))).into_response()
}

/// `GET /api/cluster/summary` — one-call operator dashboard. Aggregates:
/// - spawned_count (subprocess registry size)
/// - meshes (distinct mesh_id values observed in last 2m of heartbeats)
/// - chaos_events_1m (rafka.chaos.primitive.executed in last 1m via Jaeger)
/// - mean_peer_count (avg peer_count from each known service's last heartbeat)
/// Used by the UI status banner so operators see one-line health at a glance.
async fn handle_cluster_summary(State(state): State<AppState>) -> impl IntoResponse {
    let spawned_count = state.processes.iter().count() as i64;

    // Meshes + mean_peer_count: one heartbeat query per known node type
    let mut meshes: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut peer_sum: i64 = 0;
    let mut peer_n: i64 = 0;
    for svc in KNOWN_NODE_TYPES.iter() {
        let url = format!(
            "{}/api/traces?service={}&operation=rafka.mesh.heartbeat&limit=1&lookback=2m",
            state.jaeger_url, svc
        );
        if let Ok(resp) = state.http.get(&url).send().await {
            if let Ok(body) = resp.json::<Value>().await {
                if let Some(first) = body["data"]
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|t| t["spans"].as_array())
                    .and_then(|a| a.first())
                {
                    if let Some(tags) = first["tags"].as_array() {
                        for t in tags {
                            if t["key"] == "mesh_id" {
                                if let Some(m) = t["value"].as_str() {
                                    meshes.insert(m.to_string());
                                }
                            }
                            if t["key"] == "peer_count" {
                                if let Some(p) = t["value"].as_i64() {
                                    peer_sum += p;
                                    peer_n += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    let mean_peer_count: f64 = if peer_n > 0 {
        peer_sum as f64 / peer_n as f64
    } else {
        0.0
    };

    // chaos_events_1m
    let chaos_url = format!(
        "{}/api/traces?service=rfa&operation=rafka.chaos.primitive.executed&limit=300&lookback=1m",
        state.jaeger_url
    );
    let chaos_events_1m: i64 = match state.http.get(&chaos_url).send().await {
        Ok(r) => match r.json::<Value>().await {
            Ok(b) => b["data"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|t| t["spans"].as_array())
                        .flat_map(|ss| ss.iter())
                        .filter(|sp| sp["operationName"] == "rafka.chaos.primitive.executed")
                        .count() as i64
                })
                .unwrap_or(0),
            Err(_) => 0,
        },
        Err(_) => 0,
    };

    (
        StatusCode::OK,
        axum::Json(json!({
            "spawned_count": spawned_count,
            "meshes": meshes.into_iter().collect::<Vec<_>>(),
            "chaos_events_1m": chaos_events_1m,
            "mean_peer_count": mean_peer_count,
        })),
    )
        .into_response()
}

/// `GET /api/chaos/recent` — query Jaeger for chaos.primitive.executed spans
/// in the last 10 minutes; group by primitive name (counts) + return 20 most
/// recent events for the operator-visible Chaos tab.
async fn handle_chaos_recent(State(state): State<AppState>) -> impl IntoResponse {
    let url = format!(
        "{}/api/traces?service=rfa&operation=rafka.chaos.primitive.executed&limit=200&lookback=10m",
        state.jaeger_url
    );
    let body: Value = match state.http.get(&url).send().await {
        Ok(r) => match r.json::<Value>().await {
            Ok(b) => b,
            Err(_) => return (StatusCode::OK, axum::Json(json!({"counts": {}, "recent": []}))).into_response(),
        },
        Err(_) => return (StatusCode::OK, axum::Json(json!({"counts": {}, "recent": []}))).into_response(),
    };
    let mut counts: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    let mut all_events: Vec<(i64, String, String)> = Vec::new(); // (start_us, name, target)
    if let Some(arr) = body["data"].as_array() {
        for trace in arr {
            if let Some(spans) = trace["spans"].as_array() {
                for s in spans {
                    if s["operationName"] != "rafka.chaos.primitive.executed" {
                        continue;
                    }
                    let tags = s["tags"].as_array();
                    let name = tags
                        .and_then(|t| t.iter().find(|x| x["key"] == "name"))
                        .and_then(|x| x["value"].as_str())
                        .unwrap_or("?")
                        .to_string();
                    let target = tags
                        .and_then(|t| t.iter().find(|x| x["key"] == "target"))
                        .and_then(|x| x["value"].as_str())
                        .unwrap_or("")
                        .to_string();
                    let start_us = s["startTime"].as_i64().unwrap_or(0);
                    *counts.entry(name.clone()).or_insert(0) += 1;
                    all_events.push((start_us, name, target));
                }
            }
        }
    }
    all_events.sort_by(|a, b| b.0.cmp(&a.0));
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    let recent: Vec<Value> = all_events
        .iter()
        .take(20)
        .map(|(t, name, target)| {
            let age_s = ((now_us - t).max(0)) / 1_000_000;
            let when = if age_s < 60 {
                format!("{age_s}s ago")
            } else if age_s < 3600 {
                format!("{}m ago", age_s / 60)
            } else {
                format!("{}h ago", age_s / 3600)
            };
            json!({
                "name": name,
                "description": primitive_description(name),
                "target": target,
                "when": when,
            })
        })
        .collect();
    (
        StatusCode::OK,
        axum::Json(json!({"counts": counts, "recent": recent})),
    )
        .into_response()
}

/// `GET /api/alerts` — query Jaeger for chaos.primitive.detected spans with
/// non-Passed results in the last 10 minutes; surface them as alerts.
async fn handle_alerts(State(state): State<AppState>) -> impl IntoResponse {
    let span = info_span!("rafka.ui.alerts.query", "otel.kind" = "internal");
    let _enter = span.enter();

    let url = format!(
        "{}/api/traces?service=rfa&operation=rafka.chaos.primitive.detected&limit=100&lookback=10m",
        state.jaeger_url
    );
    let body: Value = match state.http.get(&url).send().await {
        Ok(r) => match r.json::<Value>().await {
            Ok(b) => b,
            Err(_) => return (StatusCode::OK, axum::Json(json!({"alerts": []}))).into_response(),
        },
        Err(_) => return (StatusCode::OK, axum::Json(json!({"alerts": []}))).into_response(),
    };
    let mut alerts: Vec<Value> = Vec::new();
    if let Some(arr) = body["data"].as_array() {
        for trace in arr {
            let trace_id = trace["traceID"].as_str().unwrap_or("").to_string();
            if let Some(spans) = trace["spans"].as_array() {
                for s in spans {
                    if s["operationName"] != "rafka.chaos.primitive.detected" {
                        continue;
                    }
                    let result_tag = s["tags"]
                        .as_array()
                        .and_then(|tags| {
                            tags.iter()
                                .find(|t| t["key"] == "result")
                                .and_then(|t| t["value"].as_str())
                        })
                        .unwrap_or("");
                    if result_tag != "passed" && !result_tag.is_empty() {
                        let name_tag = s["tags"]
                            .as_array()
                            .and_then(|tags| {
                                tags.iter()
                                    .find(|t| t["key"] == "name")
                                    .and_then(|t| t["value"].as_str())
                            })
                            .unwrap_or("?");
                        alerts.push(json!({
                            "kind": format!("chaos:{result_tag}"),
                            "message": format!("primitive '{name_tag}' did not pass detection"),
                            "trace_id": trace_id.clone(),
                        }));
                    }
                }
            }
        }
    }
    (StatusCode::OK, axum::Json(json!({"alerts": alerts}))).into_response()
}

/// `GET /api/topology` — return adjacency for the live mesh.
/// Nodes: ONE PER SPAWNED SUBPROCESS (so 3 brokers = 3 distinct nodes). Each
/// node's mesh_id is resolved by querying Jaeger heartbeats filtered on
/// `node_name` tag. Edges: within-mesh full clique + cross-mesh dashed edges.
async fn handle_topology(State(state): State<AppState>) -> impl IntoResponse {
    let span = info_span!("rafka.ui.topology.query", "otel.kind" = "internal");
    let _enter = span.enter();

    let spawned: Vec<String> = state.processes.iter().map(|e| e.key().clone()).collect();
    let mut nodes: Vec<Value> = Vec::new();
    for name in &spawned {
        // Derive node_type from the name prefix (e.g. "broker-abc" → "broker").
        let node_type = KNOWN_NODE_TYPES
            .iter()
            .find(|t| name.starts_with(*t))
            .copied()
            .unwrap_or("?");
        // Resolve per-instance mesh_id from the most-recent heartbeat span filtered
        // by node_name tag. Jaeger tag-filter format: tags={"node_name":"<value>"}.
        let tags_json = serde_json::to_string(&serde_json::json!({"node_name": name}))
            .unwrap_or_else(|_| "{}".into());
        let tags_enc = urlencoding::encode(&tags_json);
        let hb_url = format!(
            "{}/api/traces?service={}&operation=rafka.mesh.heartbeat&limit=1&lookback=2m&tags={}",
            state.jaeger_url, node_type, tags_enc
        );
        let (mesh_id, peer_count) = match state.http.get(&hb_url).send().await {
            Ok(resp) => match resp.json::<Value>().await {
                Ok(body) => {
                    let span = body["data"]
                        .as_array()
                        .and_then(|a| a.first())
                        .and_then(|t| t["spans"].as_array())
                        .and_then(|a| a.first());
                    let m = span
                        .and_then(|sp| sp["tags"].as_array())
                        .and_then(|tags| {
                            tags.iter()
                                .find(|t| t["key"] == "mesh_id")
                                .and_then(|t| t["value"].as_str())
                                .map(String::from)
                        })
                        .unwrap_or_else(|| "default".to_string());
                    let p = span
                        .and_then(|sp| sp["tags"].as_array())
                        .and_then(|tags| {
                            tags.iter()
                                .find(|t| t["key"] == "peer_count")
                                .and_then(|t| t["value"].as_i64())
                        })
                        .unwrap_or(0);
                    (m, p)
                }
                Err(_) => ("default".to_string(), 0),
            },
            Err(_) => ("default".to_string(), 0),
        };
        nodes.push(json!({
            "id": name,
            "type": node_type,
            "mesh_id": mesh_id,
            "peer_count": peer_count,
        }));
    }

    // 2. edges = pairs of services that exchanged frames recently.
    //    For each known service A, query its frame.sent traces; the peer_id
    //    tag tells us who it talked to. Cross-reference: if peer_id matches
    //    another service's most-recent boot-trace node_id, add an edge.
    //
    //    Simpler heuristic until we wire that cross-ref: query peer.connected
    //    for each service, count distinct peer_ids → if >0, draw an edge to
    //    *some* peer. For chunk-1 of the topology view we just enumerate the
    //    "this service has peers" facts. A future iteration adds true pairwise
    //    edges by resolving peer_id back to a service name.
    let mut edges: Vec<Value> = Vec::new();

    // Edges: render within-mesh full mesh + explicit cross-mesh edges. Within-mesh:
    // any two nodes with the same mesh_id get a "within" edge (frame_count=0 as
    // placeholder until traffic weighting lands). Cross-mesh: pairs whose mesh_ids
    // differ are flagged so the UI can draw them with a distinct style.
    for (i, a) in nodes.iter().enumerate() {
        for b in nodes.iter().skip(i + 1) {
            let mesh_a = a["mesh_id"].as_str().unwrap_or("default");
            let mesh_b = b["mesh_id"].as_str().unwrap_or("default");
            let kind = if mesh_a == mesh_b { "within" } else { "cross" };
            edges.push(json!({
                "from": a["id"].as_str().unwrap_or(""),
                "to": b["id"].as_str().unwrap_or(""),
                "kind": kind,
                "frame_count": 0
            }));
        }
    }

    drop(_enter);
    (
        StatusCode::OK,
        axum::Json(json!({"nodes": nodes, "edges": edges})),
    )
        .into_response()
}

async fn handle_nodes(State(state): State<AppState>) -> impl IntoResponse {
    let url = format!("{}/api/services", state.jaeger_url);
    let span = info_span!("rafka.ui.jaeger.query", endpoint = "/api/services", "otel.kind" = "client");
    let result = state.http.get(&url).send().instrument(span).await;

    match result {
        Ok(resp) => match resp.json::<Value>().await {
            Ok(body) => {
                let nodes: Vec<&str> = body["data"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str())
                            .filter(|s| KNOWN_NODE_TYPES.contains(s))
                            .collect()
                    })
                    .unwrap_or_default();
                (StatusCode::OK, axum::Json(json!({"nodes": nodes}))).into_response()
            }
            Err(_) => (
                StatusCode::BAD_GATEWAY,
                axum::Json(json!({"error": "invalid response from jaeger"})),
            )
                .into_response(),
        },
        Err(_) => (
            StatusCode::BAD_GATEWAY,
            axum::Json(json!({"error": "jaeger unreachable"})),
        )
            .into_response(),
    }
}

async fn handle_boot_trace(
    State(state): State<AppState>,
    Query(params): Query<BootTraceQuery>,
) -> impl IntoResponse {
    let svc = &params.service;
    let url = format!(
        "{}/api/traces?service={}&operation=rafka.mesh.node.ready&limit=1&lookback=10m",
        state.jaeger_url, svc
    );
    let span = info_span!(
        "rafka.ui.jaeger.query",
        endpoint = "/api/traces",
        service = %svc,
        "otel.kind" = "client",
    );
    let result = state.http.get(&url).send().instrument(span).await;

    match result {
        Ok(resp) => match resp.json::<Value>().await {
            Ok(body) => {
                let traces = body["data"].as_array();
                match traces.and_then(|arr| arr.first()) {
                    Some(first) => (StatusCode::OK, axum::Json(json!({"data": [first]}))).into_response(),
                    None => (
                        StatusCode::NOT_FOUND,
                        axum::Json(json!({"error": format!("no boot trace found for service {svc}")})),
                    )
                        .into_response(),
                }
            }
            Err(_) => (
                StatusCode::BAD_GATEWAY,
                axum::Json(json!({"error": "invalid response from jaeger"})),
            )
                .into_response(),
        },
        Err(_) => (
            StatusCode::BAD_GATEWAY,
            axum::Json(json!({"error": "jaeger unreachable"})),
        )
            .into_response(),
    }
}

async fn handle_heartbeat(
    State(state): State<AppState>,
    Query(params): Query<BootTraceQuery>,
) -> impl IntoResponse {
    let svc = &params.service;
    let url = format!(
        "{}/api/traces?service={}&operation=rafka.mesh.heartbeat&limit=1&lookback=10m",
        state.jaeger_url, svc
    );
    let span = info_span!(
        "rafka.ui.jaeger.query",
        endpoint = "/api/traces/heartbeat",
        service = %svc,
        "otel.kind" = "client",
    );
    let result = state.http.get(&url).send().instrument(span).await;

    match result {
        Ok(resp) => match resp.json::<Value>().await {
            Ok(body) => {
                let first_span = body["data"]
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|t| t["spans"].as_array())
                    .and_then(|a| a.first())
                    .cloned();
                match first_span {
                    Some(sp) => {
                        let tags: std::collections::HashMap<String, Value> = sp["tags"]
                            .as_array()
                            .unwrap_or(&vec![])
                            .iter()
                            .filter_map(|t| Some((t["key"].as_str()?.to_string(), t["value"].clone())))
                            .collect();
                        let node_id = tags.get("node_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let peer_count = tags.get("peer_count").and_then(|v| v.as_i64()).unwrap_or(0) as u64;
                        let last_heartbeat_us = sp["startTime"].as_i64().unwrap_or(0);
                        // Compute age_ms so the heartbeat panel can show a relative "X.X s ago"
                        // without needing client-side wall-clock arithmetic against Jaeger's
                        // microsecond timestamps.
                        let now_us = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_micros() as i64)
                            .unwrap_or(0);
                        let age_ms = if last_heartbeat_us > 0 {
                            ((now_us - last_heartbeat_us).max(0)) / 1000
                        } else {
                            0
                        };
                        (StatusCode::OK, axum::Json(json!({
                            "node_id": node_id,
                            "peer_count": peer_count,
                            "last_heartbeat_us": last_heartbeat_us,
                            "age_ms": age_ms,
                        }))).into_response()
                    }
                    None => (
                        StatusCode::NOT_FOUND,
                        axum::Json(json!({"error": format!("no heartbeat trace found for {svc}")})),
                    ).into_response(),
                }
            }
            Err(_) => (
                StatusCode::BAD_GATEWAY,
                axum::Json(json!({"error": "invalid response from jaeger"})),
            ).into_response(),
        },
        Err(_) => (
            StatusCode::BAD_GATEWAY,
            axum::Json(json!({"error": "jaeger unreachable"})),
        ).into_response(),
    }
}

async fn handle_spawn(
    State(state): State<AppState>,
    Json(body): Json<SpawnRequest>,
) -> impl IntoResponse {
    let node_type = body.node_type.as_str();
    if !KNOWN_NODE_TYPES.contains(&node_type) {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": format!("unknown node_type: {node_type}")})),
        )
            .into_response();
    }

    let suffix: String = {
        let mut rng = rand::thread_rng();
        (0..8).map(|_| format!("{:x}", rng.gen::<u8>() & 0xf)).collect()
    };
    let node_name = format!("{}-{}", node_type, suffix);

    let spawn_dir = format!("E:/tmp/rafka-ui-nodes/{}", node_name);
    if let Err(e) = std::fs::create_dir_all(&spawn_dir) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": format!("failed to create spawn dir: {e}")})),
        )
            .into_response();
    }

    let binary = format!(
        "{}/debug/rafka-{}.exe",
        state.cargo_target_dir, node_type
    );

    let otlp = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4316".to_string());
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());

    let mut cmd = tokio::process::Command::new(&binary);
    cmd.env("OTEL_EXPORTER_OTLP_ENDPOINT", &otlp)
        .env("OTEL_SERVICE_NAME", node_type)
        .env("RAFKA_DATA_DIR", &spawn_dir)
        .env("RAFKA_NODE_NAME", &node_name)
        .env("RUST_LOG", &rust_log);
    if let Some(extras) = &body.extra_env {
        for (k, v) in extras {
            cmd.env(k, v);
        }
    }

    let node_name_c = node_name.clone();
    let node_type_c = node_type.to_string();

    match cmd.spawn() {
        Ok(child) => {
            let pid = child.id().unwrap_or(0);
            state.processes.insert(node_name.clone(), Mutex::new(child));

            let span = info_span!(
                "rafka.ui.subprocess.spawned",
                node_name = %node_name_c,
                node_type = %node_type_c,
                pid = pid,
                "otel.kind" = "internal",
            );
            span.in_scope(|| {
                info!(node_name = %node_name_c, node_type = %node_type_c, pid, "subprocess spawned");
            });

            (
                StatusCode::CREATED,
                axum::Json(json!({"node_name": node_name, "pid": pid})),
            )
                .into_response()
        }
        Err(e) => {
            let span = info_span!(
                "rafka.ui.subprocess.spawn_failed",
                node_name = %node_name_c,
                node_type = %node_type_c,
                error = %e,
                "otel.kind" = "internal",
            );
            span.in_scope(|| {
                tracing::error!(error = %e, binary = %binary, "subprocess spawn failed");
            });

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": format!("spawn failed: {e}")})),
            )
                .into_response()
        }
    }
}

async fn handle_kill(
    State(state): State<AppState>,
    Path(node_name): Path<String>,
) -> impl IntoResponse {
    let entry = state.processes.remove(&node_name);
    let (_, mutex_child) = match entry {
        Some(e) => e,
        None => {
            return (
                StatusCode::NOT_FOUND,
                axum::Json(json!({"error": format!("no subprocess named {node_name}")})),
            )
                .into_response();
        }
    };

    let mut child = mutex_child.into_inner();
    let pid = child.id().unwrap_or(0);

    // Phase 1: start_kill (Windows terminate-process; SIGTERM-equivalent on Unix)
    let _ = child.start_kill();

    let reason = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(_) => "graceful",
        Err(_) => {
            // Timeout — force kill and wait
            let _ = child.kill().await;
            let _ = child.wait().await;
            "forced"
        }
    };

    // Best-effort data dir cleanup — don't fail response if this errors
    let spawn_dir = format!("E:/tmp/rafka-ui-nodes/{}", node_name);
    if let Err(e) = tokio::fs::remove_dir_all(&spawn_dir).await {
        tracing::warn!(dir = %spawn_dir, error = %e, "failed to remove subprocess data dir");
    }

    let span = info_span!(
        "rafka.ui.subprocess.killed",
        node_name = %node_name,
        pid = pid,
        reason = reason,
        "otel.kind" = "internal",
    );
    span.in_scope(|| {
        info!(node_name = %node_name, pid, reason, "subprocess killed");
    });

    (StatusCode::OK, axum::Json(json!({"node_name": node_name, "reason": reason}))).into_response()
}

async fn trace_middleware(req: Request, next: Next) -> Response {
    use opentelemetry::global;
    use opentelemetry_http::HeaderExtractor;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let method = req.method().to_string();
    let path = req.uri().path().to_string();

    // Extract incoming W3C traceparent so the rafka.ui.http.request span chains
    // under the caller's trace (e.g. rfa CLI invocation). When no traceparent
    // header is present, set_parent on a default context is a no-op and the span
    // becomes its own root — matches in-browser-fetch behaviour.
    let parent_ctx = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(req.headers()))
    });

    let span = info_span!(
        "rafka.ui.http.request",
        method = %method,
        path = %path,
        "otel.kind" = "server",
    );
    span.set_parent(parent_ctx);

    next.run(req).instrument(span).await
}

/// Background task that periodically reaps subprocesses which have already exited
/// (crashed, panicked, OOM-killed) but whose handle still sits in the DashMap.
/// Without this, chaos primitives keep picking dead names from /api/nodes/spawned
/// and DELETE returns 404, polluting the soak report.
async fn reaper_loop(processes: Arc<DashMap<String, Mutex<tokio::process::Child>>>) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        let names: Vec<String> = processes.iter().map(|e| e.key().clone()).collect();
        for name in names {
            // Grab the child briefly to call try_wait; if exited, remove the entry.
            let exited_status = if let Some(entry) = processes.get(&name) {
                let mut guard = entry.value().lock().await;
                match guard.try_wait() {
                    Ok(Some(status)) => Some(status),
                    _ => None,
                }
            } else {
                None
            };
            if let Some(status) = exited_status {
                processes.remove(&name);
                tracing::info_span!(
                    "rafka.ui.subprocess.reaped",
                    node_name = %name,
                    exit_code = status.code().unwrap_or(-1) as i64,
                    "otel.kind" = "internal",
                )
                .in_scope(|| info!(node_name = %name, exit_code = status.code().unwrap_or(-1), "subprocess reaped — exited without DELETE"));
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = rafka_telemetry::init_telemetry("topology-ui");

    let bind_addr = std::env::var("RAFKA_TOPOLOGY_UI_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:19090".to_string());

    let jaeger_url = std::env::var("JAEGER_QUERY_URL")
        .unwrap_or_else(|_| "http://localhost:16686".to_string());

    // CARGO_TARGET_DIR env wins. Otherwise: derive from our own exe path so spawned
    // siblings match the build that produced us. Falls back to "./target" only if exe
    // path lookup fails.
    let cargo_target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| {
        std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().and_then(|d| d.parent()).map(|p| p.to_path_buf()))
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| "./target".to_string())
    });
    tracing::info!(cargo_target_dir = %cargo_target_dir, "subprocess binary search root");

    let addr: SocketAddr = bind_addr.parse()?;

    let state = AppState {
        http: reqwest::Client::new(),
        jaeger_url,
        cargo_target_dir,
        processes: Arc::new(DashMap::new()),
    };

    // Subprocess reaper: every 5s, iterate processes + call try_wait. If a child
    // has exited (crash, OOM, panic) the DashMap still holds the handle but the
    // PID is dead. Reaper removes those entries so /api/nodes/spawned reflects
    // reality. Closes spawned-list runbook Mode 3 ("lists names of dead subprocs").
    tokio::spawn(reaper_loop(Arc::clone(&state.processes)));

    let app = Router::new()
        .route("/", get(handle_root))
        .route("/api/health", get(handle_health))
        .route("/api/nodes", get(handle_nodes))
        .route("/api/boot-trace", get(handle_boot_trace))
        .route("/api/heartbeat", get(handle_heartbeat))
        .route("/api/nodes/spawn", post(handle_spawn))
        .route("/api/nodes/spawned", get(handle_spawned_list))
        .route("/api/topology", get(handle_topology))
        .route("/api/alerts", get(handle_alerts))
        .route("/api/chaos/recent", get(handle_chaos_recent))
        .route("/api/chaos/timeline", get(handle_chaos_timeline))
        .route("/api/heartbeats", get(handle_heartbeats))
        .route("/api/cluster/summary", get(handle_cluster_summary))
        .route("/api/nodes/{node_name}", delete(handle_kill))
        .with_state(state)
        .layer(middleware::from_fn(trace_middleware));

    info!("topology-ui listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
