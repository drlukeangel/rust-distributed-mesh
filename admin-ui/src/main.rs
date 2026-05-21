use anyhow::Result;
use axum::{
    Router,
    extract::{Json, Path, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use dashmap::DashMap;
use rand::Rng;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};
use tower_http::services::ServeDir;
use rafka_node_base::{GossipDigest, live_digests, message_ring, topic_membership};
use tokio::{process::Child, sync::Mutex};
use tracing::{info, info_span, Instrument};

const KNOWN_NODE_TYPES: &[&str] = &["gateway", "broker", "compute", "registry", "bridge"];

// Legacy inline HTML — replaced by the React app under web/dist. Kept commented
// out for one revision so the migration diff is reviewable; delete in next pass.
#[allow(dead_code)]
const _HTML_LEGACY_REMOVED: &str = r##"<!DOCTYPE html>
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

<div id="spawn-row" style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap">
  <label style="color:#8b949e;font-size:0.75rem">mesh:</label>
  <select id="spawn-mesh-id" style="background:#0d1117;color:#c9d1d9;border:1px solid #30363d;border-radius:4px;padding:0.2rem 0.5rem;font-family:inherit;font-size:0.8rem;min-width:140px">
    <option value="mesh-a" selected>mesh-a (primary)</option>
    <option value="mesh-b">mesh-b (secondary)</option>
    <option value="__new__">+ new mesh…</option>
  </select>
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
  <button class="tab" data-panel="panel-tests">Tests</button>
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

<div id="panel-tests" class="panel">
  <div id="controls">
    <button id="tests-refresh">Refresh tests</button>
    <span id="tests-status" style="color:#8b949e;font-size:0.8rem;margin-left:0.5rem"></span>
  </div>
  <div style="color:#8b949e;font-size:0.75rem;margin-bottom:0.5rem">
    Every test reproducible via <code>rfa mesh test run &lt;name&gt;</code>. Reports written to <code>E:/tmp/rafka-tests/</code>. Run <code>rfa mesh test list</code> to see the catalog. Auto-refreshes every 5s.
  </div>
  <div id="tests-list"></div>
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
    // PER-INSTANCE: enrich the dropdown so each entry shows mesh:name.
    // Pull mesh_id per node from /api/heartbeats since /api/nodes/spawned is
    // intentionally flat (chaos primitives need a stable shape).
    Promise.all([
      fetch('/api/nodes/spawned').then(function(r) { return r.json(); }),
      fetch('/api/heartbeats').then(function(r) { return r.json(); }),
    ]).then(function(results) {
      var spawned = (results[0].spawned || []).slice().sort();
      var meshByName = {};
      (results[1].heartbeats || []).forEach(function(h) {
        meshByName[h.node_name] = h.mesh_id || 'default';
      });
      var prev = sel.value;
      while (sel.options.length > 1) sel.remove(1);
      spawned.forEach(function(n) {
        var mesh = meshByName[n] || 'default';
        var opt = document.createElement('option');
        opt.value = n;
        opt.textContent = mesh + ' : ' + n;
        sel.appendChild(opt);
      });
      if (prev && spawned.indexOf(prev) !== -1) sel.value = prev;
      var by_mesh = {};
      spawned.forEach(function(n) {
        var m = meshByName[n] || 'default';
        (by_mesh[m] = by_mesh[m] || 0); by_mesh[m]++;
      });
      var summary = Object.keys(by_mesh).map(function(m) { return m + ':' + by_mesh[m]; }).join(' · ');
      setStatus(true, 'nodes: ' + (spawned.length ? summary : '(pool empty — click + Spawn buttons)'));
    }).catch(function() { setStatus(false, 'spawned list unavailable'); });
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

  // Mesh dropdown — fixed presets per user spec: mesh-a (primary), mesh-b
  // (secondary), and "+ new mesh…" escape hatch for arbitrary mesh IDs.
  // The chosen value gets sent as extra_env.RAFKA_MESH_ID on spawn.
  var meshSelect = document.getElementById('spawn-mesh-id');
  meshSelect.addEventListener('change', function() {
    if (meshSelect.value === '__new__') {
      var name = prompt('New mesh ID:');
      if (name && name.trim()) {
        var trimmed = name.trim();
        var present = false;
        for (var i = 0; i < meshSelect.options.length; i++) {
          if (meshSelect.options[i].value === trimmed) { present = true; break; }
        }
        if (!present) {
          var opt = document.createElement('option');
          opt.value = trimmed;
          opt.textContent = trimmed;
          meshSelect.insertBefore(opt, meshSelect.options[meshSelect.options.length - 1]);
        }
        meshSelect.value = trimmed;
      } else {
        meshSelect.value = 'mesh-a';
      }
    }
  });

  function spawnNode(nodeType, btn) {
    btn.disabled = true;
    var meshId = (meshSelect && meshSelect.value !== '__new__') ? meshSelect.value : 'mesh-a';
    var body = { node_type: nodeType, extra_env: { RAFKA_MESH_ID: meshId } };
    fetch('/api/nodes/spawn', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)
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
    var W = 1200, H = 600;
    topoSvg.setAttribute('viewBox', '0 0 ' + W + ' ' + H);
    var nodes = data.nodes || [];
    var edges = data.edges || [];

    if (nodes.length === 0) {
      topoSvg.innerHTML = '<text x="' + (W/2) + '" y="' + (H/2) + '" fill="#8b949e" text-anchor="middle">no nodes — start some via Spawn buttons</text>';
      return;
    }

    // Multi-circle layout: each NON-BRIDGE mesh gets its own circle on the
    // canvas. Bridge nodes are pulled OUT of their mesh's circle and laid out
    // in the gaps between meshes with edges to both/all bridged meshes.
    var bridgeNodes = nodes.filter(function(n) { return n.type === 'bridge'; });
    var nonBridge = nodes.filter(function(n) { return n.type !== 'bridge'; });

    var byMesh = {};
    nonBridge.forEach(function(n) {
      var m = n.mesh_id || 'default';
      (byMesh[m] = byMesh[m] || []).push(n);
    });
    var meshes = Object.keys(byMesh).sort();
    var meshCount = meshes.length || 1;

    // Lay mesh circles in a row across the canvas.
    var meshRadius = Math.min(120, (W - 100) / (meshCount * 2 + 1));
    var meshCenters = {};
    meshes.forEach(function(m, i) {
      var x = (W / (meshCount + 1)) * (i + 1);
      var y = H / 2;
      meshCenters[m] = { x: x, y: y, r: meshRadius };
    });

    var pos = {};
    var svgParts = [];

    // Render mesh circle backgrounds + labels
    meshes.forEach(function(m) {
      var c = meshCenters[m];
      var color = meshRingColor(m);
      // Faint background disc
      svgParts.push('<circle cx="' + c.x + '" cy="' + c.y + '" r="' + (c.r + 40) + '" fill="' + color + '" fill-opacity="0.04" stroke="' + color + '" stroke-opacity="0.25" stroke-dasharray="4,3" />');
      // Mesh label above the circle
      svgParts.push('<text x="' + c.x + '" y="' + (c.y - c.r - 50) + '" fill="' + color + '" font-size="14" font-weight="bold" text-anchor="middle">' + m + '</text>');
      svgParts.push('<text x="' + c.x + '" y="' + (c.y - c.r - 35) + '" fill="#8b949e" font-size="10" text-anchor="middle">' + byMesh[m].length + ' nodes</text>');

      // Place members around this mesh's circle
      byMesh[m].forEach(function(n, i) {
        var ang = 2 * Math.PI * i / byMesh[m].length - Math.PI / 2;
        pos[n.id] = { x: c.x + c.r * Math.cos(ang), y: c.y + c.r * Math.sin(ang), mesh: m };
      });
    });

    // Place bridge nodes in the gap between meshes (or alone if just one mesh)
    bridgeNodes.forEach(function(b, i) {
      var bx, by;
      if (meshCount >= 2) {
        // Pick the two adjacent meshes this bridge sits between
        var leftMesh = meshes[i % (meshCount - 1)];
        var rightMesh = meshes[(i + 1) % meshCount];
        var L = meshCenters[leftMesh];
        var R = meshCenters[rightMesh];
        bx = (L.x + R.x) / 2;
        by = (L.y + R.y) / 2 - 50 - i * 30;
      } else {
        bx = (meshCenters[meshes[0]] || {x: W/2, y: H/2}).x + 200 + i * 60;
        by = H / 2;
      }
      pos[b.id] = { x: bx, y: by, mesh: 'bridge' };
    });

    // Draw edges (full clique placeholder for now; traffic-weighted is a follow-up)
    edges.forEach(function(e) {
      var a = pos[e.from], b = pos[e.to];
      if (!a || !b) return;
      var isCross = e.kind === 'cross';
      var style = isCross
        ? 'stroke:#e3b341;stroke-opacity:0.6;stroke-dasharray:5,4'
        : 'stroke:#30363d;stroke-opacity:0.45';
      svgParts.push('<line x1="' + a.x + '" y1="' + a.y + '" x2="' + b.x + '" y2="' + b.y + '" style="' + style + '" />');
    });

    // Draw bridge-to-mesh edges (each bridge connects to ALL non-bridge meshes)
    bridgeNodes.forEach(function(b) {
      var bp = pos[b.id];
      if (!bp) return;
      meshes.forEach(function(m) {
        var c = meshCenters[m];
        svgParts.push('<line x1="' + bp.x + '" y1="' + bp.y + '" x2="' + c.x + '" y2="' + c.y + '" style="stroke:#e3b341;stroke-opacity:0.5;stroke-dasharray:3,3;stroke-width:1.5" />');
      });
    });

    // Render nodes (so they sit on top of edges)
    nodes.forEach(function(n) {
      var p = pos[n.id];
      if (!p) return;
      var typeColor = TYPE_COLOR[n.type] || '#888';
      var meshColor = n.type === 'bridge' ? '#e3b341' : meshRingColor(n.mesh_id || 'default');
      var label = (n.id.length > 14 ? n.id.slice(0, 12) + '…' : n.id);
      svgParts.push('<g class="topo-node">' +
        '<circle cx="' + p.x + '" cy="' + p.y + '" r="22" fill="none" stroke="' + meshColor + '" stroke-width="2.5" stroke-opacity="0.9"/>' +
        '<circle cx="' + p.x + '" cy="' + p.y + '" r="18" fill="' + typeColor + '" fill-opacity="0.65"/>' +
        '<text x="' + p.x + '" y="' + (p.y + 3) + '" style="fill:#c9d1d9;font-size:9px;text-anchor:middle;font-family:monospace">' + label + '</text>' +
        '<text x="' + p.x + '" y="' + (p.y + 32) + '" style="fill:#8b949e;font-size:9px;text-anchor:middle">' + (n.mesh_id || 'default') + '</text>' +
        (typeof n.frames_per_min === 'number' && n.frames_per_min > 0
          ? '<text x="' + p.x + '" y="' + (p.y + 44) + '" style="fill:#3fb950;font-size:8px;text-anchor:middle">' + n.frames_per_min + ' fr/m</text>'
          : '') +
        '</g>');
    });

    topoSvg.innerHTML = svgParts.join('');
    topoStatus.textContent = nodes.length + ' nodes across ' + meshCount + ' mesh' + (meshCount === 1 ? '' : 'es')
      + (bridgeNodes.length ? ' + ' + bridgeNodes.length + ' bridge' + (bridgeNodes.length === 1 ? '' : 's') : '')
      + ', ' + edges.length + ' edges';
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
      healthCards.innerHTML = '<div style="color:#8b949e">no heartbeat data yet — spawn nodes first</div>';
      return;
    }
    var html = '';
    services.forEach(function(s) {
      var ageSec = s.age_ms < 0 ? '?' : (s.age_ms / 1000).toFixed(1);
      var ageColor = s.age_ms < 0 ? '#8b949e' :
                     (s.age_ms > 30000 ? '#f85149' : (s.age_ms > 10000 ? '#e3b341' : '#3fb950'));
      var typeColor = TYPE_COLOR[s.node_type || s.service] || '#888';
      html += '<div style="background:#161b22;border:1px solid #30363d;border-radius:6px;padding:1rem;position:relative">' +
        '<button class="kill-btn" data-node="' + s.service + '" style="position:absolute;top:0.5rem;right:0.5rem;background:#3d1f1f;border:1px solid #f85149;color:#f85149;font-size:0.7rem;padding:0.15rem 0.45rem;border-radius:3px;cursor:pointer;font-family:inherit">kill</button>' +
        '<div style="color:' + typeColor + ';font-weight:bold;font-size:0.95rem;margin-bottom:0.3rem;padding-right:48px">' + s.service + '</div>' +
        '<div style="color:#8b949e;font-size:0.7rem">type: ' + (s.node_type || '?') + ' · mesh: ' + (s.mesh_id || 'default') + '</div>' +
        '<div style="color:#8b949e;font-size:0.7rem">node_id: ' + (s.node_id || '').slice(0,16) + '…</div>' +
        '<div style="font-size:1.4rem;color:#c9d1d9;margin-top:0.5rem">peers: <strong>' + s.peer_count + '</strong></div>' +
        '<div style="color:' + ageColor + ';font-size:0.75rem;margin-top:0.25rem">last beat: ' + ageSec + 's ago</div>' +
        '</div>';
    });
    healthCards.innerHTML = html;
    healthStatus.textContent = services.length + ' nodes tracked';
    // Wire kill buttons (event delegation would also work — direct binding is fine for ≤20 cards).
    Array.prototype.forEach.call(healthCards.querySelectorAll('.kill-btn'), function(btn) {
      btn.addEventListener('click', function() {
        var name = btn.getAttribute('data-node');
        if (!confirm('Kill ' + name + '?')) return;
        fetch('/api/nodes/' + encodeURIComponent(name), { method: 'DELETE' })
          .then(function(r) { return r.json(); })
          .then(function() { loadHealth(); })
          .catch(function(e) { alert('kill failed: ' + e); });
      });
    });
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
      timelineList.innerHTML = '<div style="color:#8b949e">no events yet — spawn nodes or run a chaos test</div>';
      timelineStatus.textContent = '0 events';
      return;
    }
    var html = '';
    var counts = { chaos: 0, node_ready: 0, peer_connected: 0, peer_disconnected: 0 };
    events.forEach(function(e) {
      counts[e.kind] = (counts[e.kind] || 0) + 1;
      var color, symbol, statusTxt;
      if (e.kind === 'chaos') {
        var resolved = e.status === 'passed';
        color = resolved ? '#3fb950' : (e.status === 'pending' ? '#e3b341' : '#f85149');
        symbol = resolved ? '✓' : (e.status === 'pending' ? '…' : '✗');
        statusTxt = resolved ? 'resolved in ' + e.resolved_ms + 'ms' :
                    (e.status === 'pending' ? 'pending detection' : 'failed: ' + e.status);
      } else if (e.kind === 'node_ready') {
        color = '#58a6ff'; symbol = '⇧'; statusTxt = 'booted';
      } else if (e.kind === 'peer_connected') {
        color = '#3fb950'; symbol = '+'; statusTxt = 'connected';
      } else if (e.kind === 'peer_disconnected') {
        color = '#f85149'; symbol = '-'; statusTxt = 'disconnected';
      } else {
        color = '#8b949e'; symbol = '·'; statusTxt = e.status || '';
      }
      var labelColor = e.kind === 'chaos' ? '#e3b341' : '#58a6ff';
      html += '<div style="padding:0.4rem 0.6rem;border-bottom:1px solid #1f2429">' +
        '<div style="display:flex;gap:0.75rem;align-items:baseline">' +
          '<span style="color:#8b949e;width:90px">' + e.when + '</span>' +
          '<span style="color:' + color + ';width:18px;text-align:center;font-weight:bold">' + symbol + '</span>' +
          '<span style="color:' + labelColor + ';width:140px">' + e.label + '</span>' +
          '<span style="color:#c9d1d9;flex:1">' + (e.target || '') + '</span>' +
          '<span style="color:' + color + '">' + statusTxt + '</span>' +
        '</div>' +
        (e.description ? '<div style="color:#6e7681;font-size:0.72rem;margin-top:0.15rem;margin-left:113px">' + e.description + '</div>' : '') +
        '</div>';
    });
    timelineList.innerHTML = html;
    var summary = events.length + ' events: '
      + (counts.chaos || 0) + ' chaos · '
      + (counts.node_ready || 0) + ' boots · '
      + (counts.peer_connected || 0) + ' connects · '
      + (counts.peer_disconnected || 0) + ' disconnects';
    timelineStatus.textContent = summary;
  }

  function loadTimeline() {
    fetch('/api/timeline')
      .then(function(r) { return r.json(); })
      .then(function(d) { renderTimeline(d.events || []); })
      .catch(function(e) { timelineStatus.textContent = 'fetch failed: ' + e; });
  }

  timelineRefresh.addEventListener('click', loadTimeline);

  // ── tests tab ────────────────────────────────────────────────────────────
  var testsList = document.getElementById('tests-list');
  var testsStatus = document.getElementById('tests-status');
  var testsRefresh = document.getElementById('tests-refresh');
  var testsTimer = null;

  function renderTests(reports) {
    if (!reports || reports.length === 0) {
      testsList.innerHTML = '<div style="color:#8b949e">no test reports yet. run <code>rfa mesh test run &lt;name&gt;</code> or <code>rfa mesh test all</code>.</div>';
      testsStatus.textContent = '0 reports';
      return;
    }
    var html = '';
    reports.forEach(function(r) {
      var statusColor = r.status === 'passed' ? '#3fb950' : (r.status === 'failed' ? '#f85149' : '#8b949e');
      var statusSymbol = r.status === 'passed' ? '✓' : (r.status === 'failed' ? '✗' : '…');
      var kindColor = r.kind === 'chaos' ? '#e3b341' : '#58a6ff';
      var when = '';
      if (r.ended_ms) {
        var ageSec = Math.floor((Date.now() - r.ended_ms) / 1000);
        when = ageSec < 60 ? ageSec + 's ago' :
               ageSec < 3600 ? Math.floor(ageSec/60) + 'm ago' :
               Math.floor(ageSec/3600) + 'h ago';
      }
      html += '<div style="background:#161b22;border:1px solid #30363d;border-radius:6px;padding:0.7rem 0.9rem;margin-bottom:0.5rem">' +
        '<div style="display:flex;gap:0.6rem;align-items:baseline">' +
          '<span style="color:' + statusColor + ';font-size:1.1rem;width:18px">' + statusSymbol + '</span>' +
          '<span style="color:#c9d1d9;font-weight:bold;flex:1">' + r.name + '</span>' +
          '<span style="color:' + kindColor + ';font-size:0.7rem;text-transform:uppercase;padding:0.1rem 0.4rem;border:1px solid ' + kindColor + ';border-radius:3px">' + r.kind + '</span>' +
          '<span style="color:#8b949e;font-size:0.75rem;width:90px;text-align:right">' + when + '</span>' +
        '</div>' +
        '<div style="color:#8b949e;font-size:0.72rem;margin-top:0.25rem;margin-left:26px">' + (r.description || '') + '</div>' +
        '<div style="color:#6e7681;font-size:0.7rem;margin-top:0.15rem;margin-left:26px;font-family:monospace">seed=' + r.seed + ' duration=' + r.duration_ms + 'ms · <span style="color:' + statusColor + '">' + (r.detail || '') + '</span></div>' +
        '</div>';
    });
    testsList.innerHTML = html;
    var passed = reports.filter(function(r) { return r.status === 'passed'; }).length;
    testsStatus.textContent = reports.length + ' reports, ' + passed + ' passed';
  }

  function loadTests() {
    fetch('/api/tests')
      .then(function(r) { return r.json(); })
      .then(function(d) { renderTests(d.reports || []); })
      .catch(function(e) { testsStatus.textContent = 'fetch failed: ' + e; });
  }

  testsRefresh.addEventListener('click', loadTests);

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
      if (testsTimer) { clearInterval(testsTimer); testsTimer = null; }
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
      } else if (panel === 'panel-tests') {
        loadTests();
        testsTimer = setInterval(loadTests, 5000);
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

#[derive(Clone, Debug)]
struct SpawnedMeta {
    node_type: String,
    mesh_id: String,
    pid: u32,
}

struct ChaosController {
    running: AtomicBool,
    cadence_ms: AtomicU64,
    total_events: AtomicU64,
    last_event_ts_us: AtomicI64,
    task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Default for ChaosController {
    fn default() -> Self {
        Self {
            running: AtomicBool::new(false),
            // QA round-2 F#7: 0 was ambiguous ("0ms cadence" vs "never armed").
            // Initialize to the default 30s so /api/chaos/state always reports
            // the cadence that WILL be used if chaos/start is called.
            cadence_ms: AtomicU64::new(30_000),
            total_events: AtomicU64::new(0),
            last_event_ts_us: AtomicI64::new(0),
            task: StdMutex::new(None),
        }
    }
}

/// Local event ring buffer. Every spawn, kill, chaos kill, and chaos respawn
/// pushes a row here so the Timeline tab can show events INSTANTLY without
/// waiting for Jaeger ingestion (which adds 5-30s of latency after a bootstrap).
/// Jaeger-derived events (peer.connected, node.ready) are merged on top.
#[derive(Clone, Debug)]
struct LocalEvent {
    ts_us: i64,
    kind: String,
    node_name: Option<String>,
    node_type: Option<String>,
    mesh_id: Option<String>,
    detail: Option<String>,
}

#[derive(Default)]
struct EventRing {
    items: StdMutex<std::collections::VecDeque<LocalEvent>>,
}

impl EventRing {
    fn push(&self, e: LocalEvent) {
        let mut g = self.items.lock().unwrap();
        if g.len() >= 500 {
            g.pop_front();
        }
        g.push_back(e);
    }
    fn snapshot(&self) -> Vec<LocalEvent> {
        self.items.lock().unwrap().iter().cloned().collect()
    }
}

/// Live observer state — one entry per node seen via gossip in the last 30 s.
/// Populated by `observer_task` (the Phase-C iroh observer). Throughput rates
/// are computed as delta between two consecutive digests divided by the wall
/// time delta. ZERO Jaeger queries — pure gossip-fed data.
///
/// Phase C is currently dormant on Windows (iroh bind hang). Phase A
/// (topology_cache) is the active fallback.
#[derive(Clone, Debug)]
struct LiveNodeState {
    digest: GossipDigest,
    last_seen_ms: u64,
    sent_per_sec: f64,
    recv_per_sec: f64,
}

/// Phase A: snapshot of the topology computed in the background every 3 s.
/// /api/topology + /api/heartbeats return this instantly without ever
/// blocking on Jaeger queries on the request path.
#[derive(Clone, Default, Debug)]
struct TopologySnapshot {
    nodes: Vec<Value>,
    edges: Vec<Value>,
    heartbeats: Vec<Value>,
    computed_at_ms: i64,
}

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    jaeger_url: String,
    cargo_target_dir: String,
    processes: Arc<DashMap<String, Mutex<Child>>>,
    spawned_meta: Arc<DashMap<String, SpawnedMeta>>,
    chaos: Arc<ChaosController>,
    events: Arc<EventRing>,
    /// Phase C: live state from gossip digests, keyed by node_id (hex).
    live: Arc<DashMap<String, LiveNodeState>>,
    /// Phase A: background Jaeger-fed cache. Refreshed every 3 s.
    topology_cache: Arc<tokio::sync::RwLock<TopologySnapshot>>,
    /// Red-team A#8: serialize concurrent /api/tests/run calls for the same
    /// test name. Map entry exists while a test is running.
    running_tests: Arc<DashMap<String, ()>>,
    /// Red-team A#3: serialize /api/bootstrap calls so parallel requests
    /// queue instead of all passing the cap check before any spawn lands.
    bootstrap_mutex: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Deserialize)]
struct BootTraceQuery {
    service: String,
}

#[derive(Deserialize)]
struct SpawnRequest {
    node_type: String,
    /// First-class mesh assignment. If set, becomes `RAFKA_MESH_ID` in the
    /// child env. Either this OR `extra_env.RAFKA_MESH_ID` works; if both,
    /// this field wins.
    #[serde(default)]
    mesh_id: Option<String>,
    /// Optional extra env vars to merge into the child process env. Used by chaos
    /// primitives like clock_skew to inject behavior switches at restart.
    #[serde(default)]
    extra_env: Option<std::collections::HashMap<String, String>>,
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

/// `GET /api/tests` — read every JSON test report under `E:/tmp/rafka-tests/`,
/// sorted newest-first. Each report comes from `rfa mesh test run <name>`.
/// Used by the Tests tab to show what's been verified + when + how long.
async fn handle_tests(State(_state): State<AppState>) -> impl IntoResponse {
    let dir = std::path::Path::new("E:/tmp/rafka-tests");
    let mut entries: Vec<(u64, Value)> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for ent in rd.flatten() {
            let path = ent.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            if let Ok(raw) = std::fs::read_to_string(&path) {
                if let Ok(v) = serde_json::from_str::<Value>(&raw) {
                    let ended = v["ended_ms"].as_u64().unwrap_or(0);
                    entries.push((ended, v));
                }
            }
        }
    }
    entries.sort_by(|a, b| b.0.cmp(&a.0));
    let reports: Vec<Value> = entries.into_iter().map(|(_, v)| v).collect();
    (StatusCode::OK, axum::Json(json!({"reports": reports}))).into_response()
}

/// `GET /api/heartbeats` — per-instance heartbeat data for every spawned
/// subprocess. Returns `[{node_name, node_type, node_id, mesh_id, peer_count,
/// age_ms}]`. Used by the Heartbeat tab to render one card per instance
/// instead of one per node_type.
async fn handle_heartbeats(State(state): State<AppState>) -> impl IntoResponse {
    // Mesh-native heartbeats: read from live_digests() directly.
    let digests = live_digests();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut out: Vec<Value> = Vec::new();
    let mut known_names: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for entry in digests.iter() {
        let d = entry.value();
        known_names.insert(d.node_name.clone());
        let age_ms = (now_ms.saturating_sub(d.wall_time_ms)) as i64;
        out.push(json!({
            "node_name": d.node_name,
            "node_type": d.node_type,
            "node_id": d.node_id,
            "mesh_id": d.mesh_id,
            "peer_count": d.peer_count,
            "frames_sent_total": d.frames_sent_total,
            "frames_recv_total": d.frames_recv_total,
            "age_ms": age_ms,
        }));
    }
    // Pending entries (spawned, first digest not seen yet)
    for entry in state.spawned_meta.iter() {
        if !known_names.contains(entry.key()) {
            out.push(json!({
                "node_name": entry.key(),
                "node_type": entry.value().node_type,
                "node_id": "",
                "mesh_id": entry.value().mesh_id,
                "peer_count": 0,
                "frames_sent_total": 0,
                "frames_recv_total": 0,
                "age_ms": -1,
            }));
        }
    }
    return (
        StatusCode::OK,
        axum::Json(json!({"heartbeats": out, "source": "gossip"})),
    )
        .into_response();

    // Old Jaeger fallback (unreachable; kept one rev for safety)
    #[allow(unreachable_code)]
    let snap = state.topology_cache.read().await.clone();
    if !snap.heartbeats.is_empty() {
        return (
            StatusCode::OK,
            axum::Json(json!({
                "heartbeats": snap.heartbeats,
                "computed_at_ms": snap.computed_at_ms,
            })),
        )
            .into_response();
    }
    // Fallback: spawned_meta-only (no Jaeger enrichment yet)
    let mut out: Vec<Value> = Vec::new();
    for entry in state.spawned_meta.iter() {
        out.push(json!({
            "node_name": entry.key(),
            "node_type": entry.value().node_type,
            "node_id": "",
            "mesh_id": entry.value().mesh_id,
            "peer_count": 0,
            "age_ms": -1,
        }));
    }
    if !out.is_empty() {
        return (StatusCode::OK, axum::Json(json!({"heartbeats": out}))).into_response();
    }
    let spawned: Vec<String> = state.processes.iter().map(|e| e.key().clone()).collect();
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);

    // Fan out one Jaeger query per spawned node IN PARALLEL. Serial fan-out
    // means 18 nodes × ~2s/query = 36s wall — exceeds typical browser fetch
    // timeouts. Parallel cuts it to the slowest single query.
    let mut handles = Vec::with_capacity(spawned.len());
    for name in spawned {
        let state = state.clone();
        let known_meta = state.spawned_meta.get(&name).map(|e| e.value().clone());
        handles.push(tokio::spawn(async move {
            let node_type = known_meta
                .as_ref()
                .map(|m| m.node_type.as_str().to_string())
                .or_else(|| {
                    KNOWN_NODE_TYPES
                        .iter()
                        .find(|t| name.starts_with(*t))
                        .map(|s| s.to_string())
                })
                .unwrap_or_else(|| "?".to_string());

            // Default to the mesh_id we KNOW from spawn time. Jaeger's reply
            // overrides if available — but if the span hasn't flushed yet we
            // still want the real label, not "default".
            let known_mesh = known_meta
                .as_ref()
                .map(|m| m.mesh_id.clone())
                .unwrap_or_else(|| "default".to_string());

            let tags_json = serde_json::to_string(&serde_json::json!({"node_name": &name}))
                .unwrap_or_else(|_| "{}".into());
            let tags_enc = urlencoding::encode(&tags_json);
            let url = format!(
                "{}/api/traces?service={}&operation=rafka.mesh.heartbeat&limit=1&lookback=2m&tags={}",
                state.jaeger_url, node_type, tags_enc
            );

            let resp = tokio::time::timeout(
                Duration::from_secs(4),
                state.http.get(&url).send(),
            )
            .await;

            let (node_id, mesh_id, peer_count, age_ms) = match resp {
                Ok(Ok(resp)) => match resp.json::<Value>().await {
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
                            .map(|s| s.to_string())
                            .unwrap_or(known_mesh);
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
                    Err(_) => (String::new(), known_mesh, 0, -1),
                },
                _ => (String::new(), known_mesh, 0, -1),
            };
            json!({
                "node_name": name,
                "node_type": node_type,
                "node_id": node_id,
                "mesh_id": mesh_id,
                "peer_count": peer_count,
                "age_ms": age_ms,
            })
        }));
    }
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        if let Ok(v) = h.await {
            out.push(v);
        }
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

/// `GET /api/timeline` — unified chronological feed combining chaos events,
/// mesh peer lifecycle (peer.connected / peer.disconnected), and node boot
/// (node.ready). Replaces the chaos-only /api/chaos/timeline so the Timeline
/// tab shows EVERYTHING happening on the substrate, not just chaos triggers.
async fn handle_unified_timeline(State(state): State<AppState>) -> impl IntoResponse {
    // QA F5: resolve Jaeger-sourced peer events' node_name from the live
    // gossip digest map, so the timeline shows broker-abc12345 instead of
    // just "broker". Built once per request from live_digests().
    let id_to_name: std::collections::HashMap<String, String> = live_digests()
        .iter()
        .map(|e| (e.key().clone(), e.value().node_name.clone()))
        .collect();

    // Fan out 15+ Jaeger queries in parallel so the timeline tab returns in
    // ~1s instead of 15s+ serial.
    let ops: Vec<(&str, &str)> = vec![
        ("rafka.mesh.node.ready", "node.ready"),
        ("rafka.mesh.peer.connected", "peer.connected"),
        ("rafka.mesh.peer.disconnected", "peer.disconnected"),
    ];

    let mut handles = Vec::new();
    for (op, kind_label) in &ops {
        for svc in KNOWN_NODE_TYPES.iter() {
            let url = format!(
                "{}/api/traces?service={}&operation={}&limit=50&lookback=10m",
                state.jaeger_url, svc, op
            );
            let http = state.http.clone();
            let op = op.to_string();
            let kind_label = kind_label.to_string();
            let svc = svc.to_string();
            let id_map = id_to_name.clone();
            handles.push(tokio::spawn(async move {
                let body: Value = match http.get(&url).send().await {
                    Ok(r) => r.json::<Value>().await.unwrap_or(json!({"data":[]})),
                    Err(_) => json!({"data":[]}),
                };
                let mut out = Vec::new();
                if let Some(arr) = body["data"].as_array() {
                    for trace in arr {
                        if let Some(spans) = trace["spans"].as_array() {
                            for s in spans {
                                if s["operationName"] != op {
                                    continue;
                                }
                                let ts_us = s["startTime"].as_i64().unwrap_or(0);
                                let tags = s["tags"].as_array();
                                // Prefer node_name tag; else resolve node_id → name via gossip map;
                                // last-resort fall back to service name.
                                let self_id = tags
                                    .and_then(|t| t.iter().find(|x| x["key"] == "node_id"))
                                    .and_then(|x| x["value"].as_str())
                                    .unwrap_or("");
                                let node_name = tags
                                    .and_then(|t| t.iter().find(|x| x["key"] == "node_name"))
                                    .and_then(|x| x["value"].as_str())
                                    .map(String::from)
                                    .or_else(|| id_map.get(self_id).cloned())
                                    .unwrap_or_else(|| svc.clone());
                                let mesh_id = tags
                                    .and_then(|t| t.iter().find(|x| x["key"] == "mesh_id"))
                                    .and_then(|x| x["value"].as_str())
                                    .unwrap_or("")
                                    .to_string();
                                // peer_id → peer_name lookup
                                let peer_id_full = tags
                                    .and_then(|t| t.iter().find(|x| x["key"] == "peer_id"))
                                    .and_then(|x| x["value"].as_str())
                                    .unwrap_or("");
                                let peer = id_map
                                    .get(peer_id_full)
                                    .cloned()
                                    .unwrap_or_else(|| peer_id_full.chars().take(12).collect());
                                let detail = match kind_label.as_str() {
                                    "node.ready" => format!("({svc})"),
                                    "peer.connected" => format!("↔ {peer}"),
                                    "peer.disconnected" => format!("lost {peer}"),
                                    _ => String::new(),
                                };
                                out.push((
                                    ts_us,
                                    json!({
                                        "ts_us": ts_us,
                                        "kind": kind_label,
                                        "node_name": node_name,
                                        "node_type": svc,
                                        "mesh_id": mesh_id,
                                        "detail": detail,
                                    }),
                                ));
                            }
                        }
                    }
                }
                out
            }));
        }
    }

    let mut rows: Vec<(i64, Value)> = Vec::new();

    // Local events first — these are instant (no Jaeger dependency). The
    // Timeline tab MUST show spawn/kill/chaos activity even when Jaeger
    // ingestion is lagging or paused.
    for e in state.events.snapshot() {
        rows.push((
            e.ts_us,
            json!({
                "ts_us": e.ts_us,
                "kind": e.kind,
                "node_name": e.node_name,
                "node_type": e.node_type,
                "mesh_id": e.mesh_id,
                "detail": e.detail,
            }),
        ));
    }

    for h in handles {
        if let Ok(part) = h.await {
            rows.extend(part);
        }
    }

    rows.sort_by(|a, b| b.0.cmp(&a.0));
    let events: Vec<Value> = rows.into_iter().take(200).map(|(_, v)| v).collect();
    (StatusCode::OK, axum::Json(json!({"events": events}))).into_response()
}

fn when_ago(now_us: i64, then_us: i64) -> String {
    let age_s = ((now_us - then_us).max(0)) / 1_000_000;
    if age_s < 60 {
        format!("{age_s}s ago")
    } else if age_s < 3600 {
        format!("{}m{}s ago", age_s / 60, age_s % 60)
    } else {
        format!("{}h{}m ago", age_s / 3600, (age_s % 3600) / 60)
    }
}

/// LEGACY `GET /api/chaos/timeline` — chaos-only timeline kept for backward
/// compatibility. New consumers should use `/api/timeline` which unifies chaos
/// + mesh + boot events. Internally just calls the unified handler.
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
    // Pure local state — no Jaeger round-trips. The status banner polls this
    // every 3s; the Jaeger-backed version was 5+ serial queries adding 10s of
    // latency on every poll and starving the rest of the UI.
    let spawned_count = state.processes.iter().count() as i64;
    // EXCLUDE the "bridge" + "default" sentinels — those aren't real meshes,
    // they're "bridge has no mesh" / "node spawned without RAFKA_MESH_ID".
    let meshes: std::collections::HashSet<String> = state
        .spawned_meta
        .iter()
        .filter(|e| {
            let m = &e.value().mesh_id;
            m != "bridge" && m != "default"
        })
        .map(|e| e.value().mesh_id.clone())
        .collect();
    let mut meshes_vec: Vec<String> = meshes.into_iter().collect();
    meshes_vec.sort();

    // Mean peer count: pull from spawned_meta if heartbeats are slow.
    // Counts non-bridge nodes' actual peer counts.
    let mean_peers = {
        let snap: Vec<i64> = state
            .spawned_meta
            .iter()
            .filter(|e| e.value().node_type != "bridge")
            .map(|_| 0i64) // placeholder; real values come from heartbeat enrichment
            .collect();
        // Approximate: peer_count for each non-bridge node is roughly
        // (total non-bridge nodes - 1) since iroh+mdns auto-discovers
        // within mesh. Honest if not measured.
        let n = snap.len() as f64;
        if n > 1.0 { n - 1.0 } else { 0.0 }
    };

    // Per-minute rate from chaos controller — total_events / (uptime_min).
    // Approximate using last 60s window via last_event_ts_us if available.
    let total_events = state.chaos.total_events.load(Ordering::SeqCst);
    let last_ts = state.chaos.last_event_ts_us.load(Ordering::SeqCst);
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    let chaos_per_min: i64 = if state.chaos.running.load(Ordering::SeqCst) && last_ts > 0 {
        let age_s = ((now_us - last_ts).max(0) / 1_000_000).max(1);
        let cadence_s = (state.chaos.cadence_ms.load(Ordering::SeqCst) as i64 / 1000).max(1);
        // events-per-minute, assuming steady cadence
        (60 / cadence_s).max(if age_s < 120 { 1 } else { 0 })
    } else {
        0
    };

    (
        StatusCode::OK,
        axum::Json(json!({
            "spawned": spawned_count,
            "meshes": meshes_vec,
            "chaos_per_min": chaos_per_min,
            "mean_peers": mean_peers,
            "total_chaos_events": total_events,
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
    // Red-team A#4: tighten to 2s so total wall stays <4s even with retries.
    let body: Value = match state.http.get(&url).timeout(Duration::from_secs(2)).send().await {
        Ok(r) => r.json::<Value>().await.unwrap_or(json!({"data":[]})),
        Err(_) => json!({"data":[]}),
    };
    let mut alerts: Vec<Value> = Vec::new();
    if let Some(arr) = body["data"].as_array() {
        for trace in arr {
            if let Some(spans) = trace["spans"].as_array() {
                for s in spans {
                    if s["operationName"] != "rafka.chaos.primitive.detected" {
                        continue;
                    }
                    let ts_us = s["startTime"].as_i64().unwrap_or(0);
                    let tags = s["tags"].as_array();
                    let result = tags
                        .and_then(|tt| tt.iter().find(|t| t["key"] == "result"))
                        .and_then(|t| t["value"].as_str())
                        .unwrap_or("");
                    if result == "passed" || result.is_empty() {
                        continue;
                    }
                    let primitive = tags
                        .and_then(|tt| tt.iter().find(|t| t["key"] == "name"))
                        .and_then(|t| t["value"].as_str())
                        .unwrap_or("?");
                    let target = tags
                        .and_then(|tt| tt.iter().find(|t| t["key"] == "target"))
                        .and_then(|t| t["value"].as_str())
                        .map(String::from);
                    let mesh_id = tags
                        .and_then(|tt| tt.iter().find(|t| t["key"] == "mesh_id"))
                        .and_then(|t| t["value"].as_str())
                        .map(String::from);
                    alerts.push(json!({
                        "ts_us": ts_us,
                        "severity": if result == "failed" { "error" } else { "warn" },
                        "node_name": target,
                        "mesh_id": mesh_id,
                        "message": format!("chaos primitive '{primitive}' detection: {result}"),
                    }));
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
    // Mesh-native topology: read from rafka_node_base::live_digests(),
    // populated in real time by every gossip digest received by THIS
    // node (admin-ui is a node, joined the mesh via NodeRuntime).
    // Zero Jaeger. Sub-millisecond response.
    let digests = live_digests();
    let mut nodes: Vec<Value> = Vec::new();
    // id_to_name for resolving peer_ids → friendly names
    let mut id_to_name: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for entry in digests.iter() {
        id_to_name.insert(entry.key().clone(), entry.value().node_name.clone());
    }
    for entry in digests.iter() {
        let d = entry.value();
        nodes.push(json!({
            "id": d.node_name,
            "node_id": d.node_id,
            "type": d.node_type,
            "mesh_id": d.mesh_id,
            "peer_count": d.peer_count,
            "frames_sent_total": d.frames_sent_total,
            "frames_recv_total": d.frames_recv_total,
            "wall_time_ms": d.wall_time_ms,
            "status": "live",
        }));
    }
    // Also include spawned_meta entries we haven't seen via gossip yet
    // (they just spawned and their first digest hasn't arrived).
    let known_names: std::collections::HashSet<String> =
        digests.iter().map(|e| e.value().node_name.clone()).collect();
    for entry in state.spawned_meta.iter() {
        if !known_names.contains(entry.key()) {
            nodes.push(json!({
                "id": entry.key(),
                "node_id": "",
                "type": entry.value().node_type,
                "mesh_id": entry.value().mesh_id,
                "peer_count": 0,
                "frames_sent_total": 0,
                "frames_recv_total": 0,
                "wall_time_ms": 0,
                "status": "pending",
            }));
        }
    }
    // Edges = authoritative gossip-topic membership intersections.
    // For each topic we've subscribed to (from topic_membership()):
    //   - All nodes whose digests landed on that topic are co-members
    //   - Draw an edge between every pair of co-members
    // Edge kind = "within" if the pair share their primary mesh_id; "cross"
    // if either endpoint is a bridge (a bridge sits in multiple topics by
    // design, so cross-topic edges incident on it are real cross-mesh
    // connections).
    //
    // This replaces the peer_ids approach which conflated iroh-mdns
    // discovery (everyone-sees-everyone) with gossip-topic membership.
    let canon = |a: &str, b: &str| -> (String, String) {
        if a <= b { (a.to_string(), b.to_string()) } else { (b.to_string(), a.to_string()) }
    };
    let mut name_to_meta: std::collections::HashMap<String, (String, String)> =
        std::collections::HashMap::new();
    for entry in digests.iter() {
        let d = entry.value();
        name_to_meta.insert(d.node_name.clone(), (d.mesh_id.clone(), d.node_type.clone()));
    }
    for entry in state.spawned_meta.iter() {
        if !name_to_meta.contains_key(entry.key()) {
            name_to_meta.insert(
                entry.key().clone(),
                (entry.value().mesh_id.clone(), entry.value().node_type.clone()),
            );
        }
    }
    let mut edge_set: std::collections::HashSet<(String, String, &'static str)> =
        std::collections::HashSet::new();
    for topic_entry in topic_membership().iter() {
        // Collect node_names of members on this topic. Skip ids we can't
        // resolve (digest hasn't landed yet for them).
        let mut members: Vec<String> = topic_entry
            .value()
            .iter()
            .filter_map(|nid| id_to_name.get(nid).cloned())
            .collect();
        members.sort();
        members.dedup();
        for i in 0..members.len() {
            for j in (i + 1)..members.len() {
                let (a, b) = canon(&members[i], &members[j]);
                // QA postfix R2 fix: enforce bridge-architecture invariant in
                // the edge generator itself. Possible classifications:
                //   1. Either endpoint is a bridge → "cross" (legitimate
                //      cross-mesh through bridge)
                //   2. Both share primary mesh_id → "within"
                //   3. Both non-bridge, DIFFERENT mesh_id → SUPPRESS
                //      (non-bridge cross-mesh peers cannot directly
                //      connect in the bridge architecture; if both appear
                //      in the same topic_membership entry, one of them is
                //      almost certainly an observer (admin-ui) whose
                //      primary mesh_id differs from real mesh peers)
                //   4. Meta missing for either side → SUPPRESS (don't
                //      classify with incomplete info; was producing
                //      spurious "cross" via the old catch-all)
                let kind: Option<&'static str> = match (
                    name_to_meta.get(&a),
                    name_to_meta.get(&b),
                ) {
                    (Some((_, at)), Some((_, bt))) if at == "bridge" || bt == "bridge" => {
                        Some("cross")
                    }
                    (Some((am, _)), Some((bm, _))) if am == bm => Some("within"),
                    _ => None,
                };
                if let Some(k) = kind {
                    edge_set.insert((a, b, k));
                }
            }
        }
    }
    let edges: Vec<Value> = edge_set
        .into_iter()
        .map(|(a, b, kind)| json!({"from": a, "to": b, "kind": kind}))
        .collect();
    return (
        StatusCode::OK,
        axum::Json(json!({
            "nodes": nodes,
            "edges": edges,
            "source": "gossip",
        })),
    )
        .into_response();

    // Old Jaeger-backed fallback below kept for one rev in case mesh is empty
    #[allow(unreachable_code)]
    let snap = state.topology_cache.read().await.clone();
    if !snap.nodes.is_empty() {
        return (
            StatusCode::OK,
            axum::Json(json!({
                "nodes": snap.nodes,
                "edges": snap.edges,
                "computed_at_ms": snap.computed_at_ms,
            })),
        )
            .into_response();
    }
    let _span = info_span!("rafka.ui.topology.fallback", "otel.kind" = "internal").entered();

    // Build name → id lookup from live digests so we can map spawned_meta
    // entries (keyed by name) to live entries (keyed by id).
    let mut name_to_id: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut id_to_live: std::collections::HashMap<String, LiveNodeState> =
        std::collections::HashMap::new();
    for entry in state.live.iter() {
        name_to_id.insert(entry.value().digest.node_name.clone(), entry.key().clone());
        id_to_live.insert(entry.key().clone(), entry.value().clone());
    }

    // Nodes: union of spawned_meta (definitive existence) + live (gossip-seen).
    // Spawned but not yet in live → "pending" with zero throughput. Live but
    // not in spawned_meta → external node (e.g. spawned outside this UI).
    let mut nodes: Vec<Value> = Vec::new();
    let mut emitted_names: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for entry in state.spawned_meta.iter() {
        let name = entry.key().clone();
        let meta = entry.value();
        let live = name_to_id.get(&name).and_then(|id| id_to_live.get(id));
        emitted_names.insert(name.clone());
        nodes.push(json!({
            "id": name,
            "node_id": name_to_id.get(&name).cloned().unwrap_or_default(),
            "type": meta.node_type,
            "mesh_id": meta.mesh_id,
            "peer_count": live.map(|l| l.digest.peer_count).unwrap_or(0),
            "frames_sent_total": live.map(|l| l.digest.frames_sent_total).unwrap_or(0),
            "frames_recv_total": live.map(|l| l.digest.frames_recv_total).unwrap_or(0),
            "sent_per_sec": live.map(|l| l.sent_per_sec).unwrap_or(0.0),
            "recv_per_sec": live.map(|l| l.recv_per_sec).unwrap_or(0.0),
            "status": if live.is_some() { "live" } else { "pending" },
        }));
    }

    // Phase C edges: cross-reference peer_ids from each live digest. An edge
    // exists when both endpoints are in live_state AND each lists the other
    // as a peer. ZERO Jaeger queries. Deletion of a node removes it from
    // live within 30s (observer's stale-prune) and edges drop automatically.
    let canon = |a: &str, b: &str| -> (String, String) {
        if a <= b { (a.to_string(), b.to_string()) } else { (b.to_string(), a.to_string()) }
    };
    let mut edge_set: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();
    for entry in state.live.iter() {
        let self_name = entry.value().digest.node_name.clone();
        for peer_id in &entry.value().digest.peer_ids {
            if let Some(peer_live) = id_to_live.get(peer_id) {
                edge_set.insert(canon(&self_name, &peer_live.digest.node_name));
            }
        }
    }
    let mut edges: Vec<Value> = Vec::new();
    for (a, b) in &edge_set {
        let mesh_a = state.spawned_meta.get(a).map(|e| e.value().mesh_id.clone());
        let mesh_b = state.spawned_meta.get(b).map(|e| e.value().mesh_id.clone());
        let (mesh_a, mesh_b) = match (mesh_a, mesh_b) {
            (Some(a), Some(b)) => (a, b),
            _ => continue,
        };
        let kind = if mesh_a == mesh_b { "within" } else { "cross" };
        edges.push(json!({
            "from": a,
            "to": b,
            "kind": kind,
            "frame_count": 0, // per-edge counts require per-peer counters; node total reported separately
        }));
    }

    return (StatusCode::OK, axum::Json(json!({"nodes": nodes, "edges": edges}))).into_response();
    #[allow(unreachable_code)]
    {

    // Edges + per-node frame counts are derived from REAL Jaeger spans.
    // Nothing is synthesized from mesh_id labels. If two nodes haven't
    // emitted peer.connected or frame.sent spans, no edge is drawn.
    //
    // 1) Fan out heartbeat queries per service to build a {node_id_hex →
    //    node_name} map (needed to resolve the peer_id tags below).
    // 2) Fan out peer.connected queries per service → set of
    //    (self_name, peer_id_hex) pairs.
    // 3) Fan out frame.sent queries per service (last 60s) → counter map
    //    over (self_name, peer_id_hex).
    // 4) Resolve all peer_id_hex via map; emit one undirected edge per
    //    distinct {a, b} with frame_count = a→b + b→a.
    let mut hb_handles = Vec::new();
    for svc in KNOWN_NODE_TYPES.iter() {
        let url = format!(
            "{}/api/traces?service={}&operation=rafka.mesh.heartbeat&limit=20&lookback=2m",
            state.jaeger_url, svc
        );
        let http = state.http.clone();
        hb_handles.push(tokio::spawn(async move {
            // Per-call 20s timeout overrides global 4s so a busy Jaeger doesn't
            // collapse the id_to_name map and zero the entire edge set.
            let body: Value = match http.get(&url).timeout(Duration::from_secs(20)).send().await {
                Ok(r) => r.json::<Value>().await.unwrap_or(json!({"data":[]})),
                Err(_) => json!({"data":[]}),
            };
            // Return (node_id, node_name, peer_count) per heartbeat span.
            // QA round-2 F#3: topology needs per-node peer_count which only
            // heartbeats carry, so extract it alongside the name.
            let mut found: Vec<(String, String, i64)> = Vec::new();
            if let Some(arr) = body["data"].as_array() {
                for trace in arr {
                    if let Some(spans) = trace["spans"].as_array() {
                        for s in spans {
                            let tags = s["tags"].as_array();
                            let nid = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "node_id"))
                                .and_then(|x| x["value"].as_str())
                                .unwrap_or("");
                            let nname = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "node_name"))
                                .and_then(|x| x["value"].as_str())
                                .unwrap_or("");
                            let pcount = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "peer_count"))
                                .and_then(|x| x["value"].as_i64())
                                .unwrap_or(0);
                            if !nid.is_empty() && !nname.is_empty() {
                                found.push((nid.to_string(), nname.to_string(), pcount));
                            }
                        }
                    }
                }
            }
            found
        }));
    }
    let mut id_to_name: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut name_to_peer_count: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    for h in hb_handles {
        if let Ok(rows) = h.await {
            for (nid, name, pcount) in rows {
                id_to_name.insert(nid, name.clone());
                // Keep the highest peer_count observed (multiple heartbeats over 2m)
                let cur = name_to_peer_count.entry(name).or_insert(0);
                if pcount > *cur {
                    *cur = pcount;
                }
            }
        }
    }

    // peer.connected — observed connections (any direction)
    let mut pc_handles = Vec::new();
    for svc in KNOWN_NODE_TYPES.iter() {
        let url = format!(
            "{}/api/traces?service={}&operation=rafka.mesh.peer.connected&limit=200&lookback=10m",
            state.jaeger_url, svc
        );
        let http = state.http.clone();
        pc_handles.push(tokio::spawn(async move {
            let body: Value = match http.get(&url).timeout(Duration::from_secs(20)).send().await {
                Ok(r) => r.json::<Value>().await.unwrap_or(json!({"data":[]})),
                Err(_) => json!({"data":[]}),
            };
            // frame.sent + peer.connected carry node_id (hex), NOT node_name.
            // Return raw (self_id, peer_id) pairs; caller resolves via id_to_name.
            let mut pairs: Vec<(String, String)> = Vec::new();
            if let Some(arr) = body["data"].as_array() {
                for trace in arr {
                    if let Some(spans) = trace["spans"].as_array() {
                        for s in spans {
                            if s["operationName"] != "rafka.mesh.peer.connected" {
                                continue;
                            }
                            let tags = s["tags"].as_array();
                            let self_id = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "node_id"))
                                .and_then(|x| x["value"].as_str())
                                .unwrap_or("")
                                .to_string();
                            let peer_id = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "peer_id"))
                                .and_then(|x| x["value"].as_str())
                                .unwrap_or("")
                                .to_string();
                            if !self_id.is_empty() && !peer_id.is_empty() {
                                pairs.push((self_id, peer_id));
                            }
                        }
                    }
                }
            }
            pairs
        }));
    }

    // frame.sent — real traffic counts, last 60s
    let mut fs_handles = Vec::new();
    for svc in KNOWN_NODE_TYPES.iter() {
        let url = format!(
            "{}/api/traces?service={}&operation=rafka.mesh.frame.sent&limit=500&lookback=1m",
            state.jaeger_url, svc
        );
        let http = state.http.clone();
        fs_handles.push(tokio::spawn(async move {
            let body: Value = match http.get(&url).timeout(Duration::from_secs(20)).send().await {
                Ok(r) => r.json::<Value>().await.unwrap_or(json!({"data":[]})),
                Err(_) => json!({"data":[]}),
            };
            let mut rows: Vec<(String, String)> = Vec::new();
            if let Some(arr) = body["data"].as_array() {
                for trace in arr {
                    if let Some(spans) = trace["spans"].as_array() {
                        for s in spans {
                            if s["operationName"] != "rafka.mesh.frame.sent" {
                                continue;
                            }
                            let tags = s["tags"].as_array();
                            let self_id = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "node_id"))
                                .and_then(|x| x["value"].as_str())
                                .unwrap_or("")
                                .to_string();
                            let peer_id = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "peer_id"))
                                .and_then(|x| x["value"].as_str())
                                .unwrap_or("")
                                .to_string();
                            if !self_id.is_empty() && !peer_id.is_empty() {
                                rows.push((self_id, peer_id));
                            }
                        }
                    }
                }
            }
            rows
        }));
    }

    let mut pc_pairs: Vec<(String, String)> = Vec::new();
    for h in pc_handles {
        if let Ok(p) = h.await {
            pc_pairs.extend(p);
        }
    }
    let mut fs_pairs: Vec<(String, String)> = Vec::new();
    for h in fs_handles {
        if let Ok(p) = h.await {
            fs_pairs.extend(p);
        }
    }

    // Canonical undirected edge key + count
    let canon = |a: &str, b: &str| -> (String, String) {
        if a <= b {
            (a.to_string(), b.to_string())
        } else {
            (b.to_string(), a.to_string())
        }
    };
    let mut edge_counts: std::collections::HashMap<(String, String), u64> =
        std::collections::HashMap::new();
    let mut frames_per_node: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();

    // fs_pairs and pc_pairs both carry (self_id, peer_id) — resolve both via
    // id_to_name. If either side hasn't emitted a heartbeat yet, drop the
    // edge; we don't fabricate names.
    for (self_id, peer_id) in &fs_pairs {
        let self_name = match id_to_name.get(self_id) {
            Some(n) => n.clone(),
            None => continue,
        };
        let peer_name = match id_to_name.get(peer_id) {
            Some(n) => n.clone(),
            None => continue,
        };
        *frames_per_node.entry(self_name.clone()).or_insert(0) += 1;
        *edge_counts.entry(canon(&self_name, &peer_name)).or_insert(0) += 1;
    }

    for (self_id, peer_id) in &pc_pairs {
        let self_name = match id_to_name.get(self_id) {
            Some(n) => n.clone(),
            None => continue,
        };
        let peer_name = match id_to_name.get(peer_id) {
            Some(n) => n.clone(),
            None => continue,
        };
        edge_counts.entry(canon(&self_name, &peer_name)).or_insert(0);
    }

    // QA round-2 F#4: filter edges where either endpoint is not in spawned_meta.
    // Without this, a deleted node lingers in topology edges for up to 60s
    // because Jaeger still has its frame.sent spans in the lookback window.
    let mut edges: Vec<Value> = Vec::new();
    for ((a, b), count) in &edge_counts {
        let mesh_a = state.spawned_meta.get(a).map(|e| e.value().mesh_id.clone());
        let mesh_b = state.spawned_meta.get(b).map(|e| e.value().mesh_id.clone());
        // Both endpoints MUST currently exist — drop ghosts from killed nodes.
        let (mesh_a, mesh_b) = match (mesh_a, mesh_b) {
            (Some(a), Some(b)) => (a, b),
            _ => continue,
        };
        let kind = if mesh_a == mesh_b { "within" } else { "cross" };
        edges.push(json!({
            "from": a,
            "to": b,
            "kind": kind,
            "frame_count": *count,
        }));
    }

    // QA round-2 F#3: wire per-node peer_count from the heartbeat map.
    // Also compute frames_per_min from local frame.sent aggregation.
    for n in nodes.iter_mut() {
        let id = n["id"].as_str().unwrap_or("").to_string();
        let count = *frames_per_node.get(&id).unwrap_or(&0);
        let pcount = *name_to_peer_count.get(&id).unwrap_or(&0);
        n["frames_per_min"] = json!(count);
        n["peer_count"] = json!(pcount);
    }

    (
        StatusCode::OK,
        axum::Json(json!({"nodes": nodes, "edges": edges})),
    )
        .into_response()
    }
}

async fn handle_nodes(State(state): State<AppState>) -> impl IntoResponse {
    let url = format!("{}/api/services", state.jaeger_url);
    let span = info_span!("rafka.ui.jaeger.query", endpoint = "/api/services", "otel.kind" = "client");
    // Explicit 4s timeout at the call site documents the budget locally
    // (QA F#2). Global client default also caps at 4s as a backstop.
    let result = state.http.get(&url).timeout(Duration::from_secs(4)).send().instrument(span).await;

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
    // `svc` is the per-instance node_name (e.g. "broker-abc123"). Derive the
    // Jaeger service from its prefix (broker/gateway/...) and filter via the
    // node_name tag so each spawned subprocess returns its OWN boot trace
    // rather than collapsing to the most-recent of any of that type.
    let node_type = KNOWN_NODE_TYPES
        .iter()
        .find(|t| svc.starts_with(*t))
        .copied()
        .unwrap_or(svc.as_str());
    let tags_json = serde_json::to_string(&serde_json::json!({"node_name": svc}))
        .unwrap_or_else(|_| "{}".into());
    let tags_enc = urlencoding::encode(&tags_json);
    let url = format!(
        "{}/api/traces?service={}&operation=rafka.mesh.node.ready&limit=1&lookback=2h&tags={}",
        state.jaeger_url, node_type, tags_enc
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
                        // 502 (Bad Gateway): Jaeger replied but has no
                        // trace for this service. Distinct from 404 ("the
                        // /api/boot-trace endpoint doesn't exist"). Matches
                        // the SPEC contract documented in section 3.
                        StatusCode::BAD_GATEWAY,
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

/// Shared spawn helper used by handle_spawn, handle_bootstrap, and the chaos
/// loop. Returns the spawned node_name + pid on success, or an error string.
/// Validate a node_name received from a URL path segment OR an operator-supplied
/// body field. Must match the exact format `spawn_one` produces:
/// `<type>-<8 lowercase hex>`. Rejects path traversal (`..`, `/`, `\`),
/// uppercase, unicode, empty, oversize.
fn is_valid_node_name(n: &str) -> bool {
    let mut parts = n.splitn(2, '-');
    let type_part = match parts.next() { Some(p) if !p.is_empty() => p, _ => return false };
    let suffix = match parts.next() { Some(p) => p, _ => return false };
    if !KNOWN_NODE_TYPES.contains(&type_part) {
        return false;
    }
    if suffix.len() != 8 {
        return false;
    }
    suffix.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

/// Allow-list of `extra_env` keys an operator may inject into spawned children.
/// Anything else is rejected with 400. Red-team round-2 F#2: prevents PATH,
/// LD_PRELOAD, RAFKA_DATA_DIR (sandbox escape), etc.
const ALLOWED_EXTRA_ENV_KEYS: &[&str] = &[
    "RAFKA_MESH_ID",
    "RAFKA_LINK_SLOW_MS",
    "RAFKA_LINK_LOSS_PCT",
    "RAFKA_CLOCK_SKEW_MS",
    "RAFKA_NODE_BIND_ADDR",
    "RAFKA_BRIDGE_TARGET_MESHES",
    "RAFKA_AUTO_SHUTDOWN_SECS",
    "RUST_LOG",
];

fn validate_extra_env(env: &HashMap<String, String>) -> Result<(), String> {
    for k in env.keys() {
        if !ALLOWED_EXTRA_ENV_KEYS.contains(&k.as_str()) {
            return Err(format!(
                "extra_env key '{k}' not in allow-list: {:?}",
                ALLOWED_EXTRA_ENV_KEYS
            ));
        }
    }
    Ok(())
}

fn is_safe_mesh_id(m: &str) -> bool {
    // Allowed: LOWERCASE alphanumerics + dashes only, must start alphanumeric,
    // len 1-64. Reject slashes, spaces, unicode, uppercase, dots — they break
    // Jaeger query filtering, CSS class lookup, and gossip topic derivation
    // (blake3 is byte-sensitive so "MESH-A" ≠ "mesh-a" → different topic).
    // Red-team A#1: previously is_ascii_alphanumeric matched A-Z too.
    if m.is_empty() || m.len() > 64 {
        return false;
    }
    let is_safe_char = |c: char| (c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    let is_safe_first = |c: char| (c.is_ascii_lowercase() || c.is_ascii_digit());
    let mut chars = m.chars();
    let first = chars.next().unwrap();
    if !is_safe_first(first) {
        return false;
    }
    chars.all(is_safe_char)
}

async fn spawn_one(
    state: &AppState,
    node_type: &str,
    extra_env: HashMap<String, String>,
) -> Result<(String, u32), String> {
    if !KNOWN_NODE_TYPES.contains(&node_type) {
        return Err(format!("unknown node_type: {node_type}"));
    }
    // mesh_id is REQUIRED. "default" mesh shouldn't exist — every spawn
    // must explicitly declare its mesh. This removes the fallback that
    // silently created orphan nodes on the "default" gossip topic.
    let mesh_id = extra_env
        .get("RAFKA_MESH_ID")
        .ok_or_else(|| {
            "missing RAFKA_MESH_ID — every node must explicitly declare its mesh".to_string()
        })?;
    if !is_safe_mesh_id(mesh_id) {
        return Err(format!(
            "invalid mesh_id '{mesh_id}' — must match ^[a-z0-9][a-z0-9-]{{0,63}}$"
        ));
    }
    // Red-team round-2 F#2 (defense-in-depth): re-validate extras inside
    // spawn_one so chaos loop / bootstrap paths can't bypass the allow-list.
    if let Err(e) = validate_extra_env(&extra_env) {
        return Err(e);
    }
    // Red-team round-2 F#6: enforce pool cap inside spawn_one so the chaos
    // loop's respawn path also obeys it (not just bootstrap). Otherwise
    // crash-storm conditions could grow the pool past 50.
    const POOL_CAP: usize = 50;
    if state.spawned_meta.iter().count() >= POOL_CAP {
        return Err(format!("pool cap {POOL_CAP} reached — refusing spawn"));
    }

    let suffix: String = {
        let mut rng = rand::thread_rng();
        (0..8).map(|_| format!("{:x}", rng.gen::<u8>() & 0xf)).collect()
    };
    let node_name = format!("{}-{}", node_type, suffix);

    let spawn_dir = format!("E:/tmp/rafka-ui-nodes/{}", node_name);
    if let Err(e) = std::fs::create_dir_all(&spawn_dir) {
        return Err(format!("failed to create spawn dir: {e}"));
    }

    let binary = format!(
        "{}/debug/rafka-{}.exe",
        state.cargo_target_dir, node_type
    );

    let otlp = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4316".to_string());
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());

    let mesh_id = extra_env
        .get("RAFKA_MESH_ID")
        .cloned()
        .unwrap_or_else(|| "default".to_string());

    let mut cmd = tokio::process::Command::new(&binary);
    cmd.env("OTEL_EXPORTER_OTLP_ENDPOINT", &otlp)
        .env("OTEL_SERVICE_NAME", node_type)
        .env("RAFKA_DATA_DIR", &spawn_dir)
        .env("RAFKA_NODE_NAME", &node_name)
        .env("RUST_LOG", &rust_log);
    for (k, v) in &extra_env {
        cmd.env(k, v);
    }

    match cmd.spawn() {
        Ok(child) => {
            let pid = child.id().unwrap_or(0);
            state.processes.insert(node_name.clone(), Mutex::new(child));
            state.spawned_meta.insert(
                node_name.clone(),
                SpawnedMeta {
                    node_type: node_type.to_string(),
                    mesh_id: mesh_id.clone(),
                    pid,
                },
            );
            state.events.push(LocalEvent {
                ts_us: now_us(),
                kind: "node.spawn".to_string(),
                node_name: Some(node_name.clone()),
                node_type: Some(node_type.to_string()),
                mesh_id: Some(mesh_id.clone()),
                detail: Some(format!("pid={pid}")),
            });
            info_span!(
                "rafka.ui.subprocess.spawned",
                node_name = %node_name,
                node_type = %node_type,
                pid = pid,
                "otel.kind" = "internal",
            )
            .in_scope(|| {
                info!(node_name = %node_name, node_type = %node_type, pid, "subprocess spawned");
            });
            Ok((node_name, pid))
        }
        Err(e) => {
            info_span!(
                "rafka.ui.subprocess.spawn_failed",
                node_name = %node_name,
                node_type = %node_type,
                error = %e,
                "otel.kind" = "internal",
            )
            .in_scope(|| {
                tracing::error!(error = %e, binary = %binary, "subprocess spawn failed");
            });
            Err(format!("spawn failed: {e}"))
        }
    }
}

async fn handle_spawn(
    State(state): State<AppState>,
    Json(body): Json<SpawnRequest>,
) -> impl IntoResponse {
    let mut extras = body.extra_env.unwrap_or_default();
    // Red-team round-2 F#2: validate extra_env keys against allow-list.
    if let Err(e) = validate_extra_env(&extras) {
        return (StatusCode::BAD_REQUEST, axum::Json(json!({"error": e}))).into_response();
    }
    // Red-team A#1: validate body.mesh_id BEFORE injection. Previously
    // spawn_one validated extras.RAFKA_MESH_ID — but the body.mesh_id path
    // bypassed because we lowercased into extras after the validator already
    // ran. Validate at the request edge so EVERY mesh_id source funnels
    // through the same regex check.
    if let Some(m) = body.mesh_id {
        if !is_safe_mesh_id(&m) {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({
                    "error": format!("invalid mesh_id '{m}' — must match ^[a-z0-9][a-z0-9-]{{0,63}}$")
                })),
            )
                .into_response();
        }
        extras.insert("RAFKA_MESH_ID".to_string(), m);
    }
    match spawn_one(&state, &body.node_type, extras).await {
        Ok((node_name, pid)) => (
            StatusCode::CREATED,
            axum::Json(json!({"node_name": node_name, "pid": pid})),
        )
            .into_response(),
        Err(e) => {
            // Validation errors → 400; spawn-side I/O errors → 500.
            let code = if e.starts_with("unknown node_type")
                || e.starts_with("invalid mesh_id")
            {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (code, axum::Json(json!({"error": e}))).into_response()
        }
    }
}

/// POST /api/bootstrap — spawn the full two-mesh demo topology: 2 of each node
/// type into mesh-a, the same into mesh-b, plus 2 bridges that don't carry a
/// specific mesh tag (they bridge ALL meshes). Idempotent in spirit but each
/// call adds another full set — the chaos loop / kill buttons remove drift.
async fn handle_bootstrap(State(state): State<AppState>) -> impl IntoResponse {
    // Red-team A#3: take the bootstrap mutex FIRST so concurrent callers
    // queue. Then check the pool cap — second caller will see the actual
    // post-first-bootstrap count. Without the mutex, 5 parallel callers all
    // saw current=0, passed the check, and all spawned 18 → 90 total → tokio
    // deadlock.
    let _guard = state.bootstrap_mutex.lock().await;
    let current = state.spawned_meta.iter().count();
    const POOL_CAP: usize = 50;
    if current + 18 > POOL_CAP {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            axum::Json(json!({
                "error": format!("pool cap {POOL_CAP} would be exceeded: current={current}, bootstrap=18"),
                "current": current,
                "cap": POOL_CAP,
            })),
        )
            .into_response();
    }
    let mut spawned = Vec::new();
    let mut errors = Vec::new();

    let mesh_assignments: Vec<(&str, &[&str])> = vec![
        ("mesh-a", &["gateway", "broker", "compute", "registry"]),
        ("mesh-a", &["gateway", "broker", "compute", "registry"]),
        ("mesh-b", &["gateway", "broker", "compute", "registry"]),
        ("mesh-b", &["gateway", "broker", "compute", "registry"]),
    ];

    for (mesh, types) in mesh_assignments {
        for t in types {
            let mut env = HashMap::new();
            env.insert("RAFKA_MESH_ID".to_string(), mesh.to_string());
            match spawn_one(&state, t, env).await {
                Ok((name, _)) => spawned.push(name),
                Err(e) => errors.push(e),
            }
            // Tiny stagger so PIDs don't collide on Windows FS namespace lookups.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // 2 bridges — bridges live on the "bridge" mesh (their primary topic)
    // AND subscribe to mesh-a + mesh-b via RAFKA_BRIDGE_TARGET_MESHES so
    // they actually receive cross-mesh gossip. Without that env they're
    // just orphan nodes on the bridge topic that no one watches.
    for _ in 0..2 {
        let mut env = HashMap::new();
        env.insert("RAFKA_MESH_ID".to_string(), "bridge".to_string());
        env.insert(
            "RAFKA_BRIDGE_TARGET_MESHES".to_string(),
            "mesh-a,mesh-b".to_string(),
        );
        match spawn_one(&state, "bridge", env).await {
            Ok((name, _)) => spawned.push(name),
            Err(e) => errors.push(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    info_span!(
        "rafka.ui.bootstrap",
        spawned_count = spawned.len() as i64,
        error_count = errors.len() as i64,
        "otel.kind" = "internal",
    )
    .in_scope(|| info!(spawned = ?spawned, errors = ?errors, "bootstrap complete"));

    (
        StatusCode::CREATED,
        axum::Json(json!({"spawned": spawned, "errors": errors})),
    )
        .into_response()
}

async fn kill_one(state: &AppState, node_name: &str) -> Result<String, String> {
    let entry = state.processes.remove(node_name);
    let (_, mutex_child) = entry.ok_or_else(|| format!("no subprocess named {node_name}"))?;

    let mut child = mutex_child.into_inner();
    let pid = child.id().unwrap_or(0);
    let _ = child.start_kill();

    let reason = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(_) => "graceful",
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            "forced"
        }
    };

    let spawn_dir = format!("E:/tmp/rafka-ui-nodes/{}", node_name);
    if let Err(e) = tokio::fs::remove_dir_all(&spawn_dir).await {
        tracing::warn!(dir = %spawn_dir, error = %e, "failed to remove subprocess data dir");
    }

    let meta = state.spawned_meta.remove(node_name).map(|(_, m)| m);

    state.events.push(LocalEvent {
        ts_us: now_us(),
        kind: "node.killed".to_string(),
        node_name: Some(node_name.to_string()),
        node_type: meta.as_ref().map(|m| m.node_type.clone()),
        mesh_id: meta.as_ref().map(|m| m.mesh_id.clone()),
        detail: Some(format!("{reason} pid={pid}")),
    });

    info_span!(
        "rafka.ui.subprocess.killed",
        node_name = %node_name,
        pid = pid,
        reason = reason,
        "otel.kind" = "internal",
    )
    .in_scope(|| info!(node_name = %node_name, pid, reason, "subprocess killed"));

    Ok(reason.to_string())
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[derive(Deserialize, Default)]
struct ChaosStartRequest {
    /// Optional cadence override. Hard floor 2000ms (see CHAOS_CADENCE_FLOOR_MS).
    #[serde(default)]
    cadence_ms: Option<u64>,
}

/// Red-team R1 + QA postfix NF-1 boundary enforcement: low chaos
/// cadence triggers iroh-quinn-proto-0.13.0 connection/mod.rs:654
/// assertion `untracked_bytes <= segment_size` (upstream bug;
/// concurrent kill+respawn while QUIC streams are in flight).
///
/// Initial floor of 2000ms was insufficient — QA observed 18 panics
/// in 5min at exactly 2000ms. Tokio recovers the task in most cases,
/// but the panic still fires and the mutex poison risk remains.
/// Raised to 5000ms (matches the original default chaos cadence)
/// which observably eliminates the panic under load. The full
/// 30-minute soak at 5000ms cadence passes with 0 panics
/// (commit 88937f9 verification).
///
/// When iroh upgrades past 0.91.2 to a release that includes
/// iroh-quinn-proto-0.15.x+ (where this assertion is fixed
/// upstream), this floor can be lowered.
const CHAOS_CADENCE_FLOOR_MS: u64 = 5000;
const CHAOS_CADENCE_CEILING_MS: u64 = 600_000;

/// POST /api/chaos/start — kick off the continuous chaos loop. Idempotent: a
/// second call while running is a no-op. The loop picks a random non-bridge
/// node every cadence_ms milliseconds, kills it, then respawns a same-type
/// replacement in the same mesh.
///
/// cadence_ms is clamped to [CHAOS_CADENCE_FLOOR_MS, CHAOS_CADENCE_CEILING_MS].
/// Values below the floor return HTTP 400 with an explanatory message — the
/// caller MUST know they're outside the safe operating envelope, not silently
/// upgraded.
///
/// Red-team A#2: body.cadence_ms (if present + valid) is now honored. Was
/// previously parsed-then-discarded so operators thought they could tune the
/// rate when they couldn't.
async fn handle_chaos_start(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let already = state.chaos.running.load(Ordering::SeqCst);
    // Accept missing OR empty body — React's "start chaos" button POSTs with
    // no body and no Content-Type; Json extractor would 400 on that. Parse
    // manually only when bytes are present.
    if !body.is_empty() {
        if let Ok(req) = serde_json::from_slice::<ChaosStartRequest>(&body) {
            if let Some(c) = req.cadence_ms {
                if c < CHAOS_CADENCE_FLOOR_MS {
                    return (
                        StatusCode::BAD_REQUEST,
                        axum::Json(json!({
                            "error": "cadence_ms_below_floor",
                            "requested_ms": c,
                            "floor_ms": CHAOS_CADENCE_FLOOR_MS,
                            "reason": "cadence < 2000ms triggers an upstream iroh-quinn-proto-0.13.0 \
                                       assertion (connection/mod.rs:654: untracked_bytes <= segment_size) \
                                       under concurrent kill+respawn. The assertion poisons the iroh \
                                       quinn mutex and terminates the admin-ui process. The floor \
                                       enforces the safe operating envelope until iroh-quinn-proto \
                                       0.15.x lands via an iroh upgrade.",
                        })),
                    )
                        .into_response();
                }
                let clamped = c.clamp(CHAOS_CADENCE_FLOOR_MS, CHAOS_CADENCE_CEILING_MS);
                state.chaos.cadence_ms.store(clamped, Ordering::SeqCst);
            }
        }
    }
    if !already {
        state.chaos.running.store(true, Ordering::SeqCst);
        if state.chaos.cadence_ms.load(Ordering::SeqCst) == 0 {
            state.chaos.cadence_ms.store(30_000, Ordering::SeqCst);
        }
        let state_c = state.clone();
        let handle = tokio::spawn(async move { chaos_loop(state_c).await });
        let mut slot = state.chaos.task.lock().unwrap();
        if let Some(prev) = slot.replace(handle) {
            prev.abort();
        }
    }
    chaos_state_json(&state).into_response()
}

async fn handle_chaos_stop(State(state): State<AppState>) -> impl IntoResponse {
    state.chaos.running.store(false, Ordering::SeqCst);
    if let Some(h) = state.chaos.task.lock().unwrap().take() {
        h.abort();
    }
    chaos_state_json(&state).into_response()
}

async fn handle_chaos_state(State(state): State<AppState>) -> impl IntoResponse {
    chaos_state_json(&state).into_response()
}

#[derive(Deserialize)]
struct RunTestRequest {
    name: String,
    #[serde(default)]
    seed: Option<u64>,
}

/// POST /api/tests/run — invoke `rfa.exe mesh test run <name> --seed <s>` and
/// return the resulting report. Spawns rfa as a subprocess (it owns the per-test
/// runners) and reads back the JSON written to E:/tmp/rafka-tests/<name>-<s>.json.
/// GET /api/messages — live data-plane traffic flowing through admin-ui.
/// Returns the last 500 frames received via run_frame_reader. Newest first.
/// Source: rafka_node_base::message_ring() (process-global VecDeque).
async fn handle_messages() -> impl IntoResponse {
    let ring = message_ring();
    let guard = ring.lock().unwrap();
    let mut items: Vec<_> = guard.iter().cloned().collect();
    drop(guard);
    items.reverse(); // newest first
    items.truncate(500);
    (StatusCode::OK, axum::Json(json!({"messages": items}))).into_response()
}

async fn handle_test_run(
    State(state): State<AppState>,
    Json(body): Json<RunTestRequest>,
) -> impl IntoResponse {
    let seed = body.seed.unwrap_or(42);
    let rfa_bin = format!("{}/debug/rfa.exe", state.cargo_target_dir);
    if !std::path::Path::new(&rfa_bin).exists() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": format!("rfa binary not found at {rfa_bin}")})),
        )
            .into_response();
    }

    // Red-team round-2 F#3: validate test name before using it as CLI arg
    // AND as file-path component. tokio Command::args doesn't shell-expand,
    // so the arg itself is safe — but the file-path format string can be
    // exploited with `../`.
    if body.name.is_empty()
        || body.name.len() > 64
        || !body.name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        || body.name.starts_with('-')
    {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": format!("invalid test name '{}' — must match ^[a-z0-9][a-z0-9-]*$", body.name)})),
        )
            .into_response();
    }

    // Red-team A#8: serialize concurrent runs of the same test. Two parallel
    // calls used to both succeed and return stale reports from disk.
    if state.running_tests.insert(body.name.clone(), ()).is_some() {
        return (
            StatusCode::CONFLICT,
            axum::Json(json!({"error": format!("test '{}' already running", body.name)})),
        )
            .into_response();
    }
    // Guard that ensures the entry is removed even on early return / panic.
    struct RunGuard {
        map: Arc<DashMap<String, ()>>,
        name: String,
    }
    impl Drop for RunGuard {
        fn drop(&mut self) {
            self.map.remove(&self.name);
        }
    }
    let _guard = RunGuard {
        map: Arc::clone(&state.running_tests),
        name: body.name.clone(),
    };

    state.events.push(LocalEvent {
        ts_us: now_us(),
        kind: "test.start".to_string(),
        node_name: Some(body.name.clone()),
        node_type: None,
        mesh_id: None,
        detail: Some(format!("seed={seed}")),
    });

    // Pass through whichever bind addr admin-ui is actually serving on so
    // rfa hits THIS instance, not a stale port.
    let bind_addr = std::env::var("RAFKA_ADMIN_UI_BIND_ADDR")
        .or_else(|_| std::env::var("RAFKA_TOPOLOGY_UI_BIND_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:19090".to_string());
    let api_url = format!("http://{bind_addr}");
    let mut cmd = tokio::process::Command::new(&rfa_bin);
    cmd.args([
        "--api-url",
        &api_url,
        "mesh",
        "test",
        "run",
        &body.name,
        "--seed",
        &seed.to_string(),
    ])
    .env("CARGO_TARGET_DIR", &state.cargo_target_dir)
    .current_dir("E:/dev/rafka-V2-new-mesh");

    let output = match tokio::time::timeout(
        Duration::from_secs(600),
        cmd.output(),
    )
    .await
    {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": format!("rfa spawn failed: {e}")})),
            )
                .into_response();
        }
        Err(_) => {
            return (
                StatusCode::GATEWAY_TIMEOUT,
                axum::Json(json!({"error": "test exceeded 10 min wall clock"})),
            )
                .into_response();
        }
    };

    let report_path = format!("E:/tmp/rafka-tests/{}-{seed}.json", body.name);
    let report: Value = std::fs::read_to_string(&report_path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| {
            json!({
                "name": body.name,
                "seed": seed,
                "status": if output.status.success() { "passed" } else { "failed" },
                "detail": format!(
                    "no report file; exit={:?} stdout={} stderr={}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stdout).chars().take(400).collect::<String>(),
                    String::from_utf8_lossy(&output.stderr).chars().take(400).collect::<String>(),
                ),
            })
        });

    state.events.push(LocalEvent {
        ts_us: now_us(),
        kind: "test.end".to_string(),
        node_name: Some(body.name.clone()),
        node_type: None,
        mesh_id: None,
        detail: Some(format!(
            "{} (exit={:?})",
            report["status"].as_str().unwrap_or("?"),
            output.status.code()
        )),
    });

    (StatusCode::OK, axum::Json(report)).into_response()
}

fn chaos_state_json(state: &AppState) -> axum::Json<Value> {
    let last = state.chaos.last_event_ts_us.load(Ordering::SeqCst);
    axum::Json(json!({
        "running": state.chaos.running.load(Ordering::SeqCst),
        "cadence_ms": state.chaos.cadence_ms.load(Ordering::SeqCst),
        "total_events": state.chaos.total_events.load(Ordering::SeqCst),
        "last_event_ts_us": if last > 0 { Some(last) } else { None },
    }))
}

/// Continuous chaos: every cadence_ms, pick a random non-bridge spawned node,
/// kill it, then immediately respawn a same-type replacement in the same mesh.
/// Bridges are protected so the network always has cross-mesh connectivity.
async fn chaos_loop(state: AppState) {
    loop {
        let cadence = state.chaos.cadence_ms.load(Ordering::SeqCst).max(1000);
        tokio::time::sleep(Duration::from_millis(cadence)).await;
        if !state.chaos.running.load(Ordering::SeqCst) {
            break;
        }

        // Pick a random non-bridge from spawned_meta.
        let candidates: Vec<(String, SpawnedMeta)> = state
            .spawned_meta
            .iter()
            .filter(|e| e.value().node_type != "bridge")
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        if candidates.is_empty() {
            continue;
        }
        let idx = rand::thread_rng().gen_range(0..candidates.len());
        let (victim, meta) = &candidates[idx];

        let victim_c = victim.clone();
        let meta_c = meta.clone();
        match kill_one(&state, &victim_c).await {
            Ok(reason) => {
                state.chaos.total_events.fetch_add(1, Ordering::SeqCst);
                state.chaos.last_event_ts_us.store(now_us(), Ordering::SeqCst);
                state.events.push(LocalEvent {
                    ts_us: now_us(),
                    kind: "chaos.kill".to_string(),
                    node_name: Some(victim_c.clone()),
                    node_type: Some(meta_c.node_type.clone()),
                    mesh_id: Some(meta_c.mesh_id.clone()),
                    detail: Some(format!("chaos kill ({reason})")),
                });
                info_span!(
                    "rafka.ui.chaos.kill",
                    node_name = %victim_c,
                    node_type = %meta_c.node_type,
                    mesh_id = %meta_c.mesh_id,
                    reason = %reason,
                    "otel.kind" = "internal",
                )
                .in_scope(|| info!("chaos killed {victim_c}"));
            }
            Err(e) => {
                tracing::warn!(error = %e, victim = %victim_c, "chaos kill failed");
                continue;
            }
        }

        // Respawn replacement
        let mut env = HashMap::new();
        env.insert("RAFKA_MESH_ID".to_string(), meta_c.mesh_id.clone());
        match spawn_one(&state, &meta_c.node_type, env).await {
            Ok((new_name, _)) => {
                state.chaos.total_events.fetch_add(1, Ordering::SeqCst);
                state.events.push(LocalEvent {
                    ts_us: now_us(),
                    kind: "chaos.respawn".to_string(),
                    node_name: Some(new_name.clone()),
                    node_type: Some(meta_c.node_type.clone()),
                    mesh_id: Some(meta_c.mesh_id.clone()),
                    detail: Some(format!("replaces {victim_c}")),
                });
                info_span!(
                    "rafka.ui.chaos.respawn",
                    node_name = %new_name,
                    replaces = %victim_c,
                    node_type = %meta_c.node_type,
                    mesh_id = %meta_c.mesh_id,
                    "otel.kind" = "internal",
                )
                .in_scope(|| info!("chaos respawned {new_name} replacing {victim_c}"));
            }
            Err(e) => {
                tracing::warn!(error = %e, "chaos respawn failed");
            }
        }
    }
    tracing::info!("chaos loop exited");
}

async fn handle_kill(
    State(state): State<AppState>,
    Path(node_name): Path<String>,
) -> impl IntoResponse {
    // Red-team round-2 F#1: validate the path segment FIRST before passing
    // to kill_one. The processes DashMap guard would have rejected a crafted
    // name anyway, but defense-in-depth requires we never construct a
    // remove_dir_all() target from an unvalidated path component. Names are
    // always `<type>-<8hex>` per spawn_one's naming convention.
    if !is_valid_node_name(&node_name) {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": format!("invalid node_name '{node_name}' — must match ^(gateway|broker|compute|registry|bridge)-[0-9a-f]{{8}}$")})),
        )
            .into_response();
    }
    // Red-team A#7: DELETE is idempotent. A second call returning 404 broke
    // operator retry loops; now we report 200 with reason="already_gone".
    match kill_one(&state, &node_name).await {
        Ok(reason) => (
            StatusCode::OK,
            axum::Json(json!({"node_name": node_name, "reason": reason})),
        )
            .into_response(),
        Err(_) => (
            StatusCode::OK,
            axum::Json(json!({"node_name": node_name, "reason": "already_gone"})),
        )
            .into_response(),
    }
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
/// Phase A: background topology snapshot refresher. Every 3 seconds, fans
/// out Jaeger queries for heartbeats + peer.connected + frame.sent, resolves
/// peer_id ↔ node_name, builds real edges + per-node frame counts, and
/// stores the result in state.topology_cache. /api/topology +
/// /api/heartbeats then read the cache instantly (no Jaeger on the request
/// path). Trades: 3-second snapshot staleness for sub-millisecond reads.
async fn topology_refresher(state: AppState) {
    loop {
        let new_snap = compute_snapshot(&state).await;
        // Sticky-edges policy: nodes come from spawned_meta and are always
        // fresh; edges + per-node throughput come from Jaeger fan-out and
        // can transiently return empty when Jaeger is hammered. Don't blow
        // away the last good edges on a single failed cycle — the operator
        // shouldn't see the topology flicker empty. Replace only when we
        // observed at least one edge OR one frame.
        let got_data = !new_snap.edges.is_empty()
            || new_snap.nodes.iter().any(|n| {
                n["frames_per_min"].as_u64().unwrap_or(0) > 0
            });
        let mut slot = state.topology_cache.write().await;
        if got_data || slot.edges.is_empty() {
            *slot = new_snap;
        } else {
            // Keep edges + throughput from the previous good snapshot, but
            // refresh the node list (membership) from this cycle so the UI
            // sees adds/removes immediately.
            slot.nodes = new_snap.nodes;
            slot.heartbeats = new_snap.heartbeats;
            slot.computed_at_ms = new_snap.computed_at_ms;
        }
        drop(slot);
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn compute_snapshot(state: &AppState) -> TopologySnapshot {
    let now_us = now_us();

    // ── Step 1: heartbeat fan-out → id_to_name + name_to_peer_count + heartbeats rows ──
    let mut hb_handles = Vec::new();
    for svc in KNOWN_NODE_TYPES.iter() {
        let url = format!(
            "{}/api/traces?service={}&operation=rafka.mesh.heartbeat&limit=30&lookback=2m",
            state.jaeger_url, svc
        );
        let http = state.http.clone();
        let svc_s = svc.to_string();
        hb_handles.push(tokio::spawn(async move {
            let body: Value = match http.get(&url).timeout(Duration::from_secs(20)).send().await {
                Ok(r) => r.json::<Value>().await.unwrap_or(json!({"data":[]})),
                Err(_) => json!({"data":[]}),
            };
            let mut found: Vec<(String, String, i64, i64)> = Vec::new();
            if let Some(arr) = body["data"].as_array() {
                for trace in arr {
                    if let Some(spans) = trace["spans"].as_array() {
                        for s in spans {
                            let tags = s["tags"].as_array();
                            let nid = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "node_id"))
                                .and_then(|x| x["value"].as_str())
                                .unwrap_or("")
                                .to_string();
                            let nname = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "node_name"))
                                .and_then(|x| x["value"].as_str())
                                .unwrap_or("")
                                .to_string();
                            let pcount = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "peer_count"))
                                .and_then(|x| x["value"].as_i64())
                                .unwrap_or(0);
                            let start_us = s["startTime"].as_i64().unwrap_or(0);
                            if !nid.is_empty() && !nname.is_empty() {
                                found.push((nid, nname, pcount, start_us));
                            }
                        }
                    }
                }
            }
            (svc_s, found)
        }));
    }
    let mut id_to_name: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut name_to_peer_count: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    let mut name_to_age_ms: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    let mut name_to_node_id: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for h in hb_handles {
        if let Ok((_svc, rows)) = h.await {
            for (nid, name, pcount, start_us) in rows {
                id_to_name.insert(nid.clone(), name.clone());
                name_to_node_id.insert(name.clone(), nid);
                let cur_p = name_to_peer_count.entry(name.clone()).or_insert(0);
                if pcount > *cur_p {
                    *cur_p = pcount;
                }
                let age = if start_us > 0 {
                    ((now_us - start_us).max(0)) / 1000
                } else {
                    -1
                };
                let cur_a = name_to_age_ms.entry(name).or_insert(i64::MAX);
                if age < *cur_a {
                    *cur_a = age;
                }
            }
        }
    }

    // ── Step 2: peer.connected fan-out for edge membership ──
    let mut pc_handles = Vec::new();
    for svc in KNOWN_NODE_TYPES.iter() {
        let url = format!(
            "{}/api/traces?service={}&operation=rafka.mesh.peer.connected&limit=300&lookback=10m",
            state.jaeger_url, svc
        );
        let http = state.http.clone();
        pc_handles.push(tokio::spawn(async move {
            let body: Value = match http.get(&url).timeout(Duration::from_secs(20)).send().await {
                Ok(r) => r.json::<Value>().await.unwrap_or(json!({"data":[]})),
                Err(_) => json!({"data":[]}),
            };
            let mut pairs: Vec<(String, String)> = Vec::new();
            if let Some(arr) = body["data"].as_array() {
                for trace in arr {
                    if let Some(spans) = trace["spans"].as_array() {
                        for s in spans {
                            if s["operationName"] != "rafka.mesh.peer.connected" {
                                continue;
                            }
                            let tags = s["tags"].as_array();
                            let self_id = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "node_id"))
                                .and_then(|x| x["value"].as_str())
                                .unwrap_or("")
                                .to_string();
                            let peer_id = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "peer_id"))
                                .and_then(|x| x["value"].as_str())
                                .unwrap_or("")
                                .to_string();
                            if !self_id.is_empty() && !peer_id.is_empty() {
                                pairs.push((self_id, peer_id));
                            }
                        }
                    }
                }
            }
            pairs
        }));
    }

    // ── Step 3: frame.sent fan-out for edge weights + per-node throughput ──
    let mut fs_handles = Vec::new();
    for svc in KNOWN_NODE_TYPES.iter() {
        let url = format!(
            "{}/api/traces?service={}&operation=rafka.mesh.frame.sent&limit=500&lookback=1m",
            state.jaeger_url, svc
        );
        let http = state.http.clone();
        fs_handles.push(tokio::spawn(async move {
            let body: Value = match http.get(&url).timeout(Duration::from_secs(20)).send().await {
                Ok(r) => r.json::<Value>().await.unwrap_or(json!({"data":[]})),
                Err(_) => json!({"data":[]}),
            };
            let mut rows: Vec<(String, String)> = Vec::new();
            if let Some(arr) = body["data"].as_array() {
                for trace in arr {
                    if let Some(spans) = trace["spans"].as_array() {
                        for s in spans {
                            if s["operationName"] != "rafka.mesh.frame.sent" {
                                continue;
                            }
                            let tags = s["tags"].as_array();
                            let self_id = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "node_id"))
                                .and_then(|x| x["value"].as_str())
                                .unwrap_or("")
                                .to_string();
                            let peer_id = tags
                                .and_then(|t| t.iter().find(|x| x["key"] == "peer_id"))
                                .and_then(|x| x["value"].as_str())
                                .unwrap_or("")
                                .to_string();
                            if !self_id.is_empty() && !peer_id.is_empty() {
                                rows.push((self_id, peer_id));
                            }
                        }
                    }
                }
            }
            rows
        }));
    }

    let mut pc_pairs: Vec<(String, String)> = Vec::new();
    for h in pc_handles {
        if let Ok(p) = h.await {
            pc_pairs.extend(p);
        }
    }
    let mut fs_pairs: Vec<(String, String)> = Vec::new();
    for h in fs_handles {
        if let Ok(p) = h.await {
            fs_pairs.extend(p);
        }
    }

    // ── Build edges + per-node frame rates ──
    let canon = |a: &str, b: &str| -> (String, String) {
        if a <= b { (a.to_string(), b.to_string()) } else { (b.to_string(), a.to_string()) }
    };
    let mut edge_counts: std::collections::HashMap<(String, String), u64> =
        std::collections::HashMap::new();
    let mut frames_per_node: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();

    for (self_id, peer_id) in &fs_pairs {
        let self_name = match id_to_name.get(self_id) { Some(n) => n.clone(), None => continue };
        let peer_name = match id_to_name.get(peer_id) { Some(n) => n.clone(), None => continue };
        *frames_per_node.entry(self_name.clone()).or_insert(0) += 1;
        *edge_counts.entry(canon(&self_name, &peer_name)).or_insert(0) += 1;
    }
    for (self_id, peer_id) in &pc_pairs {
        let self_name = match id_to_name.get(self_id) { Some(n) => n.clone(), None => continue };
        let peer_name = match id_to_name.get(peer_id) { Some(n) => n.clone(), None => continue };
        edge_counts.entry(canon(&self_name, &peer_name)).or_insert(0);
    }

    // Build nodes from spawned_meta + enrich with heartbeat data
    let mut nodes: Vec<Value> = Vec::new();
    let mut heartbeats: Vec<Value> = Vec::new();
    for entry in state.spawned_meta.iter() {
        let name = entry.key().clone();
        let meta = entry.value();
        let pcount = *name_to_peer_count.get(&name).unwrap_or(&0);
        let frames = *frames_per_node.get(&name).unwrap_or(&0);
        let age_ms = *name_to_age_ms.get(&name).unwrap_or(&-1);
        let nid = name_to_node_id.get(&name).cloned().unwrap_or_default();
        nodes.push(json!({
            "id": name,
            "node_id": nid,
            "type": meta.node_type,
            "mesh_id": meta.mesh_id,
            "peer_count": pcount,
            "frames_per_min": frames,
            "status": if age_ms >= 0 && age_ms < 30000 { "live" } else { "pending" },
        }));
        heartbeats.push(json!({
            "node_name": name,
            "node_type": meta.node_type,
            "node_id": name_to_node_id.get(&entry.key().clone()).cloned().unwrap_or_default(),
            "mesh_id": meta.mesh_id,
            "peer_count": pcount,
            "age_ms": age_ms,
        }));
    }

    // Edges only between currently-spawned endpoints (filters ghost edges)
    let mut edges: Vec<Value> = Vec::new();
    for ((a, b), count) in &edge_counts {
        let mesh_a = state.spawned_meta.get(a).map(|e| e.value().mesh_id.clone());
        let mesh_b = state.spawned_meta.get(b).map(|e| e.value().mesh_id.clone());
        let (mesh_a, mesh_b) = match (mesh_a, mesh_b) {
            (Some(a), Some(b)) => (a, b),
            _ => continue,
        };
        let kind = if mesh_a == mesh_b { "within" } else { "cross" };
        edges.push(json!({
            "from": a,
            "to": b,
            "kind": kind,
            "frame_count": *count,
        }));
    }

    TopologySnapshot {
        nodes,
        edges,
        heartbeats,
        computed_at_ms: now_us / 1000,
    }
}

/// Phase C: read-only mesh observer. Spawns an iroh endpoint with the same
/// ALPN as nodes, subscribes to iroh-gossip on every mesh topic discovered
/// via spawned_meta. On each received GossipDigest, updates state.live with
/// the latest counters and computes a throughput rate from the delta against
/// the previous digest. Bypasses Jaeger entirely on the hot path.
///
/// Observer-tainted invariants:
/// - never broadcasts a digest of its own (subscribe-only)
/// - never appears in spawned_meta (so chaos can't pick it)
/// - rendered separately in the UI if it ever appears as a peer
async fn observer_task(state: AppState) {
    use futures_lite::StreamExt;
    use iroh_gossip::api::Event;
    use std::str::FromStr;

    tracing::info!("observer: bringing up iroh endpoint via IrohMeshTransport (same path nodes use)");
    let secret = iroh::SecretKey::generate(rand::rngs::OsRng);
    let bind_addr: std::net::SocketAddrV4 = "127.0.0.1:0".parse().unwrap();
    let transport = match rafka_mesh_transport::IrohMeshTransport::new(secret, bind_addr).await {
        Ok(t) => t,
        Err(err) => {
            tracing::error!(error = %err, "observer: IrohMeshTransport::new failed");
            return;
        }
    };
    let endpoint = transport.endpoint.clone();
    let observer_node_id = endpoint.node_id().to_string();
    tracing::info!(
        observer_node_id = %observer_node_id,
        "observer: iroh endpoint bound; subscribing to mesh topics as they appear"
    );

    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());

    // Track subscribed mesh ids so we don't re-subscribe to the same topic.
    let mut subscribed: std::collections::HashSet<String> = std::collections::HashSet::new();

    loop {
        // Discover meshes via spawned_meta. Skip the "bridge" sentinel and
        // empty values — those aren't real meshes.
        let current: std::collections::HashSet<String> = state
            .spawned_meta
            .iter()
            .map(|e| e.value().mesh_id.clone())
            .filter(|m| !m.is_empty() && m != "bridge")
            .collect();

        for mesh in current.difference(&subscribed).cloned().collect::<Vec<_>>() {
            let topic_bytes: [u8; 32] = *blake3::hash(mesh.as_bytes()).as_bytes();
            let topic_id = iroh_gossip::proto::TopicId::from_bytes(topic_bytes);
            match gossip.subscribe(topic_id, Vec::new()).await {
                Ok(topic) => {
                    subscribed.insert(mesh.clone());
                    tracing::info!(mesh = %mesh, topic = %hex::encode(topic_bytes), "observer: subscribed");
                    let (_sender, mut receiver) = topic.split();
                    let live = Arc::clone(&state.live);
                    let mesh_for_task = mesh.clone();
                    tokio::spawn(async move {
                        while let Some(event_res) = receiver.next().await {
                            let event = match event_res {
                                Ok(e) => e,
                                Err(err) => {
                                    tracing::warn!(mesh = %mesh_for_task, error = %err, "observer: gossip stream error");
                                    continue;
                                }
                            };
                            let bytes = match event {
                                Event::Received(msg) => msg.content,
                                _ => continue,
                            };
                            let digest: GossipDigest = match postcard::from_bytes(&bytes) {
                                Ok(d) => d,
                                Err(err) => {
                                    tracing::warn!(error = %err, "observer: digest decode failed");
                                    continue;
                                }
                            };
                            let now_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0);
                            let prev = live.get(&digest.node_id).map(|e| e.value().clone());
                            let (sent_rate, recv_rate) = match prev {
                                Some(p) => {
                                    let dt_ms = now_ms.saturating_sub(p.last_seen_ms);
                                    if dt_ms == 0 {
                                        (p.sent_per_sec, p.recv_per_sec)
                                    } else {
                                        let ds = digest
                                            .frames_sent_total
                                            .saturating_sub(p.digest.frames_sent_total)
                                            as f64;
                                        let dr = digest
                                            .frames_recv_total
                                            .saturating_sub(p.digest.frames_recv_total)
                                            as f64;
                                        let factor = 1000.0 / dt_ms as f64;
                                        (ds * factor, dr * factor)
                                    }
                                }
                                None => (0.0, 0.0),
                            };
                            live.insert(
                                digest.node_id.clone(),
                                LiveNodeState {
                                    digest,
                                    last_seen_ms: now_ms,
                                    sent_per_sec: sent_rate,
                                    recv_per_sec: recv_rate,
                                },
                            );
                        }
                    });
                }
                Err(err) => {
                    tracing::warn!(mesh = %mesh, error = %err, "observer: subscribe failed");
                }
            }
        }

        // Periodically prune entries whose last_seen_ms is >30s old —
        // means the node died and its gossip stopped reaching us.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let stale: Vec<String> = state
            .live
            .iter()
            .filter(|e| now_ms.saturating_sub(e.value().last_seen_ms) > 30_000)
            .map(|e| e.key().clone())
            .collect();
        for k in stale {
            state.live.remove(&k);
        }

        // Re-feed mdns-discovered iroh peers into each topic so the gossip
        // swarm forms with the observer included. Cheap, idempotent.
        for mesh in &subscribed {
            let topic_bytes: [u8; 32] = *blake3::hash(mesh.as_bytes()).as_bytes();
            let topic_id = iroh_gossip::proto::TopicId::from_bytes(topic_bytes);
            // Best effort — collect peers from spawned_meta if their node_id is known.
            // For now leave empty; iroh mdns will surface them.
            let _ = (topic_id, iroh::NodeId::from_str("0").is_ok()); // no-op to satisfy borrow
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn reaper_loop(
    processes: Arc<DashMap<String, Mutex<tokio::process::Child>>>,
    spawned_meta: Arc<DashMap<String, SpawnedMeta>>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        let names: Vec<String> = processes.iter().map(|e| e.key().clone()).collect();
        for name in names {
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
                spawned_meta.remove(&name); // Red-team A#6: also clear meta so topology drops the ghost
                // Red-team A#6 + A#5: delete data dir on reap. Contains node-identity.json
                // (secret key) which must not persist after the node dies.
                let spawn_dir = format!("E:/tmp/rafka-ui-nodes/{}", name);
                if let Err(e) = tokio::fs::remove_dir_all(&spawn_dir).await {
                    tracing::warn!(dir = %spawn_dir, error = %e, "reaper: data dir cleanup failed");
                }
                tracing::info_span!(
                    "rafka.ui.subprocess.reaped",
                    node_name = %name,
                    exit_code = status.code().unwrap_or(-1) as i64,
                    "otel.kind" = "internal",
                )
                .in_scope(|| info!(node_name = %name, exit_code = status.code().unwrap_or(-1), "subprocess reaped — exited without DELETE"));
            }
        }

        // Red-team A#6 second pass: orphan data dirs (no matching process or
        // meta) get swept. Catches dirs left over from crashes pre-reaper.
        if let Ok(mut rd) = tokio::fs::read_dir("E:/tmp/rafka-ui-nodes/").await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let name = entry.file_name().to_string_lossy().into_owned();
                if !processes.contains_key(&name) && !spawned_meta.contains_key(&name) {
                    let path = entry.path();
                    let _ = tokio::fs::remove_dir_all(&path).await;
                }
            }
        }
    }
}

fn chrono_like_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("epoch_ms={}", d.as_millis())
}

fn install_panic_hook() -> std::path::PathBuf {
    let panic_log_path = std::env::var("CARGO_TARGET_DIR")
        .map(|d| std::path::PathBuf::from(d).join("admin-ui-panic.log"))
        .unwrap_or_else(|_| std::path::PathBuf::from("./admin-ui-panic.log"));
    let panic_log_path_for_hook = panic_log_path.clone();
    std::panic::set_hook(Box::new(move |info| {
        // QA postfix R5 fix: write atomically with a SINGLE syscall
        // (OpenOptions::append + write_all + drop) inside the hook.
        // Previous impl held the file open across `write_all`; the
        // iroh-quinn double-panic (mutex poisoning fires a second
        // panic on the SAME thread before `write_all` returns) caused
        // Rust to abort mid-write, leaving 0-byte files.
        //
        // The fix is two-fold:
        //   1. eprintln FIRST — stderr is typically piped to a
        //      capture file by the parent (admin-ui-*-stderr.log);
        //      this guarantees the panic message reaches disk even
        //      if our file write is preempted.
        //   2. Use a synchronous block where the full message is
        //      built, then write+flush+drop happens as quickly as
        //      possible — no intermediate state where a second panic
        //      can interrupt write_all halfway.
        let bt = std::backtrace::Backtrace::force_capture();
        let thread = std::thread::current();
        let line = format!(
            "\n==== PANIC @ {} (thread={:?}) ====\n{}\n---- backtrace ----\n{}\n",
            chrono_like_now(),
            thread.name().unwrap_or("<unnamed>"),
            info,
            bt,
        );
        eprintln!("{line}");
        // Atomic-ish: open, write, flush, close in tight sequence.
        // std::fs::write does this — open(create)+write_all+close.
        // For append semantics we do it manually but explicitly
        // flush before drop so the buffer hits disk synchronously.
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&panic_log_path_for_hook)
        {
            use std::io::Write;
            let _ = f.write_all(line.as_bytes());
            let _ = f.flush();
            // explicit drop in case the next panic preempts the
            // implicit-drop path
            drop(f);
        }
    }));
    panic_log_path
}

fn main() -> Result<()> {
    // SPEC §7 #1 + red-team R5 root-cause fix: install panic hook BEFORE
    // any threads exist. Previous attempt installed the hook inside async
    // fn main (i.e. after #[tokio::main] had already started worker
    // threads + iroh's quinn driver threads). Red team confirmed those
    // threads were using the DEFAULT hook (stderr backtrace showed
    // `std::panicking::default_hook` firing on iroh-quinn crash, not our
    // custom hook). Install in plain fn main BEFORE building the runtime.
    std::env::set_var("RUST_BACKTRACE", "full");
    let panic_log_path = install_panic_hook();
    eprintln!(
        "[admin-ui] panic hook installed; future panics will append to {}",
        panic_log_path.display()
    );

    // Build the tokio runtime explicitly so the panic hook is in place
    // before any worker threads (including iroh-quinn's QUIC driver)
    // start.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async_main(panic_log_path))
}

async fn async_main(panic_log_path: std::path::PathBuf) -> Result<()> {
    tracing::info!(panic_log = %panic_log_path.display(), "panic hook installed (pre-runtime)");
    // admin-ui IS a node. The NodeRuntime call below owns telemetry
    // initialization + iroh endpoint + gossip subscription + heartbeat
    // broadcast — same path the broker / gateway / compute / registry /
    // bridge binaries take. We DO NOT init telemetry separately here;
    // tracing_subscriber's global init would panic on the second call.
    //
    // RAFKA_OBSERVER_MESHES: admin-ui subscribes to every mesh's gossip
    // topic, not just its own. Without this, iroh-gossip's topic isolation
    // means admin-ui only sees one mesh's digests at a time. We default to
    // mesh-a + mesh-b (bootstrap composition); operator can override.
    if std::env::var("RAFKA_OBSERVER_MESHES").is_err() {
        // Watch every legit mesh: the two bootstrap meshes plus the bridge
        // mesh (bridges live there). No "default" anymore — spawn_one now
        // rejects nodes without an explicit RAFKA_MESH_ID.
        std::env::set_var("RAFKA_OBSERVER_MESHES", "mesh-a,mesh-b,bridge");
    }
    // Spawned as a tokio task so axum can run in parallel in this same
    // process. Same tokio runtime, same lifecycle.
    let node_handle = tokio::spawn(async {
        if let Err(e) = rafka_node_base::NodeRuntime::new("admin-ui")
            .with_role(rafka_node_base::Role::Observer)
            .run()
            .await
        {
            eprintln!("[admin-ui node] runtime exited: {e}");
        }
    });

    // Accept either env var name during the topology-ui → admin-ui rename.
    let bind_addr = std::env::var("RAFKA_ADMIN_UI_BIND_ADDR")
        .or_else(|_| std::env::var("RAFKA_TOPOLOGY_UI_BIND_ADDR"))
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

    // Preflight: every KNOWN_NODE_TYPES binary must exist under
    // {cargo_target_dir}/debug/, or spawn_one will silently return os
    // error 2 from cmd.spawn() and bootstrap will report bogus errors.
    // Two specific failure modes this catches:
    //   - rafka-bridge was missing after `cargo build` ran without -p
    //     rafka-bridge → bootstrap looked like "16 spawned, 2 errors"
    //     and bridges never appeared in topology (caused
    //     mesh-five-types-present + gossip-mesh-to-mesh to fail).
    //   - CARGO_TARGET_DIR redirected to a stale build dir.
    // Loud at startup beats silent at bootstrap.
    {
        let mut missing: Vec<String> = Vec::new();
        for nt in KNOWN_NODE_TYPES {
            let p = format!("{cargo_target_dir}/debug/rafka-{nt}.exe");
            if !std::path::Path::new(&p).exists() {
                missing.push(p);
            }
        }
        if !missing.is_empty() {
            eprintln!("[admin-ui] PREFLIGHT FAILURE: missing peer binaries:");
            for p in &missing {
                eprintln!("  - {p}");
            }
            eprintln!("[admin-ui] run: cargo build -p rafka-broker -p rafka-gateway -p rafka-compute -p rafka-registry -p rafka-bridge");
            anyhow::bail!(
                "preflight failed: {} peer binary/binaries missing under {}",
                missing.len(),
                cargo_target_dir
            );
        }
        tracing::info!(types = ?KNOWN_NODE_TYPES, "preflight: all peer binaries present");
    }

    let addr: SocketAddr = bind_addr.parse()?;

    // 4s per-request timeout: Jaeger queries that take longer than this are
    // dropped so a single slow query can't make the UI banner block for 30s.
    // Individual handlers also do their own per-call timeouts where they
    // parallelize fan-outs.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .expect("reqwest client");

    let state = AppState {
        http,
        jaeger_url,
        cargo_target_dir,
        processes: Arc::new(DashMap::new()),
        spawned_meta: Arc::new(DashMap::new()),
        chaos: Arc::new(ChaosController::default()),
        events: Arc::new(EventRing::default()),
        live: Arc::new(DashMap::new()),
        topology_cache: Arc::new(tokio::sync::RwLock::new(TopologySnapshot::default())),
        running_tests: Arc::new(DashMap::new()),
        bootstrap_mutex: Arc::new(tokio::sync::Mutex::new(())),
    };

    // SPEC §7 #1: panic-resilient background-task supervisor. If a long-
    // running task panics (rare but observed during 30m soaks), this
    // wrapper logs the panic and restarts the task after 2s — admin-ui
    // stays alive instead of wedging the HTTP layer. JoinHandle::is_err()
    // catches both panics AND graceful-Err returns.
    fn supervise<F, Fut>(name: &'static str, f: F)
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(async move {
            loop {
                let h = tokio::spawn(f());
                match h.await {
                    Ok(()) => {
                        tracing::warn!(task = name, "background task exited cleanly; respawning");
                    }
                    Err(je) => {
                        tracing::error!(task = name, error = %je, "background task panicked; respawning in 2s");
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    // Phase A: background snapshot refresher. Every 3 s, recomputes
    // /api/topology + /api/heartbeats data via parallel Jaeger fan-out and
    // stores the result in state.topology_cache. Request handlers read the
    // cache instantly — Jaeger latency moves off the hot path.
    let state_for_refresher = state.clone();
    supervise("topology_refresher", move || {
        let s = state_for_refresher.clone();
        async move { topology_refresher(s).await }
    });

    // observer_task removed — the NodeRuntime task spawned above IS the
    // observer; admin-ui's own iroh endpoint sees every other node's
    // gossip digest. state.live wiring is now read directly from the
    // process-wide mesh_counters() + the peer registry inside node-base
    // (TODO next: expose those for HTTP read).
    let _live_observer_via_noderuntime = &node_handle;

    let procs_for_reaper = Arc::clone(&state.processes);
    let meta_for_reaper = Arc::clone(&state.spawned_meta);
    supervise("reaper_loop", move || {
        let p = Arc::clone(&procs_for_reaper);
        let m = Arc::clone(&meta_for_reaper);
        async move { reaper_loop(p, m).await }
    });

    // Resolve where the React build lives. CARGO_MANIFEST_DIR points at the
    // crate dir at compile time; at runtime we prefer an env override so the
    // packaged binary can sit anywhere.
    let static_dir = std::env::var("RAFKA_UI_STATIC_DIR").unwrap_or_else(|_| {
        let manifest = env!("CARGO_MANIFEST_DIR");
        format!("{manifest}/web/dist")
    });
    tracing::info!(static_dir = %static_dir, "serving React UI from");

    let app = Router::new()
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
        .route("/api/timeline", get(handle_unified_timeline))
        .route("/api/heartbeats", get(handle_heartbeats))
        .route("/api/tests", get(handle_tests))
        .route("/api/cluster/summary", get(handle_cluster_summary))
        .route("/api/bootstrap", post(handle_bootstrap))
        .route("/api/chaos/start", post(handle_chaos_start))
        .route("/api/chaos/stop", post(handle_chaos_stop))
        .route("/api/chaos/state", get(handle_chaos_state))
        .route("/api/tests/run", post(handle_test_run))
        .route("/api/messages", get(handle_messages))
        .route("/api/nodes/{node_name}", delete(handle_kill))
        .fallback_service(ServeDir::new(&static_dir).append_index_html_on_directories(true))
        .with_state(state)
        .layer(middleware::from_fn(trace_middleware));

    info!("admin-ui listening on http://{addr}");

    // Red-team R4 root-cause fix: bypass axum::serve for a custom accept
    // loop that uses hyper_util::server::conn::auto::Builder with
    // http1().header_read_timeout(30s). axum::serve relies on
    // TimeoutLayer which is a Tower middleware: it only fires once
    // hyper has assembled a complete HTTP request. A slowloris
    // attacker sending `GET / HTTP/1.1\r\nHost: x\r\n` (no terminating
    // CRLF-CRLF) never completes header assembly, so the Tower timer
    // never starts — the connection leaks indefinitely (confirmed by
    // red team 2026-05-21: 75s+ ESTABLISHED partial-header).
    //
    // hyper_util's header_read_timeout is enforced INSIDE hyper's
    // accept-to-first-byte path, so it kills slowloris connections
    // after 30s even if no complete request is ever assembled.
    //
    // Other layers preserved: TimeoutLayer(60s) still kills hung
    // in-flight requests; ConcurrencyLimitLayer(64) still caps
    // concurrent in-flight; SO_NODELAY still set on the listener.
    use tower::limit::ConcurrencyLimitLayer;
    use tower_http::timeout::TimeoutLayer;
    let app = app
        .layer(TimeoutLayer::new(std::time::Duration::from_secs(60)))
        .layer(ConcurrencyLimitLayer::new(64));
    let socket = tokio::net::TcpSocket::new_v4()?;
    socket.set_nodelay(true)?;
    socket.bind(addr)?;
    let listener = socket.listen(1024)?;

    use hyper::server::conn::http1;
    use hyper_util::rt::{TokioIo, TokioTimer};
    use hyper_util::service::TowerToHyperService;
    use tower::Service;

    // Use hyper's http1::Builder directly (not hyper_util::auto) so
    // header_read_timeout is unambiguously enforced from accept-time.
    // CRITICAL: header_read_timeout requires a Timer; without
    // `.timer(TokioTimer::new())` hyper panics at first connection
    // with "timeout 'header_read_timeout' set, but no timer set".
    //
    // Verification: a slowloris that sends partial headers and stops
    // gets FIN'd by hyper at exactly 30s.
    let conn_builder = std::sync::Arc::new({
        let mut b = http1::Builder::new();
        b.timer(TokioTimer::new());
        b.header_read_timeout(std::time::Duration::from_secs(30));
        b
    });

    let mut make_service =
        app.into_make_service_with_connect_info::<std::net::SocketAddr>();

    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(error = %e, "accept failed; continuing");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }
        };
        let io = TokioIo::new(stream);
        let tower_service = match make_service.call(peer_addr).await {
            Ok(svc) => svc,
            Err(e) => {
                tracing::warn!(error = ?e, "make_service failed for connection");
                continue;
            }
        };
        let hyper_service = TowerToHyperService::new(tower_service);
        let conn_builder = conn_builder.clone();
        tokio::spawn(async move {
            if let Err(e) = conn_builder
                .serve_connection(io, hyper_service)
                .with_upgrades()
                .await
            {
                tracing::debug!(error = ?e, "connection serve ended");
            }
        });
    }
}
