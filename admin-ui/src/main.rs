// Workaround for rustc Linux toolchain (1.94 + 1.95) ICE in `check_mod_deathness`
// when processing the unused `when_ago` helper and unused struct fields. Windows MSVC toolchain compiles fine without this; Linux ICEs.
// See memory `project_rustc_195_ice.md`.
#![allow(dead_code)]

use anyhow::Result;
use axum::{
    Router,
    extract::{Json, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use dashmap::DashMap;
use rand::Rng;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};
use tower_http::services::ServeDir;

use tracing::{info, info_span, Instrument};

const KNOWN_NODE_TYPES: &[&str] = &["gateway", "broker", "compute", "registry"];

// dhat heap profiling — OFF by default, only built with --features dhat-heap.
// The profiler is held in a static so a timer can drop it (which writes
// dhat-heap.json) and exit cleanly — admin-ui has no graceful-shutdown path,
// and dhat only dumps on Profiler::drop.
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(feature = "dhat-heap")]
static DHAT_PROFILER: std::sync::Mutex<Option<dhat::Profiler>> = std::sync::Mutex::new(None);

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

// The Admin UI is a client of node-admin core (i143.e1.s6): it reads
// node-admin's views and submits Builds; it never starts or stops a runtime.
use rafka_admin_ui::control::{self, BuildEvents};
use rafka_mesh_entity::PathName;
use rafka_node_admin_client::{BuildId, NodeAdminClient};

/// The Build routes' events, on the UI's own event timeline.
struct TimelineEvents(Arc<EventRing>);

impl BuildEvents for TimelineEvents {
    fn submitted(&self, what: &str, build_id: &BuildId, node: Option<&str>, mesh: Option<&str>) {
        self.0.push(LocalEvent {
            ts_us: now_us(),
            kind: "build.submitted".to_string(),
            node_name: node.map(str::to_string),
            node_type: None,
            mesh_id: mesh.map(str::to_string),
            detail: Some(format!("{what}: build {build_id}")),
        });
    }
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
#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    jaeger_url: String,
    cargo_target_dir: String,
    /// The node-admin this UI drives (`RAFKA_NODE_ADMIN_API_BASE`).
    admin: Option<NodeAdminClient>,
    /// Node-admin's nodes, by path.name (read-only projection of `GET /api/nodes`).
    known: Arc<DashMap<String, KnownNode>>,
    chaos: Arc<ChaosController>,
    events: Arc<EventRing>,
    /// Red-team A#8: serialize concurrent /api/tests/run calls for the same
    /// test name. Map entry exists while a test is running.
    running_tests: Arc<DashMap<String, ()>>,
}

#[derive(Deserialize)]
struct BootTraceQuery {
    service: String,
}


/// One node node-admin manages, as its `GET /api/nodes` view reports it.
#[derive(Debug, Clone)]
struct KnownNode {
    /// The node kind's segment: `broker`, `gateway`, `compute`, `rpc_node`, `node_admin`.
    node_type: String,
    mesh_id: String,
    node_id: String,
    status: String,
    is_primary: bool,
    is_fabric_primary: bool,
    incarnation_id: Option<String>,
    /// The lifecycle state the birth declared to its authority, once applied.
    declared: Option<String>,
}

impl KnownNode {
    fn of(n: &rafka_node_admin_client::NodeView) -> Self {
        let kind = serde_json::to_value(n.kind).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default();
        let status = serde_json::to_value(&n.status).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default();
        KnownNode {
            node_type: kind,
            mesh_id: n.mesh.clone(),
            node_id: n.node_id.to_string(),
            status,
            is_primary: n.is_primary,
            is_fabric_primary: n.is_fabric_primary,
            incarnation_id: n.incarnation_id.as_ref().map(|i| i.to_string()),
            declared: n.declared.clone(),
        }
    }

    /// The node as `/api/topology` and `/api/heartbeats` render it.
    fn view_json(&self, name: &str) -> Value {
        json!({
            "id": name,
            "node_name": name,
            "node_id": self.node_id,
            "type": self.node_type,
            "node_type": self.node_type,
            "mesh_id": self.mesh_id,
            "status": self.status,
            "is_primary": self.is_primary,
            "is_fabric_primary": self.is_fabric_primary,
            "incarnation_id": self.incarnation_id,
            "declared": self.declared,
        })
    }
}

/// Every node node-admin manages, sorted by path.name.
fn known_nodes_json(state: &AppState) -> Vec<Value> {
    let mut out: Vec<Value> = state.known.iter().map(|e| e.value().view_json(e.key())).collect();
    out.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    out
}

async fn handle_health() -> impl IntoResponse {
    axum::Json(json!({"status": "ok"}))
}

async fn handle_spawned_list(State(state): State<AppState>) -> impl IntoResponse {
    let names: Vec<String> = state.known.iter().map(|e| e.key().clone()).collect();
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

/// `GET /api/heartbeats` — one entry per node node-admin manages, from its
/// `GET /api/nodes` view: the node's status, seat and incarnation.
async fn handle_heartbeats(State(state): State<AppState>) -> impl IntoResponse {
    let out = known_nodes_json(&state);
    (StatusCode::OK, axum::Json(json!({"heartbeats": out, "source": "node-admin"}))).into_response()
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
    // Resolve Jaeger-sourced peer events' node_name through node-admin's
    // view, so the timeline shows the path.name instead of the kind alone.
    let id_to_name: std::collections::HashMap<String, String> = state
        .known
        .iter()
        .filter(|e| !e.value().node_id.is_empty())
        .map(|e| (e.value().node_id.clone(), e.key().clone()))
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
    let spawned_count = state.known.iter().count() as i64;
    let meshes: std::collections::HashSet<String> =
        state.known.iter().map(|e| e.value().mesh_id.clone()).collect();
    let mut meshes_vec: Vec<String> = meshes.into_iter().collect();
    meshes_vec.sort();

    // node-admin's view carries no peer counts; a node's peers within its
    // mesh are the mesh's other nodes.
    let mean_peers = {
        let n = spawned_count as f64;
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

/// `GET /api/topology` — the nodes node-admin manages, one per path.name,
/// each with its status, seat and incarnation from node-admin's view. The
/// view carries no peer connections, so the response carries no edges.
async fn handle_topology(State(state): State<AppState>) -> impl IntoResponse {
    let nodes = known_nodes_json(&state);
    (StatusCode::OK, axum::Json(json!({"nodes": nodes, "source": "node-admin"}))).into_response()
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

/// Red-team R1 + QA postfix NF-1/NF-2 boundary enforcement: low
/// chaos cadence triggers iroh-quinn-proto-0.13.0
/// connection/mod.rs:654 assertion `untracked_bytes <= segment_size`
/// (upstream bug; concurrent kill+respawn while QUIC streams are
/// in flight).
///
/// Floor history:
///   * 2000ms → QA found 18 panics in 5min (NF-1)
///   * 5000ms → direct retest showed 6+ panic pairs in 90s of
///     chaos; admin-ui process terminated mid-test (panics
///     escaped tokio task supervision via iroh's QUIC worker pool)
///   * 30000ms → matches the original `cadence_ms` AtomicU64 default;
///     the 30-min chaos-soak-9prim-30min test passes cleanly at
///     this cadence (40+ events, 0 failed, commit 88937f9 evidence)
///
/// Why we land at 30s: the cadence floor sets the minimum gap
/// between kill+respawn cycles. iroh's QUIC connection cleanup +
/// new connection establishment cycles complete reliably within a
/// 30-second window; tighter than that exposes the race
/// upstream-known-unfixed assertion.
///
/// When iroh upgrades past 0.91.2 to a release that includes
/// iroh-quinn-proto-0.15.x+ (where this assertion is fixed
/// upstream), this floor can be lowered toward 1000ms.
const CHAOS_CADENCE_FLOOR_MS: u64 = 30_000;
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

async fn handle_test_run(
    State(state): State<AppState>,
    Json(body): Json<RunTestRequest>,
) -> impl IntoResponse {
    let seed = body.seed.unwrap_or(42);
    let rfa_bin = format!("{}/debug/rfa{}", state.cargo_target_dir, std::env::consts::EXE_SUFFIX);
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

/// Continuous chaos: every cadence_ms, pick a random live rpc node from
/// node-admin's view and ask node-admin to restart it (a `RestartNode`
/// Build). Node-admins are never chosen, so the fabric keeps its control.
async fn chaos_loop(state: AppState) {
    loop {
        let cadence = state.chaos.cadence_ms.load(Ordering::SeqCst).max(1000);
        tokio::time::sleep(Duration::from_millis(cadence)).await;
        if !state.chaos.running.load(Ordering::SeqCst) {
            break;
        }
        let Some(client) = state.admin.clone() else { continue };
        let candidates: Vec<(String, KnownNode)> = state
            .known
            .iter()
            .filter(|e| e.value().node_type == "rpc_node")
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        if candidates.is_empty() {
            continue;
        }
        let idx = rand::thread_rng().gen_range(0..candidates.len());
        let (victim, meta) = candidates[idx].clone();
        let Ok(node) = victim.parse::<PathName>() else { continue };
        match client.restart(&node).await {
            Ok(build_id) => {
                state.chaos.total_events.fetch_add(1, Ordering::SeqCst);
                state.chaos.last_event_ts_us.store(now_us(), Ordering::SeqCst);
                state.events.push(LocalEvent {
                    ts_us: now_us(),
                    kind: "chaos.restart".to_string(),
                    node_name: Some(victim.clone()),
                    node_type: Some(meta.node_type.clone()),
                    mesh_id: Some(meta.mesh_id.clone()),
                    detail: Some(format!("restart build {build_id}")),
                });
                info_span!(
                    "rafka.ui.chaos.restart",
                    node_name = %victim,
                    mesh_id = %meta.mesh_id,
                    build_id = %build_id,
                    "otel.kind" = "internal",
                )
                .in_scope(|| info!("chaos asked node-admin to restart {victim}"));
            }
            Err(e) => tracing::warn!(error = %e, victim = %victim, "chaos restart refused"),
        }
    }
    tracing::info!("chaos loop exited");
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
/// Keep `state.known` equal to node-admin's `GET /api/nodes`: the UI's
/// read-only projection of what node-admin manages.
async fn known_nodes_refresher(state: AppState) {
    let Some(client) = state.admin.clone() else { return };
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        interval.tick().await;
        match client.nodes().await {
            Ok(nodes) => {
                let names: std::collections::HashSet<String> = nodes.iter().map(|n| n.name.to_string()).collect();
                state.known.retain(|k, _| names.contains(k));
                for n in nodes {
                    state.known.insert(n.name.to_string(), KnownNode::of(&n));
                }
            }
            Err(e) => tracing::info!(error = %e, "node-admin view unavailable"),
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
    #[cfg(feature = "dhat-heap")]
    {
        *DHAT_PROFILER.lock().unwrap() = Some(dhat::Profiler::builder().build());
    }
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
    // admin-ui is a node-admin client, not a node: it owns its own telemetry.
    let _telemetry = rafka_mesh_telemetry::init_telemetry("rafka-admin-ui");
    tracing::info!(panic_log = %panic_log_path.display(), "panic hook installed (pre-runtime)");
    #[cfg(feature = "dhat-heap")]
    {
        let secs: u64 = std::env::var("RAFKA_DHAT_DUMP_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(360);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            let _ = DHAT_PROFILER.lock().unwrap().take(); // drop -> writes dhat-heap.json
            eprintln!("[dhat] heap profile written to dhat-heap.json after {secs}s; exiting");
            std::process::exit(0);
        });
    }
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
        admin: std::env::var("RAFKA_NODE_ADMIN_API_BASE").ok().filter(|b| !b.trim().is_empty()).map(NodeAdminClient::new),
        known: Arc::new(DashMap::new()),
        chaos: Arc::new(ChaosController::default()),
        events: Arc::new(EventRing::default()),
        running_tests: Arc::new(DashMap::new()),
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

    let state_for_known = state.clone();
    supervise("known_nodes_refresher", move || {
        let s = state_for_known.clone();
        async move { known_nodes_refresher(s).await }
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
        .route("/api/nodes/spawned", get(handle_spawned_list))
        .route("/api/topology", get(handle_topology))
        .route("/api/alerts", get(handle_alerts))
        .route("/api/chaos/recent", get(handle_chaos_recent))
        .route("/api/chaos/timeline", get(handle_chaos_timeline))
        .route("/api/timeline", get(handle_unified_timeline))
        .route("/api/heartbeats", get(handle_heartbeats))
        .route("/api/tests", get(handle_tests))
        .route("/api/cluster/summary", get(handle_cluster_summary))
        .route("/api/chaos/start", post(handle_chaos_start))
        .route("/api/chaos/stop", post(handle_chaos_stop))
        .route("/api/chaos/state", get(handle_chaos_state))
        .route("/api/tests/run", post(handle_test_run))
        .fallback_service(ServeDir::new(&static_dir).append_index_html_on_directories(true))
        .with_state(state.clone())
        .merge(control::router(state.admin.clone(), Arc::new(TimelineEvents(state.events.clone()))))
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
        .layer(TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, std::time::Duration::from_secs(60)))
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

