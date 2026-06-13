#!/bin/bash
# Two-admin cold-start chaos soak (new topology model: mesh1 + mesh2, shared CA).
# Replaces stale soak30.sh (which assumed old single-admin ports 19100/19101).
#
# Usage: bash soak.sh
# Runs for ~14 min total: cold-start + 25s convergence + chaos on both admins
# + 7 monitoring cycles (every ~2 min).
#
# Directories:
#   ADMIN1_DATA = ./data-soak1   (admin-1 = mesh1)
#   ADMIN2_DATA = ./data-soak1-mesh2  (admin-2 = mesh2; auto-derived by bootstrap-2mesh)
#   ADMIN1_LOG  = $ADMIN1_DATA/admin.log  (admin-1 stdout; worker-mesh1 output goes here too)
#   ADMIN2_LOG  = $ADMIN2_DATA/admin.log  (admin-2 stdout; worker-mesh2 output goes here too — written by bootstrap-2mesh)
#
# Ports:
#   admin-1: HTTP=19090, iroh=14819, spawn-base=15820
#   admin-2: HTTP=19091, iroh=14919, spawn-base=16820  (all derived +1/+100/+1000 by bootstrap-2mesh)

set -e

SOAK_LOG=./soak-new.log
ADMIN1_DATA="./data-soak1"
ADMIN_EXE="./target/debug/rafka-admin-ui.exe"

: > "$SOAK_LOG"
log() { echo "$*" | tee -a "$SOAK_LOG"; }
log "===== SOAK START $(date -u +%Y-%m-%dT%H:%M:%SZ) ====="

# ---- 0. Kill any stale rafka processes ----
log ""
log "[pre-flight] killing any stale rafka processes..."
powershell.exe -NoProfile -Command "Get-Process | Where-Object {\$_.ProcessName -like 'rafka*'} | Stop-Process -Force -ErrorAction SilentlyContinue" 2>/dev/null || true
sleep 3

# ---- 1. Clean old soak data dirs ----
log "[pre-flight] removing old soak data dirs..."
rm -rf "$ADMIN1_DATA" "${ADMIN1_DATA}-mesh2" 2>/dev/null || true
mkdir -p "$ADMIN1_DATA"

# ---- 2. Launch admin-1 ----
log "[cold-start] launching admin-1 (mesh1) on http://127.0.0.1:19090 ..."
RAFKA_MESH_ID=mesh1 \
RAFKA_DATA_DIR="$ADMIN1_DATA" \
RAFKA_ADMIN_UI_BIND_ADDR=127.0.0.1:19090 \
RAFKA_NODE_BIND_ADDR=127.0.0.1:14819 \
RAFKA_SPAWN_PORT_BASE=15820 \
RAFKA_CHILD_BUILD_PROFILE=debug \
CARGO_TARGET_DIR=./target \
RUST_LOG=info \
  "$ADMIN_EXE" >> "$ADMIN1_DATA/admin.log" 2>&1 &
ADMIN1_PID=$!
log "[cold-start] admin-1 PID=$ADMIN1_PID"

# Wait for admin-1 HTTP to come up (up to 20s)
log "[cold-start] waiting for admin-1 HTTP..."
for i in $(seq 1 40); do
  if curl -s http://127.0.0.1:19090/api/health >/dev/null 2>&1; then
    log "[cold-start] admin-1 up after ~${i}x0.5s"
    break
  fi
  sleep 0.5
done

# Confirm admin-1 is actually up
if ! curl -s http://127.0.0.1:19090/api/health >/dev/null 2>&1; then
  log "[FATAL] admin-1 did not come up within 20s — aborting"
  log "admin-1 log tail:"
  tail -20 "$ADMIN1_DATA/admin.log" >> "$SOAK_LOG" 2>/dev/null || true
  exit 1
fi
log "[cold-start] admin-1 health OK"

# ---- 3. POST bootstrap-2mesh (launches admin-2 + fills both meshes) ----
log "[cold-start] POST bootstrap-2mesh → launches admin-2 (mesh2) + 16 workers..."
BOOTSTRAP_RESP=$(curl -s -X POST http://127.0.0.1:19090/api/bootstrap-2mesh)
log "[cold-start] bootstrap-2mesh response: $BOOTSTRAP_RESP"

TOTAL=$(echo "$BOOTSTRAP_RESP" | python -c "import sys,json; d=json.load(sys.stdin); print(d.get('total_nodes',0))" 2>/dev/null || echo "?")
SHARED=$(echo "$BOOTSTRAP_RESP" | python -c "import sys,json; d=json.load(sys.stdin); print(d.get('shared_root','?'))" 2>/dev/null || echo "?")
log "[cold-start] total_nodes=$TOTAL shared_root=$SHARED"

# Confirm admin-2 is up
if ! curl -s http://127.0.0.1:19091/api/health >/dev/null 2>&1; then
  log "[FATAL] admin-2 did not come up — aborting"
  exit 1
fi
log "[cold-start] admin-2 health OK (HTTP 19091)"

# ---- 4. Wait for topology convergence (~25s) ----
log "[cold-start] waiting 25s for backbone convergence..."
sleep 25

# ---- 5. Start chaos on BOTH admins ----
log "[chaos] starting chaos on admin-1 (mesh1)..."
C1_RESP=$(curl -s -X POST http://127.0.0.1:19090/api/chaos/start)
log "[chaos] admin-1 chaos/start: $C1_RESP"

log "[chaos] starting chaos on admin-2 (mesh2)..."
C2_RESP=$(curl -s -X POST http://127.0.0.1:19091/api/chaos/start)
log "[chaos] admin-2 chaos/start: $C2_RESP"

# ---- 6. Snapshot function ----
snap() {
  local mark=$1
  {
    echo ""
    echo "================= T+${mark} min  $(date -u +%H:%M:%SZ) ================="

    # Per-mesh node counts from /api/topology (mesh_id field)
    for port in 19090 19091; do
      local label="admin-$([ "$port" = "19090" ] && echo 1 || echo 2)/$([ "$port" = "19090" ] && echo mesh1 || echo mesh2)"
      curl -s "http://127.0.0.1:$port/api/topology" 2>/dev/null | python -c "
import sys, json, collections
try:
    d = json.load(sys.stdin)
except Exception as e:
    print('  [$label] topology parse error: ' + str(e))
    sys.exit()
nodes = d.get('nodes', [])
by_mesh = collections.defaultdict(list)
for n in nodes:
    by_mesh[n.get('mesh_id','?')].append(n)
print('  [$label] total=%d nodes across %d meshes' % (len(nodes), len(by_mesh)))
for mesh_id in sorted(by_mesh.keys()):
    mns = by_mesh[mesh_id]
    by_type = collections.Counter(n.get('type','?') for n in mns)
    print('    mesh=%s  count=%d  types=%s' % (mesh_id, len(mns), dict(by_type)))
" 2>/dev/null || echo "  [$label] (no response)"
    done

    # Chaos state on both admins
    for port in 19090 19091; do
      local alabel="admin-$([ "$port" = "19090" ] && echo 1 || echo 2)"
      curl -s "http://127.0.0.1:$port/api/chaos/state" 2>/dev/null | python -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print('  [$alabel] chaos: running=%s  events=%s  cadence_ms=%s' % (
        d.get('running'), d.get('total_events'), d.get('cadence_ms')))
except:
    print('  [$alabel] chaos state unavailable')
" 2>/dev/null || echo "  [$alabel] (no response)"
    done

    # OS process count, RAM, and CPU
    powershell.exe -NoProfile -Command "\$p=Get-Process | Where-Object {\$_.ProcessName -like 'rafka*'}; \$p | Select-Object ProcessName, Id, @{Name='CPU_s';Expression={[math]::Round(\$_.CPU, 1)}}, @{Name='CPU_frac';Expression={[math]::Round(\$_.CPU / ((Get-Date) - \$_.StartTime).TotalSeconds, 2)}}, @{Name='RAM_MB';Expression={[math]::Round(\$_.WorkingSet64 / 1MB, 1)}} | Format-Table -AutoSize; '  OS: {0} rafka procs  total_RSS={1:N0}MB  total_CPU={2:N1}s' -f \$p.Count, ((\$p | Measure-Object WorkingSet64 -Sum).Sum / 1MB), ((\$p | Measure-Object CPU -Sum).Sum)" 2>/dev/null

    # Recent chaos events from admin-1 local timeline (instant, no Jaeger)
    curl -s "http://127.0.0.1:19090/api/timeline" 2>/dev/null | python -c "
import sys, json
try:
    d = json.load(sys.stdin)
    evts = [e for e in d.get('events',[]) if 'chaos' in e.get('kind','')]
    print('  admin-1 chaos events in timeline: %d' % len(evts))
    for e in evts[-5:]:
        print('    ' + str(e.get('kind')) + ' ' + str(e.get('node_name','')) + ' mesh=' + str(e.get('mesh_id','')))
except:
    pass
" 2>/dev/null

    curl -s "http://127.0.0.1:19091/api/timeline" 2>/dev/null | python -c "
import sys, json
try:
    d = json.load(sys.stdin)
    evts = [e for e in d.get('events',[]) if 'chaos' in e.get('kind','')]
    print('  admin-2 chaos events in timeline: %d' % len(evts))
except:
    pass
" 2>/dev/null

    # Panic check on admin logs + admin-2 log
    local ADMIN2_DATA="${ADMIN1_DATA}-mesh2"
    local p1=$(grep -aiE 'thread .* panicked|FATAL' "$ADMIN1_DATA/admin.log" 2>/dev/null | wc -l | tr -d ' ')
    local p2=$(grep -aiE 'thread .* panicked|FATAL' "$ADMIN2_DATA/admin.log" 2>/dev/null | wc -l | tr -d ' ')
    echo "  panics: admin-1=$p1  admin-2=$p2"
    # node evictions in admin-1 log
    local ev1=$(grep -aic 'node evicted' "$ADMIN1_DATA/admin.log" 2>/dev/null | tr -d ' ')
    local ev2=$(grep -aic 'node evicted' "$ADMIN2_DATA/admin.log" 2>/dev/null | tr -d ' ')
    echo "  evictions: admin-1=$ev1  admin-2=$ev2"
  } | tee -a "$SOAK_LOG"
}

# ---- 7. Refill function (top-up to 2-of-each per mesh) ----
refill() {
  curl -s -X POST http://127.0.0.1:19090/api/bootstrap >/dev/null 2>&1 || true
  curl -s -X POST http://127.0.0.1:19091/api/bootstrap >/dev/null 2>&1 || true
}

# ---- 8. Monitoring loop: 7 cycles of 2 min each ----
# T+0 snapshot first (after chaos arms)
snap "0"
# Then cycles every ~4 min
for mark in 4 8 12 16 20 24 28 32; do
  refill
  sleep 240
  snap "$mark"
done

# ---- 9. Final state ----
log ""
log "===== SOAK COMPLETE $(date -u +%Y-%m-%dT%H:%M:%SZ) ====="
log "Final chaos state:"
log "  admin-1: $(curl -s http://127.0.0.1:19090/api/chaos/state 2>/dev/null)"
log "  admin-2: $(curl -s http://127.0.0.1:19091/api/chaos/state 2>/dev/null)"
log "Final topology:"
log "  admin-1 nodes: $(curl -s http://127.0.0.1:19090/api/topology 2>/dev/null | python -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('nodes',[])))" 2>/dev/null)"
log "  admin-2 nodes: $(curl -s http://127.0.0.1:19091/api/topology 2>/dev/null | python -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('nodes',[])))" 2>/dev/null)"
log ""
log "Stopping chaos on both admins..."
curl -s -X POST http://127.0.0.1:19090/api/chaos/stop >/dev/null 2>&1 || true
curl -s -X POST http://127.0.0.1:19091/api/chaos/stop >/dev/null 2>&1 || true
log "Soak done. Log: $SOAK_LOG"
