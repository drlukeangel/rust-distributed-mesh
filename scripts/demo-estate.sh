#!/bin/bash
# demo-estate.sh: the R-shape consumer estate, live, by hand (no test harness): the Day-0 node-admin from
# the consumer's four executables, the Admin UI entered at its birth, and the shape (mesh1 and mesh2, each
# 2 node-admins / 2 computes / 3 gateways / 3 brokers) as one Build through the UI's own /api/bootstrap.
# Every process exports to the collector OTEL_EXPORTER_OTLP_ENDPOINT names (default: the .99 collector);
# the UI reads flames from JAEGER_QUERY_URL (default: Jaeger on .99).
#
#   scripts/demo-estate.sh up     [<root dir>]   bring it up; prints the UI's URL; the root holds the logs,
#                                                evidence, store and the binding set
#   scripts/demo-estate.sh stream [<root dir>]   the data-plane proof: five `demo/echo` streams, four across
#                                                the meshes, through the originate door (rafka-rpc-probe stream)
#   scripts/demo-estate.sh down   [<root dir>]   stop the UI and every role process of that root
#
# Inputs: RDM_DEMO_BIN (the directory holding rshape-node-admin, rshape-gateway, rshape-compute,
# rshape-broker and rafka-admin-ui, built from demo/ against this tree: `cargo build --bins` in the
# consumer workspace and `cargo build --bin rafka-admin-ui` in demo/admin-ui), RDM_DEMO_PROBE (the
# rafka-rpc-probe of this tree's build, default target/debug/rafka-rpc-probe), RDM_DEMO_WEB (the built web
# bundle, default demo/admin-ui/web/dist), RDM_DEMO_UI_BIND (default 127.0.0.1:19090).
set -euo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd)
CMD=${1:-}; ROOT=${2:-}
BIN=${RDM_DEMO_BIN:?set RDM_DEMO_BIN to the directory holding the consumer executables}
PROBE=${RDM_DEMO_PROBE:-$HERE/target/debug/rafka-rpc-probe}
WEB=${RDM_DEMO_WEB:-$HERE/demo/admin-ui/web/dist}
UI_BIND=${RDM_DEMO_UI_BIND:-127.0.0.1:19090}
export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://192.168.68.99:5318}
JAEGER=${JAEGER_QUERY_URL:-http://192.168.68.99:16687}
CURRENT=$HERE/target/demo-estate.current

up() {
  ROOT=${ROOT:-$HERE/target/demo-estate-$(date -u +%Y%m%dT%H%M%S)}
  mkdir -p "$ROOT/evidence" "$ROOT/store" "$ROOT/no-built-ins"
  echo "$ROOT" > "$CURRENT"
  local candidate; candidate=$(git -C "$HERE" rev-parse HEAD)
  # The binding set node-admin launches from (rdm_core::binding): every role kind bound to the consumer's
  # executable by sha256; nothing falls back to a built-in.
  python3 - "$BIN" "$candidate" "$ROOT/executable-bindings.json" <<'EOF'
import hashlib, json, sys
bin_dir, sha, out = sys.argv[1], sys.argv[2], sys.argv[3]
kinds = {"node_admin": "rshape-node-admin", "gateway": "rshape-gateway", "compute": "rshape-compute", "broker": "rshape-broker"}
def h(p): return hashlib.sha256(open(p, "rb").read()).hexdigest()
bindings = [{"launch_id": k, "executable": f"{bin_dir}/{f}", "sha256": h(f"{bin_dir}/{f}")} for k, f in kinds.items()]
json.dump({"candidate": {"sha": sha, "build": ""}, "launch_ids": list(kinds), "bindings": bindings}, open(out, "w"), indent=2)
EOF
  export RDM_EVIDENCE_DIR=$ROOT/evidence
  MESH_SPAWN_TYPE=process RDM_FABRIC=fabric1 RDM_MESH=mesh1 \
  RDM_DATA_DIR=$ROOT/mesh1.admin.1 RDM_STORE_DIR=$ROOT/store RDM_ESTATE_ROOT=$ROOT \
  RDM_EXECUTABLE_BINDINGS=$ROOT/executable-bindings.json RDM_EXECUTABLE_CANDIDATE=$candidate RDM_BIN_DIR=$ROOT/no-built-ins \
  RDM_NODE_ADMIN_API_BIND=127.0.0.1:0 \
    setsid nohup "$BIN/rshape-node-admin" > "$ROOT/admin1.log" 2>&1 < /dev/null &
  echo $! > "$ROOT/admin1.pid"
  local birth="" base=""
  for _ in $(seq 1 120); do
    birth=$(grep -m1 '^RDM_NODE_BIRTH=' "$ROOT/admin1.log" 2>/dev/null | cut -d= -f2- || true)
    base=$(grep -m1 '^RDM_NODE_ADMIN_API_BASE=' "$ROOT/admin1.log" 2>/dev/null | cut -d= -f2- || true)
    [ -n "$birth" ] && [ -n "$base" ] && break; sleep 0.5
  done
  [ -n "$birth" ] && [ -n "$base" ] || { echo "node-admin never printed its birth and API base; see $ROOT/admin1.log"; exit 1; }
  echo "$birth" > "$ROOT/birth.json"; echo "$base" > "$ROOT/api_base"
  echo "day-0 admin up: $base (pid $(cat "$ROOT/admin1.pid"))"
  RDM_NODE_ADMIN_BIRTH="$birth" RDM_ADMIN_UI_BIND_ADDR=$UI_BIND RDM_UI_STATIC_DIR=$WEB JAEGER_QUERY_URL=$JAEGER \
    setsid nohup "$BIN/rafka-admin-ui" > "$ROOT/ui.log" 2>&1 < /dev/null &
  echo $! > "$ROOT/ui.pid"
  for _ in $(seq 1 60); do curl -sf "http://$UI_BIND/api/health" >/dev/null 2>&1 && break; sleep 0.5; done
  curl -sf "http://$UI_BIND/api/health" >/dev/null || { echo "the UI never answered /api/health; see $ROOT/ui.log"; exit 1; }
  echo "UI up: http://$UI_BIND"
  for _ in $(seq 1 120); do
    curl -s "http://$UI_BIND/api/overview" 2>/dev/null | grep -q '"ready-for-traffic"' && break; sleep 0.5
  done
  local shape='{"meshes":[{"name":"mesh1","node_admin":2,"compute":2,"gateway":3,"broker":3},{"name":"mesh2","node_admin":2,"compute":2,"gateway":3,"broker":3}]}'
  local acc; acc=$(curl -s -X POST "http://$UI_BIND/api/bootstrap" -H 'content-type: application/json' -d "$shape")
  echo "bootstrap: $acc"; echo "$acc" > "$ROOT/bootstrap.json"
  local t0 ready total; t0=$(date +%s)
  for _ in $(seq 1 600); do
    read -r ready total < <(curl -s "http://$UI_BIND/api/topology" 2>/dev/null | python3 -c 'import json,sys
try:
    n=json.load(sys.stdin).get("nodes",[]); print(sum(1 for x in n if x.get("status")=="ready-for-traffic"), len(n))
except Exception: print(0, 0)')
    [ "$ready" = 20 ] && break; sleep 1
  done
  echo "topology after $(( $(date +%s) - t0 ))s: $ready of $total ready-for-traffic"
  echo "ESTATE UP at $ROOT; UI http://$UI_BIND"
}

stream() {
  ROOT=${ROOT:-$(cat "$CURRENT")}
  export RDM_EVIDENCE_DIR=$ROOT/evidence
  local birth; birth=$(cat "$ROOT/birth.json")
  one() {
    local out; out=$("$PROBE" --admin-birth "$birth" stream --target "path:$1" --destination "path:$2" --value "$3" 2>/dev/null)
    echo "$1 -> $2: $(echo "$out" | python3 -c 'import sys,json
d=json.load(sys.stdin); print({k: d.get(k) for k in ("outcome","streamed","kind","peer","answered","reason","refused")}, "trace", (d.get("traceparent") or "")[3:35])')"
  }
  one mesh1.compute.1 mesh2.broker.1  "hello from mesh1.compute.1"
  one mesh1.compute.1 mesh2.gateway.2 "hello again from mesh1.compute.1"
  one mesh2.compute.2 mesh1.broker.3  "hello from mesh2.compute.2"
  one mesh2.gateway.1 mesh1.compute.2 "hello from mesh2.gateway.1"
  one mesh1.gateway.1 mesh1.broker.2  "same mesh, gateway to broker"
}

down() {
  ROOT=${ROOT:-$(cat "$CURRENT")}
  [ -f "$ROOT/ui.pid" ] && kill "$(cat "$ROOT/ui.pid")" 2>/dev/null || true
  # The launched nodes carry none of the Day-0 admin's environment: they are found by their executable.
  pkill -f "$BIN/rshape-" 2>/dev/null || true
  for _ in $(seq 1 20); do pgrep -f "$BIN/rshape-" >/dev/null || break; sleep 0.5; done
  pkill -9 -f "$BIN/rshape-" 2>/dev/null || true
  echo "estate at $ROOT stopped; left: $(pgrep -fc "$BIN/rshape-" || true)"
}

case "$CMD" in up) up ;; stream) stream ;; down) down ;; *) echo "usage: $0 up|stream|down [<root dir>]" >&2; exit 2 ;; esac
