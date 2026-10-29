#!/bin/bash
# demo-estate.sh: the R-shape consumer estate, live, by hand (no test harness): the Day-0 node-admin from
# the consumer's four executables, the Admin UI entered at its birth, and the shape (two meshes, each
# 2 node-admins / 2 computes / 3 gateways / 3 brokers) as one Build through the UI's own /api/bootstrap.
# Every `up` names a new fabric and new meshes (fabric-<tag>, mesh-<tag>-1, mesh-<tag>-2, the tag minted
# per run), recorded in <root>/names, so no run reuses another's names; Day 0 is the fabric-primary and
# its own mesh, and the second mesh comes from the bootstrap Build (Q170).
# Every process exports to the collector OTEL_EXPORTER_OTLP_ENDPOINT names (default: the local collector,
# http://localhost:4318, as every node binary defaults to);
# the UI reads flames from JAEGER_QUERY_URL (default: the local Jaeger that collector feeds,
# http://localhost:16686, the admin UI's own default).
#
#   scripts/demo-estate.sh up     [<root dir>]   bring it up; prints the UI's URL; the root holds the logs,
#                                                evidence and store
#   scripts/demo-estate.sh stream [<root dir>]   the data-plane proof: five `demo/echo` streams, four across
#                                                the meshes, through the originate door (rdm-rpc-probe stream)
#   scripts/demo-estate.sh example [<root dir>]  the consumer guide's compiled example (demo/src/consumer_example.rs):
#                                                <mesh 2>.broker.3 and <mesh 2>.compute.1 replaced by a Build onto
#                                                $BIN/rshape-example; the compute submits an operation to a broker
#                                                it selects from the topology every 10 s and copies its record
#   scripts/demo-estate.sh example-crash [<root dir>]  the example broker's process ended by SIGKILL while it holds
#                                                pending work: node recovery starts it again over its data dir,
#                                                and its pre_node_ready resumes the work
#   scripts/demo-estate.sh down   [<root dir>]   the fabric's teardown (fabric-delete.md): fabric.delete at the
#                                                Day-0 admin, then the fabric-primary's own node.delete from
#                                                outside (rdm-rpc-probe fabric-delete, node-delete), its
#                                                runtime gone inside the stop's bound; then the UI, and any
#                                                role process still running is named and stopped
#
# Inputs: RDM_DEMO_BIN (the directory holding rshape-node-admin, rshape-gateway, rshape-compute,
# rshape-broker, rdm-admin-ui and, for `example`, rshape-example, built from demo/ against this
# tree: `cargo build --bins` in the consumer workspace and `cargo build --bin rdm-admin-ui` in
# demo/admin-ui; scripts/i143-rshape-build-consumer.sh's --bin-dir holds them all), RDM_DEMO_PROBE (the
# rdm-rpc-probe of this tree's build, default target/debug/rdm-rpc-probe), RDM_DEMO_WEB (the built web
# bundle, default demo/admin-ui/web/dist), RDM_DEMO_UI_BIND (default 127.0.0.1:0: the operating system picks
# the UI's port, and the UI puts it on the Day-0 admin's row config as admin_ui_port, where `up` reads it).
set -euo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd)
CMD=${1:-}; ROOT=${2:-}
BIN=${RDM_DEMO_BIN:?set RDM_DEMO_BIN to the directory holding the consumer executables}
PROBE=${RDM_DEMO_PROBE:-$HERE/target/debug/rdm-rpc-probe}
WEB=${RDM_DEMO_WEB:-$HERE/demo/admin-ui/web/dist}
UI_BIND=${RDM_DEMO_UI_BIND:-127.0.0.1:0}
export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4318}
JAEGER=${JAEGER_QUERY_URL:-http://localhost:16686}
CURRENT=$HERE/target/demo-estate.current

# The port the UI put on `$1`'s row config (admin_ui_port): the row's latest line in the store root's nodes.jsonl.
ui_port() {
  python3 - "$ROOT/store" "$1" <<'PY'
import glob, json, sys
store, name = sys.argv[1], sys.argv[2]
port = ""
for f in glob.glob(f"{store}/*/nodes.jsonl"):
    for line in open(f):
        try:
            r = json.loads(line)["record"]
        except Exception:
            continue
        if r.get("name") == name:
            port = (r.get("config") or {}).get("admin_ui_port", port)
print(port)
PY
}

up() {
  ROOT=${ROOT:-$HERE/target/demo-estate-$(date -u +%Y%m%dT%H%M%S)}
  mkdir -p "$ROOT/evidence" "$ROOT/store"
  echo "$ROOT" > "$CURRENT"
  local tag; tag=$(head -c 64 /dev/urandom | tr -dc 'a-z0-9' | head -c 6)
  FABRIC=fabric-$tag; M1=mesh-$tag-1; M2=mesh-$tag-2
  printf 'FABRIC=%s\nM1=%s\nM2=%s\n' "$FABRIC" "$M1" "$M2" > "$ROOT/names"
  echo "fabric $FABRIC, meshes $M1 and $M2"
  # The Day-0 node-admin is the consumer's and records what it runs; every other node is created by a Build
  # the UI submits naming each path's executable (the consumer's, beside the UI in $BIN, by sha256).
  # RDM_BIN_DIR is where the node-admin itself is found, never a launch source.
  export RDM_EVIDENCE_DIR=$ROOT/evidence
  MESH_SPAWN_TYPE=process RDM_FABRIC=$FABRIC RDM_MESH=$M1 \
  RDM_DATA_DIR=$ROOT/$M1.admin.1 RDM_STORE_DIR=$ROOT/store RDM_ESTATE_ROOT=$ROOT \
  RDM_BIN_DIR=$BIN \
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
    setsid nohup "$BIN/rdm-admin-ui" > "$ROOT/ui.log" 2>&1 < /dev/null &
  echo $! > "$ROOT/ui.pid"
  # The UI's port is the one it bound, as its admin's row config holds it (admin_ui_port).
  local port=""
  for _ in $(seq 1 120); do
    port=$(ui_port "$M1.admin.1")
    [ -n "$port" ] && curl -sf "http://${UI_BIND%:*}:$port/api/health" >/dev/null 2>&1 && break; sleep 0.5
  done
  [ -n "$port" ] || { echo "the UI's port is not on $M1.admin.1's row config; see $ROOT/ui.log"; exit 1; }
  UI_BIND=${UI_BIND%:*}:$port; echo "$UI_BIND" > "$ROOT/ui_bind"
  echo "UI up: http://$UI_BIND"
  for _ in $(seq 1 120); do
    curl -s "http://$UI_BIND/api/overview" 2>/dev/null | grep -q '"ready"' && break; sleep 0.5
  done
  local shape="{\"meshes\":[{\"name\":\"$M1\",\"node_admin\":2,\"compute\":2,\"gateway\":3,\"broker\":3},{\"name\":\"$M2\",\"node_admin\":2,\"compute\":2,\"gateway\":3,\"broker\":3}]}"
  # The Day-0 Build is Complete from its own ready, one Build at a time: while the admin
  # refuses the bootstrap build-in-progress naming it, the bootstrap is submitted again once it is.
  local acc
  for _ in $(seq 1 120); do
    acc=$(curl -s -X POST "http://$UI_BIND/api/bootstrap" -H 'content-type: application/json' -d "$shape")
    echo "$acc" | grep -q '"build-in-progress"' || break; sleep 1
  done
  echo "bootstrap: $acc"; echo "$acc" > "$ROOT/bootstrap.json"
  local t0 ready total; t0=$(date +%s)
  for _ in $(seq 1 600); do
    read -r ready total < <(curl -s "http://$UI_BIND/api/topology" 2>/dev/null | python3 -c 'import json,sys
try:
    n=json.load(sys.stdin).get("nodes",[]); print(sum(1 for x in n if x.get("status")=="ready"), len(n))
except Exception: print(0, 0)')
    [ "$ready" = 20 ] && break; sleep 1
  done
  echo "topology after $(( $(date +%s) - t0 ))s: $ready of $total ready"
  echo "ESTATE UP at $ROOT; UI http://$UI_BIND"
}

stream() {
  ROOT=${ROOT:-$(cat "$CURRENT")}
  export RDM_EVIDENCE_DIR=$ROOT/evidence
  local birth; birth=$(cat "$ROOT/birth.json")
  # shellcheck disable=SC1091
  . "$ROOT/names"
  one() {
    local out; out=$("$PROBE" --admin-birth "$birth" stream --target "path:$1" --destination "path:$2" --value "$3" 2>/dev/null)
    echo "$1 -> $2: $(echo "$out" | python3 -c 'import sys,json
d=json.load(sys.stdin); print({k: d.get(k) for k in ("outcome","streamed","kind","peer","answered","reason","refused")}, "trace", (d.get("traceparent") or "")[3:35])')"
  }
  one "$M1.compute.1" "$M2.broker.1"  "hello from $M1.compute.1"
  one "$M1.compute.1" "$M2.gateway.2" "hello again from $M1.compute.1"
  one "$M2.compute.2" "$M1.broker.3"  "hello from $M2.compute.2"
  one "$M2.gateway.1" "$M1.compute.2" "hello from $M2.gateway.1"
  one "$M1.gateway.1" "$M1.broker.2"  "same mesh, gateway to broker"
}

# Replace each named node by a Build onto $BIN/rshape-example, one Build at a time.
example() {
  ROOT=${ROOT:-$(cat "$CURRENT")}
  # shellcheck disable=SC1091
  . "$ROOT/names"
  local ui acc n; ui=$(cat "$ROOT/ui_bind")
  for n in "$M2.broker.3" "$M2.compute.1"; do
    for _ in $(seq 1 120); do
      acc=$(curl -s -X POST "http://$ui/api/nodes/$n/replace" -H 'content-type: application/json' -d "{\"executable\":\"$BIN/rshape-example\"}")
      echo "$acc" | grep -q '"build-in-progress"' || break; sleep 1
    done
    echo "replace $n onto rshape-example: $acc"
  done
  echo "the example's lines (target rshape) are in each node's stderr.log under $ROOT/<path.name>-<node id>/"
}

# The example broker's process, found by the path.name in its environment, ended by SIGKILL.
example_crash() {
  ROOT=${ROOT:-$(cat "$CURRENT")}
  # shellcheck disable=SC1091
  . "$ROOT/names"
  local p
  for p in $(pgrep -f "$BIN/rshape-example" || true); do
    if tr '\0' '\n' < "/proc/$p/environ" 2>/dev/null | grep -qx "RDM_NODE_NAME=$M2.broker.3"; then
      kill -9 "$p" && echo "$M2.broker.3 (pid $p) ended by SIGKILL at $(date -u +%H:%M:%S.%N)"; return
    fi
  done
  echo "no rshape-example process holds $M2.broker.3"; return 1
}

down() {
  ROOT=${ROOT:-$(cat "$CURRENT")}
  export RDM_EVIDENCE_DIR=$ROOT/evidence
  # The teardown the pages name: fabric.delete reported by the fabric-primary alive, then its own
  # node.delete from outside, whose node.deleted is its last reply (fabric-delete.md steps 1-4). The
  # Day-0 admin is the fabric-primary the probe enters at.
  if [ -f "$ROOT/birth.json" ]; then
    local birth fp fp_pid out; birth=$(cat "$ROOT/birth.json")
    fp=$(echo "$birth" | python3 -c 'import json,sys; print(json.load(sys.stdin)["name"])')
    fp_pid=$(cat "$ROOT/admin1.pid" 2>/dev/null || true)
    out=$("$PROBE" --admin-birth "$birth" fabric-delete --target "path:$fp" 2>/dev/null || true)
    echo "fabric.delete: $(echo "$out" | python3 -c 'import sys,json
try: d=json.loads(sys.stdin.read()); print({k: d.get(k) for k in ("outcome","terminal","build_id","elapsed_ms","reason")})
except Exception as e: print("no answer", e)')"
    out=$("$PROBE" --admin-birth "$birth" node-delete --target "path:$fp" 2>/dev/null || true)
    echo "node.delete $fp: $(echo "$out" | python3 -c 'import sys,json
try: d=json.loads(sys.stdin.read()); print({k: d.get(k) for k in ("outcome","deleted","elapsed_ms","reason")})
except Exception as e: print("no answer", e)')"
    # The stop's bound: RDM_CONNECTIONS_DELETE_DEADLINE_MS (5 s) and the 10 s an executor allows a stop's reply past it.
    if [ -n "$fp_pid" ]; then
      for _ in $(seq 1 30); do kill -0 "$fp_pid" 2>/dev/null || break; sleep 0.5; done
      if kill -0 "$fp_pid" 2>/dev/null; then echo "the fabric-primary $fp (pid $fp_pid) still runs 15 s after its node.delete"; else echo "the fabric-primary $fp (pid $fp_pid) ended"; fi
    fi
  fi
  [ -f "$ROOT/ui.pid" ] && kill "$(cat "$ROOT/ui.pid")" 2>/dev/null || true
  local left; left=$(pgrep -fa "$BIN/rshape-" || true)
  [ -n "$left" ] && echo "still running after the teardown, stopped by signal: $left"
  # The launched nodes carry none of the Day-0 admin's environment: they are found by their executable.
  pkill -f "$BIN/rshape-" 2>/dev/null || true
  for _ in $(seq 1 20); do pgrep -f "$BIN/rshape-" >/dev/null || break; sleep 0.5; done
  pkill -9 -f "$BIN/rshape-" 2>/dev/null || true
  echo "estate at $ROOT stopped; left: $(pgrep -fc "$BIN/rshape-" || true)"
}

case "$CMD" in up) up ;; stream) stream ;; example) example ;; example-crash) example_crash ;; down) down ;; *) echo "usage: $0 up|stream|example|example-crash|down [<root dir>]" >&2; exit 2 ;; esac
