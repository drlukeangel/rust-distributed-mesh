#!/usr/bin/env bash
# demo/ui.sh: (re)start only the admin UI against the estate demo/up.sh birthed. The estate keeps running.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
HOME_DIR=${RSHAPE_DEMO_HOME:-$ROOT/target/rshape-demo}
export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://192.168.68.99:5317}
# NO_OTLP=1 runs the estate with evidence files only (no collector traffic): the Boot Waterfall then has no trace.
[ -n "${NO_OTLP:-}" ] && unset OTEL_EXPORTER_OTLP_ENDPOINT
export JAEGER_QUERY_URL=${JAEGER_QUERY_URL:-http://192.168.68.99:16687}
BIND=${RDM_ADMIN_UI_BIND_ADDR:-0.0.0.0:19090}
[ -f "$HOME_DIR/estate.json" ] || { echo "[demo/ui] REFUSED: no estate.json in $HOME_DIR; run demo/up.sh" >&2; exit 1; }
if [ -f "$HOME_DIR/admin-ui.pid" ]; then
    pid=$(cat "$HOME_DIR/admin-ui.pid")
    if kill -0 "$pid" 2>/dev/null; then kill -TERM "$pid"; for _ in $(seq 1 10); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done; kill -0 "$pid" 2>/dev/null && kill -KILL "$pid"; fi
    rm -f "$HOME_DIR/admin-ui.pid"
fi
ADMIN=$(jq -r .node_admin_api_base "$HOME_DIR/estate.json")
echo "[demo/ui] starting the admin UI on $BIND against $ADMIN"
RDM_CPU_ALERT_THRESHOLD=${RDM_CPU_ALERT_THRESHOLD:-1.0} RDM_NODE_ADMIN_API_BASE="$ADMIN" RDM_EVIDENCE_DIR=$(jq -r .evidence_dir "$HOME_DIR/estate.json") RDM_ESTATE_ROOT=$(jq -r .estate_root "$HOME_DIR/estate.json") RSHAPE_DEMO_STATE="$HOME_DIR" \
RDM_ADMIN_UI_BIND_ADDR="$BIND" RDM_UI_STATIC_DIR="$ROOT/demo/admin-ui/web/dist" \
    setsid nohup "$ROOT/target/i143-rshape/consumer-bin/rafka-admin-ui" > "$HOME_DIR/admin-ui.log" 2>&1 < /dev/null &
echo $! > "$HOME_DIR/admin-ui.pid"
for _ in $(seq 1 30); do curl -fsS --max-time 2 "http://127.0.0.1:${BIND##*:}/api/health" > /dev/null 2>&1 && break; sleep 1; done
curl -fsS --max-time 2 "http://127.0.0.1:${BIND##*:}/api/health" > /dev/null || { echo "[demo/ui] REFUSED: the UI did not answer on $BIND (see $HOME_DIR/admin-ui.log)" >&2; exit 1; }
echo "[demo/ui] up, pid $(cat "$HOME_DIR/admin-ui.pid")"
