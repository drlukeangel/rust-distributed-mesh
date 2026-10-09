#!/usr/bin/env bash
# demo/up.sh: bring up the canonical R-shape and its admin UI, in one command.
#
#   demo/up.sh                 builds the consumer at this checkout's HEAD (which must be pushed),
#                              births the estate, starts the traffic driver and the UI
#   CANDIDATE_SHA=<40-hex> demo/up.sh    builds the consumer at another pushed commit
#
# What it starts (state under $RSHAPE_DEMO_HOME, default target/rshape-demo):
#   rshape-demo   births two meshes x {node_admin 2, gateway 3, broker 3, compute 2} = 20 nodes on the
#                 consumer's four executables (one Build executed by node-admin), then drives R-shape
#                 traffic until stopped: puts to brokers by exact node, half carried through a gateway
#                 of the same or the other mesh. Pid file: $HOME_DIR/rshape-demo.pid
#   rafka-admin-ui  the UI on $RDM_ADMIN_UI_BIND_ADDR (default 0.0.0.0:19090), reading and driving the
#                 estate's node-admin; OTLP to $OTEL_EXPORTER_OTLP_ENDPOINT, trace links to Jaeger
#                 at $JAEGER_QUERY_URL. Pid file: $HOME_DIR/admin-ui.pid
# demo/down.sh stops both by pid.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
unset CARGO_TARGET_DIR
HOME_DIR=${RSHAPE_DEMO_HOME:-$ROOT/target/rshape-demo}
export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://192.168.68.99:5317}
# NO_OTLP=1 runs the estate with evidence files only (no collector traffic): the Boot Waterfall then has no trace.
[ -n "${NO_OTLP:-}" ] && unset OTEL_EXPORTER_OTLP_ENDPOINT
export JAEGER_QUERY_URL=${JAEGER_QUERY_URL:-http://192.168.68.99:16687}
BIND=${RDM_ADMIN_UI_BIND_ADDR:-0.0.0.0:19090}
say() { echo "[demo/up] $*"; }
die() { echo "[demo/up] REFUSED: $*" >&2; exit 1; }

SHA=${CANDIDATE_SHA:-$(git rev-parse HEAD)}
echo "$SHA" | grep -qE '^[0-9a-f]{40}$' || die "candidate '$SHA' is not a 40-hex commit"
git fetch -q origin || die "git fetch origin failed"
[ -n "$(git branch -r --contains "$SHA" 2>/dev/null)" ] || die "commit $SHA is not on any pushed branch; the consumer imports RDM by git rev, so push first"

[ -f "$HOME_DIR/rshape-demo.pid" ] && "$ROOT/demo/down.sh" || true
mkdir -p "$HOME_DIR"

say "building the consumer at $SHA"
bash scripts/i143-rshape-build-consumer.sh --candidate-sha "$SHA" --workspace target/i143-rshape/consumer-workspace \
    --bin-dir target/i143-rshape/consumer-bin --output target/i143-rshape/consumer-build
say "building the estate driver and the probe"
cargo build -q -p rafka-test-scenario --bin rshape-demo
cargo build -q -p rafka-node-rpc-testkit --bin rafka-rpc-probe
if [ ! -d demo/admin-ui/web/dist ] || [ -n "$(find demo/admin-ui/web/src -newer demo/admin-ui/web/dist/index.html -type f | head -1)" ]; then
    say "building the web app"
    (cd demo/admin-ui/web && { [ -d node_modules ] || npm ci --silent; } && npm run build --silent)
fi

# The estate launches every node from copies it owns: a later consumer build (demo/ui.sh after a UI change)
# replaces target/i143-rshape/consumer-bin, and node-admin refuses a launch whose bytes differ from the bound hash.
rm -rf "$HOME_DIR/bin" && mkdir -p "$HOME_DIR/bin"
cp target/i143-rshape/consumer-bin/rshape-* "$HOME_DIR/bin/"
cp target/i143-rshape/consumer-build/manifest.json "$HOME_DIR/manifest.json"
rm -f "$HOME_DIR/estate.json" "$HOME_DIR/traffic.json"
say "birthing the canonical R-shape (20 nodes) and starting traffic"
# A tighter staleness floor and gossip cadence than the defaults, so a cut shows within seconds.
RDM_STALENESS_MS=${RDM_STALENESS_MS:-6000} RDM_GOSSIP_INTERVAL_MS=${RDM_GOSSIP_INTERVAL_MS:-2000} RDM_BACKBONE_INTERVAL_MS=${RDM_BACKBONE_INTERVAL_MS:-2000} \
RDM_RSHAPE_CONSUMER_BIN_DIR="$HOME_DIR/bin" RSHAPE_MANIFEST="$HOME_DIR/manifest.json" \
RSHAPE_DEMO_STATE="$HOME_DIR" RDM_ARTIFACTS_DIR="$HOME_DIR/artifacts" MESH_SPAWN_TYPE=process \
    setsid nohup "$ROOT/target/debug/rshape-demo" > "$HOME_DIR/rshape-demo.log" 2>&1 < /dev/null &
echo $! > "$HOME_DIR/rshape-demo.pid"
for _ in $(seq 1 600); do
    [ -f "$HOME_DIR/estate.json" ] && break
    kill -0 "$(cat "$HOME_DIR/rshape-demo.pid")" 2>/dev/null || { tail -20 "$HOME_DIR/rshape-demo.log" >&2; die "rshape-demo exited before the estate was ready"; }
    sleep 1
done
[ -f "$HOME_DIR/estate.json" ] || die "the estate was not ready within 600 s (see $HOME_DIR/rshape-demo.log)"
ADMIN=$(jq -r .node_admin_api_base "$HOME_DIR/estate.json")
EVIDENCE=$(jq -r .evidence_dir "$HOME_DIR/estate.json")
ESTATE_ROOT=$(jq -r .estate_root "$HOME_DIR/estate.json")

"$ROOT/demo/ui.sh"
say "ready: UI http://$(hostname -I | awk '{print $1}'):${BIND##*:}  node-admin $ADMIN  candidate $SHA"
say "pids: rshape-demo $(cat "$HOME_DIR/rshape-demo.pid"), admin-ui $(cat "$HOME_DIR/admin-ui.pid"); stop with demo/down.sh"
