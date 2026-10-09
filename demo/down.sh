#!/usr/bin/env bash
# demo/down.sh: stop what demo/up.sh started, by pid. The UI first, then rshape-demo, which stops the
# traffic and shuts the estate down through the fabric-primary; the estate's reaper removes anything
# that outlives it.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
HOME_DIR=${RSHAPE_DEMO_HOME:-$ROOT/target/rshape-demo}
say() { echo "[demo/down] $*"; }
stop() { # <pidfile> <label> <seconds>
    [ -f "$1" ] || { say "$2: no pid file"; return 0; }
    pid=$(cat "$1")
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid"
        for _ in $(seq 1 "$3"); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
        if kill -0 "$pid" 2>/dev/null; then say "$2 pid $pid still running after $3 s; killing that pid"; kill -KILL "$pid"; else say "$2 pid $pid stopped"; fi
    else
        say "$2 pid $pid was not running"
    fi
    rm -f "$1"
}
stop "$HOME_DIR/admin-ui.pid" "admin-ui" 10
stop "$HOME_DIR/test-runner.pid" "test-runner" 10
stop "$HOME_DIR/rshape-demo.pid" "rshape-demo" 120
