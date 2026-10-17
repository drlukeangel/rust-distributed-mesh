#!/bin/bash
# cells.sh — run RDM test cells in a lane from the gate's one build.
#
# Cargo keeps one copy of a workspace crate per package selection: each `-p` set resolves its own
# features (the tools add serde_core, hyper-util, rustix and proc-macro2 features under rdm-transport),
# so a different selection compiles the whole stack again. This script builds exactly the gate's
# selection (every workspace member, `--tests --bins`, scripts/rdm-gate-at.sh), so a lane holds one
# copy and an edit is paid once, then runs the test executable you name from that build.
#
# usage: cells.sh <package> [<test target>|lib] [-- <libtest args: filter, --nocapture, --exact ...>]
#   scripts/cells.sh rdm-transport main -- delivery::
#   scripts/cells.sh rdm-admin lib -- status_declare::
#   scripts/cells.sh rdm-test main -- node_stop::      (every cell runs at the gate cadence)
# Run it from the lane's root.
set -u
pkg=${1:?usage: cells.sh <package> [<test target>|lib] [-- args]}; shift
target=main; if [ $# -gt 0 ] && [ "$1" != "--" ]; then target=$1; shift; fi
[ "${1:-}" = "--" ] && shift
lane=$(pwd); out=$lane/target/cells; mkdir -p $out
# Workspace crates compile incrementally through cargo's own setting (build.incremental), the gate's
# value, never the env var CARGO_INCREMENTAL: sccache refuses every vendor compile when it is set.
export CARGO_BUILD_INCREMENTAL=true
unset CARGO_TARGET_DIR
MEMBERS=$(cargo metadata --no-deps --format-version 1 2>/dev/null | python3 -c 'import json,sys; print(" ".join(p["name"] for p in json.load(sys.stdin)["packages"]))')
case " $MEMBERS " in *" $pkg "*) ;; *) echo "cells: $pkg is not a member of this workspace" >&2; exit 2 ;; esac
sel=$(for p in $MEMBERS; do printf " -p %s" "$p"; done)
s=$(date +%s.%N)
if ! cargo build --message-format=json --tests --bins $sel > $out/artifacts.json 2> $out/build.err; then
  python3 -c 'import json,sys
for l in open(sys.argv[1]):
    try: m=json.loads(l)
    except Exception: continue
    if m.get("reason")=="compiler-message" and m["message"].get("level")=="error": print(m["message"].get("rendered",""))' $out/artifacts.json | head -80
  grep -E "^error" $out/build.err | tail -3; echo "cells: build failed (compiler output above; cargo log $out/build.err)" >&2; exit 1
fi
printf "cells: build %.1fs, %d crates compiled\n" "$(echo "$(date +%s.%N) - $s" | bc)" "$(grep -c '^ *Compiling' $out/build.err)" >&2
read -r exe dir < <(python3 - "$out/artifacts.json" "$pkg" "$target" <<'PY'
import json, sys
path, pkg, want = sys.argv[1:4]
for l in open(path):
    try: m = json.loads(l)
    except Exception: continue
    if m.get("reason") != "compiler-artifact" or not m.get("executable") or not m.get("profile", {}).get("test"): continue
    pid = m["package_id"]
    name = pid.split("#")[1].split("@")[0] if "#" in pid and "@" in pid.split("#")[1] else pid.rstrip("/").split("/")[-1].split("#")[0]
    kinds = m["target"]["kind"]
    if name != pkg: continue
    if (want == "lib" and "lib" in kinds) or m["target"]["name"] == want:
        print(m["executable"], m["manifest_path"].rsplit("/", 1)[0]); break
PY
)
[ -n "${exe:-}" ] || { echo "cells: no test executable '$target' in $pkg from this build" >&2; exit 2; }
# Every executable runs at the gate's cadence, as the gate exports it for every executable (tools/test-runner lib.rs).
export RDM_STALENESS_MS=3000 RDM_GOSSIP_INTERVAL_MS=500 RDM_BACKBONE_INTERVAL_MS=500 RAFKA_STALENESS_MS=3000 RAFKA_GOSSIP_INTERVAL_MS=500 RAFKA_BACKBONE_INTERVAL_MS=500
cd "$dir" && CARGO_MANIFEST_DIR=$dir exec "$exe" "$@"
