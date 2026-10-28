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
# It runs from anywhere: the lane is the repo root this script sits in.
set -u
pkg=${1:?usage: cells.sh <package> [<test target>|lib] [-- args]}; shift
target=main; if [ $# -gt 0 ] && [ "$1" != "--" ]; then target=$1; shift; fi
[ "${1:-}" = "--" ] && shift
lane=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd); cd "$lane"; out=$lane/target/cells; mkdir -p $out
# This run's own build output: parallel runs in one lane each read the executables their own build
# named, never a file another run is writing.
art=$out/artifacts.$$.json; berr=$out/build.$$.err; trap 'rm -f "$art" "$berr"' EXIT
# Workspace crates compile incrementally through cargo's own setting (build.incremental), the gate's
# value, never the env var CARGO_INCREMENTAL: sccache refuses every vendor compile when it is set.
export CARGO_BUILD_INCREMENTAL=true
unset CARGO_TARGET_DIR
MEMBERS=$(cargo metadata --no-deps --format-version 1 2>/dev/null | python3 -c 'import json,sys; print(" ".join(p["name"] for p in json.load(sys.stdin)["packages"]))')
case " $MEMBERS " in *" $pkg "*) ;; *) echo "cells: $pkg is not a member of this workspace" >&2; exit 2 ;; esac
sel=$(for p in $MEMBERS; do printf " -p %s" "$p"; done)
s=$(date +%s.%N)
: > $berr
# Cargo's stderr is opened for append, so scripts/rustc-wrapper.sh can name a missing optional tool
# (sccache, mold) on it once (the wrapper writes no line into a file opened with `2>`).
cargo build --message-format=json --tests --bins $sel > $art 2>> $berr; brc=$?
grep '^rdm build: ' $berr >&2
if [ $brc -ne 0 ]; then
  python3 -c 'import json,sys
for l in open(sys.argv[1]):
    try: m=json.loads(l)
    except Exception: continue
    if m.get("reason")=="compiler-message" and m["message"].get("level")=="error": print(m["message"].get("rendered",""))' $art | head -80
  cp $berr $out/build.err; grep -E "^error" $berr | tail -3; echo "cells: build failed (compiler output above; cargo log $out/build.err)" >&2; exit 1
fi
printf "cells: build %.1fs, %d crates compiled\n" "$(echo "$(date +%s.%N) - $s" | bc)" "$(grep -c '^ *Compiling' $berr)" >&2
read -r exe dir < <(python3 - "$art" "$pkg" "$target" <<'PY'
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
# Test stores in RAM: every temp dir a cell makes (each estate's store root and data dirs, each testkit
# span directory) is under this run's own directory on tmpfs, made here and removed when the run ends.
# 2026-10-28, rdm#117's fsync measurement: a Day-0 admin makes ~30 serial fsyncs before it is Ready, and
# with several lanes' gates writing to one ext4 journal each took 0.5-3 s, so a 0.5 s boot passed its
# 10 s bound. Each cells.sh run has its own directory: parallel runs in one lane never remove another's.
export TMPDIR=/dev/shm/rdm-lane-$(basename "$lane")/cells-$$
rm -rf "$TMPDIR"; mkdir -p "$TMPDIR"
started=$(date +%s)
cd "$dir" && CARGO_MANIFEST_DIR=$dir "$exe" "$@"; rc=$?
# The cells ledger: one line per run, so lane-land.sh's time report counts the seat's cell time beside
# its gates (Luke, 2026-10-28).
python3 -c 'import json,sys; print(json.dumps({"lane":sys.argv[1],"start":int(sys.argv[2]),"end":int(sys.argv[3]),"package":sys.argv[4],"target":sys.argv[5],"args":sys.argv[7:],"rc":int(sys.argv[6])}))' \
  "$(basename "$lane")" "$started" "$(date +%s)" "$pkg" "$target" "$rc" "$@" >> "${RDM_DEPLOY:-$HOME/rust-distributed-mesh-deploy}/cells.jsonl" 2>/dev/null || true
# An estate's reaper (rdm-test estate.rs) outlives a killed test binary in its own session and reads
# its root under TMPDIR (the fabric_id whose containers it removes): TMPDIR goes once every reaper of
# this run has ended, waited for at most 60 s.
for _ in $(seq 300); do pgrep -f "estate-reaper [0-9]+ $TMPDIR/" > /dev/null || break; sleep 0.2; done
rm -rf "$TMPDIR"
exit $rc
