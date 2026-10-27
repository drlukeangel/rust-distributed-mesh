#!/usr/bin/env bash
# post-land-gate.sh <lane> — prove the tip that will land, in the lane: rebase on origin/main, rebuild a
# moved lane's own crates, run the lane's own fast gate. Its GREEN line (the tree recorded in
# green-trees) is the landing's proof; lane-land.sh refuses a tip without one and never gates.
set -uo pipefail
. "$(dirname "$0")/lane-lib.sh"
L="${1:?usage: post-land-gate.sh <lane>}"; d=$(lane_dir "$L")
[ "$(lane_busy "$L")" -eq 0 ] || { echo "REFUSED: lane $L is busy"; exit 3; }
[ "$(lane_dirty "$L")" -eq 0 ] || { echo "REFUSED: lane $L has uncommitted changes"; exit 6; }
git -C "$d" fetch -q origin
git -C "$d" merge-base --is-ancestor origin/main HEAD || git -C "$d" rebase -q origin/main \
  || { git -C "$d" rebase --abort; echo "REFUSED: rebase on origin/main conflicts"; exit 4; }
lane_target_at_path "$L" || exit 7
out=$(bash "$d/scripts/rdm-gate-at.sh" "$d" fast 2>&1); echo "$out" | grep -E "^FAIL|^DONE|^TIMINGS" | tail -12
T=$(git -C "$d" rev-parse HEAD^{tree})
if grep -q "^$T " "$DEPLOY_ROOT/green-trees" 2>/dev/null; then echo "GREEN lane $L $(git -C "$d" rev-parse --short HEAD) tree $T"
else echo "RED lane $L $(git -C "$d" rev-parse --short HEAD): read the red, fix it in the lane, never re-run it"; exit 2; fi
