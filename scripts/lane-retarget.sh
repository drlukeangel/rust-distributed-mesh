#!/usr/bin/env bash
# lane-retarget.sh <lane> --base <lead lane> — point a lane's unit at another lead lane, keeping its
# branch: its cells.sh rebases, post-land-gate.sh proves and lane-land.sh lands onto that lead lane's
# branch from then on (testing-lane-scripts.md §1.1). For a unit stacked on a lane whose own branch is
# now carried by it (lane 4 over lane 3's cells): the stack lands on the lead lane in one go.
# Refuses a busy lane (exit 3), a dirty one (exit 6), a lead lane with no branch (exit 5) and a lead
# lane whose branch is not an ancestor-or-relative of the unit's (the unit must rebase onto it, exit 4).
set -euo pipefail
. "$(dirname "$0")/lane-lib.sh"
L="${1:?usage: lane-retarget.sh <lane> --base <lead lane>}"
[ "${2:-}" = "--base" ] || { echo "usage: lane-retarget.sh <lane> --base <lead lane>"; exit 2; }
LEAD="${3:?usage: lane-retarget.sh <lane> --base <lead lane>}"
d=$(lane_dir "$L"); s=$(state_dir "$L")
[ "$(lane_busy "$L")" -eq 0 ] || { echo "REFUSED: lane $L is busy ($(lane_busy "$L") processes in it)"; exit 3; }
[ "$(lane_dirty "$L")" -eq 0 ] || { echo "REFUSED: lane $L has uncommitted changes"; exit 6; }
BASE=$(git -C "$(lane_dir "$LEAD")" branch --show-current)
[ -n "$BASE" ] || { echo "REFUSED: lead lane $LEAD has no branch checked out"; exit 5; }
git -C "$d" merge-base HEAD "$BASE" >/dev/null || { echo "REFUSED: lane $L's branch shares no history with $BASE"; exit 4; }
was=$(lane_base "$L")
echo "$BASE" > "$s/base"; echo "$LEAD" > "$s/lead"
echo "RETARGETED lane $L: $(git -C "$d" branch --show-current) now on $BASE (lead lane $LEAD), was $was"
