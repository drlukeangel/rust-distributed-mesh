#!/usr/bin/env bash
# lane-status.sh [lane…] — one row per lane: branch, commits past main, dirty files, live processes,
# last commit age, the seat's issue and agent type. No lane named: every lane under $HOME/rdm-lanes.
set -uo pipefail
. "$(dirname "$0")/lane-lib.sh"
git -C "$MAIN" fetch -q origin 2>/dev/null
lanes=("$@"); [ ${#lanes[@]} -eq 0 ] && lanes=($(ls "$LANES_ROOT" | grep -E '^[0-9]+$' | sort -n))
printf '%-5s %-44s %5s %5s %5s %-14s %-7s %s\n' LANE BRANCH AHEAD DIRTY PROCS LAST ISSUE SEAT
for l in "${lanes[@]}"; do
  d=$(lane_dir "$l"); s=$(state_dir "$l")
  [ -d "$d" ] || { printf '%-5s missing\n' "$l"; continue; }
  b=$(git -C "$d" branch --show-current); [ -z "$b" ] && b="(detached $(git -C "$d" rev-parse --short HEAD))"
  printf '%-5s %-44s %5s %5s %5s %-14s %-7s %s\n' "$l" "${b:0:44}" "$(git -C "$d" rev-list --count origin/main..HEAD 2>/dev/null)" \
    "$(lane_dirty "$l")" "$(lane_busy "$l")" "$(git -C "$d" log -1 --format=%cr | sed 's/ ago//')" \
    "$(cat "$s/issue" 2>/dev/null || echo -)" "$(cat "$s/seat" 2>/dev/null || echo -)"
done
