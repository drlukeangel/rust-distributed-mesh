#!/usr/bin/env bash
# lane-prep.sh <lane> <branch> — claim a lane for one unit on a fresh branch from origin/main.
# Refuses a lane that is busy (exit 3), dirty (exit 6), or whose branch name is taken (exit 4). The
# lane is reused, never re-created, so its target stays warm. Clears the previous unit's plan, seat
# and issue; keeps its inbox history under inbox/done-<branch>/.
set -euo pipefail
. "$(dirname "$0")/lane-lib.sh"
L="${1:?usage: lane-prep.sh <lane> <branch>}"; B="${2:?usage: lane-prep.sh <lane> <branch>}"
d=$(lane_dir "$L"); s=$(state_dir "$L")
[ -d "$d" ] || { echo "REFUSED: $d is not a lane"; exit 2; }
[ "$(lane_busy "$L")" -eq 0 ] || { echo "REFUSED: lane $L is busy ($(lane_busy "$L") processes in it)"; exit 3; }
[ "$(lane_dirty "$L")" -eq 0 ] || { echo "REFUSED: lane $L has uncommitted changes:"; git -C "$d" status --short --untracked-files=no; exit 6; }
git -C "$d" fetch -q origin
git -C "$d" show-ref -q --verify "refs/heads/$B" && { echo "REFUSED: branch $B exists"; exit 4; }
git -C "$d" switch -q -c "$B" origin/main
mkdir -p "$s/inbox"
prev=$(cat "$s/branch" 2>/dev/null || true)
if [ -n "$prev" ] && ls "$s/inbox"/[0-9]*.md >/dev/null 2>&1; then mkdir -p "$s/inbox/done-$prev"; mv "$s/inbox"/[0-9]*.md "$s/inbox/done-$prev/"; fi
rm -f "$s/plan.md" "$s/seat" "$s/issue"
echo "$B" > "$s/branch"; echo "$d" > "$s/worktree"
echo "PREPPED lane $L: $B at $(git -C "$d" rev-parse --short HEAD)"
