#!/usr/bin/env bash
# post-land-gate.sh <lane> — prove the tip that will land, in the lane: rebase on origin/main, rebuild a
# moved lane's own crates, run the lane's own fast gate. Its GREEN line (the tree recorded in
# green-trees) is the landing's proof; lane-land.sh refuses a tip without one and never gates. The agent
# runs it at the end of its unit (seat-rules item 7).
#
# A rebase conflict only in the generated reference pages (docs/reference/rdm/*.md) is resolved by
# regenerating them with the lane's rdm-docgen (target/debug/rdm-docgen, the gate's build) over the
# commit being applied, the pages left exactly the ones it writes (spans.md, config.md: regenerate_pages
# in lane-lib.sh; a wire.md, frames.md or statuses.md a branch still changes is removed, Q262); a
# conflict in any other file aborts the rebase and is refused, naming the files.
# With each GREEN it records the proven range in green-ranges: the tree, the patch-id of
# origin/main..HEAD (`git patch-id --stable` of the whole diff but the generated pages docs/reference/rdm/*.md), the origin/main it was proven on and
# the tip, so lane-land.sh can let the GREEN stand when main moves only in other files.
set -uo pipefail
. "$(dirname "$0")/lane-lib.sh"
L="${1:?usage: post-land-gate.sh <lane>}"; d=$(lane_dir "$L")
[ "$(lane_busy "$L")" -eq 0 ] || { echo "REFUSED: lane $L is busy"; exit 3; }
[ "$(lane_dirty "$L")" -eq 0 ] || { echo "REFUSED: lane $L has uncommitted changes"; exit 6; }
git -C "$d" fetch -q origin
GENERATED='docs/reference/rdm/[^/]+\.md'
if ! git -C "$d" merge-base --is-ancestor origin/main HEAD && ! git -C "$d" rebase -q origin/main; then
  while :; do
    u=$(git -C "$d" diff --name-only --diff-filter=U)
    other=$(printf '%s\n' "$u" | grep -vxE "$GENERATED" | grep . | tr '\n' ' ')
    if [ -z "$u" ] || [ -n "$other" ]; then
      git -C "$d" rebase --abort; echo "REFUSED: rebase on origin/main conflicts${other:+ in ${other% }}"; exit 4
    fi
    at=$(git -C "$d" rev-parse --short REBASE_HEAD 2>/dev/null)
    if ! regenerate_pages "$d"; then
      git -C "$d" rebase --abort; echo "REFUSED: rebase on origin/main conflicts in $(echo $u); $d/target/debug/rdm-docgen did not regenerate them"; exit 4
    fi
    echo "rebase: regenerated docs/reference/rdm with rdm-docgen at $at ($(echo $u))"
    GIT_EDITOR=true git -C "$d" rebase --continue >/dev/null 2>&1 && break
  done
fi
lane_target_at_path "$L" || exit 7
BASE=$(git -C "$d" rev-parse origin/main); HEAD_SHA=$(git -C "$d" rev-parse HEAD)
PATCH=$(git -C "$d" diff "$BASE" HEAD -- . ':(exclude)docs/reference/rdm/*.md' | git -C "$d" patch-id --stable | cut -d' ' -f1); PATCH=${PATCH:-empty}
out=$(bash "$d/scripts/rdm-gate-at.sh" "$d" fast 2>&1); echo "$out" | grep -E "^FAIL|^DONE|^TIMINGS" | tail -12
T=$(git -C "$d" rev-parse HEAD^{tree})
# GREEN is this run's own verdict: no FAIL line and a DONE with its seconds. A tree an earlier run
# proved is never GREEN for a run that failed (rdm#126: lane 2 printed GREEN with two red stems).
if ! echo "$out" | grep -q "^FAIL" && echo "$out" | grep -qE "^DONE [0-9]+s$" && grep -q "^$T " "$DEPLOY_ROOT/green-trees" 2>/dev/null; then
  echo "$T $PATCH $BASE $HEAD_SHA $(date -Is)" >> "$DEPLOY_ROOT/green-ranges"
  echo "GREEN lane $L $(git -C "$d" rev-parse --short HEAD) tree $T"
else echo "RED lane $L $(git -C "$d" rev-parse --short HEAD): read the red, fix it in the lane, never re-run it"; exit 2; fi
