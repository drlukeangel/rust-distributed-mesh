#!/usr/bin/env bash
# lane-land.sh <lane> <branch> <issue> <close-comment-file> — land a seat's branch and close its issue:
# on main, or, for a unit prepped with --base, by fast-forwarding the lead lane's branch.
# Refuses a tip without a standing GREEN post-land-gate.sh; then runs origin/main's
# scripts/rebase-gate-land.sh, which finds the green tree and fast-forwards without gating,
# then closes the issue with the comment file, @SHA@ filled with the landed SHA. LAND_NOCLOSE=1
# comments instead of closing. Releases the lane's seat on a landing, then prints the unit's
# TIME-SPENT report (time_spent below): where the seat's time went, from its plan to the landing.
#
# A GREEN stands for a rebased tip when main moved since the proof: the branch's patch is unchanged
# (the git patch-id of origin/main..HEAD equals the proven range's, post-land-gate.sh's green-ranges)
# and main's movement since the proof touches none of the branch's files (the generated pages
# docs/reference/rdm/*.md are not the branch's files: a patch-id and an overlap leave them out). Then it
# rebases (a conflict only in the generated pages is regenerated with the lane's rdm-docgen; any other
# conflict is refused), regenerates the pages with the lane's rdm-docgen, leaving exactly the pages it
# writes (spans.md, config.md: regenerate_pages in lane-lib.sh, Q262), and commits what changed onto
# the tip (a failing rdm-docgen refuses and leaves the lane as it was), records the
# rebased tree in green-trees (`stands-on <proven tree>`) and green-ranges, and lands without a second
# gate. Otherwise: "prove it again".
set -uo pipefail
. "$(dirname "$0")/lane-lib.sh"
L="${1:?usage: lane-land.sh <lane> <branch> <issue> <close-comment-file>}"; B="${2:?}"; I="${3:?}"; C="${4:?}"
[ -f "$C" ] || { echo "REFUSED: comment file $C does not exist"; exit 2; }
[ "$(lane_busy "$L")" -eq 0 ] || { echo "REFUSED: lane $L is busy"; exit 3; }
# A landing never proves the lane: the tip's GREEN post-land-gate.sh must stand, on the current main or,
# for a unit prepped on a lead lane's branch (lane-prep.sh --base), on that branch: the landing then
# fast-forwards the lead lane's branch in the lead lane, never main (the lead lands it on main).
d=$(lane_dir "$L"); git -C "$d" fetch -q origin
ON=$(lane_base "$L"); LEAD=$(lane_lead "$L")
[ "$(git -C "$d" branch --show-current)" = "$B" ] || { echo "REFUSED: lane $L has $(git -C "$d" branch --show-current) checked out, not $B"; exit 3; }
T=$(git -C "$d" rev-parse HEAD^{tree})
# The seat's own commits, read before a rebase rewrites their dates: count and the first one's time.
COMMITS=$(git -C "$d" rev-list --count "$ON"..HEAD); FIRST=$(git -C "$d" log --reverse --format=%at "$ON"..HEAD | head -1)
grep -q "^$T " "$DEPLOY_ROOT/green-trees" 2>/dev/null || { echo "REFUSED: lane $L's tip has no GREEN post-land-gate.sh; prove it in the lane first"; exit 8; }
if ! git -C "$d" merge-base --is-ancestor "$ON" HEAD; then
  again="prove it again (post-land-gate.sh $L)"
  rec=$(grep "^$T " "$DEPLOY_ROOT/green-ranges" 2>/dev/null | tail -1)
  [ -n "$rec" ] || { echo "REFUSED: the base moved past lane $L's tip and its GREEN records no proven range; $again"; exit 8; }
  read -r _ PATCH BASE _ <<< "$rec"
  { git -C "$d" merge-base --is-ancestor "$BASE" "$ON" && git -C "$d" merge-base --is-ancestor "$BASE" HEAD; } 2>/dev/null \
    || { echo "REFUSED: the base moved past lane $L's tip and no longer holds the main it was proven on ($BASE); $again"; exit 8; }
  GEN='^docs/reference/rdm/[^/]+\.md$'
  both=$(comm -12 <(git -C "$d" diff --no-renames --name-only "$BASE" "$ON" | grep -vE "$GEN" | sort) <(git -C "$d" diff --no-renames --name-only "$BASE" HEAD | grep -vE "$GEN" | sort) | tr '\n' ' ')
  # Main moved in the branch's own files: the GREEN still stands when the branch rebases onto main with
  # no conflict and its patch is unchanged (checked below) (Luke, 2026-10-28: "you don't run tests
  # again"). A conflict, or a changed patch, is proved again.
  [ -z "$both" ] || echo "the base moved in the branch's own files (${both% }): a clean rebase of the same patch keeps the GREEN"
  [ "$(lane_dirty "$L")" -eq 0 ] || { echo "REFUSED: lane $L has uncommitted changes"; exit 6; }
  orig=$(git -C "$d" rev-parse HEAD); docgen="$d/target/debug/rdm-docgen"
  if ! git -C "$d" rebase -q "$ON"; then
    while :; do
      u=$(git -C "$d" diff --name-only --diff-filter=U); other=$(printf '%s\n' "$u" | grep -vE "$GEN" | grep . | tr '\n' ' ')
      if [ -z "$u" ] || [ -n "$other" ] || ! regenerate_pages "$d"; then
        git -C "$d" rebase --abort; echo "REFUSED: rebase of lane $L on $ON conflicts${other:+ in ${other% }}; $again"; exit 8
      fi
      GIT_EDITOR=true git -C "$d" rebase --continue >/dev/null 2>&1 && break
    done
  fi
  now=$(git -C "$d" diff "$ON" HEAD -- . ':(exclude)docs/reference/rdm/*.md' | git -C "$d" patch-id --stable | cut -d' ' -f1); now=${now:-empty}
  [ "$now" = "$PATCH" ] || { git -C "$d" reset -q --keep "$orig"; echo "REFUSED: lane $L's patch on $ON is not the proven one ($now, proven $PATCH); $again"; exit 8; }
  if ! regenerate_pages "$d"; then
    git -C "$d" reset -q --keep "$orig"; echo "REFUSED: $docgen did not regenerate docs/reference/rdm; $again"; exit 8
  fi
  if ! git -C "$d" diff --cached --quiet -- docs/reference/rdm; then
    git -C "$d" commit -q -m "docs(reference): regenerate docs/reference/rdm with rdm-docgen on the rebased tip (lane-land, rdm#103)" \
      || { git -C "$d" reset -q --keep "$orig"; echo "REFUSED: the regenerated pages could not be committed; $again"; exit 8; }
    echo "regenerated docs/reference/rdm with rdm-docgen and committed it onto the tip"
  fi
  T2=$(git -C "$d" rev-parse HEAD^{tree}); H2=$(git -C "$d" rev-parse HEAD)
  echo "$T2 $(date -Is) $(git -C "$d" rev-parse --short HEAD) stands-on $T" >> "$DEPLOY_ROOT/green-trees"
  echo "$T2 $PATCH $(git -C "$d" rev-parse "$ON") $H2 $(date -Is) stands-on $T" >> "$DEPLOY_ROOT/green-ranges"
  echo "STANDS lane $L: the GREEN of tree $T stands for $(git -C "$d" rev-parse --short HEAD) (tree $T2): same patch, the base moved in none of the branch's files but the generated pages"
fi
# time_spent: where a unit's time went, recorded from what the lane scripts keep: the plan's time
# (lane-dispatch.sh), the seat's launch (lane-launched.sh), the seat's commits, and every post-land-gate.sh
# log of this lane since the plan. landings_trend.py appends one JSON line to $DEPLOY_ROOT/landings.jsonl
# and prints the phases, every gate and the trend as tables.
time_spent() {
  local st plan launched
  st=$(state_dir "$L"); plan=$(stat -c %Y "$st/plan.md" 2>/dev/null || date +%s)
  launched=$(cat "$st/launched" 2>/dev/null || echo -)
  python3 "$(dirname "$0")/landings_trend.py" record "$DEPLOY_ROOT/landings.jsonl" "$DEPLOY_ROOT" "$L" "$I" "$B" "$sha" "$plan" "$launched" "${FIRST:--}" "${COMMITS:-0}"
}
if [ -n "$LEAD" ]; then
  ld=$(lane_dir "$LEAD")
  [ "$(git -C "$ld" branch --show-current)" = "$ON" ] || { echo "REFUSED: lead lane $LEAD has $(git -C "$ld" branch --show-current) checked out, not $ON"; exit 3; }
  [ "$(lane_busy "$LEAD")" -eq 0 ] || { echo "REFUSED: lead lane $LEAD is busy"; exit 3; }
  [ "$(lane_dirty "$LEAD")" -eq 0 ] || { echo "REFUSED: lead lane $LEAD has uncommitted changes"; exit 6; }
  git -C "$ld" merge -q --ff-only "$B" || { echo "NOT LANDED: lead lane $LEAD's $ON does not fast-forward to $B; issue #$I left as it is"; exit 8; }
  sha=$(git -C "$ld" rev-parse --short HEAD); echo "LANDED $sha on lead lane $LEAD ($ON)"
  body=$(sed "s/@SHA@/$sha/g" "$C"); cd "$MAIN"
  if [ -n "${LAND_NOCLOSE:-}" ]; then gh issue comment "$I" --repo "$(lane_repo "$L")" -b "$body" >/dev/null && echo "COMMENTED #$I"
  else gh issue close "$I" --repo "$(lane_repo "$L")" -c "$body" >/dev/null && echo "CLOSED #$I"; fi
  rm -f "$(state_dir "$L")/seat"
  echo "LANDED-AND-RECORDED $sha lead lane $LEAD issue #$I"
  time_spent; exit 0
fi
git -C "$MAIN" fetch -q origin && c=$(mktemp /tmp/rdm-land-run.XXXXXX) && git -C "$MAIN" show origin/main:scripts/rebase-gate-land.sh > "$c" \
  || { echo "REFUSED: origin/main's scripts/rebase-gate-land.sh could not be read"; exit 6; }
out=$(LAND_PRIVATE_COPY=1 bash "$c" "$L" "$B" 2>&1); rc=$?; rm -f "$c"; echo "$out"
sha=$(echo "$out" | sed -n 's/^LANDED \([0-9a-f]*\).*/\1/p' | tail -1)
[ $rc -eq 0 ] && [ -n "$sha" ] || { echo "NOT LANDED (exit $rc); issue #$I left as it is"; exit "${rc:-1}"; }
body=$(sed "s/@SHA@/$sha/g" "$C")
cd "$MAIN"
if [ -n "${LAND_NOCLOSE:-}" ]; then gh issue comment "$I" --repo "$(lane_repo "$L")" -b "$body" >/dev/null && echo "COMMENTED #$I"
else gh issue close "$I" --repo "$(lane_repo "$L")" -c "$body" >/dev/null && echo "CLOSED #$I"; fi
rm -f "$(state_dir "$L")/seat"
echo "LANDED-AND-RECORDED $sha lane $L issue #$I"
time_spent
