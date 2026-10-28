#!/usr/bin/env bash
# lane-land.sh <lane> <branch> <issue> <close-comment-file> — land a seat's branch and close its issue.
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
# conflict is refused), regenerates the pages with the lane's rdm-docgen and commits what it writes onto
# the tip (a failing rdm-docgen refuses and leaves the lane as it was), records the
# rebased tree in green-trees (`stands-on <proven tree>`) and green-ranges, and lands without a second
# gate. Otherwise: "prove it again".
set -uo pipefail
. "$(dirname "$0")/lane-lib.sh"
L="${1:?usage: lane-land.sh <lane> <branch> <issue> <close-comment-file>}"; B="${2:?}"; I="${3:?}"; C="${4:?}"
[ -f "$C" ] || { echo "REFUSED: comment file $C does not exist"; exit 2; }
[ "$(lane_busy "$L")" -eq 0 ] || { echo "REFUSED: lane $L is busy"; exit 3; }
# A landing never proves the lane: the tip's GREEN post-land-gate.sh must stand, on the current main.
d=$(lane_dir "$L"); git -C "$d" fetch -q origin
[ "$(git -C "$d" branch --show-current)" = "$B" ] || { echo "REFUSED: lane $L has $(git -C "$d" branch --show-current) checked out, not $B"; exit 3; }
T=$(git -C "$d" rev-parse HEAD^{tree})
# The seat's own commits, read before a rebase rewrites their dates: count and the first one's time.
COMMITS=$(git -C "$d" rev-list --count origin/main..HEAD); FIRST=$(git -C "$d" log --reverse --format=%at origin/main..HEAD | head -1)
grep -q "^$T " "$DEPLOY_ROOT/green-trees" 2>/dev/null || { echo "REFUSED: lane $L's tip has no GREEN post-land-gate.sh; prove it in the lane first"; exit 8; }
if ! git -C "$d" merge-base --is-ancestor origin/main HEAD; then
  again="prove it again (post-land-gate.sh $L)"
  rec=$(grep "^$T " "$DEPLOY_ROOT/green-ranges" 2>/dev/null | tail -1)
  [ -n "$rec" ] || { echo "REFUSED: main moved past lane $L's tip and its GREEN records no proven range; $again"; exit 8; }
  read -r _ PATCH BASE _ <<< "$rec"
  { git -C "$d" merge-base --is-ancestor "$BASE" origin/main && git -C "$d" merge-base --is-ancestor "$BASE" HEAD; } 2>/dev/null \
    || { echo "REFUSED: main moved past lane $L's tip and no longer holds the main it was proven on ($BASE); $again"; exit 8; }
  GEN='^docs/reference/rdm/[^/]+\.md$'
  both=$(comm -12 <(git -C "$d" diff --no-renames --name-only "$BASE" origin/main | grep -vE "$GEN" | sort) <(git -C "$d" diff --no-renames --name-only "$BASE" HEAD | grep -vE "$GEN" | sort) | tr '\n' ' ')
  [ -z "$both" ] || { echo "REFUSED: main moved past lane $L's tip in the branch's own files (${both% }); $again"; exit 8; }
  [ "$(lane_dirty "$L")" -eq 0 ] || { echo "REFUSED: lane $L has uncommitted changes"; exit 6; }
  orig=$(git -C "$d" rev-parse HEAD); docgen="$d/target/debug/rdm-docgen"
  if ! git -C "$d" rebase -q origin/main; then
    while :; do
      u=$(git -C "$d" diff --name-only --diff-filter=U); other=$(printf '%s\n' "$u" | grep -vE "$GEN" | grep . | tr '\n' ' ')
      if [ -z "$u" ] || [ -n "$other" ] || ! [ -x "$docgen" ] || ! "$docgen" --root "$d" --out "$d/docs/reference/rdm" >/dev/null; then
        git -C "$d" rebase --abort; echo "REFUSED: rebase of lane $L on origin/main conflicts${other:+ in ${other% }}; $again"; exit 8
      fi
      git -C "$d" add docs/reference/rdm
      GIT_EDITOR=true git -C "$d" rebase --continue >/dev/null 2>&1 && break
    done
  fi
  now=$(git -C "$d" diff origin/main HEAD -- . ':(exclude)docs/reference/rdm/*.md' | git -C "$d" patch-id --stable | cut -d' ' -f1); now=${now:-empty}
  [ "$now" = "$PATCH" ] || { git -C "$d" reset -q --keep "$orig"; echo "REFUSED: lane $L's patch on origin/main is not the proven one ($now, proven $PATCH); $again"; exit 8; }
  if ! [ -x "$docgen" ] || ! "$docgen" --root "$d" --out "$d/docs/reference/rdm" >/dev/null; then
    git -C "$d" reset -q --keep "$orig"; echo "REFUSED: $docgen did not regenerate docs/reference/rdm; $again"; exit 8
  fi
  if [ -n "$(git -C "$d" status --porcelain --untracked-files=no -- docs/reference/rdm)" ]; then
    git -C "$d" add docs/reference/rdm && git -C "$d" commit -q -m "docs(reference): regenerate docs/reference/rdm with rdm-docgen on the rebased tip (lane-land, rdm#103)" \
      || { git -C "$d" reset -q --keep "$orig"; echo "REFUSED: the regenerated pages could not be committed; $again"; exit 8; }
    echo "regenerated docs/reference/rdm with rdm-docgen and committed it onto the tip"
  fi
  T2=$(git -C "$d" rev-parse HEAD^{tree}); H2=$(git -C "$d" rev-parse HEAD)
  echo "$T2 $(date -Is) $(git -C "$d" rev-parse --short HEAD) stands-on $T" >> "$DEPLOY_ROOT/green-trees"
  echo "$T2 $PATCH $(git -C "$d" rev-parse origin/main) $H2 $(date -Is) stands-on $T" >> "$DEPLOY_ROOT/green-ranges"
  echo "STANDS lane $L: the GREEN of tree $T stands for $(git -C "$d" rev-parse --short HEAD) (tree $T2): same patch, main moved in none of the branch's files but the generated pages"
fi
# time_spent: one report of where a unit's time went, from the lane's plan (lane-dispatch.sh writes it)
# to now. Gates are this lane's post-land-gate.sh logs written since the plan, each with its DONE seconds;
# a red gate is one whose log names a FAIL.
time_spent() {
  local plan="$(state_dir "$L")/plan.md" now start g n=0 red=0 secs=0 s
  now=$(date +%s); start=$(stat -c %Y "$plan" 2>/dev/null || echo "$now")
  for g in "$DEPLOY_ROOT"/gates-"$L"-*.log; do
    [ -f "$g" ] && [ "$(stat -c %Y "$g")" -ge "$start" ] || continue
    n=$((n+1)); s=$(sed -n 's/^DONE \([0-9]*\)s$/\1/p' "$g" | tail -1); secs=$((secs+${s:-0}))
    grep -q "FAIL" "$g" && red=$((red+1))
  done
  m() { printf '%dh%02dm' $(( $1/3600 )) $(( ($1%3600)/60 )); }
  echo "TIME-SPENT lane $L issue #$I: $(m $((now-start))) from the plan ($(date -d @"$start" '+%F %T')) to the landing"
  if [ -n "${FIRST:-}" ]; then echo "  first commit: $(m $((FIRST-start))) after the plan; commits: ${COMMITS:-0}"; else echo "  first commit: none; commits: 0"; fi
  echo "  gates: $n run, $red red, $(m "$secs") gating; the rest ($(m $(( now-start-secs )))) building, cells and waiting"
}
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
