#!/usr/bin/env bash
# lane-land.sh <lane> <branch> <issue> <close-comment-file> — land a seat's branch and close its issue.
# Refuses a tip without a standing GREEN post-land-gate.sh; then runs origin/main's
# scripts/rebase-gate-land.sh, which finds the green tree and fast-forwards without gating,
# then closes the issue with the comment file, @SHA@ filled with the landed SHA. LAND_NOCLOSE=1
# comments instead of closing. Releases the lane's seat on a landing.
#
# A GREEN stands for a rebased tip when main moved since the proof: the branch's patch is unchanged
# (the git patch-id of origin/main..HEAD equals the proven range's, post-land-gate.sh's green-ranges)
# and main's movement since the proof touches none of the branch's files. Then it rebases, records the
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
grep -q "^$T " "$DEPLOY_ROOT/green-trees" 2>/dev/null || { echo "REFUSED: lane $L's tip has no GREEN post-land-gate.sh; prove it in the lane first"; exit 8; }
if ! git -C "$d" merge-base --is-ancestor origin/main HEAD; then
  again="prove it again (post-land-gate.sh $L)"
  rec=$(grep "^$T " "$DEPLOY_ROOT/green-ranges" 2>/dev/null | tail -1)
  [ -n "$rec" ] || { echo "REFUSED: main moved past lane $L's tip and its GREEN records no proven range; $again"; exit 8; }
  read -r _ PATCH BASE _ <<< "$rec"
  { git -C "$d" merge-base --is-ancestor "$BASE" origin/main && git -C "$d" merge-base --is-ancestor "$BASE" HEAD; } 2>/dev/null \
    || { echo "REFUSED: main moved past lane $L's tip and no longer holds the main it was proven on ($BASE); $again"; exit 8; }
  both=$(comm -12 <(git -C "$d" diff --no-renames --name-only "$BASE" origin/main | sort) <(git -C "$d" diff --no-renames --name-only "$BASE" HEAD | sort) | tr '\n' ' ')
  [ -z "$both" ] || { echo "REFUSED: main moved past lane $L's tip in the branch's own files (${both% }); $again"; exit 8; }
  [ "$(lane_dirty "$L")" -eq 0 ] || { echo "REFUSED: lane $L has uncommitted changes"; exit 6; }
  git -C "$d" rebase -q origin/main || { git -C "$d" rebase --abort; echo "REFUSED: rebase of lane $L on origin/main conflicts; $again"; exit 8; }
  now=$(git -C "$d" diff origin/main HEAD | git -C "$d" patch-id --stable | cut -d' ' -f1); now=${now:-empty}
  [ "$now" = "$PATCH" ] || { echo "REFUSED: lane $L's patch on origin/main is not the proven one ($now, proven $PATCH); $again"; exit 8; }
  T2=$(git -C "$d" rev-parse HEAD^{tree}); H2=$(git -C "$d" rev-parse HEAD)
  echo "$T2 $(date -Is) $(git -C "$d" rev-parse --short HEAD) stands-on $T" >> "$DEPLOY_ROOT/green-trees"
  echo "$T2 $PATCH $(git -C "$d" rev-parse origin/main) $H2 $(date -Is) stands-on $T" >> "$DEPLOY_ROOT/green-ranges"
  echo "STANDS lane $L: the GREEN of tree $T stands for $(git -C "$d" rev-parse --short HEAD) (tree $T2): same patch, main moved in none of the branch's files"
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
