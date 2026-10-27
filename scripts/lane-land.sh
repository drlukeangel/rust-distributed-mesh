#!/usr/bin/env bash
# lane-land.sh <lane> <branch> <issue> <close-comment-file> — land a seat's branch and close its issue.
# Refuses a tip without a standing GREEN post-land-gate.sh; then runs origin/main's
# scripts/rebase-gate-land.sh, which finds the green tree and fast-forwards without gating,
# then closes the issue with the comment file, @SHA@ filled with the landed SHA. LAND_NOCLOSE=1
# comments instead of closing. Releases the lane's seat on a landing.
set -uo pipefail
. "$(dirname "$0")/lane-lib.sh"
L="${1:?usage: lane-land.sh <lane> <branch> <issue> <close-comment-file>}"; B="${2:?}"; I="${3:?}"; C="${4:?}"
[ -f "$C" ] || { echo "REFUSED: comment file $C does not exist"; exit 2; }
[ "$(lane_busy "$L")" -eq 0 ] || { echo "REFUSED: lane $L is busy"; exit 3; }
# A landing never proves the lane: the tip's GREEN post-land-gate.sh must stand, on the current main.
d=$(lane_dir "$L"); git -C "$d" fetch -q origin
[ "$(git -C "$d" branch --show-current)" = "$B" ] || { echo "REFUSED: lane $L has $(git -C "$d" branch --show-current) checked out, not $B"; exit 3; }
git -C "$d" merge-base --is-ancestor origin/main HEAD || { echo "REFUSED: main moved past lane $L's tip; prove it again (post-land-gate.sh $L)"; exit 8; }
grep -q "^$(git -C "$d" rev-parse HEAD^{tree}) " "$DEPLOY_ROOT/green-trees" 2>/dev/null || { echo "REFUSED: lane $L's tip has no GREEN post-land-gate.sh; prove it in the lane first"; exit 8; }
git -C "$MAIN" fetch -q origin && c=$(mktemp /tmp/rdm-land-run.XXXXXX) && git -C "$MAIN" show origin/main:scripts/rebase-gate-land.sh > "$c" \
  || { echo "REFUSED: origin/main's scripts/rebase-gate-land.sh could not be read"; exit 6; }
out=$(LAND_PRIVATE_COPY=1 bash "$c" "$L" "$B" 2>&1); rc=$?; rm -f "$c"; echo "$out"
sha=$(echo "$out" | sed -n 's/^LANDED \([0-9a-f]*\).*/\1/p' | tail -1)
[ $rc -eq 0 ] && [ -n "$sha" ] || { echo "NOT LANDED (exit $rc); issue #$I left as it is"; exit "${rc:-1}"; }
body=$(sed "s/@SHA@/$sha/g" "$C")
cd "$MAIN"
if [ -n "${LAND_NOCLOSE:-}" ]; then gh issue comment "$I" --repo drlukeangel/rafka-v2 -b "$body" >/dev/null && echo "COMMENTED #$I"
else gh issue close "$I" --repo drlukeangel/rafka-v2 -c "$body" >/dev/null && echo "CLOSED #$I"; fi
rm -f "$(state_dir "$L")/seat"
echo "LANDED-AND-RECORDED $sha lane $L issue #$I"
