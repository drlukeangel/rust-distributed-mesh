#!/bin/bash
[ -z "$LAND_PRIVATE_COPY" ] && { c=$(mktemp /tmp/rdm-land-run.XXXXXX); cp "$0" "$c"; chmod +x "$c"; LAND_PRIVATE_COPY=1 exec bash "$c" "$@"; }
# usage: rebase-gate-land.sh <lane> <branch>
set -u
# One landing at a time box-wide: a gate is only valid for the main it lands on.
# The lock is held by a wrapping flock(1) with -o: the fd is closed before this script runs, so
# no node a gate spawns can inherit it and hold the box after the run dies.
[ -z "${LAND_LOCKED:-}" ] && { LAND_LOCKED=1 exec flock -o /tmp/rdm-land.lock bash "$0" "$@"; }
L=/home/admin/rdm-lanes/$1; B=$2
# What the gates leave behind, beside their logs: the red trees and the timings ledger.
DEPLOY=/home/admin/rust-distributed-mesh-deploy
cd /home/admin/rust-distributed-mesh && git fetch -q origin
cd $L && git fetch -q /home/admin/rust-distributed-mesh main:refs/remotes/origin/main 2>/dev/null; git fetch -q origin
cur=$(git branch --show-current); [ "$cur" = "$B" ] || { echo "REFUSED: $L has $cur checked out, not $B"; exit 3; }
if git merge-base --is-ancestor origin/main HEAD; then :  # already on main: a rebase would only flatten merges
elif [ -n "$(git rev-list --merges origin/main..HEAD)" ]; then
  # A branch that already merged main keeps its merge resolutions: merge main again, never flatten.
  if ! git merge --no-edit -q origin/main; then
    echo "MERGE CONFLICT in $B:"; git diff --name-only --diff-filter=U; git merge --abort; exit 1
  fi
elif ! git rebase -q origin/main; then
  echo "REBASE CONFLICT in $B:"; git diff --name-only --diff-filter=U; git rebase --abort; exit 1
fi
echo "rebased $B -> $(git rev-parse --short HEAD) on $(git rev-parse --short origin/main)"
# The name check (rdm-crate-and-test-layout.md §4): a diff that ADDS a retired word in an identifier or a file
# name, or an id-named test file, is refused before any gate runs.
NC=$(python3 scripts/gate/name-check.py origin/main HEAD 2>&1); [ -n "$NC" ] && { echo "REFUSED: name check"; echo "$NC"; exit 5; }
# A tree whose gate went red is never gated again: a red is a defect to fix, not a roll to repeat.
TREE=$(git rev-parse HEAD^{tree}); RED=$DEPLOY/red-trees
if grep -q "^$TREE " "$RED" 2>/dev/null; then echo "REFUSED: this exact tree was red ($(grep "^$TREE " "$RED" | tail -1 | cut -d' ' -f2-)); fix the red, do not re-gate it"; exit 4; fi
OUT=$(bash $L/scripts/gate/rdm-gate-at.sh $L fast 9>&- 2>&1); echo "$OUT" | grep -E "FAIL|DONE" | tail -6
# The landing's timings row goes to the ledger whatever the gate's verdict, with the tree it measured.
TROW=$(echo "$OUT" | grep "^TIMINGS" | tail -1); echo "$TROW"; echo "$(date -Is) $B $(git rev-parse --short HEAD) $TROW" >> $DEPLOY/timings-ledger.txt
if echo "$OUT" | grep -q FAIL; then echo "$TREE $(date -Is) $B $(echo "$OUT" | grep FAIL | head -3 | tr '\n' ' ')" >> "$RED"; echo "NOT LANDED: gate red; this tree is recorded and will not be gated again"; exit 2; fi
# Land from the lane itself: the remote refuses anything but a fast-forward, and the shared main checkout (which may hold someone
# else's uncommitted edit) is never touched.
cd $L && git push -q origin HEAD:main && echo "LANDED $(git log --oneline -1 | cut -c1-90)"
