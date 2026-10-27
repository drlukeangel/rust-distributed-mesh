#!/usr/bin/env bash
# lane-dispatch.sh --lane <n> --issue <N> --name <seat-name> --title <title> --design <file> [--model opus|sonnet]
# Hands one prepped lane its unit: writes the plan ($HOME/lane-queue/rdm/<n>/plan.md), writes the seat's
# agent type i143-<issue> ($HOME/.claude/agents/i143-<issue>.md) and comments the issue with the lane,
# branch and seat. Launch the seat ONLY after this returns, with subagent_type = the SEAT TYPE line; a
# NEW type launches after Claude Code's "New agent types are now available" notice names it.
set -euo pipefail
. "$(dirname "$0")/lane-lib.sh"
LANE= ISSUE= NAME= TITLE= DESIGN= MODEL=opus
while [ $# -gt 0 ]; do case "$1" in
  --lane) LANE=$2; shift 2;; --issue) ISSUE=$2; shift 2;; --name) NAME=$2; shift 2;;
  --title) TITLE=$2; shift 2;; --design) DESIGN=$2; shift 2;; --model) MODEL=$2; shift 2;;
  *) echo "REFUSED: unknown argument $1"; exit 2;; esac; done
for v in LANE ISSUE NAME TITLE DESIGN; do [ -n "${!v}" ] || { echo "REFUSED: --${v,,} is required"; exit 2; }; done
[ -f "$DESIGN" ] || { echo "REFUSED: design $DESIGN does not exist"; exit 2; }
d=$(lane_dir "$LANE"); s=$(state_dir "$LANE")
B=$(cat "$s/branch" 2>/dev/null) || { echo "REFUSED: lane $LANE is not prepped (lane-prep.sh)"; exit 2; }
[ "$(git -C "$d" branch --show-current)" = "$B" ] || { echo "REFUSED: lane $LANE has $(git -C "$d" branch --show-current) checked out, not $B"; exit 3; }
[ -f "$s/seat" ] && { echo "REFUSED: lane $LANE already has seat $(cat "$s/seat"); prep it again for a new unit"; exit 4; }
cd "$MAIN" && gh issue view "$ISSUE" --repo drlukeangel/rafka-v2 --json state --jq .state | grep -q OPEN || { echo "REFUSED: issue #$ISSUE is not open"; exit 5; }
SEAT="i143-$ISSUE"
{
  printf '# %s\n\nIssue: https://github.com/drlukeangel/rafka-v2/issues/%s\nLane: %s (branch %s, from %s)\nSeat: %s (%s)\n\n' \
    "$TITLE" "$ISSUE" "$d" "$B" "$(git -C "$d" rev-parse --short HEAD)" "$SEAT" "$NAME"
  cat "$DESIGN"
} > "$s/plan.md.tmp" && mv -f "$s/plan.md.tmp" "$s/plan.md"
f="$AGENTS_DIR/$SEAT.md"; state=registered; [ -f "$f" ] || state=NEW
mkdir -p "$AGENTS_DIR"
cat > "$f" <<AGENT
---
name: $SEAT
description: RDM seat for issue #$ISSUE ($NAME) in lane $LANE, branch $B. Works only that unit; never gates or lands.
model: $MODEL
---
You are the seat for rafka-v2 issue #$ISSUE, "$TITLE", in lane $d on branch $B.

1. Read /home/admin/rust-distributed-mesh/CLAUDE.md and /home/admin/rafka-v2/CLAUDE.md and follow them exactly.
2. Your plan is $s/plan.md. Read every file in $s/inbox/ numbered above the last one you acted on, in order.
3. Work only in $d, only on $B. Never gate, never land, never push, never switch branch, never \`cargo test -p\`,
   never export CARGO_TARGET_DIR, never touch iroh, iroh-base, iroh-gossip or noq.
4. Run cells only through \`flock -o /tmp/rdm-gate-turn.lock $d/scripts/cells.sh <crate> lib|main -- <filter>\`.
5. A cause is stated only with its span lines; otherwise it is unattributed.
6. End every turn with one line: \`STATUS: <done|blocked|working> inbox <NNNN> commits <sha,…>\`, then what you ran and its result.
AGENT
echo "$ISSUE" > "$s/issue"; echo "$SEAT" > "$s/seat"
gh issue comment "$ISSUE" --repo drlukeangel/rafka-v2 -b "Dispatched to RDM lane $LANE ($d), branch \`$B\`, seat \`$SEAT\` ($MODEL)." >/dev/null
echo "DISPATCHED lane $LANE issue #$ISSUE branch $B"
echo "PLAN $s/plan.md"
echo "SEAT TYPE $SEAT ($state)"
