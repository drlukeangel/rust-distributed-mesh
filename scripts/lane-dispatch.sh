#!/usr/bin/env bash
# lane-dispatch.sh --lane <n> --issue <N> --name <seat-name> --title <title> --design <file> [--model opus|sonnet] [--repo <owner/name>]
# Hands one prepped lane its unit: writes the plan ($HOME/lane-queue/rdm/<n>/plan.md), writes the seat's
# agent type i143-<issue> (the main checkout's .claude/agents/i143-<issue>.md) and comments the issue with the lane,
# branch and seat. Launch the seat ONLY after this returns, with subagent_type = the SEAT TYPE line; a
# NEW type launches after Claude Code's "New agent types are now available" notice names it.
set -euo pipefail
. "$(dirname "$0")/lane-lib.sh"
LANE= ISSUE= NAME= TITLE= DESIGN= MODEL=opus REPO=$ISSUE_REPO
while [ $# -gt 0 ]; do case "$1" in
  --lane) LANE=$2; shift 2;; --issue) ISSUE=$2; shift 2;; --name) NAME=$2; shift 2;;
  --title) TITLE=$2; shift 2;; --design) DESIGN=$2; shift 2;; --model) MODEL=$2; shift 2;; --repo) REPO=$2; shift 2;;
  *) echo "REFUSED: unknown argument $1"; exit 2;; esac; done
for v in LANE ISSUE NAME TITLE DESIGN; do [ -n "${!v}" ] || { echo "REFUSED: --${v,,} is required"; exit 2; }; done
[ -f "$DESIGN" ] || { echo "REFUSED: design $DESIGN does not exist"; exit 2; }
d=$(lane_dir "$LANE"); s=$(state_dir "$LANE")
B=$(cat "$s/branch" 2>/dev/null) || { echo "REFUSED: lane $LANE is not prepped (lane-prep.sh)"; exit 2; }
[ "$(git -C "$d" branch --show-current)" = "$B" ] || { echo "REFUSED: lane $LANE has $(git -C "$d" branch --show-current) checked out, not $B"; exit 3; }
[ -f "$s/seat" ] && { echo "REFUSED: lane $LANE already has seat $(cat "$s/seat"); prep it again for a new unit"; exit 4; }
cd "$MAIN" && gh issue view "$ISSUE" --repo "$REPO" --json state --jq .state | grep -q OPEN || { echo "REFUSED: issue #$ISSUE is not open"; exit 5; }
SEAT="i143-$ISSUE"; SLUG="i143-$ISSUE-$NAME"
{
  printf '# %s\n\nIssue: https://github.com/%s/issues/%s\nLane: %s (branch %s, from %s)\nSeat: %s (%s)\n\n' \
    "$TITLE" "$REPO" "$ISSUE" "$d" "$B" "$(git -C "$d" rev-parse --short HEAD)" "$SEAT" "$NAME"
  cat "$DESIGN"
} > "$s/plan.md.tmp" && mv -f "$s/plan.md.tmp" "$s/plan.md"
f="$AGENTS_DIR/$SEAT.md"; state=registered; [ -f "$f" ] || state=NEW
mkdir -p "$AGENTS_DIR"
# The seat's standing rules: one file in the lane's state, the type's body points at it. The agent ends its
# unit with its own evidence (item 7): its own post-land-gate.sh GREEN, which lane-land.sh needs.
cat > "$s/seat-rules.md" <<RULES
You are the seat for $REPO issue #$ISSUE, "$TITLE", in lane $d on branch $B.

1. Read /home/admin/rust-distributed-mesh/CLAUDE.md and /home/admin/rafka-v2/CLAUDE.md and follow them exactly.
2. Your plan is $s/plan.md. Read every file in $s/inbox/ numbered above the last one you acted on, in order.
3. Work only in $d, only on $B. Never land, never push, never switch branch, never \`cargo test -p\`,
   never export CARGO_TARGET_DIR, never touch iroh, iroh-base, iroh-gossip or noq.
4. Run cells only through \`flock -o /tmp/rdm-gate-turn.lock $d/scripts/cells.sh <crate> lib|main -- <filter>\`.
5. A cause is stated only with its span lines; otherwise it is unattributed.
6. End every turn with one line: \`STATUS: <done|blocked|working> inbox <NNNN> commits <sha,…>\`, then what you ran and its result.
7. End the unit with its evidence, which the lead reviews before anything lands: run \`bash $d/scripts/post-land-gate.sh $LANE\` yourself, not under the turn lock (it rebases on origin/main and proves the tip; resolve rebase conflicts yourself: generated docs/reference/rdm/*.md by regenerating with rdm-docgen, INDEX/PRD rows by keeping both sides); report its GREEN line; the cells you ran with their results; and Jaeger links (http://192.168.68.99:16687/trace/<trace id>, found in ClickHouse otel.otel_traces on 192.168.68.99:18123, which the gate exports to) for the traces that show your fix working. A RED is read and fixed in this lane, never re-run to see if it passes; a red that is another known issue's is reported as that issue.
RULES
# The type (rafka-v2's format): file name, name and subagent_type are i143-<issue>; the slug
# i143-<issue>-<name> rides only in its description and in the launch's description.
printf -- '---\nname: %s\ndescription: %s\nmodel: %s\ntools: Bash, Read, Write, Edit, Glob, Grep, SendMessage\n---\n\n%s\n' \
  "$SEAT" "i143 #$ISSUE — $SLUG" "$MODEL" \
  "Before anything else, read $s/seat-rules.md (the standing seat rules) and follow it for the whole unit. Your launch prompt names your lane and your unit." > "$f"
# The lead session lists a new .claude/agents file only once it reads the folder again (its notice
# "New agent types are now available"); a session that does not reload lists it after it resumes.
# Touch once more, a modify the watcher reliably sees.
[ "$state" = NEW ] && { sleep 5; touch "$f"; }
echo "$ISSUE" > "$s/issue"; echo "$REPO" > "$s/repo"; echo "$SEAT" > "$s/seat"
gh issue comment "$ISSUE" --repo "$REPO" -b "Dispatched to RDM lane $LANE ($d), branch \`$B\`, seat \`$SEAT\` ($MODEL)." >/dev/null
echo "DISPATCHED lane $LANE issue #$ISSUE branch $B"
echo "PLAN $s/plan.md"
if [ "$state" = NEW ]; then
  echo "SEAT TYPE $SEAT: NEW, written to $f. DO NOT LAUNCH YET: launch only after Claude Code's notice \"New agent types are now available ... $SEAT\" (Agent subagent_type: $SEAT, description: \"lane $LANE $SLUG\"). No notice after 2 min: touch $f and wait again."
else
  echo "SEAT TYPE $SEAT: registered; launch NOW (Agent subagent_type: $SEAT, description: \"lane $LANE $SLUG\")"
fi
