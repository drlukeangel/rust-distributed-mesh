# Sourced by the lane verbs. A lane is $HOME/rdm-lanes/<n> (a worktree of the main checkout); its
# lead-side state (plan, inbox, seat, issue) is $HOME/lane-queue/rdm/<n>/, never inside the tree,
# so a seat's tree stays clean for its gate.
LANES_ROOT="${RDM_LANES_ROOT:-$HOME/rdm-lanes}"
STATE_ROOT="${RDM_LANE_STATE_ROOT:-$HOME/lane-queue/rdm}"
MAIN="${RDM_MAIN:-$HOME/rust-distributed-mesh}"
DEPLOY_ROOT="${RDM_DEPLOY:-$HOME/rust-distributed-mesh-deploy}"
# Seat agent types live in the MAIN checkout's .claude/agents, where the lead's Claude Code session reads them.
AGENTS_DIR="${RDM_AGENTS_DIR:-$MAIN/.claude/agents}"
# The repo a lane's issue lives in: RDM's own (Luke), unless lane-dispatch.sh --repo recorded another in
# $HOME/lane-queue/rdm/<n>/repo; a lane with no repo file was dispatched before the move and is rafka-v2.
ISSUE_REPO="${RDM_ISSUE_REPO:-drlukeangel/rust-distributed-mesh}"
lane_dir() { echo "$LANES_ROOT/$1"; }
state_dir() { echo "$STATE_ROOT/$1"; }
# A lane is busy while any process runs with its cwd inside the lane (a build, a cell, a gate, a node).
# The verb's own shell and its ancestors are not counted, so a verb run from inside a lane does not see itself.
lane_busy() {
  local d p mine=" " n=0; d=$(lane_dir "$1"); p=$BASHPID; cd /
  while [ "${p:-0}" -gt 1 ]; do mine="$mine$p "; p=$(awk '{print $4}' "/proc/$p/stat" 2>/dev/null); done
  for c in /proc/[0-9]*/cwd; do
    p=${c#/proc/}; p=${p%/cwd}
    case "$mine" in *" $p "*) continue;; esac
    case "$(readlink "$c" 2>/dev/null)" in "$d"|"$d"/*) n=$((n+1));; esac
  done; echo $n; }
lane_dirty() { git -C "$(lane_dir "$1")" status --porcelain --untracked-files=no | wc -l; }
# A lane whose folder moved keeps test binaries built with the old CARGO_MANIFEST_DIR compiled in
# (env!), and cargo does not rebuild them: every test reading its own source tree then fails NotFound
# (2026-10-27, lane 1 after e6s7 -> 1). target/.lane-path records where the target was built; on a
# mismatch the workspace's own crates are cleaned (vendor crates keep their cache) and rebuilt once.
lane_target_at_path() {
  local d t members; d=$(lane_dir "$1"); t="$d/target"
  [ -d "$t" ] || { echo "$d" > /dev/null; return 0; }
  [ "$(cat "$t/.lane-path" 2>/dev/null)" = "$d" ] && return 0
  members=$(cd "$d" && cargo metadata --no-deps --format-version 1 2>/dev/null | python3 -c 'import json,sys; print(" ".join("-p "+p["name"] for p in json.load(sys.stdin)["packages"]))')
  [ -n "$members" ] || { echo "REFUSED: cargo metadata failed in $d"; return 1; }
  echo "lane $1: target was built at $(cat "$t/.lane-path" 2>/dev/null || echo 'an unrecorded path'); cleaning the workspace crates"
  (cd "$d" && cargo clean $members >/dev/null 2>&1) || { echo "REFUSED: cargo clean failed in $d"; return 1; }
  echo "$d" > "$t/.lane-path"
}
# A seat is never launched into a cold lane (Luke 2026-10-27): build the gate's graph (every workspace
# member's bins and tests, rdm-gate-at.sh step 1) under the box's turn lock before the lane is handed
# out. Vendor crates come from sccache; only the lane's own crates compile.
lane_warm() {
  local d members; d=$(lane_dir "$1")
  members=$(cd "$d" && cargo metadata --no-deps --format-version 1 2>/dev/null | python3 -c 'import json,sys; print(" ".join("-p "+p["name"] for p in json.load(sys.stdin)["packages"]))')
  [ -n "$members" ] || { echo "REFUSED: cargo metadata failed in $d"; return 1; }
  local s=$(date +%s)
  (cd "$d" && flock -o /tmp/rdm-gate-turn.lock env CARGO_BUILD_INCREMENTAL=true cargo build --tests --bins $members >/dev/null 2>&1) \
    || { echo "REFUSED: lane $1 does not build at $(git -C "$d" rev-parse --short HEAD)"; return 1; }
  echo "lane $1: warm at $(git -C "$d" rev-parse --short HEAD) in $(( $(date +%s)-s ))s"
}
lane_repo() { cat "$(state_dir "$1")/repo" 2>/dev/null || echo drlukeangel/rafka-v2; }
