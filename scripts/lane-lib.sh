# Sourced by the lane verbs. A lane is $HOME/rdm-lanes/<n> (a worktree of the main checkout); its
# lead-side state (plan, inbox, seat, issue) is $HOME/lane-queue/rdm/<n>/, never inside the tree,
# so a seat's tree stays clean for its gate.
LANES_ROOT="${RDM_LANES_ROOT:-$HOME/rdm-lanes}"
STATE_ROOT="${RDM_LANE_STATE_ROOT:-$HOME/lane-queue/rdm}"
MAIN="${RDM_MAIN:-$HOME/rust-distributed-mesh}"
AGENTS_DIR="${RDM_AGENTS_DIR:-$HOME/.claude/agents}"
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
