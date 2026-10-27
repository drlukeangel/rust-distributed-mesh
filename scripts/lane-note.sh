#!/usr/bin/env bash
# lane-note.sh <lane> <note-file|-> — the one way a lead update reaches a seat: its own numbered file,
# $HOME/lane-queue/rdm/<lane>/inbox/NNNN.md, written atomically. The seat reads every inbox file above
# the last one it acted on and names that number in its STATUS line.
set -euo pipefail
. "$(dirname "$0")/lane-lib.sh"
L="${1:?usage: lane-note.sh <lane> <note-file|->}"; SRC="${2:?usage: lane-note.sh <lane> <note-file|->}"
s=$(state_dir "$L"); [ -f "$s/branch" ] || { echo "REFUSED: lane $L is not prepped (lane-prep.sh)"; exit 2; }
mkdir -p "$s/inbox"
if [ "$SRC" = "-" ]; then note="$(cat)"; else note="$(cat "$SRC")"; fi
[ -n "${note//[[:space:]]/}" ] || { echo "REFUSED: empty note"; exit 2; }
last="$( (ls "$s/inbox" | grep -E '^[0-9]{4}\.md$' || true) | sort | tail -1 | cut -c1-4)"
next="$(printf '%04d' $(( 10#${last:-0} + 1 )))"
tmp="$s/inbox/.$next.tmp.$$"
{ printf '# LEAD update %s — %s\n\n' "$next" "$(date '+%Y-%m-%d %H:%M')"; printf '%s\n' "$note"; } > "$tmp"
mv -f "$tmp" "$s/inbox/$next.md"
echo "NOTED lane $L: $s/inbox/$next.md"
