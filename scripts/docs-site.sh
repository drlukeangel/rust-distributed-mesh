#!/usr/bin/env bash
# scripts/docs-site.sh <rafka-v2 checkout>
#
# The docs site, built locally (there is no CI step): regenerate rafka-v2's RDM reference pages and
# its SUMMARY from this tree with rafka-docgen, check them against what rafka-v2 holds committed,
# build the mdBook site, and list its dead links.
#
#   1. regenerate  cargo run -p rafka-docgen -- --out <rafka-v2>/docs/reference/rdm --summary <rafka-v2>/docs
#   2. check       the generated paths are clean in rafka-v2: no diff and nothing untracked (a new
#                  generated page that was never committed fails the check as a changed one does)
#   3. build       mdbook build <rafka-v2>  (html with the mermaid preprocessor; book.toml sets
#                  build-dir = target/docs-site)
#   4. links       mdbook-linkcheck2 --standalone: every dead link is listed and counted; dead links
#                  in hand-written pages are reported, never a failure
#   5. print       the site's directory
#
# Exit: 0 regenerated clean and built; 1 the generated pages differ from rafka-v2's committed ones;
# 2 rafka-docgen failed; 3 mdbook build failed; 4 mdbook-linkcheck2 failed without reporting a dead
# link (it could not check); 64 bad usage or a missing tool.
# Tools: cargo install mdbook mdbook-mermaid mdbook-linkcheck2 (mdbook-linkcheck 0.7 cannot read an
# mdbook 0.5 render context; linkcheck2 is its maintained fork).
set -uo pipefail

usage() {
    echo "usage: scripts/docs-site.sh <rafka-v2 checkout>" >&2
    exit 64
}
[ $# -eq 1 ] || usage
RV2=$(cd "$1" 2>/dev/null && pwd) || { echo "docs-site: no directory $1" >&2; exit 64; }
[ -f "$RV2/book.toml" ] || { echo "docs-site: $RV2 has no book.toml" >&2; exit 64; }
RDM=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
for tool in mdbook mdbook-mermaid mdbook-linkcheck2; do
    command -v "$tool" >/dev/null || { echo "docs-site: $tool is not installed (cargo install $tool)" >&2; exit 64; }
done
GENERATED=(docs/reference/rdm docs/SUMMARY.md)

echo "docs-site: regenerating ${GENERATED[*]} in $RV2 from $RDM"
if ! (cd "$RDM" && cargo run -q -p rafka-docgen -- --root "$RDM" --out "$RV2/docs/reference/rdm" --summary "$RV2/docs"); then
    echo "docs-site: rafka-docgen failed" >&2
    exit 2
fi

dirty=$(git -C "$RV2" status --porcelain --untracked-files=all -- "${GENERATED[@]}")
if [ -n "$dirty" ]; then
    echo "docs-site: the generated pages differ from what $RV2 has committed:" >&2
    echo "$dirty" >&2
    git -C "$RV2" --no-pager diff --stat -- "${GENERATED[@]}" >&2
    exit 1
fi
echo "docs-site: the generated pages match what $RV2 has committed"

if ! mdbook build "$RV2"; then
    echo "docs-site: mdbook build failed" >&2
    exit 3
fi

LOG="$RV2/target/docs-site-linkcheck.log"
mdbook-linkcheck2 --standalone --colour never "$RV2" > "$LOG" 2>&1
lc=$?
dead=$(grep -c '^error:' "$LOG")
if [ "$lc" -ne 0 ] && [ "$dead" -eq 0 ]; then
    echo "docs-site: mdbook-linkcheck2 exited $lc without reporting a dead link (it could not check); see $LOG" >&2
    tail -20 "$LOG" >&2
    exit 4
fi
echo "docs-site: $dead dead links (listed in $LOG)"
grep '^error:' "$LOG" | sort | uniq -c | sort -rn | head -20

echo "$RV2/target/docs-site"
