#!/usr/bin/env bash
# scripts/docs-site.sh [--check]
#
# The docs site, built locally from this repository (there is no CI step, Q262): the guide (mdBook,
# docs/ with docs/SUMMARY.md) and the API docs (rustdoc) side by side in target/docs-site, the guide
# linking the API docs.
#
#   1. tools       mdbook, mdbook-mermaid and mdbook-linkcheck2 are installed
#   2. regenerate  rdm-docgen writes docs/reference/rdm (spans.md, config.md) from this tree; a
#                  generated page that differs from what is committed (or is untracked) is listed
#   3. api         RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace, copied to
#                  target/docs-site/api
#   4. guide       mdbook build: target/docs-site/guide (book.toml build-dir), with mdbook-mermaid's
#                  two scripts written into target/docs-site/mermaid from the installed mdbook-mermaid
#   5. links       mdbook-linkcheck2 --standalone: every dead link is listed and counted, and those in
#                  generated pages (docs/reference/rdm) apart; a link to the API docs leaves the book
#                  and is checked against target/docs-site/api instead, as the API docs' links into
#                  the guide are checked against target/docs-site/guide
#   6. index       target/docs-site/index.html links the guide and the API docs
#
# Exit: 0 built; 1 (--check) a generated page differs from what is committed; 2 rdm-docgen failed;
# 3 mdbook build failed; 4 (--check) a dead link, or mdbook-linkcheck2 failed without reporting one
# (it could not check); 5 rustdoc failed; 64 bad usage or a missing tool. Without --check a dirty
# generated page and a dead link in a hand-written page are reported, never a failure; a dead link in
# a generated page, or between the guide and the API docs, always fails (4).
# Tools: cargo install mdbook mdbook-mermaid mdbook-linkcheck2 (mdbook-linkcheck 0.7 cannot read an
# mdbook 0.5 render context; linkcheck2 is its maintained fork).
set -uo pipefail

CHECK=0
case "${1:-}" in
    "") ;;
    --check) CHECK=1 ;;
    *) echo "usage: scripts/docs-site.sh [--check]" >&2; exit 64 ;;
esac
RDM=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SITE="$RDM/target/docs-site"
for tool in mdbook mdbook-mermaid mdbook-linkcheck2; do
    command -v "$tool" >/dev/null || { echo "docs-site: $tool is not installed (cargo install $tool)" >&2; exit 64; }
done
GENERATED=docs/reference/rdm
mkdir -p "$SITE"

echo "docs-site: regenerating $GENERATED with rdm-docgen"
if ! (cd "$RDM" && cargo run -q -p rdm-docgen -- --root "$RDM" --out "$RDM/$GENERATED"); then
    echo "docs-site: rdm-docgen failed" >&2
    exit 2
fi
dirty=$(git -C "$RDM" status --porcelain --untracked-files=all -- "$GENERATED")
if [ -n "$dirty" ]; then
    echo "docs-site: the generated pages differ from what is committed:" >&2
    echo "$dirty" >&2
    [ "$CHECK" -eq 1 ] && exit 1
else
    echo "docs-site: the generated pages match what is committed"
fi

echo "docs-site: building the API docs (rustdoc)"
if ! (cd "$RDM" && RUSTDOCFLAGS="-D warnings" cargo doc -q --no-deps --workspace); then
    echo "docs-site: rustdoc failed" >&2
    exit 5
fi
rm -rf "$SITE/api" && cp -r "$RDM/target/doc" "$SITE/api"

echo "docs-site: building the guide (mdBook)"
m=$(mktemp -d) && printf '[book]\n' > "$m/book.toml"
if ! mdbook-mermaid install "$m" >/dev/null; then
    rm -rf "$m"; echo "docs-site: mdbook-mermaid install failed" >&2; exit 3
fi
rm -rf "$SITE/mermaid" && mkdir -p "$SITE/mermaid" && cp "$m"/mermaid.min.js "$m"/mermaid-init.js "$SITE/mermaid/"; rm -rf "$m"
if ! mdbook build "$RDM"; then
    echo "docs-site: mdbook build failed" >&2
    exit 3
fi

# The check reads the book's own config plus its exclusions: a link to the API docs (`../api/...`)
# leaves the book and is checked below against what rustdoc wrote.
LOG="$SITE/linkcheck.log"
lcroot=$(mktemp -d)
sed "s#^src = \"docs\"#src = \"$RDM/docs\"#" "$RDM/book.toml" > "$lcroot/book.toml"
printf '\n[output.linkcheck2]\nexclude = [%s]\n' "'^(\\.\\./)+api/'" >> "$lcroot/book.toml"
mdbook-linkcheck2 --standalone --colour never "$lcroot" > "$LOG" 2>&1
lc=$?
rm -rf "$lcroot"
dead=$(grep -c '^error:' "$LOG")
if [ "$lc" -ne 0 ] && [ "$dead" -eq 0 ]; then
    echo "docs-site: mdbook-linkcheck2 exited $lc without reporting a dead link (it could not check); see $LOG" >&2
    tail -20 "$LOG" >&2
    exit 4
fi
# A dead link's report names its page on the line after the `error:` line (`┌─ <page>:<line>:<col>`).
generated_dead=$(grep -A1 '^error:' "$LOG" | grep -cE "─ ${GENERATED#docs/}/")
echo "docs-site: $dead dead links ($generated_dead in generated pages), listed in $LOG"
grep '^error:' "$LOG" | sort | uniq -c | sort -rn | head -20

# The guide's links to the API docs (`../api/...`, excluded from the book's check): each names a file
# the rustdoc build wrote.
api_dead=0
while IFS= read -r line; do
    page=${line%%:*}; href=${line#*:}
    html="$SITE/guide/${page%.md}.html"
    target=$(cd "$(dirname "$html")" && realpath -m "$href")
    [ -f "$target" ] || { echo "docs-site: dead API link in docs/$page: $href" >&2; api_dead=$((api_dead + 1)); }
done < <(cd "$RDM/docs" && grep -roP --include='*.md' --exclude-dir=plans '\]\(\K(\.\./)+api/[^)#]+' . | sed 's#^\./##')
# And the API docs' links into the guide (a crate's front page names its guide pages).
while IFS= read -r line; do
    page=${line%%:*}; href=${line#*:}
    target=$(cd "$(dirname "$page")" && realpath -m "$href")
    [ -f "$target" ] || { echo "docs-site: dead guide link in ${page#"$SITE"/}: $href" >&2; api_dead=$((api_dead + 1)); }
done < <(grep -roP --include='*.html' 'href="\K(\.\./)+guide/[^"#]+' "$SITE/api" | sort -u)
echo "docs-site: $api_dead dead links between the guide and the API docs"

cat > "$SITE/index.html" <<'HTML'
<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>RDM: rust-distributed-mesh</title></head>
<body>
<h1>RDM: rust-distributed-mesh</h1>
<ul>
<li><a href="guide/index.html">The guide</a>: the architecture, the workflows, building an application, running and testing a fabric</li>
<li><a href="guide/reference/api.html">The API documentation</a> (rustdoc): every crate's front page and types</li>
</ul>
</body></html>
HTML

if [ "$generated_dead" -gt 0 ] || [ "$api_dead" -gt 0 ] || { [ "$CHECK" -eq 1 ] && [ "$dead" -gt 0 ]; }; then
    exit 4
fi
echo "file://$SITE/index.html"
