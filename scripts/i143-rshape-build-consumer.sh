#!/usr/bin/env bash
# i143 R-shape consumer build: materialize the independent consumer workspace at one exact
# candidate RDM commit, build its four node entry points from the lockfile, and leave the
# evidence that the import is exactly that commit.
#
#   scripts/i143-rshape-build-consumer.sh --candidate-sha <40-hex> \
#       --workspace target/i143-rshape/consumer-workspace \
#       --bin-dir   target/i143-rshape/consumer-bin \
#       --output    target/i143-rshape/consumer-build
#
# The fixture is demo (its own Cargo.toml and Cargo.lock; excluded from
# the RDM root workspace). Its RDM dependencies are git dependencies pinned to ONE rev. The
# workspace is a clean copy of the fixture with that rev set to the candidate in Cargo.toml and
# Cargo.lock, then `cargo build --locked --bins`. A lockfile the candidate's dependency set no
# longer satisfies fails `--locked`: the fixture's lock is refreshed deliberately, never here.
#
# Output: manifest.json (candidate, fixture/workspace hashes, binaries and their sha256, the
# executable map), metadata.json (`cargo metadata --locked`), gate.log (the whole build).
# REFUSED, by name and nonzero: a candidate that is not 40-hex, a build that fails, any RDM package
# that does not resolve to the candidate git rev, any path/[patch]/[replace] shortcut, any
# non-RDM git source other than the pinned drlukeangel forks (iroh, iroh-gossip, netwatch, noq), any package outside the approved
# RDM list, a missing binary.
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
unset CARGO_TARGET_DIR
FIXTURE=demo
RDM_URL=https://github.com/drlukeangel/rust-distributed-mesh
BINS="rshape-node-admin rshape-compute rshape-gateway rshape-broker"
refuse() { echo "i143-rshape-build-consumer: REFUSED $*" >&2; exit 1; }

CANDIDATE="" WORKSPACE="" BIN_DIR="" OUTPUT=""
while [ $# -gt 0 ]; do
    case "$1" in
        --candidate-sha) CANDIDATE="${2:-}"; shift 2 ;;
        --workspace) WORKSPACE="${2:-}"; shift 2 ;;
        --bin-dir) BIN_DIR="${2:-}"; shift 2 ;;
        --output) OUTPUT="${2:-}"; shift 2 ;;
        *) refuse "unknown argument '$1'; usage: --candidate-sha <sha> --workspace <dir> --bin-dir <dir> --output <dir>" ;;
    esac
done
[ -n "$CANDIDATE" ] || refuse "no --candidate-sha: the candidate is always named, never an implicit HEAD"
[ -n "$WORKSPACE" ] && [ -n "$BIN_DIR" ] && [ -n "$OUTPUT" ] || refuse "--workspace, --bin-dir and --output are all required"
echo "$CANDIDATE" | grep -qE '^[0-9a-f]{40}$' || refuse "--candidate-sha '$CANDIDATE' is not a 40-hex commit"
case "$WORKSPACE" in "$FIXTURE"|"$FIXTURE"/*|/|.|"") refuse "--workspace '$WORKSPACE' is the fixture itself or the root; it is a clean copy" ;; esac

FIXTURE_REV=$(grep -ohE 'rev = "[0-9a-f]{40}"' "$FIXTURE/Cargo.toml" "$FIXTURE/admin-ui/Cargo.toml" | sort -u)
[ "$(echo "$FIXTURE_REV" | wc -l)" = 1 ] && [ -n "$FIXTURE_REV" ] || refuse "$FIXTURE/Cargo.toml and $FIXTURE/admin-ui/Cargo.toml pin more than one rev (or none): $FIXTURE_REV"
FIXTURE_REV=${FIXTURE_REV#rev = \"}; FIXTURE_REV=${FIXTURE_REV%\"}

rm -rf "$WORKSPACE" "$BIN_DIR" "$OUTPUT"
mkdir -p "$WORKSPACE" "$BIN_DIR" "$OUTPUT"
LOG="$OUTPUT/gate.log"
materialize_and_build() {
    echo "candidate=$CANDIDATE fixture_rev=$FIXTURE_REV workspace=$WORKSPACE"
    # The fixture is a workspace of the consumer and the admin UI; the UI's web app is not part of
    # the Rust build (it is built with npm).
    (cd "$FIXTURE" && tar --exclude=./target --exclude=./admin-ui/web -cf - .) | tar -xf - -C "$WORKSPACE" || return 1
    if [ "$FIXTURE_REV" != "$CANDIDATE" ]; then
        sed -i "s/$FIXTURE_REV/$CANDIDATE/g" "$WORKSPACE/Cargo.toml" "$WORKSPACE/admin-ui/Cargo.toml" "$WORKSPACE/Cargo.lock" || return 1
    fi
    cargo build --locked --manifest-path "$WORKSPACE/Cargo.toml" --workspace --bins || return 1
    # The admin UI's own tests (its flows run against node-admin's real control router).
    cargo test --locked --manifest-path "$WORKSPACE/Cargo.toml" -p rafka-admin-ui
}
materialize_and_build > "$LOG" 2>&1 || { tail -20 "$LOG" >&2; refuse "the consumer did not materialize or build at $CANDIDATE (see $LOG)"; }

cargo metadata --locked --format-version 1 --manifest-path "$WORKSPACE/Cargo.toml" > "$OUTPUT/metadata.json" 2>> "$LOG" || refuse "cargo metadata --locked failed (see $LOG)"

# The import proof: every RDM package resolves to the one candidate git rev, nothing else is a
# shortcut, and no package outside the approved RDM crate list is present.
python3 -I - "$OUTPUT/metadata.json" "$CANDIDATE" "$RDM_URL" <<'PY' || exit 1
import json, sys
meta, cand, url = sys.argv[1:4]
m = json.load(open(meta))
APPROVED = {"rafka-node-base", "rafka-node-admin-core", "rafka-mesh-transport", "rafka-mesh-telemetry", "rafka-node-rpc-testkit",
            "rafka-node-rpc", "rafka-node-rpc-contract", "rafka-node-admin-client", "rafka-mesh-entity", "rafka-chaos"}
bad = []
rdm = []
for p in m["packages"]:
    src = p.get("source")
    if src is None:
        if p["name"] not in ("rshape-consumer", "rafka-admin-ui"):
            bad.append(f"path package {p['name']} at {p['manifest_path']}")
        continue
    if src.startswith("git+"):
        base = src[4:].split("?")[0].split("#")[0]
        if base == url:
            rev = src.split("#")[-1]
            if rev != cand or f"rev={cand}" not in src:
                bad.append(f"{p['name']} resolves to {src}, not rev {cand}")
            if p["name"] not in APPROVED:
                bad.append(f"{p['name']} is an RDM package outside the approved import list")
            rdm.append(p["name"])
        elif base not in ("https://github.com/drlukeangel/iroh-gossip", "https://github.com/drlukeangel/iroh", "https://github.com/drlukeangel/netwatch", "https://github.com/drlukeangel/noq"):
            bad.append(f"{p['name']} comes from the unapproved git source {src}")
for need in ("rafka-node-base", "rafka-node-admin-core", "rafka-node-rpc-testkit"):
    if need not in rdm:
        bad.append(f"{need} is not imported from the candidate")
if bad:
    print("i143-rshape-build-consumer: REFUSED inconsistent import:\n  " + "\n  ".join(bad), file=sys.stderr)
    sys.exit(1)
print("import proof ok:", len(rdm), "RDM packages at", cand)
PY
grep -qE '^\[(patch|replace)' "$WORKSPACE/Cargo.toml" && refuse "$WORKSPACE/Cargo.toml carries [patch]/[replace]"

TARGET=$(jq -r .target_directory "$OUTPUT/metadata.json")
for b in $BINS; do
    [ -x "$TARGET/debug/$b" ] || refuse "binary $b was not built at $TARGET/debug/$b"
    cp "$TARGET/debug/$b" "$BIN_DIR/$b"
done
# The demo's admin UI binary rides beside the node binaries (not part of the hashed set).
[ -x "$TARGET/debug/rafka-admin-ui" ] || refuse "binary rafka-admin-ui was not built at $TARGET/debug/rafka-admin-ui"
cp "$TARGET/debug/rafka-admin-ui" "$BIN_DIR/rafka-admin-ui"
hashes=$(for b in $BINS; do printf '%s\t%s\n' "$b" "$(sha256sum "$BIN_DIR/$b" | cut -d' ' -f1)"; done | jq -R -s 'split("\n")[:-1] | map(split("\t") | {(.[0]): .[1]}) | add')
src_hash=$(cd "$WORKSPACE" && find src admin-ui/src -type f | sort | xargs sha256sum | sha256sum | cut -d' ' -f1)
jq -n --arg cand "$CANDIDATE" --arg url "$RDM_URL" --arg fixture_rev "$FIXTURE_REV" --argjson bins "$hashes" \
    --arg toml "$(sha256sum "$WORKSPACE/Cargo.toml" | cut -d' ' -f1)" --arg lock "$(sha256sum "$WORKSPACE/Cargo.lock" | cut -d' ' -f1)" \
    --arg src "$src_hash" --arg rustc "$(rustc --version)" --arg cargo "$(cargo --version)" --arg bindir "$BIN_DIR" --arg ws "$WORKSPACE" \
    --argjson rdm "$(jq '[.packages[] | select((.source // "") | startswith("git+https://github.com/drlukeangel/rust-distributed-mesh")) | {name, version, source}] | sort_by(.name)' "$OUTPUT/metadata.json")" \
    '{candidate_sha:$cand, rdm_url:$url, fixture_pinned_rev:$fixture_rev, workspace:$ws, consumer_manifest_sha256:$toml, consumer_lockfile_sha256:$lock, consumer_source_sha256:$src,
      toolchain:{rustc:$rustc, cargo:$cargo}, bin_dir:$bindir, binaries:$bins,
      executable_map:{node_admin:"rshape-node-admin", compute:"rshape-compute", gateway:"rshape-gateway", broker:"rshape-broker"},
      rdm_packages:$rdm}' > "$OUTPUT/manifest.json"
echo "i143-rshape-build-consumer: ok candidate=$CANDIDATE binaries=$BIN_DIR manifest=$OUTPUT/manifest.json"
