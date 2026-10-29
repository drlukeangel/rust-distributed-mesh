#!/usr/bin/env bash
# rdm-load.sh <profile> [--debug]: run one load profile (docs/eng/testing-load-harness.md).
#
# The nodes under load are a release build of the lane's commit (cargo build --release --bins), the
# estate pointed at them with RDM_BIN_DIR; --debug runs on the gate's debug build instead. The cell is
# rdm-test's load_profile_<profile>_baseline, ignored by every gate and run here with --ignored. Its rows
# are appended to $HOME/rust-distributed-mesh-deploy/load/<short sha>.jsonl. No lock is taken; the rows
# record the box's load and busy lanes.
set -euo pipefail
cd "$(dirname "$0")/.."
profile="${1:?usage: rdm-load.sh <L1|L2|L3|L4> [--debug]}"
build="${2:-}"
lower=$(echo "$profile" | tr 'A-Z' 'a-z')
if [ "$build" = "--debug" ]; then
    bins="$PWD/target/debug"
else
    echo "rdm-load: building the release node binaries at $(git rev-parse --short HEAD)"
    cargo build --release --bins -q
    bins="$PWD/target/release"
fi
echo "rdm-load: profile $profile, nodes from $bins"
RDM_BIN_DIR="$bins" scripts/cells.sh rdm-test main -- "load::load_profile_${lower}_baseline" --exact --ignored --nocapture 2>&1 | grep -E '^\[load\]|^test |test result|panicked|missed'
