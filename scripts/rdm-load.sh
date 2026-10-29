#!/usr/bin/env bash
# rdm-load.sh <profile> [--debug] [--no-evidence]: run one load profile (docs/eng/testing-load-harness.md).
#
# The nodes under load are a release build of the lane's commit (cargo build --release --bins), the
# estate pointed at them with RDM_BIN_DIR; --debug runs on the gate's debug build instead; --no-evidence
# runs the profile with no span evidence written (load_profile_<profile>_no_evidence); --perf=<dir> records a
# CPU profile of one node of each role over M8's idle window into <dir> (its rows are marked profiled). The cell is
# rdm-test's load_profile_<profile>_baseline, ignored by every gate and run here with --ignored. Its rows
# are appended to $HOME/rust-distributed-mesh-deploy/load/<short sha>.jsonl. No lock is taken; the rows
# record the box's load and busy lanes.
set -euo pipefail
cd "$(dirname "$0")/.."
profile="${1:?usage: rdm-load.sh <L1|L2|L3|L4> [--debug] [--no-evidence] [--perf=<dir>]}"
shift
build="" cell=baseline
for a in "$@"; do
    case "$a" in
        --debug) build=--debug ;;
        --no-evidence) cell=no_evidence ;;
        --perf=*) export RDM_LOAD_PERF="${a#--perf=}" ;;
        *) echo "rdm-load: unknown argument $a"; exit 2 ;;
    esac
done
lower=$(echo "$profile" | tr 'A-Z' 'a-z')
if [ "$build" = "--debug" ]; then
    bins="$PWD/target/debug"
else
    echo "rdm-load: building the release node binaries at $(git rev-parse --short HEAD)"
    cargo build --release --bins -q
    bins="$PWD/target/release"
fi
# The production cadence (load-testing.md "Conditions"; Q267): never the tests' fast cadence, which cells.sh
# exports for every other run. Each row records the cadence it ran at.
export RDM_STALENESS_MS=30000 RDM_GOSSIP_INTERVAL_MS=2000 RDM_BACKBONE_INTERVAL_MS=5000
echo "rdm-load: profile $profile, nodes from $bins, cadence staleness ${RDM_STALENESS_MS} ms, gossip ${RDM_GOSSIP_INTERVAL_MS} ms, backbone ${RDM_BACKBONE_INTERVAL_MS} ms"
# The whole output is kept beside the rows, so a red is read from it and never run again to be read;
# the terminal gets the rows, the verdict and each panic with the message on the lines that follow it.
log="$HOME/rust-distributed-mesh-deploy/load/$(git rev-parse --short HEAD)-${lower}-${cell}.log"
mkdir -p "$(dirname "$log")"
echo "rdm-load: the whole output is kept at $log"
RDM_BIN_DIR="$bins" scripts/cells.sh rdm-test main -- "load::load_profile_${lower}_${cell}" --exact --ignored --nocapture 2>&1 | tee "$log" \
    | awk '/panicked at/ { print; after = 4; next } after > 0 { print; after--; next } /^\[load\]|^test |test result|missed| did not |assertion/ { print }'
