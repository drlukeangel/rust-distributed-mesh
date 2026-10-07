#!/usr/bin/env bash
# i143 acceptance gate: run one registered job from the RDM root and leave its receipt.
#
#   scripts/i143-acceptance-gate.sh <job-id>
#
# The registry is tools/mesh-audit/i143-acceptance-jobs.json (I143_ACCEPTANCE_JOBS overrides it
# for the runner's own cells). Every cell of the job runs in order; each one is refused BY NAME
# when its command matches no test, leaves a test ignored, fails, leaves no result.json (a UNIT
# cell: no spans.json; an estate cell: no estate manifest), ran on a provider other than the
# job's layer, or the source SHA moved while it ran. A job with an estate layer (process,
# container, fast) builds the estate binaries first so they match HEAD; a container job
# requires a reachable Docker domain and sets RAFKA_REQUIRE_CONTAINER=1, so a host that cannot
# run containers fails by name instead of substituting the process provider.
#
# Per cell (dir = target/i143-acceptance/<issue>/<layer>/<cell>): gate.log (the command's whole
# output), manifest.json (job, cell, layer, issue, source SHA, dirty state, command, provider,
# start, finish, outcome). The cell itself writes result.json (and a UNIT cell spans.json) into
# $I143_ACCEPTANCE_DIR, exported for it with $I143_ACCEPTANCE_CELL. Per job:
# target/i143-acceptance/jobs/<job-id>.json lists every executed cell, its outcome and every
# artifact path with its sha256.
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
unset CARGO_TARGET_DIR
REG="${I143_ACCEPTANCE_JOBS:-tools/mesh-audit/i143-acceptance-jobs.json}"
JOB="${1:-}"
refuse() { echo "i143-acceptance-gate: REFUSED $*" >&2; }
[ -n "$JOB" ] || { refuse "no job id given; usage: scripts/i143-acceptance-gate.sh <job-id>"; exit 2; }
[ -f "$REG" ] || { refuse "$JOB: registry $REG does not exist"; exit 2; }
jq -e --arg j "$JOB" '.jobs[$j]' "$REG" > /dev/null 2>&1 || { refuse "$JOB: not a job in $REG"; exit 2; }
ISSUE=$(jq -r --arg j "$JOB" '.jobs[$j].issue' "$REG")
LAYER=$(jq -r --arg j "$JOB" '.jobs[$j].layer' "$REG")
NCELLS=$(jq -r --arg j "$JOB" '.jobs[$j].cells | length' "$REG")
[ "$NCELLS" -gt 0 ] || { refuse "$JOB: registers no cell"; exit 2; }
SHA=$(git rev-parse HEAD)
DIRTY=$(git status --porcelain | wc -l | tr -d ' ')
JOBS_DIR=target/i143-acceptance/jobs
mkdir -p "$JOBS_DIR"
RECEIPT="$JOBS_DIR/$JOB.json"
STARTED=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
now() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }

# The estate binaries, built once per job so every launched admin and node is this SHA.
case "$LAYER" in
    process|container|fast)
        if [ -z "${I143_ACCEPTANCE_SKIP_BUILD:-}" ]; then
            if ! cargo build -p rafka-node-admin-core -p rafka-node-rpc-testkit --bins > "$JOBS_DIR/$JOB.build.log" 2>&1; then
                refuse "$JOB: the estate binaries did not build (see $JOBS_DIR/$JOB.build.log)"
                exit 1
            fi
        fi
        ;;
    unit|export) ;;
    *) refuse "$JOB: layer $LAYER is not unit, process, container, fast or export"; exit 2 ;;
esac
if [ "$LAYER" = container ]; then
    export RAFKA_REQUIRE_CONTAINER=1
    if ! docker info > /dev/null 2>&1; then
        refuse "$JOB: no reachable Docker domain (docker info failed; set DOCKER_HOST or start the daemon)"
        exit 1
    fi
fi

CELLS_JSON="[]"
FAILED=0
for i in $(seq 0 $((NCELLS - 1))); do
    cell=$(jq -r --arg j "$JOB" --argjson i "$i" '.jobs[$j].cells[$i].name' "$REG")
    command=$(jq -r --arg j "$JOB" --argjson i "$i" '.jobs[$j].cells[$i].command' "$REG")
    dir=$(jq -r --arg j "$JOB" --argjson i "$i" '.jobs[$j].cells[$i].dir' "$REG")
    mkdir -p "$dir"
    rm -f "$dir/gate.log" "$dir/manifest.json" "$dir/result.json" "$dir/spans.json" "$dir"/*.spans.jsonl
    start=$(now)
    # The cell's directory is handed over absolute: a test binary runs in its crate's directory,
    # not the root the registry's paths are relative to.
    case "$dir" in /*) cell_dir="$dir" ;; *) cell_dir="$ROOT/$dir" ;; esac
    I143_ACCEPTANCE_DIR="$cell_dir" I143_ACCEPTANCE_CELL="$cell" bash -c "$command" > "$dir/gate.log" 2>&1
    rc=$?
    finish=$(now)
    # An existing stem run as a regression (`evidence: runner`) writes no result of its own: the
    # runner records the test summary, the estate manifest and every spans file for it.
    evidence=$(jq -r --arg j "$JOB" --argjson i "$i" '.jobs[$j].cells[$i].evidence // "cell"' "$REG")
    if [ "$evidence" = runner ] && [ ! -s "$dir/result.json" ]; then
        summary_line=$(grep -E '^test result: ' "$dir/gate.log" | tail -1)
        estate_manifest=$(find "$dir/estate" -name manifest.json 2>/dev/null | head -1)
        spans_json="[]"
        for f in $(find "$dir/estate" -name '*.spans.jsonl' 2>/dev/null | sort); do
            spans_json=$(echo "$spans_json" | jq --arg p "$f" --argjson n "$(wc -l < "$f")" --arg h "$(sha256sum "$f" | cut -d' ' -f1)" '. + [{path:$p, spans:$n, sha256:$h}]')
        done
        jq -n --arg cell "$cell" --arg test "$(jq -r --arg j "$JOB" --argjson i "$i" '.jobs[$j].cells[$i].test // .jobs[$j].cells[$i].name' "$REG")" \
              --arg summary "$summary_line" --argjson rc "$rc" --arg manifest "$estate_manifest" --argjson spans "$spans_json" \
              --argjson estate "$( [ -n "$estate_manifest" ] && cat "$estate_manifest" || echo null )" \
              '{cell:$cell, test:$test, evidence:"runner", exit:$rc, summary:$summary, estate_manifest:$manifest, estate:$estate, spans_files:$spans}' > "$dir/result.json"
    fi
    reason=""
    if grep -qE '^running 0 tests' "$dir/gate.log"; then
        reason="the command matched no test (running 0 tests)"
    else
        summary=$(grep -E '^test result: ' "$dir/gate.log" | tail -1)
        passed=$(echo "$summary" | sed -nE 's/.* ([0-9]+) passed.*/\1/p')
        failed=$(echo "$summary" | sed -nE 's/.* ([0-9]+) failed.*/\1/p')
        ignored=$(echo "$summary" | sed -nE 's/.* ([0-9]+) ignored.*/\1/p')
        if [ -z "$summary" ]; then
            err=$(grep -m1 -E '^error(\[E[0-9]+\])?: ' "$dir/gate.log")
            reason="no test summary line in gate.log (exit $rc${err:+; $err})"
        elif [ "${ignored:-0}" != 0 ]; then
            reason="the cell was ignored ($summary)"
        elif [ "${failed:-0}" != 0 ] || [ "$rc" -ne 0 ]; then
            reason="the cell failed (exit $rc; $summary)"
        elif [ "${passed:-0}" != 1 ]; then
            reason="the command ran ${passed:-0} tests, not exactly this cell ($summary)"
        elif [ ! -s "$dir/result.json" ] || ! jq -e . "$dir/result.json" > /dev/null 2>&1; then
            reason="the cell left no result.json in $dir"
        fi
    fi
    provider=""
    if [ -z "$reason" ]; then
        case "$LAYER" in
            unit)
                if [ ! -s "$dir/spans.json" ] || ! jq -e . "$dir/spans.json" > /dev/null 2>&1; then
                    reason="the UNIT cell left no spans.json in $dir"
                fi
                ;;
            process|container|fast)
                estate_manifest=$(find "$dir/estate" -name manifest.json 2>/dev/null | head -1)
                if [ -z "$estate_manifest" ]; then
                    reason="the cell left no estate manifest under $dir/estate"
                else
                    provider=$(jq -r '.provider // empty' "$estate_manifest")
                    want=$LAYER; [ "$want" = fast ] && want=process
                    if [ "$provider" != "$want" ]; then
                        reason="provider substitution: the estate ran on '${provider:-none}', the job's layer is $want"
                    fi
                fi
                ;;
        esac
    fi
    if [ -z "$reason" ] && [ "$(git rev-parse HEAD)" != "$SHA" ]; then
        reason="the source SHA moved while the cell ran (started at $SHA)"
    fi
    outcome=ok
    if [ -n "$reason" ]; then
        outcome=refused
        FAILED=$((FAILED + 1))
        refuse "$JOB/$cell: $reason"
    fi
    jq -n --arg job "$JOB" --arg cell "$cell" --arg layer "$LAYER" --argjson issue "$ISSUE" --arg sha "$SHA" --argjson dirty "$DIRTY" \
          --arg command "$command" --arg provider "$provider" --arg start "$start" --arg finish "$finish" --arg outcome "$outcome" --arg reason "$reason" \
          '{job:$job, cell:$cell, layer:$layer, issue:$issue, source_sha:$sha, dirty_paths:$dirty, command:$command, provider:$provider, started:$start, finished:$finish, outcome:$outcome, refusal:(if $reason == "" then null else $reason end)}' \
          > "$dir/manifest.json"
    artifacts="{}"
    for f in $(find "$dir" -type f | sort); do
        h=$(sha256sum "$f" | cut -d' ' -f1)
        artifacts=$(echo "$artifacts" | jq --arg p "$f" --arg h "$h" '. + {($p): $h}')
    done
    CELLS_JSON=$(echo "$CELLS_JSON" | jq --arg cell "$cell" --arg outcome "$outcome" --arg reason "$reason" --argjson artifacts "$artifacts" \
        '. + [{name:$cell, outcome:$outcome, refusal:(if $reason == "" then null else $reason end), artifacts:$artifacts}]')
    echo "i143-acceptance-gate: $JOB/$cell $outcome"
done
FINISHED=$(now)
OUTCOME=ok; [ "$FAILED" -eq 0 ] || OUTCOME=refused
jq -n --arg job "$JOB" --argjson issue "$ISSUE" --arg layer "$LAYER" --arg sha "$SHA" --argjson dirty "$DIRTY" --arg started "$STARTED" --arg finished "$FINISHED" \
      --arg outcome "$OUTCOME" --argjson cells "$CELLS_JSON" \
      '{job:$job, issue:$issue, layer:$layer, source_sha:$sha, dirty_paths:$dirty, started:$started, finished:$finished, outcome:$outcome, cells:$cells}' > "$RECEIPT"
echo "i143-acceptance-gate: $JOB $OUTCOME ($((NCELLS - FAILED))/$NCELLS cells); receipt $RECEIPT"
[ "$FAILED" -eq 0 ]
