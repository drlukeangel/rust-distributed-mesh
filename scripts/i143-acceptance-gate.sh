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
# requires a reachable Docker domain and sets RDM_REQUIRE_CONTAINER=1, so a host that cannot
# run containers fails by name instead of substituting the process provider.
#
# Jobs live under the registry's `jobs` section or its `rshape_jobs` section (the R-shape
# qualification namespace, target/i143-rshape/...); one runner and one receipt schema serve both.
# Layers: unit, static and export (no estate); process, container, fast(-process|-container),
# chaos-(process|container) and soak-(process|container) (an estate on the named provider). A cell
# whose command sets RDM_RSHAPE_CONSUMER_BIN_DIR runs an external consumer's executables: the
# runner first re-hashes every binary of the consumer build manifest
# (target/i143-rshape/consumer-build/manifest.json, or the cell's `consumer_manifest`) and refuses
# the cell by name on a missing manifest or file or a hash mismatch, and afterwards refuses it when
# its estate manifest does not record explicit executable bindings (a built-in substitution).
# A registry is refused before any cell runs when it holds a job id twice, a cell without name,
# command or dir, a duplicate cell name in a job, or a second generic runner or receipt schema
# (a job or cell carrying `runner`, `receipt_writer` or `receipt_schema`).
# `--verify-receipt <path>` re-checks a written receipt: outcome ok, source sha equal to HEAD,
# every artifact present with its recorded sha256.
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
if [ "${1:-}" = "--verify-receipt" ]; then
    r="${2:-}"
    [ -s "$r" ] || { echo "i143-acceptance-gate: REFUSED receipt $r does not exist" >&2; exit 2; }
    [ "$(jq -r .outcome "$r")" = ok ] || { echo "i143-acceptance-gate: REFUSED receipt $r: outcome is not ok" >&2; exit 1; }
    head=$(git rev-parse HEAD)
    [ "$(jq -r .source_sha "$r")" = "$head" ] || { echo "i143-acceptance-gate: REFUSED receipt $r: source sha $(jq -r .source_sha "$r") is not HEAD $head" >&2; exit 1; }
    bad=0
    while IFS=$'\t' read -r path hash; do
        if [ ! -f "$path" ]; then echo "i143-acceptance-gate: REFUSED receipt $r: artifact $path is missing" >&2; bad=1
        elif [ "$(sha256sum "$path" | cut -d' ' -f1)" != "$hash" ]; then echo "i143-acceptance-gate: REFUSED receipt $r: artifact $path no longer hashes to $hash" >&2; bad=1; fi
    done < <(jq -r '.cells[].artifacts | to_entries[] | [.key, .value] | @tsv' "$r")
    [ "$bad" = 0 ] || exit 1
    echo "i143-acceptance-gate: receipt $r verified at $head"
    exit 0
fi
REG="${I143_ACCEPTANCE_JOBS:-tools/mesh-audit/i143-acceptance-jobs.json}"
JOB="${1:-}"
refuse() { echo "i143-acceptance-gate: REFUSED $*" >&2; }
[ -n "$JOB" ] || { refuse "no job id given; usage: scripts/i143-acceptance-gate.sh <job-id>"; exit 2; }
[ -f "$REG" ] || { refuse "$JOB: registry $REG does not exist"; exit 2; }
jq -e . "$REG" > /dev/null 2>&1 || { refuse "$JOB: registry $REG is not JSON"; exit 2; }
# The registry as a whole, before anything runs.
dups=$(jq -r '[(.jobs // {}), (.rshape_jobs // {} | del(._contract))] | map(keys[]) | group_by(.) | map(select(length > 1) | .[0]) | .[]' "$REG")
[ -z "$dups" ] || { refuse "$JOB: registry $REG holds job id(s) in more than one place: $(echo $dups)"; exit 2; }
second=$(jq -r '[(.jobs // {}), (.rshape_jobs // {} | del(._contract))] | map(to_entries[] | select((.value | has("runner", "receipt_writer", "receipt_schema")) or (.value.cells | map(has("runner", "receipt_writer", "receipt_schema")) | any)) | .key) | .[]' "$REG")
[ -z "$second" ] || { refuse "$JOB: a second generic runner or receipt schema is declared by job(s) $(echo $second): scripts/i143-acceptance-gate.sh and its receipt are the one owner (#2776)"; exit 2; }
malformed=$(jq -r '[(.jobs // {}), (.rshape_jobs // {} | del(._contract))] | map(to_entries[] | .key as $k | .value.cells[]? | select((.name // "" | length) == 0 or (.command // "" | length) == 0 or (.dir // "" | length) == 0) | $k) | unique | .[]' "$REG")
[ -z "$malformed" ] || { refuse "$JOB: job(s) $(echo $malformed) hold a cell without name, command or dir"; exit 2; }
dupcells=$(jq -r '[(.jobs // {}), (.rshape_jobs // {} | del(._contract))] | map(to_entries[] | .key as $k | select((.value.cells | map(.name) | length) != (.value.cells | map(.name) | unique | length)) | $k) | .[]' "$REG")
[ -z "$dupcells" ] || { refuse "$JOB: job(s) $(echo $dupcells) name a cell twice"; exit 2; }
badevidence=$(jq -r '[(.jobs // {}), (.rshape_jobs // {} | del(._contract))] | map(to_entries[] | .key as $k | .value.layer as $l | .value.cells[]? | select(.evidence != null) | select(((.evidence == "runner") or (.evidence == "cell") or (.evidence == "model" and $l == "unit")) | not) | "\($k)/\(.name) evidence \(.evidence) on layer \($l)") | .[]' "$REG")
[ -z "$badevidence" ] || { refuse "$JOB: a cell declares evidence the runner does not admit (model is for UNIT cells only; others are runner or cell): $(echo $badevidence)"; exit 2; }
JOBFILE=$(mktemp)
trap 'rm -f "$JOBFILE"' EXIT
jq --arg j "$JOB" '(.jobs[$j] // .rshape_jobs[$j])' "$REG" > "$JOBFILE"
[ "$(jq -r type "$JOBFILE")" = object ] || { refuse "$JOB: not a job in $REG"; exit 2; }
ISSUE=$(jq -r --arg j "$JOB" '.issue' "$JOBFILE")
LAYER=$(jq -r --arg j "$JOB" '.layer' "$JOBFILE")
NCELLS=$(jq -r --arg j "$JOB" '.cells | length' "$JOBFILE")
[ "$NCELLS" -gt 0 ] || { refuse "$JOB: registers no cell"; exit 2; }
SHA=$(git rev-parse HEAD)
DIRTY=$(git status --porcelain | wc -l | tr -d ' ')
JOBS_DIR=target/i143-acceptance/jobs
export RDM_CANDIDATE_SHA="${RDM_CANDIDATE_SHA:-$SHA}"
jq -e --arg j "$JOB" '.jobs[$j]' "$REG" > /dev/null 2>&1 || JOBS_DIR=target/i143-rshape/jobs
mkdir -p "$JOBS_DIR"
RECEIPT="$JOBS_DIR/$JOB.json"
STARTED=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
now() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }

# The estate binaries, built once per job so every launched admin and node is this SHA.
case "$LAYER" in
    process|container|fast|fast-process|fast-container|chaos-process|chaos-container|soak-process|soak-container)
        if [ -z "${I143_ACCEPTANCE_SKIP_BUILD:-}" ]; then
            if ! cargo build -p rafka-node-admin-core -p rafka-node-rpc-testkit -p rafka-consumer-fixture --bins > "$JOBS_DIR/$JOB.build.log" 2>&1; then
                refuse "$JOB: the estate binaries did not build (see $JOBS_DIR/$JOB.build.log)"
                exit 1
            fi
        fi
        ;;
    unit|static|export) ;;
    *) refuse "$JOB: layer $LAYER is not unit, static, export, process, container, fast, fast-process, fast-container, chaos-process, chaos-container, soak-process or soak-container"; exit 2 ;;
esac
case "$LAYER" in *container) ON_CONTAINER=1 ;; *) ON_CONTAINER= ;; esac
if [ -n "$ON_CONTAINER" ]; then
    export RDM_REQUIRE_CONTAINER=1
    if ! docker info > /dev/null 2>&1; then
        refuse "$JOB: no reachable Docker domain (docker info failed; set DOCKER_HOST or start the daemon)"
        exit 1
    fi
fi

TMPD=$(mktemp -d)
trap 'rm -rf "$TMPD"' EXIT
CELLS_JSON="[]"
FAILED=0
for i in $(seq 0 $((NCELLS - 1))); do
    cell=$(jq -r --arg j "$JOB" --argjson i "$i" '.cells[$i].name' "$JOBFILE")
    command=$(jq -r --arg j "$JOB" --argjson i "$i" '.cells[$i].command' "$JOBFILE")
    dir=$(jq -r --arg j "$JOB" --argjson i "$i" '.cells[$i].dir' "$JOBFILE")
    mkdir -p "$dir"
    # An external consumer's executables are the ones the build manifest hashed.
    consumer_reason=""
    consumer=0
    case "$command" in *RDM_RSHAPE_CONSUMER_BIN_DIR=*) consumer=1 ;; esac
    if [ "$consumer" = 1 ]; then
        cm=$(jq -r --argjson i "$i" '.cells[$i].consumer_manifest // "target/i143-rshape/consumer-build/manifest.json"' "$JOBFILE")
        cbin=$(echo "$command" | sed -nE 's/.*RDM_RSHAPE_CONSUMER_BIN_DIR=([^ ]+).*/\1/p')
        if [ ! -s "$cm" ]; then
            consumer_reason="the cell runs external consumer executables but their build manifest $cm does not exist"
        else
            for f in $(jq -r '.binaries | keys[]' "$cm"); do
                want=$(jq -r --arg f "$f" '.binaries[$f]' "$cm")
                if [ ! -f "$cbin/$f" ]; then consumer_reason="consumer executable $cbin/$f named by $cm does not exist"; break; fi
                have=$(sha256sum "$cbin/$f" | cut -d' ' -f1)
                if [ "$have" != "$want" ]; then consumer_reason="consumer executable $cbin/$f hashes to $have, the build manifest $cm records $want"; break; fi
            done
        fi
    fi
    rm -f "$dir/gate.log" "$dir/manifest.json" "$dir/result.json" "$dir/spans.json" "$dir"/*.spans.jsonl
    start=$(now)
    # The cell's directory is handed over absolute: a test binary runs in its crate's directory,
    # not the root the registry's paths are relative to.
    case "$dir" in /*) cell_dir="$dir" ;; *) cell_dir="$ROOT/$dir" ;; esac
    if [ -n "$consumer_reason" ]; then
        echo "$consumer_reason" > "$dir/gate.log"
        rc=1
    else
        I143_ACCEPTANCE_DIR="$cell_dir" I143_ACCEPTANCE_CELL="$cell" bash -c "$command" > "$dir/gate.log" 2>&1
        rc=$?
    fi
    finish=$(now)
    # An existing stem run as a regression (`evidence: runner`) writes no result of its own: the
    # runner records the test summary, the estate manifest and every spans file for it.
    evidence=$(jq -r --arg j "$JOB" --argjson i "$i" '.cells[$i].evidence // "cell"' "$JOBFILE")
    if [ "$evidence" = runner ] && [ ! -s "$dir/result.json" ]; then
        summary_line=$(grep -E '^test result: ' "$dir/gate.log" | tail -1)
        estate_manifest=$(find "$dir/estate" -name manifest.json 2>/dev/null | head -1)
        spans_json="[]"
        for f in $(find "$dir/estate" -name '*.spans.jsonl' 2>/dev/null | sort); do
            spans_json=$(echo "$spans_json" | jq --arg p "$f" --argjson n "$(wc -l < "$f")" --arg h "$(sha256sum "$f" | cut -d' ' -f1)" '. + [{path:$p, spans:$n, sha256:$h}]')
        done
        jq -n --arg cell "$cell" --arg test "$(jq -r --arg j "$JOB" --argjson i "$i" '.cells[$i].test // .cells[$i].name' "$JOBFILE")" \
              --arg summary "$summary_line" --argjson rc "$rc" --arg manifest "$estate_manifest" --argjson spans "$spans_json" \
              --argjson estate "$( [ -n "$estate_manifest" ] && cat "$estate_manifest" || echo null )" \
              '{cell:$cell, test:$test, evidence:"runner", exit:$rc, summary:$summary, estate_manifest:$manifest, estate:$estate, spans_files:$spans}' > "$dir/result.json"
    fi
    # A script cell (its source is a .sh file) has no cargo test summary: it passes when it exits 0,
    # and the runner records its result.
    script=0
    case "$(jq -r --argjson i "$i" '.cells[$i].source // ""' "$JOBFILE")" in *.sh) script=1 ;; esac
    if [ "$script" = 1 ] && [ -z "$consumer_reason" ]; then
        jq -n --arg cell "$cell" --argjson rc "$rc" --arg command "$command" '{cell:$cell, evidence:"runner", script:true, exit:$rc, command:$command}' > "$dir/result.json"
    fi
    reason=""
    if [ -n "$consumer_reason" ]; then
        reason="$consumer_reason"
    elif [ "$script" = 1 ]; then
        [ "$rc" -eq 0 ] || reason="the script cell failed (exit $rc)"
    elif grep -qE '^running 0 tests' "$dir/gate.log"; then
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
            static) ;;
            unit)
                # A model cell (pure schema/model/algebra) has no runtime span: it declares that, and the receipt says so.
                if [ "$evidence" = model ]; then
                    :
                elif [ ! -s "$dir/spans.json" ] || ! jq -e . "$dir/spans.json" > /dev/null 2>&1; then
                    reason="the UNIT cell left no spans.json in $dir"
                fi
                ;;
            process|container|fast|fast-process|fast-container|chaos-process|chaos-container|soak-process|soak-container)
                estate_manifest=$(find "$dir/estate" -name manifest.json 2>/dev/null | head -1)
                if [ -z "$estate_manifest" ]; then
                    reason="the cell left no estate manifest under $dir/estate"
                else
                    provider=$(jq -r '.provider // empty' "$estate_manifest")
                    want=$LAYER; case "$want" in *container) want=container ;; *) want=process ;; esac
                    if [ "$provider" != "$want" ]; then
                        reason="provider substitution: the estate ran on '${provider:-none}', the job's layer is $want"
                    elif [ "$consumer" = 1 ] && [ "$(jq -r '.executable_bindings.mode // empty' "$estate_manifest")" != explicit ]; then
                        reason="built-in substitution: the cell sets RDM_RSHAPE_CONSUMER_BIN_DIR but its estate ran with executable bindings '$(jq -r '.executable_bindings.mode // "none"' "$estate_manifest")', not explicit"
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
    # The artifact map of a twenty-node cell is far past the 128 KiB a single argument can carry
    # (MAX_ARG_STRLEN): it goes to jq as a file, never as an argument.
    echo "$artifacts" > "$TMPD/artifacts.json"
    CELLS_JSON=$(echo "$CELLS_JSON" | jq --arg cell "$cell" --arg evidence "$evidence" --arg outcome "$outcome" --arg reason "$reason" --slurpfile artifacts "$TMPD/artifacts.json" \
        '. + [{name:$cell, evidence:$evidence, outcome:$outcome, refusal:(if $reason == "" then null else $reason end), artifacts:$artifacts[0]}]')
    echo "i143-acceptance-gate: $JOB/$cell $outcome"
done
FINISHED=$(now)
OUTCOME=ok; [ "$FAILED" -eq 0 ] || OUTCOME=refused
echo "$CELLS_JSON" > "$TMPD/cells.json"
jq -n --arg job "$JOB" --argjson issue "$ISSUE" --arg layer "$LAYER" --arg sha "$SHA" --argjson dirty "$DIRTY" --arg started "$STARTED" --arg finished "$FINISHED" \
      --arg outcome "$OUTCOME" --slurpfile cells "$TMPD/cells.json" \
      '{job:$job, issue:$issue, layer:$layer, source_sha:$sha, dirty_paths:$dirty, started:$started, finished:$finished, outcome:$outcome, cells:$cells[0]}' > "$RECEIPT"
if [ ! -s "$RECEIPT" ] || ! jq -e . "$RECEIPT" > /dev/null 2>&1; then
    refuse "$JOB: the receipt $RECEIPT was not written"
    exit 1
fi
echo "i143-acceptance-gate: $JOB $OUTCOME ($((NCELLS - FAILED))/$NCELLS cells); receipt $RECEIPT"
[ "$FAILED" -eq 0 ]
