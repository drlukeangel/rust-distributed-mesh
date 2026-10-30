#!/usr/bin/env bash
# i143 export run (e10.s2, #2790): every job of the acceptance registry at HEAD, a batch at a time,
# each batch under the box's gate turn (`flock -o /tmp/rdm-gate-turn.lock`, so landings interleave
# between batches), leaving one receipt per job (scripts/i143-acceptance-gate.sh) for
# `export-manifest` to certify.
#
#   scripts/i143-export-run.sh [--plan]
#   scripts/i143-export-run.sh --soaks [--plan]
#
# The plan: the consumer build (`i143-rshape-consumer-build`) is in the first batch, and every job
# whose command runs the consumer's executables (RDM_RSHAPE_CONSUMER_BIN_DIR) comes after it. The
# other jobs fill the batches longest first, by the wall of the job's last receipt in this checkout,
# or by its layer when it has none; at most I143_EXPORT_BATCH jobs (default 6) run in a batch. The
# soak jobs (layer soak-*) are held by name: no soak runs until the fast gate and the release suite
# are green on one SHA (Luke 2026-10-09). `--plan` prints the plan and runs nothing.
#
# `--soaks` runs the held soak jobs instead, with the consumer build first, and only at a HEAD whose
# fast and release gate logs in $HOME/rust-distributed-mesh-deploy are green: a `gates-<lane>-<short>.log`
# and a `gates-<lane>-<short>release.log` whose first line is `[gate] head=<HEAD> dirty=0`, with a DONE
# line and no FAIL line. Without both, the run is refused by name and nothing runs.
#
# `I143_SOAKS_LEARN=1` with `--soaks` is a learning run (Luke 2026-10-27: "we can only learn more
# things"): the soak jobs run without the green-gate bar, every receipt is marked `learning: true`
# (scripts/i143-acceptance-gate.sh reads I143_SOAKS_LEARN), and export-manifest refuses to certify a
# learning receipt. Without `--soaks` the variable is dropped: no other run is a learning run.
#
# A tree with an uncommitted path is refused by name, and the run stops by name when HEAD moves or a
# path changes between batches: every receipt is the one SHA's. Each job runs with the release gate's
# environment (RDM_CONTAINER_STORE_QUALIFIED=1) and RDM_REQUIRE_NETFAULT=1, so a cell that cannot cut
# fails instead of skipping. target/i143-export/<sha>/ holds each job's log and run.log (each job's
# exit and wall). The exit is 1 when any job refused.
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
unset CARGO_TARGET_DIR
REG="${I143_ACCEPTANCE_JOBS:-tools/mesh-audit/i143-acceptance-jobs.json}"
N="${I143_EXPORT_BATCH:-6}"
SOAKS=0 DRY=0
for a in "$@"; do
    case "$a" in
        --soaks) SOAKS=1 ;;
        --plan) DRY=1 ;;
        *) echo "usage: scripts/i143-export-run.sh [--soaks] [--plan]" >&2; exit 2 ;;
    esac
done

PLAN=$(python3 - "$REG" "$N" "$SOAKS" <<'PY'
import datetime, json, os, re, sys
reg_path, n, soaks = sys.argv[1], int(sys.argv[2]), sys.argv[3] == "1"
if n < 1:
    sys.exit(f"I143_EXPORT_BATCH={n}: a batch runs at least one job")
reg = json.load(open(reg_path))
# The wall a layer's job takes when this checkout holds no receipt of it.
LAYER_S = {"chaos-container": 400, "chaos-process": 300, "process": 200, "container": 150, "fast": 120,
           "fast-process": 120, "fast-container": 120, "unit": 60, "static": 30, "export": 10}
RECEIPTS = {"jobs": "target/i143-acceptance/jobs", "rshape_jobs": "target/i143-rshape/jobs"}
def wall(section, job, layer):
    try:
        r = json.load(open(os.path.join(RECEIPTS[section], f"{job}.json")))
        t = lambda s: datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return (t(r["finished"]) - t(r["started"])).total_seconds()
    except (OSError, KeyError, ValueError):
        return LAYER_S.get(layer, 120)
jobs, held = [], []
for section in ("jobs", "rshape_jobs"):
    for job, v in reg.get(section, {}).items():
        if job.startswith("_"):
            continue
        layer = v.get("layer", "")
        # Chaos at scale (the 200-node tier) runs on its own before the export (Q290 step 4), never in it.
        if layer == "chaos-scale":
            continue
        if layer.startswith("soak") != soaks and job != "i143-rshape-consumer-build":
            if not soaks:
                held.append((job, layer))
            continue
        consumer = any("RDM_RSHAPE_CONSUMER_BIN_DIR" in c.get("command", "") for c in v.get("cells", []))
        # The cargo package a job's cells build: jobs of two packages in one batch relink the same
        # target/debug binaries under each other's running cells (a node-admin hashed at a cell's
        # start no longer the file its Build launches), so a batch holds one package's jobs.
        shape = next((m.group(1) for c in v.get("cells", []) for m in [re.search(r"cargo \w+ -p (\S+)", c.get("command", ""))] if m), "")
        jobs.append((job, consumer, wall(section, job, layer), shape))
build = [j for j in jobs if j[0] == "i143-rshape-consumer-build"]
others = sorted((j for j in jobs if j not in build), key=lambda j: -j[2])
lead = next((j[3] for j in others if not j[1]), "")
first = build + [j for j in others if not j[1] and j[3] == lead][: n - len(build)]
rest = [j for j in others if j not in first]
batches = [first]
for shape in dict.fromkeys(j[3] for j in rest):
    same = [j for j in rest if j[3] == shape]
    batches += [same[i:i + n] for i in range(0, len(same), n)]
for i, b in enumerate(x for x in batches if x):
    print(f"batch {i + 1}: " + " ".join(j[0] for j in b))
for job, layer in held:
    print(f"held: {job} ({layer}): no soak runs until the fast gate and the release suite are green on one SHA (Luke 2026-10-09)")
PY
) || { echo "i143-export-run: REFUSED no plan could be made from $REG" >&2; exit 2; }

[ "$SOAKS" = 1 ] && [ "${I143_SOAKS_LEARN:-}" = 1 ] && LEARN=1 || { LEARN=0; unset I143_SOAKS_LEARN; }

# The soaks' bar (Luke 2026-10-09): the fast gate and the release suite green at this HEAD.
green_gate() {
    local suffix=$1 f
    for f in "$HOME"/rust-distributed-mesh-deploy/gates-"$(basename "$PWD")"-*.log; do
        case "$f" in *release.log) [ "$suffix" = release ] || continue ;; *) [ "$suffix" = fast ] || continue ;; esac
        head -1 "$f" | grep -qx "\[gate\] head=$(git rev-parse HEAD) dirty=0" && grep -q '^DONE' "$f" && ! grep -q '^FAIL' "$f" && { echo "$f"; return 0; }
    done
    return 1
}
if [ "$LEARN" = 1 ]; then
    echo "i143-export-run: learning run at $(git rev-parse --short HEAD) (I143_SOAKS_LEARN=1): the soaks run without the green-gate bar; every receipt is marked learning: true and certifies nothing"
elif [ "$SOAKS" = 1 ]; then
    for g in fast release; do
        log=$(green_gate $g) || { echo "i143-export-run: REFUSED --soaks at $(git rev-parse --short HEAD): no green $g gate log at this HEAD (no soak runs until the fast gate and the release suite are green on one SHA, Luke 2026-10-09)" >&2; exit 2; }
        echo "i143-export-run: $g gate green at this HEAD: $log"
    done
fi

if [ "$DRY" = 1 ]; then
    echo "i143-export-run: plan at $(git rev-parse --short HEAD) from $REG, at most $N jobs a batch"
    echo "$PLAN"
    exit 0
fi

SHA=$(git rev-parse HEAD)
dirty=$(git status --porcelain | wc -l | tr -d ' ')
[ "$dirty" = 0 ] || { echo "i143-export-run: REFUSED $dirty uncommitted paths at $SHA: a receipt written over them certifies nothing" >&2; exit 2; }
OUT=target/i143-export/$SHA
mkdir -p "$OUT"
: > "$OUT/run.log"
echo "i143-export-run: $SHA, $(echo "$PLAN" | grep -c '^batch ') batches of at most $N$([ "$LEARN" = 1 ] && echo ', a learning run: every receipt learning: true')" | tee -a "$OUT/run.log"
echo "$PLAN" | grep '^held: ' | tee -a "$OUT/run.log"
t0=$(date +%s)
while read -r line; do
    jobs=${line#*: }
    echo "${line%%:*} waiting for the turn: $jobs ($(date -u +%T))" | tee -a "$OUT/run.log"
    flock -o /tmp/rdm-gate-turn.lock bash -c '
        sha="$1"; out="$2"; shift 2
        if [ "$(git rev-parse HEAD)" != "$sha" ] || [ -n "$(git status --porcelain)" ]; then
            echo "i143-export-run: STOPPED the checkout moved off $sha (HEAD $(git rev-parse --short HEAD), $(git status --porcelain | wc -l) uncommitted paths): no receipt of another tree joins this run" | tee -a "$out/run.log" >&2
            exit 3
        fi
        export RDM_CONTAINER_STORE_QUALIFIED=1 RDM_REQUIRE_NETFAULT=1
        echo "  turn held $(date -u +%T)" | tee -a "$out/run.log"
        for j in "$@"; do
            ( s=$(date +%s); scripts/i143-acceptance-gate.sh "$j" > "$out/$j.log" 2>&1; rc=$?
              echo "  $j rc=$rc $(( $(date +%s) - s ))s" | tee -a "$out/run.log" ) &
        done
        wait' _ "$SHA" "$OUT" $jobs
    [ $? = 3 ] && exit 3
done < <(echo "$PLAN" | grep '^batch ')
refused=$(grep -E '^  [^ ]+ rc=[1-9]' "$OUT/run.log" | awk '{print $1}' | tr '\n' ' ')
ran=$(grep -cE '^  [^ ]+ rc=' "$OUT/run.log")
echo "i143-export-run: $ran jobs at $SHA in $(( $(date +%s) - t0 ))s; refused: ${refused:-none}" | tee -a "$OUT/run.log"
[ -z "$refused" ]
