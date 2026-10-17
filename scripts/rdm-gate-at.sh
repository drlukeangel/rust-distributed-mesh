#!/usr/bin/env bash
[ -z "$GATE_PRIVATE_COPY" ] && { c=$(mktemp /tmp/rdm-gate-run.XXXXXX); cp "$0" "$c"; chmod +x "$c"; GATE_PRIVATE_COPY=1 exec bash "$c" "$@"; }
# RDM local gate: build everything once, then run every check and every scenario binary in
# parallel. Ports need nothing here: node-admin's EndpointAllocator claims each port host-wide.
cd "${1:-$HOME/rust-distributed-mesh}"
# Every path is this repo root (the lane) or the box's directories under $HOME.
ROOT=$(pwd); DEPLOY=$HOME/rust-distributed-mesh-deploy
# Workspace crates incremental, the same value cells.sh uses, so a lane holds one build (measured: an admin.rs edit 8.5 s, not 55.3 s).
# Incremental for workspace crates through cargo's own setting (build.incremental), never the env var
# CARGO_INCREMENTAL: sccache refuses every vendor compile when CARGO_INCREMENTAL is set (2026-10-13, a cold
# lane's gate: "incremental compilation is prohibited"); cargo applies build.incremental to workspace crates only.
export CARGO_BUILD_INCREMENTAL=true
# One gate per lane at a time: a second gate on the same lane would share its port block and
# its log and produce a mixed gate (the wsl-lane flock rule). It aborts by name instead.
# The lock is held by a wrapping flock(1) with `-o` (closed before the gate runs), so no estate
# process a stem leaves behind inherits it and holds the lane shut after the gate ends.
if [ -z "$RDM_GATE_LOCKED" ]; then
  RDM_GATE_LOCKED=1 flock -n -o -E 99 "/tmp/rdm-gate-$(basename "$(pwd)").lock" "$0" "$@"; rc=$?
  [ $rc = 99 ] && echo "ABORT: a gate is already running on lane $(basename "$(pwd)")" >&2
  exit $rc
fi
# Two gates over one suite. `release` (rdm-gate-at.sh <lane> release) runs every scenario at the
# documented production windows (staleness 30 s, gossip and backbone 2 s). The default gate runs
# the same scenarios at the test cadence: the proofs are about ordering and outcomes, not the
# length of a window, and a stem that still takes long at this cadence is a defect to read.
GATE="${2:-fast}"
case "$GATE" in
  release) echo "[gate] release: production cadence" ;;
  fast)    export RDM_STALENESS_MS=3000 RDM_GOSSIP_INTERVAL_MS=500 RDM_BACKBONE_INTERVAL_MS=500 RAFKA_STALENESS_MS=3000 RAFKA_GOSSIP_INTERVAL_MS=500 RAFKA_BACKBONE_INTERVAL_MS=500
           echo "[gate] fast: test cadence staleness=${RAFKA_STALENESS_MS}ms gossip=${RAFKA_GOSSIP_INTERVAL_MS}ms backbone=${RAFKA_BACKBONE_INTERVAL_MS}ms" ;;
  *)       echo "ABORT: gate is fast or release, not $GATE" >&2; exit 1 ;;
esac
# Never export CARGO_TARGET_DIR: sccache folds it into every Rust cache key, splitting the cache
# per worktree. Each worktree builds into its own ./target by default.
unset CARGO_TARGET_DIR
# OTLP over HTTP (e12.s80: the exporter is http-proto); 5318 is the collector's OTLP/HTTP port, 5317 its gRPC one.
export OTEL_EXPORTER_OTLP_ENDPOINT=http://192.168.68.99:5318
# Ports: every node binds :0 and the OS assigns it (R-J1); a gate reserves no port range.
JOBS="${GATE_JOBS:-40}"
# The estate binaries come from the one build below: no job builds or relinks a binary while other
# stems are starting.
export I143_ACCEPTANCE_SKIP_BUILD=1
SHA=$(git rev-parse --short HEAD)
O=$DEPLOY/gates-$(basename $(pwd))-$SHA${GATE/fast/}.log; : > $O; : > $O.detail
D=$O.d; rm -rf $D; mkdir -p $D
export I143_PREBUILT_ARTIFACTS="$D/artifacts.json"
SCENARIOS="node_delete node_connections_delete node_replace node_restart routing status shape_reconcile live_resize role_build role_cohort restart_target role_routing role_context role_wedge provider_state late_authority sticky_seat mesh_create fabric_primary_handover canonical_ids cohort_election fabric_primary fabric_takeover rpc_proof_store accepted_topology cohort_recovery rpc_certainty peer_mesh_investigation joiner_build_facts boot_trace restored_faults admin_restart rafka_time_discipline issuance rounds owed_check_ins node_stop build_op mesh_delete_resume mesh_delete_no_survivor apply_cuts scheduler_replay shared_dial digest_sequence"
t0=$(date +%s)

# 1. Build once: check, every bin, every test binary. Nothing compiles after this.
step(){ s=$(date +%s); if eval "$2" >> $O.detail 2>&1; then r=ok; else r=FAIL; fi; echo "$r $1 $(( $(date +%s)-s ))s" | tee -a $O; [ $r = ok ]; }
# The no-default-features check runs as a job beside the stems (below), off the critical path.
# The three binaries the scenarios run, the two audit bins, and the gated packages' test binaries.
# Only the packages this tree's workspace has: a package that left it (admin-ui to demo/) is
# skipped by name, as a stem absent from the tree is.
MEMBERS=$(cargo metadata --no-deps --format-version 1 2>/dev/null | python3 -c 'import json,sys; print(" ".join(p["name"] for p in json.load(sys.stdin)["packages"]))')
PKGS=$(for p in $MEMBERS; do printf " -p %s" "$p"; done)
ROLES=""
# ONE cargo invocation, one feature set: every bin and every test executable in one graph, built in
# parallel. Separate invocations each resolve their own feature set (build, test with dev-deps, the
# extra bins), so every changed crate compiled once per set (measured on ae840b9: iroh, gossip,
# transport, node-rpc and node-admin-core compiled in each of the invocations; 87 + 188 + 96 s).
step build "cargo build --timings --message-format=json --tests --bins $PKGS $ROLES > $D/artifacts.json" || { echo DONE | tee -a $O; exit 1; }
# The timings row (Luke 2026-10-12, layout proposal §5): wall, units, max concurrency, the unit-time sum and the five longest units.
python3 $ROOT/scripts/timings_row.py target/cargo-timings/cargo-timing.html 2>&1 | tee -a $O
# One build, one set of test binaries: every job runs an executable this build produced, and any
# test executable in the lane this build did not produce (an older SHA's, another selection's) is
# deleted, so a lane holds one set, never one per gate.
python3 - "$D" <<'PY'
import json, os, sys, glob
d = sys.argv[1]; keep = set()
for f in ("artifacts.json",):
    for l in open(os.path.join(d, f)):
        try: m = json.loads(l)
        except Exception: continue
        if m.get("reason") == "compiler-artifact":
            keep.update(os.path.realpath(x) for x in m.get("filenames", []))
            if m.get("executable"): keep.add(os.path.realpath(m["executable"]))
gone = 0
for x in glob.glob("target/debug/deps/*"):
    if os.path.isfile(x) and os.access(x, os.X_OK) and not x.endswith((".so", ".rlib", ".rmeta", ".d")) and os.path.realpath(x) not in keep:
        os.remove(x); gone += 1
print(f"[gate] pruned {gone} stale test executables")
PY

# 1b. Test discovery ratchet: run --list for every test executable of every workspace package on
# origin/main and on the candidate, and refuse any drop in test discovery naming the package and binary.
discover_check(){
    MAIN_SHA=$(git rev-parse --short origin/main)
    MAIN_DISC="$DEPLOY/discovery-$MAIN_SHA.json"
    CAND_DISC="$D/discovery-candidate.json"
    python3 - "$D/artifacts.json" "$CAND_DISC" <<'PY'
import json, subprocess, sys
art_path, out_path = sys.argv[1], sys.argv[2]
meta = json.loads(subprocess.check_output(["cargo", "metadata", "--no-deps", "--format-version", "1"], text=True))
pkgs = {p["name"]: {} for p in meta["packages"]}
for l in open(art_path):
    try: m = json.loads(l)
    except Exception: continue
    if m.get("reason") != "compiler-artifact" or not m.get("executable") or not m.get("profile", {}).get("test"): continue
    pid = m["package_id"]
    pkg = pid.split("#")[1].split("@")[0] if "#" in pid and "@" in pid.split("#")[1] else pid.rstrip("/").split("/")[-1].split("#")[0]
    if pkg not in pkgs: continue
    tname = m["target"]["name"]
    tkind = ",".join(m["target"]["kind"])
    exe = m["executable"]
    out = subprocess.check_output([exe, "--list", "--format", "terse"], text=True, stderr=subprocess.DEVNULL)
    tests = [line[:-len(": test")] if line.endswith(": test") else line[:-len(": benchmark")]
             for line in out.splitlines() if line.endswith(": test") or line.endswith(": benchmark")]
    pkgs[pkg][tname] = {"kind": tkind, "count": len(tests), "tests": tests}
json.dump(pkgs, open(out_path, "w"), indent=2, sort_keys=True)
PY
    cp "$CAND_DISC" "$DEPLOY/discovery-$SHA.json"
    if [ ! -f "$MAIN_DISC" ]; then
        if [ "$SHA" = "$MAIN_SHA" ]; then
            cp "$CAND_DISC" "$MAIN_DISC"
        else
            MAIN_REPO="$HOME/rust-distributed-mesh"
            git -C "$MAIN_REPO" fetch -q origin
            git -C "$MAIN_REPO" checkout -q origin/main
            MAIN_MEMBERS=$(cargo metadata --manifest-path "$MAIN_REPO/Cargo.toml" --no-deps --format-version 1 2>/dev/null | python3 -c 'import json,sys; print(" ".join("-p "+p["name"] for p in json.load(sys.stdin)["packages"]))')
            (cd "$MAIN_REPO" && CARGO_BUILD_INCREMENTAL=true cargo build --message-format=json --tests --bins $MAIN_MEMBERS > "$D/main-artifacts.json")
            python3 - "$D/main-artifacts.json" "$MAIN_DISC" "$MAIN_REPO/Cargo.toml" <<'PY'
import json, subprocess, sys
art_path, out_path, manifest = sys.argv[1], sys.argv[2], sys.argv[3]
meta = json.loads(subprocess.check_output(["cargo", "metadata", "--manifest-path", manifest, "--no-deps", "--format-version", "1"], text=True))
pkgs = {p["name"]: {} for p in meta["packages"]}
for l in open(art_path):
    try: m = json.loads(l)
    except Exception: continue
    if m.get("reason") != "compiler-artifact" or not m.get("executable") or not m.get("profile", {}).get("test"): continue
    pid = m["package_id"]
    pkg = pid.split("#")[1].split("@")[0] if "#" in pid and "@" in pid.split("#")[1] else pid.rstrip("/").split("/")[-1].split("#")[0]
    if pkg not in pkgs: continue
    tname = m["target"]["name"]
    tkind = ",".join(m["target"]["kind"])
    exe = m["executable"]
    out = subprocess.check_output([exe, "--list", "--format", "terse"], text=True, stderr=subprocess.DEVNULL)
    tests = [line[:-len(": test")] if line.endswith(": test") else line[:-len(": benchmark")]
             for line in out.splitlines() if line.endswith(": test") or line.endswith(": benchmark")]
    pkgs[pkg][tname] = {"kind": tkind, "count": len(tests), "tests": tests}
json.dump(pkgs, open(out_path, "w"), indent=2, sort_keys=True)
PY
        fi
    fi
    python3 - "$MAIN_DISC" "$CAND_DISC" <<'PY'
import collections, json, sys
base = json.load(open(sys.argv[1]))
cand = json.load(open(sys.argv[2]))
base_tot = sum(b["count"] for p in base.values() for b in p.values())
cand_tot = sum(b["count"] for p in cand.values() for b in p.values())
print(f"[discovery] origin/main={base_tot} tests across {len(base)} packages; candidate={cand_tot} tests across {len(cand)} packages")
# Pool of newly added tests in candidate binaries whose count grew or in new packages, keyed by tail name (fn name) and full name
added_full = collections.Counter()
added_leaf = collections.Counter()
for pkg, bins in cand.items():
    for tname, info in bins.items():
        b_info = base.get(pkg, {}).get(tname)
        if b_info is None:
            for t in info["tests"]:
                added_full[t] += 1
                added_leaf[t.split("::")[-1]] += 1
        elif info["count"] > b_info["count"]:
            old_c = collections.Counter(b_info["tests"])
            new_c = collections.Counter(info["tests"])
            for t, n in (new_c - old_c).items():
                added_full[t] += n
                added_leaf[t.split("::")[-1]] += n
errors = []
for pkg, bins in sorted(base.items()):
    if not bins and pkg in cand:
        errors.append(f"{pkg}: produced 0 test executables on origin/main")
    if pkg in cand and not cand[pkg]:
        errors.append(f"{pkg}: produced 0 test executables on candidate")
    for tname, b_info in sorted(bins.items()):
        c_info = cand.get(pkg, {}).get(tname)
        m_cnt = b_info["count"]
        if c_info is not None and c_info["count"] >= m_cnt:
            continue
        c_cnt = c_info["count"] if c_info is not None else 0
        # Check if the tests that left (pkg, tname) moved to another package/binary in candidate
        old_c = collections.Counter(b_info["tests"])
        cur_c = collections.Counter(c_info["tests"]) if c_info is not None else collections.Counter()
        left = old_c - cur_c
        missing = []
        for t, n in sorted(left.items()):
            leaf = t.split("::")[-1]
            if added_full[t] >= n:
                added_full[t] -= n
                added_leaf[leaf] -= n
            elif added_leaf[leaf] >= n:
                added_leaf[leaf] -= n
            else:
                missing.append(t)
        if missing or cand_tot < base_tot or (c_info is None and pkg in cand and m_cnt == 0):
            sample = ", ".join(missing[:5]) + ("..." if len(missing) > 5 else "")
            reason = f"missing tests: {sample}" if missing else ("binary missing from package" if c_info is None else f"workspace total fell {base_tot} -> {cand_tot}")
            errors.append(f"{pkg}:{tname} ({b_info['kind']}) count fell {m_cnt} -> {c_cnt} ({reason})")
if cand_tot < base_tot and not errors:
    errors.append(f"workspace total test discovery fell {base_tot} -> {cand_tot}")
if errors:
    for e in errors:
        print(f"FAIL test-discovery: {e}", file=sys.stderr)
    sys.exit(1)
PY
}
step discovery "discover_check" || { echo DONE | tee -a $O; exit 1; }
# Consumer binaries and their import manifest are prerequisites, built before any tests.
# Their immutable copied binaries are what every concurrent R-shape job runs.
if [ "$GATE" = release ] && [ -f demo/Cargo.toml ] && [ -x scripts/i143-rshape-build-consumer.sh ]; then
    step consumer-build "scripts/i143-rshape-build-consumer.sh --candidate-sha $(git rev-parse HEAD) --workspace target/i143-rshape/consumer-workspace --bin-dir target/i143-rshape/consumer-bin --output target/i143-rshape/consumer-build --skip-ui-tests" || { echo DONE | tee -a $O; exit 1; }
fi


# 2. Every check and every scenario, in parallel, each a prebuilt test binary.
exe(){ python3 - "$D/artifacts.json" "$@" <<'PY'
import json,sys,os
path,pkg=sys.argv[1],sys.argv[2]; want=sys.argv[3] if len(sys.argv)>3 and sys.argv[3]!="-" else None; cmd=len(sys.argv)>4
for l in open(path):
    try: m=json.loads(l)
    except Exception: continue
    if m.get("reason")!="compiler-artifact" or not m.get("executable") or not m.get("profile",{}).get("test"): continue
    name=m["package_id"].split("#")[0].rstrip("/").split("/")[-1]
    pid=m["package_id"]
    pkgname=pid.split("#")[1].split("@")[0] if "#" in pid and "@" in pid.split("#")[1] else name
    if pkgname!=pkg and name!=pkg: continue
    if want and m["target"]["name"]!=want: continue
    if cmd:
        d=m["manifest_path"].rsplit("/",1)[0]
        print(f'(cd {d} && CARGO_MANIFEST_DIR={d} {m["executable"]} -q {os.environ.get("FP_KILL_SKIP_ARGS","")}) &&')
    else:
        print(m["executable"])
PY
}
# All FP hand-off and recovery cells are unskipped and run in the gate.
FP_KILL_SKIP_ARGS=""
export FP_KILL_SKIP_ARGS
: > $D/jobs
# A crate's integration tests are either one executable per file (a stem is its own executable) or one
# executable `main` with a module per file (a stem is the module filter `<stem>::`). A missing stem fails by name.
SCENARIO_PKG=rafka-test-scenario
[ -d crates/rdm-test ] && SCENARIO_PKG=rdm-test
for t in $SCENARIOS; do
    e=$(exe $SCENARIO_PKG $t)
    if [ -z "$e" ] && [ -f crates/$SCENARIO_PKG/tests/main.rs ] && grep -qs "^mod $t;" crates/$SCENARIO_PKG/tests/main.rs; then
        m=$(exe $SCENARIO_PKG main)
        if [ -n "$m" ]; then
            skips=$(sed -n "s/^mod \(..*$t\);/ --skip \1::/p" crates/$SCENARIO_PKG/tests/main.rs | tr -d '\n')
            e="$m --test-threads=8$skips $t::"
        fi
    fi
    if [ -n "$e" ]; then echo "$t $e${FP_KILL_SKIP_ARGS:+ $FP_KILL_SKIP_ARGS}" >> $D/jobs; else echo "FAIL $t 0s" | tee -a $O; echo "FAIL scenario stem $t missing from $SCENARIO_PKG" >> $O.detail; fi
done
SCENARIO_SKIP=$(for s in $SCENARIOS build_log estate_handover estate_teardown await_attempt seeded operation_ledger driver_reconcile proof_holder_lifecycle manifest_replay restart_canary_seed action_generator failpoint_explorer wedge_detector process_fault_backend container_fault_backend bootstrap_pending primary_loss_takeover supplied_clock delta_gap_recovery held_status_join partition_rejoin proxy_hydration_cutback restarted_member_rejoin stopped_last_admin peer_mesh_topology topology_get mesh_recover backbone container_kill successor_adoption node_connections_delete poc_chaos_kit rshape_burn_in; do printf " --skip %s::" "$s"; done)
# One job per test executable (2026-10-12): a crate's lib, bin and main executables run in parallel, each its tests in
# parallel threads. Chained in one job they summed: admin-core lib 39.7 s + main 30.4 s ran as one 116 s job under load.
per_exe(){
    name=$1; pkg=$2
    exe $pkg - cmd | while read -r line; do
        c=${line% &&}
        b=$(echo "$c" | grep -o 'target/debug/deps/[^ )]*' | sed 's#.*/##; s/-[0-9a-f]*$//')
        if [ "$pkg" = "rafka-node-admin-core" ] || [ "$pkg" = "rdm-admin" ]; then
            if [ "$b" = "rafka_node_admin_core" ] || [ "$b" = "rdm_admin" ]; then
                echo "$name:race sh:${c%)} a_list_racing_a_removal_answers_the_records_that_remain)" >> $D/jobs
                echo "$name:storage sh:${c%)} --skip a_list_racing_a_removal_answers_the_records_that_remain storage:: record_store:: fabric_storage::)" >> $D/jobs
                echo "$name:lib sh:${c%)} --skip storage:: --skip record_store:: --skip fabric_storage::)" >> $D/jobs
            elif [ "$b" = "main" ]; then
                echo "$name:$b sh:${c%)} --test-threads=8)" >> $D/jobs
            else
                echo "$name:$b sh:$c" >> $D/jobs
            fi
        elif [ "$pkg" = "rafka-test-scenario" ] && [ "$b" = "main" ]; then
            :
        elif [ "$pkg" = "rafka-node-rpc-testkit" ] && [ "$b" = "main" ]; then
            echo "$name:$b sh:${c%)} --test-threads=8)" >> $D/jobs
        elif [ "$pkg" = "rdm-test" ] && [ "$b" = "main" ]; then
            echo "$name:$b sh:${c%)} --test-threads=8 $SCENARIO_SKIP)" >> $D/jobs
        else
            echo "$name:$b sh:$c" >> $D/jobs
        fi
    done
}
per_exe audit-tests rafka-mesh-audit
per_exe audit-tests rdm-mesh-audit
per_exe docgen rafka-docgen
per_exe docgen rdm-docgen
per_exe test-runner rdm-test-runner
per_exe node-rpc-contract rafka-node-rpc-contract
per_exe node-rpc rafka-node-rpc
per_exe mesh-entity rafka-mesh-entity
per_exe mesh-ops rafka-mesh-ops
per_exe mesh-telemetry rafka-mesh-telemetry
per_exe node-admin-client rafka-node-admin-client
per_exe chaos rafka-chaos
per_exe test-scenario rafka-test-scenario
per_exe rdm-core rdm-core
per_exe rdm-client rdm-client
per_exe node-admin-core rafka-node-admin-core
per_exe rdm-admin rdm-admin
per_exe rpc-testkit rafka-node-rpc-testkit
per_exe rdm-test rdm-test
case "$PKGS " in *"-p rafka-admin-ui "*) echo "admin-ui sh:$(exe rafka-admin-ui ui_flows cmd | tr '\n' ' ') true" >> $D/jobs ;; esac
per_exe node-base rafka-node-base
per_exe rdm-node rdm-node
per_exe mesh-transport rafka-mesh-transport
per_exe rdm-transport rdm-transport
echo "i143-2777-process sh:scripts/i143-acceptance-gate.sh i143-2777-process" >> $D/jobs
echo "i143-2780-process sh:scripts/i143-acceptance-gate.sh i143-2780-process" >> $D/jobs
# R14 item 6: the provider mounts one qualified store namespace in every container.
export RDM_CONTAINER_STORE_QUALIFIED=1
echo "i143-2777-container sh:scripts/i143-acceptance-gate.sh i143-2777-container" >> $D/jobs
echo "i143-2784-chaos-container sh:scripts/i143-acceptance-gate.sh i143-2784-chaos-container" >> $D/jobs
echo "doctests cargo:--doc $PKGS" >> $D/jobs
echo "workspace-check sh:cargo check --workspace --tests --no-default-features" >> $D/jobs
# Container jobs require the Docker host; absence is a refusal, never a skip.
echo "container-proof sh:export RDM_REQUIRE_CONTAINER=1 RAFKA_REQUIRE_CONTAINER=1; $(exe rdm-admin main) -q container_proof:: provider_state::container:: & provider_pid=\$!; $(exe $SCENARIO_PKG main) -q container_kill:: & kill_pid=\$!; proof_rc=0; wait \$provider_pid || proof_rc=1; wait \$kill_pid || proof_rc=1; exit \$proof_rc" >> $D/jobs
# The Rafka-shaped (R-shape) canonical process jobs: each >1 min, so release gate only (run between epics).
for j in i143-rshape-composition-process i143-rshape-authority-chaos-process i143-rshape-lifecycle-chaos-process i143-rshape-routing-chaos-process i143-rshape-partition-chaos-process; do
    grep -q "\"$j\"" tools/mesh-audit/i143-acceptance-jobs.json && echo "rshape-${j#i143-rshape-} sh:scripts/i143-acceptance-gate.sh $j" >> $D/jobs
done
# The consumer's UI test executables were built with its immutable runtime binaries.
# Start each in the same concurrent batch, with no Cargo compilation during a live cell.
if [ "$GATE" = release ] && [ -f demo/Cargo.toml ] && [ -x scripts/i143-rshape-build-consumer.sh ]; then
    python3 - target/i143-rshape/consumer-build/artifacts.json >> "$D/jobs" <<'PYUI'
import json, os, shlex, sys
seen = set()
for line in open(sys.argv[1]):
    row = json.loads(line)
    if row.get("reason") != "compiler-artifact" or not row.get("profile", {}).get("test") or not row.get("executable"):
        continue
    identity = row["package_id"]
    name = identity.rsplit("#", 1)[-1].split("@", 1)[0] if "@" in identity.rsplit("#", 1)[-1] else identity.split("#", 1)[0].rstrip("/").rsplit("/", 1)[-1]
    if name != "rafka-admin-ui" or row["executable"] in seen:
        continue
    seen.add(row["executable"])
    directory = shlex.quote(os.path.dirname(row["manifest_path"]))
    exe = shlex.quote(row["executable"])
    print(f"demo-admin-ui-{row['target']['name']} sh:(cd {directory} && CARGO_MANIFEST_DIR={directory} {exe} -q)")
if not seen:
    raise SystemExit("the consumer build produced no admin-ui test executables")
PYUI
fi
echo "dep-rules run:dep-rules" >> $D/jobs
echo "address-lookup run:address-lookup" >> $D/jobs

one(){
    name=$1; shift; what="$*"; log=$D/$name.log; s=$(date +%s)
    case "$what" in
        cargo:*) cmd="cargo test -q ${what#cargo:}" ;;
        run:*)   cmd="target/debug/${what#run:}" ;;
        sh:*)    cmd="${what#sh:}" ;;
        *)       cmd="(cd crates/$SCENARIO_PKG && $what)" ;;
    esac
    # 3-minute cycle: a job past the bound is killed with its whole process group (the test binary
    # and every admin/node it spawned) and named; nothing else on the box is touched.
    setsid bash -c "$cmd" > $log 2>&1 &
    jp=$!
    r=ok
    while kill -0 $jp 2>/dev/null; do
        if [ $(( $(date +%s)-s )) -ge ${GATE_JOB_SECS:-300} ]; then kill -9 -- -$jp 2>/dev/null; r="FAIL(timed-out)"; break; fi
        sleep 1
    done
    wait $jp 2>/dev/null; rc=$?
    [ $r = ok ] && [ $rc -ne 0 ] && r=FAIL
    echo "$r $name $(( $(date +%s)-s ))s" | tee -a $O
}
export -f one; export O D DEPLOY SCENARIO_PKG OTEL_EXPORTER_OTLP_ENDPOINT RDM_STALENESS_MS RDM_GOSSIP_INTERVAL_MS RDM_BACKBONE_INTERVAL_MS RAFKA_STALENESS_MS RAFKA_GOSSIP_INTERVAL_MS RAFKA_BACKBONE_INTERVAL_MS
# Release-only: proofs of the offline ladder's own windows (production cadence), containers, and
# every job over a minute on its own (Luke 10-08: stems > 1 min run in the release suite between
# epics). The cargo-invoking jobs (doctests, the no-default-features check) are release-only too:
# during the stem phase they take cargo's package-cache lock that audit-tests' `cargo tree` waits on.
RELEASE_ONLY='^(mesh_delete_resume|mesh_delete_no_survivor|container-proof|i143-2777-container|i143-2784-chaos-container|cohort_election|rshape-[a-z-]+|i143-2777-process|i143-2780-process|cohort_recovery|peer_mesh_investigation|doctests|workspace-check|demo-admin-ui|restored_faults|node_replace|rafka_time_discipline|owed_check_ins|fabric_takeover) '
if [ "$GATE" = fast ]; then grep -vE "$RELEASE_ONLY" $D/jobs > $D/jobs.fast && mv $D/jobs.fast $D/jobs; fi
# Every suite job starts in the same concurrent batch. Long release jobs retain their
# existing 900 s cap; other jobs retain 300 s. GATE_JOBS controls the batch concurrency.
# Prioritize known heavy/longer jobs so they start at T=0 and don't stall in the queue tail
python3 - "$D/jobs" <<'PY'
import sys
path = sys.argv[1]
lines = open(path).readlines()
PRIORITY = [
    "node_stop",
    "node-admin-core:race",
    "rdm-admin:race",
    "node-admin-core:storage",
    "rdm-admin:storage",
    "node-admin-core:main",
    "rdm-admin:main",
    "rdm-test:main",
    "node-admin-core:lib",
    "rdm-admin:lib",
    "accepted_topology",
    "rounds",
    "shape_reconcile",
    "sticky_seat",
    "canonical_ids",
    "fabric_primary",
    "rpc_proof_store",
    "role_wedge",
    "live_resize",
]
def score(line):
    for i, p in enumerate(PRIORITY):
        if line.startswith(p):
            return i
    return len(PRIORITY)
lines.sort(key=score)
open(path, "w").writelines(lines)
PY
xargs -P "$JOBS" -L1 bash -c '
    case "$1" in rshape-*|i143-2784-chaos-container|i143-2777-container|container-proof)
        export GATE_JOB_SECS=${GATE_HEAVY_JOB_SECS:-900} ;;
    esac
    one "$@"
' _ < $D/jobs
cat $D/*.log >> $O.detail
# Nothing an estate launched may outlive the gate: a node process of this lane's build that the
# init/user manager has adopted is a leak, named and failed (then killed so the next gate is clean).
LANE_ROOT=$(pwd)
leaked=$(ps -eo pid,ppid,args | awk -v r="$LANE_ROOT/target/debug/" '$2!=0 && index($0, r) && /(rafka-node-admin|rafka-rpc-node|rshape-node-admin|rafka-broker|rafka-gateway|rafka-compute)/ {print $1":"$2}' | while IFS=: read pid ppid; do pcomm=$(ps -o comm= -p $ppid 2>/dev/null); case "$pcomm" in systemd|init) echo $pid;; esac; done)
if [ -n "$leaked" ]; then echo "FAIL leaked-processes $(echo $leaked | wc -w)" | tee -a $O; for p in $leaked; do echo "leaked: $(ps -o pid=,etimes=,args= -p $p | cut -c1-160)" >> $O.detail; kill -9 $p 2>/dev/null; done; fi
# Preserve once after all live tests and their runtimes ended. Per-job copies during the
# concurrent batch multiply every artifact by every red and perform disk work inside other
# cells' bounds. Each refused job instead points at this gate's one immutable snapshot.
if grep -q '^FAIL' "$O"; then
    step evidence-keep 'python3 scripts/keep-gate-evidence.py "$O" "crates/$SCENARIO_PKG/tests/artifacts" "$DEPLOY/kept"'
fi
echo "DONE $(( $(date +%s)-t0 ))s" | tee -a $O
