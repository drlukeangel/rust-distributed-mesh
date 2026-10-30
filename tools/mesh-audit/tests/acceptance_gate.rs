//! The i143 acceptance gate runner (`scripts/i143-acceptance-gate.sh`) and its registry
//! (`tools/mesh-audit/i143-acceptance-jobs.json`): a job is refused by name when a cell matches
//! no test or is ignored, and a passing cell leaves its receipt with every artifact hashed; every
//! registered command names exactly its own cell and its own artifact directory.

use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}

static N: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    dir: PathBuf,
    registry: PathBuf,
    job: String,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
        let _ = std::fs::remove_file(root().join("target/i143-acceptance/jobs").join(format!("{}.json", self.job)));
    }
}

/// A UNIT job of one cell whose command is `command` (a shell line printing cargo-shaped
/// output); the cell's dir is inside the fixture's own temp dir.
fn fixture(tag: &str, command: &str) -> Fixture {
    fixture_on(tag, "unit", command)
}

/// [`fixture`] on the layer `layer` (an estate layer requires the cell's estate manifest).
fn fixture_on(tag: &str, layer: &str, command: &str) -> Fixture {
    let n = N.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("i143-gate-fixture-{}-{n}-{tag}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let job = format!("i143-gate-fixture-{}-{n}-{tag}", std::process::id());
    let cell_dir = dir.join("cell");
    let registry = dir.join("jobs.json");
    let v = serde_json::json!({
        "contract": "fixture",
        "jobs": { &job: { "issue": 0, "layer": layer, "cells": [
            { "name": "fixture_cell", "source": "none", "command": command, "dir": cell_dir.to_str().unwrap() }
        ] } }
    });
    std::fs::write(&registry, serde_json::to_vec_pretty(&v).unwrap()).unwrap();
    Fixture { dir, registry, job }
}

fn run(f: &Fixture) -> (bool, String, Value) {
    run_env(f, &[])
}

fn run_env(f: &Fixture, env: &[(&str, &Path)]) -> (bool, String, Value) {
    let out = Command::new("bash")
        .arg(root().join("scripts/i143-acceptance-gate.sh"))
        .arg(&f.job)
        .env("I143_ACCEPTANCE_JOBS", &f.registry)
        .envs(env.iter().map(|(k, v)| (*k, *v)))
        .current_dir(root())
        .output()
        .expect("the runner starts");
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    let receipt = root().join("target/i143-acceptance/jobs").join(format!("{}.json", f.job));
    let receipt: Value = serde_json::from_slice(&std::fs::read(&receipt).unwrap_or_else(|e| panic!("no receipt {}: {e}\n{text}", receipt.display()))).unwrap();
    (out.status.success(), text, receipt)
}

#[test]
fn a_job_whose_cell_matches_no_test_is_refused_by_name() {
    let f = fixture("zero", "echo 'running 0 tests'; echo; echo 'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s'");
    let (ok, text, receipt) = run(&f);
    assert!(!ok, "a zero-match job is refused:\n{text}");
    assert!(text.contains("REFUSED") && text.contains("matched no test"), "{text}");
    assert_eq!(receipt["outcome"], "refused");
    assert!(receipt["cells"][0]["refusal"].as_str().unwrap().contains("matched no test"), "{receipt}");
}

#[test]
fn an_ignored_cell_is_refused_by_name() {
    let f = fixture("ignored", "echo 'running 1 test'; echo 'test fixture_cell ... ignored'; echo; echo 'test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s'");
    let (ok, text, receipt) = run(&f);
    assert!(!ok, "an ignored cell is refused:\n{text}");
    assert!(text.contains("REFUSED") && text.contains("was ignored"), "{text}");
    assert_eq!(receipt["cells"][0]["outcome"], "refused");
}

/// A passing test summary for the fixture's one cell.
const PASSED: &str = "echo 'running 1 test'; echo 'test fixture_cell ... ok'; echo; echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s'";

/// CONTRACT: a cell that skips itself by name (a host that cannot do what it needs, netfault.rs: "a test
/// skips by that name") passes, forms no estate and writes `result.json` naming the skip and its reason
/// (`"skipped": {"name", "reason"}`): the runner reads it as a skip, never a failure. The cell's outcome
/// is `skipped` with the skip named, the job's outcome is `ok`, and no estate manifest is asked of it.
/// Must NOT happen: a named skip refused, or a skip without its reason taken as one.
#[test]
fn a_cell_that_skips_by_name_is_a_skip_not_a_refusal() {
    let skip = format!("echo '{{\"cell\":\"fixture_cell\",\"skipped\":{{\"name\":\"host-cannot-cut\",\"reason\":\"no nft\"}}}}' > \"$I143_ACCEPTANCE_DIR/result.json\"; {PASSED}");
    // The estate binaries are not this cell's subject: the runner's own build of them is skipped.
    let prebuilt = [("I143_ACCEPTANCE_SKIP_BUILD", Path::new("1"))];
    let f = fixture_on("skip", "chaos-process", &skip);
    let (ok, text, receipt) = run_env(&f, &prebuilt);
    assert!(ok, "a named skip is not a failure:\n{text}");
    assert_eq!(receipt["outcome"], "ok", "{receipt}");
    assert_eq!(receipt["cells"][0]["outcome"], "skipped", "{receipt}");
    assert_eq!(receipt["cells"][0]["skip"], "host-cannot-cut: no nft", "{receipt}");
    assert!(text.contains("skipped by name"), "{text}");
    // A skip that names no reason is no skip: the estate layer still asks for its estate.
    let unnamed = format!("echo '{{\"cell\":\"fixture_cell\",\"skipped\":{{\"name\":\"host-cannot-cut\"}}}}' > \"$I143_ACCEPTANCE_DIR/result.json\"; {PASSED}");
    let f = fixture_on("unnamed-skip", "chaos-process", &unnamed);
    let (ok, text, receipt) = run_env(&f, &prebuilt);
    assert!(!ok && text.contains("left no estate manifest"), "{text}");
    assert_eq!(receipt["cells"][0]["outcome"], "refused", "{receipt}");
}

#[test]
fn a_cell_that_leaves_no_result_is_refused_by_name() {
    let f = fixture("noresult", "echo 'running 1 test'; echo 'test fixture_cell ... ok'; echo; echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s'");
    let (ok, text, _) = run(&f);
    assert!(!ok && text.contains("left no result.json"), "{text}");
}

#[test]
fn a_passing_cell_leaves_its_receipt_with_every_artifact_hashed() {
    let f = fixture(
        "pass",
        "echo '{\"observed\":true}' > \"$I143_ACCEPTANCE_DIR/result.json\"; echo '[]' > \"$I143_ACCEPTANCE_DIR/spans.json\"; echo \"cell=$I143_ACCEPTANCE_CELL\"; echo 'running 1 test'; echo 'test fixture_cell ... ok'; echo; echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s'",
    );
    let (ok, text, receipt) = run(&f);
    assert!(ok, "{text}");
    assert_eq!(receipt["outcome"], "ok", "{receipt}");
    let head = String::from_utf8(Command::new("git").args(["rev-parse", "HEAD"]).current_dir(root()).output().unwrap().stdout).unwrap().trim().to_string();
    assert_eq!(receipt["source_sha"], head);
    let cell = &receipt["cells"][0];
    assert_eq!(cell["outcome"], "ok");
    let artifacts = cell["artifacts"].as_object().unwrap();
    for name in ["gate.log", "manifest.json", "result.json", "spans.json"] {
        let (path, hash) = artifacts.iter().find(|(p, _)| p.ends_with(&format!("/{name}"))).unwrap_or_else(|| panic!("{name} is hashed in the receipt: {receipt}"));
        assert_eq!(hash.as_str().unwrap().len(), 64, "{path} carries a sha256");
    }
    let manifest: Value = serde_json::from_slice(&std::fs::read(f.dir.join("cell/manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["source_sha"], head);
    assert_eq!(manifest["outcome"], "ok");
    assert!(manifest["refusal"].is_null());
    let log = std::fs::read_to_string(f.dir.join("cell/gate.log")).unwrap();
    assert!(log.contains("cell=fixture_cell"), "the cell saw its name: {log}");
}

/// CONTRACT (#3203, Luke 2026-10-27: "we can only learn more things"): a job run in a learning soak run
/// (`I143_SOAKS_LEARN=1`, which `scripts/i143-export-run.sh --soaks` passes to the runner) leaves its
/// receipt marked `learning: true`, which `export-manifest` refuses to certify; any other run's receipt
/// says `learning: false`.
#[test]
fn a_learning_runs_receipt_is_marked_learning() {
    let cell = "echo '{\"observed\":true}' > \"$I143_ACCEPTANCE_DIR/result.json\"; echo '[]' > \"$I143_ACCEPTANCE_DIR/spans.json\"; echo 'running 1 test'; echo 'test fixture_cell ... ok'; echo; echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s'";
    let f = fixture("learning", cell);
    let (ok, text, receipt) = run_env(&f, &[("I143_SOAKS_LEARN", Path::new("1"))]);
    assert!(ok, "{text}");
    assert_eq!(receipt["learning"], true, "a learning run's receipt says so: {receipt}");
    let f = fixture("certifying", cell);
    let (ok, text, receipt) = run(&f);
    assert!(ok, "{text}");
    assert_eq!(receipt["learning"], false, "any other run's receipt says it is no learning run: {receipt}");
}

/// The registry ratchet: a job is `i143-<issue>-<layer>`, every command names exactly its own
/// test with `-- --exact`, its dir is `target/i143-acceptance/<issue>/<layer>/<cell>`, and an
/// estate command lands its estate under that dir on the job's provider.
#[test]
fn every_registered_command_names_its_own_cell_and_its_own_directory() {
    let reg: Value = serde_json::from_slice(&std::fs::read(root().join("tools/mesh-audit/i143-acceptance-jobs.json")).unwrap()).unwrap();
    let mut all: Vec<(&String, &Value, bool)> = reg["jobs"].as_object().expect("jobs").iter().map(|(k, v)| (k, v, false)).collect();
    // The R-shape qualification namespace (epic #2943) is run by the same runner.
    all.extend(reg["rshape_jobs"].as_object().into_iter().flatten().filter(|(k, _)| !k.starts_with('_')).map(|(k, v)| (k, v, true)));
    assert!(!all.is_empty());
    for (job, j, rshape) in all {
        // A job answers an issue (a number) or a ruling (its id, a string such as `rw1`).
        let issue = match &j["issue"] {
            Value::Number(n) => n.as_u64().expect("issue number").to_string(),
            Value::String(ruling) => ruling.clone(),
            other => panic!("{job}: issue is a number or a ruling id, not {other}"),
        };
        let layer = j["layer"].as_str().expect("layer");
        if rshape {
            assert!(job.starts_with("i143-rshape-"), "{job}: an R-shape job id is i143-rshape-<name>");
        } else {
            assert_eq!(job, &format!("i143-{issue}-{layer}"), "job id names its issue and layer");
        }
        let cells = j["cells"].as_array().expect("cells");
        assert!(!cells.is_empty(), "{job} registers a cell");
        for c in cells {
            let name = c["name"].as_str().unwrap();
            let test = c["test"].as_str().unwrap_or(name);
            let command = c["command"].as_str().unwrap();
            let dir = c["dir"].as_str().unwrap();
            if rshape {
                assert_eq!(dir, format!("target/i143-rshape/{job}/{name}"), "{job}/{name}");
            } else {
                assert_eq!(dir, format!("target/i143-acceptance/{issue}/{layer}/{name}"), "{job}/{name}");
            }
            if !c["source"].as_str().unwrap().ends_with(".sh") {
                assert!(command.ends_with(&format!("::{test} -- --exact")), "{job}/{name}: the command names exactly its test: {command}");
                assert!(command.contains(" --test main "), "{job}/{name}: the command names its crate's one test executable");
                let stem = std::path::Path::new(c["source"].as_str().unwrap()).file_stem().unwrap().to_str().unwrap();
                assert!(command.ends_with(&format!(" {stem}::{test} -- --exact")), "{job}/{name}: the command names the module of its source file {stem}: {command}");
            }
            assert!(root().join(c["source"].as_str().unwrap()).parent().unwrap().is_dir(), "{job}/{name}: the source's crate exists");
            match c.get("evidence").and_then(Value::as_str) {
                None => {}
                Some("runner") => assert!(c.get("test").is_some(), "{job}/{name}: a runner-evidenced cell names the existing test it runs"),
                Some("model") => assert_eq!(layer, "unit", "{job}/{name}: `evidence: model` (no runtime span) is for a UNIT cell only"),
                Some(other) => panic!("{job}/{name}: evidence `{other}` is not admitted (runner or model)"),
            }
            for forbidden in ["runner", "receipt_writer", "receipt_schema"] {
                assert!(c.get(forbidden).is_none() && j.get(forbidden).is_none(), "{job}/{name}: a second generic `{forbidden}` is declared; scripts/i143-acceptance-gate.sh is the one runner");
            }
            match layer {
                "unit" | "static" | "export" => assert!(!command.contains("RDM_ARTIFACTS_DIR"), "{job}/{name}: a unit cell has no estate"),
                "process" | "container" | "fast" | "fast-process" | "fast-container" | "chaos-process" | "chaos-container" | "chaos-scale" | "soak-process" | "soak-container" => {
                    let provider = if layer.ends_with("container") { "container" } else { "process" };
                    assert!(command.contains(&format!("MESH_SPAWN_TYPE={provider} ")), "{job}/{name}: the estate runs on the job's provider: {command}");
                    assert!(command.contains(&format!("RDM_ARTIFACTS_DIR={dir}/estate ")), "{job}/{name}: the estate lands under the cell's dir: {command}");
                }
                other => panic!("{job}: layer {other} is not registered"),
            }
        }
    }
}

/// A model cell (pure schema/model/algebra) needs a result and no span; the receipt records the
/// declaration. The same declaration on any other layer is refused by name.
#[test]
fn a_model_cell_needs_no_span_and_only_a_unit_cell_may_declare_it() {
    let pass = "echo '{\"observed\":true}' > \"$I143_ACCEPTANCE_DIR/result.json\"; echo 'running 1 test'; echo 'test fixture_cell ... ok'; echo; echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s'";
    let f = fixture("model", pass);
    let mut reg: Value = serde_json::from_slice(&std::fs::read(&f.registry).unwrap()).unwrap();
    reg["jobs"][&f.job]["cells"][0]["evidence"] = serde_json::json!("model");
    std::fs::write(&f.registry, serde_json::to_vec(&reg).unwrap()).unwrap();
    let (ok, text, receipt) = run(&f);
    assert!(ok, "a model cell with a result and no spans.json passes:\n{text}");
    assert_eq!(receipt["cells"][0]["evidence"], "model", "the receipt records the declaration: {receipt}");
    assert!(!f.dir.join("cell/spans.json").exists(), "no placeholder span was written");

    // Without the declaration the same cell is refused for its missing span.
    let g = fixture("nomodel", pass);
    let (ok, text, _) = run(&g);
    assert!(!ok && text.contains("left no spans.json"), "{text}");

    // A model cell with no result is refused.
    let h = fixture("modelnoresult", "echo 'running 1 test'; echo 'test fixture_cell ... ok'; echo; echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s'");
    let mut reg: Value = serde_json::from_slice(&std::fs::read(&h.registry).unwrap()).unwrap();
    reg["jobs"][&h.job]["cells"][0]["evidence"] = serde_json::json!("model");
    std::fs::write(&h.registry, serde_json::to_vec(&reg).unwrap()).unwrap();
    let (ok, text, _) = run(&h);
    assert!(!ok && text.contains("left no result.json"), "{text}");

    // Another layer may not declare it.
    let p = fixture("modelprocess", pass);
    let mut reg: Value = serde_json::from_slice(&std::fs::read(&p.registry).unwrap()).unwrap();
    reg["jobs"][&p.job]["layer"] = serde_json::json!("process");
    reg["jobs"][&p.job]["cells"][0]["evidence"] = serde_json::json!("model");
    std::fs::write(&p.registry, serde_json::to_vec(&reg).unwrap()).unwrap();
    let out = Command::new("bash").arg(root().join("scripts/i143-acceptance-gate.sh")).arg(&p.job).env("I143_ACCEPTANCE_JOBS", &p.registry).env("I143_ACCEPTANCE_SKIP_BUILD", "1").current_dir(root()).output().unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(!out.status.success() && text.contains("model is for UNIT cells only"), "{text}");
}

/// The suite built once: registry commands keep their environment and libtest arguments,
/// execute that exact artifact in its crate directory, and never invoke Cargo in a live batch.
#[test]
fn a_prebuilt_acceptance_cell_executes_the_gate_artifact_without_invoking_cargo() {
    use std::os::unix::fs::PermissionsExt;
    let f = fixture("prebuilt", "KEPT=yes cargo test -p rdm-test --test main fixture_cell -- --exact");
    let bin = f.dir.join("bin");
    let crate_dir = f.dir.join("crate");
    std::fs::create_dir_all(&bin).unwrap();
    std::fs::create_dir_all(&crate_dir).unwrap();
    let fake_cargo = bin.join("cargo");
    std::fs::write(&fake_cargo, "#!/bin/sh\necho 'Cargo must not execute in a prebuilt batch' >&2\nexit 77\n").unwrap();
    std::fs::set_permissions(&fake_cargo, std::fs::Permissions::from_mode(0o755)).unwrap();
    let exe = bin.join("the-gate-test-binary");
    std::fs::write(&exe, r#"#!/bin/sh
[ "$*" = 'fixture_cell --exact' ] || exit 21
[ "$KEPT" = yes ] || exit 22
[ "$PWD" = "$CARGO_MANIFEST_DIR" ] || exit 23
printf '{"passed":true}\n' > "$I143_ACCEPTANCE_DIR/result.json"
printf '[]\n' > "$I143_ACCEPTANCE_DIR/spans.json"
echo 'running 1 test'
echo 'test fixture_cell ... ok'
echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s'
"#).unwrap();
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
    let artifacts = f.dir.join("artifacts.json");
    let row = serde_json::json!({"reason":"compiler-artifact", "package_id":"path+file:///fixture/rdm-test#0.1.0", "manifest_path":crate_dir.join("Cargo.toml"), "profile":{"test":true}, "target":{"name":"main", "kind":["test"]}, "executable":exe});
    std::fs::write(&artifacts, serde_json::to_string(&row).unwrap()).unwrap();
    let mut paths = vec![bin];
    paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
    let path = PathBuf::from(std::env::join_paths(paths).unwrap());
    let (ok, text, receipt) = run_env(&f, &[("I143_ACCEPTANCE_SKIP_BUILD", Path::new("1")), ("I143_PREBUILT_ARTIFACTS", &artifacts), ("PATH", &path)]);
    assert!(ok, "the registered cell must execute the built artifact, preserving its environment, cwd and arguments: {text}\n{receipt}");
    assert_eq!(receipt["outcome"], "ok");
    let mut registry: Value = serde_json::from_slice(&std::fs::read(&f.registry).unwrap()).unwrap();
    registry["jobs"][&f.job]["cells"][0]["command"] = "cargo test -p rdm-test --test missing fixture_cell -- --exact".into();
    std::fs::write(&f.registry, serde_json::to_vec(&registry).unwrap()).unwrap();
    let (ok, _, _) = run_env(&f, &[("I143_ACCEPTANCE_SKIP_BUILD", Path::new("1")), ("I143_PREBUILT_ARTIFACTS", &artifacts), ("PATH", &path)]);
    assert!(!ok, "a target the gate did not build must be refused without fallback compilation");
    let refused = std::fs::read_to_string(f.dir.join("cell/gate.log")).unwrap();
    assert!(refused.contains("prebuilt-test: REFUSED") && refused.contains("rdm-test:missing"), "{refused}");
}
