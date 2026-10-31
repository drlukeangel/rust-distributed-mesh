//! Failed jobs share one immutable evidence snapshot after every live test has ended.

use std::path::{Path, PathBuf};
use std::process::Command;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}

#[test]
fn evidence_preservation_runs_after_the_concurrent_test_barrier() {
    let source = std::fs::read_to_string(root().join("scripts/rdm-gate-at.sh")).unwrap();
    let one = source.split_once("one(){").unwrap().1.split_once("export -f one").unwrap().0;
    assert!(!one.contains("cp -r"), "a failed live job must not copy every scenario's evidence while sibling tests run");
    assert!(!one.contains("keep-gate-evidence.py"), "a failed live job must not start preservation");
    let barrier = source.find("' _ < $D/jobs").unwrap();
    let preserve = source.find("scripts/keep-gate-evidence.py").expect("one preservation after the test barrier");
    assert!(preserve > barrier, "preservation must follow every concurrent job");
}

#[test]
fn refused_jobs_share_one_snapshot_that_later_source_writes_cannot_change() {
    let dir = std::env::temp_dir().join(format!("rdm-gate-evidence-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let artifacts = dir.join("source");
    let kept = dir.join("kept");
    std::fs::create_dir_all(&artifacts).unwrap();
    std::fs::write(artifacts.join("evidence.jsonl"), "original\n").unwrap();
    let log = dir.join("gates-fixture.log");
    std::fs::write(&log, "ok green 1s\nFAIL first 2s\nFAIL(timed-out) second 3s\n").unwrap();
    let testkit = dir.join("testkit");
    for cell in ["a_red_cell", "another_red_cell", "a_green_cell"] {
        std::fs::create_dir_all(testkit.join(cell)).unwrap();
        std::fs::write(testkit.join(cell).join("testkit.1-1.spans.jsonl"), format!("{cell}\n")).unwrap();
    }
    let jobs = dir.join("gates-fixture.log.d");
    std::fs::create_dir_all(&jobs).unwrap();
    std::fs::write(jobs.join("first.log"), "test m::a_green_cell ... ok\ntest m::a_red_cell ... FAILED\n").unwrap();
    std::fs::write(jobs.join("second.log"), "m::another_red_cell --- FAILED\n").unwrap();
    let out = Command::new("python3").arg(root().join("scripts/keep-gate-evidence.py"))
        .args([&log, &artifacts, &kept, &testkit]).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let first = kept.join("gate-gates-fixture-first/artifacts");
    let second = kept.join("gate-gates-fixture-second/artifacts");
    assert_eq!(first.canonicalize().unwrap(), second.canonicalize().unwrap(), "one shared snapshot for every refused job");
    assert!(!kept.join("gate-gates-fixture-green").exists());
    std::fs::write(artifacts.join("evidence.jsonl"), "later\n").unwrap();
    assert_eq!(std::fs::read_to_string(first.join("evidence.jsonl")).unwrap(), "original\n");
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(kept.join("gate-gates-fixture/manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["jobs"], serde_json::json!(["first", "second"]));
    // A failed testkit cell's span directory is kept beside the artifacts; a passing one is not.
    assert_eq!(manifest["testkit_cells"], serde_json::json!(["a_red_cell", "another_red_cell"]));
    assert_eq!(std::fs::read_to_string(kept.join("gate-gates-fixture/testkit/a_red_cell/testkit.1-1.spans.jsonl")).unwrap(), "a_red_cell\n");
    assert!(!kept.join("gate-gates-fixture/testkit/a_green_cell").exists());
    let _ = std::fs::remove_dir_all(dir);
}

/// A refused acceptance job's cell directories (`target/i143-rshape/<job>/<cell>`, from the acceptance
/// registry; the gate names an rshape job without its `i143-` prefix) are kept in the gate's snapshot:
/// the export run after the release writes the same directories, so a release red's spans would
/// otherwise be the export's. Must NOT happen: a release red read from a later passing run's evidence
/// (rshape-authority-chaos-process at 257156168).
#[test]
fn a_refused_acceptance_jobs_cell_directories_are_kept_from_its_later_runs() {
    let dir = std::env::temp_dir().join(format!("rdm-gate-acceptance-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cell = dir.join("target/i143-rshape/i143-rshape-fixture-process/a_red_cell");
    std::fs::create_dir_all(cell.join("spans")).unwrap();
    std::fs::write(cell.join("spans/admin.1.spans.jsonl"), "release\n").unwrap();
    std::fs::create_dir_all(dir.join("tools/mesh-audit")).unwrap();
    std::fs::write(
        dir.join("tools/mesh-audit/i143-acceptance-jobs.json"),
        serde_json::json!({"jobs": {}, "rshape_jobs": {"i143-rshape-fixture-process": {"layer": "chaos-process", "cells": [{"name": "a_red_cell", "command": "true", "dir": "target/i143-rshape/i143-rshape-fixture-process/a_red_cell"}]}}}).to_string(),
    )
    .unwrap();
    let log = dir.join("gates-fixture.log");
    std::fs::write(&log, "FAIL rshape-fixture-process 2s\n").unwrap();
    std::fs::create_dir_all(dir.join("gates-fixture.log.d")).unwrap();
    let kept = dir.join("kept");
    let out = Command::new("python3")
        .arg(root().join("scripts/keep-gate-evidence.py"))
        .args([&log, &dir.join("none"), &kept, &dir.join("testkit")])
        .current_dir(&dir)
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    // The export's later run writes the same cell directory.
    std::fs::write(cell.join("spans/admin.1.spans.jsonl"), "export\n").unwrap();
    let held = kept.join("gate-gates-fixture/acceptance/rshape-fixture-process/a_red_cell/spans/admin.1.spans.jsonl");
    assert_eq!(std::fs::read_to_string(held).unwrap(), "release\n", "the release run's spans are kept, never the later run's");
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(kept.join("gate-gates-fixture/manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["acceptance_cells"], serde_json::json!(["rshape-fixture-process/a_red_cell"]));
    let _ = std::fs::remove_dir_all(dir);
}
