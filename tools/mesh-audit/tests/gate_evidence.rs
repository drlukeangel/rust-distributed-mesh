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
    let out = Command::new("python3").arg(root().join("scripts/keep-gate-evidence.py"))
        .args([&log, &artifacts, &kept]).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let first = kept.join("gate-gates-fixture-first/artifacts");
    let second = kept.join("gate-gates-fixture-second/artifacts");
    assert_eq!(first.canonicalize().unwrap(), second.canonicalize().unwrap(), "one shared snapshot for every refused job");
    assert!(!kept.join("gate-gates-fixture-green").exists());
    std::fs::write(artifacts.join("evidence.jsonl"), "later\n").unwrap();
    assert_eq!(std::fs::read_to_string(first.join("evidence.jsonl")).unwrap(), "original\n");
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(kept.join("gate-gates-fixture/manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["jobs"], serde_json::json!(["first", "second"]));
    let _ = std::fs::remove_dir_all(dir);
}
