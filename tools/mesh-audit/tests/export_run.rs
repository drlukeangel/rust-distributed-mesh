//! i143 e10.s2 (#2790): the export run (`scripts/i143-export-run.sh`) runs every job of the
//! acceptance registry at one SHA, a batch at a time under the box's gate turn, leaving one receipt
//! per job for `export-manifest` to certify. Its plan (`--plan`) is the run's own: the consumer build
//! first (the jobs that run its executables refuse without it), each other job exactly once, no batch
//! over its size, and the soak jobs held by name (no soak until the fast gate and the release suite
//! are green on one SHA).

use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}

/// A registry of both sections: the consumer build, a job that runs its executables, jobs of the
/// other layers, and a soak job in each section.
fn registry(dir: &Path) -> PathBuf {
    let cell = |job: &str, command: &str| json!({ "issue": 0, "layer": "", "cells": [{ "name": "c", "source": "none", "command": command, "dir": format!("target/{job}/c") }] });
    let mut jobs = serde_json::Map::new();
    let mut rshape = serde_json::Map::new();
    rshape.insert("_contract".into(), json!("fixture"));
    let consumer = "RDM_RSHAPE_CONSUMER_BIN_DIR=target/i143-rshape/consumer-bin true";
    for (section, job, layer, command) in [
        ("jobs", "i143-1-unit", "unit", "true"),
        ("jobs", "i143-2-process", "process", "true"),
        ("jobs", "i143-3-chaos-container", "chaos-container", "true"),
        ("jobs", "i143-4-soak-process", "soak-process", "true"),
        ("jobs", "i143-5-process", "process", consumer),
        ("rshape_jobs", "i143-rshape-consumer-build", "static", "true"),
        ("rshape_jobs", "i143-rshape-x-chaos-process", "chaos-process", consumer),
        ("rshape_jobs", "i143-rshape-y-static", "static", "true"),
        ("rshape_jobs", "i143-rshape-soak-z-container", "soak-container", consumer),
    ] {
        let mut v = cell(job, command);
        v["layer"] = json!(layer);
        if section == "jobs" { &mut jobs } else { &mut rshape }.insert(job.into(), v);
    }
    let path = dir.join("jobs.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&json!({ "contract": "fixture", "jobs": jobs, "rshape_jobs": rshape })).unwrap()).unwrap();
    path
}

/// CONTRACT (e10.s2, the export run): `--plan` prints the run it would make and runs nothing. The
/// consumer build is in the first batch, every job whose command runs the consumer's executables
/// comes after it, every other job of both sections is in exactly one batch, no batch holds more
/// jobs than the batch size, and each soak job is held by name, in no batch.
#[test]
fn export_run_plans_every_registered_job_once_with_the_consumer_build_first_and_the_soaks_held() {
    let dir = std::env::temp_dir().join(format!("i143-export-run-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let reg = registry(&dir);
    let out = Command::new("bash")
        .arg(root().join("scripts/i143-export-run.sh"))
        .arg("--plan")
        .env("I143_ACCEPTANCE_JOBS", &reg)
        .env("I143_EXPORT_BATCH", "2")
        .current_dir(root())
        .output()
        .expect("the export run starts");
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "--plan exits 0: {text}");
    let mut batches: Vec<Vec<String>> = Vec::new();
    let mut held: BTreeMap<String, String> = BTreeMap::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("batch ") {
            let (_, jobs) = rest.split_once(": ").unwrap_or_else(|| panic!("a batch line names its jobs: {line}"));
            batches.push(jobs.split_whitespace().map(str::to_string).collect());
        } else if let Some(rest) = line.strip_prefix("held: ") {
            let (job, why) = rest.split_once(' ').unwrap_or((rest, ""));
            held.insert(job.to_string(), why.to_string());
        }
    }
    assert!(!batches.is_empty(), "the plan names its batches: {text}");
    assert!(batches[0].contains(&"i143-rshape-consumer-build".to_string()), "the consumer build is in the first batch: {batches:?}");
    for consumer in ["i143-5-process", "i143-rshape-x-chaos-process"] {
        assert!(!batches[0].contains(&consumer.to_string()), "{consumer} runs the consumer's executables, after the batch that builds them: {batches:?}");
    }
    assert!(batches.iter().all(|b| b.len() <= 2 && !b.is_empty()), "no batch is empty or over the batch size 2: {batches:?}");
    let mut planned: Vec<String> = batches.concat();
    planned.sort();
    let mut want: Vec<String> = ["i143-1-unit", "i143-2-process", "i143-3-chaos-container", "i143-5-process", "i143-rshape-consumer-build", "i143-rshape-x-chaos-process", "i143-rshape-y-static"].iter().map(|s| s.to_string()).collect();
    want.sort();
    assert_eq!(planned, want, "every job but the soaks is planned exactly once, the `_contract` note is no job");
    assert_eq!(held.keys().cloned().collect::<Vec<_>>(), vec!["i143-4-soak-process".to_string(), "i143-rshape-soak-z-container".to_string()], "each soak job is held by name: {text}");
    assert!(held.values().all(|why| why.contains("soak")), "a hold says why: {held:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (#3203, Luke 2026-10-27: "we can only learn more things"): `--soaks` at a HEAD without green
/// fast and release gate logs is refused by name, and with `I143_SOAKS_LEARN=1` it plans the soak jobs
/// anyway, as a learning run whose every receipt is marked `learning: true` and certifies nothing.
#[test]
fn export_run_plans_the_soaks_as_a_learning_run_without_the_green_gate_bar() {
    let dir = std::env::temp_dir().join(format!("i143-export-run-learn-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let reg = registry(&dir);
    // A deploy directory holding no gate log: no HEAD is green there.
    let home = dir.join("home");
    std::fs::create_dir_all(home.join("rust-distributed-mesh-deploy")).unwrap();
    let plan = |learn: bool| {
        let mut c = Command::new("bash");
        c.arg(root().join("scripts/i143-export-run.sh")).args(["--soaks", "--plan"]).env("I143_ACCEPTANCE_JOBS", &reg).env("HOME", &home).current_dir(root());
        if learn {
            c.env("I143_SOAKS_LEARN", "1");
        } else {
            c.env_remove("I143_SOAKS_LEARN");
        }
        let out = c.output().expect("the export run starts");
        (out.status.code(), format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr)))
    };
    let (code, text) = plan(false);
    assert_eq!(code, Some(2), "the soaks' bar holds without green gate logs: {text}");
    assert!(text.contains("REFUSED --soaks") && text.contains("no green fast gate log"), "{text}");
    let (code, text) = plan(true);
    assert_eq!(code, Some(0), "a learning run plans without the bar: {text}");
    assert!(text.contains("learning run") && text.contains("learning: true"), "the run says it certifies nothing: {text}");
    let planned: Vec<&str> = text.lines().filter_map(|l| l.strip_prefix("batch ")).flat_map(|l| l.split_once(": ").map(|(_, j)| j.split_whitespace()).into_iter().flatten()).collect();
    for soak in ["i143-4-soak-process", "i143-rshape-soak-z-container"] {
        assert!(planned.contains(&soak), "{soak} is planned: {text}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
