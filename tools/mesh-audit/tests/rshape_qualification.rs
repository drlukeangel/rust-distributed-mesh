//! i143.e11.s6 (#2949): the export qualification audit (`rshape-qualification`). A complete green
//! fixture is synthesised in a temp dir from the REAL matrix and registry (every owner job, every
//! canonical and static cell, S1-S5 and both replays on both providers), then each planted defect
//! must fail by its exact named rule `rshape-qualification-<rule>`.

use rafka_mesh_audit::rshape::{qualify, sha_file, Args};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static N: AtomicUsize = AtomicUsize::new(0);
const SHA: &str = "78b1dcac5d5a0c44d2fb6a19632d8793e2c2b0a1";
const IMPORTED: &str = "0b2fc0f8584e1eee2e027bc1e3399d01fcf1af12";

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}

fn jread(p: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))).unwrap()
}

fn jwrite(p: &Path, v: &Value) {
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, serde_json::to_vec_pretty(v).unwrap()).unwrap();
}

fn s<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or_default()
}

struct Fixture {
    root: PathBuf,
    matrix: Value,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn all_files(dir: &Path) -> Vec<PathBuf> {
    let mut v = vec![];
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        if e.path().is_dir() { v.extend(all_files(&e.path())) } else { v.push(e.path()) }
    }
    v.sort();
    v
}

fn receipt(root: &Path, job: &str, issue: u64, layer: &str, names: &[String], dirs: &[PathBuf]) -> Value {
    let cells: Vec<Value> = names
        .iter()
        .zip(dirs)
        .map(|(n, d)| {
            let arts: serde_json::Map<String, Value> = all_files(d).iter().map(|f| (f.to_string_lossy().to_string(), json!(sha_file(f).unwrap()))).collect();
            json!({"name": n, "evidence": "cell", "outcome": "ok", "refusal": null, "artifacts": arts})
        })
        .collect();
    let _ = root;
    json!({"job": job, "issue": issue, "layer": layer, "source_sha": SHA, "dirty_paths": 0, "started": "t", "finished": "t", "outcome": "ok", "cells": cells})
}

impl Fixture {
    fn build(pre: impl Fn(&Path, &mut Value)) -> Fixture {
        Self::build2(pre, |_, _| {})
    }

    /// `pre` runs before the replay manifests hash their sources; `post` after, before the receipts.
    fn build2(pre: impl Fn(&Path, &mut Value), post: impl Fn(&Path, &mut Value)) -> Fixture {
        let root = std::env::temp_dir().join(format!("rshape-qual-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
        let _ = std::fs::remove_dir_all(&root);
        let mut matrix = jread(&repo().join("tools/mesh-audit/i143-rshape-matrix.json"));
        let registry = jread(&repo().join("tools/mesh-audit/i143-acceptance-jobs.json"));
        let canon = matrix["shapes"]["canonical"].clone();
        let (ev, owners) = (root.join("target/i143-rshape"), root.join("target/i143-acceptance/jobs"));
        jwrite(&root.join("tools/mesh-audit/i143-acceptance-jobs.json"), &registry);
        // Owner receipts: one artifact each.
        for (job, j) in registry["jobs"].as_object().unwrap() {
            let d = root.join("owner-cells").join(job);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join("result.json"), job).unwrap();
            jwrite(&owners.join(format!("{job}.json")), &receipt(&root, job, j["issue"].as_u64().unwrap_or(0), s(j, "layer"), &["cell".into()], &[d]));
        }
        jwrite(&ev.join("consumer-build/manifest.json"), &json!({"candidate_sha": IMPORTED}));
        jwrite(&ev.join("parity.json"), &json!({"eligible": true, "mirror_pending": 0}));
        // Cell directories from the matrix.
        let cells: Vec<Value> = matrix["cells"].as_array().unwrap().to_vec();
        for c in &cells {
            if !matches!(s(c, "tier"), "canonical" | "static") {
                continue;
            }
            let d = root.join(s(c, "dir"));
            let provider = s(c, "provider");
            jwrite(&d.join("manifest.json"), &json!({"provider": if provider == "none" { "" } else { provider }}));
            if s(c, "tier") == "canonical" {
                jwrite(&d.join("topology-initial.json"), &json!({"shape_requested": {"tier": "canonical", "meshes": ["mesh1", "mesh2"], "per_mesh": canon["per_mesh"], "total": canon["total"]}}));
                jwrite(&d.join("result.json"), &json!({"rdm_candidate_sha": IMPORTED, "soak_seconds": 1800, "qualifies_export": true}));
            }
        }
        pre(&root, &mut matrix);
        for p in ["process", "container"] {
            let rc = cells.iter().find(|c| s(c, "test") == "mock_soak_replay_reproduces_qualified_invariants" && s(c, "provider") == p).unwrap();
            let mut stories = vec![];
            for n in 1..=5 {
                let so = &matrix["soaks"][format!("S{n}")];
                let sc = cells.iter().find(|c| s(c, "test") == s(so, "test") && s(c, "provider") == p).unwrap();
                stories.push(json!({"story": format!("S{n}"), "source_dir": s(sc, "dir"), "source_result_sha256": sha_file(&root.join(s(sc, "dir")).join("result.json")).unwrap(), "seed": matrix["seed"], "reproduced": true}));
            }
            jwrite(&root.join(s(rc, "dir")).join("replay-manifest.json"), &json!({"provider": p, "stories": stories}));
        }
        post(&root, &mut matrix);
        // R-shape receipts from the matrix.
        let mut jobs: std::collections::BTreeMap<String, Vec<&Value>> = Default::default();
        for c in &cells {
            if matches!(s(c, "tier"), "canonical" | "static") {
                jobs.entry(s(c, "job").to_string()).or_default().push(c);
            }
        }
        for (job, cs) in jobs {
            let names: Vec<String> = cs.iter().map(|c| s(c, "test").to_string()).collect();
            let dirs: Vec<PathBuf> = cs.iter().map(|c| root.join(s(c, "dir"))).collect();
            let j = &registry["rshape_jobs"][&job];
            let j = if j.is_null() { &json!({"issue": 2949, "layer": "process"}) } else { j };
            jwrite(&ev.join("jobs").join(format!("{job}.json")), &receipt(&root, &job, j["issue"].as_u64().unwrap(), s(j, "layer"), &names, &dirs));
        }
        jwrite(&root.join("tools/mesh-audit/i143-rshape-matrix.json"), &matrix);
        Fixture { root, matrix }
    }

    fn run(&self) -> Vec<String> {
        qualify(&Args {
            matrix: self.root.join("tools/mesh-audit/i143-rshape-matrix.json"),
            evidence: self.root.join("target/i143-rshape"),
            owner_receipts: self.root.join("target/i143-acceptance/jobs"),
            output: self.root.join("target/i143-rshape/export/qualification.json"),
            root: self.root.clone(),
            candidate: Some(SHA.into()),
            parity: None,
            registry: None,
            consumer_manifest: None,
        })
        .violations
    }

    fn cell_dir(&self, test: &str, provider: &str) -> PathBuf {
        let c = self.matrix["cells"].as_array().unwrap().iter().find(|c| s(c, "test") == test && s(c, "provider") == provider).unwrap();
        self.root.join(s(c, "dir"))
    }

    fn job_receipt(&self, job: &str) -> PathBuf {
        self.root.join("target/i143-rshape/jobs").join(format!("{job}.json"))
    }
}

fn names(v: &[String], rule: &str) -> Vec<String> {
    v.iter().filter(|x| x.starts_with(&format!("rshape-qualification-{rule}:"))).cloned().collect()
}

fn assert_only(v: &[String], rule: &str, mention: &str) {
    let hit = names(v, rule);
    assert!(hit.iter().any(|x| x.contains(mention)), "expected rshape-qualification-{rule} naming `{mention}`, got {v:#?}");
    let other: Vec<&String> = v.iter().filter(|x| !x.starts_with(&format!("rshape-qualification-{rule}:"))).collect();
    assert!(other.is_empty(), "the planted defect fails by its own rule only: {other:#?}");
}

const SOAK1: &str = "mock_soak_routing_preserves_continuous_invariants";
const REPLAY: &str = "mock_soak_replay_reproduces_qualified_invariants";

#[test]
fn rshape_manifest_requires_all_canonical_owner_and_provider_receipts() {
    let f = Fixture::build(|_, _| {});
    let v = f.run();
    assert!(v.is_empty(), "the complete fixture qualifies: {v:#?}");
}

#[test]
fn a_missing_owner_receipt_fails_missing_receipt() {
    let f = Fixture::build(|_, _| {});
    let job = jread(&f.root.join("tools/mesh-audit/i143-acceptance-jobs.json"))["jobs"].as_object().unwrap().keys().next().unwrap().clone();
    std::fs::remove_file(f.root.join("target/i143-acceptance/jobs").join(format!("{job}.json"))).unwrap();
    assert_only(&f.run(), "missing-receipt", &job);
}

#[test]
fn a_missing_provider_receipt_fails_missing_receipt() {
    let f = Fixture::build(|_, _| {});
    std::fs::remove_file(f.job_receipt("i143-rshape-soak-routing-container")).unwrap();
    assert_only(&f.run(), "missing-receipt", "i143-rshape-soak-routing-container");
}

#[test]
fn a_cell_run_on_the_wrong_provider_fails_wrong_provider() {
    let f = Fixture::build(|root, m| {
        let c = m["cells"].as_array().unwrap().iter().find(|c| s(c, "test") == SOAK1 && s(c, "provider") == "container").unwrap();
        jwrite(&root.join(s(c, "dir")).join("manifest.json"), &json!({"provider": "process"}));
    });
    assert_only(&f.run(), "wrong-provider", SOAK1);
}

#[test]
fn a_cell_on_the_wrong_shape_fails_wrong_shape() {
    let f = Fixture::build(|root, m| {
        let c = m["cells"].as_array().unwrap().iter().find(|c| s(c, "test") == SOAK1 && s(c, "provider") == "process").unwrap();
        jwrite(&root.join(s(c, "dir")).join("topology-initial.json"), &json!({"shape_requested": {"tier": "fast", "meshes": ["mesh1", "mesh2"], "per_mesh": {"node_admin": 2, "compute": 1, "gateway": 1, "broker": 1}, "total": 10}}));
    });
    assert_only(&f.run(), "wrong-shape", SOAK1);
}

#[test]
fn a_receipt_at_another_sha_fails_stale_sha() {
    let f = Fixture::build(|_, _| {});
    let p = f.job_receipt("i143-rshape-composition-process");
    let mut r = jread(&p);
    r["source_sha"] = json!("0000000000000000000000000000000000000000");
    jwrite(&p, &r);
    assert_only(&f.run(), "stale-sha", "i143-rshape-composition-process");
}

#[test]
fn a_result_that_imported_another_candidate_fails_stale_sha() {
    let f = Fixture::build(|root, m| {
        let c = m["cells"].as_array().unwrap().iter().find(|c| s(c, "test") == SOAK1 && s(c, "provider") == "process").unwrap();
        jwrite(&root.join(s(c, "dir")).join("result.json"), &json!({"rdm_candidate_sha": "1111111111111111111111111111111111111111", "soak_seconds": 1800, "qualifies_export": true}));
    });
    let v = f.run();
    assert!(!names(&v, "stale-sha").is_empty() && v.iter().all(|x| x.starts_with("rshape-qualification-stale-sha:")), "{v:#?}");
}

#[test]
fn a_receipt_of_an_unknown_job_fails_unknown_job() {
    let f = Fixture::build(|_, _| {});
    jwrite(&f.root.join("target/i143-acceptance/jobs/i143-9999-process.json"), &json!({"job": "i143-9999-process", "outcome": "ok", "cells": []}));
    assert_only(&f.run(), "unknown-job", "i143-9999-process");
    let f = Fixture::build(|_, _| {});
    jwrite(&f.root.join("target/i143-rshape/jobs/i143-rshape-invented.json"), &json!({"job": "i143-rshape-invented", "outcome": "ok", "cells": []}));
    assert_only(&f.run(), "unknown-job", "i143-rshape-invented");
}

#[test]
fn an_artifact_changed_after_the_receipt_fails_hash_mismatch() {
    let f = Fixture::build(|_, _| {});
    let d = f.cell_dir(SOAK1, "process");
    std::fs::write(d.join("manifest.json"), serde_json::to_vec(&json!({"provider": "process", "edited": true})).unwrap()).unwrap();
    assert_only(&f.run(), "hash-mismatch", "manifest.json");
}

#[test]
fn a_receipt_from_a_dirty_tree_fails_dirty() {
    let f = Fixture::build(|_, _| {});
    let p = f.job_receipt("i143-rshape-composition-container");
    let mut r = jread(&p);
    r["dirty_paths"] = json!(3);
    jwrite(&p, &r);
    assert_only(&f.run(), "dirty", "i143-rshape-composition-container");
}

#[test]
fn a_receipt_without_a_matrix_cell_fails_missing_cell() {
    let f = Fixture::build(|_, _| {});
    let p = f.job_receipt("i143-rshape-composition-process");
    let mut r = jread(&p);
    r["cells"].as_array_mut().unwrap().pop();
    jwrite(&p, &r);
    assert_only(&f.run(), "missing-cell", "i143-rshape-composition-process");
}

#[test]
fn a_short_soak_fails_soak_too_short() {
    let f = Fixture::build(|root, m| {
        let c = m["cells"].as_array().unwrap().iter().find(|c| s(c, "test") == SOAK1 && s(c, "provider") == "process").unwrap();
        jwrite(&root.join(s(c, "dir")).join("result.json"), &json!({"rdm_candidate_sha": IMPORTED, "soak_seconds": 120, "qualifies_export": false}));
    });
    assert_only(&f.run(), "soak-too-short", "S1");
}

#[test]
fn a_replay_missing_a_story_fails_replay_incomplete() {
    let f = Fixture::build2(|_, _| {}, |root, m| {
        let c = m["cells"].as_array().unwrap().iter().find(|c| s(c, "test") == REPLAY && s(c, "provider") == "process").unwrap();
        let p = root.join(s(c, "dir")).join("replay-manifest.json");
        let mut man = jread(&p);
        man["stories"].as_array_mut().unwrap().pop();
        jwrite(&p, &man);
    });
    assert_only(&f.run(), "replay-incomplete", "S5");
}

#[test]
fn a_replay_of_another_run_fails_replay_source_mismatch() {
    let f = Fixture::build2(|_, _| {}, |root, m| {
        let c = m["cells"].as_array().unwrap().iter().find(|c| s(c, "test") == REPLAY && s(c, "provider") == "process").unwrap();
        let other = m["cells"].as_array().unwrap().iter().find(|c| s(c, "test") == SOAK1 && s(c, "provider") == "container").unwrap();
        let p = root.join(s(c, "dir")).join("replay-manifest.json");
        let mut man = jread(&p);
        man["stories"][0]["source_dir"] = json!(s(other, "dir"));
        jwrite(&p, &man);
    });
    assert_only(&f.run(), "replay-source-mismatch", "S1");
}

#[test]
fn an_ineligible_parity_report_fails_parity_ineligible() {
    let f = Fixture::build(|_, _| {});
    jwrite(&f.root.join("target/i143-rshape/parity.json"), &json!({"eligible": false, "mirror_pending": 2}));
    assert_only(&f.run(), "parity-ineligible", "eligible=false");
}
