//! i143 e10.s2 (#2790) acceptance: the export certifier. One candidate RDM SHA is eligible only when
//! the transport-parity scan is eligible (no `MIRROR pending` row, no unclassified boundary commit)
//! and every owner's job receipt (every job of the registry's `jobs` and `rshape_jobs`) is `ok` at
//! that SHA, with no uncommitted path, with every recorded artifact still hashing; anything else is refused
//! by name. Run by `scripts/i143-acceptance-gate.sh i143-2790-export`, each cell writes its
//! `result.json` into `I143_ACCEPTANCE_DIR`.

use rafka_mesh_audit::export::{self, CertifyInput, ExportManifest, Refusal};
use rafka_mesh_audit::parity::{self, ScanInput};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::Command;

const CANDIDATE: &str = "0123456789abcdef0123456789abcdef01234567";

fn temp(tag: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!("export-2790-{tag}-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@t")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@t")
        .output()
        .expect("git runs");
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn commit(dir: &Path, file: &str, msg: &str) -> String {
    let p = dir.join(file);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    let prev = std::fs::read_to_string(&p).unwrap_or_default();
    std::fs::write(&p, format!("{prev}{msg}\n")).unwrap();
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", msg]);
    git(dir, &["rev-parse", "HEAD"])
}

/// A throwaway rafka-v2 history: a base and two boundary commits after it.
struct Rafka {
    dir: PathBuf,
    base: String,
    transport: String,
    node_base: String,
    tip: String,
}

impl Drop for Rafka {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn rafka() -> Rafka {
    let dir = temp("rafka");
    git(&dir, &["init", "-q", "-b", "main"]);
    commit(&dir, "README.md", "root");
    let base = commit(&dir, "crates/rafka-mesh-transport/src/lib.rs", "base");
    let transport = commit(&dir, "crates/rafka-mesh-transport/src/lib.rs", "transport change");
    let node_base = commit(&dir, "crates/rafka-node-base/src/lib.rs", "node-base change");
    let tip = git(&dir, &["rev-parse", "HEAD"]);
    Rafka { dir, base, transport, node_base, tip }
}

fn row(sha: &str, disposition: &str) -> String {
    let proof = if disposition == "**MIRROR**" { "RDM `0123abcd99`, test `carrying_test`" } else { "proof text" };
    format!("| `{}` | #1 | change | {disposition}. {proof} |\n", &sha[..10])
}

fn scan(r: &Rafka, rows: &str) -> parity::Report {
    let ledger = format!(
        "# ledger\n\n**Fixed audit base:** `{}`  \n**Review-through tip for this reconciliation:** `{}`\n\n## Boundary\n\n```text\ncrates/rafka-mesh-transport/**\ncrates/rafka-node-base/**\n```\n\n## Post-base ledger\n\n| Rafka SHA | issue | change | disposition / proof |\n|---|---|---|---|\n{rows}",
        &r.base[..10],
        &r.tip[..10],
    );
    parity::scan(&ScanInput { repo: &r.dir, ledger_text: &ledger, through: None, connections_text: None })
}

fn clean(r: &Rafka) -> parity::Report {
    let report = scan(r, &[row(&r.transport, "**MIRROR**"), row(&r.node_base, "**RAFKA DOMAIN**")].concat());
    assert!(report.eligible, "the fixture's complete ledger is eligible: {:#?}", report.violations);
    report
}

/// An RDM root holding the registry's owners and the receipts the runner writes for them.
struct Estate {
    root: PathBuf,
    registry: Value,
}

impl Drop for Estate {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

impl Estate {
    /// Two owners under `jobs`, one under `rshape_jobs` (beside the section's `_contract` note).
    fn new() -> Self {
        let cell = |job: &str| json!({ "cells": [{ "name": "cell", "source": "x.rs", "command": "true", "dir": format!("target/{job}/cell") }] });
        let registry = json!({
            "contract": "fixture",
            "jobs": { "i143-1-unit": cell("i143-1-unit"), "i143-2-process": cell("i143-2-process") },
            "rshape_jobs": { "_contract": "fixture", "i143-rshape-a": cell("i143-rshape-a") },
        });
        Self { root: temp("estate"), registry }
    }

    fn receipt_path(job: &str) -> String {
        let dir = if job.starts_with("i143-rshape-") { "target/i143-rshape/jobs" } else { "target/i143-acceptance/jobs" };
        format!("{dir}/{job}.json")
    }

    fn artifact(job: &str) -> String {
        format!("target/{job}/cell/result.json")
    }

    /// The runner's receipt for `job`: its one cell's artifact written and hashed.
    fn receipt(&self, job: &str, source_sha: &str, outcome: &str, dirty_paths: u64) {
        let artifact = Self::artifact(job);
        let body = format!("{{\"cell\":\"cell\",\"job\":\"{job}\"}}");
        std::fs::create_dir_all(self.root.join(&artifact).parent().unwrap()).unwrap();
        std::fs::write(self.root.join(&artifact), &body).unwrap();
        let receipt = json!({
            "job": job, "issue": 1, "layer": "unit", "source_sha": source_sha, "dirty_paths": dirty_paths,
            "started": "t0", "finished": "t1", "outcome": outcome,
            "cells": [{ "name": "cell", "evidence": "cell", "outcome": outcome, "refusal": null, "skip": null,
                        "artifacts": { artifact.clone(): sha256(body.as_bytes()) } }],
        });
        let path = self.root.join(Self::receipt_path(job));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();
    }

    fn all_ok(&self, source_sha: &str) {
        for job in ["i143-1-unit", "i143-2-process", "i143-rshape-a"] {
            self.receipt(job, source_sha, "ok", 0);
        }
    }

    fn certify(&self, candidate: &str, parity: &parity::Report) -> ExportManifest {
        export::certify(&CertifyInput { candidate, parity, registry: &self.registry, root: &self.root })
    }
}

/// The runner's `result.json` for this cell: its direct observations.
fn record(cell: &str, observations: Value) {
    if let Ok(dir) = std::env::var("I143_ACCEPTANCE_DIR") {
        std::fs::create_dir_all(&dir).unwrap();
        let result = json!({ "cell": cell, "evidence": "cell", "observations": observations });
        std::fs::write(Path::new(&dir).join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    }
}

/// CONTRACT (PRD §14, e10.s2): a candidate whose parity scan holds a `MIRROR pending` row or an
/// unclassified boundary commit is refused by name, whatever its receipts; the same candidate and
/// receipts certify once the ledger is complete.
#[test]
fn export_manifest_rejects_pending_or_unclassified_parity() {
    let r = rafka();
    let estate = Estate::new();
    estate.all_ok(CANDIDATE);

    let pending = estate.certify(CANDIDATE, &scan(&r, &[row(&r.transport, "**MIRROR pending**"), row(&r.node_base, "**RAFKA DOMAIN**")].concat()));
    assert!(!pending.eligible, "a MIRROR pending row refuses the candidate: {pending:#?}");
    assert_eq!(pending.refusals, vec![Refusal::ParityMirrorPending { sha: r.transport.clone() }], "named, and nothing else refused");
    assert_eq!(pending.mirror_pending, 1);

    let unclassified = estate.certify(CANDIDATE, &scan(&r, &row(&r.transport, "**MIRROR**")));
    assert!(!unclassified.eligible, "an unclassified boundary commit refuses the candidate: {unclassified:#?}");
    assert_eq!(
        unclassified.refusals,
        vec![Refusal::ParityUnclassified { sha: r.node_base.clone(), subject: "node-base change".into() }],
        "named, and nothing else refused"
    );

    let both = estate.certify(CANDIDATE, &scan(&r, &row(&r.transport, "**MIRROR pending**")));
    assert!(!both.eligible);
    assert_eq!(
        both.refusals,
        vec![
            Refusal::ParityMirrorPending { sha: r.transport.clone() },
            Refusal::ParityUnclassified { sha: r.node_base.clone(), subject: "node-base change".into() },
        ],
        "every parity refusal is named in one run"
    );

    let complete = estate.certify(CANDIDATE, &clean(&r));
    assert!(complete.eligible, "the same candidate and receipts certify once parity is clean: {complete:#?}");
    assert_eq!(complete.refusals, vec![]);

    record(
        "export_manifest_rejects_pending_or_unclassified_parity",
        json!({
            "candidate": CANDIDATE,
            "mirror_pending": { "eligible": pending.eligible, "refusals": pending.refusals },
            "unclassified": { "eligible": unclassified.eligible, "refusals": unclassified.refusals },
            "both": { "eligible": both.eligible, "refusals": both.refusals },
            "complete": { "eligible": complete.eligible, "parity_ledger_digest": complete.parity_ledger_digest },
        }),
    );
}

/// CONTRACT (PRD §14, e10.s2): one candidate is certified only with every owner's receipt: every job
/// of the registry's `jobs` and `rshape_jobs` has an `ok` receipt at that SHA, written with no uncommitted path,
/// whose artifacts still hash. A missing, red, other-SHA or dirty receipt, a changed or missing
/// artifact, and a candidate that is not one exact SHA are each refused by name.
#[test]
fn export_manifest_certifies_one_candidate_with_all_owner_receipts() {
    let r = rafka();
    let parity = clean(&r);

    let estate = Estate::new();
    estate.all_ok(CANDIDATE);
    let certified = estate.certify(CANDIDATE, &parity);
    assert!(certified.eligible, "{certified:#?}");
    assert_eq!(certified.candidate_sha, CANDIDATE);
    assert_eq!(certified.owners, 3, "both registry sections' jobs are owners; the `_contract` note is not");
    let jobs: Vec<(&str, &str)> = certified.receipts.iter().map(|c| (c.job.as_str(), c.section.as_str())).collect();
    assert_eq!(jobs, vec![("i143-1-unit", "jobs"), ("i143-2-process", "jobs"), ("i143-rshape-a", "rshape_jobs")]);
    for c in &certified.receipts {
        assert_eq!(c.sha256, sha256(&std::fs::read(estate.root.join(&c.path)).unwrap()), "{}: the manifest records the receipt's own hash", c.job);
    }
    assert_eq!(certified.rafka_parity_through, r.tip);

    let other = estate.certify("89abcdef0123456789abcdef0123456789abcdef", &parity);
    assert!(!other.eligible, "the receipts certify their own SHA and no other candidate");
    assert_eq!(other.refusals.len(), 3);
    assert!(other.refusals.iter().all(|x| matches!(x, Refusal::ReceiptAtAnotherSha { source_sha, .. } if source_sha == CANDIDATE)), "{:#?}", other.refusals);

    let short = estate.certify("0123456789", &parity);
    assert!(short.refusals.contains(&Refusal::CandidateNotASha { candidate: "0123456789".into() }), "{:#?}", short.refusals);

    let missing = Estate::new();
    missing.receipt("i143-1-unit", CANDIDATE, "ok", 0);
    missing.receipt("i143-2-process", CANDIDATE, "ok", 0);
    let m = missing.certify(CANDIDATE, &parity);
    assert!(!m.eligible);
    assert_eq!(m.refusals, vec![Refusal::ReceiptMissing { job: "i143-rshape-a".into(), path: "target/i143-rshape/jobs/i143-rshape-a.json".into() }]);
    assert_eq!(m.receipts.len(), 2);

    let red = Estate::new();
    red.all_ok(CANDIDATE);
    red.receipt("i143-2-process", CANDIDATE, "refused", 0);
    let rr = red.certify(CANDIDATE, &parity);
    assert!(!rr.eligible);
    assert_eq!(rr.refusals, vec![Refusal::ReceiptNotOk { job: "i143-2-process".into(), outcome: "refused".into() }]);

    let stale = Estate::new();
    stale.all_ok(CANDIDATE);
    stale.receipt("i143-1-unit", "fedcba9876543210fedcba9876543210fedcba98", "ok", 0);
    let s = stale.certify(CANDIDATE, &parity);
    assert_eq!(s.refusals, vec![Refusal::ReceiptAtAnotherSha { job: "i143-1-unit".into(), source_sha: "fedcba9876543210fedcba9876543210fedcba98".into() }]);

    let dirty = Estate::new();
    dirty.all_ok(CANDIDATE);
    dirty.receipt("i143-rshape-a", CANDIDATE, "ok", 3);
    let d = dirty.certify(CANDIDATE, &parity);
    assert_eq!(d.refusals, vec![Refusal::ReceiptDirty { job: "i143-rshape-a".into(), dirty_paths: 3 }]);

    let moved = Estate::new();
    moved.all_ok(CANDIDATE);
    let changed = Estate::artifact("i143-1-unit");
    let recorded = sha256(&std::fs::read(moved.root.join(&changed)).unwrap());
    std::fs::write(moved.root.join(&changed), b"rewritten after the receipt").unwrap();
    let gone = Estate::artifact("i143-2-process");
    std::fs::remove_file(moved.root.join(&gone)).unwrap();
    let a = moved.certify(CANDIDATE, &parity);
    assert!(!a.eligible);
    assert_eq!(
        a.refusals,
        vec![
            Refusal::ArtifactChanged {
                job: "i143-1-unit".into(),
                cell: "cell".into(),
                path: changed.clone(),
                recorded,
                now: sha256(b"rewritten after the receipt"),
            },
            Refusal::ArtifactMissing { job: "i143-2-process".into(), cell: "cell".into(), path: gone.clone() },
        ],
        "every artifact refusal is named in one run"
    );

    record(
        "export_manifest_certifies_one_candidate_with_all_owner_receipts",
        json!({
            "candidate": CANDIDATE,
            "certified": { "eligible": certified.eligible, "owners": certified.owners, "receipts": certified.receipts },
            "another_candidate": other.refusals,
            "not_a_sha": short.refusals,
            "missing": m.refusals,
            "red": rr.refusals,
            "another_sha": s.refusals,
            "dirty": d.refusals,
            "artifacts": a.refusals,
        }),
    );
}
