//! i143 e10.s2 (#2790) acceptance: the export certifier. One candidate RDM SHA is eligible only when
//! the transport-parity scan is eligible (no `MIRROR pending` row, no unclassified boundary commit)
//! and every owner's job receipt (every job of the registry's `jobs` and `rshape_jobs`) is `ok` at
//! that SHA, with no uncommitted path, with every recorded artifact still hashing, and the fast and
//! release gate logs at that SHA prove every §14 cell of the import manifest; anything else is refused
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

/// An RDM root holding the registry's owners and the receipts the runner writes for them, beside
/// green fast and release gate records at the candidate that prove the fixture's §14 cells.
struct Estate {
    root: PathBuf,
    registry: Value,
    gates: Gates,
}

impl Drop for Estate {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// The candidate's short SHA, as the gate names its logs (`git rev-parse --short HEAD`).
const SHORT: &str = "01234567";
const FAST: &str = "gates-lane1-01234567.log";
const RELEASE: &str = "gates-lane1-01234567release.log";
const EXE_MAIN: &str = "/lane/target/debug/deps/main-0a1b";
const EXE_LIB: &str = "/lane/target/debug/deps/pkg-2c3d";
const EXE_PLAIN: &str = "/lane/target/debug/deps/plain-4e5f";
/// The fixture's §14 cells the green gates prove.
const CELLS: [&str; 3] = ["crates/pkg/tests/plain.rs::a_cell", "crates/pkg/tests/stem_a.rs::b_cell", "crates/pkg/src/thing.rs::tests::d_cell"];

/// What `scripts/rdm-gate-at.sh` leaves in the deploy directory at one SHA, for a package `pkg`
/// (`crates/pkg`): its `main` test executable (the file `plain` and the stems `stem_a`, `stem_i` and
/// `orphan`), its lib (`thing::tests`) and the file `plain`'s own executable. The rdm-test shape: the
/// whole executable runs as one job skipping every stem, and each stem runs as its own job.
struct Gates {
    dir: PathBuf,
}

impl Drop for Gates {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Gates {
    /// Green fast and release logs at `head`, their jobs, artifacts and summaries, and the discovery.
    fn green(head: &str) -> Self {
        let g = Self { dir: temp("gates") };
        let discovery = json!({ "pkg": {
            "main": { "kind": "test", "count": 5, "tests": ["orphan::e_cell", "plain::a_cell", "plain::twice_cell", "stem_a::b_cell", "stem_i::i_cell"] },
            "pkg": { "kind": "lib", "count": 1, "tests": ["thing::tests::d_cell"] },
            "plain": { "kind": "test", "count": 1, "tests": ["twice_cell"] },
        }});
        std::fs::write(g.dir.join(format!("discovery-{SHORT}.json")), serde_json::to_vec_pretty(&discovery).unwrap()).unwrap();
        for log in [FAST, RELEASE] {
            g.write_log(log, &format!("[gate] head={head} dirty=0"), &["ok pkg:main 1s", "ok pkg:pkg 1s", "ok stem_a 2s", "ok stem_i 2s", "DONE 5s"]);
        }
        g
    }

    /// `log` with its first line and its status lines, beside its `.log.d`.
    fn write_log(&self, log: &str, first: &str, status: &[&str]) {
        let mut text = format!("{first}\nok build 1s\nok discovery 0s\n");
        for l in status {
            text.push_str(l);
            text.push('\n');
        }
        std::fs::write(self.dir.join(log), text).unwrap();
        let d = self.dir.join(format!("{log}.d"));
        std::fs::create_dir_all(&d).unwrap();
        let artifact = |name: &str, kind: &str, src: &str, exe: &str, test: bool| {
            json!({ "reason": "compiler-artifact", "package_id": "path+file:///lane/crates/pkg#0.1.0", "manifest_path": "/lane/crates/pkg/Cargo.toml",
                    "target": { "kind": [kind], "name": name, "src_path": src }, "profile": { "test": test }, "executable": exe })
            .to_string()
        };
        let artifacts = [
            artifact("pkg", "lib", "/lane/crates/pkg/src/lib.rs", "", false),
            artifact("main", "test", "/lane/crates/pkg/tests/main.rs", EXE_MAIN, true),
            artifact("pkg", "lib", "/lane/crates/pkg/src/lib.rs", EXE_LIB, true),
            artifact("plain", "test", "/lane/crates/pkg/tests/plain.rs", EXE_PLAIN, true),
        ];
        std::fs::write(d.join("artifacts.json"), artifacts.join("\n") + "\n").unwrap();
        let jobs = [
            format!("pkg:main sh:(cd /lane/crates/pkg && CARGO_MANIFEST_DIR=/lane/crates/pkg {EXE_MAIN} -q  --test-threads=8  --skip stem_a:: --skip stem_i:: --skip orphan::)"),
            format!("pkg:pkg sh:(cd /lane/crates/pkg && CARGO_MANIFEST_DIR=/lane/crates/pkg {EXE_LIB} -q )"),
            format!("stem_a {EXE_MAIN} --test-threads=8 stem_a::"),
            format!("stem_i {EXE_MAIN} --test-threads=8 stem_i::"),
        ];
        std::fs::write(d.join("jobs"), jobs.join("\n") + "\n").unwrap();
        let summary = |passed: u32, ignored: u32| format!("test result: ok. {passed} passed; 0 failed; {ignored} ignored; 0 measured; 3 filtered out; finished in 0.10s\n");
        std::fs::write(d.join("pkg:main.log"), format!("\nrunning 2 tests\n..\n{}", summary(2, 0))).unwrap();
        std::fs::write(d.join("pkg:pkg.log"), format!("\nrunning 1 test\n.\n{}", summary(1, 0))).unwrap();
        std::fs::write(d.join("stem_a.log"), format!("\nrunning 1 test\ntest stem_a::b_cell ... ok\n\n{}", summary(1, 0))).unwrap();
        std::fs::write(d.join("stem_i.log"), format!("\nrunning 1 test\ntest stem_i::i_cell ... ignored\n\n{}", summary(0, 1))).unwrap();
    }

    fn certify(&self, estate: &Estate, candidate: &str, parity: &parity::Report, cells: &[String]) -> ExportManifest {
        export::certify(&CertifyInput { candidate, parity, registry: &estate.registry, root: &estate.root, gates: &self.dir, cells })
    }
}

fn cells(names: &[&str]) -> Vec<String> {
    names.iter().map(|c| c.to_string()).collect()
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
        Self { root: temp("estate"), registry, gates: Gates::green(CANDIDATE) }
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
        self.receipt_of(job, source_sha, outcome, dirty_paths, None);
    }

    /// The runner's receipt for `job` whose one cell `skip`ped by name: the runner calls the job `ok`.
    fn receipt_skipped(&self, job: &str, source_sha: &str, skip: &str) {
        self.receipt_of(job, source_sha, "ok", 0, Some(skip));
    }

    fn receipt_of(&self, job: &str, source_sha: &str, outcome: &str, dirty_paths: u64, skip: Option<&str>) {
        let cell_outcome = if skip.is_some() { "skipped" } else { outcome };
        let artifact = Self::artifact(job);
        let body = format!("{{\"cell\":\"cell\",\"job\":\"{job}\"}}");
        std::fs::create_dir_all(self.root.join(&artifact).parent().unwrap()).unwrap();
        std::fs::write(self.root.join(&artifact), &body).unwrap();
        let receipt = json!({
            "job": job, "issue": 1, "layer": "unit", "source_sha": source_sha, "dirty_paths": dirty_paths,
            "started": "t0", "finished": "t1", "outcome": outcome,
            "cells": [{ "name": "cell", "evidence": "cell", "outcome": cell_outcome, "refusal": null, "skip": skip,
                        "artifacts": { artifact.clone(): sha256(body.as_bytes()) } }],
        });
        let path = self.root.join(Self::receipt_path(job));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();
    }

    /// The runner's `ok` receipt for `job`, written by a learning soak run (`learning: true`).
    fn receipt_learning(&self, job: &str, source_sha: &str) {
        self.receipt(job, source_sha, "ok", 0);
        let path = self.root.join(Self::receipt_path(job));
        let mut receipt: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        receipt["learning"] = json!(true);
        std::fs::write(path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();
    }

    fn all_ok(&self, source_sha: &str) {
        for job in ["i143-1-unit", "i143-2-process", "i143-rshape-a"] {
            self.receipt(job, source_sha, "ok", 0);
        }
    }

    fn certify(&self, candidate: &str, parity: &parity::Report) -> ExportManifest {
        self.gates.certify(self, candidate, parity, &cells(&CELLS))
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
/// whose artifacts still hash. A missing, red, other-SHA or dirty receipt, a cell skipped by name
/// under an `ok` receipt (it proved nothing: §14 needs the container proof, and a cut cell skipped
/// without `nft` cuts nothing), a changed or missing artifact, and a candidate that is not one exact
/// SHA are each refused by name.
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
    assert!(!other.eligible, "the receipts and the gate logs certify their own SHA and no other candidate");
    let (at_another_sha, gates): (Vec<_>, Vec<_>) = other.refusals.iter().partition(|x| matches!(x, Refusal::ReceiptAtAnotherSha { .. }));
    assert_eq!(at_another_sha.len(), 3, "{:#?}", other.refusals);
    assert!(at_another_sha.iter().all(|x| matches!(x, Refusal::ReceiptAtAnotherSha { source_sha, .. } if source_sha == CANDIDATE)), "{:#?}", other.refusals);
    assert_eq!(
        gates,
        vec![
            &Refusal::GateLogMissing { gate: "fast".into(), pattern: "gates-<lane>-89abcdef.log".into() },
            &Refusal::GateLogMissing { gate: "release".into(), pattern: "gates-<lane>-89abcdefrelease.log".into() },
        ]
    );

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

    let skipped = Estate::new();
    skipped.all_ok(CANDIDATE);
    skipped.receipt_skipped("i143-2-process", CANDIDATE, "no-docker: the host has no reachable Docker domain");
    let k = skipped.certify(CANDIDATE, &parity);
    assert!(!k.eligible, "a cell skipped by name under an ok receipt proves nothing: {k:#?}");
    assert_eq!(
        k.refusals,
        vec![Refusal::ReceiptCellSkipped {
            job: "i143-2-process".into(),
            cell: "cell".into(),
            skip: "no-docker: the host has no reachable Docker domain".into(),
        }]
    );

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
            "skipped": k.refusals,
            "artifacts": a.refusals,
        }),
    );
}

/// CONTRACT (PRD §14, e10.s2): the fast and release gate logs at the candidate prove the import
/// manifest's §14 cells. Each log names the candidate as its full HEAD with no uncommitted path, ends
/// `DONE` with no `FAIL` line, and its sha256 is in the manifest; each cell is one discovered test that
/// a job of a certified log selects (its filter matches, no `--skip` does), whose summaries are `ok`
/// with nothing ignored. The rdm-test shape: a cell of the whole executable is proven by the job that
/// skips every stem, a stem's cell by the stem's own job, a lib cell by the lib's job.
#[test]
fn export_manifest_takes_green_fast_and_release_gate_logs_as_the_proof_of_every_section_14_cell() {
    let r = rafka();
    let parity = clean(&r);
    let estate = Estate::new();
    estate.all_ok(CANDIDATE);
    let m = estate.certify(CANDIDATE, &parity);
    assert!(m.eligible, "{m:#?}");
    assert_eq!(
        m.gate_logs,
        vec![
            export::CertifiedGateLog { gate: "fast".into(), path: FAST.into(), sha256: sha256(&std::fs::read(estate.gates.dir.join(FAST)).unwrap()) },
            export::CertifiedGateLog { gate: "release".into(), path: RELEASE.into(), sha256: sha256(&std::fs::read(estate.gates.dir.join(RELEASE)).unwrap()) },
        ],
        "both gate logs at the candidate are certified with their own hash"
    );
    let proven = |cell: &str, test: &str, job: &str| export::ProvenCell { cell: cell.into(), test: test.into(), log: FAST.into(), job: job.into() };
    assert_eq!(
        m.cells,
        vec![
            proven(CELLS[0], "pkg/main: plain::a_cell", "pkg:main"),
            proven(CELLS[1], "pkg/main: stem_a::b_cell", "stem_a"),
            proven(CELLS[2], "pkg/pkg: thing::tests::d_cell", "pkg:pkg"),
        ],
        "each cell names the discovered test and the job that ran it"
    );

    let none = estate.gates.certify(&estate, CANDIDATE, &parity, &[]);
    assert_eq!(none.refusals, vec![Refusal::ManifestCells { reason: "the §14 section names no cell".into() }], "an empty §14 proves nothing");

    let manifest = "# m\n\n## Gate record\n\n`T/x.rs::not_section_14`\n\n## §14 export gate: every bullet\n\n| bullet | cell |\n|---|---|\n\
        | a | `T/mesh/canonical_ids.rs::every_id`; `crates/rdm-core/src/ids.rs::tests::refuses` |\n\
        | b | `A/tests/node_objects.rs::a_call`; `…::another_call`; `T/build/accepted_topology.rs` (above) |\n\
        | c | `A/src/admin.rs::tests::only_root`; `T/mesh/canonical_ids.rs::every_id` |\n\n## Ops\n\n`A/tests/join_admission.rs::not_section_14`\n";
    assert_eq!(
        export::manifest_cells(manifest),
        vec![
            "crates/rdm-test/tests/mesh/canonical_ids.rs::every_id",
            "crates/rdm-core/src/ids.rs::tests::refuses",
            "crates/rdm-admin/tests/node_objects.rs::a_call",
            "crates/rdm-admin/tests/node_objects.rs::another_call",
            "crates/rdm-admin/src/admin.rs::tests::only_root",
        ],
        "only the §14 section's cells, `T/` and `A/` written out, `…::` taking the file before it, each once"
    );
    let real = export::manifest_cells(&std::fs::read_to_string(export::default_manifest()).unwrap());
    assert!(!real.is_empty(), "the import manifest names §14 cells");
    for c in &real {
        let file = c.split_once("::").unwrap().0;
        assert!(rafka_mesh_audit::workspace_root().join(file).is_file(), "§14 cell {c}: {file} is not a file of the workspace");
    }

    record(
        "export_manifest_takes_green_fast_and_release_gate_logs_as_the_proof_of_every_section_14_cell",
        json!({ "candidate": CANDIDATE, "gate_logs": m.gate_logs, "cells": m.cells, "empty_section": none.refusals, "manifest_cells": real.len() }),
    );
}

/// CONTRACT (PRD §14, e10.s2): a gate log at the candidate that is missing, red, unfinished, written at
/// another HEAD, written with uncommitted paths or without its HEAD line, or whose test discovery is
/// missing, is refused by name; the other gate's green log still proves the cells.
#[test]
fn export_manifest_refuses_a_candidate_whose_fast_or_release_gate_log_is_missing_red_or_at_another_tree() {
    let r = rafka();
    let parity = clean(&r);
    let estate = Estate::new();
    estate.all_ok(CANDIDATE);
    let certify = |g: &Gates| g.certify(&estate, CANDIDATE, &parity, &cells(&CELLS));
    let green = &["ok pkg:main 1s", "ok pkg:pkg 1s", "ok stem_a 2s", "ok stem_i 2s", "DONE 5s"];
    let head = format!("[gate] head={CANDIDATE} dirty=0");

    let missing = Gates { dir: temp("gates") };
    let m = certify(&missing);
    assert_eq!(
        m.refusals,
        vec![
            Refusal::GateLogMissing { gate: "fast".into(), pattern: "gates-<lane>-01234567.log".into() },
            Refusal::GateLogMissing { gate: "release".into(), pattern: "gates-<lane>-01234567release.log".into() },
        ]
    );
    assert!(m.cells.is_empty() && m.gate_logs.is_empty());

    let red = Gates::green(CANDIDATE);
    red.write_log(FAST, &head, &["ok pkg:main 1s", "ok pkg:pkg 1s", "FAIL stem_a 2s", "FAIL(timed-out) stem_i 300s", "DONE 300s"]);
    red.write_log(RELEASE, &head, &["ok pkg:main 1s", "ok pkg:pkg 1s", "ok stem_a 2s", "ok stem_i 2s"]);
    let rr = certify(&red);
    assert_eq!(
        rr.refusals,
        vec![
            Refusal::GateRed { log: FAST.into(), failed: vec!["stem_a".into(), "stem_i".into()], done: true },
            Refusal::GateRed { log: RELEASE.into(), failed: vec![], done: false },
        ]
    );
    assert!(rr.cells.is_empty(), "no certified log proves a cell: {:#?}", rr.cells);

    let tree = Gates::green(CANDIDATE);
    tree.write_log(FAST, "[gate] head=fedcba9876543210fedcba9876543210fedcba98 dirty=2", green);
    let t = certify(&tree);
    assert_eq!(
        t.refusals,
        vec![
            Refusal::GateAtAnotherSha { log: FAST.into(), head: "fedcba9876543210fedcba9876543210fedcba98".into() },
            Refusal::GateDirty { log: FAST.into(), dirty_paths: 2 },
        ]
    );
    assert_eq!(t.gate_logs.iter().map(|g| g.gate.as_str()).collect::<Vec<_>>(), vec!["release"]);
    assert!(t.cells.iter().all(|c| c.log == RELEASE) && t.cells.len() == 3, "the green release log still proves every cell: {:#?}", t.cells);

    let headless = Gates::green(CANDIDATE);
    headless.write_log(RELEASE, "ok consumer-build 84s", green);
    let h = certify(&headless);
    assert_eq!(
        h.refusals,
        vec![Refusal::GateUnreadable { log: RELEASE.into(), reason: "its first line is not `[gate] head=<40-hex> dirty=<n>`: \"ok consumer-build 84s\"".into() }]
    );

    let undiscovered = Gates::green(CANDIDATE);
    std::fs::remove_file(undiscovered.dir.join(format!("discovery-{SHORT}.json"))).unwrap();
    let u = certify(&undiscovered);
    assert_eq!(u.refusals, vec![Refusal::DiscoveryMissing { path: format!("discovery-{SHORT}.json") }]);

    record(
        "export_manifest_refuses_a_candidate_whose_fast_or_release_gate_log_is_missing_red_or_at_another_tree",
        json!({ "missing": m.refusals, "red": rr.refusals, "another_tree": t.refusals, "headless": h.refusals, "undiscovered": u.refusals }),
    );
}

/// CONTRACT (PRD §14, e10.s2): a §14 cell that is no discovered test, that names two discovered
/// tests, that no job of a certified gate log selects, or that only a job ignoring tests ran, is
/// refused by name; the cells the gates did prove are still listed.
#[test]
fn export_manifest_refuses_a_section_14_cell_no_green_gate_job_ran_clean() {
    let r = rafka();
    let parity = clean(&r);
    let estate = Estate::new();
    estate.all_ok(CANDIDATE);
    let named = [
        CELLS[0],
        "crates/pkg/tests/plain.rs::gone_cell",
        "crates/pkg/tests/plain.rs::twice_cell",
        "crates/pkg/tests/orphan.rs::e_cell",
        "crates/pkg/tests/stem_i.rs::i_cell",
        "crates/other/src/lib.rs::tests::x",
    ];
    let m = estate.gates.certify(&estate, CANDIDATE, &parity, &cells(&named));
    assert!(!m.eligible);
    assert_eq!(m.cells.iter().map(|c| c.cell.as_str()).collect::<Vec<_>>(), vec![CELLS[0]], "the proven cell is still listed");
    assert_eq!(m.refusals.len(), 5, "{:#?}", m.refusals);
    assert_eq!(
        m.refusals[0],
        Refusal::CellNotDiscovered { cell: named[1].into(), looked_for: vec!["pkg/plain: gone_cell".into(), "pkg/main: plain::gone_cell".into()] }
    );
    assert_eq!(m.refusals[1], Refusal::CellAmbiguous { cell: named[2].into(), tests: vec!["pkg/plain: twice_cell".into(), "pkg/main: plain::twice_cell".into()] });
    match &m.refusals[2] {
        Refusal::CellRunByNoJob { cell, test, reason } => {
            assert_eq!((cell.as_str(), test.as_str()), (named[3], "pkg/main: orphan::e_cell"));
            assert!(reason.contains("pkg:main") && reason.contains("--skip orphan::"), "the reason names the job and the skip: {reason}");
        }
        other => panic!("expected cell-run-by-no-job, got {other:?}"),
    }
    assert_eq!(
        m.refusals[3],
        Refusal::CellJobIgnored {
            cell: named[4].into(),
            log: FAST.into(),
            job: "stem_i".into(),
            summary: "test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 3 filtered out; finished in 0.10s".into(),
        }
    );
    assert_eq!(m.refusals[4], Refusal::CellNotDiscovered { cell: named[5].into(), looked_for: vec!["no package of the gate's build is at crates/other".into()] });

    record("export_manifest_refuses_a_section_14_cell_no_green_gate_job_ran_clean", json!({ "cells": m.cells, "refusals": m.refusals }));
}

/// CONTRACT (#3203, Luke 2026-10-27: "we can only learn more things"): a receipt a learning soak run
/// wrote (`learning: true`, `I143_SOAKS_LEARN=1`) certifies nothing: it is refused by name, however `ok`
/// it is; the same receipt without the mark (a receipt written before the field, or `learning: false`)
/// certifies.
#[test]
fn export_manifest_refuses_to_certify_a_learning_receipt() {
    let r = rafka();
    let parity = clean(&r);
    let estate = Estate::new();
    estate.all_ok(CANDIDATE);
    estate.receipt_learning("i143-2-process", CANDIDATE);
    let m = estate.certify(CANDIDATE, &parity);
    assert!(!m.eligible, "a learning receipt certifies nothing: {m:#?}");
    let named: Vec<String> = m.refusals.iter().map(|x| x.to_string()).collect();
    assert_eq!(named, vec!["i143-2-process: the receipt was written by a learning run (learning: true); it certifies nothing".to_string()], "refused by name, and nothing else");
    assert_eq!(m.refusals, vec![Refusal::ReceiptLearning { job: "i143-2-process".into() }]);
    estate.receipt("i143-2-process", CANDIDATE, "ok", 0);
    let ok = estate.certify(CANDIDATE, &parity);
    assert!(ok.eligible, "{ok:#?}");
}

/// CONTRACT (PRD §14, "an ignored cell proves nothing"): a run's ignored count says a §14 cell did not run
/// only when the cell itself is `#[ignore]`d in its source; libtest ignores no other test, so a cell with
/// no `#[ignore]` in a run that ignored other cells ran. A source that cannot be read proves nothing.
#[test]
fn a_cell_is_ignored_only_when_its_own_source_says_so() {
    let root = std::env::temp_dir().join(format!("export-ignored-{}", std::process::id()));
    std::fs::create_dir_all(root.join("crates/pkg/tests")).unwrap();
    std::fs::write(
        root.join("crates/pkg/tests/cells.rs"),
        "#[tokio::test]\n/// runs\nasync fn runs_cell() {}\n\n#[ignore = \"why\"]\n#[tokio::test]\nasync fn ignored_cell() {}\n",
    )
    .unwrap();
    let ignored = |cell: &str| rafka_mesh_audit::export::ignored_in_source(&root, cell);
    assert!(!ignored("crates/pkg/tests/cells.rs::runs_cell"), "a cell with no #[ignore] ran");
    assert!(ignored("crates/pkg/tests/cells.rs::ignored_cell"), "an #[ignore]d cell did not");
    assert!(ignored("crates/pkg/tests/cells.rs::absent_cell"), "a cell its source does not hold proves nothing");
    assert!(ignored("crates/pkg/tests/gone.rs::runs_cell"), "an unreadable source proves nothing");
    let _ = std::fs::remove_dir_all(&root);
}
