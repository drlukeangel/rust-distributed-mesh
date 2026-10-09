//! `rshape-qualification --matrix <json> --evidence <dir> --owner-receipts <dir> --output <qualification.json>
//! [--root <dir>] [--candidate-sha <sha>] [--parity <json>] [--registry <json>] [--consumer-manifest <json>]`
//!
//! Writes `manifest.json`, `gate.log`, `invariant-results.json` and `qualification.json` beside
//! `--output`; exits 1 and names every violated rule when the evidence does not qualify.

use rafka_mesh_audit::rshape::{qualify, sha_file, Args};
use serde_json::json;
use std::path::PathBuf;

fn usage(msg: &str) -> ! {
    eprintln!("rshape-qualification: {msg}\nusage: rshape-qualification --matrix <json> --evidence <dir> --owner-receipts <dir> --output <json> [--root <dir>] [--candidate-sha <sha>] [--parity <json>] [--registry <json>] [--consumer-manifest <json>]");
    std::process::exit(2)
}

fn main() {
    let (mut matrix, mut evidence, mut owners, mut output, mut root) = (None, None, None, None, PathBuf::from("."));
    let (mut candidate, mut parity, mut registry, mut consumer) = (None, None, None, None);
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| usage(&format!("{a} needs a value")));
        match a.as_str() {
            "--matrix" => matrix = Some(PathBuf::from(val())),
            "--evidence" => evidence = Some(PathBuf::from(val())),
            "--owner-receipts" => owners = Some(PathBuf::from(val())),
            "--output" => output = Some(PathBuf::from(val())),
            "--root" => root = PathBuf::from(val()),
            "--candidate-sha" => candidate = Some(val()),
            "--parity" => parity = Some(PathBuf::from(val())),
            "--registry" => registry = Some(PathBuf::from(val())),
            "--consumer-manifest" => consumer = Some(PathBuf::from(val())),
            _ => usage(&format!("unknown argument {a}")),
        }
    }
    let args = Args {
        matrix: matrix.unwrap_or_else(|| usage("--matrix is required")),
        evidence: evidence.unwrap_or_else(|| usage("--evidence is required")),
        owner_receipts: owners.unwrap_or_else(|| usage("--owner-receipts is required")),
        output: output.unwrap_or_else(|| usage("--output is required")),
        root,
        candidate,
        parity,
        registry,
        consumer_manifest: consumer,
    };
    let out = qualify(&args);
    let dir = args.output.parent().map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| usage(&format!("cannot create {}: {e}", dir.display())));
    let write = |name: &str, text: String| std::fs::write(dir.join(name), text).unwrap_or_else(|e| usage(&format!("cannot write {name}: {e}")));
    let mut log = String::new();
    for c in &out.checks {
        log.push_str(&format!("{} {}\n", if c["holds"] == json!(true) { "HOLDS " } else { "BROKEN" }, c["invariant"].as_str().unwrap_or_default()));
    }
    for v in &out.violations {
        log.push_str(&format!("{v}\n"));
    }
    log.push_str(&format!("rshape-qualification: {} violation(s), eligible={}\n", out.violations.len(), out.violations.is_empty()));
    write("gate.log", log.clone());
    write("invariant-results.json", serde_json::to_string_pretty(&json!({"cell": "rshape_manifest_requires_all_canonical_owner_and_provider_receipts", "invariants": out.checks})).unwrap());
    write("qualification.json", serde_json::to_string_pretty(&out.report).unwrap());
    let hash = |p: &PathBuf| sha_file(p).ok();
    write(
        "manifest.json",
        serde_json::to_string_pretty(&json!({
            "tool": "rshape-qualification",
            "candidate_sha": out.report["candidate_sha"],
            "outcome": if out.violations.is_empty() { "ok" } else { "refused" },
            "inputs": {"matrix": {"path": args.matrix, "sha256": hash(&args.matrix)}},
            "outputs": ["gate.log", "invariant-results.json", "qualification.json"],
        }))
        .unwrap(),
    );
    eprint!("{log}");
    std::process::exit(if out.violations.is_empty() { 0 } else { 1 });
}
