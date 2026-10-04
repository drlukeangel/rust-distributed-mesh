//! `parity-scan --repo <rafka-v2 checkout> [--ledger <path>] [--through <sha>] [--json <out>]`
//!
//! Runs the i143 transport-parity gate and prints the JSON report. Exits 1
//! when the gate fails (any unclassified boundary commit, duplicate or
//! ambiguous row, ancestry violation, or `MIRROR pending` row).

use rafka_mesh_audit::parity;
use std::path::PathBuf;

fn main() {
    let mut repo: Option<PathBuf> = None;
    let mut ledger: Option<PathBuf> = None;
    let mut through: Option<String> = None;
    let mut json: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || args.next().unwrap_or_else(|| usage(&format!("{a} needs a value")));
        match a.as_str() {
            "--repo" => repo = Some(val().into()),
            "--ledger" => ledger = Some(val().into()),
            "--through" => through = Some(val()),
            "--json" => json = Some(val().into()),
            _ => usage(&format!("unknown argument {a}")),
        }
    }
    let repo = repo.unwrap_or_else(|| usage("--repo is required"));
    let ledger = ledger.unwrap_or_else(|| parity::default_ledger(&repo));
    let report = match parity::scan_files(&repo, &ledger, through.as_deref()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("parity-scan: cannot read {}: {e}", ledger.display());
            std::process::exit(2);
        }
    };
    let text = serde_json::to_string_pretty(&report).expect("report serializes");
    if let Some(out) = json {
        std::fs::write(&out, &text).unwrap_or_else(|e| {
            eprintln!("parity-scan: cannot write {}: {e}", out.display());
            std::process::exit(2);
        });
    } else {
        println!("{text}");
    }
    for v in &report.violations {
        eprintln!("parity-scan: {v}");
    }
    eprintln!(
        "parity-scan: {} boundary commits, {} rows, {} MIRROR pending, {} violations, eligible={}",
        report.boundary_commits,
        report.rows.len(),
        report.mirror_pending,
        report.violations.len(),
        report.eligible
    );
    std::process::exit(if report.eligible { 0 } else { 1 });
}

fn usage(msg: &str) -> ! {
    eprintln!("parity-scan: {msg}\nusage: parity-scan --repo <rafka-v2> [--ledger <md>] [--through <sha>] [--json <out>]");
    std::process::exit(2)
}
