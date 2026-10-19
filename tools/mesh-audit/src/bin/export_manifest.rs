//! `export-manifest --candidate <40-hex RDM SHA> --repo <rafka-v2 checkout> [--ledger <md>] [--through <sha>]
//!  [--connections <md>] [--registry <json>] [--root <RDM root>] [--json <out>]`
//!
//! Certifies one candidate RDM SHA for export (e10.s2, #2790): runs the transport-parity scan, reads
//! every owner's job receipt (every job of the acceptance registry's `jobs` and `rshape_jobs`), and
//! prints the export manifest. Exits 0 only when the candidate is eligible; 1 names every refusal.

use rafka_mesh_audit::{export, parity};
use std::path::PathBuf;

fn main() {
    let mut candidate: Option<String> = None;
    let mut repo: Option<PathBuf> = None;
    let mut ledger: Option<PathBuf> = None;
    let mut through: Option<String> = None;
    let mut connections: Option<PathBuf> = None;
    let mut registry: Option<PathBuf> = None;
    let mut root: Option<PathBuf> = None;
    let mut json: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || args.next().unwrap_or_else(|| usage(&format!("{a} needs a value")));
        match a.as_str() {
            "--candidate" => candidate = Some(val()),
            "--repo" => repo = Some(val().into()),
            "--ledger" => ledger = Some(val().into()),
            "--through" => through = Some(val()),
            "--connections" => connections = Some(val().into()),
            "--registry" => registry = Some(val().into()),
            "--root" => root = Some(val().into()),
            "--json" => json = Some(val().into()),
            _ => usage(&format!("unknown argument {a}")),
        }
    }
    let candidate = candidate.unwrap_or_else(|| usage("--candidate is required"));
    let repo = repo.unwrap_or_else(|| usage("--repo is required"));
    let ledger = ledger.unwrap_or_else(|| parity::default_ledger(&repo));
    let report = parity::scan_files(&repo, &ledger, through.as_deref(), connections.as_deref()).unwrap_or_else(|e| {
        eprintln!("export-manifest: cannot read {}: {e}", ledger.display());
        std::process::exit(2);
    });
    let registry_path = registry.unwrap_or_else(export::default_registry);
    let registry: serde_json::Value = std::fs::read(&registry_path)
        .map_err(|e| e.to_string())
        .and_then(|b| serde_json::from_slice(&b).map_err(|e| e.to_string()))
        .unwrap_or_else(|e| {
            eprintln!("export-manifest: cannot read the registry {}: {e}", registry_path.display());
            std::process::exit(2);
        });
    let root = root.unwrap_or_else(rafka_mesh_audit::workspace_root);
    let manifest = export::certify(&export::CertifyInput { candidate: &candidate, parity: &report, registry: &registry, root: &root });
    let text = serde_json::to_string_pretty(&manifest).expect("the manifest serializes");
    if let Some(out) = json {
        std::fs::write(&out, &text).unwrap_or_else(|e| {
            eprintln!("export-manifest: cannot write {}: {e}", out.display());
            std::process::exit(2);
        });
    } else {
        println!("{text}");
    }
    for r in &manifest.refusals {
        eprintln!("export-manifest: REFUSED {r}");
    }
    eprintln!(
        "export-manifest: candidate {}, {} owners, {} receipts certified, {} refusals, eligible={}",
        manifest.candidate_sha,
        manifest.owners,
        manifest.receipts.len(),
        manifest.refusals.len(),
        manifest.eligible
    );
    std::process::exit(if manifest.eligible { 0 } else { 1 });
}

fn usage(msg: &str) -> ! {
    eprintln!(
        "export-manifest: {msg}\nusage: export-manifest --candidate <40-hex> --repo <rafka-v2> [--ledger <md>] [--through <sha>] [--connections <md>] [--registry <json>] [--root <rdm>] [--json <out>]"
    );
    std::process::exit(2)
}
