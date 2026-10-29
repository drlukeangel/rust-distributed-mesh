//! `rdm-docgen --out <dir> [--root <rdm workspace>]`
//!
//! Writes spans.md and config.md into `--out`, read from the RDM workspace at `--root` (default: the
//! workspace this binary was built in). Exits 1 naming what it could not read.

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut root: Option<PathBuf> = None;
    let mut out: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let Some(value) = args.next() else { return usage(&format!("{a} needs a value")) };
        match a.as_str() {
            "--root" => root = Some(value.into()),
            "--out" => out = Some(value.into()),
            _ => return usage(&format!("unknown argument {a}")),
        }
    }
    let Some(out) = out else { return usage("--out is required") };
    let root = root.unwrap_or_else(rafka_mesh_audit::workspace_root);
    match rdm_docgen::run(&root, &out) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("rdm-docgen: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn usage(why: &str) -> ExitCode {
    eprintln!("rdm-docgen: {why}\nusage: rdm-docgen --out <dir> [--root <rdm workspace>]");
    ExitCode::from(2)
}
