//! `rafka-docgen --out <dir> [--summary <docs dir>] [--root <rdm workspace>]`
//!
//! Writes wire.md, frames.md, statuses.md, spans.md and config.md into `--out`, read from the RDM
//! workspace at `--root` (default: the workspace this binary was built in). With `--summary`, also
//! writes `<docs dir>/SUMMARY.md`; `--out` must sit inside it. Exits 1 naming what it could not read.

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut root: Option<PathBuf> = None;
    let mut out: Option<PathBuf> = None;
    let mut summary: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let Some(value) = args.next() else { return usage(&format!("{a} needs a value")) };
        match a.as_str() {
            "--root" => root = Some(value.into()),
            "--out" => out = Some(value.into()),
            "--summary" => summary = Some(value.into()),
            _ => return usage(&format!("unknown argument {a}")),
        }
    }
    let Some(out) = out else { return usage("--out is required") };
    let root = root.unwrap_or_else(rafka_mesh_audit::workspace_root);
    match rafka_docgen::run(&root, &out, summary.as_deref()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("rafka-docgen: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn usage(why: &str) -> ExitCode {
    eprintln!("rafka-docgen: {why}\nusage: rafka-docgen --out <dir> [--summary <docs dir>] [--root <rdm workspace>]");
    ExitCode::from(2)
}
