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

#[cfg(test)]
mod tests {
    use std::process::Command;

    /// CONTRACT: tools/docgen/src/main.rs:1 "`rdm-docgen --out <dir> [--root <rdm workspace>]`"
    /// tools/docgen/src/main.rs:3 "Writes spans.md and config.md into `--out`, read from the RDM workspace at `--root` (default: the workspace this binary was built in). Exits 1 naming what it could not read."
    #[test]
    fn docgen_cli_writes_spans_and_config_pages_and_reports_usage_or_failure_codes() {
        let test_exe = std::env::current_exe().expect("current_exe");
        let bin = test_exe
            .parent()
            .and_then(|p| p.parent())
            .expect("target profile dir")
            .join("rdm-docgen");
        let root = rafka_mesh_audit::workspace_root();
        let expected = rdm_docgen::pages(&root).expect("pages(&root) renders");

        let missing_out = Command::new(&bin)
            .arg("--root")
            .arg(&root)
            .output()
            .expect("spawn rdm-docgen without --out");
        assert_eq!(missing_out.status.code(), Some(2), "missing --out must exit 2");
        assert!(
            String::from_utf8_lossy(&missing_out.stderr).contains("--out is required"),
            "stderr must report --out is required"
        );

        let unknown_arg = Command::new(&bin)
            .args(["--bogus", "val"])
            .output()
            .expect("spawn rdm-docgen with unknown argument");
        assert_eq!(unknown_arg.status.code(), Some(2), "unknown argument must exit 2");
        assert!(
            String::from_utf8_lossy(&unknown_arg.stderr).contains("unknown argument --bogus"),
            "stderr must name the unknown argument"
        );

        let missing_val = Command::new(&bin)
            .arg("--out")
            .output()
            .expect("spawn rdm-docgen with flag lacking value");
        assert_eq!(missing_val.status.code(), Some(2), "flag without value must exit 2");
        assert!(
            String::from_utf8_lossy(&missing_val.stderr).contains("--out needs a value"),
            "stderr must report missing flag value"
        );

        let file_as_out = std::env::temp_dir().join(format!("rdm-docgen-cli-file-{}", std::process::id()));
        std::fs::write(&file_as_out, "not a directory").unwrap();
        let failed = Command::new(&bin)
            .arg("--root")
            .arg(&root)
            .arg("--out")
            .arg(&file_as_out)
            .output()
            .expect("spawn rdm-docgen with file as --out");
        assert_eq!(failed.status.code(), Some(1), "unwritable --out must exit 1");
        assert!(
            String::from_utf8_lossy(&failed.stderr).contains("rdm-docgen: creating"),
            "stderr must name what it could not write"
        );
        let _ = std::fs::remove_file(&file_as_out);

        let out = std::env::temp_dir().join(format!("rdm-docgen-cli-ok-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&out);
        let ok = Command::new(&bin)
            .arg("--root")
            .arg(&root)
            .arg("--out")
            .arg(&out)
            .output()
            .expect("spawn rdm-docgen with --root and --out");
        assert!(
            ok.status.success(),
            "rdm-docgen failed: {}",
            String::from_utf8_lossy(&ok.stderr)
        );
        for p in &expected {
            let disk = std::fs::read_to_string(out.join(p.file)).expect("generated file exists");
            assert_eq!(disk, p.body, "CLI-written {} must match rendered body", p.file);
        }
        let _ = std::fs::remove_dir_all(&out);
    }
}
