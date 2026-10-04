//! `dep-rules [--root <workspace>]`: the i143 PRD §4 dependency rules.
//! Exits 1 and names every violation when a rule fails.

fn main() {
    let mut args = std::env::args().skip(1);
    let root = match (args.next().as_deref(), args.next()) {
        (Some("--root"), Some(r)) => std::path::PathBuf::from(r),
        (None, _) => rafka_mesh_audit::workspace_root(),
        _ => {
            eprintln!("usage: dep-rules [--root <workspace>]");
            std::process::exit(2)
        }
    };
    let v = rafka_mesh_audit::deps::check(&root);
    for x in &v {
        eprintln!("dep-rules: {x}");
    }
    eprintln!("dep-rules: {} violation(s) in {}", v.len(), root.display());
    std::process::exit(if v.is_empty() { 0 } else { 1 });
}
