//! `storage-seam [--root <workspace>]`: the i143 1b storage seam. Exits 1 and names every product
//! source that reaches the file store outside the file backend's own modules and outside cfg(test).

fn main() {
    let mut args = std::env::args().skip(1);
    let root = match (args.next().as_deref(), args.next()) {
        (Some("--root"), Some(r)) => std::path::PathBuf::from(r),
        (None, _) => rafka_mesh_audit::workspace_root(),
        _ => {
            eprintln!("usage: storage-seam [--root <workspace>]");
            std::process::exit(2)
        }
    };
    let v = rafka_mesh_audit::storage_seam::check(&root);
    for x in &v {
        eprintln!("storage-seam: {x}");
    }
    eprintln!("storage-seam: {} violation(s) in {}", v.len(), root.display());
    std::process::exit(if v.is_empty() { 0 } else { 1 });
}
