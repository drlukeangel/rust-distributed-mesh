//! `address-lookup [--root <workspace>]`: the i143.e4.s13 address-lookup
//! boundary. Exits 1 and names every violation when a rule fails.

fn main() {
    let mut args = std::env::args().skip(1);
    let root = match (args.next().as_deref(), args.next()) {
        (Some("--root"), Some(r)) => std::path::PathBuf::from(r),
        (None, _) => rafka_mesh_audit::workspace_root(),
        _ => {
            eprintln!("usage: address-lookup [--root <workspace>]");
            std::process::exit(2)
        }
    };
    let v = rafka_mesh_audit::address_lookup::check(&root);
    for x in &v {
        eprintln!("address-lookup: {x}");
    }
    eprintln!("address-lookup: {} violation(s) in {}", v.len(), root.display());
    std::process::exit(if v.is_empty() { 0 } else { 1 });
}
