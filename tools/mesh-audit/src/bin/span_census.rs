//! Emit runtime span sites as JSON for an auditable ownership census.
fn main() -> anyhow::Result<()> {
    let root = std::env::args_os().nth(1).map(std::path::PathBuf::from)
        .unwrap_or_else(rafka_mesh_audit::workspace_root);
    println!("{}", serde_json::to_string_pretty(&rafka_mesh_audit::telemetry::emitters(&root)?)?);
    Ok(())
}
