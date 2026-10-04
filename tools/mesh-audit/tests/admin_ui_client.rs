//! i143.e1.s6: no lifecycle mutation code remains in admin-ui.

use rafka_mesh_audit::admin_ui::lifecycle_violations;
use rafka_mesh_audit::workspace_root;

#[test]
fn admin_ui_holds_no_lifecycle_authority() {
    let v = lifecycle_violations(&workspace_root());
    assert!(v.is_empty(), "admin-ui must be a client of node-admin core:\n{}", v.join("\n"));
}

#[test]
fn the_ratchet_names_every_kind_of_violation() {
    let dir = std::env::temp_dir().join(format!("admin-ui-ratchet-{}", std::process::id()));
    let src = dir.join("admin-ui/src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(dir.join("admin-ui/Cargo.toml"), "[dependencies]\nrafka-node-admin-core = { path = \"x\" }\n\n[dev-dependencies]\nrafka-node-admin-client = { path = \"y\" }\n").unwrap();
    std::fs::write(
        src.join("main.rs"),
        "use rafka_node_admin_core::process_table::ProcessTable;\nfn f(c: &mut std::process::Child) { let _ = c.kill(); let _ = std::process::Command::new(\"rafka-rpc-node\"); }\n// Command::new(\"in a comment\") is fine\nfn g() { tokio::process::Command::new(&rfa_bin); }\n",
    )
    .unwrap();
    let v = lifecycle_violations(&dir);
    let has = |s: &str| v.iter().any(|x| x.contains(s));
    assert!(has("depends on rafka-node-admin-core"), "{v:#?}");
    assert!(has("does not depend on rafka-node-admin-client"), "{v:#?}");
    assert!(has("`ProcessTable`") && has("`rafka_node_admin_core`") && has("`.kill(`") && has("`process::Child`"), "{v:#?}");
    assert!(has("main.rs:2: starts a process"), "{v:#?}");
    assert!(!v.iter().any(|x| x.contains("main.rs:3") || x.contains("main.rs:4")), "comments and the rfa runner pass: {v:#?}");
    let _ = std::fs::remove_dir_all(&dir);
}
