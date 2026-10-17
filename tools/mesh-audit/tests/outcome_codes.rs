//! Runtime auth-code constructions are rejected by the actual landing checker.
#[test]
fn permission_denied_and_unauthenticated_are_never_constructed() {
    let root = rafka_mesh_audit::workspace_root();
    let output = std::process::Command::new("python3")
        .arg(root.join("scripts/test_name_check.py"))
        .output().expect("run the landing checker cells");
    assert!(output.status.success(), "{}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
}
