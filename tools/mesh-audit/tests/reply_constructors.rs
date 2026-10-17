//! The shared-outcome rule is enforced across the runtime source, not a handler census.
use rafka_mesh_audit::replies;
use std::path::Path;

#[test]
fn every_runtime_shared_reply_is_constructed_through_the_library() {
    let violations=replies::check(&rafka_mesh_audit::workspace_root());
    assert!(violations.is_empty(), "shared reply variants constructed outside reply:::\n{}", violations.iter().map(|v| format!("{}:{}: {}", v.file.display(),v.line,v.constructor)).collect::<Vec<_>>().join("\n"));
}

#[test]
fn the_rule_refuses_a_handler_in_every_runtime_family_but_accepts_a_read_pattern() {
    for family in ["rdm-admin","rdm-node","rdm-test","rdm-transport"] {
        let p=format!("crates/{family}/src/handler.rs");
        for source in ["fn handle() -> StatusReply { StatusReply::Applied }", "fn handle() -> StatusReply { StatusReply::RequestNotHandled { request: String::new() } }"] {
            assert_eq!(replies::scan(Path::new(&p),source).len(),1,"{p}");
        }
        assert!(replies::scan(Path::new(&p),"fn inspect(r: StatusReply) { match r { StatusReply::Applied => (), _ => () } }").is_empty());
    }
}
