//! A launched process runtime sees exactly the launch contract its spec names. The environment
//! of the admin that launches it carries that admin's own launch contract (an admin launched as a
//! restart holds `RDM_SUPERSEDES`), and none of it is the child's.

use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch};
use rafka_node_admin_core::model::DeploymentId;
use std::collections::BTreeMap;
use std::time::Duration;

/// CONTRACT: the launcher's own `RDM_*`, `TRACEPARENT` and `TRACESTATE` never reach a child
/// process. A variable the spec sets reaches it with the spec's value; a variable the spec does
/// not set is absent; what the process needs from the host (`PATH`) is kept.
#[tokio::test]
async fn a_launched_process_sees_the_launch_contract_its_spec_names_and_never_its_launchers() {
    for (k, v) in [
        ("RDM_SUPERSEDES", "launcher-predecessor"),
        ("RDM_LAUNCHER", "mesh1.admin.1,x,y"),
        ("RDM_MESH_ID", "launcher-mesh"),
        ("RDM_NOT_YET_INVENTED", "future-launch-variable"),
        ("TRACEPARENT", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        ("TRACESTATE", "launcher=1"),
    ] {
        std::env::set_var(k, v);
    }
    let dir = std::env::temp_dir().join(format!("rafka-launch-env-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let spec = ResolvedNodeLaunch {
        node: "mesh2.admin.1".parse().unwrap(),
        deployment_id: DeploymentId::mint(),
        executable: "/bin/sh".into(),
        args: vec!["-c".into(), "env > \"$0\"".into(), dir.join("env.out").display().to_string()],
        env: BTreeMap::from([("RDM_MESH_ID".to_string(), "spec-mesh".to_string())]),
        data_dir: dir.clone(),
        transport: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
    };
    let p = ProcessDeploymentProvider::new();
    let h = p.spawn(&spec).await.unwrap();
    for _ in 0..200 {
        if !matches!(p.inspect(&h).await, DeploymentStatus::Running) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let seen = std::fs::read_to_string(dir.join("env.out")).expect("the child wrote its environment");
    let has = |k: &str| seen.lines().any(|l| l.starts_with(&format!("{k}=")));
    assert!(seen.lines().any(|l| l == "RDM_MESH_ID=spec-mesh"), "the spec's value wins:\n{seen}");
    for k in ["RDM_SUPERSEDES", "RDM_LAUNCHER", "RDM_NOT_YET_INVENTED", "TRACEPARENT", "TRACESTATE"] {
        assert!(!has(k), "{k} is the launcher's, not the spec's:\n{seen}");
    }
    assert!(has("PATH"), "the host's PATH is kept:\n{seen}");
    let _ = std::fs::remove_dir_all(&dir);
}
