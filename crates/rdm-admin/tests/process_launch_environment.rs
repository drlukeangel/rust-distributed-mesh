//! A launched process runtime sees exactly the launch contract its spec names. The environment
//! of the admin that launches it carries that admin's own launch contract (an admin launched as a
//! restart holds `RDM_SUPERSEDES`), and none of it is the child's.

use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch};
use rdm_admin::model::DeploymentId;
use std::collections::BTreeMap;
use std::time::Duration;

/// CONTRACT: the launcher's own `RDM_*`, `TRACEPARENT` and `TRACESTATE` never reach a child
/// process. A variable the spec sets reaches it with the spec's value; a variable the spec does
/// not set is absent; what the process needs from the host (`PATH`) is kept.
#[tokio::test]
async fn a_launched_process_sees_the_launch_contract_its_spec_names_and_never_its_launchers() {
    // Its own process, the launcher, started with a launch contract of its own in its environment.
    let launcher_env = [
        ("RDM_SUPERSEDES", "launcher-predecessor"),
        ("RDM_LAUNCHER", "mesh1.admin.1,x,y"),
        ("RDM_MESH_ID", "launcher-mesh"),
        ("RDM_NOT_YET_INVENTED", "future-launch-variable"),
        ("TRACEPARENT", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        ("TRACESTATE", "launcher=1"),
    ]
    .map(|(k, v)| (k, Some(std::ffi::OsStr::new(v))));
    if crate::own_process::delegated_with_env(module_path!(), "a_launched_process_sees_the_launch_contract_its_spec_names_and_never_its_launchers", &launcher_env) {
        return;
    }
    for (k, v) in launcher_env {
        assert_eq!(std::env::var_os(k).as_deref(), v, "the launcher process holds {k} as the cell started it");
    }
    let dir = std::env::temp_dir().join(format!("rafka-launch-env-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let spec = ResolvedNodeLaunch { secrets: Default::default(), withhold: Vec::new(),
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
    let h = p.spawn(&spec, rdm_admin::deployment::deadline::Deadline::step()).await.unwrap();
    for _ in 0..200 {
        if !matches!(p.inspect(&h, rdm_admin::deployment::deadline::Deadline::step()).await, DeploymentStatus::Running) {
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

/// CONTRACT (Q150 amendment 1; cell 2 at the provider): a declared name is withheld from what a child
/// inherits, as `RDM_*` is: the launcher's own value of a declared name never reaches a child, whether
/// the spec sets that name or not; a declared value the spec sets reaches the child decoded, with the
/// value RDM holds; what the process needs from the host (`PATH`) is kept.
#[tokio::test]
async fn a_launched_process_never_inherits_a_declared_name_and_sees_the_declared_value_its_spec_sets() {
    let launcher_env = [("APP_DECLARED_KEY", "the-launchers-own-value"), ("APP_DECLARED_OTHER", "the-launchers-other-value")].map(|(k, v)| (k, Some(std::ffi::OsStr::new(v))));
    if crate::own_process::delegated_with_env(module_path!(), "a_launched_process_never_inherits_a_declared_name_and_sees_the_declared_value_its_spec_sets", &launcher_env) {
        return;
    }
    let dir = std::env::temp_dir().join(format!("rafka-launch-declared-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let spec = ResolvedNodeLaunch {
        secrets: BTreeMap::from([("APP_DECLARED_KEY".to_string(), rdm_core::launch::SecretHex::of_value("the value RDM holds"))]),
        withhold: vec!["APP_DECLARED_KEY".into(), "APP_DECLARED_OTHER".into()],
        node: "mesh1.broker.1".parse().unwrap(),
        deployment_id: DeploymentId::mint(),
        executable: "/bin/sh".into(),
        args: vec!["-c".into(), "env > \"$0\"".into(), dir.join("env.out").display().to_string()],
        env: BTreeMap::new(),
        data_dir: dir.clone(),
        transport: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
    };
    assert!(!format!("{spec:?}").contains("the value RDM holds"), "the spec's debug form names no declared value: {spec:?}");
    let p = ProcessDeploymentProvider::new();
    let h = p.spawn(&spec, rdm_admin::deployment::deadline::Deadline::step()).await.unwrap();
    for _ in 0..200 {
        if !matches!(p.inspect(&h, rdm_admin::deployment::deadline::Deadline::step()).await, DeploymentStatus::Running) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let seen = std::fs::read_to_string(dir.join("env.out")).expect("the child wrote its environment");
    assert!(seen.lines().any(|l| l == "APP_DECLARED_KEY=the value RDM holds"), "the declared value the spec sets, decoded:\n{seen}");
    assert!(!seen.contains("the-launchers-own-value") && !seen.lines().any(|l| l.starts_with("APP_DECLARED_OTHER=")), "the launcher's declared names are never inherited:\n{seen}");
    assert!(seen.lines().any(|l| l.starts_with("PATH=")), "the host's PATH is kept:\n{seen}");
    let _ = std::fs::remove_dir_all(&dir);
}
