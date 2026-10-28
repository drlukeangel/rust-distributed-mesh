//! A node-admin's router opens on its exact birth, never on the name it booted under (R-JN1; R12: a
//! node owns its name, and the join answer hands it the row's; lifecycles/node-create.md step 4: the
//! accepting authority admits a held birth under its row's name).
//!
//! CONTRACT: a node-admin process boots under `mesh1.admin.2`, the name its authority last accepted for
//! its exact birth, while its row stands at `mesh1.admin.3`. The authority admits it under the row's
//! name and its view holds the birth (its node id and incarnation) as `mesh1.admin.3`. Its Node RPC
//! router opens once its Status authority is filled and that view holds its exact birth: a Ping from
//! its mesh's primary is answered. What must NOT happen: the router gated on the boot name, which no
//! view lists, so every call to the admin is refused typed `NotReady` for the life of the process.
//!
//! The mesh's primary is a real Day-0 node-admin in this process; it launches the second admin. Its own
//! process: the cell holds process-wide state (the stop flag).

use rdm_core::launch::{Launch, Launcher};
use rdm_core::runtime::RuntimeFact;
use rdm_core::{IncarnationId, MemberStatus, MeshDigest, NodeId, PathName};
use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::node_hooks::NodeHooks;
use rdm_admin::join::Deployed;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingReply, PingRequest};
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
const WRONG: Duration = Duration::from_secs(60);
/// What the admitted admin's router is given to open once the authority holds it under its row's
/// name: its view rebuild runs on the join's own facts.
const OPENS: Duration = Duration::from_secs(10);

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" => Some(dir.clone()),
        "RDM_BIN_DIR" => Some(dir.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

async fn until_ready(what: &str, digest: impl Fn() -> MeshDigest) {
    let waited = async {
        while digest().status != MemberStatus::Ready {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(WRONG, waited).await.unwrap_or_else(|_| panic!("{what} never published Ready"));
}

/// A launch of `name` by `root`, its data dir holding the runtime record its provider would write.
fn launch_by(root: &Running, name: &str, dir: &std::path::Path) -> Launch {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(dir).unwrap();
    Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: "fabric1".into(),
        fabric_id: d.fabric_id.clone(),
        name: name.parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![("control".into(), "127.0.0.1:0".parse().unwrap())],
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: dir.to_path_buf(),
        store_dir: dir.to_path_buf(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    }
}

#[test]
fn a_node_admin_booted_under_the_name_its_mesh_primary_last_accepted_serves_once_its_view_holds_its_exact_birth_under_the_rows_name() {
    if crate::own_process::delegated(module_path!(), "a_node_admin_booted_under_the_name_its_mesh_primary_last_accepted_serves_once_its_view_holds_its_exact_birth_under_the_rows_name") {
        return;
    }
    let cap = crate::common::capture("a_node_admin_booted_under_the_name_its_mesh_primary_last_accepted_serves_once_its_view_holds_its_exact_birth_under_the_rows_name");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("admin-boot-name-{}", NodeId::mint()));
        let (root_dir, admin_dir) = (base.join("admin1"), base.join("admin2"));
        std::fs::create_dir_all(&root_dir).unwrap();
        // The mesh's primary: a Day-0 node-admin that does not reconcile (it would retire the admin this
        // cell launches in its own process).
        let root = start_with(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling().await;
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;

        // The process boots under `mesh1.admin.2`, the name last accepted for its birth; its row stands
        // at `mesh1.admin.3`, the name the authority admits it under.
        let (boot, row): (PathName, PathName) = ("mesh1.admin.2".parse().unwrap(), "mesh1.admin.3".parse().unwrap());
        let launch = launch_by(&root, &boot.to_string(), &admin_dir);
        let key = rdm_test::node::load_or_mint_key(&launch.data_dir).unwrap();
        let _ = root.runner.joins.expect(Deployed {
            name: row.clone(),
            node_id: launch.node_id.clone(),
            incarnation: launch.incarnation.clone(),
            supersedes: None,
            endpoint_id: rdm_core::EndpointId(key.public().to_string()),
            runtime: rdm_core::runtime::await_own_record(&launch.data_dir, Duration::from_secs(10)).unwrap(),
            data_dir: launch.data_dir.display().to_string(),
            last_accepted_name: Some(boot.clone()),
            held: true,
            listeners: Vec::new(),
        });
        let digest_view = root.topology_view.digest_view.clone();
        let mut digests = digest_view.digest_changes();
        let env = launch.to_env();
        let admin_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
        let admin = start_with(admin_cfg, Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the launched admin comes up");
        // The authority admitted the birth under the row's name: the admin publishes it.
        tokio::time::timeout(WRONG, async {
            while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.node.name != row) {
                digests.changed().await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the authority never held {} under its row's name {row}", launch.node_id));

        // The router opens on the view holding the exact birth: the span says so, and a Ping from the
        // mesh's primary is then answered.
        let birth = launch.node_id.to_string();
        let opened = tokio::time::timeout(OPENS, spans.recorded("rdm.node_admin.node.serve.via-view-holds-birth", |f| f.get("node_id") == Some(&birth))).await;
        let ping = PingRequest::Ping { payload: b"the mesh primary reaches the admin it admitted".to_vec() };
        let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(5) }, ..Default::default() };
        let answer = match root.node_rpc.client.call::<Ping>(&rdm_transport::NodeTarget::ExactNode(launch.node_id.clone()), &ping, &opts).await.0 {
            RpcOutcome::Reply(r) => Ok(r.into_value()),
            other => Err(format!("{other:?}")),
        };
        assert!(matches!(answer, Ok(PingReply::Pong { .. })), "the admin admitted as {row} (booted as {boot}) did not answer its mesh primary's Ping {OPENS:?} after the authority held its birth under that name: {answer:?}");
        assert!(opened.is_ok(), "the router opened with no via-view-holds-birth span naming {birth}");
        let opened: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, f)| n == "rdm.node_admin.node.serve.via-view-holds-birth" && f.get("node_id") == Some(&birth)).map(|(_, t, f)| (t.clone(), f.clone())).collect();
        assert_eq!(opened.len(), 1, "the router opens once: {opened:?}");
        let (trace, f) = &opened[0];
        assert_eq!((f.get("held_name").map(String::as_str), f.get("boot_name").map(String::as_str)), (Some(row.to_string().as_str()), Some(boot.to_string().as_str())), "{f:?}");
        eprintln!("trace {trace:?}: rdm.node_admin.node.serve.via-view-holds-birth {f:?}");

        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
