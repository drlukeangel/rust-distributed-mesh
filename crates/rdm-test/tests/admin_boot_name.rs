//! A node-admin's router opens on its exact birth, never on the name it booted under (R-JN1; R12: a
//! node owns its name, and the join answer hands it the row's; lifecycles/node-create.md step 4: the
//! accepting authority admits a held birth under its row's name).
//!
//! CONTRACT: a node-admin process boots under `mesh1.node-admin.2`, the name its authority last accepted for
//! its exact birth, while its row stands at `mesh1.node-admin.3`. The authority admits it under the row's
//! name and its view holds the birth (its node id and incarnation) as `mesh1.node-admin.3`. Its Node RPC
//! router opens once its Status authority is filled and that view holds its exact birth: a Ping from
//! its mesh's primary is answered. What must NOT happen: the router gated on the boot name, which no
//! view lists, so every call to the admin is refused typed `NotReady` for the life of the process.
//!
//! The mesh's primary is a real Day-0 node-admin in this process; it creates `mesh1.node-admin.2`
//! through its `node.create` once its Day-0 Build is complete (started in this process). The admin is
//! stopped, then renamed `mesh1.node-admin.3` while stopped, through the product's node.stop and
//! node.name.update: its row stands at `.3`, and its mesh-primary last accepted `.2` for its birth. Its
//! start is the product's node.start, which registers the birth's join (held, under its row's name and
//! the name last accepted) before it commands the start.
//!
//! The fault, which the product never makes: the stopped process ends, and its old launch, under `.2`,
//! is started again by hand in its place, a stale start of the launch it was created with. Its own
//! process: the cell holds process-wide state (the stop flag).

use crate::in_process_provider::{self as in_process, InProcessProvider};
use rdm_core::{model::NodeStatus, MeshDigest, NodeId, PathName};
use rdm_admin::admin::{start_with, AdminConfig};
use rdm_admin::join::{JoinReport, Standing};
use rdm_admin::node_hooks::NodeHooks;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingReply, PingRequest};
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
const WRONG: Duration = Duration::from_secs(60);
/// What the admitted admin's router is given to open once its mesh-primary holds it under its row's
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
        while digest().status != NodeStatus::Ready {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(WRONG, waited).await.unwrap_or_else(|_| panic!("{what} never published Ready"));
}

/// A node-admin started from its launch's whole environment, with no hooks and no certs.
async fn admin_of(env: std::collections::BTreeMap<String, String>) -> Result<rdm_admin::admin::Running, String> {
    let cfg = AdminConfig::from_env(|k| env.get(k).cloned()).map_err(|e| format!("the launch is not a readable environment: {e}"))?;
    start_with(cfg, Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.map_err(|e| format!("{e:#}"))
}

#[test]
#[ignore = "https://github.com/drlukeangel/rust-distributed-mesh/issues/343"]
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
        // The mesh's primary: a Day-0 node-admin that creates mesh1.node-admin.2 through its node.create once
        // its Day-0 Build is complete, then reconciles nothing.
        let provider = InProcessProvider::new(std::sync::Arc::new(|_, env| Box::pin(async move { admin_of(env).await.map(in_process::Started::Admin) })));
        let root = start_with(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), provider: provider.wiring(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;

        // Created as `mesh1.node-admin.2`, the name its launch boots under; renamed `mesh1.node-admin.3`
        // while stopped, the name its mesh-primary admits it under.
        let (boot, row): (PathName, PathName) = (rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), rdm_core::PathName::new("mesh1.node-admin", "3").unwrap());
        provider.create(&root, vec![in_process::node_spec(&boot, "mesh1", "node-admin", &admin_dir)]).await;
        root.stop_reconciling().await;
        let (launch, env) = (provider.launch(&boot), provider.env(&boot));
        let created = provider.admin(&boot).await;
        until_ready("mesh1.node-admin.2", || created.digest.lock().unwrap().clone()).await;
        let stopped = in_process::stop(&root, &boot).await;
        assert_eq!(stopped.last(), Some(&rdm_client::Frame::Complete), "the created admin stops: {stopped:?}");
        let caller = in_process::Caller::of(&root).await;
        let root_id = root.digest.lock().unwrap().node.node_id.clone();
        let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(launch.node_id.clone()), node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone() };
        let renamed = caller.nodes.name_update(&birth, &root_id, &row.name).await.unwrap_or_else(|e| panic!("node.name.update of {boot:?} to {row:?}: {e}"));
        assert!(matches!(renamed, rdm_core::status::StatusReply::NameUpdated { .. }), "the stopped admin is renamed {row:?}: {renamed:?}");

        // The fault: the stopped process ends. The product's node.start of the stopped birth registers its
        // join and commands its start; once the join is registered (read, nothing registered by hand),
        // the old launch is started again by hand in place of the stopped process, under `.2`.
        let last = created.digest.lock().unwrap().clone();
        created.leave_after_stop().await;
        let starting = {
            let (nodes, row) = (caller.nodes.clone(), row.clone());
            tokio::spawn(async move {
                match nodes.start(&row).await {
                    Ok(stream) => format!("{:?}", in_process::frames("node.start", stream).await),
                    Err(e) => format!("refused: {e}"),
                }
            })
        };
        tokio::time::timeout(WRONG, async {
            while matches!(root.runner.joins.standing(&JoinReport::of(last.clone())), Standing::Unknown | Standing::Abandoned(_)) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("node.start registers the stopped birth's join");
        let digest_view = root.topology_view.digest_view.clone();
        let mut digests = digest_view.digest_changes();
        let admin = admin_of(env).await.expect("the old launch comes up");
        assert_eq!(admin.digest.lock().unwrap().node.location(), boot, "the old launch boots under the name it was created with");
        // The mesh-primary admitted the birth under the row's name: the admin publishes it.
        tokio::time::timeout(WRONG, async {
            while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.node.location() != row) {
                digests.changed().await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the mesh-primary never held {} under its row's name {row:?}", launch.node_id));

        // The router opens on the view holding the exact birth: the span says so, and a Ping from the
        // mesh's primary is then answered.
        let birth = launch.node_id.to_string();
        let opened = tokio::time::timeout(OPENS, spans.recorded("rdm.node_admin.node.serve.via-view-holds-birth", |f| f.get("node_id") == Some(&birth))).await;
        let ping = PingRequest::Ping { payload: b"the mesh primary reaches the admin it admitted".to_vec() };
        let opts = rdm_transport::CallOptions { send: rdm_transport::Send::with_time_limit(Duration::from_secs(5) + rdm_transport::TRANSPORT_LIMIT), ..Default::default() };
        let answer = match root.node_rpc.client.call::<Ping>(&rdm_transport::NodeTarget::ExactNode(launch.node_id.clone()), &ping, &opts).await.0 {
            RpcOutcome::Reply(r) => Ok(r.into_value()),
            other => Err(format!("{other:?}")),
        };
        assert!(matches!(answer, Ok(PingReply::Pong { .. })), "the admin admitted as {row:?} (booted as {boot:?}) did not answer its mesh primary's Ping {OPENS:?} after its mesh-primary held its birth under that name: {answer:?}");
        assert!(opened.is_ok(), "the router opened with no via-view-holds-birth span naming {birth}");
        let opened: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, f)| n == "rdm.node_admin.node.serve.via-view-holds-birth" && f.get("node_id") == Some(&birth)).map(|(_, t, f)| (t.clone(), f.clone())).collect();
        assert_eq!(opened.len(), 1, "the router opens once: {opened:?}");
        let (trace, f) = &opened[0];
        assert_eq!((f.get("held_path"), f.get("held_name"), f.get("boot_path"), f.get("boot_name")), (Some(&row.path), Some(&row.name), Some(&boot.path), Some(&boot.name)), "{f:?}");
        eprintln!("trace {trace:?}: rdm.node_admin.node.serve.via-view-holds-birth {f:?}");
        // How the product's node.start ended: its command went to the birth the old launch now runs.
        starting.abort();
        eprintln!("node.start of {row:?}: {:?}", starting.await);

        caller.close().await;
        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
