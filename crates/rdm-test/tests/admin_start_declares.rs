//! A node-admin reports its node.started at every start (node-status.md "Status report": "A node
//! reports to its mesh's primary, which sets its status"; states.md "Who sets each state": node
//! Ready is set by "the node's mesh-primary" from "the node's node.started report, after every
//! start gate").
//!
//! CONTRACT (Q47, Q93; node-start.md step 5): the first start owes a Ready declaration. A subsequent
//! commanded start reports NodeJoined and NodeStarted on 0x22; its primary records Ready before
//! returning the completion. "The node also declares Ready to its mesh-primary as an owed message, as
//! it declares Stopped (Q36, Q87); frame and declaration apply once (Q49)." Q93 supersedes Q47's line
//! "The open start sends no separate readiness declaration."
//!
//! The mesh's primary is a real Day-0 node-admin in this process; it creates the second admin through
//! its `node.create` once its Day-0 Build is complete (started in this process); the admin's stop and
//! start are the product's node.stop and node.start, sent as any caller sends them. Its own process:
//! the cell holds process-wide state (the stop flag).

use crate::in_process_provider::{self as in_process, InProcessProvider};
use rdm_core::{model::NodeStatus, MeshDigest, NodeId, PathName};
use rdm_admin::admin::{start_with, AdminConfig};
use rdm_admin::node_hooks::NodeHooks;
use rdm_admin::wiring::{FabricHooks, Wiring};
use std::collections::BTreeMap;
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
const WRONG: Duration = Duration::from_secs(60);
/// What a declaration owed at a report is given: the declarer's checklist runs on the report's wake.
const DECLARED: Duration = Duration::from_secs(10);

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

/// The node events of a workflow's frames, in order.
fn events(frames: &[rdm_client::Frame]) -> Vec<rdm_client::NodeEvent> {
    frames.iter().filter_map(|f| if let rdm_client::Frame::Event(e) = f { Some(*e) } else { None }).collect()
}

/// The finished own-state declarations `node` sent: each one's target and outcome.
fn own_declarations(spans: &crate::common::Spans, node: &PathName) -> Vec<BTreeMap<String, String>> {
    spans.0.lock().unwrap().values().filter(|(n, _, f)| n == "rdm.node_admin.status.update.via-declare" && f.get("node_path") == Some(&node.path) && f.get("node_name") == Some(&node.name) && f.get("key").is_some_and(|k| k.contains("OwnState")) && f.get("finished").map(String::as_str) == Some("true")).map(|(_, _, f)| f.clone()).collect()
}

#[test]
fn a_node_admins_start_after_its_stop_reports_its_node_started_to_its_mesh_primary_again() {
    if crate::own_process::delegated(module_path!(), "a_node_admins_start_after_its_stop_reports_its_node_started_to_its_mesh_primary_again") {
        return;
    }
    let cap = crate::common::capture("a_node_admins_start_after_its_stop_reports_its_node_started_to_its_mesh_primary_again");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("admin-start-declares-{}", NodeId::mint()));
        let (root_dir, admin_dir) = (base.join("admin1"), base.join("admin2"));
        std::fs::create_dir_all(&root_dir).unwrap();
        // The mesh's primary: a Day-0 node-admin that creates mesh1.node-admin.2 through its node.create once
        // its Day-0 Build is complete, then reconciles nothing.
        let provider = InProcessProvider::new(crate::common::passing_start());
        let root = start_with(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), provider: provider.wiring(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;
        tokio::time::timeout(WRONG, async {
            while root.topology_view.primaries().mesh("mesh1").is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the Day-0 admin takes mesh1's primary role");
        let root_id = root.digest.lock().unwrap().node.node_id.to_string();

        let at = PathName::new("mesh1.node-admin", "2").unwrap();
        provider.create(&root, vec![in_process::node_spec(&at, "mesh1", "node-admin", &admin_dir)]).await;
        root.stop_reconciling().await;
        let launch = provider.launch(&at);
        let admin = provider.admin(&at).await;
        until_ready("mesh1.node-admin.2 (first start)", || admin.digest.lock().unwrap().clone()).await;
        let first = tokio::time::timeout(DECLARED, async {
            loop {
                let held = own_declarations(&spans, &launch.location());
                if !held.is_empty() {
                    return held;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("mesh1.node-admin.2 declares its first start's node.started");
        assert_eq!(first.iter().map(|f| f["to_node_id"].clone()).collect::<Vec<_>>(), vec![root_id.clone()], "to its mesh's primary: {first:#?}");

        // Its stop and its start, through the product's node.stop and node.start (NodeCommand 0x22 to its
        // mesh-primary, the root, whose node.start registers the birth's join before it commands the start).
        let stopped = in_process::stop(&root, &at).await;
        assert_eq!((events(&stopped), stopped.last()), (vec![rdm_client::NodeEvent::ConnectionsDeleted, rdm_client::NodeEvent::Stopped], Some(&rdm_client::Frame::Complete)), "the created admin stops: {stopped:?}");
        let started = in_process::start(&root, &at).await;
        assert_eq!((events(&started), started.last()), (vec![rdm_client::NodeEvent::Joined, rdm_client::NodeEvent::Started], Some(&rdm_client::Frame::Complete)), "the created admin starts after its stop: {started:?}");
        until_ready("mesh1.node-admin.2 (start after stop)", || admin.digest.lock().unwrap().clone()).await;
        tokio::time::timeout(DECLARED, spans.recorded("rdm.node_admin.node.update.via-command-frame", |f| f.get("node_id").map(String::as_str) == Some(launch.node_id.as_str()) && f.get("frame").map(String::as_str) == Some("node-started"))).await.expect("the mesh-primary records NodeStarted on the open command stream");
        let status_service = root.status_service().unwrap();
        assert_eq!(status_service.declared.lock().unwrap().node(&launch.node_id, &launch.incarnation), Some(rdm_core::status::NodeState::Ready));
        let started = tokio::time::timeout(DECLARED, async {
            loop {
                let held: Vec<_> = own_declarations(&spans, &launch.location()).into_iter().filter(|f| f.get("key").is_some_and(|v| v.contains("Ready")) && f.get("operation").map(String::as_str) == Some("node.start")).collect();
                if !held.is_empty() {
                    return held;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the start declares its Ready naming the start, beside its NodeStarted frame");
        assert_eq!(started.len(), 1, "the start's Ready is declared once: {started:#?}");
        assert_eq!(started[0]["to_node_id"], root_id, "to its mesh's primary: {started:#?}");
        assert!(matches!(started[0]["outcome"].as_str(), "applied" | "already-applied"), "frame and declaration apply once: the declaration is applied or answered already applied: {started:#?}");
        let first_again: Vec<_> = own_declarations(&spans, &launch.location()).into_iter().filter(|f| f.get("key").is_some_and(|v| v.contains("Ready")) && f.get("operation").is_none()).collect();
        assert_eq!(first_again, first, "the first start's declaration stands beside it: {first_again:#?}");

        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
