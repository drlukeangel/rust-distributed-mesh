//! product=mesh, feature=deployment, subfeature=named-listeners, rung=MM, provider=process (and
//! container, opt-in).
//!
//! A product kind's listeners (Q151; node-create.md "Bound endpoint and join admission"; the
//! deployment-provider page's assigned endpoints): the application's wiring names each kind's
//! listeners. A create assigns each the birth's address with port 0 (`RDM_LISTENERS`); the node binds
//! them, keeps every bind open through its join and reports the ports the operating system selected;
//! the accepting primary verifies the report against the assignment and the node's row publishes them.
//!
//! Every estate runs the testkit's faulted node-admin, whose application declares `kafka`, `rest` and
//! `proxy` on the gateway (`rdm_test::test_app::listeners`).

use rdm_client::{FabricDesired, MeshDesired};
use rdm_core::views::NodeView;
use rdm_test::estate::{Estate, Owner};
use rdm_test::faults::{binding_set_with_roles, candidate_sha};
use rdm_test::test_app::{LISTENERS, LISTENER_KIND};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(180);

fn owner(test: &str, provider: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "deployment".into(), subfeature: "named-listeners".into(), rung: "MM".into(), provider: provider.into(), test: test.into() }
}

/// A fabric of the faulted node-admin with the role kinds bound, on `provider`.
async fn estate_on(test: &str, provider: &str) -> Estate {
    let sha = candidate_sha();
    let mut set = binding_set_with_roles(&sha);
    if provider == "container" {
        for b in set.bindings.iter_mut() {
            b.image = Some(rdm_test::estate::RUNTIME_IMAGE.into());
        }
    }
    Estate::bootstrap_on_with_env(owner(test, provider), "fabric1", "mesh1", &set, &sha, &["broker", "gateway", "compute"], &[])
        .await
        .expect("the faulted-admin binding set with the role kinds is accepted")
}

/// mesh1 with `gateways` gateways beside its two node-admins.
fn mesh_with_gateways(gateways: u32) -> FabricDesired {
    FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired { name: "mesh1".into(), node_admin: 2, rpc_node: 0, broker: 0, gateway: gateways, compute: 0 }] }
}

/// The record of `node` on the fabric's store root, as its mesh-primary wrote it.
fn row_of(estate: &Estate, node: &NodeView) -> Value {
    estate.node_record(node.node_id.as_str()).unwrap_or_else(|| panic!("{}: its row is not in nodes.jsonl", node.name))
}

/// The listeners the row's facts of `node`'s current birth carry.
fn row_listeners(estate: &Estate, node: &NodeView) -> Vec<(String, SocketAddr)> {
    let row = row_of(estate, node);
    let incarnation = node.incarnation_id.as_ref().map(|i| i.as_str().to_string()).unwrap_or_default();
    let birth = row["record"]["births"].as_array().and_then(|b| b.iter().find(|b| b["incarnation_id"] == incarnation.as_str())).cloned().unwrap_or_else(|| panic!("{}: its row holds no facts of birth {incarnation}: {row}", node.name));
    serde_json::from_value(birth["listeners"].clone()).unwrap_or_default()
}

/// The listeners the join that admitted `node` reported, as the accepting primary's span records them,
/// once that admin's evidence file holds the span.
async fn join_reported(estate: &Estate, node: &NodeView) -> Option<String> {
    let until = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let found = estate
            .spans()
            .into_iter()
            .find(|s| s["name"] == "rdm.node_admin.node.update.via-join" && s["attributes"]["node_id"] == node.node_id.to_string().as_str() && s["attributes"]["outcome"] == "installed")
            .map(|s| s["attributes"]["listeners"].as_str().unwrap_or_default().to_string());
        if found.is_some() || std::time::Instant::now() > until {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// `node`'s listeners as the view, its row and its admitted join carry them: exactly the declared
/// names, each on the birth's own address (its transport's) with a port the operating system selected.
async fn published(estate: &Estate, node: &NodeView) -> Vec<(String, SocketAddr)> {
    let ip = node.transport_addr.unwrap_or_else(|| panic!("{}: the view names no address", node.name)).ip();
    let names: BTreeSet<&str> = node.listeners.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, LISTENERS.iter().copied().collect::<BTreeSet<_>>(), "{}: the view carries exactly the declared listeners: {:?}", node.name, node.listeners);
    assert_eq!(node.listeners.len(), LISTENERS.len(), "{}: each once: {:?}", node.name, node.listeners);
    for (n, a) in &node.listeners {
        assert!(a.ip() == ip && a.port() != 0, "{}: {n} at {a} is on the birth's address {ip}, with the port the operating system selected", node.name);
    }
    let mut row = row_listeners(estate, node);
    let mut view = node.listeners.clone();
    row.sort();
    view.sort();
    assert_eq!(row, view, "{}: its row on the store root publishes the listeners the view carries", node.name);
    let reported = join_reported(estate, node).await.unwrap_or_else(|| panic!("{}: no admitted join of it in the spans", node.name));
    for (n, a) in &node.listeners {
        assert!(reported.split(',').any(|e| e == format!("{n}={a}")), "{}: its join reported {n}={a}: {reported}", node.name);
    }
    node.listeners.clone()
}

/// Cell 1 (Q151): a gateway's three listeners are bound by its process, reported at its join and
/// published on its row and in the node view.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateways_listeners_are_bound_reported_at_join_and_published_on_its_row() {
    let test = "a_gateways_listeners_are_bound_reported_at_join_and_published_on_its_row";
    let mut estate = estate_on(test, "process").await;
    estate.reconcile(&mesh_with_gateways(1)).await;
    let want: BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.gateway.1"].iter().map(|n| n.to_string()).collect();
    let views = estate.settles_to_named(&want, SETTLE).await;
    let gateway = views.iter().find(|n| n.kind == LISTENER_KIND).expect("the gateway is in the view");
    estate.artifact("views.json", &json!(Estate::values(&views)));
    estate.artifact("row.json", &row_of(&estate, gateway));
    let listeners = published(&estate, gateway).await;
    // Bound, and held: each port is a listening socket of the gateway's own process.
    let pid = estate.pid_of(&gateway.name.to_string()).await;
    for (n, a) in &listeners {
        let who = rdm_admin::deployment::endpoint::port_holder(*a, rdm_admin::deployment::endpoint::SlotTransport::Tcp).unwrap();
        assert!(who.contains("listen") && who.contains(&format!("pid {pid} ")), "{n} at {a} is held listening by the gateway's process (pid {pid}): {who}");
    }
    for v in views.iter().filter(|n| n.kind != LISTENER_KIND) {
        assert!(v.listeners.is_empty(), "{}: a kind that declares none binds none: {:?}", v.name, v.listeners);
    }
    estate.stop().await;
}

/// Cell 2 (Q151): two gateways launched at once bind with no conflict: each its three listeners, six
/// distinct ports, both ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_gateways_launched_at_once_bind_their_listeners_with_no_conflict() {
    let test = "two_gateways_launched_at_once_bind_their_listeners_with_no_conflict";
    let mut estate = estate_on(test, "process").await;
    estate.reconcile(&mesh_with_gateways(2)).await;
    let want: BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.gateway.1", "mesh1.gateway.2"].iter().map(|n| n.to_string()).collect();
    let views = estate.settles_to_named(&want, SETTLE).await;
    estate.artifact("views.json", &json!(Estate::values(&views)));
    let mut ports: BTreeMap<SocketAddr, String> = BTreeMap::new();
    let gateways: Vec<&NodeView> = views.iter().filter(|n| n.kind == LISTENER_KIND).collect();
    assert_eq!(gateways.len(), 2, "both gateways are ready");
    for g in gateways {
        for (n, a) in published(&estate, g).await {
            if let Some(other) = ports.insert(a, format!("{}:{n}", g.name)) {
                panic!("{}:{n} and {other} report one address {a}", g.name);
            }
        }
    }
    assert_eq!(ports.len(), 2 * LISTENERS.len(), "six distinct listener addresses: {ports:?}");
    estate.stop().await;
}

/// Cell 4 (Q151; container provider): a container fabric's gateway binds its listeners on the
/// container's own address, the one its transport binds, never loopback. Opt-in: the container-proof
/// step (`RDM_CONTAINER_PROOF=1`, or `RDM_REQUIRE_CONTAINER=1`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_container_fabrics_gateway_listeners_are_on_the_containers_own_address() {
    if std::env::var("RDM_CONTAINER_PROOF").as_deref() != Ok("1") && std::env::var("RDM_REQUIRE_CONTAINER").as_deref() != Ok("1") {
        eprintln!("SKIP container named listeners: opt-in with RDM_CONTAINER_PROOF=1 (the container-proof step)");
        return;
    }
    let test = "a_container_fabrics_gateway_listeners_are_on_the_containers_own_address";
    let mut estate = estate_on(test, "container").await;
    estate.reconcile(&mesh_with_gateways(1)).await;
    let want: BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.gateway.1"].iter().map(|n| n.to_string()).collect();
    let views = estate.settles_to_named(&want, SETTLE).await;
    let gateway = views.iter().find(|n| n.kind == LISTENER_KIND).expect("the gateway is in the view");
    estate.artifact("views.json", &json!(Estate::values(&views)));
    estate.artifact("row.json", &row_of(&estate, gateway));
    let listeners = published(&estate, gateway).await;
    let ip = gateway.transport_addr.expect("the gateway's address").ip();
    assert!(!ip.is_loopback() && !ip.is_unspecified(), "the gateway's address is its container's own on the fabric network: {ip}");
    assert!(listeners.iter().all(|(_, a)| a.ip() == ip), "every listener is on the container's address {ip}: {listeners:?}");
    estate.stop().await;
}
