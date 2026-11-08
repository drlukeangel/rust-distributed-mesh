//! Process E2E: a node type's first primary in a mesh (Q407; node-create.md step 6;
//! lifecycles/node-primary-update.md; fabric-node-lifecycle-elections.md §4.1).
//!
//! One fabric, one mesh. A Build creates one gateway; a second Build creates the mesh's first broker.
//! From public surfaces and spans:
//! - the mesh-primary's create of the broker commands it its type's first primary once it is Ready: a
//!   `rdm.node_admin.primary.update.via-create` span (`outcome = updated`, revision 1) at the
//!   mesh-primary, the broker's own install of it (`rdm.node_admin.primary.update.via-primary-updated`,
//!   `outcome = held`), and the role on the broker's own record (`is_primary` 1);
//! - the gateway, which joined before the broker, holds the broker primary from the broker's Digest:
//!   `node.topology.get` at the gateway lists the broker with its broker-primary role at revision 1;
//! - no node-admin computes the broker election: no `rdm.mesh.election.resolve.via-recompute` span names
//!   the broker type.

use rdm_client::{FabricDesired, MeshDesired, NodeStatus};
use rdm_core::{PrimaryRole, RoleRevision};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use serde_json::json;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "first-primary".into(),
        rung: "SN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

/// CONTRACT (Q407, Luke "a with a gossip": "the mesh-primary's create step commands the first node of a
/// type in its mesh by `node.primary.update` once that node is Ready. The node installs the role, records
/// `is_primary` on its own record, and gossips it in its Digest's `primary`"): a mesh's first broker is
/// commanded primary at its create, records it on its own record, and a gateway that joined before it
/// holds the broker primary from the broker's Digest. No node-admin computes a broker election (Q406).
/// Must NOT happen: a broker that never learns it is primary (the #338 step 3a red: the demo's
/// admin_app_protocol, fabric_hooks, one_trace and replica cells), or a gateway holding no broker primary.
// @feature: mesh-elections
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_meshs_first_broker_is_commanded_primary_at_its_create_and_a_gateway_holds_it_from_its_digest() {
    let test = "a_meshs_first_broker_is_commanded_primary_at_its_create_and_a_gateway_holds_it_from_its_digest";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 1), ("gateway", 1)])] }).await;
    estate.resize(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 1), ("gateway", 1), ("broker", 1)])] }).await;
    let (broker, gateway) = estate
        .until_view("the broker and the gateway are ready", Duration::from_secs(30), |view| {
            let broker = view.iter().find(|n| n.r#type == "broker" && n.state == NodeStatus::Ready)?;
            let gateway = view.iter().find(|n| n.r#type == "gateway" && n.state == NodeStatus::Ready)?;
            Some((broker.clone(), gateway.clone()))
        })
        .await;
    let broker_id = broker.node_id.to_string();
    let broker_role = RoleRevision { role: PrimaryRole::NodePrimary("broker".into()), revision: 1 };

    // The broker records the role on its own record (Q356): `is_primary` at revision 1.
    wait_for(&format!("{broker_id}'s own record holds is_primary 1"), Duration::from_secs(10), || async { (estate.record(&broker.node_id).and_then(|r| r.is_primary) == Some(1)).then_some(()) }).await;
    // The gateway, which joined before the broker, holds the broker primary from the broker's Digest.
    let held = wait_for(&format!("{:?} holds {broker_id} as the broker primary", gateway.location()), Duration::from_secs(10), || async {
        let held = estate.held_topology_at(&gateway.location()).await?;
        held.members.iter().find(|m| m.node.node_id == broker.node_id).filter(|m| m.primary.contains(&broker_role)).map(|m| m.primary.clone())
    })
    .await;
    estate.artifact("gateway-holds.json", &json!({"broker": broker_id, "gateway": gateway.node_id.to_string(), "broker_roles_at_gateway": format!("{held:?}")}));
    estate.stop().await;

    let spans = estate.spans();
    // The mesh-primary's create commanded it, revision 1, answered.
    let commanded = named(&spans, "rdm.node_admin.primary.update.via-create")
        .into_iter()
        .find(|sp| sp["attributes"]["subject_id"] == broker_id.as_str() && sp["attributes"]["type"] == "broker")
        .unwrap_or_else(|| panic!("the mesh-primary's create of {broker_id} commanded its first primary"));
    assert_eq!(commanded["attributes"]["outcome"], "updated", "{commanded:#}");
    assert_eq!(commanded["attributes"]["revision"].to_string().trim_matches('"'), "1", "{commanded:#}");
    // The broker installed it itself.
    let installed = named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().find(|sp| sp["attributes"]["primaries"].as_str().is_some_and(|p| p.starts_with("broker ") && p.contains(&broker_id) && p.ends_with("@1")));
    let installed = installed.unwrap_or_else(|| panic!("{broker_id} installed its first primary role"));
    assert_eq!(installed["attributes"]["outcome"], "held", "{installed:#}");
    // The gateway's own create is the first gateway's: it was commanded too, and no node-admin elected either type.
    assert!(named(&spans, "rdm.node_admin.primary.update.via-create").iter().any(|sp| sp["attributes"]["type"] == "gateway" && sp["attributes"]["outcome"] == "updated"), "the mesh's first gateway was commanded its first primary too");
    let node_admin_elections: Vec<_> = named(&spans, "rdm.mesh.election.resolve.via-recompute").into_iter().filter(|sp| sp["attributes"]["type"] != "node-admin").collect();
    assert!(node_admin_elections.iter().all(|sp| sp["attributes"]["observer_type"] == sp["attributes"]["type"]), "no node-admin computes another type's election: {node_admin_elections:#?}");
    estate.record_trace_url(commanded["trace_id"].as_str().unwrap_or(""));
}
