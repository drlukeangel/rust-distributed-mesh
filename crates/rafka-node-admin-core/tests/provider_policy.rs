//! i143.e2.s1 functional: the deployment provider is fabric policy.
//!
//! The first admin's `MESH_SPAWN_TYPE` becomes the fabric's provider, which the
//! fabric view advertises; a second admin inherits it from that view (or is
//! refused by name when its own value disagrees); a Build that names a
//! provider is refused and publishes nothing.

use rafka_node_admin_core::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
use rafka_node_admin_core::deployment::provider::{FabricPolicy, PolicyRefusal};
use rafka_node_admin_core::control_plane::ControlPlane;
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc_contract::status::StatusReply;
use serde_json::{json, Value};
use std::sync::Arc;

async fn first_admin(spawn_type: Option<&str>) -> (Arc<MemoryBuildStateAdapter>, Arc<ControlPlane>) {
    let policy = FabricPolicy::bootstrap(spawn_type).expect("bootstrap");
    let mut admin = Node::allocated("mesh1.admin.1".parse().unwrap());
    admin.status = NodeStatus::ReadyForTraffic;
    admin.is_primary = true;
    admin.is_fabric_primary = true;
    admin.admin_api_base = Some("http://127.0.0.1:18001".into());
    let topology = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Pending, provider: policy.provider },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Pending }],
        nodes: vec![admin],
    };
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = rafka_node_admin_core::accepted::AcceptedStore::seeded(&*builds, topology.fabric.id.clone(), rafka_node_admin_core::accepted::FabricTopology::of_observed(&topology), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), topology, crate::adopted_time()));
    (builds, cp)
}

/// What `fabric.get` answers at the admin `cp` serves (`StatusRequest::GetFabric`, answered from its view by
/// `views::fabric_answer`), as the JSON the view serializes to.
async fn fabric_get(cp: &ControlPlane) -> Value {
    match rafka_node_admin_core::views::fabric_answer(&cp.me, &*cp.topology.read().await, None) {
        StatusReply::Fabric { fabric } => serde_json::to_value(fabric).expect("a view serializes"),
        other => panic!("fabric.get answered {other:?}"),
    }
}

/// A joining admin reads the fabric's provider from the advertised view.
fn policy_from_view(view: &Value) -> FabricPolicy {
    let provider: ProviderKind = serde_json::from_value(view["provider"].clone()).expect("fabric view names its provider");
    FabricPolicy { provider }
}

#[tokio::test]
async fn a_second_admin_inherits_the_fabric_policy_or_refuses_by_name() {
    let (_, cp) = first_admin(Some("container")).await;
    let view = fabric_get(&cp).await;
    assert_eq!(view["provider"], "container");
    let established = policy_from_view(&view);
    assert_eq!(FabricPolicy::inherit(established, None).unwrap().provider, ProviderKind::Container, "unset inherits");
    assert_eq!(
        FabricPolicy::inherit(established, Some("process")),
        Err(PolicyRefusal::ProviderMismatch { fabric: ProviderKind::Container, local: ProviderKind::Process })
    );
}

#[tokio::test]
async fn an_unknown_provider_never_bootstraps_a_fabric() {
    assert_eq!(FabricPolicy::bootstrap(Some("vm")), Err(PolicyRefusal::UnknownProvider { value: "vm".into() }));
}

