//! i143.e2.s1 functional: the deployment provider is fabric policy.
//!
//! The first admin's `MESH_SPAWN_TYPE` becomes the fabric's provider, which the
//! fabric view advertises; a second admin inherits it from that view (or is
//! refused by name when its own value disagrees); a Build that names a
//! provider is refused and publishes nothing.

use rdm_admin::build_state::MemoryBuildStateAdapter;
use rdm_admin::deployment::provider::{FabricPolicy, PolicyRefusal};
use rdm_admin::control_plane::ControlPlane;
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use rdm_core::status::StatusReply;
use serde_json::Value;
use std::sync::Arc;

async fn first_admin(spawn_type: Option<&str>) -> (Arc<MemoryBuildStateAdapter>, Arc<ControlPlane>) {
    let policy = FabricPolicy::bootstrap(spawn_type).expect("bootstrap");
    let mut admin = Node::allocated("mesh1.admin.1".parse().unwrap());
    admin.status = NodeStatus::Ready;
    admin.is_primary = true;
    admin.is_fabric_primary = true;
    admin.admin_api_base = Some("http://127.0.0.1:18001".into());
    let topology = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Pending, provider: policy.provider },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Pending }],
        nodes: vec![admin],
    };
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = rdm_admin::accepted::AcceptedStore::seeded(&*builds, topology.fabric.id.clone(), rdm_admin::accepted::FabricTopology::of_observed(&topology), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), topology, crate::adopted_time()));
    (builds, cp)
}

/// What `fabric.get` answers at the admin `cp` serves (answered from its view by
/// `views::fabric_answer`), as the JSON the view serializes to.
async fn fabric_get(cp: &ControlPlane) -> Value {
    match rdm_admin::views::fabric_answer(&cp.me, &*cp.topology.read().await, None, &rdm_core::filter::Filter::new()) {
        StatusReply::Fabrics { fabrics } if fabrics.len() == 1 => serde_json::to_value(&fabrics[0]).expect("a view serializes"),
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

