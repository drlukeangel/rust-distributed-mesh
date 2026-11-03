//! A created node's type at the fabric-primary's acceptance (node-identity-and-state.md §2: "RDM ships
//! two types: `node`, the base every type extends, and `node-admin`. The application declares its own on
//! the Fabric (`types`), each overriding `node`'s defaults (`storage_persistence`, `executable`); a node
//! naming an undeclared type is refused by name at node.create, and neither of RDM's two can be declared
//! again").

use rdm_admin::accepted::{AcceptedStore, FabricTopology};
use rdm_admin::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::control_plane::Refusal;
use rdm_admin::fabric_storage::FabricStorage as _;
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use rdm_core::meta::{NodeSpec, StoragePersistence};
use std::sync::Arc;

fn path(p: &str, n: &str) -> rdm_core::PathName {
    rdm_core::PathName::new(p, n).unwrap()
}

/// A ready fabric of one mesh holding its one node-admin, the fabric-primary.
fn view() -> Topology {
    let mut admin = Node::allocated(path("mesh1.node-admin", "1"), "mesh1", rdm_core::NODE_ADMIN);
    admin.status = NodeStatus::Ready;
    admin.is_primary = true;
    admin.is_fabric_primary = true;
    admin.incarnation_id = Some(IncarnationId::mint());
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![admin],
    }
}

/// The fabric-primary's control plane over `view`, its Fabric record declaring `types`, and the
/// Create that adds `added` to what Day 0 accepted.
async fn submit_adding(types: Vec<rdm_core::NodeType>, added: NodeSpec) -> (Result<rdm_admin::control_plane::Opened, Refusal>, Arc<MemoryBuildStateAdapter>) {
    let t = view();
    let admin = t.node(&path("mesh1.node-admin", "1")).unwrap();
    let executor = rdm_core::framing::Birth { node_id: admin.node_id.clone(), incarnation: admin.incarnation_id.clone().unwrap() };
    let held = FabricTopology::of_observed(&t);
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), held.clone(), &executor).await.unwrap();
    // The Fabric record declares `types`, as Day 0 writes RDM_NODE_TYPES on it.
    let mut record = accepted.storage().fabric().await.unwrap().expect("the seeded Fabric record");
    record.types = types;
    accepted.storage().put_fabric(&record).await.unwrap();
    let cp = crate::control_plane(builds.clone(), accepted, path("mesh1.node-admin", "1"), t.clone(), crate::adopted_time());
    let _ = cp.node_rows.set(crate::records_of(&t).await);
    let node = rdm_core::build::Nodes { path: added.path, name: added.name, mesh: added.mesh, r#type: added.r#type, storage_persistence: None, executable: added.executable, tags: added.tags, config: added.config };
    (cp.submit("test", rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: vec![node] }).await, builds)
}

// @feature: node-types
/// CONTRACT (node-identity-and-state.md §2): a node of a type the Fabric declares, created with no
/// executable of its own, is accepted with its type's: the declared type's defaults apply at
/// acceptance. Must NOT happen: the create refused `executable-missing` while its type names one, or
/// accepted with an executable other than its type's.
#[tokio::test]
async fn a_declared_types_defaults_are_applied_at_acceptance() {
    let worker = crate::test_executable("worker");
    let declared = vec![rdm_core::NodeType { name: "worker".into(), storage_persistence: StoragePersistence::Ephemeral, executable: Some(worker.clone()) }];
    let added = NodeSpec::default_for(path("mesh1.worker", "1"), "mesh1", "worker");
    assert_eq!(added.executable, None, "the request names no executable of its own");
    let (opened, builds) = submit_adding(declared, added).await;
    let opened = opened.unwrap_or_else(|e| panic!("a declared type's node is accepted: {e:?}"));
    let build = builds.read_build(&opened.build_id).await.unwrap();
    let spec = build.topology.node_spec(&path("mesh1.worker", "1")).expect("the accepted topology holds the node").clone();
    assert_eq!((spec.r#type.as_str(), spec.executable), ("worker", Some(worker)), "the node runs its declared type's executable");
}

// @feature: node-types
/// CONTRACT (node-identity-and-state.md §2): a node naming a type the Fabric does not declare, and
/// that is neither `node` nor `node-admin`, is refused by name at acceptance, naming the node and the
/// type; nothing is accepted. Must NOT happen: an undeclared type accepted, or refused under another
/// reason.
#[tokio::test]
async fn an_undeclared_type_is_refused_by_name_at_acceptance() {
    let mut added = NodeSpec::default_for(path("mesh1.worker", "1"), "mesh1", "worker");
    added.executable = Some(crate::test_executable("worker"));
    let (opened, builds) = submit_adding(Vec::new(), added).await;
    match opened {
        Err(Refusal::Reject(reject)) => {
            assert_eq!(reject.reason(), "unknown-type", "{reject}");
            let text = reject.to_string();
            assert!(text.contains("mesh1.worker") && text.contains("`worker`"), "the refusal names the node and its type: {text}");
        }
        other => panic!("an undeclared type is refused by name: {other:?}"),
    }
    assert_eq!(builds.current().await.unwrap().map(|b| b.topology.contains(&path("mesh1.worker", "1"))), Some(false), "nothing is accepted");
    // RDM's two types need no declaration.
    let mut plain = NodeSpec::default_for(path("mesh1.node", "1"), "mesh1", rdm_core::NODE);
    plain.executable = Some(crate::test_executable(rdm_core::NODE));
    let (opened, _) = submit_adding(Vec::new(), plain).await;
    opened.unwrap_or_else(|e| panic!("a `node` needs no declaration: {e:?}"));
}
