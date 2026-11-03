//! The fabric-primary composes a Build's topology from the change as it came in (build.md §1, §4.4;
//! Q342): a `Create` adds exactly the meshes and nodes it names, each node whole, its type's storage
//! where it names none; a `Delete` removes exactly the nodes its node.ids hold now and the meshes it
//! names. Every refusal is by name, nothing composed.

use rdm_admin::accepted::{compile, FabricTopology};
use rdm_core::build::BuildChange;
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use rdm_core::build::{Meshes, Nodes};

fn node(path: &str, name: &str, mesh: &str, r#type: &str) -> Node {
    let mut n = Node::allocated(PathName::new(path, name).unwrap(), mesh, r#type);
    n.status = NodeStatus::Ready;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// mesh1: its admin and mesh1.node.1.
fn view() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node("mesh1.node-admin", "1", "mesh1", "node-admin"), node("mesh1.node", "1", "mesh1", "node")],
    }
}

fn nodes(path: &str, name: &str, mesh: &str, r#type: &str) -> Nodes {
    Nodes { path: path.into(), name: name.into(), mesh: mesh.into(), r#type: r#type.into(), storage_persistence: None, executable: Some(crate::test_executable(r#type)), tags: Default::default(), config: Default::default() }
}

fn create(meshes: &[&str], n: Vec<Nodes>) -> BuildChange {
    BuildChange::Create { meshes: meshes.iter().map(|m| Meshes { name: (*m).into() }).collect(), nodes: n }
}

fn refusal(current: &FabricTopology, change: &BuildChange, observed: &Topology) -> String {
    match compile(current, change, observed) {
        Ok(t) => panic!("{change:?} is refused by name, not composed into {t:?}"),
        Err(r) => r.reason().to_string(),
    }
}

// @feature: fabric-build
/// CONTRACT (build.md §1: `Create { meshes, nodes }`, "each node by its Nodes schema, whole; a node
/// whose mesh is not yet live creates that mesh with its first node-admin"; the topology composed at
/// acceptance "each created node's type defaults filled in"): a Create of mesh2 and mesh3, each with
/// its node-admin and an rpc node, composes the topology with both meshes under ids minted now and each
/// named node at its path and name, its storage its type's, mesh1 and its nodes untouched. A node naming
/// its storage keeps it.
#[test]
fn a_create_of_two_meshes_composes_each_named_node_and_leaves_the_rest() {
    let observed = view();
    let current = FabricTopology::of_observed(&observed);
    let ephemeral_admin = Nodes { storage_persistence: Some(rdm_core::meta::StoragePersistence::Ephemeral), ..nodes("mesh3.node-admin", "1", "mesh3", "node-admin") };
    let change = create(&["mesh2", "mesh3"], vec![nodes("mesh2.node-admin", "1", "mesh2", "node-admin"), nodes("mesh2.node", "1", "mesh2", "node"), ephemeral_admin, nodes("mesh3.node", "1", "mesh3", "node")]);
    let next = compile(&current, &change, &observed).unwrap_or_else(|e| panic!("the Create composes: {e}"));
    assert_eq!(next.meshes.keys().cloned().collect::<Vec<_>>(), ["mesh1", "mesh2", "mesh3"]);
    assert_eq!(next.meshes["mesh1"], current.meshes["mesh1"], "mesh1 is untouched");
    assert_ne!(next.meshes["mesh2"].mesh_id, next.meshes["mesh3"].mesh_id, "each new mesh's id is minted for it");
    let spec = |p: &str, n: &str| next.node_spec(&PathName::new(p, n).unwrap()).cloned().unwrap_or_else(|| panic!("{p}.{n} is in the topology"));
    assert_eq!(spec("mesh2.node-admin", "1").storage_persistence, rdm_core::meta::StoragePersistence::of_type("node-admin"), "no storage named: the type's");
    assert_eq!(spec("mesh2.node", "1").r#type, "node");
    assert_eq!(spec("mesh3.node-admin", "1").storage_persistence, rdm_core::meta::StoragePersistence::Ephemeral, "the storage the node names");
    assert_eq!(next.meshes["mesh3"].nodes.len(), 2);
}

// @feature: fabric-build
/// CONTRACT (build.md §1, `Create` "Refused by name": "a new mesh with no node-admin among its nodes; a
/// node naming a mesh neither in the request nor live; a path and name already held; an undeclared
/// type; a node with no executable, its own or its type's"): each is refused by its name and nothing is
/// composed. The undeclared type is refused at the `BuildService`, before composing, against the Fabric's types
/// (`declared_types::an_undeclared_type_is_refused_by_name_at_acceptance`).
#[test]
fn a_create_is_refused_by_name_for_each_rule_it_breaks() {
    let observed = view();
    let current = FabricTopology::of_observed(&observed);
    let no_admin = create(&["mesh2"], vec![nodes("mesh2.node", "1", "mesh2", "node")]);
    assert_eq!(refusal(&current, &no_admin, &observed), "mesh-without-admin");
    let no_mesh = create(&[], vec![nodes("mesh9.node", "1", "mesh9", "node")]);
    assert_eq!(refusal(&current, &no_mesh, &observed), "unknown-mesh");
    let held = create(&[], vec![nodes("mesh1.node", "1", "mesh1", "node")]);
    assert_eq!(refusal(&current, &held, &observed), "name-held");
    let bare = create(&[], vec![Nodes { executable: None, ..nodes("mesh1.node", "2", "mesh1", "node") }]);
    assert_eq!(refusal(&current, &bare, &observed), "executable-missing");
    let existing = create(&["mesh1"], vec![nodes("mesh1.node-admin", "2", "mesh1", "node-admin")]);
    assert_eq!(refusal(&current, &existing, &observed), "mesh-already-exists");
}

// @feature: fabric-build
/// CONTRACT (build.md §1: `Delete { meshes, node_ids }`, "each node by its node.id, never by its path
/// and name", refused by name "a node.id already deleted or replaced, in its own result"; §4.4 "A
/// deleted node.id stands"): a Delete of mesh1.node.1's node.id removes that path and nothing else. A
/// node.id no birth holds, and the node.id of a birth a new one replaced at its path and name, are
/// refused `unknown-node`, naming the node.id.
#[test]
fn a_delete_removes_the_node_its_node_id_holds_and_refuses_a_gone_or_replaced_one_by_name() {
    let observed = view();
    let current = FabricTopology::of_observed(&observed);
    let rpc = PathName::new("mesh1.node", "1").unwrap();
    let rpc_id = observed.node(&rpc).unwrap().node_id.clone();
    let next = compile(&current, &BuildChange::Delete { meshes: vec![], node_ids: vec![rpc_id.clone()] }, &observed).unwrap_or_else(|e| panic!("the Delete composes: {e}"));
    assert!(!next.contains(&rpc), "mesh1.node.1 leaves the topology");
    assert!(next.contains(&PathName::new("mesh1.node-admin", "1").unwrap()), "the admin stays");

    let gone = NodeId::mint();
    match compile(&current, &BuildChange::Delete { meshes: vec![], node_ids: vec![gone.clone()] }, &observed) {
        Err(r) => {
            assert_eq!(r.reason(), "unknown-node");
            assert!(r.to_string().contains(gone.as_str()), "the refusal names the node.id: {r}");
        }
        Ok(t) => panic!("a node.id no birth holds is refused by name, not composed into {t:?}"),
    }

    // mesh1.node.1 re-born as a new node.id: the view holds the successor at the path, and the old birth
    // behind it.
    let mut replaced = observed.clone();
    let mut successor = node("mesh1.node", "1", "mesh1", "node");
    successor.node_id = NodeId::mint();
    replaced.nodes.retain(|n| n.node_id != rpc_id);
    replaced.nodes.push(successor);
    let mut old = observed.node(&rpc).unwrap().clone();
    old.status = NodeStatus::Dead;
    replaced.nodes.push(old);
    match compile(&current, &BuildChange::Delete { meshes: vec![], node_ids: vec![rpc_id.clone()] }, &replaced) {
        Err(r) => {
            assert_eq!(r.reason(), "unknown-node");
            assert!(r.to_string().contains(rpc_id.as_str()), "the refusal names the replaced node.id: {r}");
        }
        Ok(t) => panic!("a replaced node.id is refused by name, not composed into {t:?}"),
    }
}
