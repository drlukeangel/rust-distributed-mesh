//! Readiness gates (PRD §9 readiness example; mesh-control-plane.md §8).
//!
//! Eligibility derives from desired topology, so one rule gates SN, MN and
//! MM alike:
//!
//! ```text
//! Fabric Pending -> ReadyForTraffic
//!     all desired meshes exist
//!     all desired members of each mesh are ReadyForTraffic
//!     all blocking pre-ready hooks complete      (the transition pipeline)
//! ```
//!
//! SN/MN desire one mesh, MM two; nothing here branches on the shape name.
//! Genuinely shape-specific behaviour uses an explicit `ShapePredicate` on a hook.

use crate::build::{FabricDesired, MeshDesired};
use crate::lifecycle::ShapeFacts;
use crate::model::NodeStatus;
use crate::topology::Topology;

impl ShapeFacts {
    /// The shape facts hook predicates read, derived from desired topology.
    pub fn from_desired(desired: &FabricDesired) -> Self {
        Self { desired_meshes: desired.meshes.len() as u32 }
    }
}

/// Every reason `mesh` is not ready, named; empty means ready.
pub(crate) fn mesh_not_ready(desired: &MeshDesired, observed: &Topology) -> Vec<String> {
    let mut why = Vec::new();
    if !observed.meshes.iter().any(|m| m.name == desired.name) {
        why.push(format!("mesh {} does not exist", desired.name));
        return why;
    }
    for (kind, want) in desired.counts() {
        let ready = observed.cohort(&desired.name, kind).filter(|n| n.status == NodeStatus::ReadyForTraffic).count() as u32;
        if ready < want {
            why.push(format!("mesh {}: {ready}/{want} {} ready-for-traffic", desired.name, kind.segment()));
        }
    }
    why
}

/// Fabric `Pending -> ReadyForTraffic` eligibility.
pub fn fabric_ready_eligibility(desired: &FabricDesired, observed: &Topology) -> Result<(), String> {
    let why: Vec<String> = desired.meshes.iter().flat_map(|m| mesh_not_ready(m, observed)).collect();
    if why.is_empty() {
        Ok(())
    } else {
        Err(why.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;

    fn desired(meshes: &[&str]) -> FabricDesired {
        FabricDesired {
            fabric: "fabric1".into(),
            meshes: meshes.iter().map(|m| MeshDesired::of((*m).to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 3)])).collect(),
        }
    }

    fn mesh_nodes(m: &str, status: NodeStatus) -> Vec<Node> {
        let mut v = Vec::new();
        for (kind, n) in [("admin", 2), ("rpc", 3)] {
            for i in 1..=n {
                let mut node = Node::allocated(format!("{m}.{kind}.{i}").parse().unwrap());
                node.status = status;
                v.push(node);
            }
        }
        v
    }

    fn observed(meshes: &[(&str, NodeStatus)]) -> Topology {
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Pending, provider: ProviderKind::Process },
            meshes: meshes.iter().map(|(m, _)| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::Pending }).collect(),
            nodes: meshes.iter().flat_map(|(m, s)| mesh_nodes(m, *s)).collect(),
        }
    }

    #[test]
    fn mn_is_ready_when_its_one_desired_mesh_is_fully_ready() {
        assert_eq!(fabric_ready_eligibility(&desired(&["mesh1"]), &observed(&[("mesh1", NodeStatus::ReadyForTraffic)])), Ok(()));
    }

    #[test]
    fn fabric_readiness_waits_on_every_mesh() {
        let d = desired(&["mesh1", "mesh2"]);
        assert_eq!(
            fabric_ready_eligibility(&d, &observed(&[("mesh1", NodeStatus::ReadyForTraffic)])),
            Err("mesh mesh2 does not exist".into())
        );
        let mut t = observed(&[("mesh1", NodeStatus::ReadyForTraffic), ("mesh2", NodeStatus::ReadyForTraffic)]);
        t.nodes.last_mut().unwrap().status = NodeStatus::Pending; // mesh2.rpc.3
        assert_eq!(fabric_ready_eligibility(&d, &t), Err("mesh mesh2: 2/3 rpc ready-for-traffic".into()));
        t.nodes.last_mut().unwrap().status = NodeStatus::ReadyForTraffic;
        assert_eq!(fabric_ready_eligibility(&d, &t), Ok(()));
    }

    #[test]
    fn shape_facts_derive_from_desired_topology() {
        assert_eq!(ShapeFacts::from_desired(&desired(&["mesh1"])).desired_meshes, 1);
        assert_eq!(ShapeFacts::from_desired(&desired(&["mesh1", "mesh2"])).desired_meshes, 2);
    }
}
