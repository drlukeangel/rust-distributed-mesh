//! Proven drift: a birth the accepted topology names is gone (RDM #47;
//! `docs/i143/design.md` §2.2, §2.4).
//!
//! The fabric authority (the fabric primary, while its view authorizes)
//! compares the current Build's topology with the births it holds. A birth
//! counts as present until canonical evidence retires it: one the view no
//! longer hears (silent, unreachable, partitioned) is still present. It is
//! missing only when the provider inspected its exact published runtime and
//! found it exited. RPC failure, gossip silence, a partition or a runtime in
//! another control domain is never proof.
//!
//! A path in the topology with no present birth and a proven exit is drift.
//! The authority opens the next attempt of the same Build (`Fabric.build_id`
//! is unchanged: the topology did not change); it never mints a Build.

use crate::accepted::FabricTopology;
use crate::model::{IncarnationId, NodeKind, NodeStatus};
use crate::topology::Topology;
use std::collections::HashSet;

/// One birth whose exact runtime the provider proved exited, and the exit code that proof carries
/// (`None` when no code is provable: the exit is then unplanned and unexplained).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitedBirth {
    pub node_id: crate::model::NodeId,
    pub incarnation: IncarnationId,
    pub code: Option<i32>,
    /// Where the code came from: the provider's own status (`provider`), the runtime's exit record
    /// in its data dir (`exit-record`), or nothing provable (`none`).
    pub source: &'static str,
}

impl ExitedBirth {
    /// The proof names `TRANSPORT_STOPPED`: exactly the reserved exit code, never inferred from a
    /// network symptom. Every other exit, and an unknown reason, is a replacement.
    pub fn transport_stopped(&self) -> bool {
        self.code == Some(rafka_mesh_entity::runtime::TRANSPORT_STOPPED_EXIT_CODE)
    }
}

/// One cohort below what the topology names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shortfall {
    pub mesh: String,
    pub kind: NodeKind,
    pub desired: u32,
    pub present: u32,
    /// The births whose exact runtime was proven exited (`path.name`).
    pub exited: Vec<String>,
}

impl std::fmt::Display for Shortfall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = self.kind.name();
        write!(f, "{}.{kind}: {} present of {} accepted (exited: {})", self.mesh, self.present, self.desired, self.exited.join(" "))
    }
}

/// The births of `t` the authority may have to prove: the ones it no longer hears
/// (`PendingReconnect`, or `Dead` once its offline tickle found no path). Only these are inspected.
pub fn unheard(t: &Topology) -> Vec<&crate::model::Node> {
    t.nodes.iter().filter(|n| matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead)).collect()
}

/// The cohorts of `topology` that `t` holds fewer births for than it names,
/// counting every held birth but those in `exited` (incarnations whose exact
/// runtime was inspected and found exited). A cohort short without a proven
/// exit is not drift.
pub fn shortfall(topology: &FabricTopology, t: &Topology, exited: &HashSet<IncarnationId>) -> Vec<Shortfall> {
    let mut out = Vec::new();
    for m in topology.meshes.values() {
        for kind in NodeKind::ALL {
            let want = m.count(kind);
            let (mut present, mut gone) = (0u32, Vec::new());
            for n in t.cohort(&m.name, kind) {
                if n.incarnation_id.as_ref().is_some_and(|i| exited.contains(i)) {
                    gone.push(n.name.to_string());
                } else {
                    present += 1;
                }
            }
            if present < want && !gone.is_empty() {
                out.push(Shortfall { mesh: m.name.clone(), kind, desired: want, present, exited: gone });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accepted::MeshTopology;
    use crate::build::MeshDesired;
    use crate::model::{Fabric, FabricId, Mesh, MeshId, Node, ProviderKind, ScopeStatus};

    fn node(name: &str, status: NodeStatus) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = status;
        n.incarnation_id = Some(IncarnationId::mint());
        n
    }

    fn view(nodes: Vec<Node>) -> Topology {
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
            nodes,
        }
    }

    fn desired(a: u32, r: u32) -> FabricTopology {
        FabricTopology { fabric: "fabric1".into(), meshes: [("mesh1".to_string(), MeshTopology::of(&MeshDesired::of("mesh1", [(rafka_mesh_entity::NodeKind::NodeAdmin, a), (rafka_mesh_entity::NodeKind::RpcNode, r)])))].into() }
    }

    #[test]
    fn a_silent_birth_is_held_until_its_exact_runtime_is_proven_exited() {
        use NodeStatus::{Dead, PendingReconnect, ReadyForTraffic as R};
        let t = view(vec![node("mesh1.admin.1", R), node("mesh1.rpc.1", R), node("mesh1.rpc.2", PendingReconnect), node("mesh1.rpc.3", R)]);
        let d = desired(1, 3);
        assert_eq!(unheard(&t).len(), 1);
        assert_eq!(unheard(&view(vec![node("mesh1.rpc.2", Dead)])).len(), 1, "a Dead birth is unheard too");
        // Unreachable, partitioned or in another control domain: no drift.
        assert!(shortfall(&d, &t, &HashSet::new()).is_empty());
        // Its runtime inspected and exited: one rpc node short.
        let exited: HashSet<_> = [t.nodes[2].incarnation_id.clone().unwrap()].into();
        let s = shortfall(&d, &t, &exited);
        assert_eq!(s, vec![Shortfall { mesh: "mesh1".into(), kind: NodeKind::RpcNode, desired: 3, present: 2, exited: vec!["mesh1.rpc.2".into()] }]);
        assert_eq!(s[0].to_string(), "mesh1.rpc_node: 2 present of 3 accepted (exited: mesh1.rpc.2)");
    }

    #[test]
    fn a_cohort_short_without_a_proven_exit_is_not_drift() {
        use NodeStatus::ReadyForTraffic as R;
        // A failed grow leaves the cohort short: that is the Build's outcome,
        // not runtime drift, and must not start Build after Build.
        let t = view(vec![node("mesh1.admin.1", R), node("mesh1.rpc.1", R)]);
        assert!(shortfall(&desired(1, 3), &t, &HashSet::new()).is_empty());
        // At or above desired with an exited birth: not short.
        let mut t = view(vec![node("mesh1.admin.1", R), node("mesh1.rpc.1", R), node("mesh1.rpc.2", NodeStatus::PendingReconnect)]);
        t.nodes.push(node("mesh1.rpc.3", R));
        let exited: HashSet<_> = [t.nodes[2].incarnation_id.clone().unwrap()].into();
        assert!(shortfall(&desired(1, 2), &t, &exited).is_empty());
    }
}
