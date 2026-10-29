//! Drift: a birth the accepted topology names is unheard (RDM #47).
//!
//! The fabric-primary compares the current Build's topology with the births
//! it holds. A birth counts as present until node investigation decides it:
//! one the view no longer hears is unheard, and the provider's inspection of
//! its exact runtime gives the verdict that re-creates it (node-recovery.md
//! step 3, Q111).
//!
//! A path in the topology with no present birth is drift. The fabric-primary
//! opens the next attempt of the same Build (the current Build is unchanged:
//! the topology did not change); it never mints a Build.

use crate::accepted::FabricTopology;
use crate::model::{IncarnationId, NodeKind, NodeStatus};
use crate::topology::Topology;
use std::collections::HashSet;

/// One unheard birth the provider was asked about, its verdict and, when it exited, the exit code
/// (`None` when none is provable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExitedBirth {
    pub node_id: crate::model::NodeId,
    pub incarnation: IncarnationId,
    pub code: Option<i32>,
    /// node.runtime.inspect's verdict: `running`, `os-stopped`, `exited` or `unavailable`.
    pub verdict: &'static str,
}

/// One cohort below what the topology names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Shortfall {
    pub mesh: String,
    pub kind: NodeKind,
    pub desired: u32,
    pub present: u32,
    /// The unheard births the provider was asked about (`path.name`).
    pub exited: Vec<String>,
}

impl std::fmt::Display for Shortfall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = self.kind.name();
        write!(f, "{}.{kind}: {} present of {} accepted (unheard: {})", self.mesh, self.present, self.desired, self.exited.join(" "))
    }
}

/// The births of `t` the fabric-primary may have to recover: the ones it no longer hears
/// (`PendingReconnect`, or `Dead` once its offline tickle found no path). Only these are inspected.
pub fn unheard(t: &Topology) -> Vec<&crate::model::Node> {
    // `members`: a replaced predecessor (`<path>.old`) is its replacement operation's, never drift's.
    t.members().filter(|n| matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead)).collect()
}

/// The cohorts of `topology` that `t` holds fewer births for than it names,
/// counting every held birth but those in `exited` (unheard incarnations the
/// provider was asked about). A cohort short without one is not drift.
pub(crate) fn shortfall(topology: &FabricTopology, t: &Topology, exited: &HashSet<IncarnationId>) -> Vec<Shortfall> {
    let mut out = Vec::new();
    for m in topology.meshes.values() {
        for kind in NodeKind::ALL {
            let want = m.count(kind);
            let (mut present, mut gone) = (0u32, Vec::new());
            for n in t.nodes_of_type(&m.name, kind) {
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
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes,
        }
    }

    fn desired(a: u32, r: u32) -> FabricTopology {
        FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), MeshTopology::of(&MeshDesired::of("mesh1", [(rdm_core::NodeKind::NodeAdmin, a), (rdm_core::NodeKind::RpcNode, r)])))].into() }
    }

    #[test]
    fn a_silent_birth_counts_short_only_once_its_exact_runtime_is_inspected_exited() {
        use NodeStatus::{Dead, PendingReconnect, Ready as R};
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
        assert_eq!(s[0].to_string(), "mesh1.rpc_node: 2 present of 3 accepted (unheard: mesh1.rpc.2)");
    }

    /// CONTRACT: a replaced predecessor (`<path>.old`) is its replacement's, never drift's: it is not
    /// unheard, and it counts as no birth of its cohort, so the successor at `<path>` is the one
    /// present.
    #[test]
    fn a_renamed_predecessor_is_neither_unheard_nor_a_birth_of_its_node_type() {
        use NodeStatus::{Dead, Ready as R};
        let mut old = node("mesh1.rpc.1", Dead);
        old.name = old.name.renamed();
        let t = view(vec![node("mesh1.admin.1", R), node("mesh1.rpc.1", R), old.clone()]);
        assert!(unheard(&t).is_empty());
        let exited: HashSet<_> = [old.incarnation_id.clone().unwrap()].into();
        assert!(shortfall(&desired(1, 1), &t, &exited).is_empty(), "one rpc node present of one accepted");
    }

    #[test]
    fn a_node_type_short_with_no_exited_birth_is_not_drift() {
        use NodeStatus::Ready as R;
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
