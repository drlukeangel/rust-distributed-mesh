//! Build: the only topology mutation engine (PRD §1.2–3, §7;
//! mesh-control-plane.md §1).
//!
//! Every topology-changing request compiles to a [`BuildIntent`]. The planner
//! computes desired − observed and emits idempotent operations keyed by stable
//! logical identities. It never resumes from a saved instruction pointer: each
//! execution attempt re-plans from the current desired and observed state, so
//! operations already satisfied are not replayed.
//!
//! An intent relative to the topology it was submitted against (add *a*
//! node, restart *this* birth, remove *this* node) is pinned at submit by
//! [`pin`]: it records the exact node or incarnation it means. A re-plan of
//! a pinned intent, by any executor and any attempt, computes only what is
//! left; once satisfied it plans nothing, never a second node or restart.

use crate::model::{is_valid_mesh_name, IncarnationId, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;

/// Opaque Build identity. It belongs to fabric control state, not to the
/// executor that happens to run it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BuildId(pub String);

impl BuildId {
    pub fn mint() -> Self {
        Self(format!("bld-{}", hex::encode(rand::random::<[u8; 12]>())))
    }
}

impl fmt::Display for BuildId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeshDesired {
    pub name: String,
    pub node_admin: u32,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub rpc_node: u32,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub broker: u32,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub gateway: u32,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub compute: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

impl MeshDesired {
    /// The desired count of every kind, `node_admin` first.
    pub fn counts(&self) -> [(NodeKind, u32); 5] {
        [
            (NodeKind::NodeAdmin, self.node_admin),
            (NodeKind::RpcNode, self.rpc_node),
            (NodeKind::Broker, self.broker),
            (NodeKind::Gateway, self.gateway),
            (NodeKind::Compute, self.compute),
        ]
    }

    pub fn count(&self, kind: NodeKind) -> u32 {
        self.counts().into_iter().find(|(k, _)| *k == kind).map(|(_, n)| n).unwrap_or(0)
    }

    /// A mesh named `name` with `counts`; every other kind at 0.
    pub fn of(name: impl Into<String>, counts: impl IntoIterator<Item = (NodeKind, u32)>) -> Self {
        let mut d = MeshDesired { name: name.into(), node_admin: 0, rpc_node: 0, broker: 0, gateway: 0, compute: 0 };
        for (k, n) in counts {
            *d.count_mut(k) = n;
        }
        d
    }

    pub fn count_mut(&mut self, kind: NodeKind) -> &mut u32 {
        match kind {
            NodeKind::NodeAdmin => &mut self.node_admin,
            NodeKind::RpcNode => &mut self.rpc_node,
            NodeKind::Broker => &mut self.broker,
            NodeKind::Gateway => &mut self.gateway,
            NodeKind::Compute => &mut self.compute,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FabricDesired {
    pub fabric: String,
    pub meshes: Vec<MeshDesired>,
}

/// What a control surface asked for. Front doors (routes, clients, tests)
/// only ever submit one of these.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BuildIntent {
    /// The whole fabric's desired meshes and counts (`POST /api/build`).
    ReconcileFabric { desired: FabricDesired },
    /// One mesh's desired counts (grow/shrink).
    ReconcileMesh { desired: MeshDesired },
    /// `POST /api/nodes/spawn`. Pinned: `target` is the node it adds.
    AddNode {
        mesh: String,
        node_kind: NodeKind,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<PathName>,
    },
    /// `DELETE /api/nodes/{name}`. Pinned: `incarnation` is the birth it removes.
    RemoveNode {
        node: PathName,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        incarnation: Option<IncarnationId>,
    },
    /// `POST /api/nodes/{name}/restart`. Pinned: `from_incarnation` is the
    /// birth it replaces.
    RestartNode {
        node: PathName,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from_incarnation: Option<IncarnationId>,
    },
    /// Retire the node and create a new logical node at the same path.
    ReplaceNode { node: PathName },
    /// `POST /api/meshes`.
    CreateMesh { desired: MeshDesired },
    /// `DELETE /api/meshes/{id|name}`.
    RemoveMesh { mesh: String },
}

/// One idempotent operation. Its key is a stable logical identity, so a
/// re-plan never duplicates an operation that observed state already satisfies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum BuildOperation {
    CreateMesh { mesh: String },
    CreateNode { node: PathName },
    RestartNode { node: PathName },
    /// Retire through the retire pipeline; `permanent` releases storage.
    RetireNode { node: PathName, permanent: bool },
    RetireMesh { mesh: String },
}

impl BuildOperation {
    /// The idempotency key.
    pub fn key(&self) -> String {
        match self {
            Self::CreateMesh { mesh } => format!("create-mesh:{mesh}"),
            Self::CreateNode { node } => format!("create-node:{node}"),
            Self::RestartNode { node } => format!("restart-node:{node}"),
            Self::RetireNode { node, .. } => format!("retire-node:{node}"),
            Self::RetireMesh { mesh } => format!("retire-mesh:{mesh}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildPlan {
    pub operations: Vec<BuildOperation>,
}

/// A Build refused by name (`rafka.node_admin.build.reject.via-<reason>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "kebab-case")]
pub enum BuildReject {
    InvalidMeshName { mesh: String },
    DuplicateMesh { mesh: String },
    MeshWithoutAdmin { mesh: String },
    UnknownMesh { mesh: String },
    UnknownNode { node: String },
    NodeNotLive { node: String },
    WouldLeaveMeshWithoutAdmin { mesh: String },
    MeshAlreadyExists { mesh: String },
    FabricMismatch { requested: String, fabric: String },
    EmptyFabric,
    /// A Build tried to choose a provider; provider is fabric policy (PRD §1.7).
    ProviderInBuild { fabric_provider: crate::model::ProviderKind },
    /// A mesh's paths and its per-path meta disagree: a materialized path with no meta, or meta
    /// for a path the mesh does not hold.
    NodeMetaMismatch { mesh: String, missing: Vec<String>, extra: Vec<String> },
}

impl BuildReject {
    /// The span reason segment: `via-<this>`.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::InvalidMeshName { .. } => "invalid-mesh-name",
            Self::DuplicateMesh { .. } => "duplicate-mesh",
            Self::MeshWithoutAdmin { .. } => "mesh-without-admin",
            Self::UnknownMesh { .. } => "unknown-mesh",
            Self::UnknownNode { .. } => "unknown-node",
            Self::NodeNotLive { .. } => "node-not-live",
            Self::WouldLeaveMeshWithoutAdmin { .. } => "would-leave-mesh-without-admin",
            Self::MeshAlreadyExists { .. } => "mesh-already-exists",
            Self::FabricMismatch { .. } => "fabric-mismatch",
            Self::EmptyFabric => "empty-fabric",
            Self::ProviderInBuild { .. } => "provider-mismatch",
            Self::NodeMetaMismatch { .. } => "node-meta-mismatch",
        }
    }
}

impl fmt::Display for BuildReject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMeshName { mesh } => write!(f, "mesh name `{mesh}` must match [a-z0-9][a-z0-9-]{{0,63}}"),
            Self::DuplicateMesh { mesh } => write!(f, "mesh `{mesh}` is desired twice"),
            Self::MeshWithoutAdmin { mesh } => write!(f, "mesh `{mesh}` needs at least one node-admin"),
            Self::UnknownMesh { mesh } => write!(f, "mesh `{mesh}` is not in the fabric"),
            Self::UnknownNode { node } => write!(f, "node `{node}` is not in the fabric"),
            Self::NodeNotLive { node } => write!(f, "node `{node}` is not live"),
            Self::WouldLeaveMeshWithoutAdmin { mesh } => write!(f, "mesh `{mesh}` would be left without a live node-admin"),
            Self::MeshAlreadyExists { mesh } => write!(f, "mesh `{mesh}` already exists"),
            Self::FabricMismatch { requested, fabric } => write!(f, "Build names fabric `{requested}`, this is `{fabric}`"),
            Self::EmptyFabric => write!(f, "a fabric needs at least one desired mesh"),
            Self::ProviderInBuild { fabric_provider } => write!(
                f,
                "a Build may not choose a deployment provider; this fabric's policy is {fabric_provider:?}, fixed at bootstrap"
            ),
            Self::NodeMetaMismatch { mesh, missing, extra } => write!(
                f,
                "mesh `{mesh}`: every materialized path carries exactly one NodeMeta; missing [{}], without a node [{}]",
                missing.join(", "),
                extra.join(", ")
            ),
        }
    }
}

fn validate_mesh(m: &MeshDesired) -> Result<(), BuildReject> {
    if !is_valid_mesh_name(&m.name) {
        return Err(BuildReject::InvalidMeshName { mesh: m.name.clone() });
    }
    if m.node_admin == 0 {
        return Err(BuildReject::MeshWithoutAdmin { mesh: m.name.clone() });
    }
    Ok(())
}

fn live<'a>(t: &'a Topology, mesh: &str, kind: NodeKind) -> Vec<&'a crate::model::Node> {
    t.cohort(mesh, kind).filter(|n| n.status.is_live()).collect()
}

/// Bring one existing mesh's live cohorts to `desired` counts.
///
/// Grow fills the lowest free ordinals among the live members; a dead member's
/// path is free like any other. Shrink retires the highest-ordinal non-primary
/// members first, so no seat moves while any other member can go.
fn reconcile_counts(t: &Topology, desired: &MeshDesired, ops: &mut Vec<BuildOperation>) {
    for (kind, want) in desired.counts() {
        let members = live(t, &desired.name, kind);
        let have = members.len() as u32;
        if have < want {
            let taken: BTreeSet<u32> = members.iter().map(|n| n.name.ordinal).collect();
            let mut ord = 1;
            for _ in have..want {
                while taken.contains(&ord) || ops.contains(&BuildOperation::CreateNode { node: PathName::new(&desired.name, kind, ord) }) {
                    ord += 1;
                }
                ops.push(BuildOperation::CreateNode { node: PathName::new(&desired.name, kind, ord) });
                ord += 1;
            }
        } else if have > want {
            let mut order: Vec<&&crate::model::Node> = members.iter().collect();
            // non-primaries first, then by descending ordinal
            order.sort_by(|a, b| a.is_primary.cmp(&b.is_primary).then(b.name.ordinal.cmp(&a.name.ordinal)));
            for n in order.into_iter().take((have - want) as usize) {
                ops.push(BuildOperation::RetireNode { node: n.name.clone(), permanent: true });
            }
        }
    }
}

fn create_mesh(desired: &MeshDesired, ops: &mut Vec<BuildOperation>) {
    ops.push(BuildOperation::CreateMesh { mesh: desired.name.clone() });
    for (kind, want) in desired.counts() {
        for ord in 1..=want {
            ops.push(BuildOperation::CreateNode { node: PathName::new(&desired.name, kind, ord) });
        }
    }
}

fn find_live<'a>(t: &'a Topology, node: &PathName) -> Result<&'a crate::model::Node, BuildReject> {
    let n = t.node(node).ok_or_else(|| BuildReject::UnknownNode { node: node.to_string() })?;
    if !n.status.is_live() {
        return Err(BuildReject::NodeNotLive { node: node.to_string() });
    }
    Ok(n)
}

/// Plan `intent` against `observed`. Deterministic: the same inputs give the
/// same plan.
pub fn plan(intent: &BuildIntent, observed: &Topology) -> Result<BuildPlan, BuildReject> {
    let mut ops = Vec::new();
    let mesh_exists = |m: &str| observed.meshes.iter().any(|x| x.name == m);
    match intent {
        BuildIntent::ReconcileFabric { desired } => {
            if desired.fabric != observed.fabric.name {
                return Err(BuildReject::FabricMismatch { requested: desired.fabric.clone(), fabric: observed.fabric.name.clone() });
            }
            if desired.meshes.is_empty() {
                return Err(BuildReject::EmptyFabric);
            }
            let mut names = BTreeSet::new();
            for m in &desired.meshes {
                validate_mesh(m)?;
                if !names.insert(m.name.clone()) {
                    return Err(BuildReject::DuplicateMesh { mesh: m.name.clone() });
                }
            }
            let mut sorted = desired.meshes.clone();
            sorted.sort_by(|a, b| a.name.cmp(&b.name));
            for m in &sorted {
                if mesh_exists(&m.name) {
                    reconcile_counts(observed, m, &mut ops);
                } else {
                    create_mesh(m, &mut ops);
                }
            }
            let mut gone: Vec<&String> = observed.meshes.iter().map(|m| &m.name).filter(|m| !names.contains(*m)).collect();
            gone.sort();
            for m in gone {
                ops.push(BuildOperation::RetireMesh { mesh: m.clone() });
            }
        }
        BuildIntent::ReconcileMesh { desired } => {
            validate_mesh(desired)?;
            if !mesh_exists(&desired.name) {
                return Err(BuildReject::UnknownMesh { mesh: desired.name.clone() });
            }
            reconcile_counts(observed, desired, &mut ops);
        }
        BuildIntent::AddNode { mesh, target: Some(target), .. } => {
            if !mesh_exists(mesh) {
                return Err(BuildReject::UnknownMesh { mesh: mesh.clone() });
            }
            if !observed.node(target).is_some_and(|n| n.status.is_live()) {
                ops.push(BuildOperation::CreateNode { node: target.clone() });
            }
        }
        BuildIntent::AddNode { mesh, node_kind, target: None } => {
            if !mesh_exists(mesh) {
                return Err(BuildReject::UnknownMesh { mesh: mesh.clone() });
            }
            let mut desired = MeshDesired::of(mesh.clone(), NodeKind::ALL.map(|k| (k, live(observed, mesh, k).len() as u32)));
            *desired.count_mut(*node_kind) += 1;
            reconcile_counts(observed, &desired, &mut ops);
        }
        BuildIntent::RemoveNode { node, incarnation: Some(birth) } => {
            // Done once that birth is gone or another birth holds the path.
            if observed.node(node).is_some_and(|n| n.incarnation_id.as_ref() == Some(birth) && n.status.is_live()) {
                ops.push(BuildOperation::RetireNode { node: node.clone(), permanent: true });
            }
        }
        BuildIntent::RemoveNode { node, incarnation: None } => {
            find_live(observed, node)?;
            if node.kind == NodeKind::NodeAdmin && live(observed, &node.mesh, NodeKind::NodeAdmin).len() <= 1 {
                return Err(BuildReject::WouldLeaveMeshWithoutAdmin { mesh: node.mesh.clone() });
            }
            ops.push(BuildOperation::RetireNode { node: node.clone(), permanent: true });
        }
        BuildIntent::RestartNode { node, from_incarnation } => {
            let n = observed.node(node).ok_or_else(|| BuildReject::UnknownNode { node: node.to_string() })?;
            if n.status == NodeStatus::Leaving {
                return Err(BuildReject::NodeNotLive { node: node.to_string() });
            }
            // Pinned: done once the node runs a birth other than the one replaced.
            let already = from_incarnation.as_ref().is_some_and(|from| n.incarnation_id.as_ref() != Some(from));
            if !already {
                ops.push(BuildOperation::RestartNode { node: node.clone() });
            }
        }
        BuildIntent::ReplaceNode { node } => {
            find_live(observed, node)?;
            ops.push(BuildOperation::RetireNode { node: node.clone(), permanent: true });
            ops.push(BuildOperation::CreateNode { node: node.clone() });
        }
        BuildIntent::CreateMesh { desired } => {
            validate_mesh(desired)?;
            if mesh_exists(&desired.name) {
                return Err(BuildReject::MeshAlreadyExists { mesh: desired.name.clone() });
            }
            create_mesh(desired, &mut ops);
        }
        BuildIntent::RemoveMesh { mesh } => {
            if !mesh_exists(mesh) {
                return Err(BuildReject::UnknownMesh { mesh: mesh.clone() });
            }
            if observed.meshes.len() <= 1 {
                return Err(BuildReject::EmptyFabric);
            }
            ops.push(BuildOperation::RetireMesh { mesh: mesh.clone() });
        }
    }
    Ok(BuildPlan { operations: ops })
}

/// Validate `intent` against the topology it is submitted against and pin
/// what it means there (see the module docs). The result is what a Build
/// stores and every attempt re-plans.
pub fn pin(intent: BuildIntent, observed: &Topology) -> Result<BuildIntent, BuildReject> {
    let planned = plan(&intent, observed)?;
    Ok(match intent {
        BuildIntent::AddNode { mesh, node_kind, target: None } => {
            let target = planned.operations.iter().find_map(|op| match op {
                BuildOperation::CreateNode { node } if node.kind == node_kind => Some(node.clone()),
                _ => None,
            });
            BuildIntent::AddNode { mesh, node_kind, target }
        }
        BuildIntent::RemoveNode { node, incarnation: None } => {
            let incarnation = observed.node(&node).and_then(|n| n.incarnation_id.clone());
            BuildIntent::RemoveNode { node, incarnation }
        }
        BuildIntent::RestartNode { node, from_incarnation: None } => {
            let from_incarnation = observed.node(&node).and_then(|n| n.incarnation_id.clone());
            BuildIntent::RestartNode { node, from_incarnation }
        }
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;

    fn n(name: &str, status: NodeStatus, primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = status;
        n.is_primary = primary;
        n
    }

    fn fabric(nodes: Vec<Node>, meshes: &[&str]) -> Topology {
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: meshes.iter().map(|m| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::ReadyForTraffic }).collect(),
            nodes,
        }
    }

    fn mn() -> Topology {
        use NodeStatus::ReadyForTraffic as R;
        fabric(
            vec![
                n("mesh1.admin.1", R, true),
                n("mesh1.admin.2", R, false),
                n("mesh1.rpc.1", R, true),
                n("mesh1.rpc.2", R, false),
                n("mesh1.rpc.3", R, false),
            ],
            &["mesh1"],
        )
    }

    fn empty() -> Topology {
        fabric(vec![], &[])
    }

    fn p(s: &str) -> PathName {
        s.parse().unwrap()
    }

    fn create(s: &str) -> BuildOperation {
        BuildOperation::CreateNode { node: p(s) }
    }

    fn with_incarnations(mut t: Topology) -> Topology {
        for n in &mut t.nodes {
            n.incarnation_id = Some(IncarnationId::mint());
        }
        t
    }

    #[test]
    fn a_pinned_add_plans_its_one_node_until_it_is_live_and_then_nothing() {
        let t = with_incarnations(mn());
        let pinned = pin(BuildIntent::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode, target: None }, &t).unwrap();
        assert_eq!(pinned, BuildIntent::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode, target: Some(p("mesh1.rpc.4")) });
        assert_eq!(plan(&pinned, &t).unwrap().operations, vec![create("mesh1.rpc.4")]);
        let mut after = t.clone();
        after.nodes.push(n("mesh1.rpc.4", NodeStatus::ReadyForTraffic, false));
        assert_eq!(plan(&pinned, &after).unwrap().operations, vec![], "a re-plan never adds a second node");
        // Unpinned, the same re-plan would have added mesh1.rpc.5.
        let unpinned = BuildIntent::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode, target: None };
        assert_eq!(plan(&unpinned, &after).unwrap().operations, vec![create("mesh1.rpc.5")]);
    }

    #[test]
    fn a_pinned_restart_is_done_once_the_node_runs_another_birth() {
        let t = with_incarnations(mn());
        let pinned = pin(BuildIntent::RestartNode { node: p("mesh1.rpc.2"), from_incarnation: None }, &t).unwrap();
        let BuildIntent::RestartNode { from_incarnation: Some(from), .. } = &pinned else { panic!("not pinned: {pinned:?}") };
        assert_eq!(Some(from), t.node(&p("mesh1.rpc.2")).unwrap().incarnation_id.as_ref());
        assert_eq!(plan(&pinned, &t).unwrap().operations, vec![BuildOperation::RestartNode { node: p("mesh1.rpc.2") }]);
        let mut after = t.clone();
        after.nodes.iter_mut().find(|n| n.name == p("mesh1.rpc.2")).unwrap().incarnation_id = Some(IncarnationId::mint());
        assert_eq!(plan(&pinned, &after).unwrap().operations, vec![], "a re-plan never restarts twice");
    }

    #[test]
    fn a_pinned_remove_is_done_once_that_birth_is_gone_and_never_refused() {
        let t = with_incarnations(mn());
        let pinned = pin(BuildIntent::RemoveNode { node: p("mesh1.rpc.3"), incarnation: None }, &t).unwrap();
        assert_eq!(plan(&pinned, &t).unwrap().operations, vec![BuildOperation::RetireNode { node: p("mesh1.rpc.3"), permanent: true }]);
        let mut gone = t.clone();
        gone.nodes.retain(|n| n.name != p("mesh1.rpc.3"));
        assert_eq!(plan(&pinned, &gone), Ok(BuildPlan { operations: vec![] }), "done, not unknown-node");
        let mut reborn = gone.clone();
        let mut fresh = n("mesh1.rpc.3", NodeStatus::ReadyForTraffic, false);
        fresh.incarnation_id = Some(IncarnationId::mint());
        reborn.nodes.push(fresh);
        assert_eq!(plan(&pinned, &reborn).unwrap().operations, vec![], "another birth at the path is not this Build's");
        // Submit-time refusals are unchanged.
        assert_eq!(pin(BuildIntent::RemoveNode { node: p("mesh1.rpc.9"), incarnation: None }, &t), Err(BuildReject::UnknownNode { node: "mesh1.rpc.9".into() }));
    }

    fn retire(s: &str) -> BuildOperation {
        BuildOperation::RetireNode { node: p(s), permanent: true }
    }

    fn desired(meshes: &[(&str, u32, u32)]) -> BuildIntent {
        BuildIntent::ReconcileFabric {
            desired: FabricDesired {
                fabric: "fabric1".into(),
                meshes: meshes.iter().map(|(m, a, r)| MeshDesired::of((*m).to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, *a), (rafka_mesh_entity::NodeKind::RpcNode, *r)])).collect(),
            },
        }
    }

    #[test]
    fn birth_of_mn_plans_the_mesh_its_admins_then_its_rpc_nodes() {
        let plan = plan(&desired(&[("mesh1", 2, 3)]), &empty()).unwrap();
        assert_eq!(
            plan.operations,
            vec![
                BuildOperation::CreateMesh { mesh: "mesh1".into() },
                create("mesh1.admin.1"),
                create("mesh1.admin.2"),
                create("mesh1.rpc.1"),
                create("mesh1.rpc.2"),
                create("mesh1.rpc.3"),
            ]
        );
    }

    #[test]
    fn a_satisfied_desired_state_plans_nothing() {
        assert_eq!(plan(&desired(&[("mesh1", 2, 3)]), &mn()).unwrap().operations, vec![]);
    }

    #[test]
    fn grow_fills_free_ordinals_and_reuses_a_dead_members_path() {
        let ops = plan(&BuildIntent::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 3), (rafka_mesh_entity::NodeKind::RpcNode, 5)]) }, &mn()).unwrap();
        assert_eq!(ops.operations, vec![create("mesh1.admin.3"), create("mesh1.rpc.4"), create("mesh1.rpc.5")]);
        let mut t = mn();
        t.nodes[3].status = NodeStatus::PendingReconnect; // rpc.2 lost
        let ops = plan(&desired(&[("mesh1", 2, 3)]), &t).unwrap();
        assert_eq!(ops.operations, vec![create("mesh1.rpc.2")], "the lost slot is recreated, nothing else");
    }

    #[test]
    fn shrink_retires_highest_non_primaries_and_keeps_the_primary() {
        let mut t = mn();
        t.nodes[2].is_primary = false;
        t.nodes[4].is_primary = true; // rpc.3 holds the seat
        let ops = plan(&BuildIntent::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 1)]) }, &t).unwrap();
        assert_eq!(ops.operations, vec![retire("mesh1.rpc.2"), retire("mesh1.rpc.1")]);
        let ops = plan(&BuildIntent::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 1), (rafka_mesh_entity::NodeKind::RpcNode, 3)]) }, &mn()).unwrap();
        assert_eq!(ops.operations, vec![retire("mesh1.admin.2")], "the admin primary stays");
    }

    #[test]
    fn spawn_delete_restart_and_replace_each_plan_one_shape() {
        let t = mn();
        let add = plan(&BuildIntent::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode, target: None }, &t).unwrap();
        assert_eq!(add.operations, vec![create("mesh1.rpc.4")]);
        let del = plan(&BuildIntent::RemoveNode { node: p("mesh1.rpc.2"), incarnation: None }, &t).unwrap();
        assert_eq!(del.operations, vec![retire("mesh1.rpc.2")]);
        let rs = plan(&BuildIntent::RestartNode { node: p("mesh1.rpc.2"), from_incarnation: None }, &t).unwrap();
        assert_eq!(rs.operations, vec![BuildOperation::RestartNode { node: p("mesh1.rpc.2") }]);
        let rp = plan(&BuildIntent::ReplaceNode { node: p("mesh1.rpc.2") }, &t).unwrap();
        assert_eq!(rp.operations, vec![retire("mesh1.rpc.2"), create("mesh1.rpc.2")]);
    }

    #[test]
    fn mesh_create_and_delete_plan_and_desired_removal_retires_a_mesh() {
        let t = mn();
        let c = plan(&BuildIntent::CreateMesh { desired: MeshDesired::of("mesh2".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 1), (rafka_mesh_entity::NodeKind::RpcNode, 1)]) }, &t).unwrap();
        assert_eq!(c.operations, vec![BuildOperation::CreateMesh { mesh: "mesh2".into() }, create("mesh2.admin.1"), create("mesh2.rpc.1")]);
        let mut two = mn();
        two.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
        let d = plan(&BuildIntent::RemoveMesh { mesh: "mesh2".into() }, &two).unwrap();
        assert_eq!(d.operations, vec![BuildOperation::RetireMesh { mesh: "mesh2".into() }]);
        // replacement: {mesh1, mesh2} -> {mesh2, mesh3}
        let r = plan(&desired(&[("mesh2", 0 + 1, 0), ("mesh3", 1, 0)]), &two).unwrap();
        assert_eq!(
            r.operations,
            vec![
                create("mesh2.admin.1"),
                BuildOperation::CreateMesh { mesh: "mesh3".into() },
                create("mesh3.admin.1"),
                BuildOperation::RetireMesh { mesh: "mesh1".into() },
            ]
        );
    }

    #[test]
    fn every_mutation_kind_plans_deterministically() {
        let t = mn();
        let intents = [
            desired(&[("mesh1", 3, 7), ("mesh2", 2, 3)]),
            BuildIntent::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 4)]) },
            BuildIntent::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::NodeAdmin, target: None },
            BuildIntent::RemoveNode { node: p("mesh1.rpc.3"), incarnation: None },
            BuildIntent::RestartNode { node: p("mesh1.rpc.1"), from_incarnation: None },
            BuildIntent::ReplaceNode { node: p("mesh1.rpc.1") },
            BuildIntent::CreateMesh { desired: MeshDesired::of("mesh9".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 1), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) },
        ];
        for i in &intents {
            let a = plan(i, &t).unwrap();
            for _ in 0..5 {
                assert_eq!(plan(i, &t).unwrap(), a, "{i:?}");
            }
            let keys: BTreeSet<String> = a.operations.iter().map(BuildOperation::key).collect();
            assert_eq!(keys.len(), a.operations.len(), "operation keys are unique: {i:?}");
        }
        // Mesh order in the request does not change the plan.
        let x = plan(&desired(&[("mesh2", 1, 1), ("mesh3", 1, 1)]), &empty()).unwrap();
        let y = plan(&desired(&[("mesh3", 1, 1), ("mesh2", 1, 1)]), &empty()).unwrap();
        assert_eq!(x, y);
    }

    #[test]
    fn invalid_intents_are_rejected_by_name() {
        let t = mn();
        let cases: Vec<(BuildIntent, BuildReject)> = vec![
            (desired(&[("Mesh1", 1, 1)]), BuildReject::InvalidMeshName { mesh: "Mesh1".into() }),
            (desired(&[("mesh1", 1, 1), ("mesh1", 1, 1)]), BuildReject::DuplicateMesh { mesh: "mesh1".into() }),
            (desired(&[("mesh1", 0, 3)]), BuildReject::MeshWithoutAdmin { mesh: "mesh1".into() }),
            (desired(&[]), BuildReject::EmptyFabric),
            (
                BuildIntent::ReconcileFabric { desired: FabricDesired { fabric: "other".into(), meshes: vec![] } },
                BuildReject::FabricMismatch { requested: "other".into(), fabric: "fabric1".into() },
            ),
            (BuildIntent::AddNode { mesh: "mesh7".into(), node_kind: NodeKind::RpcNode, target: None }, BuildReject::UnknownMesh { mesh: "mesh7".into() }),
            (BuildIntent::RemoveNode { node: p("mesh1.rpc.9"), incarnation: None }, BuildReject::UnknownNode { node: "mesh1.rpc.9".into() }),
            (BuildIntent::RestartNode { node: p("mesh1.rpc.9"), from_incarnation: None }, BuildReject::UnknownNode { node: "mesh1.rpc.9".into() }),
            (
                BuildIntent::CreateMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 1), (rafka_mesh_entity::NodeKind::RpcNode, 0)]) },
                BuildReject::MeshAlreadyExists { mesh: "mesh1".into() },
            ),
            (BuildIntent::RemoveMesh { mesh: "mesh1".into() }, BuildReject::EmptyFabric),
        ];
        for (intent, want) in cases {
            assert_eq!(plan(&intent, &t), Err(want.clone()), "{intent:?}");
            assert!(!want.reason().is_empty());
        }
        let mut lone = mn();
        lone.nodes.remove(1);
        assert_eq!(
            plan(&BuildIntent::RemoveNode { node: p("mesh1.admin.1"), incarnation: None }, &lone),
            Err(BuildReject::WouldLeaveMeshWithoutAdmin { mesh: "mesh1".into() })
        );
        let mut dead = mn();
        dead.nodes[3].status = NodeStatus::PendingReconnect;
        assert_eq!(plan(&BuildIntent::RemoveNode { node: p("mesh1.rpc.2"), incarnation: None }, &dead), Err(BuildReject::NodeNotLive { node: "mesh1.rpc.2".into() }));
    }

    #[test]
    fn intents_serialize_with_a_kind_tag() {
        let v = serde_json::to_value(BuildIntent::RestartNode { node: p("mesh1.rpc.2"), from_incarnation: None }).unwrap();
        assert_eq!(v["kind"], "restart_node");
        assert_eq!(v["node"], "mesh1.rpc.2");
        assert!(BuildId::mint().0.starts_with("bld-"));
    }
}
