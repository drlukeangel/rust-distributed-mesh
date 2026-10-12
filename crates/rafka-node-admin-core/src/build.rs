//! Build: the vocabulary of the only topology mutation engine (PRD §1.2–3, §7;
//! mesh-control-plane.md §1): the Build's id, the counts a request may state, the idempotent
//! operations a plan is made of, and the refusals by name. The accepted topology and its planner
//! are [`crate::accepted`]: a submitted change compiles once into the complete topology a Build
//! carries, and every attempt plans `topology − observed` from it, so an operation already
//! satisfied is not replayed and a re-run of an operation is the same operation.

use crate::model::{IncarnationId, NodeId, NodeKind, PathName};
use crate::topology::Topology;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Opaque Build identity. It belongs to fabric control state, not to the
/// executor that happens to run it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BuildId(pub String);

impl BuildId {
    /// A fresh Build id.
    pub fn mint() -> Self {
        Self(format!("bld-{}", hex::encode(rand::random::<[u8; 12]>())))
    }
}

impl fmt::Display for BuildId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The desired counts of one mesh's nodes by kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeshDesired {
    /// The mesh's name.
    pub name: String,
    /// The number of node-admins.
    pub node_admin: u32,
    /// The number of rpc nodes.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub rpc_node: u32,
    /// The number of brokers.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub broker: u32,
    /// The number of gateways.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub gateway: u32,
    /// The number of compute nodes.
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

    /// The desired count of `kind`.
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

    /// The desired count of `kind`, mutably.
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

/// The desired shape of the whole fabric.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FabricDesired {
    /// The fabric's name.
    pub fabric: String,
    /// The desired meshes.
    pub meshes: Vec<MeshDesired>,
}

/// One idempotent operation. Its key is a stable logical identity, so a
/// re-plan never duplicates an operation that observed state already satisfies.
/// One idempotent operation of a Build plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum BuildOperation {
    /// Create a mesh: claim its name for its id, the mesh's first durable write.
    CreateMesh {
        /// The mesh to create.
        mesh: String,
        /// The mesh's id, minted once when the mesh entered the accepted topology
        /// ([`crate::accepted::MeshTopology::mesh_id`]): every run of the operation, by any executor
        /// and any attempt, claims the mesh's name with it, so a re-run finds the mesh's own row.
        mesh_id: rafka_mesh_entity::MeshId,
    },
    /// Create a node at `node` (node-create.md).
    CreateNode {
        /// The node to create.
        node: PathName,
        /// Node recovery's create (node-recovery.md sequence step 4): the exited birth whose data
        /// dir the new node takes over whole, with the endpoint key it holds. `None`: a fresh data dir.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        over: Option<ExitedBirth>,
    },
    /// Restart a node's exact birth.
    RestartNode {
        /// The node to restart.
        node: PathName,
    },
    /// Replace the exact birth `from_incarnation` of `node` with a new logical node at the same
    /// path (node-replace.md, node-recovery.md): the old birth drains and is renamed
    /// `<path>.old`, the successor is created at the path, the old birth is stopped with exact
    /// terminal proof and its storage handed over, the successor starts and is admitted and
    /// ready, and only then is the old identity deleted. One operation, never a retire followed
    /// by a create.
    ReplaceNode {
        /// The path the successor takes.
        node: PathName,
        /// The birth the replacement is fenced to.
        from_incarnation: IncarnationId,
    },
    /// `node.connections.delete { All }` of the exact birth `from_incarnation` of `node`; it keeps
    /// running (node-connections-delete.md).
    DeleteConnections {
        /// The node.
        node: PathName,
        /// The birth whose connections are deleted.
        from_incarnation: IncarnationId,
    },
    /// Stop the exact birth `from_incarnation` of `node` (node-stop.md): `stop-node`, its node-left
    /// reply and the provider's Exited proof. No implicit drain, no departure.
    StopNode {
        /// The node to stop.
        node: PathName,
        /// The birth stopped.
        from_incarnation: IncarnationId,
    },
    /// Start the parked exact birth `from_incarnation` of `node` (node-start.md): `start-node`,
    /// whose reply is `Started` once the same process rejoined as itself, then the wait for ready.
    StartNode {
        /// The node to start.
        node: PathName,
        /// The birth started.
        from_incarnation: IncarnationId,
    },
    /// Delete the parked exact birth `from_incarnation` a replacement left at `node`, a `.old` path
    /// (node-decommission.md): its stop steps are done; `NodeDeleting`, the provider's exact Exited
    /// proof, `NodeDeleted`, and `Complete` naming the birth. Storage is kept.
    DeleteNode {
        /// The parked node's `.old` path.
        node: PathName,
        /// The birth deleted.
        from_incarnation: IncarnationId,
    },
    /// Remove the logical node through the retire pipeline (node-delete.md): its storage stays.
    RetireNode {
        /// The node to retire.
        node: PathName,
        /// A crashed target (node-delete.md "Crashed target"; node-recovery.md sequence step 3): the
        /// exact exited birth, deleted on its mesh-primary's recorded exit proof. Its stop steps are
        /// already done, and nothing is sent to it. `None`: the birth the view holds at `node`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exited: Option<ExitedBirth>,
    },
    /// node.update of the node at `node` (node-update.md; node-recovery.md sequence step 6): the
    /// caller-owned meta of the exited birth `from` copied onto the new node created at its path.
    UpdateNode {
        /// The node updated: the one created at the exited birth's path.name.
        node: PathName,
        /// The exited birth whose caller-owned meta is copied.
        from: ExitedBirth,
    },
    /// Shut a whole mesh down (mesh-leave.md): the fabric-primary owns the workflow, the mesh's
    /// primary drains and stops every other member, and the fabric-primary stops the final primary
    /// and records the departure. The operation key names the mesh by its minted id.
    ShutdownMesh {
        /// The mesh's name.
        mesh: String,
        /// The mesh's minted id.
        mesh_id: rafka_mesh_entity::MeshId,
    },
}

impl BuildOperation {
    /// The node the operation acts on; `None` for a mesh operation.
    pub fn node(&self) -> Option<&PathName> {
        match self {
            Self::CreateNode { node, .. } | Self::ReplaceNode { node, .. } | Self::RestartNode { node } | Self::DeleteConnections { node, .. } | Self::StopNode { node, .. } | Self::StartNode { node, .. } | Self::UpdateNode { node, .. } | Self::DeleteNode { node, .. } | Self::RetireNode { node, .. } => Some(node),
            Self::CreateMesh { .. } | Self::ShutdownMesh { .. } => None,
        }
    }

    /// The idempotency key.
    pub fn key(&self) -> String {
        match self {
            Self::CreateMesh { mesh, .. } => format!("create-mesh:{mesh}"),
            Self::CreateNode { node, .. } => format!("create-node:{node}"),
            Self::ReplaceNode { node, .. } => format!("replace-node:{node}"),
            Self::RestartNode { node } => format!("restart-node:{node}"),
            Self::DeleteConnections { node, .. } => format!("node.connections.delete:{node}"),
            Self::StopNode { node, .. } => format!("stop-node:{node}"),
            Self::StartNode { node, .. } => format!("start-node:{node}"),
            Self::UpdateNode { node, .. } => format!("update-node:{node}"),
            Self::DeleteNode { node, .. } => format!("delete-node:{node}"),
            Self::RetireNode { node, .. } => format!("retire-node:{node}"),
            Self::ShutdownMesh { mesh_id, .. } => format!("shutdown-mesh:{mesh_id}"),
        }
    }
}

/// The exact exited birth a node recovery acts for: the node.id and incarnation its mesh-primary
/// proved exited (node-recovery.md "Detection and proof").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitedBirth {
    /// The exited birth's node.id.
    pub node_id: NodeId,
    /// The exited birth's incarnation.
    pub incarnation: IncarnationId,
}

/// The ordered operations that realize a Build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildPlan {
    /// The operations, in execution order.
    pub operations: Vec<BuildOperation>,
}

/// A Build refused by name (`rdm.node_admin.build.reject.via-<reason>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "kebab-case")]
pub enum BuildReject {
    /// The mesh name is not a valid mesh name.
    InvalidMeshName {
        /// The name.
        mesh: String,
    },
    /// The Build names the mesh twice.
    DuplicateMesh {
        /// The mesh.
        mesh: String,
    },
    /// The mesh is desired with no node-admin.
    MeshWithoutAdmin {
        /// The mesh.
        mesh: String,
    },
    /// The mesh is not one the fabric holds.
    UnknownMesh {
        /// The mesh.
        mesh: String,
    },
    /// The mesh is held but no member has stated the id it carries, so its shutdown has no
    /// operation key (`shutdown-mesh:<mesh_id>`).
    MeshIdUnknown {
        /// The mesh.
        mesh: String,
    },
    /// The node is not one the fabric holds.
    UnknownNode {
        /// The node.
        node: String,
    },
    /// The node is not live.
    NodeNotLive {
        /// The node.
        node: String,
    },
    /// The change would leave the mesh without a node-admin.
    WouldLeaveMeshWithoutAdmin {
        /// The mesh.
        mesh: String,
    },
    /// The mesh already exists.
    MeshAlreadyExists {
        /// The mesh.
        mesh: String,
    },
    /// The request names another fabric than this one.
    FabricMismatch {
        /// The fabric the request names.
        requested: String,
        /// This fabric.
        fabric: String,
    },
    /// The desired fabric holds no mesh.
    EmptyFabric,
    /// A Build tried to choose a provider; provider is fabric policy (PRD §1.7).
    ProviderInBuild {
        /// The fabric's provider.
        fabric_provider: crate::model::ProviderKind,
    },
    /// A mesh's paths and its per-path meta disagree: a materialized path with no meta, or meta
    /// for a path the mesh does not hold.
    NodeMetaMismatch {
        /// The mesh.
        mesh: String,
        /// The paths with no meta.
        missing: Vec<String>,
        /// The meta for paths the mesh does not hold.
        extra: Vec<String>,
    },
    /// The node belongs to a peer mesh this fabric-primary holds unheard: loss of that mesh's
    /// control-plane visibility fences lifecycle mutation for every node of it (R-D1).
    UnheardMesh {
        /// The node.
        node: String,
        /// The mesh the node belongs to.
        mesh: String,
        /// How long the mesh has gone unheard, in milliseconds.
        unheard_ms: u64,
    },
    /// A node recovery submitted for a node-admin whose mesh holds no other live node-admin: a mesh
    /// that lost its mesh-primary is the fabric-primary's mesh recovery (mesh-recovery.md), never a
    /// node recovery.
    NodeAdminNotNodeRecovered {
        /// The node-admin named.
        node: String,
    },
    /// The Build would drain, stop, restart, replace or retire the fabric-primary while no other
    /// live node-admin of its mesh can take its seats: TakeSeats moves seats inside the holder's
    /// mesh only, and the fabric seat leaves a mesh whose last admin holds it only through the
    /// holder-initiated TakeFabricPrimary inside mesh-leave (node-rpc-envelope.md, "TakeSeats:
    /// Build-driven seat transfer", Boundary).
    LastAdmin {
        /// The fabric-primary the Build would end.
        node: String,
        /// Its mesh.
        mesh: String,
        /// What the Build would do to it (`restart`, `replace`, `drain`, `stop`, `retire`).
        action: String,
    },
    /// The stopped-node garbage collector's delete of a birth that is no longer the one stopped
    /// past the ttl at its admission (fabric-node-lifecycle.md §6.9 Recheck): started, replaced, or
    /// its current stopped period no longer the one the scan aged.
    NodeNotStopped {
        /// The node.
        node: String,
        /// What its admission found instead.
        found: String,
    },
    /// The stopped-node garbage collector's delete of a birth an open Build references
    /// (fabric-node-lifecycle.md §6.9 Selection and Recheck).
    ReferencedByOpenBuild {
        /// The node.
        node: String,
        /// The open Build.
        build_id: String,
    },
    /// The workflow would move a seat or write a caller's row while the initial fabric has not
    /// first reached ready-for-traffic: before state-commit its rows live only in their holders'
    /// scratchpads (states.md, "The initial fabric before ready-for-traffic"; R-PB1, R-PC1).
    FabricNotReady {
        /// The refused workflow and its subject (`node.stop mesh1.rpc.1`, `mesh.delete mesh2`).
        workflow: String,
        /// The fabric's status as this admin holds it.
        status: String,
    },
}

impl BuildReject {
    /// The span reason segment: `via-<this>`.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::InvalidMeshName { .. } => "invalid-mesh-name",
            Self::DuplicateMesh { .. } => "duplicate-mesh",
            Self::MeshWithoutAdmin { .. } => "mesh-without-admin",
            Self::UnknownMesh { .. } => "unknown-mesh",
            Self::MeshIdUnknown { .. } => "mesh-id-unknown",
            Self::UnknownNode { .. } => "unknown-node",
            Self::NodeNotLive { .. } => "node-not-live",
            Self::WouldLeaveMeshWithoutAdmin { .. } => "would-leave-mesh-without-admin",
            Self::MeshAlreadyExists { .. } => "mesh-already-exists",
            Self::FabricMismatch { .. } => "fabric-mismatch",
            Self::EmptyFabric => "empty-fabric",
            Self::ProviderInBuild { .. } => "provider-mismatch",
            Self::NodeMetaMismatch { .. } => "node-meta-mismatch",
            Self::UnheardMesh { .. } => "unheard-mesh",
            Self::NodeAdminNotNodeRecovered { .. } => "node-admin-not-node-recovered",
            Self::LastAdmin { .. } => "last-admin",
            Self::NodeNotStopped { .. } => "node-not-stopped",
            Self::ReferencedByOpenBuild { .. } => "referenced-by-open-build",
            Self::FabricNotReady { .. } => "fabric-not-ready",
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
            Self::MeshIdUnknown { mesh } => write!(f, "mesh `{mesh}` is held, but no member has stated its id: its shutdown cannot be keyed"),
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
            Self::NodeAdminNotNodeRecovered { node } => write!(f, "node `{node}` is a node-admin whose mesh holds no other live node-admin to recover it: a mesh that lost its mesh-primary is the fabric-primary's mesh recovery, never node recovery"),
            Self::UnheardMesh { node, mesh, unheard_ms } => write!(
                f,
                "node `{node}` is in mesh `{mesh}`, which this fabric-primary has not heard for {unheard_ms} ms: a peer mesh held unheard is held as a mesh, and its nodes are not restarted, replaced or deleted from here until it is heard again"
            ),
            Self::NodeNotStopped { node, found } => write!(f, "node `{node}` is not the birth the stopped-node collector selected at its delete's admission: {found}; nothing is deleted from the scan's decision"),
            Self::ReferencedByOpenBuild { node, build_id } => write!(f, "node `{node}`'s stopped birth is referenced by open Build {build_id}: it keeps it, and the stopped-node collector deletes nothing it holds"),
            Self::FabricNotReady { workflow, status } => write!(
                f,
                "{workflow} is refused: the initial fabric has not first reached ready-for-traffic (it is {status}), and before state-commit its rows live only in their holders' scratchpads, so nothing that moves a seat or writes caller state runs"
            ),
            Self::LastAdmin { node, mesh, action } => write!(
                f,
                "node `{node}` holds the fabric-primary seat and no other live node-admin of mesh `{mesh}` can take its seats: a {action} of it is refused; TakeSeats moves seats inside the holder's mesh only, and the fabric seat leaves mesh `{mesh}` only through TakeFabricPrimary inside a mesh-leave"
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

/// The shutdown of the mesh named `mesh`, keyed by the id its members carry; refused by name when
/// no member has stated it.
pub(crate) fn shutdown_mesh(observed: &Topology, mesh: &str) -> Result<BuildOperation, BuildReject> {
    match observed.meshes.iter().find(|m| m.name == mesh).and_then(|m| m.id.clone()) {
        Some(mesh_id) => Ok(BuildOperation::ShutdownMesh { mesh: mesh.to_string(), mesh_id }),
        None => Err(BuildReject::MeshIdUnknown { mesh: mesh.to_string() }),
    }
}
