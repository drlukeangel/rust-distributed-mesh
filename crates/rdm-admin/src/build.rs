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

/// The Build identity (`docs/architecture/build.md` §1): a Crockford id like every other id
/// (R-BI5). It belongs to fabric control state, not to the executor that happens to run it.
pub use rdm_core::BuildId;

/// What a path's entry in the accepted Build names that a Build message does not carry (proposal
/// executable-bindings-at-create.md rule 7; Q166): its executable, tags and config. The fabric-primary,
/// the one admin that reads the Build records (Q227), puts it on each create and config operation it
/// dispatches, so the executor reads no stored record for it. Its storage and placement ride the Build
/// message and are read from the Build the executor holds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntrySpec {
    /// The executable the entry names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<rdm_core::binding::Binding>,
    /// The tag labels the entry names.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub tags: std::collections::BTreeSet<String>,
    /// The application's config the entry names.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub config: std::collections::BTreeMap<String, String>,
}

impl EntrySpec {
    /// The entry `spec` names, as an operation carries it.
    pub fn of(spec: &rdm_core::meta::NodeSpec) -> Self {
        Self { executable: spec.executable.clone(), tags: spec.tags.clone(), config: spec.config.clone() }
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
        mesh_id: rdm_core::MeshId,
    },
    /// Create a node at `node` (node-create.md).
    CreateNode {
        /// The node to create.
        node: PathName,
        /// Node recovery's create (node-recovery.md sequence step 4): the exited birth whose data
        /// dir the new node takes over whole, with the endpoint key it holds. `None`: a fresh data dir.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        over: Option<ExitedBirth>,
        /// The executable binding the Build fixes for the launch, as a Build's Create step carries it
        /// (node-rpc-envelope.md "Build schema"): a topology create and a replacement's create carry
        /// the path's entry's (build-deployment.md §2), so the launch never takes the binding a
        /// Deleted row still records. `None`: the path's entry in the stored Build, else the row's
        /// (a recovery's, node-recovery.md "Retained"); a create nothing names one for is refused by
        /// name.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        executable: Option<rdm_core::binding::Binding>,
        /// The path's entry in the accepted Build, as the fabric-primary that dispatched the run holds
        /// it ([`EntrySpec`]): `None` on an operation planned and never dispatched.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        entry: Option<EntrySpec>,
    },
    /// `node.connection.delete { All }` of the exact birth `from_incarnation` of `node`; it keeps
    /// running (node-connections-delete.md).
    DeleteConnections {
        /// The node.
        node: PathName,
        /// The node id of the birth the gates pinned (build.md §4.4).
        node_id: NodeId,
        /// The birth whose connections are deleted.
        from_incarnation: IncarnationId,
    },
    /// Stop the exact birth `from_incarnation` of `node` (node-stop.md): `stop-node`, its node-left
    /// reply and the provider's Exited proof. No implicit connections delete, no departure.
    StopNode {
        /// The node to stop.
        node: PathName,
        /// The node id of the birth the gates pinned (build.md §4.4).
        node_id: NodeId,
        /// The birth stopped.
        from_incarnation: IncarnationId,
    },
    /// Start the stopped exact birth `from_incarnation` of `node` (node-start.md): `start-node`,
    /// whose reply is `Started` once the same process rejoined as itself, then the wait for ready.
    StartNode {
        /// The node to start.
        node: PathName,
        /// The node id of the birth the gates pinned (build.md §4.4).
        node_id: NodeId,
        /// The birth started.
        from_incarnation: IncarnationId,
    },
    /// node.delete of the stopped exact birth `from_incarnation` at `node` (node-delete.md): its stop
    /// steps are done; `NodeDeleting`, the provider's exact Exited proof, `NodeDeleted`, and `Complete`
    /// naming the birth. Storage is kept. Its key is node.delete's one key, `node.delete:<path>`.
    DeleteNode {
        /// The stopped node's `.old` path.
        node: PathName,
        /// The node id of the birth the gates pinned (build.md §4.4).
        node_id: NodeId,
        /// The birth deleted.
        from_incarnation: IncarnationId,
    },
    /// The live target's delete (node-delete.md "Live target"): `node.stop` completes first, then the delete; its storage stays.
    #[serde(rename = "retire_node")] // rdm#88: stored tag
    DeleteLiveNode {
        /// The node to delete.
        node: PathName,
        /// A crashed target (node-delete.md "Crashed target"; node-recovery.md sequence step 3): the
        /// exact exited birth, deleted on its mesh-primary's recorded exited verdict. Its stop steps are
        /// already done, and nothing is sent to it. `None`: the live or stopped `birth`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exited: Option<ExitedBirth>,
        /// The live or stopped birth the gates pinned at acceptance (build.md §4.4): the delete acts on
        /// it alone, never on the birth the view holds at `node`. `None` with `exited`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        birth: Option<PinnedBirth>,
    },
    /// One step of a Build (`BuildChange::Steps`; build.md §1, lifecycles/node-replace.md): its op's
    /// own workflow on the node its path names when it runs, `create-node`, `stop-node`,
    /// `update-node` (node.update's own core at the node's mesh-primary), `start-node`,
    /// `retire-node` (node.delete), node.connection.delete `{ All }` or node.mesh.leave. `birth`
    /// targets it to the exact birth its node was when the Build was accepted; `None` for a node an
    /// earlier step of the Build created, which the executor resolves at its path when the step runs.
    Step {
        /// The step.
        step: rdm_core::build::Step,
        /// The exact birth the step is targeted to, for a node the Build did not create.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        birth: Option<PinnedBirth>,
        /// A Create step's path's entry in the accepted Build, as the fabric-primary that dispatched
        /// the run holds it ([`EntrySpec`]).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        entry: Option<EntrySpec>,
    },
    /// Shut a whole mesh down (mesh-delete.md): the fabric-primary owns the workflow, the mesh's
    /// primary deletes its connections and stops every other member, and the fabric-primary stops the final primary
    /// and records the departure. The operation key names the mesh by its minted id.
    ShutdownMesh {
        /// The mesh's name.
        mesh: String,
        /// The mesh's minted id.
        mesh_id: rdm_core::MeshId,
    },
    /// Release the fabric name and record `fabric.deleted` after every mesh's `mesh.delete` and
    /// every other mesh's final primary's `node.delete` have completed (fabric-delete.md step 3).
    DeleteFabric {
        /// The fabric's minted id.
        fabric_id: rdm_core::FabricId,
    },
    /// `TerminateRuntime` of a deleted birth heard again, on its exact runtime (Q128).
    TerminateNode {
        /// The path.name the birth held.
        node: PathName,
        /// The birth's node.id.
        node_id: rdm_core::NodeId,
        /// The birth.
        from_incarnation: rdm_core::IncarnationId,
    },
    /// The node's create spec names another `config`, `storage` or `placement` than its row records
    /// (build.md §1, "The Build applies the diff"): `node.config.update` of the entry's map at the
    /// node's mesh-primary, row first, then the node (a live one sent `ApplyNodeConfig`, a stopped one
    /// handed it at its next join), the entry's storage and placement written onto the row with it. No
    /// re-birth: the fabric stays where it is. Appended, so every earlier variant keeps its name.
    UpdateConfig {
        /// The node.
        node: PathName,
        /// The node's entry in the accepted Build, as the fabric-primary that dispatched the run holds
        /// it ([`EntrySpec`]): the config the row is written with.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        entry: Option<EntrySpec>,
    },
    /// The node's create spec names tag labels its row does not carry: `node.tag.create` of them at
    /// the node's mesh-primary, the node's tag rows (build.md §1).
    CreateTags {
        /// The node.
        node: PathName,
        /// The labels the row gains.
        labels: std::collections::BTreeSet<String>,
    },
    /// The node's row carries tag labels its create spec does not name: `node.tag.delete` of them at
    /// the node's mesh-primary (build.md §1).
    DeleteTags {
        /// The node.
        node: PathName,
        /// The labels the row loses.
        labels: std::collections::BTreeSet<String>,
    },
    /// A storage change's block of `mesh` (storage.md §2 "Changing it", steps 1 and 2), run by the
    /// mesh's mesh-primary: every other node of the mesh blocks ([`crate::storage_change`]). Its key is
    /// `storage.block:<change_id>:<mesh>`. Appended, so every earlier variant keeps its name.
    BlockStorage {
        /// The mesh.
        mesh: String,
        /// The change.
        change: rdm_core::build::StorageChange,
    },
    /// A storage change's copy and switch (steps 3 and 4), run by the fabric-primary. Its key is
    /// `storage.switch:<change_id>`.
    SwitchStorage {
        /// The change.
        change: rdm_core::build::StorageChange,
    },
    /// A storage change's unblock of `mesh` (step 5), run by the mesh's mesh-primary. Its key is
    /// `storage.unblock:<change_id>:<mesh>`.
    UnblockStorage {
        /// The mesh.
        mesh: String,
        /// The change.
        change: rdm_core::build::StorageChange,
    },
    /// A storage change's verdict, the fabric-primary's once every mesh unblocked: the Build fails when
    /// the change was not switched. Its key is `storage.change:<change_id>`.
    FinishStorage {
        /// The change.
        change: rdm_core::build::StorageChange,
    },
}

impl BuildOperation {
    /// The node the operation acts on; `None` for a mesh operation.
    pub fn node(&self) -> Option<&PathName> {
        match self {
            Self::CreateNode { node, .. } | Self::DeleteConnections { node, .. } | Self::StopNode { node, .. } | Self::StartNode { node, .. } | Self::DeleteNode { node, .. } | Self::DeleteLiveNode { node, .. } => Some(node),
            Self::Step { step, .. } => Some(step.node()),
            Self::TerminateNode { node, .. } | Self::UpdateConfig { node, .. } | Self::CreateTags { node, .. } | Self::DeleteTags { node, .. } => Some(node),
            Self::CreateMesh { .. } | Self::ShutdownMesh { .. } | Self::DeleteFabric { .. } | Self::BlockStorage { .. } | Self::SwitchStorage { .. } | Self::UnblockStorage { .. } | Self::FinishStorage { .. } => None,
        }
    }

    /// The node a create operation makes (`create-node`, or a Build's Create step); `None` for any
    /// other operation.
    pub fn created_node(&self) -> Option<&PathName> {
        match self {
            Self::CreateNode { node, .. } | Self::Step { step: rdm_core::build::Step::Create { node, .. }, .. } => Some(node),
            _ => None,
        }
    }

    /// The idempotency key.
    pub fn key(&self) -> String {
        match self {
            Self::CreateMesh { mesh, .. } => format!("create-mesh:{mesh}"),
            Self::CreateNode { node, .. } => format!("node.create:{node}"),
            Self::DeleteConnections { node, .. } => format!("node.connection.delete:{node}"),
            Self::StopNode { node, .. } => format!("node.stop:{node}"),
            Self::StartNode { node, .. } => format!("node.start:{node}"),
            Self::Step { step, .. } => step_key(step),
            // node.delete has one key, whatever the node's name and whatever state the birth is
            // deleted from (a stopped birth's delete runs no stop steps, a live one's stops it first).
            Self::DeleteNode { node, .. } | Self::DeleteLiveNode { node, .. } => format!("node.delete:{node}"),
            Self::ShutdownMesh { mesh_id, .. } => format!("mesh.delete:{mesh_id}"),
            Self::DeleteFabric { fabric_id } => format!("fabric.delete:{fabric_id}"),
            // One key per birth: a birth heard again after another termination is a new run of it.
            Self::TerminateNode { node, from_incarnation, .. } => format!("node.terminate:{node}@{}", from_incarnation.as_str()),
            Self::UpdateConfig { node, .. } => format!("node.config.update:{node}"),
            Self::CreateTags { node, .. } => format!("node.tag.create:{node}"),
            Self::DeleteTags { node, .. } => format!("node.tag.delete:{node}"),
            Self::BlockStorage { mesh, change } => crate::storage_change::block_operation(&change.change_id, mesh),
            Self::SwitchStorage { change } => crate::storage_change::switch_operation(&change.change_id),
            Self::UnblockStorage { mesh, change } => crate::storage_change::unblock_operation(&change.change_id, mesh),
            Self::FinishStorage { change } => crate::storage_change::operation(&change.change_id),
        }
    }
}

/// The operation key of a step: its op's own key, the one that op's workflow receipts under.
/// One op has one key, whatever the node's name: node.delete's is `node.delete:<path>`.
pub fn step_key(step: &rdm_core::build::Step) -> String {
    use rdm_core::build::Step as S;
    match step {
        S::Create { node, .. } => format!("node.create:{node}"),
        S::Stop { node } => format!("node.stop:{node}"),
        S::Update { node, .. } => format!("node.update:{node}"),
        S::Start { node } => format!("node.start:{node}"),
        S::Delete { node } => format!("node.delete:{node}"),
        S::DeleteConnections { node } => format!("node.connection.delete:{node}"),
        S::LeaveMesh { node } => format!("node.mesh.leave:{node}"),
    }
}

/// The exact birth an operation on an existing birth carries (build.md §4.4): the node id and incarnation
/// the gates pinned at acceptance, which the executing mesh-primary verifies before any effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedBirth {
    /// The node.id.
    pub node_id: NodeId,
    /// The incarnation.
    pub incarnation: IncarnationId,
}

impl PinnedBirth {
    /// The birth `n` is, when it holds an incarnation.
    pub fn of(n: &crate::model::Node) -> Option<Self> {
        n.incarnation_id.clone().map(|incarnation| Self { node_id: n.node_id.clone(), incarnation })
    }

    /// The executing mesh-primary's check before any effect of `operation` on `node` (build.md §4.4:
    /// "The mesh-primary that runs it verifies that birth again against its own view at execution"):
    /// the birth `view` holds that this pin names, which is the only birth the operation acts on.
    /// `Ok(None)`: the view holds neither this birth nor any birth at `node`.
    pub fn held_in(&self, view: &Topology, operation: &str, node: &PathName) -> Result<Option<crate::model::Node>, BirthReplaced> {
        // The birth by its node id, wherever the view lists it: a birth renamed live is listed at the
        // name it last published (node-replace.md steps 1-2). The same node id under another
        // incarnation, or another node id at the path, is another birth.
        let refused = |n: &crate::model::Node| {
            let refusal = BirthReplaced { operation: operation.to_string(), node: node.clone(), expected: self.clone(), observed: Self::of(n) };
            tracing::info_span!(
                "rdm.node_admin.node.reject.via-birth-replaced",
                operation = %refusal.operation,
                node = %refusal.node,
                node_id = %refusal.expected.node_id,
                incarnation_id = %refusal.expected.incarnation.as_str(),
                observed_node_id = %n.node_id,
                observed_incarnation_id = %n.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or(""),
            )
            .in_scope(|| tracing::info!("the birth the operation was pinned to is not the birth this view holds: nothing is done to it"));
            refusal
        };
        if let Some(n) = view.births().find(|n| n.node_id == self.node_id) {
            return match n.incarnation_id.as_ref() == Some(&self.incarnation) {
                true => Ok(Some(n.clone())),
                false => Err(refused(n)),
            };
        }
        match view.node(node) {
            Some(n) => Err(refused(n)),
            None => Ok(None),
        }
    }

    /// Whether `n` is this birth: its node id and its incarnation.
    pub fn is(&self, n: &crate::model::Node) -> bool {
        n.node_id == self.node_id && n.incarnation_id.as_ref() == Some(&self.incarnation)
    }
}

/// What gate 5 admits a node.delete of (build.md §4.4; node-delete.md; Q219, Q220).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteAdmitted {
    /// A completed node.stop of the birth: the delete composes no stop.
    Stopped,
    /// Live and heard: its own sequence stops it first (node.delete Live target).
    Live,
    /// Node investigation's `exited` verdict on its exact runtime (node-delete.md Crashed target):
    /// nothing is sent to it, and its runtime is terminated.
    Exited,
}

impl DeleteAdmitted {
    /// The name the gate's spans carry.
    pub fn name(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Live => "live",
            Self::Exited => "exited",
        }
    }
}

/// Whether gate 5 needs node investigation's verdict to decide a delete: the birth is neither stopped
/// nor heard live (`heard_dead`: the view's Dead mark, which is silence and never says the runtime
/// exited; node-delete.md Never evidence).
pub fn delete_needs_verdict(stopped: bool, live: bool, heard_dead: bool) -> bool {
    !stopped && !(live && !heard_dead)
}

/// Gate 5 for a node.delete, the one function every path reads (build.md §4.4: "a delete a stopped
/// one, one whose exit is proven, or a live one, which its own sequence stops first"; Q219, Q220): a
/// stopped birth, a live one the view does not mark Dead, or one whose exact runtime node investigation
/// reads exited (`verdict`, asked only when [`delete_needs_verdict`]). A Dead birth with neither fact
/// is admitted by nothing: `None`, refused `node-not-live`.
pub fn delete_admits(stopped: bool, live: bool, heard_dead: bool, verdict: Option<&crate::node_investigation::Finding>) -> Option<DeleteAdmitted> {
    if stopped {
        return Some(DeleteAdmitted::Stopped);
    }
    if live && !heard_dead {
        return Some(DeleteAdmitted::Live);
    }
    matches!(verdict, Some(crate::node_investigation::Finding::Exited { .. })).then_some(DeleteAdmitted::Exited)
}

/// The operation a Build step's node.delete of `node` runs (node-delete.md), given the pinned `birth`
/// and the node the executing view holds at the path (`held`, that birth or `None`): the delete of a
/// stopped birth, which composes no stop, or the delete that stops any other first: a Dead mark alone
/// never skips the stop (Q219); the delete asks node investigation of a Dead birth and runs as a crashed
/// target's only on its `exited` verdict. A birth this view no longer holds (a resumed delete whose NodeDeleting it already heard) is deleted pinned to its exact
/// birth, its runtime read from its row.
pub fn step_delete(node: &PathName, birth: &PinnedBirth, held: Option<&crate::model::Node>) -> BuildOperation {
    match held.filter(|n| birth.is(n)) {
        Some(n) if !n.stopped => BuildOperation::DeleteLiveNode { node: node.clone(), exited: None, birth: Some(birth.clone()) },
        _ => BuildOperation::DeleteNode { node: node.clone(), node_id: birth.node_id.clone(), from_incarnation: birth.incarnation.clone() },
    }
}

/// `birth-replaced`: the birth an operation was pinned to at acceptance is not the birth the executing
/// mesh-primary's view holds (build.md §4.4). Nothing is done to the birth it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BirthReplaced {
    /// The operation's key.
    pub operation: String,
    /// The path the operation acts on.
    pub node: PathName,
    /// The birth the gates pinned.
    pub expected: PinnedBirth,
    /// The birth the view holds instead, `None` when it holds one with no incarnation.
    pub observed: Option<PinnedBirth>,
}

impl BirthReplaced {
    /// The refusal's name.
    pub fn reason(&self) -> &'static str {
        "birth-replaced"
    }
}

impl fmt::Display for BirthReplaced {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let observed = self.observed.as_ref().map(|b| format!("birth {} at incarnation {}", b.node_id, b.incarnation.as_str())).unwrap_or_else(|| "a birth with no incarnation".into());
        write!(
            f,
            "birth-replaced: {} acts on {} pinned to birth {} at incarnation {}, and this view holds {observed} there; nothing is done to it",
            self.operation,
            self.node,
            self.expected.node_id,
            self.expected.incarnation.as_str()
        )
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
    /// operation key (`mesh.delete:<mesh_id>`).
    MeshIdUnknown {
        /// The mesh.
        mesh: String,
    },
    /// The node is not one the fabric holds (build.md §4.4 gate 4): for a steps Build, a step acting on
    /// a `path.name` that holds no birth when the Build is accepted and that no earlier step of the
    /// Build creates or renames a node to, the step named (Q223).
    UnknownNode {
        /// The node.
        node: String,
        /// The step of a steps Build that names it, numbered from 1.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        step: Option<u32>,
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
    /// A mesh's paths and its create specs disagree: a materialized path with no create spec, or a
    /// create spec for a path the mesh does not hold.
    NodeSpecMismatch {
        /// The mesh.
        mesh: String,
        /// The paths with no create spec.
        missing: Vec<String>,
        /// The create specs of paths the mesh does not hold.
        extra: Vec<String>,
    },
    /// The node belongs to a peer mesh this fabric-primary holds unheard: loss of that mesh's
    /// control-plane visibility targets lifecycle mutation for every node of it (R-D1).
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
    /// The Build would stop, delete the connections of or delete a seat holder (its
    /// mesh's primary, or the fabric-primary) whose node.primary.update has no successor (the approved proposal
    /// node-primary-update-fabric-mesh-and-node-type.md, Part C1 "The no-successor cases", Q7 ruled
    /// Luke 2026-10-12): refused by name with Q7's exact text, the holder stays primary and nothing is
    /// admitted.
    LastAdmin {
        /// The seat holder the Build would end.
        node: String,
        /// Its mesh.
        mesh: String,
        /// What the Build would do to it (`node.connection.delete`, `stop`, `delete`).
        action: String,
        /// Q7's exact text: `cannot transfer: no secondary of type <node_type> in mesh <mesh>`, or the
        /// fabric seat's `cannot transfer fabric-primary: ...` naming both searches.
        refusal: String,
    },
    /// The Build would stop, delete the connections of or delete the holder of its
    /// cohort's seat, a node type other than node-admin, while no other node of its type is in its mesh
    /// (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 "The
    /// no-successor cases", Q7 ruled Luke 2026-10-12: "a mesh's sole broker ... cannot be stopped or
    /// restarted until a secondary is Ready"): refused by name with Q7's exact text, the holder
    /// stays primary and nothing is admitted.
    NoSecondary {
        /// The seat holder the Build would end.
        node: String,
        /// Its mesh.
        mesh: String,
        /// What the Build would do to it (`node.connection.delete`, `stop`, `delete`,
        /// `delete`).
        action: String,
        /// Q7's exact text: `cannot transfer: no secondary of type <node_type> in mesh <mesh>`.
        refusal: String,
    },
    /// The stopped-node garbage collector's delete of a birth that is no longer the one stopped
    /// past the ttl at its admission (node-identity-and-state.md §6.9 Recheck): started, replaced, or
    /// its current stopped period no longer the one the scan aged.
    NodeNotStopped {
        /// The node.
        node: String,
        /// What its admission found instead.
        found: String,
    },
    /// The stopped-node garbage collector's delete of a birth an open Build references
    /// (node-identity-and-state.md §6.9 Selection and Recheck).
    ReferencedByOpenBuild {
        /// The node.
        node: String,
        /// The open Build.
        build_id: String,
    },
    /// A Build with no step: a Build of nothing.
    EmptySteps,
    /// A Build naming one operation twice: a step's receipts are keyed by its operation
    /// (`<op>:<path.name>`), so two steps of one key cannot be told apart and a resume could not say
    /// which one completed.
    StepRepeated {
        /// The operation named twice.
        operation: String,
    },
    /// A complete-topology request (`Reconcile`) built from a read of a Build that is no longer the
    /// current one (proposal executable-bindings-at-create.md rule 1, optimistic concurrency): nothing
    /// is accepted, and the caller re-reads.
    BasedOnNotCurrent {
        /// The Build the request names.
        based_on: String,
        /// The current Build.
        current: String,
    },
    /// A complete-topology request that lists no node: a fabric with no mesh is fabric.delete, never a
    /// request read as "delete everything".
    NoNodeListed,
    /// A complete-topology request naming a `topology_storage_provider` the fabric-primary's wiring
    /// does not register (storage.md §2 "Changing it": "Acceptance refuses by name a provider the
    /// fabric-primary's wiring does not register"): nothing is accepted and nothing moved.
    TopologyStorageProviderNotRegistered {
        /// The provider the request names.
        provider: String,
        /// The providers the fabric-primary's wiring registers.
        registered: Vec<String>,
    },
    /// A complete-topology request naming a `topology_storage_provider` the fabric-primary's wiring
    /// registers and cannot open (storage.md §2 "Changing it": "Acceptance refuses by name a provider the
    /// fabric-primary's wiring does not register or cannot open"): nothing is accepted and nothing moved.
    TopologyStorageProviderNotOpened {
        /// The provider the request names.
        provider: String,
        /// Why it does not open.
        detail: String,
    },
    /// A complete-topology request naming one `path.name` twice.
    NodeNamedTwice {
        /// The `path.name` named twice.
        node: String,
    },
    /// A path the request creates whose config names no executable: nothing runs (proposal
    /// executable-bindings-at-create.md rule 1; no stored default).
    ExecutableMissing {
        /// The path created.
        node: String,
    },
    /// An executable a request names for a path, refused at acceptance before anything is accepted:
    /// its file missing, its hash another, its launch id not the path's kind, or its image not the
    /// fabric provider's.
    ExecutableRefused {
        /// The path.
        node: String,
        /// Why.
        detail: String,
    },
    /// The tag labels a request names for a path, refused at acceptance before anything is accepted:
    /// more than a node carries, or one outside the label grammar (node-rpc-envelope.md node.tags).
    TagsRefused {
        /// The path.
        node: String,
        /// Why.
        detail: String,
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
            Self::NodeSpecMismatch { .. } => "node-spec-mismatch",
            Self::UnheardMesh { .. } => "unheard-mesh",
            Self::NodeAdminNotNodeRecovered { .. } => "node-admin-not-node-recovered",
            Self::LastAdmin { .. } => "last-admin",
            Self::NoSecondary { .. } => "no-secondary",
            Self::NodeNotStopped { .. } => "node-not-stopped",
            Self::ReferencedByOpenBuild { .. } => "referenced-by-open-build",
            Self::EmptySteps => "empty-steps",
            Self::StepRepeated { .. } => "step-repeated",
            Self::BasedOnNotCurrent { .. } => "based-on-not-current",
            Self::NoNodeListed => "no-node-listed",
            Self::TopologyStorageProviderNotRegistered { .. } => "topology-storage-provider-not-registered",
            Self::TopologyStorageProviderNotOpened { .. } => "topology-storage-provider-not-opened",
            Self::NodeNamedTwice { .. } => "node-named-twice",
            Self::ExecutableMissing { .. } => "executable-missing",
            Self::ExecutableRefused { .. } => "executable-refused",
            Self::TagsRefused { .. } => "tags-refused",
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
            Self::UnknownNode { node, step: None } => write!(f, "node `{node}` is not in the fabric"),
            Self::UnknownNode { node, step: Some(step) } => write!(f, "step {step} of the Build acts on `{node}`, which holds no birth now and which no earlier step creates or renames a node to"),
            Self::NodeNotLive { node } => write!(f, "node `{node}` is not live"),
            Self::WouldLeaveMeshWithoutAdmin { mesh } => write!(f, "mesh `{mesh}` would be left without a node-admin in the accepted topology"),
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
            Self::EmptySteps => write!(f, "a Build needs at least one step"),
            Self::StepRepeated { operation } => write!(f, "the Build names operation `{operation}` twice: each step's receipts are keyed by its operation, so a Build names each operation once"),
            Self::LastAdmin { refusal, .. } | Self::NoSecondary { refusal, .. } => f.write_str(refusal),
            Self::BasedOnNotCurrent { based_on, current } => write!(f, "the request is based on Build {based_on}, and the current Build is {current}: nothing is accepted; read the topology again and submit from it"),
            Self::NoNodeListed => write!(f, "the request lists no node: a fabric with no mesh is fabric.delete, never a topology request; nothing is accepted"),
            Self::TopologyStorageProviderNotRegistered { provider, registered } => write!(f, "the request names topology_storage_provider `{provider}`, which the fabric-primary's wiring does not register (it registers: {}): nothing is accepted and nothing moved", registered.join(", ")),
            Self::TopologyStorageProviderNotOpened { provider, detail } => write!(f, "the request names topology_storage_provider `{provider}`, which the fabric-primary's wiring registers and cannot open ({detail}): nothing is accepted and nothing moved"),
            Self::NodeNamedTwice { node } => write!(f, "the request names `{node}` twice: each node is listed once"),
            Self::ExecutableMissing { node } => write!(f, "`{node}` is created by the request and its create spec names no executable: every created node names the executable it runs; nothing is accepted"),
            Self::ExecutableRefused { node, detail } => write!(f, "the executable the request names for `{node}` is refused: {detail}; nothing is accepted"),
            Self::TagsRefused { node, detail } => write!(f, "the tags the request names for `{node}` are refused: {detail}; nothing is accepted"),
            Self::NodeSpecMismatch { mesh, missing, extra } => write!(
                f,
                "mesh `{mesh}`: every materialized path carries exactly one create spec; missing [{}], without a node [{}]",
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

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::build::Step;

    /// CONTRACT: the node a create operation makes is named for `create-node` and for a Build's Create
    /// step alike, and for no other operation: the drive's target on a fully crashed mesh admits a create
    /// of its first node-admin and nothing else.
    #[test]
    fn only_a_create_operation_names_the_node_it_creates() {
        let node: PathName = "mesh2.admin.1".parse().unwrap();
        assert_eq!(BuildOperation::CreateNode { node: node.clone(), over: None, executable: None, entry: None }.created_node(), Some(&node));
        assert_eq!(BuildOperation::Step { step: Step::Create { node: node.clone(), executable: None }, birth: None, entry: None }.created_node(), Some(&node));
        assert_eq!(BuildOperation::Step { step: Step::Stop { node: node.clone() }, birth: None, entry: None }.created_node(), None);
        assert_eq!(BuildOperation::Step { step: Step::Start { node: node.clone() }, birth: None, entry: None }.created_node(), None);
    }
}
