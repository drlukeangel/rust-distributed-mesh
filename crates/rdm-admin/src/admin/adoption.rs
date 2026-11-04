//! The adoption launch (Q156; fabric-recovery.md "Nothing alive"): no node-admin is alive, the fabric's
//! other nodes alive or not, and a person or a script launches a node-admin at the lost fabric-primary's
//! path and name with no launcher, given the fabric id, its mesh and the store root. It is told from Day 0 by
//! that intent (`RDM_NODE_NAME`, [`AdminConfig::adopt`]), never by what the store holds.
//!
//! In order, each step before the next: what the store holds is read with nothing written ([`read`]: the
//! Fabric record and its provider, the mesh's row, the lost fabric-primary's birth at the path and name, the
//! current Build), and a store that does not hold them refuses the start by name. The adoption is fabric
//! recovery, the primary's own workflow, never a Build or part of one (Q342): nothing on the store orders
//! two adoption launches (no lock): one launch is started at a time, and [`claim`] writes nothing.
//! Then node investigation of the lost birth, and, last before the first mutation, whether any node-admin
//! the store records answers ([`check`]): an answer refuses the launch `NameHeld` ([`fail`]). Otherwise the
//! launch releases the lost birth's name and takes it as itself ([`release_name`]); fabric recovery restores
//! it as fabric-primary (step 4).
//!
//! The launch writes the mesh and node records its own operations require, never as mesh-primary.
//!
//! Under an in-force fabric.delete ("Nothing alive", its delete bullet; Q370) the launch is the same, at the
//! lost fabric-primary's path and name, and the restored fabric-primary continues the delete. With no
//! not-yet-deleted mesh left the launch is refused by name: the remaining work is the dead fabric-primary's
//! own node.delete from outside, and nothing is created.

use super::*;
use crate::build_state::BuildProjection;
use crate::node_investigation::Finding;

/// What the store holds that an adoption launch adopts, read with nothing written.
#[derive(Debug, Clone)]
pub struct Adoptable {
    /// The Fabric record.
    pub fabric: crate::fabric_storage::FabricRecord,
    /// The provider the Fabric record names.
    pub provider: ProviderKind,
    /// The mesh's row.
    pub mesh: crate::storage::MeshRecord,
    /// The lost fabric-primary's birth at the launch's path and name, as its node record records it.
    pub lost: crate::storage::OwnRow,
    /// The lost fabric-primary's node record: the config and tags the launch's own record starts from.
    pub record: crate::storage::NodeRecord,
    /// The fabric.delete in force: the launch, at the lost fabric-primary's path and name, continues it
    /// (Q370). `None` when no fabric.delete is in force.
    pub delete: Option<BuildProjection>,
}

/// The meshes of `meshes` no fabric.delete has deleted: not Dead, and no `MeshDeleted` step of their
/// mesh.delete in `delete` ended `complete`.
fn not_yet_deleted(meshes: &[crate::storage::MeshRecord], delete: &BuildProjection) -> Vec<String> {
    let deleted = |m: &crate::storage::MeshRecord| {
        delete.completed(&crate::mesh_delete::operation(&m.mesh_id).to_string(), crate::mesh_delete::STEP_MESH_DELETED)
    };
    let mut out: Vec<String> = meshes.iter().filter(|m| !m.is_deleted() && !deleted(m)).map(|m| m.name.clone()).collect();
    out.sort();
    out
}

/// The provider's name as `MESH_SPAWN_TYPE` spells it.
fn provider_name(p: ProviderKind) -> &'static str {
    match p {
        ProviderKind::Process => "process",
        ProviderKind::Container => "container",
    }
}

/// What the store root `store` holds for the adoption launch at `name` of Fabric `fabric_id` with
/// `MESH_SPAWN_TYPE` `spawn_type`: the Fabric record, which must name the provider `spawn_type` names
/// when it names one; the live row of `name`'s mesh; the fabric-primary the node rows record, which must
/// be the birth at `name`; and the current Build. Each missing or differing input refuses by name.
#[allow(clippy::too_many_arguments)]
pub async fn read(
    name: &PathName,
    mesh_name: &str,
    fabric_id: &FabricId,
    store: &Path,
    spawn_type: Option<&str>,
    fabric: &dyn crate::fabric_storage::FabricStorage,
    meshes: &dyn crate::storage::MeshStorage,
    nodes: &dyn crate::storage::NodesStorage,
    builds: &dyn BuildStateAdapter,
) -> Result<Adoptable, String> {
    let root = store.join(fabric_id.to_string());
    let of = format!("the adoption launch at path={} name={} of Fabric {fabric_id}", name.path, name.name);
    let record = fabric.fabric().await.map_err(|e| format!("{of}: the Fabric record on the store root {} could not be read: {e}", root.display()))?;
    let Some(record) = record.filter(|r| r.fabric_id == *fabric_id) else {
        return Err(format!("{of}: the store root {} holds no Fabric record of {fabric_id}: an adoption launch adopts a fabric its store root holds and creates none", root.display()));
    };
    let Some(provider) = record.provider else {
        return Err(format!("{of}: the Fabric record on the store root {} names no provider: the adoption launch takes the fabric's provider from it", root.display()));
    };
    match crate::deployment::provider::parse(spawn_type) {
        Ok(Some(given)) if given != provider => {
            return Err(format!(
                "{of}: MESH_SPAWN_TYPE={} and the Fabric record names provider {}: an adoption launch takes the fabric's provider, and a differing MESH_SPAWN_TYPE is refused",
                provider_name(given),
                provider_name(provider)
            ))
        }
        Ok(_) => {}
        Err(e) => return Err(format!("{of}: {e}")),
    }
    let delete = crate::build_state::fabric_delete_in_force(builds).await.map_err(|e| format!("{of}: the Build journal on the store root could not be read for a fabric.delete in force: {e}"))?.map(|(d, _)| d);
    // Under a delete, the launch is in its mesh while that mesh is not yet deleted; with none left, nothing
    // is created (Q159).
    if let Some(d) = &delete {
        let all = meshes.meshes().await.map_err(|e| format!("{of}: mesh.storage could not read the meshes: {e}"))?;
        let left = not_yet_deleted(&all, d);
        if left.is_empty() {
            return Err(format!(
                "{of}: fabric.delete {} is in force and no not-yet-deleted mesh remains: the remaining work is the dead fabric-primary's own node.delete from outside, and nothing is created (Q159)",
                d.build_id
            ));
        }
        if !left.iter().any(|name| name == mesh_name) {
            return Err(format!("{of}: fabric.delete {} is in force and mesh {} is deleted: the adoption launch is created in an existing, not-yet-deleted mesh, one of {} (Q159)", d.build_id, mesh_name, left.join(", ")));
        }
    }
    let mesh = meshes.mesh_named(&mesh_name).await.map_err(|e| format!("{of}: mesh.storage could not read mesh {}: {e}", mesh_name))?;
    let mesh = match mesh {
        Some(m) if !m.is_deleted() => m,
        Some(m) => return Err(format!("{of}: mesh {} ({}) is deleted: a deleted mesh is never adopted", mesh_name, m.mesh_id)),
        None => return Err(format!("{of}: the store root {} holds no row of mesh {}", root.display(), mesh_name)),
    };
    // The fabric-primary the node records record (`is_fabric_primary`, the newest revision): the lost
    // node the launch is at the path and name of.
    let rows = crate::storage::records(meshes, nodes, &mesh_name).await.map_err(|e| format!("{of}: nodes.storage could not read the node records: {e}"))?;
    let recorded = rows.iter().filter(|r| r.state != NodeStatus::Deleted).filter_map(|r| r.is_fabric_primary.map(|rev| (rev, r))).max_by_key(|(rev, _)| *rev);
    let Some((_, row)) = recorded else {
        return Err(format!("{of}: the node rows on the store root {} record no fabric-primary: an adoption launch is at the lost fabric-primary's (path, name)", root.display()));
    };
    // The launch is at the lost fabric-primary's path and name, under an in-force fabric.delete as without
    // one (fabric-recovery.md "Nothing alive", Q370).
    if row.location() != *name {
        return Err(format!("{of}: the node rows record fabric-primary node_id={} at path={} name={}; an adoption launch uses that location", row.node_id, row.path, row.name));
    }
    // Its address is its record's; its mesh's id the live Mesh record's of its mesh, by name.
    let lost = crate::storage::OwnRow {
        mesh: row.mesh(),
        node_id: row.node_id.clone(),
        fabric_id: fabric_id.clone(),
        path: row.path.clone(), name: row.name.clone(),
        mesh_id: mesh.mesh_id.clone(),
        incarnation_id: row.incarnation.clone(),
        endpoint_id: row.endpoint_id.clone(),
        transport_addr: row.transport_addr,
        listeners: row.listeners.clone(),
        is_primary: row.is_primary,
        is_fabric_primary: row.is_fabric_primary,
        topology_storage_provider: None,
    };
    let row = row.clone();
    Ok(Adoptable { fabric: record, provider, mesh, lost, record: row, delete })
}

/// An adoption launch's start: the lost birth it recovers.
#[derive(Debug, Clone)]
pub struct AdoptionLaunch {
    /// The lost fabric-primary's birth.
    pub lost: crate::storage::OwnRow,
}

/// The start of the adoption launch: nothing is written. The adoption is fabric recovery, never a Build or
/// part of one, and nothing on the store orders two launches (Q342, no lock); under an in-force
/// fabric.delete the restored fabric-primary continues the delete (Q370).
pub fn start(a: &Adoptable) -> AdoptionLaunch {
    AdoptionLaunch { lost: a.lost.clone() }
}

/// Who answers an adoption launch before its first mutation (fabric-recovery.md "Nothing alive").
#[async_trait::async_trait]
pub trait Answers: Send + Sync {
    /// Node investigation of the exact lost birth `lost`: its verdict.
    async fn investigate(&self, lost: &crate::storage::OwnRow) -> Finding;
    /// The node-admins of the fabric the store records, the lost birth among them, that answer a probe
    /// now, by path and name.
    async fn admins_answering(&self) -> Result<Vec<NodeId>, String>;
}

/// The checks before the launch's first mutation: node investigation of the lost birth, then,
/// last, whether any node-admin the store records answers. Reached, or an admin answering, refuses
/// `NameHeld` naming who answered; otherwise the investigation's verdict. One
/// `rdm.node_admin.fabric.update.via-adoption-check` span.
pub async fn check(c: &AdoptionLaunch, answers: &dyn Answers) -> Result<Finding, Refused> {
    let span = tracing::info_span!(
        "rdm.node_admin.fabric.update.via-adoption-check",
        lost_node_id = %c.lost.node_id,
        lost_incarnation_id = %c.lost.incarnation_id.as_str(),
        verdict = tracing::field::Empty,
        answered = tracing::field::Empty,
        outcome = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let decided = async {
        let finding = answers.investigate(&c.lost).await;
        tracing::Span::current().record("verdict", finding.name());
        if let Finding::Reached { .. } = finding {
            tracing::Span::current().record("answered", c.lost.node_id.as_str());
            return Err(Refused { reason: format!("NameHeld: node_id={} incarnation={} path={} name={} answered node investigation", c.lost.node_id, c.lost.incarnation_id, c.lost.path, c.lost.name), answered: Some(c.lost.node_id.clone()) });
        }
        if let Some(refused) = admin_answered(&c.lost, answers).await {
            tracing::Span::current().record("answered", refused.answered.as_ref().map(|id| id.as_str()).unwrap_or(""));
            return Err(refused);
        }
        Ok(finding)
    }
    .instrument(span.clone())
    .await;
    span.record("outcome", match &decided { Ok(_) => "clear", Err(refusal) if refusal.answered.is_some() => "name-held", Err(_) => "unreadable" });
    span.in_scope(|| tracing::info!("the adoption launch's checks before its first mutation"));
    decided
}

/// The last check before a launch's first mutation, the adoption launch's and the launch on an
/// inherited data dir's alike (Q156: "a launch that finds a living admin is refused, never an ordinary
/// admin at the primary's name"; Q199): whether any node-admin the store records answers a probe now.
/// `Some` refusal, `NameHeld` naming who answered, when one does: the fabric has a live admin, and the
/// lost birth `lost`'s path and name is its fabric recovery's to re-create.
pub async fn admin_answered(lost: &crate::storage::OwnRow, answers: &dyn Answers) -> Option<Refused> {
    let answering = match answers.admins_answering().await {
        Ok(answering) => answering,
        Err(error) => return Some(Refused { reason: format!("the answering admins could not be determined: {error}"), answered: None }),
    };
    let first = answering.first()?.clone();
    Some(Refused { reason: format!("NameHeld: answering node IDs {answering:?}; path={} name={} belongs to node_id={}", lost.path, lost.name, lost.node_id), answered: Some(first) })
}

/// Why the checks refused the launch, and the admin that answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    /// The refusal, `NameHeld` by name.
    pub reason: String,
    /// The exact node ID that answered; None for an error without an answering admin.
    pub answered: Option<NodeId>,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

/// A launch refused by its checks (`refused`): its adoption recorded failed on its span, and nothing is
/// written. Under an in-force fabric.delete the delete stays in force for the admin that answered.
pub fn fail(c: &AdoptionLaunch, refused: &Refused) {
    tracing::info_span!("rdm.node_admin.build.update.via-adoption-failed", node_id = %c.lost.node_id, reason = %refused.reason, outcome = "nothing-written")
        .in_scope(|| tracing::info!("the adoption launch is refused by its checks: it writes nothing"));
}

/// The launch, `me_id`, releases the name of `lost`, the lost birth: the lost node's record written
/// `Deleted` ([`crate::node_rows::write_deleted`]; a lost node's record is written by the admin acting
/// for it, Q356), recorded on the node records, never on a Build. The launch's own record, its first
/// line at its start, then holds the path and name, as every node's does.
pub async fn release_name(lost: &crate::storage::OwnRow, me_id: &NodeId, nodes: &dyn crate::storage::NodesStorage) -> Result<(), String> {
    crate::node_rows::write_deleted(nodes, &lost.location(), &lost.node_id).await.map(|_| ()).map_err(|e| format!("node_id={me_id} releases path={} name={} of lost node_id={}: {e}", lost.path, lost.name, lost.node_id))
}

/// [`Answers`] over this process's Node RPC client: node investigation of the lost birth direct (no view
/// holds carriers yet), the provider's inspection of its recorded runtime first (Q273); the probe of every
/// node-admin birth the store's live rows record, at once.
pub struct ProbeAnswers {
    /// This launch's explicit owning mesh.
    pub mesh: String,
    /// This launch's path.
    pub me_path: String,
    /// This node's name, beside its path.
    pub me_name: String,
    /// This launch's node id.
    pub me_id: NodeId,
    /// The process's Node RPC client.
    pub client: Arc<rdm_transport::NodeRpcClient>,
    /// The process's live resolver: each probed birth is applied to it first.
    pub resolver: Arc<rdm_transport::LiveNodeResolver>,
    /// The fabric's provider, prepared.
    pub provider: Arc<dyn crate::deployment::provider::DeploymentProvider>,
    /// The fabric's nodes and mesh stores.
    pub nodes: Arc<dyn crate::storage::NodesStorage>,
    pub meshes: Arc<dyn crate::storage::MeshStorage>,
    /// The fabric, as the empty view names it.
    pub fabric: crate::model::Fabric,
    /// The inherited birth a launch on its data dir is over, never asked: it holds this launch's own
    /// endpoint key, so it is no other node. `None` for the adoption launch, which asks the lost birth too.
    pub inherited: Option<NodeId>,
}

impl ProbeAnswers {
    fn resolve(&self, node_id: &NodeId, name: &PathName, mesh: &str, r#type: &str, endpoint: &EndpointId, addr: SocketAddr, incarnation: &IncarnationId) -> bool {
        let Ok(endpoint_id) = endpoint.0.parse::<iroh::PublicKey>() else { return false };
        self.resolver.apply(rdm_transport::ResolvedNode { mesh: mesh.to_owned(), r#type: r#type.to_string(), node_id: node_id.clone(), path: name.path.clone(), name: name.name.clone(), endpoint_id, transport_addr: addr, incarnation: incarnation.clone() }, None);
        true
    }

    fn calls(&self) -> crate::offline::TickleClient {
        crate::offline::TickleClient { client: self.client.clone(), me_id: self.me_id.clone(), me: self.me_id.to_string(), connections: None }
    }
}

#[async_trait::async_trait]
impl Answers for ProbeAnswers {
    async fn investigate(&self, lost: &crate::storage::OwnRow) -> Finding {
        let mut node = Node::allocated(lost.location(), lost.mesh.clone(), "node-admin");
        node.node_id = lost.node_id.clone();
        node.incarnation_id = Some(lost.incarnation_id.clone());
        let view = Topology { fabric: self.fabric.clone(), meshes: Vec::new(), nodes: Vec::new() };
        let inspect = async {
            let row = crate::node_rows::birth_record(&*self.nodes, &lost.node_id, &lost.incarnation_id).await.ok().flatten();
            Finding::from(super::rows_writer::inspection_of(&*self.provider, &None, row.as_ref()).await)
        };
        if !self.resolve(&lost.node_id, &lost.location(), &lost.mesh, "node-admin", &lost.endpoint_id, lost.transport_addr, &lost.incarnation_id) {
            return inspect.await;
        }
        crate::node_investigation::investigate(&node, &view, &self.calls(), inspect).await
    }

    async fn admins_answering(&self) -> Result<Vec<NodeId>, String> {
        let rows = match crate::storage::live_records(&*self.meshes, &*self.nodes, &self.mesh).await {
            Ok(rows) => rows,
            Err(e) => return Err(format!("the node rows could not be read: {e}")),
        };
        let calls = self.calls();
        // Each admin's address is its record's.
        let probes = rows.iter().filter(|r| rdm_core::is_node_admin(&r.r#type) && r.node_id != self.me_id && self.inherited.as_ref() != Some(&r.node_id)).filter_map(|r| {
            self.resolve(&r.node_id, &r.location(), &r.mesh(), &r.r#type, &r.endpoint_id, r.transport_addr, &r.incarnation).then(|| {
                let mut node = Node::allocated(r.location(), r.mesh(), &r.r#type);
                node.node_id = r.node_id.clone();
                node.incarnation_id = Some(r.incarnation.clone());
                let probe = rdm_core::status::StatusRequest::ProbeNodeState { node_id: r.node_id.clone(), incarnation: r.incarnation.clone() };
                let calls = &calls;
                async move {
                    let end = crate::offline::TickleCalls::direct(calls, &node, &probe).await;
                    matches!(end, crate::offline::ProbeEnd::Reply(_)).then(|| node.node_id.clone())
                }
            })
        });
        Ok(futures_util::future::join_all(probes).await.into_iter().flatten().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accepted::FabricTopology;
    use crate::build::BuildId;
    use crate::build_state::{BuildAccepted, MemoryBuildStateAdapter};
    use crate::fabric_storage::{FabricIdentity, FabricRecord, FabricStorage, MemoryFabricStorage};
    use crate::node_rows::{WriteRecord as _, WrittenRows};
    use crate::storage::{MemoryMeshStorage, MemoryNodesStorage, MeshRecord, MeshStorage as _, NodeRecord};
    use std::path::PathBuf;

    struct StubAnswers {
        finding: Finding,
        answering: Result<Vec<NodeId>, String>,
    }

    #[async_trait::async_trait]
    impl Answers for StubAnswers {
        async fn investigate(&self, _lost: &crate::storage::OwnRow) -> Finding {
            self.finding.clone()
        }
        async fn admins_answering(&self) -> Result<Vec<NodeId>, String> {
            self.answering.clone()
        }
    }

    /// CONTRACT: docs/architecture/lifecycles/fabric-recovery.md "Nothing alive": nothing on the store orders two adoption launches (Q342, no lock); "A launch that finds a node-admin alive exits" ... "it is refused NameHeld and exits, its adoption recorded failed. Otherwise it releases the name"
    #[test]
    fn an_adoption_launch_reads_the_store_writes_no_build_or_lock_and_takes_the_lost_name_once_its_checks_pass() {
        let capture = crate::span_capture::Capture::new(
            "adoption_read_claim_check_fail_release_name_and_complete_decide_on_store_records_and_emit_spans",
        );
        let dispatch = capture.dispatch.clone();
        let d = dispatch.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _guard = tracing::dispatcher::set_default(&d);

        rt.block_on(async move {
            let path: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
            let fabric_id = FabricId::parse("fab000000001").unwrap();
            let mesh_id = MeshId::parse("meshd0000001").unwrap();
            let store = PathBuf::from("/tmp/rdm-adoption-unit");

            let fabric_store = MemoryFabricStorage::default();
            let mesh_store = MemoryMeshStorage::default();
            let nodes_store = MemoryNodesStorage::default();
            let builds_store = MemoryBuildStateAdapter::default();

            // 1. Missing Fabric record refuses by name.
            let err = read( &path, "mesh1",
                &fabric_id,
                &store,
                Some("process"),
                &fabric_store,
                &mesh_store,
                &nodes_store,
                &builds_store,
            )
            .await
            .unwrap_err();
            assert!(err.contains("holds no Fabric record"), "{err}");

            // Populate Fabric record with provider = Process; differing MESH_SPAWN_TYPE is refused.
            let mut f_rec = FabricRecord::new(FabricIdentity {
                fabric_id: fabric_id.clone(),
                name: "fabric1".into(),
            });
            f_rec.provider = Some(ProviderKind::Process);
            fabric_store.put_fabric(&f_rec).await.unwrap();

            let err = read( &path, "mesh1",
                &fabric_id,
                &store,
                Some("container"),
                &fabric_store,
                &mesh_store,
                &nodes_store,
                &builds_store,
            )
            .await
            .unwrap_err();
            assert!(err.contains("MESH_SPAWN_TYPE=container and the Fabric record names provider process"), "{err}");

            // Populate live mesh1 record and lost fabric-primary row at mesh1.node-admin.1, plus current Build.
            crate::storage::write_mesh(
                &mesh_store,
                None,
                &MeshRecord::new(mesh_id.clone(), "mesh1"),
            )
            .await
            .unwrap()
            .unwrap();

            let lost_id = NodeId::mint();
            let lost_inc = IncarnationId::mint();
            let mut row = NodeRecord::new(path.clone(), "mesh1", "node-admin", lost_id.clone(), NodeStatus::Ready);
            row.mesh_id = Some(mesh_id.clone());
            let b = row.birth_mut(&lost_inc, &path);
            b.is_primary = Some(1);
            b.is_fabric_primary = Some(1);
            nodes_store.write_record(&row).await.unwrap();

            let build_id = BuildId::mint();
            crate::build_state::accept(
                &builds_store,
                &BuildAccepted {
                    build_id: build_id.clone(),
                    topology: FabricTopology::root("fabric1", "mesh1", mesh_id.clone()),
                    change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() },
                    source: None, time: 1000,
                },
                None,
                "mesh1.node-admin.1",
                "day-0",
            )
            .await
            .unwrap();

            let adoptable = read( &path, "mesh1",
                &fabric_id,
                &store,
                Some("process"),
                &fabric_store,
                &mesh_store,
                &nodes_store,
                &builds_store,
            )
            .await
            .expect("adoptable store succeeds");
            assert_eq!(adoptable.lost.node_id, lost_id);
            assert_eq!(adoptable.lost.incarnation_id, lost_inc);

            // 2. The adoption is fabric recovery, never a Build or part of one: its start writes nothing, and nothing
            // on the store orders two launches (Q342, no lock).
            let me_1 = NodeId::mint();
            let before = builds_store.read_build(&build_id).await.unwrap();
            let launched = start(&adoptable);
            let _second = start(&adoptable);
            let after = builds_store.read_build(&build_id).await.unwrap();
            assert_eq!(after, before, "a launch writes nothing on the Build, and a second launch starts too: nothing on the store orders launches");

            // 3. Check refuses NameHeld when an admin answers; fail writes nothing.
            let answering_id = NodeId::mint();
            let refused = check(&launched, &StubAnswers { finding: Finding::Exited { code: Some(0) }, answering: Ok(vec![answering_id.clone()]) })
                .await
                .expect_err("answering admin refuses NameHeld");
            assert!(refused.reason.contains("NameHeld"), "{}", refused.reason);
            assert_eq!(refused.answered, Some(answering_id));
            let unreadable = check(&launched, &StubAnswers { finding: Finding::Exited { code: Some(0) }, answering: Err("injected node-store read failure".into()) })
                .await
                .expect_err("read failure cannot be mistaken for no live admins");
            assert_eq!(unreadable.answered, None, "an error is not an answering node ID");
            assert!(unreadable.reason.contains("injected node-store read failure"));
            fail(&launched, &refused);

            // 4. A launch whose checks pass releases the lost birth's name and takes it.
            let finding = check(&launched, &StubAnswers { finding: Finding::Exited { code: Some(0) }, answering: Ok(Vec::new()) })
                .await
                .expect("no admin answering passes check");
            assert_eq!(finding, Finding::Exited { code: Some(0) });
            let written = WrittenRows::default();
            release_name(&launched, &me_1, &nodes_store, &written).await.expect("release_name marks the lost birth Deleted and claims its path and name for me_1");
            let lost_row = crate::storage::NodesStorage::node(&nodes_store, &lost_id).await.unwrap().expect("the lost birth's record");
            assert_eq!(lost_row.status, crate::model::NodeStatus::Deleted, "the lost birth's record is deleted");
            let unchanged = builds_store.read_build(&build_id).await.unwrap();
            assert_eq!(unchanged, before, "nothing is written on a Build");
        });

        let checks = capture.named("rdm.node_admin.fabric.update.via-adoption-check");
        let check_outcomes: Vec<_> = checks.iter().filter_map(|s| s.attributes.get("outcome").map(String::as_str)).collect();
        assert_eq!(check_outcomes, vec!["name-held", "unreadable", "clear"]);
        let fails = capture.named("rdm.node_admin.build.update.via-adoption-failed");
        assert_eq!(fails.len(), 1);
        assert_eq!(fails[0].attributes.get("outcome").map(String::as_str), Some("nothing-written"));
    }
}

