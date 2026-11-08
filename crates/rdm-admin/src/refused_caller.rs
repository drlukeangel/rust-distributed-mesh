//! A refused caller (Q207, Luke 2026-10-27): "in general a FP/MP looks to see if something at that
//! path and name exists currently; if it does it should delete the caller; if it doesn't it should check
//! atlas; if it's needed it should re-admit; if it's not it should delete."
//!
//! One decision for every primary and every service a caller birth it does not hold comes through (a
//! status declaration refused `SubjectUnknown`, a deleted birth's Digest heard again, a join report
//! refused `JoinMismatch` because the row at its path and name holds another node): decided from
//! the caller's record and the record at its `(path, name)`, read through the reader (query.md §1), and
//! the ops this admin's atlas holds executing ([`crate::atlas::Atlas::refused_caller`]), and, at an admin that
//! is not the fabric-primary, verified at the root through `fabric.atlas.get` before anything acts (the
//! root's answer wins: this admin may have missed an op). The read is owed to the root (node-rpc.md §43,
//! rdm#177): sent to the fabric-primary this admin's view names, and sent again when a view is
//! installed or after the resend schedule's next delay, a view naming no fabric-primary included, so a
//! caller refused across a cut is decided once the fabric-primary enters the view. A caller deleted has its
//! runtime terminated by the primary that decided it, its own workflow and never a Build
//! (`RecoverySubmitter::terminate`, the returned-birth path; Q342); a caller re-admitted is commanded back through `node.mesh.join`
//! (`crate::heard_again`). One decision per birth: a birth deleted and terminated is decided once.
//!
//! Q217 (Luke, 2026-10-27): "delete a caller deletes the caller, not just from the record but
//! literally as well: if they come back and they are deleted they get actually deleted." A caller whose
//! record is deleted is deleted, its runtime terminated, its record left deleted: never re-admitted. A
//! path the current Build still needs is recovered by a re-create at that path and name, a new birth, by
//! node recovery (the primary's own workflow, `RecoverySubmitter::submit_in`), never here.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::atlas::{CallerDecision, Atlas};
use crate::model::{IncarnationId, NodeId, NodeStatus, PathName};

/// What a refused caller's decision acts through: this admin's recovery submitter (the terminate
/// report) and its `HeardAgain` (the re-admission), filled once both exist.
pub struct RefusedCallers {
    pub(crate) node_id: NodeId,
    pub(crate) recovery: crate::admin::RecoverySubmitter,
    pub(crate) heard_again: crate::heard_again::HeardAgainSlot,
    /// Each refused birth's decision, by `(node_id, incarnation)`: `None` while it is being decided, the
    /// decision and its outcome once the birth is deleted and its runtime terminated. A deleted birth
    /// stays deleted (Q217): a later refusal of it, at any service, is answered by that one decision.
    decided: Mutex<BTreeMap<(String, String), Option<(CallerDecision, String)>>>,
    /// Where the caller's record and the record at its `(path, name)` are read through (query.md §1).
    stores: crate::current_record::RecordStoresSource,
    /// The tick of this admin's installed view ([`Self::watch_views`]).
    views: std::sync::OnceLock<tokio::sync::watch::Receiver<u64>>,
}

impl std::fmt::Debug for RefusedCallers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefusedCallers").field("node_id", &self.node_id).finish_non_exhaustive()
    }
}

impl RefusedCallers {
    pub(crate) fn new(node_id: NodeId, recovery: crate::admin::RecoverySubmitter, heard_again: crate::heard_again::HeardAgainSlot, stores: crate::current_record::RecordStoresSource) -> Self {
        Self { node_id, recovery, heard_again, decided: Mutex::default(), stores, views: Default::default() }
    }

    /// Hold `views`, the tick of this admin's installed view.
    pub(crate) fn watch_views(&self, views: tokio::sync::watch::Receiver<u64>) {
        let _ = self.views.set(views);
    }

    /// Decide the caller birth `(node_id, incarnation)` refused at `via` (`declaration`, `digest` or `join`)
    /// and act on it, once per birth: one `rdm.node_admin.node.update.via-refused-caller` span naming the
    /// decision, whether the fabric-primary verified it, and what was done. The decision; the one already
    /// made, with no new span, for a birth deleted and terminated; `None` while one for the birth is in
    /// flight. Any other end (a re-admission, a termination that failed, nothing done) is decided again
    /// at the birth's next refusal.
    pub(crate) async fn decide(&self, atlas: &Atlas, via: &'static str, node_id: &NodeId, incarnation: &IncarnationId) -> Option<(CallerDecision, String)> {
        let key = (node_id.to_string(), incarnation.as_str().to_string());
        {
            let mut decided = self.decided.lock().unwrap();
            match decided.get(&key) {
                Some(Some(done)) => return Some(done.clone()),
                Some(None) => return None,
                None => {
                    decided.insert(key.clone(), None);
                }
            }
        }
        let me = self.recovery.held_name();
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-refused-caller",
            node_path = %me.path, node_name = %me.name, via, node_id = %node_id, incarnation_id = %incarnation.as_str(),
            subject_path = tracing::field::Empty, subject_name = tracing::field::Empty, decision = tracing::field::Empty, own = tracing::field::Empty,
            root = tracing::field::Empty, fabric_primary = tracing::field::Empty, sends = tracing::field::Empty,
            needed = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal"
        );
        let decided = tracing::Instrument::instrument(self.decide_and_act(atlas, &span, node_id, incarnation), span.clone()).await;
        {
            let mut held = self.decided.lock().unwrap();
            if decided.1 == "terminated" {
                held.insert(key, Some(decided.clone()));
            } else {
                held.remove(&key);
            }
        }
        span.in_scope(|| tracing::info!("a caller birth this primary does not hold was decided by its (path, name) and the atlas"));
        Some(decided)
    }

    async fn decide_and_act(&self, atlas: &Atlas, span: &tracing::Span, node_id: &NodeId, incarnation: &IncarnationId) -> (CallerDecision, String) {
        // A caller this admin's atlas holds no record of, or of a mesh it is not primary for, is nothing
        // to verify.
        let first = match self.own_decision(atlas, span, node_id, incarnation).await {
            Ok((own, ..)) => own,
            Err(done) => return done,
        };
        // A node-admin that is not the fabric-primary verifies at the root before it acts.
        let root = match self.root_atlas().await {
            Ok(root) => root,
            Err(why) => {
                span.record("outcome", "root-unverified").record("detail", why.as_str());
                span.record("decision", first.name());
                return (first, "root-unverified".into());
            }
        };
        // Taken again over what this admin holds once the root answered: the wait for it may have spanned
        // a view, a Build or this admin's primary moving.
        let (own, path, planned, record) = match self.own_decision(atlas, span, node_id, incarnation).await {
            Ok(own) => own,
            Err(done) => return done,
        };
        let decided = match root {
            None => {
                span.record("root", "self");
                own
            }
            Some(root) => {
                let needed = planned.values().any(|p| p.contains(&path));
                let verified = own.clone().verified((node_id, incarnation), &root, needed);
                span.record("root", if verified == own { "agrees" } else { "differs" });
                verified
            }
        };
        span.record("decision", decided.name());
        // A deleted birth's path the topology still needs is node recovery's to re-create (Q217).
        if let CallerDecision::DeleteDeletedBirth { needed, .. } = &decided {
            span.record("needed", *needed);
        }
        let outcome = match &decided {
            CallerDecision::DeleteHeld { .. } | CallerDecision::DeleteUnneeded { .. } | CallerDecision::DeleteDeletedBirth { .. } => match self.recovery.terminate(path.clone(), node_id.clone(), incarnation.clone()).await {
                Ok(()) => "terminated".to_string(),
                Err(e) => {
                    span.record("detail", e.as_str());
                    "terminate-failed".to_string()
                }
            },
            CallerDecision::ReAdmit { .. } => match self.heard_again.get() {
                Some(heard_again) => {
                    let mut caller = crate::model::Node::allocated(path.clone(), record.mesh(), &record.r#type);
                    caller.node_id = node_id.clone();
                    caller.incarnation_id = Some(incarnation.clone());
                    caller.status = NodeStatus::Pending;
                    format!("mesh-join:{}", heard_again.command(&caller, incarnation).await)
                }
                None => "no-heard-again".to_string(),
            },
            CallerDecision::NoRecord | CallerDecision::NotMine { .. } => "nothing-done".to_string(),
        };
        span.record("outcome", outcome.as_str());
        (decided, outcome)
    }

    /// This admin's own decision on the caller birth `(node_id, incarnation)`, with its `(path, name)`, the
    /// current Build's paths by mesh and the caller's record; `Err` with the decision's end when there is
    /// nothing to act on: the Build unread, the records unread, no record of the caller, or a mesh this
    /// admin is not primary for.
    async fn own_decision(&self, atlas: &Atlas, span: &tracing::Span, node_id: &NodeId, incarnation: &IncarnationId) -> Result<(CallerDecision, PathName, BTreeMap<String, Vec<PathName>>, crate::storage::NodeRecord), (CallerDecision, String)> {
        // The topology's need: the current Build's paths, by mesh.
        let planned: BTreeMap<String, Vec<PathName>> = match self.recovery.builds.current().await {
            Ok(Some(b)) => b.topology.meshes.iter().map(|(m, t)| (m.clone(), t.nodes.iter().cloned().collect())).collect(),
            Ok(None) => BTreeMap::new(),
            Err(e) => {
                span.record("outcome", "build-unread").record("detail", format!("the current Build could not be read: {e}").as_str());
                return Err((CallerDecision::NoRecord, "build-unread".into()));
            }
        };
        // The caller's record at its node id, and the record the current-pair lookup names at its
        // (path, name) (query.md §1).
        let read = async {
            let stores = (self.stores)().ok_or_else(|| "this admin's node records are not open".to_string())?;
            let reader = stores.reader();
            let Some(caller) = reader.row(node_id).await? else { return Ok((None, None)) };
            let at_path = reader.row_at(&caller.location()).await?;
            Ok::<_, String>((Some(caller), at_path))
        };
        let (caller, at_path) = match read.await {
            Ok(read) => read,
            Err(e) => {
                span.record("outcome", "record-unread").record("detail", format!("the caller's record could not be read: {e}").as_str());
                return Err((CallerDecision::NoRecord, "record-unread".into()));
            }
        };
        let own = atlas.refused_caller(caller.as_ref(), at_path.as_ref(), incarnation, &planned);
        // A birth this primary holds deleted, from its own recovery's verdict until its NodeDeleted and from
        // the NodeDeleted on (node-recovery.md step 3: "A birth that comes back once its verdict is not
        // `reached`, during its replacement's birth or after it, is a deleted birth"), is decided from what
        // the primary holds, never from a record line the returned birth may have written since: it is
        // never re-admitted (Q217).
        let held_deleted = self.recovery.digest_view.is_recovering(node_id.as_str(), incarnation) || self.recovery.digest_view.is_deleted(node_id.as_str());
        let own = match own {
            CallerDecision::ReAdmit { path, name } if held_deleted => CallerDecision::DeleteDeletedBirth { path, name, needed: true },
            other => other,
        };
        span.record("own", own.name());
        if let Some(path) = own.path() {
            span.record("subject_path", path.path.as_str()).record("subject_name", path.name.as_str());
        }
        let Some(path) = own.path() else {
            span.record("decision", own.name()).record("outcome", "nothing-done").record("detail", "no record of the caller stands: it has no recorded path and name to decide on");
            return Err((own, "nothing-done".into()));
        };
        if matches!(own, CallerDecision::NotMine { .. }) {
            span.record("decision", own.name()).record("outcome", "nothing-done").record("detail", format!("this admin is not primary for the recorded owner of path {:?}, name {:?}", path.path, path.name).as_str());
            return Err((own, "nothing-done".into()));
        }
        let record = caller.expect("a decision that names a path holds the caller's record");
        Ok((own, path, planned, record))
    }

    /// The root's atlas to verify a decision against: `fabric.atlas.get` owed to the fabric-primary this
    /// admin's view names until it answers (node-rpc.md §43; `crate::node_config::owed`): sent again to
    /// the fabric-primary the view names when a view is installed or after the resend schedule's next
    /// delay, a view naming none, a refusal that the receiver is not the fabric-primary and a
    /// fabric-primary holding no atlas yet included. `None` when this admin is the fabric-primary: it
    /// decides from its own atlas. `Err` names why nothing verifies the decision: no node-RPC client,
    /// or an answer that is not an atlas.
    async fn root_atlas(&self) -> Result<Option<rdm_core::views::AtlasView>, String> {
        let first = self.recovery.topology.read().await.clone();
        if first.is_fabric_primary(&self.node_id) {
            return Ok(None);
        }
        let Some(client) = self.recovery.client.clone() else {
            return Err("this admin has no node-RPC client to call the fabric-primary".into());
        };
        let me = self.recovery.held_name();
        match crate::node_config::owed(&client, self.views.get().cloned(), &me, &self.node_id, &first, &self.recovery.topology, crate::node_config::PropertyCall::Status(&rdm_core::status::StatusRequest::GetAtlas), crate::node_config::Hop::Root).await {
            crate::node_config::Owed::InProcess(_) => Ok(None),
            crate::node_config::Owed::Answered(rdm_core::status::StatusReply::Atlas { atlas }) => Ok(Some(atlas)),
            crate::node_config::Owed::Answered(rdm_core::status::StatusReply::RejectedSourceFailed { source, op, cause }) => Err(format!("fabric.atlas.get at the fabric-primary answered rejected-source-failed (source {source}, op {op}: {cause}): nothing acts until the root answers its atlas")),
            crate::node_config::Owed::Answered(other) => Err(format!("fabric.atlas.get at the fabric-primary answered {}: nothing acts until the root answers its atlas", other.name())),
            crate::node_config::Owed::Gone(_) => Err("fabric.atlas.get was withdrawn".into()),
            crate::node_config::Owed::Unanswered(why) => Err(format!("fabric.atlas.get at the fabric-primary ended unanswered: nothing acts until the root answers its atlas ({why})")),
        }
    }
}

impl Atlas {
    /// The caller birth `(node_id, incarnation)` refused at `via`, decided and acted on by this admin's
    /// [`RefusedCallers`] once they are installed ([`Self::refused_callers`]), on its own task: the
    /// refusal is answered at once, never held on the decision.
    pub(crate) fn refused(self: &Arc<Self>, via: &'static str, node_id: &NodeId, incarnation: &IncarnationId) {
        let Some(callers) = self.callers().cloned() else { return };
        let (atlas, node_id, incarnation) = (self.clone(), node_id.clone(), incarnation.clone());
        tokio::spawn(async move {
            callers.decide(&atlas, via, &node_id, &incarnation).await;
        });
    }
}

#[cfg(test)]
mod tests {
    //! Q207 and Q217 through the `StatusService`: the chaos red's shape (#3211 red 3).
    use super::*;
    use crate::model::{Node};
    use crate::topology::Topology;
    use crate::storage::{NodeRecord, NodesStorage as _};
    use rdm_core::status::{NodeState, NotPrimary, StatusReply, StatusRequest};

    /// The fabric-primary `admin`'s atlas scoped to its mesh, deciding its refused callers over mesh1's `rows`
    /// through a recovery submitter that holds no `BuildService` and no client: the decision's terminate
    /// report is spanned and its open refused naming the missing `BuildService`.
    async fn atlas_deciding(admin: &Node, topology: &Arc<tokio::sync::RwLock<Topology>>, rows: &Arc<crate::storage::MemoryNodesStorage>, builds: &Arc<crate::build_state::MemoryBuildStateAdapter>) -> Arc<Atlas> {
        atlas_deciding_over(admin, topology, rows, builds, None).await.0
    }

    /// [`atlas_deciding`] over the admin's own mesh, its recovery submitter calling on `client`; with the
    /// refused callers it installed.
    async fn atlas_deciding_over(admin: &Node, topology: &Arc<tokio::sync::RwLock<Topology>>, rows: &Arc<crate::storage::MemoryNodesStorage>, builds: &Arc<crate::build_state::MemoryBuildStateAdapter>, client: Option<Arc<rdm_transport::NodeRpcClient>>) -> (Arc<Atlas>, Arc<RefusedCallers>) {
        let atlas = Arc::new(Atlas::default());
        atlas.scope([admin.mesh.clone()].into());
        let recovery = crate::admin::RecoverySubmitter {
            me_path: admin.path.clone(), me_name: admin.name.clone(),
            me_id: admin.node_id.clone(),
            topology: topology.clone(),
            digest_view: Default::default(),
            provider: Arc::new(crate::deployment::process::ProcessDeploymentProvider::new()),
            nodes_storage: rows.clone(),
            runner: Arc::default(),
            build_service: Default::default(),
            client,
            builds: builds.clone(),
            failed: Arc::default(),
            investigated: crate::node_investigation::NodeInvestigatedWriter::in_memory(&rdm_core::FabricId::mint(), &admin.node_id),
            reasons: Arc::default(),
        };
        // The records the decision reads (query.md §1): mesh1's rows under its writer in the view.
        let stores = crate::current_record::stores_over(rows.clone(), &*topology.read().await).await;
        let callers = Arc::new(RefusedCallers::new(admin.node_id.clone(), recovery, Default::default(), Arc::new(move || Some(stores.clone()))));
        atlas.refused_callers(callers.clone());
        (atlas, callers)
    }

    /// The one `rdm.node_admin.node.update.via-refused-caller` span's attributes, once the decision's
    /// own task closed it, with every span the cell captured; `None` when no decision ran.
    async fn decided(capture: &rdm_transport::EvidenceCapture) -> (Option<serde_json::Value>, Vec<serde_json::Value>) {
        let of = |spans: &[serde_json::Value]| spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-refused-caller").map(|s| s["attributes"].clone());
        let mut spans = capture.spans().expect("the cell's spans can be read");
        for _ in 0..200 {
            if of(&spans).is_some() {
                break;
            }
            tokio::task::yield_now().await;
            spans = capture.spans().expect("the cell's spans can be read");
        }
        (of(&spans), spans)
    }

    /// The `StatusService` of `admin` over `topology`, `rows` and `builds`: no certs, no app channels.
    fn status_service_of(admin: &Node, fabric_id: rdm_core::FabricId, topology: &Arc<tokio::sync::RwLock<Topology>>, rows: &Arc<crate::storage::MemoryNodesStorage>, builds: &Arc<crate::build_state::MemoryBuildStateAdapter>) -> crate::status_rpc::StatusService {
        crate::status_rpc::StatusService {
            certs: None,
            primary_commands: Default::default(),
            topology_view: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin.location(),
            node_id: admin.node_id.clone(),
            fabric_id,
            topology: topology.clone(),
            declared: Default::default(),
            meshes_storage: Arc::new(crate::storage::MemoryMeshStorage::default()),
            fabric_storage: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            mesh_ids: Arc::new(BTreeMap::new),
            durable: Default::default(),
            commands: Arc::new(crate::node_commands::CommandView::default()),
            own: Default::default(),
            mesh_deleter: Default::default(),
            storage_changes: Default::default(), 
            wake: Default::default(),
            fabric_states: Default::default(),
            atlas: Default::default(),
            fabric_recovery: Default::default(),
            nodes_storage: rows.clone(),
            caches: Default::default(),
            configs: Default::default(),
            republish: Default::default(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(), rejoin: Default::default(),
            loads: None,
            digest_view: None,
            unreachable: Default::default(),
            builds: builds.clone(),
        }
    }

    fn birth(path: rdm_core::PathName, mesh: &str, fixture_kind: String, r#type: &str) -> Node {
        let mut n = Node::allocated(path, mesh, r#type);
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(IncarnationId::mint());
        n.mesh = n.mesh.clone();
        n.r#type = r#type.to_string();
        n.status = NodeStatus::Ready;
        n
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:31
    /// "A caller birth a primary does not hold (its Digest refused by name under [gossip.md §3.1 receiver
    ///  rules](../gossip.md#31-frame-catalog), its status declaration refused `SubjectUnknown`, or its join
    ///  report refused `JoinMismatch` because the row at its path and name holds another node) is decided by
    ///  its path and name and the primary's atlas, one decision for the fabric-primary and every mesh-primary (Q207)"
    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:43
    /// "A deleted caller is reported to the fabric-primary (`rdm.node_admin.node.update.via-returned-reported`),
    ///  which terminates its runtime under `node.terminate:<path>@<incarnation>`; each decision is one
    ///  `rdm.node_admin.node.update.via-refused-caller` span."
    #[tokio::test]
    async fn a_deleted_birth_that_declares_stopped_is_refused_and_deleted_through_the_status_service() {
        let dir = std::env::temp_dir().join(format!("rdm-refused-caller-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "refused-caller-declaration").expect("the cell's evidence file can be created");
        let mut admin = birth(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin".to_string(), "node-admin");
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let caller = birth(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node".to_string(), "node");
        let caller_inc = caller.incarnation_id.clone().unwrap();
        let fabric_id = rdm_core::FabricId::mint();
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process };
        let mesh = crate::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: "mesh1".into(), status: crate::model::ScopeStatus::Ready };
        // The current Build names both paths: the deleted caller's path is still needed.
        let planned = Topology { fabric: fabric.clone(), meshes: vec![mesh.clone()], nodes: vec![admin.clone(), caller.clone()] };
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        crate::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), crate::accepted::FabricTopology::of_observed(&planned), &planned.node_type_primary("mesh1", "node-admin").unwrap().birth().unwrap()).await.unwrap();
        // This admin's view holds itself only: the caller's Digest is refused as a deleted birth.
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric, meshes: vec![mesh], nodes: vec![admin.clone()] }));
        // The node records: the admin's live, the caller's deleted.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        for (n, status) in [(&admin, NodeStatus::Ready), (&caller, NodeStatus::Deleted)] {
            let mut r = rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), rdm_core::IncarnationId::mint(), status);
            r.incarnation = n.incarnation_id.clone().unwrap();
            rows.put_node(&r).await.unwrap();
        }
        let atlas = atlas_deciding(&admin, &topology, &rows, &builds).await;
        let status = status_service_of(&admin, fabric_id, &topology, &rows, &builds);
        let _ = status.atlas.set(atlas.clone());
        let declare = StatusRequest::DeclareNodeState { node_id: caller.node_id.clone(), incarnation: caller_inc.clone(), state: NodeState::Stopped, operation: None };
        let reply = status.apply_from(None, None, &declare).await;
        assert_eq!(reply, StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "the deleted birth's declaration is refused at once");
        // The decision runs on its own task: it ends once its one span closes.
        let (d, spans) = decided(&capture).await;
        let d = d.unwrap_or_else(|| panic!("the `StatusService` decided the caller: {spans:?}"));
        assert_eq!(d["via"], "declaration", "{d}");
        assert_eq!((d["subject_path"].as_str(), d["subject_name"].as_str()), (Some("mesh1.node"), Some("2")), "decided at the caller's (path, name): {d}");
        assert_eq!(d["own"], "delete-deleted-birth", "{d}");
        assert_eq!(d["decision"], "delete-deleted-birth", "a caller whose record is deleted is deleted, never re-admitted (Q217): {d}");
        assert_eq!(d["needed"], "true", "the current Build still needs the path: node recovery re-creates it: {d}");
        assert_eq!(d["root"], "self", "the fabric-primary decides from its own atlas: {d}");
        // The rig builds no `BuildService`: the termination's open is refused naming it (node_replace's
        // e2e runs it to the runtime's end).
        assert_eq!((d["outcome"].as_str(), d["detail"].as_str()), (Some("terminate-failed"), Some(format!("{}: its runner is not built yet", admin.node_id).as_str())), "{d}");
        let reported = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-returned-reported").map(|s| s["attributes"].clone()).unwrap_or_else(|| panic!("the caller was reported for node.terminate: {spans:?}"));
        assert_eq!((reported["returned_path"].as_str(), reported["returned_name"].as_str(), reported["node_id"].as_str(), reported["incarnation_id"].as_str()), (Some("mesh1.node"), Some("2"), Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the terminate report names the exact birth: {reported}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (node-investigation.md: "A node that comes back after it was re-created is such a
    /// caller: its re-created birth is at its path and name, and it is deleted"; Q207, Q217; rdm#153, gate
    /// gates-6-9af200cc4): a birth of mesh1.node.2 that its mesh-primary commanded back through
    /// node.mesh.join (a held deployment) is deleted and re-created while it is os-stopped. Resumed, it
    /// answers the command with its join report: its own record is `Deleted` and the record at its path
    /// and name is the re-created birth's, so the join is refused `JoinMismatch` (`status`) by name at
    /// once, and the `JoinService` decides the
    /// caller by its path and name and the atlas: another birth is at its path and name, so it is deleted
    /// (`delete-path-held`), decided at the root itself, and reported for
    /// `node.terminate` (`via-returned-reported` naming the exact birth; the rig builds no `BuildService`,
    /// so the open is refused naming it).
    /// Must NOT happen: the refusal and nothing decided (the red: the deleted birth leaves its mesh,
    /// silent, its runtime never terminated), the caller re-admitted, or the refusal held on the decision.
    #[tokio::test]
    async fn a_deleted_births_join_refused_by_name_is_decided_and_deleted_through_the_join_service() {
        let dir = std::env::temp_dir().join(format!("rdm-refused-caller-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "refused-caller-join").expect("the cell's evidence file can be created");
        let mut admin = birth(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin".to_string(), "node-admin");
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let caller = birth(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node".to_string(), "node");
        let caller_inc = caller.incarnation_id.clone().unwrap();
        let recreated = birth(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node".to_string(), "node");
        let fabric_id = rdm_core::FabricId::mint();
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process };
        let mesh = crate::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: "mesh1".into(), status: crate::model::ScopeStatus::Ready };
        let planned = Topology { fabric: fabric.clone(), meshes: vec![mesh.clone()], nodes: vec![admin.clone(), recreated.clone()] };
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        crate::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), crate::accepted::FabricTopology::of_observed(&planned), &planned.node_type_primary("mesh1", "node-admin").unwrap().birth().unwrap()).await.unwrap();
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric, meshes: vec![mesh], nodes: vec![admin.clone(), recreated.clone()] }));
        // The node records: the admin's and the re-created birth's live, the caller's deleted.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        for (n, status) in [(&admin, NodeStatus::Ready), (&recreated, NodeStatus::Ready), (&caller, NodeStatus::Deleted)] {
            let mut r = rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), rdm_core::IncarnationId::mint(), status);
            r.incarnation = n.incarnation_id.clone().unwrap();
            rows.put_node(&r).await.unwrap();
        }
        let atlas = atlas_deciding(&admin, &topology, &rows, &builds).await;
        // The `JoinService`, the caller's deployment held: its mesh-primary commanded it back before the delete.
        let joins = Arc::new(crate::join::Joins::default());
        let deployed = crate::join::Deployed { mesh: caller.mesh.clone(), r#type: caller.r#type.clone(),
            path: caller.path.clone(), name: caller.name.clone(),
            node_id: caller.node_id.clone(),
            incarnation: caller_inc.clone(),
            supersedes: None,
            endpoint_id: crate::model::EndpointId("caller-endpoint".into()),
            runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap(),
            data_dir: "/data/mesh1.node.2".into(),
            last_accepted_name: Some(caller.location()),
            held: true,
            listeners: Vec::new(),
        };
        let _commanded = joins.expect(deployed.clone());
        let join_service = crate::join::JoinService {
            nodes: Some(rows.clone()),
            me_path: admin.path.clone(), me_name: admin.name.clone(),
            joins,
            answer: Arc::new(|| Box::pin(async { Err("the cell admits no join".to_string()) })),
            install: Arc::new(|_, _| {}),
            known: Arc::new(|| Box::pin(async {})),
            pending: Arc::new(|_, _| Box::pin(async {})),
            primary: Arc::new(|| None),
            atlas: Some(atlas.clone()),
            issuer: Arc::new(crate::certs::CertIssuer::no_certs({
                let t = rdm_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let report = rdm_core::MeshDigest {
            fabric_id,
            node: rdm_core::MeshNode { mesh: caller.mesh.clone(), r#type: caller.r#type.clone(),
                node_id: caller.node_id.clone(),
                path: caller.path.clone(), name: caller.name.clone(),
                endpoint_id: deployed.endpoint_id.clone(),
                transport_addr: "127.0.0.1:34567".parse().unwrap(),
                incarnation: caller_inc.clone(),
                supersedes: None,
            },
            status: rdm_core::model::NodeStatus::Pending,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            primary: Vec::new(),
        };
        let join = rdm_core::join::JoinRequest::JoinNode { digest: rdm_core::wire::WireDigest::from(&report), runtime: Some(deployed.runtime.clone()), data_dir: Some(deployed.data_dir.clone()), listeners: Vec::new() };
        let reply = join_service.serve(crate::model::EndpointId("caller-endpoint".into()), join).await;
        assert!(matches!(&reply, rdm_core::join::JoinReply::JoinMismatch { field, deployed, .. } if field == "status" && deployed.contains(caller.node_id.as_str()) && deployed.contains("deleted")), "the deleted birth's join is refused at once by its own Deleted record: {reply:?}");
        // The decision runs on its own task: it ends once its one span closes.
        let (d, spans) = decided(&capture).await;
        let d = d.unwrap_or_else(|| panic!("the `JoinService` decided the caller: {spans:?}"));
        assert_eq!(d["via"], "join", "{d}");
        assert_eq!((d["node_id"].as_str(), d["incarnation_id"].as_str()), (Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the exact birth that joined: {d}");
        assert_eq!((d["subject_path"].as_str(), d["subject_name"].as_str()), (Some("mesh1.node"), Some("2")), "decided at the caller's (path, name): {d}");
        assert_eq!(d["decision"], "delete-path-held", "another birth is at the caller's (path, name): the caller is deleted (Q207): {d}");
        assert_eq!(d["root"], "self", "the fabric-primary decides from its own atlas: {d}");
        assert_eq!((d["outcome"].as_str(), d["detail"].as_str()), (Some("terminate-failed"), Some(format!("{}: its runner is not built yet", admin.node_id).as_str())), "{d}");
        let reported = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-returned-reported").map(|s| s["attributes"].clone()).unwrap_or_else(|| panic!("the caller was reported for node.terminate: {spans:?}"));
        assert_eq!((reported["returned_path"].as_str(), reported["returned_name"].as_str(), reported["node_id"].as_str(), reported["incarnation_id"].as_str()), (Some("mesh1.node"), Some("2"), Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the terminate report names the exact birth: {reported}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (node-investigation.md: "A caller whose record is deleted is deleted, its runtime
    /// terminated, never re-admitted"; Q207, Q217; red in lane 6's gate at 7d09688bd, build_op's resumed
    /// executor admitted at its join while its row was deleted and no re-created birth was written yet):
    /// a birth of mesh1.node.2 commanded back through node.mesh.join is deleted while it is os-stopped, and
    /// nothing is re-created at its path yet. Resumed, it answers the command with its join report: the row
    /// at its path and name is its own, deleted, so the join is refused `JoinMismatch` on `status` by name at
    /// once, and the `JoinService` decides it `delete-deleted-birth` and reports it for `node.terminate`.
    /// Must NOT happen: the deleted birth issued a cert and admitted (the red), or re-admitted.
    #[tokio::test]
    async fn a_deleted_births_join_with_its_own_deleted_row_at_its_path_is_refused_and_deleted() {
        let dir = std::env::temp_dir().join(format!("rdm-refused-caller-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "refused-caller-join-own-deleted").expect("the cell's evidence file can be created");
        let mut admin = birth(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin".to_string(), "node-admin");
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let caller = birth(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node".to_string(), "node");
        let caller_inc = caller.incarnation_id.clone().unwrap();
        let fabric_id = rdm_core::FabricId::mint();
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process };
        let mesh = crate::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: "mesh1".into(), status: crate::model::ScopeStatus::Ready };
        let planned = Topology { fabric: fabric.clone(), meshes: vec![mesh.clone()], nodes: vec![admin.clone()] };
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        crate::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), crate::accepted::FabricTopology::of_observed(&planned), &planned.node_type_primary("mesh1", "node-admin").unwrap().birth().unwrap()).await.unwrap();
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric, meshes: vec![mesh], nodes: vec![admin.clone()] }));
        // The node records: the admin's live, the caller's deleted, and no re-created birth yet.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        for (n, status) in [(&admin, NodeStatus::Ready), (&caller, NodeStatus::Deleted)] {
            let mut r = rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), rdm_core::IncarnationId::mint(), status);
            r.incarnation = n.incarnation_id.clone().unwrap();
            rows.put_node(&r).await.unwrap();
        }
        let atlas = atlas_deciding(&admin, &topology, &rows, &builds).await;
        // The `JoinService`, the caller's deployment held: its mesh-primary commanded it back before the delete.
        let joins = Arc::new(crate::join::Joins::default());
        let deployed = crate::join::Deployed { mesh: caller.mesh.clone(), r#type: caller.r#type.clone(),
            path: caller.path.clone(), name: caller.name.clone(),
            node_id: caller.node_id.clone(),
            incarnation: caller_inc.clone(),
            supersedes: None,
            endpoint_id: crate::model::EndpointId("caller-endpoint".into()),
            runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap(),
            data_dir: "/data/mesh1.node.2".into(),
            last_accepted_name: Some(caller.location()),
            held: true,
            listeners: Vec::new(),
        };
        let _commanded = joins.expect(deployed.clone());
        let join_service = crate::join::JoinService {
            nodes: Some(rows.clone()),
            me_path: admin.path.clone(), me_name: admin.name.clone(),
            joins,
            answer: Arc::new(|| Box::pin(async { Err("the cell admits no join".to_string()) })),
            install: Arc::new(|_, _| {}),
            known: Arc::new(|| Box::pin(async {})),
            pending: Arc::new(|_, _| Box::pin(async {})),
            primary: Arc::new(|| None),
            atlas: Some(atlas.clone()),
            issuer: Arc::new(crate::certs::CertIssuer::no_certs({
                let t = rdm_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let report = rdm_core::MeshDigest {
            fabric_id,
            node: rdm_core::MeshNode { mesh: caller.mesh.clone(), r#type: caller.r#type.clone(),
                node_id: caller.node_id.clone(),
                path: caller.path.clone(), name: caller.name.clone(),
                endpoint_id: deployed.endpoint_id.clone(),
                transport_addr: "127.0.0.1:34567".parse().unwrap(),
                incarnation: caller_inc.clone(),
                supersedes: None,
            },
            status: rdm_core::model::NodeStatus::Pending,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            primary: Vec::new(),
        };
        let join = rdm_core::join::JoinRequest::JoinNode { digest: rdm_core::wire::WireDigest::from(&report), runtime: Some(deployed.runtime.clone()), data_dir: Some(deployed.data_dir.clone()), listeners: Vec::new() };
        let reply = join_service.serve(crate::model::EndpointId("caller-endpoint".into()), join).await;
        assert!(matches!(&reply, rdm_core::join::JoinReply::JoinMismatch { field, deployed, .. } if field == "status" && deployed.contains("is deleted")), "the deleted birth's join is refused at once by name, naming its deleted row: {reply:?}");
        // The decision runs on its own task: it ends once its one span closes.
        let (d, spans) = decided(&capture).await;
        let d = d.unwrap_or_else(|| panic!("the `JoinService` decided the caller: {spans:?}"));
        assert_eq!(d["via"], "join", "{d}");
        assert_eq!((d["node_id"].as_str(), d["incarnation_id"].as_str()), (Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the exact birth that joined: {d}");
        assert_eq!((d["subject_path"].as_str(), d["subject_name"].as_str()), (Some("mesh1.node"), Some("2")), "decided at the caller's (path, name): {d}");
        assert_eq!(d["decision"], "delete-deleted-birth", "the caller's own record is deleted: it is deleted, never re-admitted (Q217): {d}");
        assert_eq!(d["root"], "self", "the fabric-primary decides from its own atlas: {d}");
        assert_eq!((d["outcome"].as_str(), d["detail"].as_str()), (Some("terminate-failed"), Some(format!("{}: its runner is not built yet", admin.node_id).as_str())), "{d}");
        let reported = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-returned-reported").map(|s| s["attributes"].clone()).unwrap_or_else(|| panic!("the caller was reported for node.terminate: {spans:?}"));
        assert_eq!((reported["returned_path"].as_str(), reported["returned_name"].as_str(), reported["node_id"].as_str(), reported["incarnation_id"].as_str()), (Some("mesh1.node"), Some("2"), Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the terminate report names the exact birth: {reported}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (node-rpc-envelope.md:726: a primary that refuses a status declaration's caller birth
    /// `SubjectUnknown` then decides that birth; node-investigation.md: a node-admin that is not the
    /// fabric-primary verifies its decision at the root with `fabric.atlas.get` before it acts; Q207;
    /// rdm#177, the 1a6d0af25 release red): mesh2's primary,
    /// mesh2.node-admin.2, is cut from the fabric-primary, so its view names none, and a new birth of
    /// mesh2.node-admin.1 the fabric-primary created declares Ready to it. The declaration is refused
    /// `SubjectUnknown` at once and the caller's decision is owed to the root: nothing is asked while no
    /// view names a fabric-primary, and once a view naming it is installed the decision is verified there
    /// (its atlas holds the birth at mesh2.node-admin.1) and the caller is re-admitted (`re-admit`, root
    /// `agrees`; the rig builds no `HeardAgain`, so `no-heard-again`), in one decision.
    /// Must NOT happen: the decision ended `root-unverified` with nothing deciding it again (the red: the
    /// declarer's retry ended on the refusal, the birth never declares again, and the Build's claim on it
    /// is withdrawn), or a second decision of the same birth.
    #[tokio::test]
    async fn a_caller_refused_with_no_fabric_primary_in_view_is_decided_once_the_fabric_primary_enters_the_view() {
        use rdm_core::views::{AtlasNodeView, AtlasView};
        let dir = std::env::temp_dir().join(format!("rdm-refused-caller-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "refused-caller-root").expect("the cell's evidence file can be created");
        let mut admin = birth(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin".to_string(), "node-admin");
        admin.is_primary = true;
        let mut fp = birth(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin".to_string(), "node-admin");
        fp.is_primary = true;
        fp.is_fabric_primary = true;
        let mut caller = birth(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin".to_string(), "node-admin");
        caller.status = NodeStatus::Pending;
        let caller_inc = caller.incarnation_id.clone().unwrap();
        let fabric_id = rdm_core::FabricId::mint();
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::Pending, provider: crate::model::ProviderKind::Process };
        let meshes: Vec<crate::model::Mesh> = ["mesh1", "mesh2"].into_iter().map(|m| crate::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: m.into(), status: crate::model::ScopeStatus::Pending }).collect();
        // The current Build names the caller's path: it is needed.
        let planned = Topology { fabric: fabric.clone(), meshes: meshes.clone(), nodes: vec![fp.clone(), admin.clone(), caller.clone()] };
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        crate::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), crate::accepted::FabricTopology::of_observed(&planned), &planned.node_type_primary("mesh1", "node-admin").unwrap().birth().unwrap()).await.unwrap();
        // mesh2.node-admin.2's view across the cut: itself only, no fabric-primary.
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric, meshes, nodes: vec![admin.clone()] }));
        // The node records of mesh2: the admin's and the caller's, both live.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        for n in [&admin, &caller] {
            let mut r = rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), rdm_core::IncarnationId::mint(), n.status);
            r.incarnation = n.incarnation_id.clone().unwrap();
            rows.put_node(&r).await.unwrap();
        }
        // The fabric-primary: its `StatusService` answers fabric.atlas.get from its atlas, which holds the caller's
        // birth at mesh2.node-admin.1, and counts the reads.
        let fp_key = iroh::SecretKey::generate();
        let fp_ep = rdm_transport::endpoint::bind(fp_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let fp_addr = fp_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let root = AtlasView { nodes: vec![AtlasNodeView { path: caller.path.clone(), name: caller.name.clone(), node_id: caller.node_id.clone(), incarnation_id: Some(caller_inc.clone()), status: rdm_core::views::NodeStatus::Pending, stopped: false, accepted: None, observed: None }] };
        let asked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = asked.clone();
        let server = rdm_transport::ServerBuilder::new()
            .serve::<rdm_core::status::Status, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_peer, req: StatusRequest| {
                let (root, counted) = (root.clone(), counted.clone());
                async move {
                    Ok(match req {
                        StatusRequest::GetAtlas => {
                            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            StatusReply::Atlas { atlas: root }
                        }
                        other => StatusReply::NotReady { reason: format!("the cell's fabric-primary answers fabric.atlas.get only, not {}", other.op()) },
                    })
                }
            })
            .build(rdm_transport::ServedBirth { node_id: fp.node_id.to_string(), incarnation: fp.incarnation_id.as_ref().unwrap().as_str().to_string() })
            .unwrap();
        let _router = iroh::protocol::Router::builder(fp_ep).accept(rdm_transport::ALPN, server).spawn();
        let resolver = Arc::new(rdm_transport::StaticResolver::new());
        resolver.insert(rdm_transport::ResolvedNode { mesh: fp.mesh.clone(), r#type: fp.r#type.clone(), node_id: fp.node_id.clone(), path: fp.path.clone(), name: fp.name.clone(), endpoint_id: fp_key.public(), transport_addr: fp_addr, incarnation: fp.incarnation_id.clone().unwrap() });
        // The fabric-primary record names it, as the process's topology feed names it (Q313).
        resolver.set_primary(None, Some(fp.node_id.clone()));
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(rdm_transport::NodeRpcClient::new(ep, resolver));
        let (atlas, callers) = atlas_deciding_over(&admin, &topology, &rows, &builds, Some(client)).await;
        let (views, installed) = tokio::sync::watch::channel(0u64);
        callers.watch_views(installed);
        let status = status_service_of(&admin, fabric_id, &topology, &rows, &builds);
        let _ = status.atlas.set(atlas.clone());
        let declare = StatusRequest::DeclareNodeState { node_id: caller.node_id.clone(), incarnation: caller_inc.clone(), state: NodeState::Ready, operation: None };
        let reply = status.apply_from(None, None, &declare).await;
        assert_eq!(reply, StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "the declaration of a birth mesh2's primary does not hold is refused at once");
        // The decision's first look: on the cell's one thread every step that waits on nothing runs here.
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 0, "no fabric-primary is asked while no view names one");
        // The cut heals: a view naming the fabric-primary is installed.
        topology.write().await.nodes.push(fp.clone());
        views.send_modify(|v| *v += 1);
        let of = |spans: &[serde_json::Value]| spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.update.via-refused-caller").map(|s| s["attributes"].clone()).collect::<Vec<_>>();
        let mut spans = capture.spans().expect("the cell's spans can be read");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while of(&spans).iter().all(|d| d["outcome"] == "root-unverified") && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            spans = capture.spans().expect("the cell's spans can be read");
        }
        let decisions = of(&spans);
        assert_eq!(decisions.len(), 1, "one decision of the birth: {decisions:?}");
        let d = &decisions[0];
        assert_eq!(d["via"], "declaration", "{d}");
        assert_eq!((d["subject_path"].as_str(), d["subject_name"].as_str()), (Some("mesh2.node-admin"), Some("1")), "decided at the caller's (path, name): {d}");
        assert_eq!((d["node_id"].as_str(), d["incarnation_id"].as_str()), (Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the exact birth that declared: {d}");
        assert_eq!(d["root"], "agrees", "verified at the fabric-primary once a view names it: {d}");
        assert_eq!(d["decision"], "re-admit", "nothing else at its (path, name), its record live and the Build needing it: re-admitted (Q207): {d}");
        assert_eq!(d["outcome"], "no-heard-again", "the rig builds no `HeardAgain`: {d}");
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 1, "the root was read once");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (node-recovery.md step 3: "A birth that comes back once its verdict is not `reached`,
    /// during its replacement's birth or after it, is a deleted birth"; node-investigation.md; Q217;
    /// rdm#304): a birth this primary's recovery holds deleted from its verdict, refused while its
    /// replacement is born, is decided from what the primary holds: deleted, though its record still
    /// reads live (the recovery's delete not yet written, or a line the returned birth wrote since) and
    /// the topology still needs its path. The same records with no recovery held re-admit it. Must NOT
    /// happen: the held birth re-admitted (the red: re-admitted at the `StatusService`, then deleted
    /// 175 ms later once the NodeDeleted was applied).
    #[tokio::test]
    async fn a_birth_its_primarys_recovery_holds_deleted_is_decided_deleted_though_its_record_reads_live() {
        let dir = std::env::temp_dir().join(format!("rdm-refused-caller-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "refused-caller-recovering").expect("the cell's evidence file can be created");
        let mut admin = birth(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin".to_string(), "node-admin");
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let caller = birth(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node".to_string(), "node");
        let caller_inc = caller.incarnation_id.clone().unwrap();
        let fabric_id = rdm_core::FabricId::mint();
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process };
        let mesh = crate::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: "mesh1".into(), status: crate::model::ScopeStatus::Ready };
        let planned = Topology { fabric: fabric.clone(), meshes: vec![mesh.clone()], nodes: vec![admin.clone(), caller.clone()] };
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        crate::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), crate::accepted::FabricTopology::of_observed(&planned), &planned.node_type_primary("mesh1", "node-admin").unwrap().birth().unwrap()).await.unwrap();
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric, meshes: vec![mesh], nodes: vec![admin.clone()] }));
        // Both records live: the caller's delete is not written.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        for n in [&admin, &caller] {
            let mut r = rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), rdm_core::IncarnationId::mint(), NodeStatus::Ready);
            r.incarnation = n.incarnation_id.clone().unwrap();
            rows.put_node(&r).await.unwrap();
        }
        let (atlas, callers) = atlas_deciding_over(&admin, &topology, &rows, &builds, None).await;
        // Not held: the live, needed caller is re-admitted (the rig has no `HeardAgain`).
        let (unheld, _) = callers.decide(&atlas, "declaration", &caller.node_id, &caller_inc).await.expect("decided");
        assert_eq!(unheld.name(), "re-admit", "nothing held, its record live and needed: re-admitted (Q207)");
        // Held deleted from its recovery's verdict: deleted, whatever its record reads.
        callers.recovery.digest_view.hold_recovering(&caller.node_id, &caller_inc);
        let (held, outcome) = callers.decide(&atlas, "declaration", &caller.node_id, &caller_inc).await.expect("decided");
        assert_eq!(held.name(), "delete-deleted-birth", "a birth its primary's recovery holds deleted is deleted, never re-admitted: {outcome}");
        let spans = capture.spans().expect("the cell's spans can be read");
        let decisions: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.update.via-refused-caller").map(|s| &s["attributes"]).collect();
        assert_eq!(decisions.len(), 2, "{decisions:?}");
        assert_eq!((decisions[1]["own"].as_str(), decisions[1]["decision"].as_str(), decisions[1]["root"].as_str()), (Some("delete-deleted-birth"), Some("delete-deleted-birth"), Some("self")), "{:?}", decisions[1]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
