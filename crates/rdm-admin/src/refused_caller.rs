//! A refused caller (Q207, Luke 2026-10-27): "in general a FP/MP looks to see if something at that
//! path.name exists currently; if it does it should delete the caller; if it doesn't it should check
//! atlas; if it's needed it should re-admit; if it's not it should delete."
//!
//! One decision for every primary and every door a caller birth it does not hold comes through (a
//! status declaration refused `SubjectUnknown`, a deleted birth's Digest heard again, a join report
//! refused `JoinMismatch` because the row at its path.name holds another node): decided from
//! this admin's own atlas ([`crate::atlas::Atlas::refused_caller`]), and, at an admin that
//! is not the fabric-primary, verified at the root through `fabric.atlas.get` before anything acts (the
//! root's answer wins: this admin may have missed an op). The read is owed to the root (node-rpc.md §43,
//! rdm#177): sent to the fabric-primary this admin's view names, and sent again when a view is
//! installed or after the resend schedule's next delay, a view naming no fabric-primary included, so a
//! caller refused across a cut is decided once the fabric-primary enters the view. A caller deleted is reported to the
//! fabric-primary, which terminates its runtime (`RecoverySubmitter::report_terminate`, the
//! returned-birth path); a caller re-admitted is commanded back through `node.mesh.join`
//! (`crate::heard_again`). One decision in flight per birth.
//!
//! Q217 (Luke, 2026-10-27): "delete a caller deletes the caller, not just from the record but
//! literally as well: if they come back and they are deleted they get actually deleted." A caller whose
//! record is deleted is deleted, its runtime terminated, its record left deleted: never re-admitted. A
//! path the current Build still needs is recovered by a re-create at that path.name, a new birth, by
//! node recovery (the mesh-primary's owed resubmit, `RecoverySubmitter::owed_submits`), never here.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crate::atlas::{CallerDecision, Atlas};
use crate::model::{IncarnationId, NodeId, NodeStatus, PathName};

/// What a refused caller's decision acts through: this admin's recovery submitter (the terminate
/// report) and its heard-again door (the re-admission), filled once both exist.
pub struct RefusedCallers {
    pub(crate) node_id: NodeId,
    pub(crate) recovery: crate::admin::RecoverySubmitter,
    pub(crate) heard_again: crate::heard_again::HeardAgainSlot,
    in_flight: Mutex<BTreeSet<(String, String)>>,
    /// The tick of this admin's installed view ([`Self::watch_views`]).
    views: std::sync::OnceLock<tokio::sync::watch::Receiver<u64>>,
}

impl std::fmt::Debug for RefusedCallers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefusedCallers").field("node_id", &self.node_id).finish_non_exhaustive()
    }
}

impl RefusedCallers {
    pub(crate) fn new(node_id: NodeId, recovery: crate::admin::RecoverySubmitter, heard_again: crate::heard_again::HeardAgainSlot) -> Self {
        Self { node_id, recovery, heard_again, in_flight: Mutex::default(), views: Default::default() }
    }

    /// Hold `views`, the tick of this admin's installed view.
    pub(crate) fn watch_views(&self, views: tokio::sync::watch::Receiver<u64>) {
        let _ = self.views.set(views);
    }

    /// Decide the caller birth `(node_id, incarnation)` refused at `door` (`declaration`, `digest` or `join`)
    /// and act on it, once per birth at a time: one `rdm.node_admin.node.update.via-refused-caller`
    /// span naming the decision, whether the fabric-primary verified it, and what was done. The
    /// decision, or `None` when one for the birth is already in flight.
    pub(crate) async fn decide(&self, atlas: &Atlas, door: &'static str, node_id: &NodeId, incarnation: &IncarnationId) -> Option<(CallerDecision, String)> {
        let key = (node_id.to_string(), incarnation.as_str().to_string());
        if !self.in_flight.lock().unwrap().insert(key.clone()) {
            return None;
        }
        let me = self.recovery.held_name();
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-refused-caller",
            node = %me, door, node_id = %node_id, incarnation_id = %incarnation.as_str(),
            subject = tracing::field::Empty, decision = tracing::field::Empty, own = tracing::field::Empty,
            root = tracing::field::Empty, fabric_primary = tracing::field::Empty, sends = tracing::field::Empty,
            needed = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal"
        );
        let decided = tracing::Instrument::instrument(self.decide_and_act(atlas, &span, node_id, incarnation), span.clone()).await;
        self.in_flight.lock().unwrap().remove(&key);
        span.in_scope(|| tracing::info!("a caller birth this primary does not hold was decided by its path.name and the atlas"));
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
        let (own, path, planned) = match self.own_decision(atlas, span, node_id, incarnation).await {
            Ok(own) => own,
            Err(done) => return done,
        };
        let decided = match root {
            None => {
                span.record("root", "self");
                own
            }
            Some(root) => {
                let needed = planned.get(&path.mesh).is_some_and(|p| p.contains(&path));
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
            CallerDecision::DeleteHeld { .. } | CallerDecision::DeleteUnneeded { .. } | CallerDecision::DeleteDeletedBirth { .. } => match self.recovery.report_terminate(self.recovery.client.as_ref(), self.views.get().cloned(), path.clone(), node_id.clone(), incarnation.clone()).await {
                Ok(()) => "terminate-reported".to_string(),
                Err(e) => {
                    span.record("detail", e.as_str());
                    "terminate-unreported".to_string()
                }
            },
            CallerDecision::ReAdmit { .. } => match self.heard_again.get() {
                Some(heard_again) => {
                    let mut caller = crate::model::Node::allocated(path.clone());
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

    /// This admin's own decision on the caller birth `(node_id, incarnation)`, with its `path.name` and
    /// the current Build's paths by mesh; `Err` with the decision's end when there is nothing to act on:
    /// the Build unread, no record of the caller, or a mesh this admin is not primary for.
    async fn own_decision(&self, atlas: &Atlas, span: &tracing::Span, node_id: &NodeId, incarnation: &IncarnationId) -> Result<(CallerDecision, PathName, BTreeMap<String, Vec<PathName>>), (CallerDecision, String)> {
        // The topology's need: the current Build's paths, by mesh.
        let planned: BTreeMap<String, Vec<PathName>> = match self.recovery.builds.current().await {
            Ok(Some(b)) => b.topology.meshes.iter().map(|(m, t)| (m.clone(), t.nodes.iter().cloned().collect())).collect(),
            Ok(None) => BTreeMap::new(),
            Err(e) => {
                span.record("outcome", "build-unread").record("detail", format!("the current Build could not be read: {e}").as_str());
                return Err((CallerDecision::NoRecord, "build-unread".into()));
            }
        };
        let own = atlas.refused_caller(node_id, incarnation, &planned);
        span.record("own", own.name());
        if let Some(path) = own.path() {
            span.record("subject", path.to_string().as_str());
        }
        let Some(path) = own.path().cloned() else {
            span.record("decision", own.name()).record("outcome", "nothing-done").record("detail", "this admin's atlas holds no record of the caller: it has no path.name to decide on");
            return Err((own, "nothing-done".into()));
        };
        if matches!(own, CallerDecision::NotMine { .. }) {
            span.record("decision", own.name()).record("outcome", "nothing-done").record("detail", format!("{} is not a mesh this admin is primary for", path.mesh).as_str());
            return Err((own, "nothing-done".into()));
        }
        Ok((own, path, planned))
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
        match crate::node_config::owed(&client, self.views.get().cloned(), &me, &self.node_id, &first, &self.recovery.topology, &rdm_core::status::StatusRequest::GetAtlas, crate::node_config::Hop::Root).await {
            crate::node_config::Owed::InProcess(_) => Ok(None),
            crate::node_config::Owed::Answered(rdm_core::status::StatusReply::Atlas { atlas }) => Ok(Some(atlas)),
            crate::node_config::Owed::Answered(other) => Err(format!("fabric.atlas.get at the fabric-primary answered {}: nothing acts until the root answers its atlas", other.name())),
            crate::node_config::Owed::Gone(_) => Err("fabric.atlas.get was withdrawn".into()),
        }
    }
}

impl Atlas {
    /// The caller birth `(node_id, incarnation)` refused at `door`, decided and acted on by this admin's
    /// [`RefusedCallers`] once they are installed ([`Self::refused_callers`]), on its own task: the
    /// refusal is answered at once, never held on the decision.
    pub(crate) fn refused(self: &Arc<Self>, door: &'static str, node_id: &NodeId, incarnation: &IncarnationId) {
        let Some(callers) = self.callers().cloned() else { return };
        let (atlas, node_id, incarnation) = (self.clone(), node_id.clone(), incarnation.clone());
        tokio::spawn(async move {
            callers.decide(&atlas, door, &node_id, &incarnation).await;
        });
    }
}

#[cfg(test)]
mod tests {
    //! Q207 and Q217 through the declaration door: the chaos red's shape (#3211 red 3).
    use super::*;
    use crate::model::{Node, NodeKind};
    use crate::topology::Topology;
    use crate::storage::{NodeRow, NodesStorage as _};
    use rdm_core::status::{NodeState, NotPrimary, StatusReply, StatusRequest};

    /// The fabric-primary `admin`'s atlas over mesh1's `rows`, filled, deciding its refused callers
    /// through a recovery submitter that holds no Build door and no client: the decision's terminate
    /// report is spanned and its open refused naming the missing door.
    async fn atlas_deciding(admin: &Node, topology: &Arc<tokio::sync::RwLock<Topology>>, rows: &Arc<crate::storage::MemoryNodesStorage>, builds: &Arc<crate::build_state::MemoryBuildStateAdapter>) -> Arc<Atlas> {
        atlas_deciding_over(admin, topology, rows, builds, None).await.0
    }

    /// [`atlas_deciding`] over the admin's own mesh, its recovery submitter calling on `client`; with the
    /// refused callers it installed.
    async fn atlas_deciding_over(admin: &Node, topology: &Arc<tokio::sync::RwLock<Topology>>, rows: &Arc<crate::storage::MemoryNodesStorage>, builds: &Arc<crate::build_state::MemoryBuildStateAdapter>, client: Option<Arc<rdm_transport::NodeRpcClient>>) -> (Arc<Atlas>, Arc<RefusedCallers>) {
        let atlas = Arc::new(Atlas::default());
        atlas.scope(&**rows, [admin.name.mesh.clone()].into()).await.unwrap();
        let recovery = crate::admin::RecoverySubmitter {
            me: admin.name.clone(),
            topology: topology.clone(),
            digest_view: Default::default(),
            provider: Arc::new(crate::deployment::process::ProcessDeploymentProvider::new()),
            nodes_storage: rows.clone(),
            written: Default::default(),
            door: Default::default(),
            client,
            builds: builds.clone(),
            data_root: Default::default(),
            gone_dirs: Default::default(),
            resubmits: Default::default(),
        };
        let callers = Arc::new(RefusedCallers::new(admin.node_id.clone(), recovery, Default::default()));
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

    /// The status door of `admin` over `topology`, `rows` and `builds`: no certs, no app channels.
    fn status_door_of(admin: &Node, fabric_id: rdm_core::FabricId, topology: &Arc<tokio::sync::RwLock<Topology>>, rows: &Arc<crate::storage::MemoryNodesStorage>, builds: &Arc<crate::build_state::MemoryBuildStateAdapter>) -> crate::status_rpc::StatusAuthority {
        crate::status_rpc::StatusAuthority {
            certs: None,
            app_channels: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin.name.clone(),
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
            hold_next_reply: Default::default(),
            wake: Default::default(),
            fabric_states: Default::default(),
            atlas: Default::default(),
            fabric_recovery: Default::default(),
            nodes_storage: rows.clone(),
            caches: Default::default(),
            configs: Default::default(),
            republish: Default::default(),
            configure: Default::default(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: None,
            digest_view: None,
            builds: builds.clone(),
        }
    }

    fn birth(path: &str, kind: NodeKind) -> Node {
        let mut n = Node::allocated(path.parse().unwrap());
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(IncarnationId::mint());
        n.mesh = n.name.mesh.clone();
        n.kind = kind;
        n.status = NodeStatus::Ready;
        n
    }

    /// CONTRACT (Q207, Q217; #3211 red 3): a birth of mesh1.rpc.2 was deleted (its record written
    /// deleted, its Digest refused, so no view holds it), and its runtime comes back and declares
    /// Stopped to its mesh-primary, the fabric-primary here. The declaration is refused `SubjectUnknown`
    /// at once, and the declaration door decides the caller by its path.name and the atlas: nothing
    /// else is at the path and its record is deleted, so it is deleted (`delete-deleted-birth`), the
    /// current Build still needing the path (`needed`, node recovery's re-create), decided at the root
    /// itself, and reported for `node.terminate` (`via-returned-reported` naming the exact birth; the
    /// rig builds no Build door, so the open is refused naming it).
    /// Must NOT happen: the refusal and nothing decided (the red), the caller re-admitted, or the
    /// refusal held on the decision.
    #[tokio::test]
    async fn a_deleted_birth_that_declares_stopped_is_refused_and_deleted_through_the_declaration_door() {
        let dir = std::env::temp_dir().join(format!("rdm-refused-caller-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "refused-caller-declaration").expect("the cell's evidence file can be created");
        let mut admin = birth("mesh1.admin.1", NodeKind::NodeAdmin);
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let caller = birth("mesh1.rpc.2", NodeKind::RpcNode);
        let caller_inc = caller.incarnation_id.clone().unwrap();
        let fabric_id = rdm_core::FabricId::mint();
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process };
        let mesh = crate::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: "mesh1".into(), status: crate::model::ScopeStatus::Ready };
        // The current Build names both paths: the deleted caller's path is still needed.
        let planned = Topology { fabric: fabric.clone(), meshes: vec![mesh.clone()], nodes: vec![admin.clone(), caller.clone()] };
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        crate::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), crate::accepted::FabricTopology::of_observed(&planned), "mesh1.admin.1").await.unwrap();
        // This admin's view holds itself only: the caller's Digest is refused as a deleted birth.
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric, meshes: vec![mesh], nodes: vec![admin.clone()] }));
        // The node records: the admin's live, the caller's deleted.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        for (n, status) in [(&admin, NodeStatus::Ready), (&caller, NodeStatus::Deleted)] {
            let mut r = NodeRow::new(n.name.clone(), n.node_id.clone(), status);
            r.birth_mut(n.incarnation_id.as_ref().unwrap(), &n.name);
            rows.put_node(&r).await.unwrap();
        }
        let atlas = atlas_deciding(&admin, &topology, &rows, &builds).await;
        let status = status_door_of(&admin, fabric_id, &topology, &rows, &builds);
        let _ = status.atlas.set(atlas.clone());
        let declare = StatusRequest::DeclareNodeState { node_id: caller.node_id.clone(), incarnation: caller_inc.clone(), state: NodeState::Stopped, operation: None };
        let reply = status.apply_from(None, None, &declare).await;
        assert_eq!(reply, StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "the deleted birth's declaration is refused at once");
        // The decision runs on its own task: it ends once its one span closes.
        let (d, spans) = decided(&capture).await;
        let d = d.unwrap_or_else(|| panic!("the declaration door decided the caller: {spans:?}"));
        assert_eq!(d["door"], "declaration", "{d}");
        assert_eq!(d["subject"], "mesh1.rpc.2", "decided at the caller's path.name: {d}");
        assert_eq!(d["own"], "delete-deleted-birth", "{d}");
        assert_eq!(d["decision"], "delete-deleted-birth", "a caller whose record is deleted is deleted, never re-admitted (Q217): {d}");
        assert_eq!(d["needed"], "true", "the current Build still needs the path: node recovery re-creates it: {d}");
        assert_eq!(d["root"], "self", "the fabric-primary decides from its own atlas: {d}");
        // The rig builds no Build door: the termination's open is refused naming it (node_replace's
        // e2e runs it to the runtime's end).
        assert_eq!((d["outcome"].as_str(), d["detail"].as_str()), (Some("terminate-unreported"), Some("mesh1.admin.1: its Build door is not built yet")), "{d}");
        let reported = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-returned-reported").map(|s| s["attributes"].clone()).unwrap_or_else(|| panic!("the caller was reported for node.terminate: {spans:?}"));
        assert_eq!((reported["returned"].as_str(), reported["node_id"].as_str(), reported["incarnation_id"].as_str()), (Some("mesh1.rpc.2"), Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the terminate report names the exact birth: {reported}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (node-investigation.md: "A node that comes back after it was re-created is such a
    /// caller: its re-created birth is at its path.name, and it is deleted"; Q207, Q217; rdm#153, gate
    /// gates-6-9af200cc4): a birth of mesh1.rpc.2 that its mesh-primary commanded back through
    /// node.mesh.join (a held deployment) is deleted and re-created while it is os-stopped. Resumed, it
    /// answers the command with its join report: the row at its path.name now holds the re-created
    /// birth, so the join is refused `JoinMismatch` by name at once, and the join door decides the
    /// caller by its path.name and the atlas: another birth is at its path.name, so it is deleted
    /// (`delete-path-held`), decided at the root itself, and reported for
    /// `node.terminate` (`via-returned-reported` naming the exact birth; the rig builds no Build door,
    /// so the open is refused naming it).
    /// Must NOT happen: the refusal and nothing decided (the red: the deleted birth leaves its mesh,
    /// silent, its runtime never terminated), the caller re-admitted, or the refusal held on the decision.
    #[tokio::test]
    async fn a_deleted_births_join_refused_by_name_is_decided_and_deleted_through_the_join_door() {
        let dir = std::env::temp_dir().join(format!("rdm-refused-caller-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "refused-caller-join").expect("the cell's evidence file can be created");
        let mut admin = birth("mesh1.admin.1", NodeKind::NodeAdmin);
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let caller = birth("mesh1.rpc.2", NodeKind::RpcNode);
        let caller_inc = caller.incarnation_id.clone().unwrap();
        let recreated = birth("mesh1.rpc.2", NodeKind::RpcNode);
        let fabric_id = rdm_core::FabricId::mint();
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process };
        let mesh = crate::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: "mesh1".into(), status: crate::model::ScopeStatus::Ready };
        let planned = Topology { fabric: fabric.clone(), meshes: vec![mesh.clone()], nodes: vec![admin.clone(), recreated.clone()] };
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        crate::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), crate::accepted::FabricTopology::of_observed(&planned), "mesh1.admin.1").await.unwrap();
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric, meshes: vec![mesh], nodes: vec![admin.clone(), recreated.clone()] }));
        // The node records: the admin's and the re-created birth's live, the caller's deleted.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        for (n, status) in [(&admin, NodeStatus::Ready), (&recreated, NodeStatus::Ready), (&caller, NodeStatus::Deleted)] {
            let mut r = NodeRow::new(n.name.clone(), n.node_id.clone(), status);
            r.birth_mut(n.incarnation_id.as_ref().unwrap(), &n.name);
            rows.put_node(&r).await.unwrap();
        }
        let atlas = atlas_deciding(&admin, &topology, &rows, &builds).await;
        // The join door, the caller's deployment held: its mesh-primary commanded it back before the delete.
        let joins = Arc::new(crate::join::Joins::default());
        let deployed = crate::join::Deployed {
            name: caller.name.clone(),
            node_id: caller.node_id.clone(),
            incarnation: caller_inc.clone(),
            supersedes: None,
            endpoint_id: crate::model::EndpointId("caller-endpoint".into()),
            runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap(),
            data_dir: "/data/mesh1.rpc.2".into(),
            last_accepted_name: Some(caller.name.clone()),
            held: true,
            listeners: Vec::new(),
        };
        let _commanded = joins.expect(deployed.clone());
        let door = crate::join::JoinDoor {
            nodes: None,
            me: admin.name.clone(),
            joins,
            answer: Arc::new(|| Box::pin(async { Err("the cell admits no join".to_string()) })),
            types: Arc::new(|_, _| Box::pin(async { Ok(()) })),
            install: Arc::new(|_| {}),
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
            held_hash: Default::default(),
            fabric_id,
            node: rdm_core::MeshNode {
                node_id: caller.node_id.clone(),
                name: caller.name.clone(),
                endpoint_id: deployed.endpoint_id.clone(),
                transport_addr: "127.0.0.1:34567".parse().unwrap(),
                incarnation: caller_inc.clone(),
                supersedes: None,
                runtime: Some(deployed.runtime.clone()),
            },
            status: rdm_core::MemberStatus::Pending,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            data_dir: Some(deployed.data_dir.clone()),
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        };
        let join = rdm_core::join::JoinRequest::JoinNode { digest: rdm_core::wire::WireDigest::from(&report), types: Default::default(), listeners: Vec::new() };
        let reply = door.serve(crate::model::EndpointId("caller-endpoint".into()), join).await;
        assert!(matches!(&reply, rdm_core::join::JoinReply::JoinMismatch { field, deployed, .. } if field == "name" && deployed.contains(recreated.node_id.as_str())), "the deleted birth's join is refused at once by name, naming the re-created birth: {reply:?}");
        // The decision runs on its own task: it ends once its one span closes.
        let (d, spans) = decided(&capture).await;
        let d = d.unwrap_or_else(|| panic!("the join door decided the caller: {spans:?}"));
        assert_eq!(d["door"], "join", "{d}");
        assert_eq!((d["node_id"].as_str(), d["incarnation_id"].as_str()), (Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the exact birth that joined: {d}");
        assert_eq!(d["subject"], "mesh1.rpc.2", "decided at the caller's path.name: {d}");
        assert_eq!(d["decision"], "delete-path-held", "another birth is at the caller's path.name: the caller is deleted (Q207): {d}");
        assert_eq!(d["root"], "self", "the fabric-primary decides from its own atlas: {d}");
        assert_eq!((d["outcome"].as_str(), d["detail"].as_str()), (Some("terminate-unreported"), Some("mesh1.admin.1: its Build door is not built yet")), "{d}");
        let reported = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-returned-reported").map(|s| s["attributes"].clone()).unwrap_or_else(|| panic!("the caller was reported for node.terminate: {spans:?}"));
        assert_eq!((reported["returned"].as_str(), reported["node_id"].as_str(), reported["incarnation_id"].as_str()), (Some("mesh1.rpc.2"), Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the terminate report names the exact birth: {reported}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (node-investigation.md: "A caller whose record is deleted is deleted, its runtime
    /// terminated, never re-admitted"; Q207, Q217; red in lane 6's gate at 7d09688bd, build_op's resumed
    /// executor admitted at its join while its row was deleted and no re-created birth was written yet):
    /// a birth of mesh1.rpc.2 commanded back through node.mesh.join is deleted while it is os-stopped, and
    /// nothing is re-created at its path yet. Resumed, it answers the command with its join report: the row
    /// at its path.name is its own, deleted, so the join is refused `JoinMismatch` on `status` by name at
    /// once, and the join door decides it `delete-deleted-birth` and reports it for `node.terminate`.
    /// Must NOT happen: the deleted birth issued a cert and admitted (the red), or re-admitted.
    #[tokio::test]
    async fn a_deleted_births_join_with_its_own_deleted_row_at_its_path_is_refused_and_deleted() {
        let dir = std::env::temp_dir().join(format!("rdm-refused-caller-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "refused-caller-join-own-deleted").expect("the cell's evidence file can be created");
        let mut admin = birth("mesh1.admin.1", NodeKind::NodeAdmin);
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let caller = birth("mesh1.rpc.2", NodeKind::RpcNode);
        let caller_inc = caller.incarnation_id.clone().unwrap();
        let fabric_id = rdm_core::FabricId::mint();
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process };
        let mesh = crate::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: "mesh1".into(), status: crate::model::ScopeStatus::Ready };
        let planned = Topology { fabric: fabric.clone(), meshes: vec![mesh.clone()], nodes: vec![admin.clone()] };
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        crate::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), crate::accepted::FabricTopology::of_observed(&planned), "mesh1.admin.1").await.unwrap();
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric, meshes: vec![mesh], nodes: vec![admin.clone()] }));
        // The node records: the admin's live, the caller's deleted, and no re-created birth yet.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        for (n, status) in [(&admin, NodeStatus::Ready), (&caller, NodeStatus::Deleted)] {
            let mut r = NodeRow::new(n.name.clone(), n.node_id.clone(), status);
            r.birth_mut(n.incarnation_id.as_ref().unwrap(), &n.name);
            rows.put_node(&r).await.unwrap();
        }
        let atlas = atlas_deciding(&admin, &topology, &rows, &builds).await;
        // The join door, the caller's deployment held: its mesh-primary commanded it back before the delete.
        let joins = Arc::new(crate::join::Joins::default());
        let deployed = crate::join::Deployed {
            name: caller.name.clone(),
            node_id: caller.node_id.clone(),
            incarnation: caller_inc.clone(),
            supersedes: None,
            endpoint_id: crate::model::EndpointId("caller-endpoint".into()),
            runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap(),
            data_dir: "/data/mesh1.rpc.2".into(),
            last_accepted_name: Some(caller.name.clone()),
            held: true,
            listeners: Vec::new(),
        };
        let _commanded = joins.expect(deployed.clone());
        let door = crate::join::JoinDoor {
            nodes: None,
            me: admin.name.clone(),
            joins,
            answer: Arc::new(|| Box::pin(async { Err("the cell admits no join".to_string()) })),
            types: Arc::new(|_, _| Box::pin(async { Ok(()) })),
            install: Arc::new(|_| {}),
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
            held_hash: Default::default(),
            fabric_id,
            node: rdm_core::MeshNode {
                node_id: caller.node_id.clone(),
                name: caller.name.clone(),
                endpoint_id: deployed.endpoint_id.clone(),
                transport_addr: "127.0.0.1:34567".parse().unwrap(),
                incarnation: caller_inc.clone(),
                supersedes: None,
                runtime: Some(deployed.runtime.clone()),
            },
            status: rdm_core::MemberStatus::Pending,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            data_dir: Some(deployed.data_dir.clone()),
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        };
        let join = rdm_core::join::JoinRequest::JoinNode { digest: rdm_core::wire::WireDigest::from(&report), types: Default::default(), listeners: Vec::new() };
        let reply = door.serve(crate::model::EndpointId("caller-endpoint".into()), join).await;
        assert!(matches!(&reply, rdm_core::join::JoinReply::JoinMismatch { field, deployed, .. } if field == "status" && deployed.contains("is deleted")), "the deleted birth's join is refused at once by name, naming its deleted row: {reply:?}");
        // The decision runs on its own task: it ends once its one span closes.
        let (d, spans) = decided(&capture).await;
        let d = d.unwrap_or_else(|| panic!("the join door decided the caller: {spans:?}"));
        assert_eq!(d["door"], "join", "{d}");
        assert_eq!((d["node_id"].as_str(), d["incarnation_id"].as_str()), (Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the exact birth that joined: {d}");
        assert_eq!(d["subject"], "mesh1.rpc.2", "decided at the caller's path.name: {d}");
        assert_eq!(d["decision"], "delete-deleted-birth", "the caller's own record is deleted: it is deleted, never re-admitted (Q217): {d}");
        assert_eq!(d["root"], "self", "the fabric-primary decides from its own atlas: {d}");
        assert_eq!((d["outcome"].as_str(), d["detail"].as_str()), (Some("terminate-unreported"), Some("mesh1.admin.1: its Build door is not built yet")), "{d}");
        let reported = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-returned-reported").map(|s| s["attributes"].clone()).unwrap_or_else(|| panic!("the caller was reported for node.terminate: {spans:?}"));
        assert_eq!((reported["returned"].as_str(), reported["node_id"].as_str(), reported["incarnation_id"].as_str()), (Some("mesh1.rpc.2"), Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the terminate report names the exact birth: {reported}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (node-rpc-envelope.md:726: a primary that refuses a status declaration's caller birth
    /// `SubjectUnknown` then decides that birth; node-investigation.md: a node-admin that is not the
    /// fabric-primary verifies its decision at the root with `fabric.atlas.get` before it acts; Q207;
    /// rdm#177, the 1a6d0af25 release red): mesh2's primary,
    /// mesh2.admin.2, is cut from the fabric-primary, so its view names none, and a new birth of
    /// mesh2.admin.1 the fabric-primary created declares Ready to it. The declaration is refused
    /// `SubjectUnknown` at once and the caller's decision is owed to the root: nothing is asked while no
    /// view names a fabric-primary, and once a view naming it is installed the decision is verified there
    /// (its atlas holds the birth at mesh2.admin.1) and the caller is re-admitted (`re-admit`, root
    /// `agrees`; the rig builds no heard-again door, so `no-heard-again`), in one decision.
    /// Must NOT happen: the decision ended `root-unverified` with nothing deciding it again (the red: the
    /// declarer's retry ended on the refusal, the birth never declares again, and the Build's claim on it
    /// is withdrawn), or a second decision of the same birth.
    #[tokio::test]
    async fn a_caller_refused_with_no_fabric_primary_in_view_is_decided_once_the_fabric_primary_enters_the_view() {
        use rdm_core::views::{AtlasNodeView, AtlasView};
        let dir = std::env::temp_dir().join(format!("rdm-refused-caller-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "refused-caller-root").expect("the cell's evidence file can be created");
        let mut admin = birth("mesh2.admin.2", NodeKind::NodeAdmin);
        admin.is_primary = true;
        let mut fp = birth("mesh1.admin.1", NodeKind::NodeAdmin);
        fp.is_primary = true;
        fp.is_fabric_primary = true;
        let mut caller = birth("mesh2.admin.1", NodeKind::NodeAdmin);
        caller.status = NodeStatus::Pending;
        let caller_inc = caller.incarnation_id.clone().unwrap();
        let fabric_id = rdm_core::FabricId::mint();
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::Pending, provider: crate::model::ProviderKind::Process };
        let meshes: Vec<crate::model::Mesh> = ["mesh1", "mesh2"].into_iter().map(|m| crate::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: m.into(), status: crate::model::ScopeStatus::Pending }).collect();
        // The current Build names the caller's path: it is needed.
        let planned = Topology { fabric: fabric.clone(), meshes: meshes.clone(), nodes: vec![fp.clone(), admin.clone(), caller.clone()] };
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        crate::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), crate::accepted::FabricTopology::of_observed(&planned), "mesh1.admin.1").await.unwrap();
        // mesh2.admin.2's view across the cut: itself only, no fabric-primary.
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric, meshes, nodes: vec![admin.clone()] }));
        // The node records of mesh2: the admin's and the caller's, both live.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        for n in [&admin, &caller] {
            let mut r = NodeRow::new(n.name.clone(), n.node_id.clone(), n.status);
            r.birth_mut(n.incarnation_id.as_ref().unwrap(), &n.name);
            rows.put_node(&r).await.unwrap();
        }
        // The fabric-primary: its door answers fabric.atlas.get from its atlas, which holds the caller's
        // birth at mesh2.admin.1, and counts the reads.
        let fp_key = iroh::SecretKey::generate();
        let fp_ep = rdm_transport::endpoint::bind(fp_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let fp_addr = fp_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let root = AtlasView { nodes: vec![AtlasNodeView { name: caller.name.clone(), node_id: caller.node_id.clone(), incarnation_id: Some(caller_inc.clone()), status: rdm_core::views::NodeStatus::Pending, stopped: false, accepted: None, observed: None }] };
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
        resolver.insert(rdm_transport::ResolvedNode { node_id: fp.node_id.clone(), name: fp.name.clone(), endpoint_id: fp_key.public(), transport_addr: fp_addr, incarnation: fp.incarnation_id.clone().unwrap() });
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(rdm_transport::NodeRpcClient::new(ep, resolver));
        let (atlas, callers) = atlas_deciding_over(&admin, &topology, &rows, &builds, Some(client)).await;
        let (views, installed) = tokio::sync::watch::channel(0u64);
        callers.watch_views(installed);
        let status = status_door_of(&admin, fabric_id, &topology, &rows, &builds);
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
        assert_eq!(d["door"], "declaration", "{d}");
        assert_eq!(d["subject"], "mesh2.admin.1", "decided at the caller's path.name: {d}");
        assert_eq!((d["node_id"].as_str(), d["incarnation_id"].as_str()), (Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the exact birth that declared: {d}");
        assert_eq!(d["root"], "agrees", "verified at the fabric-primary once a view names it: {d}");
        assert_eq!(d["decision"], "re-admit", "nothing else at its path.name, its record live and the Build needing it: re-admitted (Q207): {d}");
        assert_eq!(d["outcome"], "no-heard-again", "the rig builds no heard-again door: {d}");
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 1, "the root was read once");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (node-investigation.md: "A deleted caller is reported to the fabric-primary ..., which
    /// terminates its runtime under `node.terminate:<path>@<incarnation>`"; node-recovery.md step 3: "A
    /// birth that comes back after it was re-created is a deleted birth ... it is reported to the
    /// fabric-primary, which terminates its runtime"; build.md: "an os-stopped executor that resumes
    /// after the next attempt is claimed is a deleted birth"; rdm#179, gate gates-3-072e4f3a0): mesh2's
    /// primary, mesh2.admin.2, decides a returned birth of mesh2.admin.1 whose path.name holds its
    /// re-created birth (`delete-path-held`, root `agrees`) while the recovery that re-created it still
    /// holds mesh2, so the fabric-primary answers the first `Terminate` `BuildInProgress`. The report
    /// stays owed: it is sent again on the resend schedule and the fabric-primary opens the
    /// termination at the second (`via-returned-reported` outcome `opened`, two sends, naming the Build
    /// and attempt), one decision throughout.
    /// Must NOT happen: the report counted `sent` as its stream opened, the refusal unread and nothing
    /// sending it again (the red: the resumed runtime is never terminated), or a second decision.
    #[tokio::test]
    async fn a_returned_births_terminate_refused_build_in_progress_stays_owed_until_the_fabric_primary_opens_it() {
        use rdm_core::build::{Build, BuildChange, BuildReply, BuildRequest, BuildSubmit, Disposition};
        use rdm_core::views::{AtlasNodeView, AtlasView};
        let dir = std::env::temp_dir().join(format!("rdm-refused-caller-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "refused-caller-terminate-owed").expect("the cell's evidence file can be created");
        let mut admin = birth("mesh2.admin.2", NodeKind::NodeAdmin);
        admin.is_primary = true;
        let mut fp = birth("mesh1.admin.1", NodeKind::NodeAdmin);
        fp.is_primary = true;
        fp.is_fabric_primary = true;
        let caller = birth("mesh2.admin.1", NodeKind::NodeAdmin);
        let caller_inc = caller.incarnation_id.clone().unwrap();
        let recreated = birth("mesh2.admin.1", NodeKind::NodeAdmin);
        let fabric_id = rdm_core::FabricId::mint();
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process };
        let meshes: Vec<crate::model::Mesh> = ["mesh1", "mesh2"].into_iter().map(|m| crate::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: m.into(), status: crate::model::ScopeStatus::Ready }).collect();
        let planned = Topology { fabric: fabric.clone(), meshes: meshes.clone(), nodes: vec![fp.clone(), admin.clone(), recreated.clone()] };
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        crate::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), crate::accepted::FabricTopology::of_observed(&planned), "mesh1.admin.1").await.unwrap();
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric, meshes, nodes: vec![fp.clone(), admin.clone(), recreated.clone()] }));
        // The node records of mesh2: the admin's and the re-created birth's live, the returned birth's deleted.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        for (n, status) in [(&admin, NodeStatus::Ready), (&recreated, NodeStatus::Ready), (&caller, NodeStatus::Deleted)] {
            let mut r = NodeRow::new(n.name.clone(), n.node_id.clone(), status);
            r.birth_mut(n.incarnation_id.as_ref().unwrap(), &n.name);
            rows.put_node(&r).await.unwrap();
        }
        // The fabric-primary: its atlas holds the re-created birth at mesh2.admin.1; its Build door answers
        // the first Terminate BuildInProgress (the recovery's attempt holds mesh2) and opens the next.
        let fp_key = iroh::SecretKey::generate();
        let fp_ep = rdm_transport::endpoint::bind(fp_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let fp_addr = fp_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let root = AtlasView { nodes: vec![AtlasNodeView { name: recreated.name.clone(), node_id: recreated.node_id.clone(), incarnation_id: recreated.incarnation_id.clone(), status: rdm_core::views::NodeStatus::Ready, stopped: false, accepted: None, observed: None }] };
        let terminates = Arc::new(Mutex::new(Vec::<(String, String, String)>::new()));
        let asked = terminates.clone();
        let server = rdm_transport::ServerBuilder::new()
            .serve::<rdm_core::status::Status, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_peer, req: StatusRequest| {
                let root = root.clone();
                async move {
                    Ok(match req {
                        StatusRequest::GetAtlas => StatusReply::Atlas { atlas: root },
                        other => StatusReply::NotReady { reason: format!("the cell's fabric-primary answers fabric.atlas.get only, not {}", other.op()) },
                    })
                }
            })
            .serve_stream::<Build, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_peer, req: BuildRequest, sink| {
                let asked = asked.clone();
                async move {
                    let BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::Terminate { node, node_id, incarnation }) } = req else {
                        return Ok(rdm_core::reply::build::malformed(rdm_core::outcome::MalformedKind::UnknownVariant));
                    };
                    let sends = {
                        let mut a = asked.lock().unwrap();
                        a.push((node.to_string(), node_id.to_string(), incarnation.as_str().to_string()));
                        a.len()
                    };
                    if sends == 1 {
                        return Ok(rdm_core::reply::build::build_in_progress("b7".into(), Some("mesh2".into()), Some("mesh1.admin.1".into())));
                    }
                    let _streaming = sink.started(BuildReply::Started { build_id: "b8".into(), attempt: 8, disposition: Disposition::Created }).await.map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                    Ok(BuildReply::Complete { build_id: "b8".into(), attempt: 8 })
                }
            })
            .build(rdm_transport::ServedBirth { node_id: fp.node_id.to_string(), incarnation: fp.incarnation_id.as_ref().unwrap().as_str().to_string() })
            .unwrap();
        let _router = iroh::protocol::Router::builder(fp_ep).accept(rdm_transport::ALPN, server).spawn();
        let resolver = Arc::new(rdm_transport::StaticResolver::new());
        resolver.insert(rdm_transport::ResolvedNode { node_id: fp.node_id.clone(), name: fp.name.clone(), endpoint_id: fp_key.public(), transport_addr: fp_addr, incarnation: fp.incarnation_id.clone().unwrap() });
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(rdm_transport::NodeRpcClient::new(ep, resolver));
        let (atlas, callers) = atlas_deciding_over(&admin, &topology, &rows, &builds, Some(client)).await;
        let (_views, installed) = tokio::sync::watch::channel(0u64);
        callers.watch_views(installed);
        let (decision, outcome) = callers.decide(&atlas, "join", &caller.node_id, &caller_inc).await.expect("one decision of the returned birth");
        assert_eq!(decision.name(), "delete-path-held", "the re-created birth is at the returned birth's path.name (Q207)");
        assert_eq!(outcome, "terminate-reported", "the termination is reported once the fabric-primary opened it");
        let (_, spans) = decided(&capture).await;
        let decisions: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.update.via-refused-caller").map(|s| s["attributes"].clone()).collect();
        assert_eq!(decisions.len(), 1, "one decision of the birth: {decisions:?}");
        assert_eq!(decisions[0]["root"], "agrees", "{}", decisions[0]);
        let reported = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-returned-reported").map(|s| s["attributes"].clone()).unwrap_or_else(|| panic!("the returned birth was reported for node.terminate: {spans:?}"));
        assert_eq!((reported["returned"].as_str(), reported["node_id"].as_str(), reported["incarnation_id"].as_str()), (Some("mesh2.admin.1"), Some(caller.node_id.as_str()), Some(caller_inc.as_str())), "the report names the exact birth: {reported}");
        let sent = terminates.lock().unwrap().clone();
        assert_eq!(sent.len(), 2, "the Terminate refused BuildInProgress is sent again until the fabric-primary opens it: {sent:?} {reported}");
        assert!(sent.iter().all(|t| *t == ("mesh2.admin.1".to_string(), caller.node_id.to_string(), caller_inc.as_str().to_string())), "every send names the exact birth: {sent:?}");
        assert_eq!((reported["outcome"].as_str(), reported["sends"].as_str(), reported["build_id"].as_str(), reported["attempt"].as_str()), (Some("opened"), Some("2"), Some("b8"), Some("8")), "the report ends once the fabric-primary opened the termination, naming its Build and attempt: {reported}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (docs/architecture/lifecycles/node-investigation.md: "A caller whose record is deleted
    /// is deleted, its runtime terminated, never re-admitted"; docs/architecture/node-rpc.md §43: "an
    /// internal command is owed until it is answered or its target is gone"; Q217): when a mesh-primary
    /// (`mesh2.admin.2`) decides a caller (`mesh2.admin.1`) `delete-deleted-birth` and reports its
    /// termination before the fabric-primary holds the delete's `NodeDeleted` receipt or deleted row,
    /// `ControlPlane::open_terminate` refuses `unknown-node`, `RecoverySubmitter::report_terminate` keeps
    /// the termination report owed, and once the current Build holds the birth's `NodeDeleted` receipt,
    /// the next send is admitted (`Recovery::Opened`).
    #[tokio::test]
    async fn a_deleted_births_terminate_refused_unknown_node_stays_owed_until_the_fabric_primary_holds_its_node_deleted_receipt() {
        use rdm_core::build::{Build, BuildChange, BuildReply, BuildRequest, BuildSubmit, Disposition};
        use rdm_core::views::AtlasView;
        let dir = std::env::temp_dir().join(format!("rdm-refused-caller-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "refused-caller-terminate-unknown-node-owed").expect("the cell's evidence file can be created");
        let mut admin = birth("mesh2.admin.2", NodeKind::NodeAdmin);
        admin.is_primary = true;
        let mut fp = birth("mesh1.admin.1", NodeKind::NodeAdmin);
        fp.is_primary = true;
        fp.is_fabric_primary = true;
        let caller = birth("mesh2.admin.1", NodeKind::NodeAdmin);
        let caller_inc = caller.incarnation_id.clone().unwrap();
        let fabric_id = rdm_core::FabricId::mint();
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process };
        let meshes: Vec<crate::model::Mesh> = ["mesh1", "mesh2"].into_iter().map(|m| crate::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: m.into(), status: crate::model::ScopeStatus::Ready }).collect();
        let planned = Topology { fabric: fabric.clone(), meshes: meshes.clone(), nodes: vec![fp.clone(), admin.clone(), caller.clone()] };
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        crate::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), crate::accepted::FabricTopology::of_observed(&planned), "mesh1.admin.1").await.unwrap();
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric: fabric.clone(), meshes: meshes.clone(), nodes: vec![fp.clone(), admin.clone()] }));
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        for (n, status) in [(&admin, NodeStatus::Ready), (&caller, NodeStatus::Deleted)] {
            let mut r = NodeRow::new(n.name.clone(), n.node_id.clone(), status);
            r.birth_mut(n.incarnation_id.as_ref().unwrap(), &n.name);
            rows.put_node(&r).await.unwrap();
        }

        // The fabric-primary's real ControlPlane: its atlas holds no deleted row of caller yet, and its
        // Build state receives the NodeDeleted receipt only after the first open_terminate is refused.
        let fp_builds: Arc<dyn crate::build_state::BuildStateAdapter> = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        let fp_accepted = crate::accepted::AcceptedStore::seeded(&*fp_builds, fabric_id, crate::accepted::FabricTopology::of_observed(&planned), "mesh1.admin.1").await.unwrap();
        let before = fp_builds.current().await.unwrap().unwrap();
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(1_000);
        let cp = Arc::new(crate::control_plane::ControlPlane::new(fp_builds.clone(), fp_accepted, fp.name.clone(), planned.clone(), time));

        let fp_key = iroh::SecretKey::generate();
        let fp_ep = rdm_transport::endpoint::bind(fp_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let fp_addr = fp_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let terminates = Arc::new(Mutex::new(Vec::<(String, String, String)>::new()));
        let asked = terminates.clone();
        let cp_served = cp.clone();
        let fp_builds_served = fp_builds.clone();
        let before_id = before.build_id.clone();
        let before_attempt = before.attempt;
        let server = rdm_transport::ServerBuilder::new()
            .serve::<rdm_core::status::Status, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_peer, req: StatusRequest| async move {
                Ok(match req {
                    StatusRequest::GetAtlas => StatusReply::Atlas { atlas: AtlasView { nodes: Vec::new() } },
                    other => StatusReply::NotReady { reason: format!("the cell's fabric-primary answers fabric.atlas.get only, not {}", other.op()) },
                })
            })
            .serve_stream::<Build, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_peer, req: BuildRequest, sink| {
                let asked = asked.clone();
                let cp = cp_served.clone();
                let fp_builds = fp_builds_served.clone();
                let before_id = before_id.clone();
                async move {
                    let BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::Terminate { node, node_id, incarnation }) } = req else {
                        return Ok(rdm_core::reply::build::malformed(rdm_core::outcome::MalformedKind::UnknownVariant));
                    };
                    let sends = {
                        let mut a = asked.lock().unwrap();
                        a.push((node.to_string(), node_id.to_string(), incarnation.as_str().to_string()));
                        a.len()
                    };
                    let span = tracing::info_span!("rdm.node_admin.build.update.via-terminate-returned", route = "returned-birth", node = %node, node_id = %node_id, incarnation_id = %incarnation.as_str());
                    match cp.open_terminate(span, "returned-birth", node.clone(), node_id.clone(), incarnation.clone()).await {
                        Ok(crate::control_plane::Recovery::Opened(o)) => {
                            let _streaming = sink.started(BuildReply::Started { build_id: o.build_id.to_string(), attempt: o.attempt, disposition: Disposition::Created }).await.map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                            Ok(BuildReply::Complete { build_id: o.build_id.to_string(), attempt: o.attempt })
                        }
                        Ok(other) => panic!("expected Opened on send {sends}, got {other:?}"),
                        Err(r) => {
                            assert_eq!(sends, 1, "only the first send before NodeDeleted is refused: {r:?}");
                            let operation = format!("node.delete:{node}");
                            let op = rdm_core::LifecycleOp { build_id: before_id.to_string(), attempt: before_attempt, operation: operation.clone(), node_id: node_id.clone(), incarnation: incarnation.clone(), name: node.clone(), event_at_rafka_ms: 0, traceparent: None, tracestate: None };
                            fp_builds
                                .append_step_receipt(&crate::build_state::BuildStepReceipt { mesh: None, build_id: before_id, attempt: before_attempt, operation, step: "NodeDeleted".into(), outcome: crate::build_state::StepOutcome::Complete, output: serde_json::to_value(&op).ok(), executor: None })
                                .await
                                .unwrap();
                            Ok(crate::build_op::refusal_reply(r))
                        }
                    }
                }
            })
            .build(rdm_transport::ServedBirth { node_id: fp.node_id.to_string(), incarnation: fp.incarnation_id.as_ref().unwrap().as_str().to_string() })
            .unwrap();
        let router = iroh::protocol::Router::builder(fp_ep.clone()).accept(rdm_transport::ALPN, server).spawn();
        let resolver = Arc::new(rdm_transport::StaticResolver::new());
        resolver.insert(rdm_transport::ResolvedNode { node_id: fp.node_id.clone(), name: fp.name.clone(), endpoint_id: fp_key.public(), transport_addr: fp_addr, incarnation: fp.incarnation_id.clone().unwrap() });
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(rdm_transport::NodeRpcClient::new(ep.clone(), resolver));
        let (atlas, callers) = atlas_deciding_over(&admin, &topology, &rows, &builds, Some(client)).await;
        let (_views, installed) = tokio::sync::watch::channel(0u64);
        callers.watch_views(installed);
        let (decision, outcome) = callers.decide(&atlas, "join", &caller.node_id, &caller_inc).await.expect("one decision of the deleted birth");
        assert_eq!(decision.name(), "delete-deleted-birth", "the caller's own row is deleted (Q217)");
        assert_eq!(outcome, "terminate-reported", "the termination report stays owed on unknown-node and succeeds once NodeDeleted is held");
        let (_, spans) = decided(&capture).await;
        let reported = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-returned-reported").map(|s| s["attributes"].clone()).unwrap_or_else(|| panic!("the deleted birth was reported for node.terminate: {spans:?}"));
        let sent = terminates.lock().unwrap().clone();
        assert_eq!(sent.len(), 2, "the Terminate refused unknown-node is sent again and admitted once NodeDeleted is held: {sent:?} {reported}");
        assert_eq!(
            (reported["outcome"].as_str(), reported["sends"].as_str(), reported["build_id"].as_str(), reported["attempt"].as_str()),
            (Some("opened"), Some("2"), Some(before.build_id.as_str()), Some((before.attempt + 1).to_string().as_str())),
            "the report opens the next attempt of the current Build on the second send: {reported}"
        );
        ep.close().await;
        let _ = router.shutdown().await;
        fp_ep.close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
