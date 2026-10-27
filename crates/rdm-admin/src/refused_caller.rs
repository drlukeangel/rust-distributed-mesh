//! A refused caller (Q207, Luke 2026-10-27): "in general a FP/MP looks to see if something at that
//! path.name exists currently; if it does it should delete the caller; if it doesn't it should check
//! atlas; if it's needed it should re-admit; if it's not it should delete."
//!
//! One decision for every primary and every door a caller birth it does not hold comes through (a
//! status declaration refused `SubjectUnknown`, a deleted birth's Digest heard again): decided from
//! this admin's own atlas ([`crate::fabric_atlas::FabricAtlas::refused_caller`]), and, at an admin that
//! is not the fabric-primary, verified at the root through `fabrics.atlas.get` before anything acts (the
//! root's answer wins: this admin may have missed an op). A caller deleted is reported to the
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

use crate::fabric_atlas::{CallerDecision, FabricAtlas};
use crate::model::{IncarnationId, NodeId, NodeStatus, PathName};

/// What a refused caller's decision acts through: this admin's recovery submitter (the terminate
/// report) and its heard-again door (the re-admission), filled once both exist.
pub struct RefusedCallers {
    pub(crate) node_id: NodeId,
    pub(crate) recovery: crate::admin::RecoverySubmitter,
    pub(crate) heard_again: crate::heard_again::HeardAgainSlot,
    in_flight: Mutex<BTreeSet<(String, String)>>,
}

impl std::fmt::Debug for RefusedCallers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefusedCallers").field("node_id", &self.node_id).finish_non_exhaustive()
    }
}

impl RefusedCallers {
    pub(crate) fn new(node_id: NodeId, recovery: crate::admin::RecoverySubmitter, heard_again: crate::heard_again::HeardAgainSlot) -> Self {
        Self { node_id, recovery, heard_again, in_flight: Mutex::default() }
    }

    /// Decide the caller birth `(node_id, incarnation)` refused at `door` (`declaration` or `digest`)
    /// and act on it, once per birth at a time: one `rdm.node_admin.node.update.via-refused-caller`
    /// span naming the decision, whether the fabric-primary verified it, and what was done. The
    /// decision, or `None` when one for the birth is already in flight.
    pub(crate) async fn decide(&self, atlas: &FabricAtlas, door: &'static str, node_id: &NodeId, incarnation: &IncarnationId) -> Option<(CallerDecision, String)> {
        let key = (node_id.to_string(), incarnation.as_str().to_string());
        if !self.in_flight.lock().unwrap().insert(key.clone()) {
            return None;
        }
        let me = self.recovery.held_name();
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-refused-caller",
            node = %me, door, node_id = %node_id, incarnation_id = %incarnation.as_str(),
            subject = tracing::field::Empty, decision = tracing::field::Empty, own = tracing::field::Empty,
            root = tracing::field::Empty, needed = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal"
        );
        let decided = tracing::Instrument::instrument(self.decide_and_act(atlas, &span, node_id, incarnation), span.clone()).await;
        self.in_flight.lock().unwrap().remove(&key);
        span.in_scope(|| tracing::info!("a caller birth this primary does not hold was decided by its path.name and the atlas"));
        Some(decided)
    }

    async fn decide_and_act(&self, atlas: &FabricAtlas, span: &tracing::Span, node_id: &NodeId, incarnation: &IncarnationId) -> (CallerDecision, String) {
        // The topology's need: the current Build's paths, by mesh.
        let planned: BTreeMap<String, Vec<PathName>> = match self.recovery.builds.current().await {
            Ok(Some(b)) => b.topology.meshes.iter().map(|(m, t)| (m.clone(), t.nodes.iter().cloned().collect())).collect(),
            Ok(None) => BTreeMap::new(),
            Err(e) => {
                span.record("outcome", "build-unread").record("detail", format!("the current Build could not be read: {e}").as_str());
                return (CallerDecision::NoRecord, "build-unread".into());
            }
        };
        let own = atlas.refused_caller(node_id, incarnation, &planned);
        span.record("own", own.name());
        if let Some(path) = own.path() {
            span.record("subject", path.to_string().as_str());
        }
        let Some(path) = own.path().cloned() else {
            span.record("decision", own.name()).record("outcome", "nothing-done").record("detail", "this admin's atlas holds no record of the caller: it has no path.name to decide on");
            return (own, "nothing-done".into());
        };
        if matches!(own, CallerDecision::NotMine { .. }) {
            span.record("decision", own.name()).record("outcome", "nothing-done").record("detail", format!("{} is not a mesh this admin is primary for", path.mesh).as_str());
            return (own, "nothing-done".into());
        }
        // A node-admin that is not the fabric-primary verifies at the root before it acts.
        let fabric_primary = self.recovery.topology.read().await.fabric_primary().map(|n| (n.name.clone(), n.node_id.clone()));
        let decided = match fabric_primary {
            Some((_, fp_id)) if fp_id == self.node_id => {
                span.record("root", "self");
                own
            }
            Some((fp_name, fp_id)) => {
                let Some(client) = self.recovery.client.clone() else {
                    span.record("decision", own.name()).record("outcome", "root-unverified").record("detail", "this admin has no node-RPC client to call the fabric-primary");
                    return (own, "root-unverified".into());
                };
                let (out, _) = client.call::<rdm_core::status::Status>(&rdm_transport::NodeTarget::ExactNode(fp_id), &rdm_core::status::StatusRequest::GetAtlas, &rdm_transport::CallOptions::default()).await;
                match out.reply().map(|r| r.value().clone()) {
                    Some(rdm_core::status::StatusReply::Atlas { atlas: root }) => {
                        let needed = planned.get(&path.mesh).is_some_and(|p| p.contains(&path));
                        let verified = own.clone().verified((node_id, incarnation), &root, needed);
                        span.record("root", if verified == own { "agrees" } else { "differs" });
                        verified
                    }
                    other => {
                        let why = other.map(|r| r.name().to_string()).unwrap_or_else(|| out.name().to_string());
                        span.record("decision", own.name()).record("outcome", "root-unverified").record("detail", format!("fabrics.atlas.get at {fp_name} ended {why}: nothing acts until the root answers").as_str());
                        return (own, "root-unverified".into());
                    }
                }
            }
            None => {
                span.record("decision", own.name()).record("outcome", "root-unverified").record("detail", "no fabric-primary in this admin's view to verify at");
                return (own, "root-unverified".into());
            }
        };
        span.record("decision", decided.name());
        // A deleted birth's path the topology still needs is node recovery's to re-create (Q217).
        if let CallerDecision::DeleteDeletedBirth { needed, .. } = &decided {
            span.record("needed", *needed);
        }
        let outcome = match &decided {
            CallerDecision::DeleteHeld { .. } | CallerDecision::DeleteUnneeded { .. } | CallerDecision::DeleteDeletedBirth { .. } => match self.recovery.report_terminate(self.recovery.client.as_ref(), path.clone(), node_id.clone(), incarnation.clone()).await {
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
}

impl FabricAtlas {
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

    fn birth(path: &str, kind: NodeKind) -> Node {
        let mut n = Node::allocated(path.parse().unwrap());
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(IncarnationId::mint());
        n.mesh = n.name.mesh.clone();
        n.kind = kind;
        n.status = NodeStatus::ReadyForTraffic;
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
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process };
        let mesh = crate::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: "mesh1".into(), status: crate::model::ScopeStatus::ReadyForTraffic };
        // The current Build names both paths: the deleted caller's path is still needed.
        let planned = Topology { fabric: fabric.clone(), meshes: vec![mesh.clone()], nodes: vec![admin.clone(), caller.clone()] };
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        crate::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), crate::accepted::FabricTopology::of_observed(&planned), "mesh1.admin.1").await.unwrap();
        // This admin's view holds itself only: the caller's Digest is refused as a deleted birth.
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric, meshes: vec![mesh], nodes: vec![admin.clone()] }));
        // The node records: the admin's live, the caller's deleted.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        for (n, status) in [(&admin, NodeStatus::ReadyForTraffic), (&caller, NodeStatus::Deleted)] {
            let mut r = NodeRow::new(n.name.clone(), n.node_id.clone(), status);
            r.birth_mut(n.incarnation_id.as_ref().unwrap(), &n.name);
            rows.put_node(&r).await.unwrap();
        }
        let atlas = Arc::new(FabricAtlas::default());
        atlas.scope(&*rows, ["mesh1".to_string()].into()).await.unwrap();
        let recovery = crate::admin::RecoverySubmitter {
            me: admin.name.clone(),
            topology: topology.clone(),
            digest_view: Default::default(),
            provider: Arc::new(crate::deployment::process::ProcessDeploymentProvider::new()),
            nodes_storage: rows.clone(),
            written: Default::default(),
            door: Default::default(),
            client: None,
            builds: builds.clone(),
            data_root: Default::default(),
            gone_dirs: Default::default(),
            resubmits: Default::default(),
        };
        atlas.refused_callers(Arc::new(RefusedCallers::new(admin.node_id.clone(), recovery, Default::default())));
        let authority = crate::status_rpc::StatusAuthority {
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
        };
        let _ = authority.atlas.set(atlas.clone());
        let declare = StatusRequest::DeclareNodeState { node_id: caller.node_id.clone(), incarnation: caller_inc.clone(), state: NodeState::Stopped, operation: None };
        let reply = authority.apply_from(None, None, &declare).await;
        assert_eq!(reply, StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "the deleted birth's declaration is refused at once");
        // The decision runs on its own task: it ends once its one span closes.
        let decided = |spans: &[serde_json::Value]| spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-refused-caller").map(|s| s["attributes"].clone());
        let mut spans = capture.spans().expect("the cell's spans can be read");
        for _ in 0..200 {
            if decided(&spans).is_some() {
                break;
            }
            tokio::task::yield_now().await;
            spans = capture.spans().expect("the cell's spans can be read");
        }
        let d = decided(&spans).unwrap_or_else(|| panic!("the declaration door decided the caller: {spans:?}"));
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
}
