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
            root = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal"
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
        let outcome = match &decided {
            CallerDecision::DeleteHeld { .. } | CallerDecision::DeleteUnneeded { .. } => match &self.recovery.client {
                Some(client) => match self.recovery.report_terminate(client, path.clone(), node_id.clone(), incarnation.clone()).await {
                    Ok(()) => "terminate-reported".to_string(),
                    Err(e) => {
                        span.record("detail", e.as_str());
                        "terminate-unreported".to_string()
                    }
                },
                None => "no-client".to_string(),
            },
            // The existing admission, node.mesh.join, takes a birth whose record is live: the join door
            // verifies the exact birth against the deployment its record holds. A caller whose record is
            // deleted and whose name is released has none to rejoin through.
            CallerDecision::ReAdmit { deleted: true, .. } => {
                span.record("detail", format!("{path}: the caller's record is deleted and its name released; node.mesh.join admits only a birth whose record is live").as_str());
                "re-admit-not-admissible".to_string()
            }
            CallerDecision::ReAdmit { deleted: false, .. } => match self.heard_again.get() {
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
