//! A birth heard again (Q87, Luke 2026-10-20): every node coming to ready-for-traffic from any other
//! state connects to its node-admin for its cert, its time and its cache, and the `before_ready`
//! hook is where it does that; node-admins included, the Day-0 fabric-primary excepted. When the
//! tickle reaches a birth its authority holds off its mesh (node-recovery.md step 2, node-tickle.md
//! "What follows"), the authority begins the birth's next cycle and commands `node.mesh.join` to it,
//! back over its own call; the birth runs node.start steps 2 to 5 again on it and is published
//! ready-for-traffic only once its hook has passed (node-mesh-join.md step 10). One function, run by
//! the mesh-primary's offline ladder for its members and by the fabric-primary's seat watch for a
//! held seat holder; nothing re-publishes a presence by hand. `AdminRunner` registers the birth's
//! join (as it does a parked birth's) so the join door admits the report.

use crate::model::{IncarnationId, Node};
use crate::status_rpc::AuthoritySlot;
use rdm_core::status::{Status, StatusRequest};
use rdm_transport::membership::DigestBook;
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

/// The births this process commanded back and has not heard ready-for-traffic since: one command
/// per silence. A watcher looks at a silent birth every pass; the re-entry it commanded takes the
/// birth off its channels for the length of its start, and a second command inside that would
/// start it over.
fn commanded() -> &'static Mutex<HashMap<(String, String), Instant>> {
    static COMMANDED: OnceLock<Mutex<HashMap<(String, String), Instant>>> = OnceLock::new();
    COMMANDED.get_or_init(Default::default)
}

/// What a watcher needs to command a birth back: this admin's client, status authority, Build state,
/// runner (once built) and digest book. One handle, held by the offline ladder, the seat watch and
/// the round driver's down op.
pub struct HeardAgain {
    /// This admin's Node RPC client: the command goes out on it.
    pub client: Arc<NodeRpcClient>,
    /// This admin's status authority: the birth's next cycle begins there.
    pub authority: AuthoritySlot,
    /// This admin's Build state: the current Build the command names.
    pub builds: Arc<dyn crate::build_state::BuildStateAdapter>,
    /// This admin's runner, once built: it registers the birth's join before the command.
    pub runner: Arc<OnceLock<Arc<crate::admin::runner::AdminRunner>>>,
    /// This admin's digest book: whether the birth has been heard ready-for-traffic since.
    pub book: DigestBook,
}

/// The handle, once this admin's runner is built.
pub type HeardAgainSlot = Arc<OnceLock<Arc<HeardAgain>>>;

impl HeardAgain {
    /// Command `node`'s exact birth back through `node.mesh.join` ([`command_mesh_join`]).
    pub async fn command(&self, node: &Node, incarnation: &IncarnationId) -> String {
        let Some(runner) = self.runner.get() else {
            return "no-runner".to_string();
        };
        command_mesh_join(&self.client, &self.authority, &self.builds, runner, &self.book, node, incarnation).await
    }
}

/// Begin `node`'s next cycle (its declared state is measured from Pending again) and command
/// `node.mesh.join` to its exact birth, naming the authority's current Build; the reply's name.
/// One span, `rdm.node_admin.node.update.via-mesh-join-commanded`, with the outcome.
async fn command_mesh_join(
    client: &Arc<NodeRpcClient>,
    authority: &AuthoritySlot,
    builds: &Arc<dyn crate::build_state::BuildStateAdapter>,
    runner: &crate::admin::runner::AdminRunner,
    book: &DigestBook,
    node: &Node,
    incarnation: &IncarnationId,
) -> String {
    // The Day-0 fabric-primary, and any fabric seat holder: no node above it to connect to (Luke,
    // Q87); it is never commanded back.
    if node.is_fabric_primary {
        return "fabric-primary: no authority above it".to_string();
    }
    // Only a birth the view holds off ready-for-traffic re-enters (Luke: "coming from a state that is
    // not ready-for-traffic"). A watcher's own fresh book (an admin just started holds every member
    // from the projection, unheard until their digests reach it) is not the birth's state; the view is.
    if node.status == crate::model::NodeStatus::ReadyForTraffic {
        return "ready-in-view: on its mesh".to_string();
    }
    // A birth with a lifecycle command of its authority in flight (a stop, a start, a restart) is that
    // command's: the start it runs is the command's own (node-start.md), never this one.
    if let Some(op) = book.in_flight().into_iter().find(|op| op.node_id == node.node_id && op.incarnation == *incarnation) {
        return format!("in-flight: {}", op.operation);
    }
    // Once per silence: a birth commanded back is commanded again only after a staleness floor, and
    // only once this process has heard it ready-for-traffic since (the watcher looks at it every pass
    // while its view still holds it silent).
    let key = (node.node_id.to_string(), incarnation.as_str().to_string());
    if let Some(at) = commanded().lock().unwrap().get(&key).copied() {
        if at.elapsed() < book.staleness_floor() {
            return "commanded-already".to_string();
        }
        let heard_ready_since = book
            .get(node.node_id.as_str())
            .is_some_and(|(d, unheard)| d.node.incarnation == *incarnation && d.status == rdm_core::MemberStatus::ReadyForTraffic && Instant::now().checked_sub(unheard).is_some_and(|heard_at| heard_at > at));
        // Heard ready-for-traffic since, or unheard again (a re-entry that stopped at a step left the
        // birth off its channels): a new silence, commanded again; else the one command stands.
        let unheard_again = !book.get(node.node_id.as_str()).is_some_and(|(d, unheard)| d.node.incarnation == *incarnation && unheard < book.staleness_floor());
        if !heard_ready_since && !unheard_again {
            return "commanded-already".to_string();
        }
    }
    // Reserved now, before the command goes out: concurrent looks at one birth command it once.
    commanded().lock().unwrap().insert(key.clone(), Instant::now());
    let (build_id, attempt) = match builds.current().await {
        Ok(Some(b)) => (b.build_id.to_string(), b.attempt),
        _ => (String::new(), 0),
    };
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-mesh-join-commanded",
        node = %node.name,
        node_id = %node.node_id,
        incarnation_id = %incarnation.as_str(),
        build_id = %build_id,
        attempt,
        outcome = tracing::field::Empty,
        "otel.kind" = "internal"
    );
    let outcome = async {
        let Some(authority) = authority.get() else {
            return "no-authority".to_string();
        };
        // A command of this authority to the birth is open (its node.start after a stop, its stop):
        // the birth's lifecycle is that command's until its completion (node-start.md, node-stop.md).
        if authority.commands.open_for(&node.node_id, incarnation) {
            commanded().lock().unwrap().remove(&key);
            return "command-open: a start or stop of this authority is under way".to_string();
        }
        // The join the command makes the birth report is verified against the deployment this admin
        // holds of it (node-mesh-join.md step 4): the live birth is registered as a start after a
        // stop registers a parked one, from its runtime handle and its admitted digest.
        match runner.stopped_birth_or_live("node.mesh.join", &node.name).await {
            Ok((_, _, deployed)) => {
                let _ = runner.joins.expect(deployed);
            }
            Err(why) => return format!("join-not-registered: {why}"),
        }
        authority.begin_cycle(node, incarnation).await;
        let req = StatusRequest::JoinMesh {
            node_id: node.node_id.clone(),
            incarnation: incarnation.clone(),
            build_id,
            attempt,
            operation: format!("node.mesh.join:{}", node.name),
        };
        let (out, _) = client.call::<Status>(&NodeTarget::ExactNode(node.node_id.clone()), &req, &CallOptions::default()).await;
        match out.reply() {
            Some(r) => r.value().name().to_string(),
            None => out.name().to_string(),
        }
    };
    let outcome = tracing::Instrument::instrument(outcome, span.clone()).await;
    if outcome.starts_with("no-authority") || outcome.starts_with("join-not-registered") {
        commanded().lock().unwrap().remove(&key);
    }
    span.record("outcome", outcome.as_str());
    span.in_scope(|| tracing::info!(node = %node.name, outcome = %outcome, "a birth heard again is commanded back through node.mesh.join"));
    outcome
}
