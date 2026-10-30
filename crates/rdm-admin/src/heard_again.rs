//! A birth heard again (Q87, Luke 2026-10-20): every node coming to ready from any other
//! state connects to its node-admin for its cert, its time and its cache, and the `pre_node_ready`
//! hook is where it does that; node-admins included, the Day-0 fabric-primary excepted. When the
//! tickle reaches a birth its authority holds off its mesh (node-recovery.md step 2, node-tickle.md
//! "What follows"), the authority begins the birth's next cycle and commands `node.mesh.join` to it,
//! back over its own call; the birth runs node.start steps 2 to 5 again on it and is published
//! ready only once its hook has passed (node-mesh-join.md step 10). One function, run by
//! the mesh-primary's offline ladder for its members and by the fabric-primary's primary watch for a
//! held primary; nothing re-publishes a presence by hand. `AdminRunner` registers the birth's
//! join (as it does a stopped birth's) so the join door admits the report.

use crate::model::{IncarnationId, Node};
use crate::status_rpc::StatusDoorSlot;
use rdm_core::status::{Status, StatusRequest};
use rdm_transport::topology::DigestView;
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

/// The births this process commanded back and has not heard ready since: one command
/// per silence. An admin looks at a silent birth every pass; the start it commanded takes the
/// birth off its mesh for the length of that start, and a second command inside it would start
/// it over.
fn commanded() -> &'static Mutex<HashMap<(String, String), Instant>> {
    static COMMANDED: OnceLock<Mutex<HashMap<(String, String), Instant>>> = OnceLock::new();
    COMMANDED.get_or_init(Default::default)
}

/// What an admin needs to command a birth back: this admin's client, Status door, Build state,
/// runner (once built) and digest view: one function, run by the offline ladder and the primary watch.
pub struct HeardAgain {
    /// This admin's Node RPC client: the command goes out on it.
    pub client: Arc<NodeRpcClient>,
    /// This admin's Status door: the birth's next cycle begins there.
    pub status_door: StatusDoorSlot,
    /// This admin's Build state: the current Build the command names.
    pub builds: Arc<dyn crate::build_state::BuildStateAdapter>,
    /// This admin's runner, once built: it registers the birth's join before the command.
    pub runner: Arc<OnceLock<Arc<crate::admin::runner::AdminRunner>>>,
    /// This admin's digest view: whether the birth has been heard ready since.
    pub digest_view: DigestView,
}

/// The handle, once this admin's runner is built.
pub type HeardAgainSlot = Arc<OnceLock<Arc<HeardAgain>>>;

impl HeardAgain {
    /// Command `node`'s exact birth back through `node.mesh.join` (`command_mesh_join`).
    pub async fn command(&self, node: &Node, incarnation: &IncarnationId) -> String {
        let outcome = match self.runner.get() {
            None => "no-runner".to_string(),
            Some(runner) => command_mesh_join(&self.client, &self.status_door, &self.builds, runner, &self.digest_view, node, incarnation).await,
        };
        // A command not sent is spanned too, with why: the flame of a birth left off its mesh reads it.
        if !outcome.starts_with("mesh-joined") && !outcome.starts_with("commanded") {
            tracing::info_span!("rdm.node_admin.node.update.via-mesh-join-commanded", node = %node.name, node_id = %node.node_id, incarnation_id = %incarnation.as_str(), sent = false, outcome = %outcome, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("a heard-again birth was looked at: node.mesh.join not sent"));
        }
        outcome
    }
}

/// Begin `node`'s next cycle (its declared state is measured from Pending again) and command
/// `node.mesh.join` to its exact birth, naming the primary's current Build; the reply's name.
/// One span, `rdm.node_admin.node.update.via-mesh-join-commanded`, with the outcome.
async fn command_mesh_join(
    client: &Arc<NodeRpcClient>,
    status_door: &StatusDoorSlot,
    builds: &Arc<dyn crate::build_state::BuildStateAdapter>,
    runner: &crate::admin::runner::AdminRunner,
    digest_view: &DigestView,
    node: &Node,
    incarnation: &IncarnationId,
) -> String {
    // The Day-0 fabric-primary, and any fabric primary: no node above it to connect to (Luke,
    // Q87); it is never commanded back.
    if node.is_fabric_primary {
        return "fabric-primary: no authority above it".to_string();
    }
    // Only a birth the view holds off ready re-enters (Luke: "coming from a state that is
    // not ready"). What an admin just started has heard is not the birth's state (it holds every
    // member from the projection, unheard until their digests reach it); the topology is.
    if node.status == crate::model::NodeStatus::Ready {
        return "ready-in-view: on its mesh".to_string();
    }
    // A birth with a lifecycle command of its authority in flight (a stop, a start, a restart) is that
    // command's: the start it runs is the command's own (node-start.md), never this one.
    if let Some(op) = digest_view.in_flight().into_iter().find(|op| op.node_id == node.node_id && op.incarnation == *incarnation) {
        return format!("in-flight: {}", op.operation);
    }
    // A birth whose node.create this admin runs is that create's (node-create.md step 3: the create
    // composes the whole node.start; `node.created` follows it): never commanded back while it runs.
    // The tickle lists it from the runtime row its admission kept before its digest was installed,
    // and hears it again at the install: a look inside the create's first start, not a return.
    if runner.joins.creating(&node.node_id) {
        return format!("create-in-flight: node.create:{}", node.name);
    }
    // Once per silence: a birth commanded back is commanded again only after a staleness floor, and
    // only once this process has heard it ready since (the admin looks at it every pass
    // while its view still holds it silent).
    let key = (node.node_id.to_string(), incarnation.as_str().to_string());
    if let Some(at) = commanded().lock().unwrap().get(&key).copied() {
        if at.elapsed() < digest_view.staleness_floor() {
            return "commanded-already".to_string();
        }
        let heard_ready_since = digest_view
            .get(node.node_id.as_str())
            .is_some_and(|(d, unheard)| d.node.incarnation == *incarnation && d.status == rdm_core::MemberStatus::Ready && Instant::now().checked_sub(unheard).is_some_and(|heard_at| heard_at > at));
        // Heard ready since, or unheard again (a commanded start that stopped at a step left the
        // birth off its mesh): a new silence, commanded again; else the one command stands.
        let unheard_again = !digest_view.get(node.node_id.as_str()).is_some_and(|(d, unheard)| d.node.incarnation == *incarnation && unheard < digest_view.staleness_floor());
        if !heard_ready_since && !unheard_again {
            return "commanded-already".to_string();
        }
    }
    // Reserved now, before the command goes out: concurrent looks at one birth command it once.
    commanded().lock().unwrap().insert(key.clone(), Instant::now());
    // Each command back is its own join (node-mesh-join.md step 10, Q97, Q99: one Pending per accepted
    // join report, per join and not per birth), so its operation names this command by the instant it
    // was reserved, fixed here: every resend of this one request carries it unchanged. The birth's
    // ready declaration carries the operation, and an operation row survives every cycle
    // begin with no state in its key (`NodeOperationRow::key`): a second command back in one Build
    // attempt under the first one's operation would have its ready answered AlreadyApplied from the
    // first join's row, and the birth would stay Pending.
    let reserved_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
    let operation = format!("node.mesh.join:{}@{reserved_ms}", node.name);
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
        operation = %operation,
        outcome = tracing::field::Empty,
        "otel.kind" = "internal"
    );
    let outcome = async {
        let Some(status_door) = status_door.get() else {
            return "no-authority".to_string();
        };
        // A command of this authority to the birth is open (its node.start after a stop, its stop):
        // the birth's lifecycle is that command's until its completion (node-start.md, node-stop.md).
        if status_door.commands.open_for(&node.node_id, incarnation) {
            commanded().lock().unwrap().remove(&key);
            return "command-open: a start or stop of this authority is under way".to_string();
        }
        // Commands go down (node-mesh-join.md, Commander): read in the view as it stands at the send, not
        // the one the look began in, where this admin may since have stopped being the mesh's primary.
        if let Err(why) = commands_join(&*status_door.topology.read().await, &status_door.node_id, node) {
            return format!("not-commander: {why}");
        }
        // The join the command makes the birth report is verified against the deployment this admin
        // holds of it (node-mesh-join.md step 4): the live birth is registered as a start after a
        // stop registers a stopped one, from its runtime handle and its admitted digest.
        // Its report, when the door accepts it, is where the birth's next cycle begins and it is
        // published Pending, before the join is answered (node-mesh-join.md step 10), as every
        // accepted join report is: nothing is written before the command reached it (node-start.md
        // step 2), and the birth's hook and its ready follow it.
        if let Err(why) = runner.stopped_birth_or_live("node.mesh.join", &node.name).await.map(|(_, _, deployed)| runner.joins.expect(deployed)) {
            return format!("join-not-registered: {why}");
        }
        // The command back is this authority's command to the birth, open from the send to the answer
        // as every command of the authority is: the birth's lifecycle is the command's until then.
        let command = crate::node_commands::CommandKey { build_id: build_id.clone(), attempt, operation: operation.clone(), node_id: node.node_id.clone(), incarnation: incarnation.clone() };
        let _ = status_door.commands.open(command.clone());
        let req = StatusRequest::JoinMesh {
            node_id: node.node_id.clone(),
            incarnation: incarnation.clone(),
            build_id,
            attempt,
            operation,
        };
        let (out, _) = client.call::<Status>(&NodeTarget::ExactNode(node.node_id.clone()), &req, &CallOptions::default()).await;
        match &out {
            rdm_core::outcome::RpcOutcome::Indeterminate(_) => status_door.commands.close_unanswered(&command),
            rdm_core::outcome::RpcOutcome::NotSent(_) => status_door.commands.close(&command),
            _ => status_door.commands.close_answered(&command),
        }
        match out.reply() {
            Some(r) => r.value().name().to_string(),
            None => out.name().to_string(),
        }
    };
    let outcome = tracing::Instrument::instrument(outcome, span.clone()).await;
    // A command the birth refused by name (it is not ready for one yet) is looked at again on the next
    // pass, not held for a floor: the refusal is the birth's knowledge at that instant, not a new silence.
    if outcome.starts_with("no-authority") || outcome.starts_with("join-not-registered") || outcome.starts_with("rejected-not-primary") || outcome.starts_with("not-ready") || outcome.starts_with("not-commander") {
        commanded().lock().unwrap().remove(&key);
    }
    span.record("outcome", outcome.as_str());
    span.in_scope(|| tracing::info!(node = %node.name, outcome = %outcome, "a birth heard again is commanded back through node.mesh.join"));
    outcome
}

/// Whether the admin born as `me` commands `node.mesh.join` to `subject` in its view now
/// (node-mesh-join.md, Commander: "Standalone: the node's mesh-primary, after a tickle that reached
/// the exact birth; the fabric-primary for a mesh-primary. Commands go down"). `Err` names why not.
pub(crate) fn commands_join(view: &crate::topology::Topology, me: &crate::model::NodeId, subject: &Node) -> Result<(), String> {
    if view.is_mesh_primary(&subject.mesh, &subject.node_id) {
        return if view.is_fabric_primary(me) { Ok(()) } else { Err(format!("{} is the mesh-primary of {} and this admin is not the fabric-primary", subject.name, subject.mesh)) };
    }
    if view.is_mesh_primary(&subject.mesh, me) { Ok(()) } else { Err(format!("this admin is not the mesh-primary of {}, which commands {}", subject.mesh, subject.name)) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{NodeKind, NodeStatus};
    use crate::topology::Topology;

    fn at(name: &str, kind: NodeKind, primary: bool, fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.incarnation_id = Some(IncarnationId::mint());
        n.mesh = name.split('.').next().unwrap().into();
        n.kind = kind;
        n.status = NodeStatus::Ready;
        n.is_primary = primary;
        n.is_fabric_primary = fabric_primary;
        n
    }

    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:25
    /// "Caller: The primary above the node: its mesh-primary for a member, the fabric-primary for a
    ///  mesh-primary or another mesh's node-admin"
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:31
    /// "What follows: The caller's: on `unreached` the investigation's verdict is the inspect's answer,
    ///  read before the tickle; on `reached` for a node that is off its channel it commands
    ///  [node.mesh.join](node-mesh-join.md); mesh investigation decides at 60 s"
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:46
    /// "5. The caller acts on the result per its own page. Nothing on this page writes a row, changes a
    ///  primary or publishes a frame."
    /// CONTRACT (node-mesh-join.md, Commander: "the node's mesh-primary ...; the fabric-primary for a
    /// mesh-primary. Commands go down"; #3211 red 2, gate 19d4afbee release: mesh2.admin.1 stopped
    /// publishing mesh2 at 484.371, held mesh2.admin.2's role at 484.404, and commanded
    /// `node.mesh.join:mesh2.admin.2` at 484.416): an admin commands a node of a mesh only as that
    /// mesh's primary, and a mesh-primary only as the fabric-primary. Must NOT happen: a former
    /// mesh-primary commanding the mesh's primary, or the fabric-primary commanding a peer mesh's node.
    #[test]
    fn only_the_mesh_primary_commands_its_node_and_only_the_fabric_primary_commands_a_mesh_primary() {
        let fp = at("mesh1.admin.1", NodeKind::NodeAdmin, true, true);
        let rpc = at("mesh1.rpc.1", NodeKind::RpcNode, false, false);
        let former = at("mesh2.admin.1", NodeKind::NodeAdmin, false, false);
        let m2p = at("mesh2.admin.2", NodeKind::NodeAdmin, true, false);
        let broker = at("mesh2.broker.1", NodeKind::RpcNode, false, false);
        let view = Topology {
            fabric: crate::model::Fabric { id: rdm_core::FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![fp.clone(), rpc.clone(), former.clone(), m2p.clone(), broker.clone()],
        };
        assert!(commands_join(&view, &former.node_id, &m2p).is_err(), "a former mesh-primary does not command the mesh's primary");
        assert!(commands_join(&view, &former.node_id, &broker).is_err(), "an admin that is not its mesh's primary commands none of its nodes");
        assert!(commands_join(&view, &fp.node_id, &broker).is_err(), "the fabric-primary commands a peer mesh's node only through its mesh-primary");
        assert_eq!(commands_join(&view, &fp.node_id, &m2p), Ok(()), "the fabric-primary commands a mesh-primary");
        assert_eq!(commands_join(&view, &m2p.node_id, &broker), Ok(()), "the mesh-primary commands its own mesh's node");
        assert_eq!(commands_join(&view, &m2p.node_id, &former), Ok(()), "the mesh-primary commands its own mesh's other node-admin");
        assert_eq!(commands_join(&view, &fp.node_id, &rpc), Ok(()), "the fabric-primary, mesh1's primary, commands mesh1's node");
    }
}
