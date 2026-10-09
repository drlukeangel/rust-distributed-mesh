//! A node birth obeying the two commands of its owning mesh-admin, `drain-node` and `stop-node`
//! (node-drain.md, node-stop.md), and calling the matching completion back (`node-drained`,
//! `node-left`). Every node kind runs this one logic over its own surface: an rpc node, a role
//! node and a node-admin differ only in how they refuse new work and how they publish.
//!
//! - `DrainNode`: answer `Applied` (or `AlreadyApplied` for the same operation), refuse new work,
//!   and when the eligible in-flight work reaches zero publish `node-drained` on the node's own
//!   mesh channel and call `NodeDrained` at the commanding admin. The process keeps running.
//! - `StopNode`: answer `Applied` (or `AlreadyApplied`), enter `Leaving` with no drain, publish
//!   `node-left` on the node's own mesh channel, call `NodeLeft` at the commanding admin while the
//!   endpoint is still open, then ask the process to stop (`stop_commanded`).
//!
//! Nothing here waits on an answer for correctness: a lost completion is the commander's
//! indeterminate, never this node's retry.

use crate::model::{IncarnationId, NodeId, PathName};
use rafka_mesh_entity::{LifecycleOp, MemberStatus, MeshDigest};
use rafka_mesh_transport::membership::{Frame, Membership};
use rafka_node_rpc::{NodeRpcClient, NodeRpcServer, NodeTarget};
use rafka_node_rpc_contract::status::{NodeState, Status, StatusReply, StatusRequest};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Whether this process was commanded to stop, and the wake of whoever waits to shut it down.
pub struct StopCommand {
    tx: tokio::sync::watch::Sender<bool>,
}

/// The process's stop command: set by `stop-node`, waited on beside the stop signal.
pub fn stop_command() -> &'static StopCommand {
    static STOP: std::sync::OnceLock<StopCommand> = std::sync::OnceLock::new();
    STOP.get_or_init(|| StopCommand { tx: tokio::sync::watch::Sender::new(false) })
}

impl StopCommand {
    /// Ask the process to stop (a `stop-node` was admitted and its `node-left` sent).
    pub fn request(&self) {
        self.tx.send_replace(true);
    }
    /// Whether a `stop-node` was admitted: the shutdown that follows has no drain leg.
    pub fn commanded(&self) -> bool {
        *self.tx.borrow()
    }
    /// Resolves once a `stop-node` was admitted.
    pub async fn wait(&self) {
        let _ = self.tx.subscribe().wait_for(|v| *v).await;
    }
}

/// Declares a state this birth entered to its authority.
pub type Declare = Arc<dyn Fn(NodeState) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// The surface a node kind gives the commands.
pub struct NodeSelf {
    /// This birth's logical node.
    pub node_id: NodeId,
    /// This birth's incarnation.
    pub incarnation: IncarnationId,
    /// This birth's `path.name`.
    pub name: PathName,
    /// This birth's Node RPC server: it refuses new calls while draining and counts the work in flight.
    pub server: NodeRpcServer,
    /// The process's one Node RPC client.
    pub client: Arc<NodeRpcClient>,
    /// This birth's membership: the mesh channel its completion frames go out on.
    pub membership: Membership,
    /// Set this birth's status and answer the digest to publish.
    pub set_status: Arc<dyn Fn(MemberStatus) -> MeshDigest + Send + Sync>,
    /// This birth's status now.
    pub status: Arc<dyn Fn() -> MemberStatus + Send + Sync>,
    /// Declare the state this birth entered to its authority (a node-admin of its mesh); `None` for
    /// a surface whose declarations are made elsewhere (a node-admin).
    pub declare: Option<Declare>,
    seen: Mutex<HashSet<String>>,
}

impl NodeSelf {
    /// A node surface over its parts.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        incarnation: IncarnationId,
        name: PathName,
        server: NodeRpcServer,
        client: Arc<NodeRpcClient>,
        membership: Membership,
        set_status: Arc<dyn Fn(MemberStatus) -> MeshDigest + Send + Sync>,
        status: Arc<dyn Fn() -> MemberStatus + Send + Sync>,
        declare: Option<Declare>,
    ) -> Self {
        Self { node_id, incarnation, name, server, client, membership, set_status, status, declare, seen: Mutex::new(HashSet::new()) }
    }

    /// Serve `DrainNode` or `StopNode` from `commander` (a node-admin the caller resolved).
    /// `None` for any other request.
    pub async fn serve(self: &Arc<Self>, commander: &NodeId, req: &StatusRequest) -> Option<StatusReply> {
        let (node_id, incarnation, build_id, attempt, operation, stop) = match req {
            StatusRequest::DrainNode { node_id, incarnation, build_id, attempt, operation } => (node_id, incarnation, build_id, attempt, operation, false),
            StatusRequest::StopNode { node_id, incarnation, build_id, attempt, operation } => (node_id, incarnation, build_id, attempt, operation, true),
            _ => return None,
        };
        if *node_id != self.node_id {
            return Some(StatusReply::RejectedNotAuthority { why: rafka_node_rpc_contract::status::NotAuthority::ReceiverNotPrimary { needed: "the subject node itself".into() } });
        }
        if *incarnation != self.incarnation {
            return Some(StatusReply::RejectedStaleIncarnation { held: self.incarnation.clone() });
        }
        let key = format!("{build_id}/{attempt}/{operation}");
        let current = (self.status)();
        if !self.seen.lock().unwrap().insert(key) {
            return Some(StatusReply::AlreadyApplied);
        }
        // Forward only: a node already leaving is not drained; a stop is legal from any state.
        if !stop && current == MemberStatus::Leaving {
            self.seen.lock().unwrap().remove(&format!("{build_id}/{attempt}/{operation}"));
            return Some(StatusReply::RejectedInvalidNodeTransition { current: NodeState::Leaving });
        }
        let op = LifecycleOp {
            build_id: build_id.clone(),
            attempt: *attempt,
            operation: operation.clone(),
            node_id: self.node_id.clone(),
            incarnation: self.incarnation.clone(),
            name: self.name.clone(),
            event_at_rafka_ms: self.membership.clock().now_rafka_ms(),
        };
        let (me, commander) = (self.clone(), commander.clone());
        // The command's serve span carries the caller's propagated trace; the work this command
        // starts continues it, so the completion call and its spans join the outer trace.
        let parent = tracing::Span::current();
        if stop {
            tokio::spawn(async move { me.stop(commander, op, parent).await });
        } else {
            tokio::spawn(async move { me.drain(commander, op, parent).await });
        }
        Some(StatusReply::Applied)
    }

    async fn drain(self: Arc<Self>, commander: NodeId, op: LifecycleOp, parent: tracing::Span) {
        use tracing::Instrument;
        let span = tracing::info_span!(
            parent: &parent,
            "rdm.node_admin.status.update.via-drain-node",
            node = %self.name, operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
            commander = %commander, in_flight_at_zero = tracing::field::Empty, "otel.kind" = "internal"
        );
        async {
            self.server.drain();
            let draining = (self.set_status)(MemberStatus::Draining);
            let _ = self.membership.publish(&draining).await;
            // This birth's own report of the state it entered goes to its authority beside the
            // completion: neither waits for the other.
            let declared = async {
                if let Some(declare) = &self.declare {
                    declare(NodeState::Draining).await;
                }
            };
            let completion = async {
                // The work still in flight besides the calls answering the command itself.
                let stats = self.server.stats();
                loop {
                    let in_flight = rafka_node_rpc::ServerStats::get(&stats.in_flight);
                    if in_flight == 0 {
                        break;
                    }
                    let mut d = draining.clone();
                    d.in_flight = Some(in_flight);
                    let _ = self.membership.publish(&d).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                let mut d = draining.clone();
                d.in_flight = Some(0);
                let _ = self.membership.publish(&d).await;
                tracing::Span::current().record("in_flight_at_zero", true);
                self.complete(&commander, &op, true).await;
            };
            tokio::join!(declared, completion);
        }
        .instrument(span)
        .await
    }

    async fn stop(self: Arc<Self>, commander: NodeId, op: LifecycleOp, parent: tracing::Span) {
        use tracing::Instrument;
        let span = tracing::info_span!(
            parent: &parent,
            "rdm.node_admin.status.update.via-stop-node",
            node = %self.name, operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
            commander = %commander, state = "Leaving", "otel.kind" = "internal"
        );
        async {
            let leaving = (self.set_status)(MemberStatus::Leaving);
            let _ = self.membership.publish(&leaving).await;
            let declared = async {
                if let Some(declare) = &self.declare {
                    declare(NodeState::Leaving).await;
                }
            };
            tokio::join!(declared, self.complete(&commander, &op, false));
            stop_command().request();
        }
        .instrument(span)
        .await
    }

    /// The completion: the node's own gossip hook on its mesh channel, then the call at the
    /// commanding admin, while this endpoint is open.
    async fn complete(&self, commander: &NodeId, op: &LifecycleOp, drained: bool) {
        let (frame, req, kind) = if drained {
            (
                Frame::NodeDrained { op: op.clone(), forwarded_by: None },
                StatusRequest::NodeDrained { node_id: op.node_id.clone(), incarnation: op.incarnation.clone(), build_id: op.build_id.clone(), attempt: op.attempt, operation: op.operation.clone() },
                "node-drained",
            )
        } else {
            (
                Frame::NodeLeft { op: op.clone(), forwarded_by: None },
                StatusRequest::NodeLeft { node_id: op.node_id.clone(), incarnation: op.incarnation.clone(), build_id: op.build_id.clone(), attempt: op.attempt, operation: op.operation.clone() },
                "node-left",
            )
        };
        let _ = self.membership.publish_lifecycle(&frame).await;
        let (out, _) = self.client.call::<Status>(&NodeTarget::ExactNode(commander.clone()), &req, &rafka_node_rpc::CallOptions::default()).await;
        let answer = match &out {
            rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) => r.value().name().to_string(),
            other => other.name().to_string(),
        };
        tracing::info_span!(
            "rdm.node_admin.status.update.via-completion-call",
            node = %self.name, completion = kind, operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
            commander = %commander, answer = %answer, "otel.kind" = "internal"
        )
        .in_scope(|| tracing::info!("the completion call was made to the commanding admin"));
    }
}
