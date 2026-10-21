//! Fabric round types, gossip hook frames, and [`RoundActions`] (`rdm-node`).

#[cfg(feature = "file-backend")]
use std::sync::Arc;

use async_trait::async_trait;
use rdm_transport::membership::{Backbone, Frame, Membership};
use rdm_core::RoundHook;
use rdm_core::status::StatusRequest;
use tracing::Instrument;

use crate::model::{EndpointId, FabricId, IncarnationId, Node, NodeId, PathName};

/// Which of the two rounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RoundKind {
    /// commit-state / state-committed.
    StateCommit,
    /// open-traffic / traffic-opened.
    OpenTraffic,
}

impl RoundKind {
    /// The down op's name.
    pub fn command(self) -> &'static str {
        match self {
            Self::StateCommit => "commit-state",
            Self::OpenTraffic => "open-traffic",
        }
    }

    /// The up op's name.
    pub fn completion(self) -> &'static str {
        match self {
            Self::StateCommit => "state-committed",
            Self::OpenTraffic => "traffic-opened",
        }
    }

    /// The round's operation: `commit-state:<fabric_id>` or `open-traffic:<fabric_id>`.
    pub fn operation(self, fabric_id: &FabricId) -> String {
        format!("{}:{}", self.command(), fabric_id)
    }

    /// The down op naming `node_id`/`incarnation` as its subject.
    pub fn down(self, key: &RoundKey, node_id: NodeId, incarnation: IncarnationId) -> StatusRequest {
        let (fabric_id, build_id, attempt, operation) = (key.fabric_id.clone(), key.build_id.clone(), key.attempt, self.operation(&key.fabric_id));
        match self {
            Self::StateCommit => StatusRequest::CommitState { fabric_id, node_id, incarnation, build_id, attempt, operation },
            Self::OpenTraffic => StatusRequest::OpenTraffic { fabric_id, node_id, incarnation, build_id, attempt, operation },
        }
    }

    /// The up op reporting `node_id`/`incarnation`.
    pub fn up(self, key: &RoundKey, node_id: NodeId, incarnation: IncarnationId) -> StatusRequest {
        let (fabric_id, build_id, attempt, operation) = (key.fabric_id.clone(), key.build_id.clone(), key.attempt, self.operation(&key.fabric_id));
        match self {
            Self::StateCommit => StatusRequest::StateCommitted { fabric_id, node_id, incarnation, build_id, attempt, operation },
            Self::OpenTraffic => StatusRequest::TrafficOpened { fabric_id, node_id, incarnation, build_id, attempt, operation },
        }
    }
}

/// The gossip hook frame of a round op (gossip.md, "Fabric round gossip hooks"): `StateCommitting`
/// or `TrafficOpening` for a command (`up == false`), `StateCommitted` or `TrafficOpened` for a
/// check-in. The subject is `node_id`/`incarnation`; `publisher` is the author at this hop.
pub fn hook_frame(key: &RoundKey, up: bool, node_id: NodeId, incarnation: IncarnationId, publisher: &str, event_at_rafka_ms: u64) -> Frame {
    let hook = RoundHook {
        fabric_id: key.fabric_id.clone(),
        node_id,
        incarnation,
        build_id: key.build_id.clone(),
        attempt: key.attempt,
        operation: key.kind.operation(&key.fabric_id),
        publisher: publisher.to_string(),
        event_at_rafka_ms,
    };
    let forwarded_by = None;
    match (key.kind, up) {
        (RoundKind::StateCommit, false) => Frame::StateCommitting { hook, forwarded_by },
        (RoundKind::StateCommit, true) => Frame::StateCommitted { hook, forwarded_by },
        (RoundKind::OpenTraffic, false) => Frame::TrafficOpening { hook, forwarded_by },
        (RoundKind::OpenTraffic, true) => Frame::TrafficOpened { hook, forwarded_by },
    }
}

/// Publish the round hook of `key`'s op (gossip.md, "Fabric round gossip hooks"), in the path of the
/// call it runs with: a command hook (`up == false`) names the commanded birth, a completion hook
/// `publisher`'s own birth. The frame goes on `publisher`'s mesh channel and, when it is on the
/// backbone, the backbone. `Err` names why the frame was not handed to every channel.
pub async fn publish_hook(publisher: &PathName, key: &RoundKey, up: bool, node_id: &NodeId, incarnation: &IncarnationId, membership: &Membership, backbone: Option<&Backbone>) -> Result<(), String> {
    let frame = hook_frame(key, up, node_id.clone(), incarnation.clone(), &publisher.to_string(), membership.clock().now_rafka_ms());
    let hook = if up { key.kind.completion() } else { key.kind.command() };
    let channels = if backbone.is_some() { "mesh,backbone" } else { "mesh" };
    let span = tracing::info_span!("rdm.node_admin.fabric.update.via-round-hook", node = %publisher, round = key.kind.command(), hook, subject_id = %node_id, incarnation_id = %incarnation.as_str(), build_id = %key.build_id, attempt = key.attempt, channels, bound = true, "otel.kind" = "internal");
    async {
        let published = match backbone {
            Some(backbone) => backbone.publish_command_hook(&frame).await,
            None => membership.publish_lifecycle(&frame).await,
        };
        match published {
            Ok(()) => {
                tracing::info!("the round hook was published");
                Ok(())
            }
            Err(e) => {
                tracing::info!(error = %e, "the round hook was not handed to every channel");
                Err(e.to_string())
            }
        }
    }
    .instrument(span)
    .await
}

/// Where an owed message (a declaration, a round command) went: the exact birth, and the address
/// facts the view held for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Destination {
    /// The exact birth's node id.
    pub node_id: NodeId,
    /// Its incarnation, as the view held it.
    pub incarnation: Option<IncarnationId>,
    /// The endpoint it is resolved by, as the view held it.
    pub endpoint: Option<EndpointId>,
    /// Its transport address, as the view held it.
    pub addr: Option<std::net::SocketAddr>,
}

impl Destination {
    /// The destination `n` is now.
    pub fn of(n: &Node) -> Self {
        Self { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone(), endpoint: n.endpoint_id.clone(), addr: n.transport_addr }
    }
}

/// The last send of one owed message that ended without the birth admitting it. The message rides
/// one Owed call (node-rpc.md §43): the delivery layer resends it until the birth answers, so a lost
/// route is the layer's to wait out and nothing here tracks one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sent {
    /// Where it went.
    pub to: Destination,
    /// A typed reply came back (the birth refused the op by name): terminal until an eligible event.
    /// Otherwise the call ended because the birth's exit or removal is proven.
    pub typed: bool,
}

/// Is an owed message due again (R-S2)? Sent when it becomes owed, and again only when the birth
/// addressed this node or the destination changed to a deliverable birth. A failed delivery is never
/// a reason: the Owed call is the layer's to deliver (node-rpc.md §43). A target that is not
/// deliverable is sent nothing.
pub fn eligible(sent: Option<&Sent>, now: &Destination, deliverable: bool, addressed: bool) -> bool {
    if addressed {
        return true;
    }
    if !deliverable {
        return false;
    }
    match sent {
        None => true,
        Some(s) => &s.to != now,
    }
}

/// The fields every round op carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundOp<'a> {
    /// Which round.
    pub kind: RoundKind,
    /// Down (command) or up (completion).
    pub is_down: bool,
    /// The fabric.
    pub fabric_id: &'a FabricId,
    /// The subject.
    pub node_id: &'a NodeId,
    /// The subject's birth.
    pub incarnation: &'a IncarnationId,
    /// The Build.
    pub build_id: &'a str,
    /// The attempt.
    pub attempt: u32,
    /// The operation.
    pub operation: &'a str,
}

impl<'a> RoundOp<'a> {
    /// The round op `req` is, when it is one.
    pub fn of(req: &'a StatusRequest) -> Option<Self> {
        let (kind, is_down, fabric_id, node_id, incarnation, build_id, attempt, operation) = match req {
            StatusRequest::CommitState { fabric_id, node_id, incarnation, build_id, attempt, operation } => (RoundKind::StateCommit, true, fabric_id, node_id, incarnation, build_id, *attempt, operation),
            StatusRequest::StateCommitted { fabric_id, node_id, incarnation, build_id, attempt, operation } => (RoundKind::StateCommit, false, fabric_id, node_id, incarnation, build_id, *attempt, operation),
            StatusRequest::OpenTraffic { fabric_id, node_id, incarnation, build_id, attempt, operation } => (RoundKind::OpenTraffic, true, fabric_id, node_id, incarnation, build_id, *attempt, operation),
            StatusRequest::TrafficOpened { fabric_id, node_id, incarnation, build_id, attempt, operation } => (RoundKind::OpenTraffic, false, fabric_id, node_id, incarnation, build_id, *attempt, operation),
            _ => return None,
        };
        Some(Self { kind, is_down, fabric_id, node_id, incarnation, build_id, attempt, operation })
    }

    /// The round this op belongs to.
    pub fn key(&self) -> RoundKey {
        RoundKey { kind: self.kind, fabric_id: self.fabric_id.clone(), build_id: self.build_id.to_string(), attempt: self.attempt }
    }
}

/// A round: the Build and attempt it belongs to, in one fabric.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoundKey {
    /// Which round.
    pub kind: RoundKind,
    /// The fabric.
    pub fabric_id: FabricId,
    /// The accepted Build.
    pub build_id: String,
    /// Its attempt.
    pub attempt: u32,
}


/// What a node does before it checks in to a round: the required writes of the phase.
///
/// Commit-state: the node writes what its scratchpad holds into the system org (a node its
/// connection rows, a mesh primary its members' Node rows and its Mesh row, the fabric-primary the
/// Fabric row). The check-in is owed only after the writes are stored; a refused write is an `Err`
/// and the node owes no check-in.
#[async_trait]
pub trait RoundActions: Send + Sync {
    /// The commit-state action of `node`.
    async fn commit_state(&self, node: &PathName) -> Result<(), String>;
    /// The open-traffic action of `node`.
    async fn open_traffic(&self, node: &PathName) -> Result<(), String>;
}

/// The actions of an embedding that binds none: nothing is published, and the span says so.
pub struct NoScratchpad;

#[async_trait]
impl RoundActions for NoScratchpad {
    async fn commit_state(&self, node: &PathName) -> Result<(), String> {
        tracing::info_span!("rdm.node_admin.fabric.update.via-round-action", node = %node, round = "commit-state", bound = false, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("no scratchpad publication is bound: this node holds nothing to publish"));
        Ok(())
    }
    async fn open_traffic(&self, node: &PathName) -> Result<(), String> {
        tracing::info_span!("rdm.node_admin.fabric.update.via-round-action", node = %node, round = "open-traffic", bound = false, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("no traffic-opening action is bound to this node"));
        Ok(())
    }
}


#[cfg(feature = "file-backend")]
/// RDM standalone's commit-state action (fabric-state-commit.md step 2: "Each node writes what it
/// holds in its scratchpad into the store the state-sync work established"; node-admin-lifecycle.md
/// §7): the node publishes its scratchpad to the fabric's store root, then runs the embedding's own
/// action, if it binds one. Each node holds only the rows it writes, so a node publishes its
/// connections rows, a mesh primary also its Mesh row and its members' Node rows, and the
/// fabric-primary also the Fabric row. A node that opened a root the Fabric row was already
/// published to holds nothing, and every later round's publication finds nothing held. A
/// publication that fails is an `Err`: the node owes no check-in.
pub struct StorePublication {
    /// What this node holds before the fabric's first state-commit; `None` when it opened the store
    /// root after it.
    scratchpad: Option<Arc<crate::record_store::Scratchpad>>,
    /// The embedding's own action, run after the publication.
    then: Option<Arc<dyn RoundActions>>,
}

#[cfg(feature = "file-backend")]
impl StorePublication {
    /// The action publishing `scratchpad`, then running `then`.
    pub fn new(scratchpad: Option<Arc<crate::record_store::Scratchpad>>, then: Option<Arc<dyn RoundActions>>) -> Self {
        Self { scratchpad, then }
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl RoundActions for StorePublication {
    async fn commit_state(&self, node: &PathName) -> Result<(), String> {
        let span = tracing::info_span!(
            "rdm.node_admin.fabric.update.via-round-action",
            node = %node,
            round = "commit-state",
            bound = true,
            rows = tracing::field::Empty,
            dropped = tracing::field::Empty,
            held = tracing::field::Empty,
            stands = tracing::field::Empty,
            refused = tracing::field::Empty,
            stores = tracing::field::Empty,
            error = tracing::field::Empty,
            "otel.kind" = "internal"
        );
        let published = match &self.scratchpad {
            Some(s) => s.publish().instrument(span.clone()).await,
            None => Ok(crate::record_store::Publication::default()),
        };
        match published {
            Ok(p) => {
                span.record("rows", p.written() as u64);
                span.record("dropped", p.dropped() as u64);
                span.record("held", p.held() as u64);
                span.record("stands", p.stands() as u64);
                span.record("refused", p.refused() as u64);
                span.record("stores", p.summary().as_str());
                span.in_scope(|| tracing::info!("this node wrote what its scratchpad held to the store root, and writes there from now on"));
            }
            Err(e) => {
                span.record("error", tracing::field::display(&e));
                span.in_scope(|| tracing::info!("this node's scratchpad could not be published to the store root"));
                return Err(format!("{node}: its scratchpad could not be published to the store root: {e}"));
            }
        }
        match &self.then {
            Some(then) => then.commit_state(node).await,
            None => Ok(()),
        }
    }
    async fn open_traffic(&self, node: &PathName) -> Result<(), String> {
        match &self.then {
            Some(then) => then.open_traffic(node).await,
            None => NoScratchpad.open_traffic(node).await,
        }
    }
}

/// Run the action of `kind` for `node` under its span.
pub async fn run_action(actions: &dyn RoundActions, node: &PathName, kind: RoundKind) -> Result<(), String> {
    match kind {
        RoundKind::StateCommit => actions.commit_state(node).await,
        RoundKind::OpenTraffic => actions.open_traffic(node).await,
    }
}

