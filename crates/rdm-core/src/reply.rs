//! Shared outcome constructors. Handlers choose a named outcome; codecs own its wire variant.

/// Constructors for the StatusReply family.
pub mod status {
use crate::status::*;
use crate::outcome::MalformedKind;
use crate::views::{ConnectionsView, FabricView, MeshView, NodeStatus, NodeView};
use crate::{ConnectionId, FabricId, IncarnationId, MeshId, NodeId, RuntimeFact, CROCKFORD60_LEN};
use std::collections::{BTreeMap, BTreeSet};
use serde::{Deserialize, Serialize};
/// Construct the typed applied outcome.
pub fn applied() -> StatusReply { StatusReply::Applied }
/// Construct the typed already-applied outcome.
pub fn already_applied() -> StatusReply { StatusReply::AlreadyApplied }
/// Construct the typed rejected-stale-incarnation outcome.
pub fn rejected_stale_incarnation(held: IncarnationId) -> StatusReply { StatusReply::RejectedStaleIncarnation { held } }
/// Construct the typed rejected-stale-mesh outcome.
pub fn rejected_stale_mesh(held: MeshId) -> StatusReply { StatusReply::RejectedStaleMesh { held } }
/// Construct the typed rejected-stale-fabric outcome.
pub fn rejected_stale_fabric(held: FabricId) -> StatusReply { StatusReply::RejectedStaleFabric { held } }
/// Construct the typed rejected-not-primary outcome.
pub fn rejected_not_primary(why: NotPrimary) -> StatusReply { StatusReply::RejectedNotPrimary { why } }
/// Construct the typed rejected-invalid-node-transition outcome.
pub fn rejected_invalid_node_transition(current: NodeState) -> StatusReply { StatusReply::RejectedInvalidNodeTransition { current } }
/// Construct the typed rejected-cert outcome: a renewed member cert or an issuer update refused by
/// name, what the receiver holds kept (node-cert-renew.md, node-issuer-update.md).
pub fn rejected_cert(reason: String) -> StatusReply { StatusReply::RejectedCert { reason } }
/// Construct the typed root-still-chained outcome: a `node.roots.update` set that drops a root the
/// birth still chains under, refused by name and nothing installed (fabric-ca-renew.md).
pub fn root_still_chained(root: String) -> StatusReply { StatusReply::RejectedRootStillChained { root } }
/// Construct the typed `fabric.root.updated` outcome: the rotation accepted, its roots row at `phase`
/// (fabric-ca-renew.md).
pub fn root_updated(phase: crate::status::RootsPhase) -> StatusReply { StatusReply::RootUpdated { phase } }
/// Construct the typed rotation-in-progress outcome: `fabric.root.update` refused by name while a
/// rotation is in flight at `phase` (fabric-ca-renew.md).
pub fn rotation_in_progress(phase: crate::status::RootsPhase) -> StatusReply { StatusReply::RejectedRotationInProgress { phase } }
/// Construct the typed `fabric.storage.updated` outcome: the storage change `change_id` is started.
pub fn storage_updated(change_id: crate::ChangeId) -> StatusReply { StatusReply::StorageUpdated { change_id } }
/// Construct the typed build-in-progress outcome of `fabric.storage.update`: a Build or another storage
/// change is in progress, and nothing is started.
pub fn storage_build_in_progress(build_id: Option<String>, change_id: Option<crate::ChangeId>) -> StatusReply { StatusReply::RejectedBuildInProgress { build_id, change_id } }
/// Construct the typed refusal of `fabric.storage.update`'s provider by the rule `reason`.
pub fn topology_storage_provider_refused(reason: &str, detail: String) -> StatusReply { StatusReply::RejectedTopologyStorageProvider { reason: reason.into(), detail } }
/// Construct the typed root-already-trusted outcome: `fabric.root.update` refused by name for a root
/// the fabric trusts (fabric-ca-renew.md).
pub fn root_already_trusted(root: String) -> StatusReply { StatusReply::RejectedRootAlreadyTrusted { root } }
/// Construct the typed rejected-invalid-mesh-transition outcome.
pub fn rejected_invalid_mesh_transition(current: MeshState) -> StatusReply { StatusReply::RejectedInvalidMeshTransition { current } }
/// Construct the typed peer-unresolved outcome.
pub fn peer_unresolved(reason: String) -> StatusReply { StatusReply::PeerUnresolved { reason } }
/// Construct the typed not-ready outcome.
pub fn not_ready(reason: String) -> StatusReply { StatusReply::NotReady { reason } }
/// Construct the typed busy outcome.
pub fn busy(reason: String) -> StatusReply { StatusReply::Busy { reason } }
/// Construct the typed connections-deleting outcome.
pub fn connections_deleting(reason: String) -> StatusReply { StatusReply::ConnectionsDeleting { reason } }
/// Construct the typed malformed outcome.
pub fn malformed(kind: MalformedKind) -> StatusReply { StatusReply::Malformed { kind } }
/// Construct the typed rejected-unmatched-completion outcome.
pub fn rejected_unmatched_completion(field: String, expected: String, reported: String) -> StatusReply { StatusReply::RejectedUnmatchedCompletion { field, expected, reported } }
/// Construct the typed start-failed outcome.
pub fn start_failed(step: String, reason: String) -> StatusReply { StatusReply::StartFailed { step, reason } }
/// Construct the typed rejected-config outcome.
/// Construct the typed config-row-refused outcome.
pub fn config_row_refused(node: String, reason: String) -> StatusReply { StatusReply::ConfigRowRefused { node, reason } }
/// Construct the typed config-not-applied outcome.
pub fn config_not_applied(node: String, answered: String, reason: String) -> StatusReply { StatusReply::ConfigNotApplied { node, answered, reason } }
/// Construct the typed rejected-admitting-connections outcome.
pub fn rejected_admitting_connections(status: NodeStatus, held_connections: Vec<ConnectionId>) -> StatusReply { StatusReply::RejectedAdmittingConnections { status, held_connections } }
/// Construct the typed name-held outcome.
pub fn name_held(location: crate::PathName, by: NodeId) -> StatusReply { StatusReply::NameHeld { path: location.path, name: location.name, by } }
/// Construct the typed node-record-refused outcome.
pub fn node_record_refused(node: String, reason: String) -> StatusReply { StatusReply::NodeRecordRefused { node, reason } }
/// Construct the typed name-not-applied outcome.
pub fn name_not_applied(node: String, answered: String, reason: String) -> StatusReply { StatusReply::NameNotApplied { node, answered, reason } }
/// Construct the typed rejected-connection-scope outcome.
/// Construct the typed unknown-mesh outcome.
pub fn unknown_mesh(mesh: String) -> StatusReply { StatusReply::UnknownMesh { mesh } }
/// Construct the typed rejected-conflicting-workflow outcome.
pub fn rejected_conflicting_workflow(held: MeshDeleteIdentity, offered: MeshDeleteIdentity) -> StatusReply { StatusReply::RejectedConflictingWorkflow { held, offered } }
/// Construct the typed rejected-incomplete-manifest outcome.
pub fn rejected_incomplete_manifest(missing: u32, without_exit: Vec<String>, reason: String) -> StatusReply { StatusReply::RejectedIncompleteManifest { missing, without_exit, reason } }
/// Construct the typed rejected-source-failed outcome.
pub fn rejected_source_failed(source: String, op: String, cause: String) -> StatusReply { StatusReply::RejectedSourceFailed { source, op, cause } }
/// Construct the typed rejected-tags outcome.
pub fn rejected_tags(reason: String) -> StatusReply { StatusReply::RejectedTags { reason } }
/// Construct the typed rejected-name outcome.
pub fn rejected_name(reason: String) -> StatusReply { StatusReply::RejectedName { reason } }
/// Construct the typed busy-holding-connections outcome.
pub fn busy_holding_connections(connections: Vec<ConnectionId>) -> StatusReply { StatusReply::BusyHoldingConnections { connections } }
/// Construct the typed request-not-handled outcome.
pub fn request_not_handled(request: String) -> StatusReply { StatusReply::RequestNotHandled { request } }
/// Construct the bounded config-row-refused outcome.
pub fn bounded_config_row_refused(node: &str, reason: &str) -> StatusReply { StatusReply::config_row_refused(node, reason) }
/// Construct the bounded config-not-applied outcome.
pub fn bounded_config_not_applied(node: &str, answered: &str, reason: &str) -> StatusReply { StatusReply::config_not_applied(node, answered, reason) }
/// Construct the bounded pending outcome: a probe's answer while the node's hook answered `Pending`.
pub fn bounded_pending(node_id: NodeId, incarnation: IncarnationId, reason: &str) -> StatusReply { StatusReply::pending(node_id, incarnation, reason) }
/// Construct the bounded start-failed outcome: a probe's answer once the node's start ended Failed.
pub fn bounded_start_failed(step: &str, reason: &str) -> StatusReply { StatusReply::start_failed(step, reason) }
/// Construct the bounded rejected-tags outcome.
pub fn bounded_rejected_tags(reason: &str) -> StatusReply { StatusReply::rejected_tags(reason) }
/// Construct the bounded rejected-filter outcome (Q167 R-RD2).
pub fn bounded_rejected_filter(reason: &str) -> StatusReply { StatusReply::rejected_filter(reason) }
/// Construct the bounded node-record-refused outcome.
pub fn bounded_node_record_refused(node: &str, reason: &str) -> StatusReply { StatusReply::node_record_refused(node, reason) }
/// Construct the bounded rejected-name outcome.
pub fn bounded_rejected_name(reason: &str) -> StatusReply { StatusReply::rejected_name(reason) }
/// Construct the bounded rejected-admitting-connections outcome.
pub fn bounded_rejected_admitting_connections(status: NodeStatus, held: &[ConnectionId]) -> StatusReply { StatusReply::rejected_admitting_connections(status, held) }
/// Construct the bounded busy-holding-connections outcome.
pub fn bounded_busy_holding_connections(held: &[ConnectionId]) -> StatusReply { StatusReply::busy_holding_connections(held) }
/// Construct the bounded name-not-applied outcome.
pub fn bounded_name_not_applied(node: &str, answered: &str, reason: &str) -> StatusReply { StatusReply::name_not_applied(node, answered, reason) }
/// Construct the bounded source-failed outcome.
pub fn bounded_source_failed(source: &str, op: &str, cause: &str) -> StatusReply { StatusReply::source_failed(source, op, cause) }
/// Construct the bounded incomplete-manifest outcome.
pub fn bounded_incomplete_manifest(without_exit: &[String], reason: &str) -> StatusReply { StatusReply::incomplete_manifest(without_exit, reason) }
}

/// Constructors for the NodeCommandReply family.
pub mod node_command {
use crate::node_command::*;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::status::{ConnectionScope, ConnectionsReceipt, MAX_CONNECTION_IDS};
use crate::streaming::{FrameKind, StreamingProtocol};
use crate::{ConnectionId, IncarnationId, NodeId};
use serde::{Deserialize, Serialize};
/// The living birth refuses a lifecycle command against its held state.
pub fn rejected_invalid_node_transition(current: crate::status::NodeState) -> NodeCommandReply { NodeCommandReply::RejectedInvalidNodeTransition { current } }
/// An accepted command failed at the named step with its typed code.
pub fn failed(step: String, reason: String, code: crate::outcome::Code) -> NodeCommandReply { NodeCommandReply::Failed { step, reason, code } }
/// A storage command's answer once its `CommandAccepted` is sent: after acceptance the stream ends
/// with the command's terminal or `Failed { step, reason, code }`, never a refusal (node-rpc-envelope.md,
/// NodeCommand op 0x22). A not-ready answer is `Failed` at `step`, `UNAVAILABLE`; a stale change is
/// `Failed` at `step`, `ABORTED`, naming the change the birth acted on; every other answer is as it is.
pub fn after_acceptance(step: &str, reply: NodeCommandReply) -> NodeCommandReply {
    match reply {
        NodeCommandReply::NotReady { reason } => failed(step.into(), reason, crate::outcome::Code::Unavailable),
        NodeCommandReply::RejectedStaleChange { held } => failed(step.into(), format!("rejected-stale-change: this node acted on storage change {held}"), crate::outcome::Code::Aborted),
        other => other,
    }
}
/// Construct the typed connections-deleting outcome.
pub fn connections_deleting(node_id: NodeId, incarnation: IncarnationId) -> NodeCommandReply { NodeCommandReply::ConnectionsDeleting { node_id, incarnation } }
/// Construct the typed rejected-connection-scope outcome.
pub fn rejected_connection_scope(reason: String) -> NodeCommandReply { NodeCommandReply::RejectedConnectionScope { reason } }
/// Construct the typed rejected-stale-incarnation outcome.
pub fn rejected_stale_incarnation(held: IncarnationId) -> NodeCommandReply { NodeCommandReply::RejectedStaleIncarnation { held } }
/// Construct the typed rejected-stale-change outcome: a storage change older than the one the birth acted on.
pub fn rejected_stale_change(held: crate::ChangeId) -> NodeCommandReply { NodeCommandReply::RejectedStaleChange { held } }
/// Construct the typed not-ready outcome.
pub fn not_ready(reason: String) -> NodeCommandReply { NodeCommandReply::NotReady { reason } }
/// A caller's node command refused by the primary above the node before anything is sent, by name.
pub fn rejected(reason: &str, detail: String) -> NodeCommandReply { NodeCommandReply::Rejected { reason: reason.into(), detail } }
/// Construct the typed peer-unresolved outcome.
pub fn peer_unresolved(reason: String) -> NodeCommandReply { NodeCommandReply::PeerUnresolved { reason } }
/// Construct the typed busy outcome.
pub fn busy(reason: String) -> NodeCommandReply { NodeCommandReply::Busy { reason } }
/// Construct the typed rejected-connections-deleting outcome.
pub fn rejected_connections_deleting(reason: String) -> NodeCommandReply { NodeCommandReply::RejectedConnectionsDeleting { reason } }
/// Construct the typed malformed outcome.
pub fn malformed(kind: MalformedKind) -> NodeCommandReply { NodeCommandReply::Malformed { kind } }
/// How a node's property command ended once it was accepted: `node.config.updated`, `node.name.updated`, or how it ended short.
pub fn property_updated(reply: crate::status::StatusReply) -> NodeCommandReply { NodeCommandReply::PropertyUpdated { reply } }
/// A node's property command refused before anything was accepted, by name.
pub fn property_refused(reply: crate::status::StatusReply) -> NodeCommandReply { NodeCommandReply::PropertyRefused { reply } }
/// A caller's command whose subject's roles moved, answered naming the new mesh-primary `node_id` that node.primary.updated named (Q413): the caller sends the command there.
pub fn primary_updated(node_id: NodeId) -> NodeCommandReply { NodeCommandReply::PrimaryUpdated { node_id } }
}

/// Constructors for the BuildReply family.
pub mod build {
use crate::build::*;
use crate::context::CallContext;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::streaming::{FrameKind, StreamingProtocol};
use crate::{IncarnationId, NodeId, PathName};
use serde::{Deserialize, Serialize};
/// Construct the typed failed outcome.
pub fn failed(build_id: String, reason: String, code: crate::outcome::Code) -> BuildReply { BuildReply::Failed { build_id, reason, code } }
/// Construct the typed incomplete Build terminal (Q346).
pub fn incomplete(build_id: String, sections: Vec<crate::build::UnfinishedSection>) -> BuildReply { BuildReply::Incomplete { build_id, sections } }
/// Construct the typed not-fabric-primary outcome.
pub fn not_fabric_primary(fabric_primary: Option<NodeId>) -> BuildReply { BuildReply::NotFabricPrimary { fabric_primary } }
/// Construct the typed not-executor outcome.
pub fn not_executor(named: crate::framing::Birth, recipient: crate::framing::Birth) -> BuildReply { BuildReply::NotExecutor { named, recipient } }
/// Construct the typed unknown-build outcome.
pub fn unknown_build(build_id: String) -> BuildReply { BuildReply::UnknownBuild { build_id } }
/// Construct the typed rejected outcome.
pub fn rejected(reason: String, detail: String) -> BuildReply { BuildReply::Rejected { reason, detail } }
/// Construct the typed build-in-progress outcome.
pub fn build_in_progress(current_build_id: String) -> BuildReply { BuildReply::BuildInProgress { current_build_id } }
/// Construct the typed role-yielded outcome.
pub fn role_yielded(node: NodeId, by: String) -> BuildReply { BuildReply::RoleYielded { node, by } }
/// Construct the typed cannot-delete outcome.
pub fn cannot_delete(build_id: String, reason: String) -> BuildReply { BuildReply::CannotDelete { build_id, reason } }
/// Construct the typed peer-unresolved outcome.
pub fn peer_unresolved(reason: String) -> BuildReply { BuildReply::PeerUnresolved { reason } }
/// Construct the typed not-ready outcome.
pub fn not_ready(reason: String) -> BuildReply { BuildReply::NotReady { reason } }
/// Construct the typed busy outcome.
pub fn busy(reason: String) -> BuildReply { BuildReply::Busy { reason } }
/// Construct the typed connections-deleting outcome.
pub fn connections_deleting(reason: String) -> BuildReply { BuildReply::ConnectionsDeleting { reason } }
/// Construct the typed malformed outcome.
pub fn malformed(kind: MalformedKind) -> BuildReply { BuildReply::Malformed { kind } }
/// Construct the typed rejected-filter outcome: a `build.get` pair the Build header's schema refuses.
pub fn rejected_filter(reason: String) -> BuildReply { BuildReply::RejectedFilter { reason } }
}

/// Constructors for the JoinReply family.
pub mod join {
use crate::join::*;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::wire::WireDigest;
use crate::{IncarnationId, NodeId};
use serde::{Deserialize, Serialize};
/// Construct the typed join-mismatch outcome.
pub fn join_mismatch(field: String, deployed: String, reported: String) -> JoinReply { JoinReply::JoinMismatch { field, deployed, reported } }
/// Construct the typed not-authority outcome.
pub fn not_primary(primary: Option<NodeId>) -> JoinReply { JoinReply::NotPrimary { primary } }
/// Construct the typed peer-unresolved outcome.
pub fn peer_unresolved(reason: String) -> JoinReply { JoinReply::PeerUnresolved { reason } }
/// Construct the typed not-ready outcome.
pub fn not_ready(reason: String) -> JoinReply { JoinReply::NotReady { reason } }
/// Construct the typed busy outcome.
pub fn busy(reason: String) -> JoinReply { JoinReply::Busy { reason } }
/// Construct the typed connections-deleting outcome.
pub fn connections_deleting(reason: String) -> JoinReply { JoinReply::ConnectionsDeleting { reason } }
/// Construct the typed malformed outcome.
pub fn malformed(kind: MalformedKind) -> JoinReply { JoinReply::Malformed { kind } }
/// Construct the typed deployment-abandoned outcome.
pub fn deployment_abandoned(build_id: String, node_id: NodeId, incarnation: IncarnationId) -> JoinReply { JoinReply::DeploymentAbandoned { build_id, node_id, incarnation } }
/// Construct the typed cert-refused outcome.
pub fn cert_refused(name: String, detail: String) -> JoinReply { JoinReply::CertRefused { name, detail } }
}

/// Constructors for the NodePrimaryUpdateReply family.
pub mod handover {
use crate::handover::*;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
/// Construct the typed rejected-conflicting-payload outcome.
pub fn rejected_conflicting_payload(operation: crate::OperationId) -> NodePrimaryUpdateReply { NodePrimaryUpdateReply::RejectedConflictingPayload { operation } }
/// Construct the typed already-applied outcome of a take-over applied already.
pub fn already_applied(revisions: Vec<(String, crate::framing::Birth, crate::framing::Birth, u64)>) -> NodePrimaryUpdateReply { NodePrimaryUpdateReply::AlreadyApplied { revisions } }
/// Construct the typed rejected-not-primary outcome.
pub fn rejected_not_primary(reason: String) -> NodePrimaryUpdateReply { NodePrimaryUpdateReply::RejectedNotPrimary { reason } }
/// Construct the typed rejected-stale-revision outcome.
pub fn rejected_stale_revision(r#type: String, expected: u64, current: u64) -> NodePrimaryUpdateReply { NodePrimaryUpdateReply::RejectedStaleRevision { r#type, expected, current } }
/// Construct the typed not-ready outcome.
pub fn not_ready(reason: String) -> NodePrimaryUpdateReply { NodePrimaryUpdateReply::NotReady { reason } }
/// Construct the typed malformed outcome.
pub fn malformed(kind: MalformedKind) -> NodePrimaryUpdateReply { NodePrimaryUpdateReply::Malformed { kind } }
/// Construct the typed peer-unresolved outcome.
pub fn peer_unresolved(reason: String) -> NodePrimaryUpdateReply { NodePrimaryUpdateReply::PeerUnresolved { reason } }
/// Construct the typed busy outcome.
pub fn busy(reason: String) -> NodePrimaryUpdateReply { NodePrimaryUpdateReply::Busy { reason } }
/// Construct the typed connections-deleting outcome.
pub fn connections_deleting(reason: String) -> NodePrimaryUpdateReply { NodePrimaryUpdateReply::ConnectionsDeleting { reason } }
/// Construct the typed failed outcome.
pub fn failed(step: String, reason: String, code: crate::outcome::Code) -> NodePrimaryUpdateReply { NodePrimaryUpdateReply::failed(&step, &reason, code) }
/// Construct the bounded failed outcome.
pub fn bounded_failed(step: &str, reason: &str, code: crate::outcome::Code) -> NodePrimaryUpdateReply { NodePrimaryUpdateReply::failed(step, reason, code) }
/// Construct the bounded not-primary outcome.
pub fn bounded_not_primary(reason: &str) -> NodePrimaryUpdateReply { NodePrimaryUpdateReply::not_primary(reason) }
}

/// Constructors for the TopologyReply family.
pub mod topology {
use crate::topology::*;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::streaming::{FrameKind, StreamingProtocol};
use crate::wire::WireDigest;
use crate::{EndpointId, IncarnationId, LifecycleOp, MeshId, NodeId, Primary, PrimaryRole, PublisherId};
use serde::{Deserialize, Serialize};
/// Construct the typed not-ready outcome.
pub fn not_ready(reason: String) -> TopologyReply { TopologyReply::NotReady { reason } }
/// Construct the typed unknown-mesh outcome.
pub fn unknown_mesh(mesh: String) -> TopologyReply { TopologyReply::UnknownMesh { mesh } }
/// Construct the typed peer-unresolved outcome.
pub fn peer_unresolved(reason: String) -> TopologyReply { TopologyReply::PeerUnresolved { reason } }
/// Construct the typed busy outcome.
pub fn busy(reason: String) -> TopologyReply { TopologyReply::Busy { reason } }
/// Construct the typed connections-deleting outcome.
pub fn connections_deleting(reason: String) -> TopologyReply { TopologyReply::ConnectionsDeleting { reason } }
/// Construct the typed malformed outcome.
pub fn malformed(kind: MalformedKind) -> TopologyReply { TopologyReply::Malformed { kind } }
}

/// Constructors for the ForwardReply family.
pub mod forward {
use crate::forward::*;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::NodeId;
use serde::{Deserialize, Serialize};
use std::time::Duration;
/// Construct the typed inner-not-sent outcome.
pub fn inner_not_sent(reason: String, cause: NotSentCause) -> ForwardReply { ForwardReply::InnerNotSent { reason, cause } }
/// Construct the typed inner-unserved outcome.
pub fn inner_unserved(op: u8) -> ForwardReply { ForwardReply::InnerUnserved { op } }
/// Construct the typed inner-rejected-stale outcome.
pub fn inner_rejected_stale(target_node_id: NodeId) -> ForwardReply { ForwardReply::InnerRejectedStale { target_node_id } }
/// Construct the typed inner-indeterminate outcome.
pub fn inner_indeterminate(reason: String, cause: IndeterminateCause) -> ForwardReply { ForwardReply::InnerIndeterminate { reason, cause } }
/// Construct the typed not-forwardable outcome.
pub fn not_forwardable(op: u8) -> ForwardReply { ForwardReply::NotForwardable { op } }
/// Construct the typed peer-unresolved outcome.
pub fn peer_unresolved(reason: String) -> ForwardReply { ForwardReply::PeerUnresolved { reason } }
/// Construct the typed not-ready outcome.
pub fn not_ready(reason: String) -> ForwardReply { ForwardReply::NotReady { reason } }
/// Construct the typed busy outcome.
pub fn busy(reason: String) -> ForwardReply { ForwardReply::Busy { reason } }
/// Construct the typed connections-deleting outcome.
pub fn connections_deleting(reason: String) -> ForwardReply { ForwardReply::ConnectionsDeleting { reason } }
/// Construct the typed malformed outcome.
pub fn malformed(kind: MalformedKind) -> ForwardReply { ForwardReply::Malformed { kind } }
/// Construct the typed connection-lost outcome.
pub fn connection_lost(reason: String) -> ForwardReply { ForwardReply::ConnectionLost { reason } }
/// Construct the typed no-inner-budget outcome.
pub fn no_inner_budget(remaining_ms: u64, reserve_ms: u64) -> ForwardReply { ForwardReply::NoInnerBudget { remaining_ms, reserve_ms } }
}

/// Constructors for the PingReply family.
pub mod ping {
use crate::ping::*;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
/// Construct the typed peer-unresolved outcome.
pub fn peer_unresolved(reason: String) -> PingReply { PingReply::PeerUnresolved { reason } }
/// Construct the typed not-ready outcome.
pub fn not_ready(reason: String) -> PingReply { PingReply::NotReady { reason } }
/// Construct the typed busy outcome.
pub fn busy(reason: String) -> PingReply { PingReply::Busy { reason } }
/// Construct the typed connections-deleting outcome.
pub fn connections_deleting(reason: String) -> PingReply { PingReply::ConnectionsDeleting { reason } }
/// Construct the typed malformed outcome.
pub fn malformed(kind: MalformedKind) -> PingReply { PingReply::Malformed { kind } }
}

/// The Status applied outcome.
pub use status::applied;
/// The Status already-applied outcome.
pub use status::already_applied;
/// A stale exact incarnation.
pub use status::rejected_stale_incarnation as stale;
/// An operation without a handler.
pub use status::request_not_handled as not_handled;
