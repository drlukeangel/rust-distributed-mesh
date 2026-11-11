//! The fault service: a testkit-only Node RPC protocol on op `0x75` that arms, releases and reads the
//! cuts of a faulted node-admin ([`crate::admin_faults`]). A scenario reaches the admin by its
//! ID or an explicit path/name pair; the admin's readiness gate answers NotReady until its
//! readiness gate is open, so a cut that must be in force before then is armed from
//! `<root>/faults/<location-index>.boot.json` (`admin_faults::arm_boot_cuts`). Testkit range only
//! ([`TESTKIT_OPS`]); the testkit's own `faulted-node-admin` serves it, never a product binary.
//!
//! [`TESTKIT_OPS`]: rdm_core::catalog::TESTKIT_OPS

use crate::admin_faults::{AdminFaults, CutSpec};
use rdm_core::PathName;
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget, PeerContext, ServerBuilder};
use rdm_core::catalog::OpOwner;
use rdm_core::outcome::{MalformedKind, ReplyKind, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// The fault service protocol on op [`OP`].
pub struct Fault;

/// The fault service's op tag.
pub const OP: u8 = <Fault as NodeProtocol>::OP;

/// A cut as the wire carries it. [`CutSpec`] is the cut as a scenario writes it in JSON (a boot file,
/// a cell's arm), internally tagged on `kind`; postcard is not self-describing and cannot decode an
/// internally tagged enum, so on the wire the same variants travel externally tagged (serde's remote
/// form: the compiler holds every variant and field of this mirror to `CutSpec`'s).
#[derive(Serialize, Deserialize)]
#[serde(remote = "CutSpec")]
enum CutWire {
    StepEvent { step: String, operation: Option<String>, node: Option<rdm_core::OperationSubject>, at: Option<rdm_core::PathName>, failed: bool },
    Event { event: String, node: Option<rdm_core::NodeId> },
    AcceptedBuild,
    ProviderDomain,
    LeaveAnnouncement { announcements: Vec<u32>, channel: String },
    TopologyRead,
    Announcement { announcement: String, channel: String },
    ProviderTerminate { pid: Option<u32> },
    Declaration { node_id: Option<rdm_core::NodeId>, node: Option<PathName>, state: Option<String> },
    ProviderUnknown { pids: Vec<u32> },
    DriftPass { node: Option<String> },
    StorageStep { node: Option<PathName> },
    StorageSwitch,
    StoreAppend { file: String, key: String, after: bool },
    PathTarget { node: Option<PathName> },
    PreNodeRecreate { node: Option<PathName> },
}

/// A fault service call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultRequest {
    /// Arm `spec` under `id`.
    ///
    /// Position 0 on the wire.
    Arm {
        /// The cut's id.
        id: String,
        /// Where the cut holds a call.
        #[serde(with = "CutWire")]
        spec: CutSpec,
    },
    /// Release cut `id`.
    ///
    /// Position 1 on the wire.
    Release {
        /// The cut's id.
        id: String,
    },
    /// Every cut the admin holds and what each held.
    ///
    /// Position 2 on the wire.
    State,
}

impl FaultRequest {
    /// Arm `spec` under `id`.
    pub fn arm(id: &str, spec: CutSpec) -> Self {
        Self::Arm { id: id.to_string(), spec }
    }

    /// The cut an arm carries.
    pub fn cut_spec(&self) -> Option<CutSpec> {
        match self {
            Self::Arm { spec, .. } => Some(spec.clone()),
            _ => None,
        }
    }
}

/// The call a cut held first, as the decorator offered it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Hit {
    /// A Build step event about to become durable.
    ///
    /// Position 0 on the wire.
    StepEvent {
        /// The step name.
        step: String,
        /// The step event's operation key.
        operation: rdm_core::OperationId,
        /// The Build.
        build_id: String,
    },
    /// A lifecycle event about to be published.
    ///
    /// Position 1 on the wire.
    Event {
        /// The event name.
        event: String,
        /// The lifecycle operation's key.
        operation: rdm_core::OperationId,
        /// The Build.
        build_id: String,
        /// The ID of the node the event concerns.
        node: rdm_core::NodeId,
        /// Its incarnation.
        incarnation_id: String,
    },
    /// An accepted Build about to become durable.
    ///
    /// Position 2 on the wire.
    Accepted {
        /// The Build.
        build_id: String,
    },
    /// Reserved: an attempt's claim. A Build has no attempts (Q344); never sent.
    ///
    /// Position 3 on the wire.
    Reserved3,
    /// The provider reported a control domain no held birth is in.
    ///
    /// Position 4 on the wire.
    ProviderDomain,
    /// A leave announcement about to be published.
    ///
    /// Position 5 on the wire.
    LeaveAnnouncement {
        /// The announcing node.
        node: String,
        /// The announcement's 1-based number.
        announcement: u32,
        /// The channel it is published on.
        channel: String,
    },
    /// A joining admin's topology read, about to be made.
    ///
    /// Position 6 on the wire.
    TopologyRead {
        /// The joining admin.
        node: String,
        /// The admin the read goes to: the one that answered the join.
        reads_from: String,
    },
    /// A final mesh topology or leadership announcement about to be sent on a channel.
    ///
    /// Position 7 on the wire.
    Announcement {
        /// The announcing node.
        node: String,
        /// The announcement (`rdm_gossip::backbone::Withhold`).
        announcement: String,
        /// The channel it is sent on.
        channel: String,
    },
    /// A provider `terminate` refused while the cut is armed.
    ///
    /// Position 8 on the wire.
    ProviderTerminate {
        /// The runtime's pid, when the handle names one.
        pid: Option<u32>,
        /// The deployment the runtime belongs to.
        deployment_id: String,
    },
    /// A node's declaration at the `StatusService`, about to be decided.
    ///
    /// Position 9 on the wire.
    Declaration {
        /// The declaring node's globally unique identity.
        node_id: rdm_core::NodeId,
        /// Its separately observed location.
        node: Option<PathName>,
        /// The state it declares.
        state: String,
    },
    /// A provider `inspect` answered `Unknown` while the cut is armed.
    ///
    /// Position 10 on the wire.
    ProviderInspect {
        /// The runtime's pid, when the handle names one.
        pid: Option<u32>,
    },
    /// A reconcile pass the drift seam held.
    ///
    /// Position 11 on the wire.
    DriftPass {
        /// The admin whose pass it is.
        node: String,
    },
    /// A storage change's move the mesh-primary held on its way to its node.
    ///
    /// Position 12 on the wire.
    StorageStep {
        /// The commanded node's separate path.
        path: String,
        /// The name, beside its path.
        name: String,
        /// The change.
        change_id: String,
    },
    /// A storage change's switch the fabric-primary held.
    ///
    /// Position 13 on the wire.
    StorageSwitch {
        /// The change.
        change_id: String,
    },
    /// A store append held in the admin's disk job, before or after its durable write.
    ///
    /// Position 14 on the wire.
    StoreAppend {
        /// The store file.
        file: String,
        /// The record's key.
        key: String,
        /// Held after the line was written and fsynced.
        after: bool,
    },
    /// A create held before it acts on its target's verdict on the unreached birth at a path.
    ///
    /// Position 15 on the wire.
    PathTarget {
        /// The path's separate path.
        path: String,
        /// The name, beside its path.
        name: String,
        /// The unreached birth's node id.
        node_id: rdm_core::NodeId,
    },
    /// A node recovery held before it asks the application's `pre_node_recreate`, its lost birth deleted.
    ///
    /// Position 16 on the wire.
    PreNodeRecreate {
        /// The path's separate path.
        path: String,
        /// The name, beside its path.
        name: String,
        /// The lost birth's node id.
        node_id: rdm_core::NodeId,
    },
}

impl Hit {
    /// The hit as the cells and the hold span read it: one flat object of its fields.
    pub fn detail(&self) -> serde_json::Value {
        use serde_json::json;
        match self {
            Hit::StepEvent { step, operation, build_id } => json!({"step": step, "operation": operation, "build_id": build_id}),
            Hit::Event { event, operation, build_id, node, incarnation_id } => json!({"event": event, "operation": operation, "build_id": build_id, "node": node, "incarnation_id": incarnation_id}),
            Hit::Accepted { build_id } => json!({"build_id": build_id}),
            Hit::Reserved3 => json!({}),
            Hit::ProviderDomain => json!({"control_domain": "reported foreign"}),
            Hit::LeaveAnnouncement { node, announcement, channel } => json!({"node": node, "announcement": announcement, "channel": channel}),
            Hit::TopologyRead { node, reads_from } => json!({"node": node, "reads_from": reads_from}),
            Hit::Announcement { node, announcement, channel } => json!({"node": node, "announcement": announcement, "channel": channel}),
            Hit::ProviderTerminate { pid, deployment_id } => json!({"pid": pid, "deployment_id": deployment_id}),
            Hit::Declaration { node_id, node, state } => json!({"node_id": node_id, "node": node, "state": state}),
            Hit::ProviderInspect { pid } => json!({"pid": pid}),
            Hit::DriftPass { node } => json!({"node": node}),
            Hit::StorageStep { path, name, change_id } => json!({"path": path, "name": name, "change_id": change_id}),
            Hit::StorageSwitch { change_id } => json!({"change_id": change_id}),
            Hit::StoreAppend { file, key, after } => json!({"file": file, "key": key, "after": after}),
            Hit::PathTarget { path, name, node_id } => json!({"path": path, "name": name, "node_id": node_id}),
            Hit::PreNodeRecreate { path, name, node_id } => json!({"path": path, "name": name, "node_id": node_id}),
        }
    }
}

/// One armed cut and what it held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CutRow {
    /// Where the cut holds a call.
    #[serde(with = "CutWire")]
    pub spec: CutSpec,
    /// Not released yet.
    pub armed: bool,
    /// It holds a call now (or, for a condition, it is in force).
    pub held: bool,
    /// Released: it holds nothing more.
    pub released: bool,
    /// The first call it held.
    pub hit: Option<Hit>,
    /// How long it has held, or held.
    pub held_for_ms: Option<u64>,
    /// Matching calls that arrived while another was held here.
    pub seen_while_held: u64,
}

/// Every cut of one admin and the consequence counters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultState {
    /// The admin's `path.name`.
    pub node: String,
    /// Every cut, by id.
    pub cuts: BTreeMap<String, CutRow>,
    /// Every Build step event the Build decorator saw.
    pub step_events_seen: u64,
}

/// A fault service answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultReply {
    /// The cut is armed.
    ///
    /// Position 0 on the wire.
    Armed {
        /// The admin.
        node: String,
        /// The cut's id.
        id: String,
        /// Where it holds a call.
        #[serde(with = "CutWire")]
        spec: CutSpec,
    },
    /// The cut is released.
    ///
    /// Position 1 on the wire.
    Released {
        /// The admin.
        node: String,
        /// The cut's id.
        id: String,
        /// It held a call when released.
        was_held: bool,
        /// How long it held.
        held_ms: Option<u64>,
    },
    /// Every cut and the counters.
    ///
    /// Position 2 on the wire.
    State(FaultState),
    /// The fault service refused the call by name: the id is armed already, or no cut is armed under it.
    ///
    /// Position 3 on the wire.
    Refused {
        /// Why.
        reason: String,
    },
    /// The peer the call needed could not be resolved.
    ///
    /// Position 4 on the wire.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// The admin's readiness gate is not open.
    ///
    /// Position 5 on the wire.
    NotReady {
        /// Why the node is not ready.
        reason: String,
    },
    /// The node is at its admission bound.
    ///
    /// Position 6 on the wire.
    Busy {
        /// Which bound it is at.
        reason: String,
    },
    /// The node is deleting its connections (`node.connection.delete`) and takes no new call.
    ///
    /// Position 7 on the wire.
    ConnectionsDeleting {
        /// Why it refuses the call.
        reason: String,
    },
    /// The request frame was malformed.
    ///
    /// Position 8 on the wire.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The transport refused the caller this call.
    ///
    /// Position 9 on the wire.
    CallerRefused {
        /// Why the call is refused.
        reason: String,
    },
}

impl NodeProtocol for Fault {
    const OP: u8 = 0x75;
    const NAME: &'static str = "fault";
    const MAX_REQUEST_FRAME_BYTES: usize = 4 * 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024;
    const REQUEST_VARIANTS: u32 = 3;
    const REPLY_VARIANTS: u32 = 10;

    type Request = FaultRequest;
    type Reply = FaultReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            FaultReply::Armed { .. } => "armed",
            FaultReply::Released { .. } => "released",
            FaultReply::State(..) => "state",
            FaultReply::Refused { .. } => "refused",
            FaultReply::PeerUnresolved { .. } => "peer-unresolved",
            FaultReply::NotReady { .. } => "not-ready",
            FaultReply::Busy { .. } => "busy",
            FaultReply::ConnectionsDeleting { .. } => "connections-deleting",
            FaultReply::Malformed { .. } => "malformed",
            FaultReply::CallerRefused { .. } => "caller-refused",
        }
    }

    fn classify_reply(reply: &FaultReply) -> ReplyKind {
        match reply {
            FaultReply::State(_) => ReplyKind::Success,
            FaultReply::Armed { .. } => ReplyKind::Success,
            FaultReply::Released { .. } => ReplyKind::Success,
            FaultReply::Refused { .. } => ReplyKind::new(rdm_core::outcome::Code::FailedPrecondition),
            FaultReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            FaultReply::NotReady { .. } => ReplyKind::NotReady,
            FaultReply::Busy { .. } => ReplyKind::Busy,
            FaultReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            FaultReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            FaultReply::CallerRefused { .. } => ReplyKind::Unauthorized,
        }
    }

    fn peer_unresolved(reason: String) -> FaultReply {
        FaultReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> FaultReply {
        FaultReply::NotReady { reason }
    }
    fn busy(reason: String) -> FaultReply {
        FaultReply::Busy { reason }
    }
    fn connections_deleting(reason: String) -> FaultReply {
        FaultReply::ConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> FaultReply {
        FaultReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> FaultReply {
        FaultReply::CallerRefused { reason }
    }
}

/// Serve the fault service over `faults` on the admin's Node RPC server: arm, release and read its cuts. The
/// fault service answers whoever calls (R-SV1): it is the testkit's own executable's.
pub fn serve(b: ServerBuilder, faults: Arc<AdminFaults>) -> ServerBuilder {
    b.serve::<Fault, _, _>(OpOwner::Testkit, move |_peer: PeerContext, req: FaultRequest| {
        let faults = faults.clone();
        async move {
            let node = faults.node_id.to_string();
            Ok(match req {
                FaultRequest::Arm { id, spec } => match faults.arm(&id, spec.clone()) {
                    Ok(()) => FaultReply::Armed { node, id, spec },
                    Err(reason) => FaultReply::Refused { reason },
                },
                FaultRequest::Release { id } => match faults.release(&id) {
                    Ok((was_held, held_ms)) => FaultReply::Released { node, id, was_held, held_ms },
                    Err(reason) => FaultReply::Refused { reason },
                },
                FaultRequest::State => FaultReply::State(faults.state()),
            })
        }
    })
}

/// One call of the fault service at the admin `admin`, by its explicit target: its answer, or how the call
/// ended, named. A NotReady answer is the admin's readiness gate not ready yet.
pub async fn ask(client: &NodeRpcClient, admin: &NodeTarget, req: &FaultRequest) -> Result<FaultReply, String> {
    let (out, _) = client.call::<Fault>(admin, req, &CallOptions::default()).await;
    match out {
        RpcOutcome::Reply(r) => Ok(r.into_value()),
        other => Err(format!("the fault service call at {admin:?} ended {}: {other:?}", other.name())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One cut of every kind the fault service arms.
    fn every_cut() -> Vec<CutSpec> {
        vec![
            CutSpec::StepEvent { step: "StopNode".into(), operation: Some("node.delete".into()), node: None, at: None, failed: false },
            CutSpec::StepEvent { step: "TerminateRuntime".into(), operation: Some("node.delete".into()), node: Some(rdm_core::OperationSubject::Node(rdm_core::NodeId::parse("200000000000").unwrap())), at: None, failed: true },
            CutSpec::Event { event: "deleting".into(), node: Some(rdm_core::NodeId::parse("100000000000").unwrap()) },
            CutSpec::AcceptedBuild,
            CutSpec::ProviderDomain,
            CutSpec::LeaveAnnouncement { announcements: vec![1, 3], channel: "mesh".into() },
            CutSpec::TopologyRead,
            CutSpec::Announcement { announcement: "aggregate-in-flight".into(), channel: "any".into() },
            CutSpec::ProviderTerminate { pid: Some(4242) },
            CutSpec::StorageStep { node: Some(PathName::new("mesh2.node", "1").unwrap()) },
            CutSpec::StorageSwitch,
            CutSpec::PreNodeRecreate { node: Some(PathName::new("mesh2.node", "1").unwrap()) },
        ]
    }

    // CONTRACT: the Node RPC wire is postcard (rafka-node-rpc-contract protocol.rs): an arm carries
    // its cut across it and the admin decodes the same cut, for every kind of cut the fault service arms.
    #[test]
    fn an_arm_of_every_cut_kind_round_trips_on_the_wire() {
        for spec in every_cut() {
            let req = FaultRequest::arm("c", spec.clone());
            let bytes = Fault::encode_request(&req).unwrap_or_else(|e| panic!("{spec:?} does not encode: {e:?}"));
            let back = Fault::decode_request(&bytes).unwrap_or_else(|e| panic!("{spec:?} does not decode: {e:?}"));
            assert_eq!(back.cut_spec(), Some(spec));
        }
        for req in [FaultRequest::Release { id: "c".into() }, FaultRequest::State] {
            assert_eq!(Fault::decode_request(&Fault::encode_request(&req).unwrap()).unwrap(), req);
        }
    }

    // CONTRACT: every answer the fault service gives decodes as it was sent, the state's cuts with what they held.
    #[test]
    fn every_reply_variant_encodes_and_round_trips() {
        let row = |spec: CutSpec, hit: Option<Hit>| CutRow { spec, armed: false, held: true, released: true, hit, held_for_ms: Some(7), seen_while_held: 1 };
        let cuts: BTreeMap<String, CutRow> = [
            ("a".to_string(), row(CutSpec::TopologyRead, Some(Hit::TopologyRead { node: "mesh1.node-admin.4".into(), reads_from: "mesh1.node-admin.1".into() }))),
            ("c".to_string(), row(CutSpec::ProviderDomain, None)),
        ]
        .into();
        let all = [
            FaultReply::Armed { node: "mesh1.node-admin.1".into(), id: "c".into(), spec: CutSpec::StepEvent { step: "StartNode".into(), operation: None, node: Some(rdm_core::OperationSubject::Node(rdm_core::NodeId::parse("100000000000").unwrap())), at: None, failed: false } },
            FaultReply::Released { node: "mesh1.node-admin.1".into(), id: "c".into(), was_held: true, held_ms: Some(3) },
            FaultReply::State(FaultState { node: "mesh1.node-admin.1".into(), cuts, step_events_seen: 4 }),
            FaultReply::Refused { reason: "r".into() },
            FaultReply::PeerUnresolved { reason: "r".into() },
            FaultReply::NotReady { reason: "r".into() },
            FaultReply::Busy { reason: "r".into() },
            FaultReply::ConnectionsDeleting { reason: "r".into() },
            FaultReply::Malformed { kind: MalformedKind::Corrupt },
            FaultReply::CallerRefused { reason: "r".into() },
        ];
        assert_eq!(all.len() as u32, Fault::REPLY_VARIANTS);
        for r in all {
            let bytes = Fault::encode_reply(&r).unwrap_or_else(|e| panic!("{r:?} does not encode: {e:?}"));
            assert_eq!(Fault::decode_reply(&bytes).unwrap_or_else(|e| panic!("{r:?} does not decode: {e:?}")), r);
        }
    }
}
