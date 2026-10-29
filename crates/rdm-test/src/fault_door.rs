//! The fault door: a testkit-only Node RPC protocol on op `0x75` that arms, releases and reads the
//! cuts of a faulted node-admin ([`crate::admin_faults`]). A scenario reaches the admin by its
//! `path.name` like every other Node RPC call; the admin's readiness gate answers NotReady until its
//! Node RPC door is ready, so a cut that must be in force before then is armed from
//! `<root>/faults/<name>.boot.json` (`admin_faults::arm_boot_cuts`). Testkit range only
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

/// The fault door protocol on op [`OP`].
pub struct FaultDoor;

/// The fault door's op tag.
pub const OP: u8 = <FaultDoor as NodeProtocol>::OP;

/// A cut as the wire carries it. [`CutSpec`] is the cut as a scenario writes it in JSON (a boot file,
/// a cell's arm), internally tagged on `kind`; postcard is not self-describing and cannot decode an
/// internally tagged enum, so on the wire the same variants travel externally tagged (serde's remote
/// form: the compiler holds every variant and field of this mirror to `CutSpec`'s).
#[derive(Serialize, Deserialize)]
#[serde(remote = "CutSpec")]
enum CutWire {
    Receipt { step: String, operation: Option<String>, node: Option<String>, failed: bool },
    Event { event: String, node: Option<String> },
    AcceptedBuild,
    ClaimedAttempt { attempt: Option<u32> },
    ProviderDomain,
    LeaveAnnouncement { announcements: Vec<u32>, channel: String },
    TopologyRead,
    Announcement { announcement: String, channel: String },
    ProviderTerminate { pid: Option<u32> },
    Declaration { node: Option<String>, state: Option<String> },
    ProviderUnproven { pids: Vec<u32> },
    DriftPass { node: Option<String> },
    StorageStep { node: Option<String> },
    StorageSwitch,
}

/// A fault door call.
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
    /// A Build step receipt about to become durable.
    ///
    /// Position 0 on the wire.
    Receipt {
        /// The step name.
        step: String,
        /// The receipt's operation key.
        operation: String,
        /// The Build.
        build_id: String,
        /// The attempt.
        attempt: u32,
    },
    /// A lifecycle event about to be published.
    ///
    /// Position 1 on the wire.
    Event {
        /// The event name.
        event: String,
        /// The lifecycle operation's key.
        operation: String,
        /// The Build.
        build_id: String,
        /// The attempt.
        attempt: u32,
        /// The node the event concerns.
        node: String,
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
    /// An attempt's claim, durable and broadcast, about to be returned to the drive.
    ///
    /// Position 3 on the wire.
    Claimed {
        /// The Build.
        build_id: String,
        /// The attempt.
        attempt: u32,
        /// The executor the claim names.
        executor: String,
    },
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
        /// The announcement (`rdm_transport::topology::Withhold`).
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
    /// A node's declaration at the status door, about to be decided.
    ///
    /// Position 9 on the wire.
    Declaration {
        /// The declaring node.
        node: String,
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
        /// The commanded node.
        node: String,
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
}

impl Hit {
    /// The hit as the cells and the hold span read it: one flat object of its fields.
    pub fn detail(&self) -> serde_json::Value {
        use serde_json::json;
        match self {
            Hit::Receipt { step, operation, build_id, attempt } => json!({"step": step, "operation": operation, "build_id": build_id, "attempt": attempt}),
            Hit::Event { event, operation, build_id, attempt, node, incarnation_id } => json!({"event": event, "operation": operation, "build_id": build_id, "attempt": attempt, "node": node, "incarnation_id": incarnation_id}),
            Hit::Accepted { build_id } => json!({"build_id": build_id}),
            Hit::Claimed { build_id, attempt, executor } => json!({"build_id": build_id, "attempt": attempt, "executor": executor}),
            Hit::ProviderDomain => json!({"control_domain": "reported foreign"}),
            Hit::LeaveAnnouncement { node, announcement, channel } => json!({"node": node, "announcement": announcement, "channel": channel}),
            Hit::TopologyRead { node, reads_from } => json!({"node": node, "reads_from": reads_from}),
            Hit::Announcement { node, announcement, channel } => json!({"node": node, "announcement": announcement, "channel": channel}),
            Hit::ProviderTerminate { pid, deployment_id } => json!({"pid": pid, "deployment_id": deployment_id}),
            Hit::Declaration { node, state } => json!({"node": node, "state": state}),
            Hit::ProviderInspect { pid } => json!({"pid": pid}),
            Hit::DriftPass { node } => json!({"node": node}),
            Hit::StorageStep { node, change_id } => json!({"node": node, "change_id": change_id}),
            Hit::StorageSwitch { change_id } => json!({"change_id": change_id}),
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
    /// Every Build step receipt the Build decorator saw.
    pub receipts_seen: u64,
}

/// A fault door answer.
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
    /// The door refused the call by name: the id is armed already, or no cut is armed under it.
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
    /// The admin's Node RPC door is not ready.
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

impl NodeProtocol for FaultDoor {
    const OP: u8 = 0x75;
    const NAME: &'static str = "fault-door";
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

/// Serve the door over `faults` on the admin's Node RPC server: arm, release and read its cuts. The
/// door answers whoever calls (R-SV1): it is the testkit's own executable's.
pub fn serve(b: ServerBuilder, faults: Arc<AdminFaults>) -> ServerBuilder {
    b.serve::<FaultDoor, _, _>(OpOwner::Testkit, move |_peer: PeerContext, req: FaultRequest| {
        let faults = faults.clone();
        async move {
            let node = faults.name.clone();
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

/// One call of the door at the admin `admin`, by its `path.name`: the door's answer, or how the call
/// ended, named. A NotReady answer is the admin's Node RPC door not ready yet.
pub async fn ask(client: &NodeRpcClient, admin: &PathName, req: &FaultRequest) -> Result<FaultReply, String> {
    let (out, _) = client.call::<FaultDoor>(&NodeTarget::CurrentPath(admin.clone()), req, &CallOptions::default()).await;
    match out {
        RpcOutcome::Reply(r) => Ok(r.into_value()),
        other => Err(format!("the fault door call at {admin} ended {}: {other:?}", other.name())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One cut of every kind the door arms.
    fn every_cut() -> Vec<CutSpec> {
        vec![
            CutSpec::Receipt { step: "StopNode".into(), operation: Some("node.delete:mesh2.rpc.2".into()), node: None, failed: false },
            CutSpec::Receipt { step: "TerminateRuntime".into(), operation: Some("node.delete".into()), node: Some("mesh2.rpc.2".into()), failed: true },
            CutSpec::Event { event: "deleting".into(), node: Some("mesh1.rpc.1".into()) },
            CutSpec::AcceptedBuild,
            CutSpec::ClaimedAttempt { attempt: Some(2) },
            CutSpec::ProviderDomain,
            CutSpec::LeaveAnnouncement { announcements: vec![1, 3], channel: "mesh".into() },
            CutSpec::TopologyRead,
            CutSpec::Announcement { announcement: "aggregate-in-flight".into(), channel: "any".into() },
            CutSpec::ProviderTerminate { pid: Some(4242) },
            CutSpec::StorageStep { node: Some("mesh2.rpc.1".into()) },
            CutSpec::StorageSwitch,
        ]
    }

    // CONTRACT: the Node RPC wire is postcard (rafka-node-rpc-contract protocol.rs): an arm carries
    // its cut across it and the admin decodes the same cut, for every kind of cut the door arms.
    #[test]
    fn an_arm_of_every_cut_kind_round_trips_on_the_wire() {
        for spec in every_cut() {
            let req = FaultRequest::arm("c", spec.clone());
            let bytes = FaultDoor::encode_request(&req).unwrap_or_else(|e| panic!("{spec:?} does not encode: {e:?}"));
            let back = FaultDoor::decode_request(&bytes).unwrap_or_else(|e| panic!("{spec:?} does not decode: {e:?}"));
            assert_eq!(back.cut_spec(), Some(spec));
        }
        for req in [FaultRequest::Release { id: "c".into() }, FaultRequest::State] {
            assert_eq!(FaultDoor::decode_request(&FaultDoor::encode_request(&req).unwrap()).unwrap(), req);
        }
    }

    // CONTRACT: every answer the door gives decodes as it was sent, the state's cuts with what they held.
    #[test]
    fn every_reply_variant_encodes_and_round_trips() {
        let row = |spec: CutSpec, hit: Option<Hit>| CutRow { spec, armed: false, held: true, released: true, hit, held_for_ms: Some(7), seen_while_held: 1 };
        let cuts: BTreeMap<String, CutRow> = [
            ("a".to_string(), row(CutSpec::TopologyRead, Some(Hit::TopologyRead { node: "mesh1.admin.4".into(), reads_from: "mesh1.admin.1".into() }))),
            ("b".to_string(), row(CutSpec::ClaimedAttempt { attempt: None }, Some(Hit::Claimed { build_id: "b".into(), attempt: 1, executor: "mesh1.admin.1".into() }))),
            ("c".to_string(), row(CutSpec::ProviderDomain, None)),
        ]
        .into();
        let all = [
            FaultReply::Armed { node: "mesh1.admin.1".into(), id: "c".into(), spec: CutSpec::Receipt { step: "StartNode".into(), operation: None, node: Some("mesh1.rpc.1".into()), failed: false } },
            FaultReply::Released { node: "mesh1.admin.1".into(), id: "c".into(), was_held: true, held_ms: Some(3) },
            FaultReply::State(FaultState { node: "mesh1.admin.1".into(), cuts, receipts_seen: 4 }),
            FaultReply::Refused { reason: "r".into() },
            FaultReply::PeerUnresolved { reason: "r".into() },
            FaultReply::NotReady { reason: "r".into() },
            FaultReply::Busy { reason: "r".into() },
            FaultReply::ConnectionsDeleting { reason: "r".into() },
            FaultReply::Malformed { kind: MalformedKind::Corrupt },
            FaultReply::CallerRefused { reason: "r".into() },
        ];
        assert_eq!(all.len() as u32, FaultDoor::REPLY_VARIANTS);
        for r in all {
            let bytes = FaultDoor::encode_reply(&r).unwrap_or_else(|e| panic!("{r:?} does not encode: {e:?}"));
            assert_eq!(FaultDoor::decode_reply(&bytes).unwrap_or_else(|e| panic!("{r:?} does not decode: {e:?}")), r);
        }
    }
}
