//! The fault door: a testkit-only Node RPC protocol on op `0x75` that arms, releases and reads the
//! cuts of a faulted node-admin ([`crate::admin_faults`]). A scenario reaches the admin by its
//! `path.name` like every other Node RPC call; the admin's readiness gate answers NotReady until its
//! Node RPC door is ready, so a cut that must be in force before then is armed from
//! `<root>/faults/<name>.boot.json` (`admin_faults::arm_boot_cuts`). Testkit range only
//! ([`TESTKIT_OPS`]); the testkit's own `faulted-node-admin` serves it, never a product binary.
//!
//! [`TESTKIT_OPS`]: rafka_node_rpc_contract::catalog::TESTKIT_OPS

use crate::admin_faults::CutSpec;
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The fault door protocol on op `0x75`.
pub struct FaultDoor;

/// A fault door call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultRequest {
    /// Arm `spec` under `id`.
    Arm {
        /// The cut's id.
        id: String,
        /// Where the cut parks a call.
        spec: CutSpec,
    },
    /// Release cut `id`.
    Release {
        /// The cut's id.
        id: String,
    },
    /// Every cut the admin holds and what it parked.
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

/// The call a cut parked first, as the decorator offered it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Hit {
    /// A Build step receipt about to become durable.
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
    /// A lifecycle hook about to run.
    Hook {
        /// The hook phase.
        phase: String,
        /// The hook's target.
        target: String,
        /// The transition the hook belongs to.
        transition_id: String,
    },
    /// An accepted Build about to become durable.
    Accepted {
        /// The Build.
        build_id: String,
    },
    /// An attempt's claim, durable and broadcast, about to be returned to the drive.
    Claimed {
        /// The Build.
        build_id: String,
        /// The attempt.
        attempt: u32,
        /// The executor the claim names.
        executor: String,
    },
    /// The provider reported a control domain no held birth is in.
    ProviderDomain,
    /// A leave announcement about to be published.
    LeaveAnnouncement {
        /// The announcing node.
        node: String,
        /// The announcement's 1-based number.
        announcement: u32,
        /// The channel it is published on.
        channel: String,
    },
    /// A catch-up about to be sent to a neighbour.
    CatchUp {
        /// The sending node.
        node: String,
        /// The neighbour.
        neighbour: String,
    },
    /// The application's answer to a `sync_state` round, about to be returned.
    StateSync {
        /// The round's Build.
        build_id: String,
        /// The round's attempt.
        attempt: u32,
    },
    /// A joining admin's topology read, about to be made.
    TopologyRead {
        /// The joining admin.
        node: String,
        /// The admin the read goes to.
        authority: String,
    },
}

/// One armed cut and what it parked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CutRow {
    /// Where the cut parks a call.
    pub spec: CutSpec,
    /// Not released yet.
    pub armed: bool,
    /// A call is parked at it now (or, for a condition, it is in force).
    pub held: bool,
    /// Released: it parks nothing more.
    pub released: bool,
    /// The first call it parked.
    pub hit: Option<Hit>,
    /// How long it has held, or held.
    pub held_for_ms: Option<u64>,
    /// Matching calls that arrived while another was parked here.
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
    Armed {
        /// The admin.
        node: String,
        /// The cut's id.
        id: String,
        /// Where it parks a call.
        spec: CutSpec,
    },
    /// The cut is released.
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
    State(FaultState),
    /// The door refused the call by name: the id is armed already, or no cut is armed under it.
    Refused {
        /// Why.
        reason: String,
    },
    /// The peer the call needed could not be resolved.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// The admin's Node RPC door is not ready.
    NotReady {
        /// Why the node is not ready.
        reason: String,
    },
    /// The node is at its admission bound.
    Busy {
        /// Which bound it is at.
        reason: String,
    },
    /// The node takes no new work.
    Draining {
        /// Why it refuses new work.
        reason: String,
    },
    /// The request frame was malformed.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The caller is not allowed this call.
    Unauthorized {
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

    fn classify_reply(reply: &FaultReply) -> ReplyKind {
        match reply {
            FaultReply::Armed { .. } | FaultReply::Released { .. } | FaultReply::State(_) => ReplyKind::Success,
            FaultReply::Refused { .. } => ReplyKind::ProtocolRefusal,
            FaultReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            FaultReply::NotReady { .. } => ReplyKind::NotReady,
            FaultReply::Busy { .. } => ReplyKind::Busy,
            FaultReply::Draining { .. } => ReplyKind::Draining,
            FaultReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            FaultReply::Unauthorized { .. } => ReplyKind::Unauthorized,
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
    fn draining(reason: String) -> FaultReply {
        FaultReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> FaultReply {
        FaultReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> FaultReply {
        FaultReply::Unauthorized { reason }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One cut of every kind the door arms.
    fn every_cut() -> Vec<CutSpec> {
        vec![
            CutSpec::Receipt { step: "StopNode".into(), operation: Some("retire-node:mesh2.rpc.2".into()), node: None },
            CutSpec::Event { event: "deleting".into(), node: Some("mesh1.rpc.1".into()) },
            CutSpec::Hook { phase: "before_eligibility".into(), node: None },
            CutSpec::AcceptedBuild,
            CutSpec::ClaimedAttempt { attempt: Some(2) },
            CutSpec::ProviderDomain,
            CutSpec::LeaveAnnouncement { announcements: vec![1, 3], channel: "mesh".into() },
            CutSpec::CatchUp,
            CutSpec::StateSync,
            CutSpec::TopologyRead,
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

    // CONTRACT: every answer the door gives decodes as it was sent, the state's cuts with what they parked.
    #[test]
    fn every_reply_variant_encodes_and_round_trips() {
        let row = |spec: CutSpec, hit: Option<Hit>| CutRow { spec, armed: false, held: true, released: true, hit, held_for_ms: Some(7), seen_while_held: 1 };
        let cuts: BTreeMap<String, CutRow> = [
            ("a".to_string(), row(CutSpec::TopologyRead, Some(Hit::TopologyRead { node: "mesh1.admin.4".into(), authority: "mesh1.admin.1".into() }))),
            ("b".to_string(), row(CutSpec::ClaimedAttempt { attempt: None }, Some(Hit::Claimed { build_id: "b".into(), attempt: 1, executor: "mesh1.admin.1".into() }))),
            ("c".to_string(), row(CutSpec::CatchUp, None)),
        ]
        .into();
        let all = [
            FaultReply::Armed { node: "mesh1.admin.1".into(), id: "c".into(), spec: CutSpec::Receipt { step: "StartNode".into(), operation: None, node: Some("mesh1.rpc.1".into()) } },
            FaultReply::Released { node: "mesh1.admin.1".into(), id: "c".into(), was_held: true, held_ms: Some(3) },
            FaultReply::State(FaultState { node: "mesh1.admin.1".into(), cuts, receipts_seen: 4 }),
            FaultReply::Refused { reason: "r".into() },
            FaultReply::PeerUnresolved { reason: "r".into() },
            FaultReply::NotReady { reason: "r".into() },
            FaultReply::Busy { reason: "r".into() },
            FaultReply::Draining { reason: "r".into() },
            FaultReply::Malformed { kind: MalformedKind::Corrupt },
            FaultReply::Unauthorized { reason: "r".into() },
        ];
        assert_eq!(all.len() as u32, FaultDoor::REPLY_VARIANTS);
        for r in all {
            let bytes = FaultDoor::encode_reply(&r).unwrap_or_else(|e| panic!("{r:?} does not encode: {e:?}"));
            assert_eq!(FaultDoor::decode_reply(&bytes).unwrap_or_else(|e| panic!("{r:?} does not decode: {e:?}")), r);
        }
    }
}
