//! The `JoinNode` answer postcard frame codec (`rdm-node`).

use serde::{Deserialize, Serialize};
use crate::model::{FabricId, FabricIdentity, PathName};
use rdm_core::BuildId;

#[derive(Serialize, Deserialize)]
struct WireFabricIdentity {
    fabric_id: FabricId,
    name: String,
}

// ---- the join answer -----------------------------------------------------------------------

/// What a join's `Joined` reply carries (`JoinNode`, op `0x1D`): the admin's control snapshot and
/// the statuses it holds, as one postcard frame. The topology is not in it: the joiner reads it
/// with `GetTopology` (op `0x1E`, [`crate::topology_read`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinAnswer {
    /// The answering admin's path name.
    pub served_by: String,
    /// The control facts the answer carries.
    pub control: JoinControl,
    /// The statuses the admin holds (original publisher and instant kept).
    pub statuses: Vec<rdm_transport::membership::Frame>,
}

/// The fabric control state an admin hands a joiner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinControl {
    /// The deployment provider the fabric's policy names.
    pub provider: crate::model::ProviderKind,
    /// The Fabric identity the admin holds.
    pub fabric: Option<FabricIdentity>,
    /// The answering admin's current Build and its attempt: the joiner's copy of that Build's facts
    /// must reach the attempt before Ready.
    pub build: Option<BuildFloor>,
    /// The answering admin's rafka-time now, in milliseconds: the joiner adopts it as it stands.
    /// An admin that holds no rafka-time answers `NotReady` naming why and builds no answer.
    pub rafka_time_ms: u64,
    /// The member cert the accepting authority's signer issued for exactly this birth, as opaque
    /// bytes RDM never parses (`fabric-certs.md`). Empty when the authority is configured with no
    /// certs.
    pub member_cert: Vec<u8>,
    /// The row's name for this birth, the name it was admitted under: the node installs it before
    /// it publishes anything or declares readiness (R-JN1, node-start.md step 2). Filled by the join
    /// door for the accepted birth; a node refuses an answer without it.
    pub name: Option<PathName>,
    /// The config the birth's row holds, handed beside its name; `None` when no row was ever
    /// written at the name. Appended last: it is the control frame's final field.
    pub config: Option<std::collections::BTreeMap<String, String>>,
}

/// The answering admin's current Build, and the attempt a joiner's copy of it must reach before Ready.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildFloor {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt.
    pub attempt: u32,
}

#[derive(Serialize, Deserialize)]
struct WireJoinAnswer {
    served_by: String,
    control: WireControl,
    statuses: Vec<rdm_transport::membership::Frame>,
}

#[derive(Serialize, Deserialize)]
struct WireControl {
    provider: crate::model::ProviderKind,
    fabric: Option<WireFabricIdentity>,
    build: Option<WireBuildFloor>,
    rafka_time_ms: u64,
    member_cert: Vec<u8>,
    name: Option<PathName>,
    config: Option<std::collections::BTreeMap<String, String>>,
}

#[derive(Serialize, Deserialize)]
struct WireBuildFloor {
    build_id: BuildId,
    attempt: u32,
}

/// `answer` as one postcard frame.
pub fn answer_to_wire(answer: &JoinAnswer) -> Result<Vec<u8>, String> {
    let c = &answer.control;
    let wire = WireJoinAnswer {
        served_by: answer.served_by.clone(),
        control: WireControl {
            provider: c.provider,
            fabric: c.fabric.as_ref().map(|r| WireFabricIdentity { fabric_id: r.fabric_id.clone(), name: r.name.clone() }),
            build: c.build.as_ref().map(|b| WireBuildFloor { build_id: b.build_id.clone(), attempt: b.attempt }),
            rafka_time_ms: c.rafka_time_ms,
            member_cert: c.member_cert.clone(),
            name: c.name.clone(),
            config: c.config.clone(),
        },
        statuses: answer.statuses.clone(),
    };
    rdm_transport::wire::encode(&wire).map_err(|e| e.to_string())
}

/// The join answer one postcard frame carries.
pub fn answer_from_wire(bytes: &[u8]) -> Result<JoinAnswer, String> {
    let w: WireJoinAnswer = rdm_transport::wire::decode(bytes).map_err(|e| e.to_string())?;
    Ok(JoinAnswer {
        served_by: w.served_by,
        control: JoinControl {
            provider: w.control.provider,
            fabric: w.control.fabric.map(|r| FabricIdentity { fabric_id: r.fabric_id, name: r.name }),
            build: w.control.build.map(|b| BuildFloor { build_id: b.build_id, attempt: b.attempt }),
            rafka_time_ms: w.control.rafka_time_ms,
            member_cert: w.control.member_cert,
            name: w.control.name,
            config: w.control.config,
        },
        statuses: w.statuses,
    })
}

