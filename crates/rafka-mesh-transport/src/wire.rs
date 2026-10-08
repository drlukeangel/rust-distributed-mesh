//! The one codec every gossip frame goes through: postcard, positional, no field names, no
//! self-description. A membership [`Frame`](crate::membership::Frame) and a Build-topic message
//! (`rafka-node-admin-core` `fabric_builds`) are encoded here and nowhere else, so what a
//! sender measured to fit one gossip message ([`crate::chunking`]) is exactly what travels.
//!
//! Postcard decodes by position. A type that reaches this codec therefore carries no
//! `skip_serializing_if`, no internally tagged enum and no `serde_json::Value`: those shift
//! fields or need a self-describing format. A type that is JSON elsewhere (REST, a journal, a
//! file) keeps its JSON shape and is mirrored by a wire type beside the codec's caller.

use serde::{de::DeserializeOwned, Serialize};
use std::fmt;

/// Why a value did not encode, or a frame did not decode. Names what was being coded and why it
/// failed; a decode failure also names the bytes it was given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError(String);

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for WireError {}

impl WireError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self(reason.into())
    }
}

/// `value` as one postcard frame.
pub fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, WireError> {
    postcard::to_allocvec(value).map_err(|e| WireError(format!("postcard encode: {e}")))
}

/// One frame of type `T` from `bytes`. A frame that decodes with bytes left over is not this
/// type: it is refused by name, never read as its prefix.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, WireError> {
    let (value, rest) = postcard::take_from_bytes::<T>(bytes).map_err(|e| WireError(format!("postcard decode of {} bytes as {}: {e}", bytes.len(), std::any::type_name::<T>())))?;
    if rest.is_empty() {
        Ok(value)
    } else {
        Err(WireError(format!("postcard decode of {} bytes as {}: {} bytes left after the frame", bytes.len(), std::any::type_name::<T>(), rest.len())))
    }
}

// ---- the wire shape of a membership digest -------------------------------------------------
//
// `MeshDigest` is JSON on the entry pull and in tests; its `skip_serializing_if` fields and its
// runtime locator's internal tag cannot be read positionally. `WireDigest` carries the same
// facts with every field present and the locator externally tagged.

use rafka_mesh_entity::runtime::{RuntimeFact, RuntimeLocator, RuntimeProvider};
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, PathName};
use serde::{Deserialize, Deserializer, Serializer};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize)]
enum WireLocator {
    Process { pid: u32, start: u64 },
    Container { id: String },
}

#[derive(Serialize, Deserialize)]
struct WireRuntime {
    deployment_id: String,
    provider: RuntimeProvider,
    control_domain: String,
    locator: WireLocator,
}

#[derive(Serialize, Deserialize)]
struct WireNode {
    node_id: NodeId,
    name: PathName,
    endpoint_id: EndpointId,
    transport_addr: std::net::SocketAddr,
    incarnation: IncarnationId,
    supersedes: Option<IncarnationId>,
    runtime: Option<WireRuntime>,
}

/// A [`MeshDigest`] as it travels in a gossip frame.
#[derive(Serialize, Deserialize)]
pub struct WireDigest {
    fabric_id: FabricId,
    node: WireNode,
    status: MemberStatus,
    admin_api_base: Option<String>,
    digest_seq: u64,
    emitted_at_rafka_ms: u64,
    data_dir: Option<String>,
    mesh_id: Option<MeshId>,
    in_flight: Option<u64>,
    extra: BTreeMap<String, String>,
}

impl From<&MeshDigest> for WireDigest {
    fn from(d: &MeshDigest) -> Self {
        let n = &d.node;
        let runtime = n.runtime.as_ref().map(|r| WireRuntime {
            deployment_id: r.deployment_id.clone(),
            provider: r.provider,
            control_domain: r.control_domain.clone(),
            locator: match &r.locator {
                RuntimeLocator::Process { pid, start } => WireLocator::Process { pid: *pid, start: *start },
                RuntimeLocator::Container { id } => WireLocator::Container { id: id.clone() },
            },
        });
        Self {
            fabric_id: d.fabric_id.clone(),
            node: WireNode {
                node_id: n.node_id.clone(),
                name: n.name.clone(),
                endpoint_id: n.endpoint_id.clone(),
                transport_addr: n.transport_addr,
                incarnation: n.incarnation.clone(),
                supersedes: n.supersedes.clone(),
                runtime,
            },
            status: d.status,
            admin_api_base: d.admin_api_base.clone(),
            digest_seq: d.digest_seq,
            emitted_at_rafka_ms: d.emitted_at_rafka_ms,
            data_dir: d.data_dir.clone(),
            mesh_id: d.mesh_id.clone(),
            in_flight: d.in_flight,
            extra: d.extra.clone(),
        }
    }
}

impl From<WireDigest> for MeshDigest {
    fn from(w: WireDigest) -> Self {
        let n = w.node;
        let runtime = n.runtime.map(|r| RuntimeFact {
            deployment_id: r.deployment_id,
            provider: r.provider,
            control_domain: r.control_domain,
            locator: match r.locator {
                WireLocator::Process { pid, start } => RuntimeLocator::Process { pid, start },
                WireLocator::Container { id } => RuntimeLocator::Container { id },
            },
        });
        MeshDigest {
            fabric_id: w.fabric_id,
            node: MeshNode {
                node_id: n.node_id,
                name: n.name,
                endpoint_id: n.endpoint_id,
                transport_addr: n.transport_addr,
                incarnation: n.incarnation,
                supersedes: n.supersedes,
                runtime,
            },
            status: w.status,
            admin_api_base: w.admin_api_base,
            digest_seq: w.digest_seq,
            emitted_at_rafka_ms: w.emitted_at_rafka_ms,
            data_dir: w.data_dir,
            mesh_id: w.mesh_id,
            in_flight: w.in_flight,
            extra: w.extra,
        }
    }
}

/// `#[serde(with = "crate::wire::digest")]`: one digest in a frame.
pub mod digest {
    use super::*;
    pub fn serialize<S: Serializer>(d: &MeshDigest, s: S) -> Result<S::Ok, S::Error> {
        WireDigest::from(d).serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<MeshDigest, D::Error> {
        WireDigest::deserialize(d).map(Into::into)
    }
}

/// `#[serde(with = "crate::wire::digests")]`: a run of digests in a frame.
pub mod digests {
    use super::*;
    pub fn serialize<S: Serializer>(ds: &[MeshDigest], s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(ds.iter().map(WireDigest::from))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<MeshDigest>, D::Error> {
        Vec::<WireDigest>::deserialize(d).map(|v| v.into_iter().map(Into::into).collect())
    }
}
