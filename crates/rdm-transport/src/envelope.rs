//! CloudEvents 1.0 binary-mode gossip binding. Data is selected only after context validation.

use crate::topology::Frame;
use crate::wire::{self, WireError};
use rdm_core::{FabricId, NodeId, IncarnationId, PublisherId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The original author's exact birth, independent of the transport sender.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Birth {
    /// The fabric identity.
    pub fabric: FabricId,
    /// The logical node identity.
    pub node: NodeId,
    /// The incarnation identity.
    pub incarnation: IncarnationId,
}

impl Birth {
    /// Bind an explicitly supplied publisher to its fabric.
    pub fn publisher(fabric: &FabricId, publisher: &PublisherId) -> Self {
        Self { fabric: fabric.clone(), node: publisher.node_id.clone(), incarnation: publisher.incarnation.clone() }
    }

    pub(crate) fn uri(&self) -> String { format!("rdm://{}/{}/{}", self.fabric, self.node, self.incarnation) }

    fn parse(uri: &str) -> Result<Self, WireError> {
        let parts: Vec<_> = uri.strip_prefix("rdm://").ok_or_else(|| invalid("source scheme"))?.split('/').collect();
        if parts.len() != 3 { return Err(invalid("source birth")); }
        Ok(Self {
            fabric: FabricId::parse(parts[0]).map_err(|_| invalid("source fabric"))?,
            node: NodeId::parse(parts[1]).map_err(|_| invalid("source node"))?,
            incarnation: IncarnationId::parse(parts[2]).map_err(|_| invalid("source incarnation"))?,
        })
    }
}

/// Immutable authored context retained by a resend or a carrier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Context {
    /// Event identity, unique within source.
    pub id: String,
    /// Original author's birth.
    pub source: Birth,
    /// Literal CloudEvents type.
    pub kind: String,
    /// Escaped subject of the event.
    pub subject: String,
    /// Lexically ordered version, absent on facts.
    pub sequence: Option<String>,
    /// Authored Rafka-time in milliseconds.
    pub time_ms: u64,
    /// Birth and existing counter of this publication's owner, for MeshTopology only.
    pub publication: Option<(Birth, u64)>,
    /// Original author’s W3C parent; preserved through replay and forwarding.
    pub traceparent: Option<String>,
    /// Original author’s vendor trace state, never a receive decision.
    pub tracestate: Option<String>,
}

/// The catalog's literal types, in historical page order.
pub const TYPES: [&str; 19] = [
    "rdm.mesh.digest", "rdm.mesh.members", "rdm.mesh.members.delta", "rdm.node.deleting",
    "rdm.node.deleted", "rdm.node.restarting", "rdm.mesh.status", "rdm.fabric.status",
    "rdm.mesh.concern", "rdm.node.connections.deleting", "rdm.node.leaving",
    "rdm.node.left", "rdm.node.connections.deleted", "rdm.fabric.primary.new",
    "rdm.fabric.state.committing", "rdm.fabric.state.committed", "rdm.fabric.traffic.opening",
    "rdm.fabric.traffic.opened", "rdm.node.stopped",
];

/// The twentieth RDM type, carried on an application channel only.
pub const CHUNK_TYPE: &str = "rdm.gossip.chunk";

/// A valid type refusal, before domain data decoding, or malformed context/data.
#[derive(Debug)]
pub enum DecodeError {
    /// Reserved historical type and one-based page position.
    Reserved(String, usize),
    /// A well-formed type outside the catalog.
    Unknown(String),
    /// Invalid context or domain data.
    Malformed(WireError),
}

impl From<WireError> for DecodeError { fn from(e: WireError) -> Self { Self::Malformed(e) } }

fn invalid(reason: &str) -> WireError { WireError::new(format!("CloudEvents: {reason}")) }
fn p(value: u64) -> String { format!("{value:020}") }
fn scalar(value: &str) -> Result<u64, WireError> {
    if value.len() != 20 || !value.bytes().all(|b| b.is_ascii_digit()) { return Err(invalid("sequence decimal width")); }
    value.parse().map_err(|_| invalid("sequence range"))
}
fn escaped(s: impl ToString) -> String {
    let mut out = String::new();
    for b in s.to_string().bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') { out.push(b as char); }
        else { out.push_str(&format!("%{b:02X}")); }
    }
    out
}
fn path(parts: &[String]) -> String { parts.iter().map(escaped).collect::<Vec<_>>().join("/") }
fn birth_parts(b: &Birth) -> Vec<String> { vec![b.fabric.to_string(), b.node.to_string(), b.incarnation.to_string()] }
fn operation(op: &rdm_core::LifecycleOp) -> Vec<String> {
    vec![op.build_id.clone(), op.attempt.to_string(), op.operation.clone(), op.node_id.to_string(), op.incarnation.to_string()]
}
pub(crate) fn date(ms: u64) -> Result<String, WireError> {
    let t = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms.try_into().map_err(|_| invalid("time range"))?).ok_or_else(|| invalid("time range"))?;
    Ok(t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}
fn date_ms(s: &str) -> Result<u64, WireError> {
    let t = chrono::DateTime::parse_from_rfc3339(s).map_err(|_| invalid("time RFC3339"))?;
    let ms: u64 = t.timestamp_millis().try_into().map_err(|_| invalid("time range"))?;
    if date(ms)? != s { return Err(invalid("time must be UTC milliseconds")); }
    Ok(ms)
}

impl Context {
    /// Declare context from the owned author, held committed revision and authored time.
    /// A forwarding publication supplies its own birth as owner while retaining source.
    pub fn authored(frame: &Frame, author: &Birth, primary_revision: u64, now_ms: u64) -> Self {
        let frame = frame.data();
        let index = frame.kind_index();
        let kind = TYPES[index].to_string();
        let mut source = author.clone();
        let mut sequence = None;
        let mut identity: Vec<String> = Vec::new();
        let mut publication = None;
        let mut time_ms = now_ms;
        let subject = match frame {
            Frame::Digest { digest } => {
                source = Birth { fabric: digest.fabric_id.clone(), node: digest.node.node_id.clone(), incarnation: digest.node.incarnation.clone() };
                sequence = Some(p(digest.digest_seq));
                identity = birth_parts(&source); identity.push(digest.digest_seq.to_string());
                time_ms = digest.emitted_at_rafka_ms;
                format!("birth/{}", path(&birth_parts(&source)))
            }
            Frame::MeshTopology { mesh, publisher, topology_version, published_at_rafka_ms, snapshot_id, chunk_index, .. } => {
                source = Birth::publisher(&author.fabric, publisher);
                sequence = Some(p(*topology_version)); time_ms = *published_at_rafka_ms;
                publication = Some((author.clone(), *snapshot_id));
                identity.push(mesh.clone()); identity.extend(birth_parts(&source)); identity.push(topology_version.to_string());
                identity.extend(birth_parts(author)); identity.push(snapshot_id.to_string()); identity.push(chunk_index.to_string());
                format!("mesh/{}/publisher/{}", escaped(mesh), path(&birth_parts(&source)))
            }
            Frame::MeshTopologyDelta { mesh, source_publisher, base_version, topology_version, published_at_rafka_ms, .. } => {
                source = Birth::publisher(&author.fabric, source_publisher);
                sequence = Some(p(*topology_version)); time_ms = *published_at_rafka_ms;
                identity.push(mesh.clone()); identity.extend(birth_parts(&source)); identity.push(base_version.to_string()); identity.push(topology_version.to_string());
                format!("mesh/{}/publisher/{}", escaped(mesh), path(&birth_parts(&source)))
            }
            Frame::MeshStatus { mesh, changed_at_rafka_ms, .. } => {
                time_ms = *changed_at_rafka_ms;
                sequence = Some(format!("{}.{}", p(primary_revision), p(time_ms)));
                identity = vec![mesh.clone(), primary_revision.to_string(), time_ms.to_string()];
                format!("mesh/{}/status", escaped(mesh))
            }
            Frame::FabricStatus { fabric, changed_at_rafka_ms, .. } => {
                time_ms = *changed_at_rafka_ms;
                sequence = Some(format!("{}.{}", p(primary_revision), p(time_ms)));
                identity = vec![fabric.to_string(), primary_revision.to_string(), time_ms.to_string()];
                format!("fabric/{fabric}/status")
            }
            Frame::Reserved15 { hook, .. } | Frame::Reserved16 { hook, .. } | Frame::Reserved17 { hook, .. } | Frame::Reserved18 { hook, .. } => {
                identity = vec![hook.fabric_id.to_string(), hook.build_id.clone(), hook.attempt.to_string(), hook.operation.clone(), hook.node_id.to_string(), hook.incarnation.to_string()];
                time_ms = hook.event_at_rafka_ms;
                format!("state/{}/{}/{}/{}/birth/{}/{}", hook.fabric_id, escaped(&hook.build_id), hook.attempt, escaped(&hook.operation), hook.node_id, hook.incarnation)
            }
            Frame::NodeDeleting { op, .. } | Frame::NodeDeleted { op, .. } | Frame::NodeRestarting { op, .. } | Frame::NodeStopped { op, .. }
            | Frame::NodeConnectionsDeleting { op, .. } | Frame::NodeConnectionsDeleted { op, .. } | Frame::NodeLeaving { op, .. } | Frame::NodeLeft { op, .. } => {
                identity = operation(op); time_ms = op.event_at_rafka_ms;
                format!("operation/{}/{}/{}/birth/{}/{}", escaped(&op.build_id), op.attempt, escaped(&op.operation), op.node_id, op.incarnation)
            }
            Frame::Concern { node_id, incarnation, observer, .. } => { identity = vec![node_id.to_string(), incarnation.to_string(), observer.clone()]; format!("birth/{}/{node_id}/{incarnation}", author.fabric) }
            Frame::NewFabricPrimary { primary, .. } => { identity = vec![primary.node_id.to_string(), primary.incarnation.to_string()]; format!("fabric/{}/primary", author.fabric) }
            Frame::Authored { .. } => unreachable!(),
        };
        let (trace, _) = sanitized_trace(crate::current_traceparent(), crate::current_tracestate());
        Self { id: format!("{kind}/{}", path(&identity)), source, kind, subject, sequence, time_ms, publication, traceparent: trace.traceparent, tracestate: trace.tracestate }
    }

    fn attributes(&self, dataschema: Option<&str>) -> Result<Vec<(String, String)>, WireError> {
        let mut a = vec![("specversion".into(), "1.0".into()), ("id".into(), self.id.clone()), ("source".into(), self.source.uri()), ("type".into(), self.kind.clone()), ("subject".into(), self.subject.clone())];
        if let Some(s) = &self.sequence { a.push(("sequence".into(), s.clone())); }
        a.push(("time".into(), date(self.time_ms)?));
        a.push(("datacontenttype".into(), "application/octet-stream".into()));
        if let Some(schema) = dataschema { a.push(("dataschema".into(), schema.into())); }
        if let Some(parent) = &self.traceparent { a.push(("traceparent".into(), parent.clone())); }
        if let Some(state) = &self.tracestate { a.push(("tracestate".into(), state.clone())); }
        Ok(a)
    }

    /// The primary record's revision in a status sequence.
    pub fn primary_revision(&self) -> Option<u64> { self.sequence.as_deref()?.split_once('.').and_then(|(r, _)| scalar(r).ok()) }
}

/// Encode one complete metadata-and-data message. The caller measures these exact bytes.
pub fn encode(context: &Context, frame: &Frame) -> Result<Vec<u8>, WireError> {
    encode_data(context, &body(context, frame)?, None)
}

/// The one binary binding for domain, application and chunk data.
pub(crate) fn encode_data(context: &Context, data: &[u8], dataschema: Option<&str>) -> Result<Vec<u8>, WireError> {
    wire::encode(&(context.attributes(dataschema)?, data))
}

fn body(context: &Context, frame: &Frame) -> Result<Vec<u8>, WireError> {
    let raw = wire::encode(frame.data())?;
    let mut data = raw[1..].to_vec();
    if matches!(frame.data(), Frame::MeshTopology { .. }) {
        // Existing fields retain their order; only the publication id gains its owner's birth.
        let (prefix, counter_at) = mesh_topology_prefix(&data)?;
        let (owner, counter) = context.publication.as_ref().ok_or_else(|| invalid("missing publication owner"))?;
        let mut out = prefix;
        out.extend(wire::encode(&(owner.fabric.clone(), owner.node.clone(), owner.incarnation.clone(), *counter))?);
        data = data[counter_at..].to_vec(); out.extend(data); return Ok(out);
    }
    Ok(data)
}

// Parse only the fixed MeshTopology prefix and existing counter to find its byte boundary. Domain
// entries remain opaque here; the selected MeshTopology schema reads them after type validation.
fn mesh_topology_prefix(data: &[u8]) -> Result<(Vec<u8>, usize), WireError> {
    let mut cursor = data;
    let _: String = wire::take(&mut cursor)?;
    let _: PublisherId = wire::take(&mut cursor)?;
    let _: Option<String> = wire::take(&mut cursor)?;
    let _: u64 = wire::take(&mut cursor)?;
    let _: u64 = wire::take(&mut cursor)?;
    let prefix = data[..data.len() - cursor.len()].to_vec();
    let _: u64 = wire::take(&mut cursor)?;
    Ok((prefix, data.len() - cursor.len()))
}

/// The receive path observes tracing even when the event type is reserved or unknown.
pub(crate) struct Received {
    pub trace: rdm_core::context::CallContext,
    pub dropped: Vec<rdm_core::context::Dropped>,
    pub frame: Result<Frame, DecodeError>,
}

fn sanitized_trace(parent: Option<String>, state: Option<String>) -> (rdm_core::context::CallContext, Vec<rdm_core::context::Dropped>) {
    use rdm_core::context::{CallContext, Dropped};
    let (mut trace, mut dropped) = CallContext { traceparent: parent, tracestate: state, ..Default::default() }.sanitized();
    if trace.tracestate.as_deref().is_some_and(|s| {
        trace.traceparent.is_none() || !s.bytes().all(|b| (0x20..=0x7e).contains(&b))
            || s.split(',').count() > 32
            || opentelemetry::trace::TraceState::from_key_value(s.split(',').map(|m| m.trim().split_once('=').expect("sanitized member"))).is_err()
            || { let mut keys = std::collections::BTreeSet::new(); s.split(',').any(|m| !keys.insert(m.trim().split_once('=').unwrap().0)) }
    }) {
        trace.tracestate = None;
        dropped.push(Dropped::Tracestate);
    }
    (trace, dropped)
}

/// Decode context, reject reserved/unknown types, then select and validate domain data.
pub fn decode(bytes: &[u8]) -> Result<Frame, DecodeError> { decode_received(bytes).frame }

pub(crate) fn decode_received(bytes: &[u8]) -> Received {
    let mut trace = rdm_core::context::CallContext::default();
    let mut dropped = Vec::new();
    let frame = decode_event(bytes, &mut trace, &mut dropped);
    Received { trace, dropped, frame }
}

/// Validated binary-mode context; dispatchers select data only after this parse.
pub(crate) struct Header {
    pub source: Birth,
    pub id: String,
    pub kind: String,
    pub subject: String,
    pub time_ms: u64,
    pub sequence: Option<String>,
    pub dataschema: Option<String>,
    pub data: Vec<u8>,
}

pub(crate) fn decode_header(bytes: &[u8], trace: &mut rdm_core::context::CallContext, dropped: &mut Vec<rdm_core::context::Dropped>, limit: usize) -> Result<Header, DecodeError> {
    if bytes.len() > limit { return Err(invalid("complete message exceeds MAX_FRAME").into()); }
    let (pairs, data): (Vec<(String, String)>, Vec<u8>) = wire::decode(bytes)?;
    // Trace values are sanitized separately: even an invalid String value cannot refuse data.
    let parent = pairs.iter().find(|(n, _)| n == "traceparent").map(|(_, v)| v.clone());
    let state = pairs.iter().find(|(n, _)| n == "tracestate").map(|(_, v)| v.clone());
    (*trace, *dropped) = sanitized_trace(parent, state);
    let mut a = BTreeMap::new();
    for (name, value) in pairs {
        if name.is_empty() || !name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()) { return Err(invalid("attribute name").into()); }
        if name != "traceparent" && name != "tracestate" && value.chars().any(|c| { let n = c as u32; n <= 0x1f || (0x7f..=0x9f).contains(&n) || (0xfdd0..=0xfdef).contains(&n) || n & 0xffff >= 0xfffe }) { return Err(invalid("attribute String value").into()); }
        if a.insert(name, value).is_some() { return Err(invalid("duplicate attribute").into()); }
    }
    let required = |name: &str| -> Result<&str, WireError> { a.get(name).filter(|v| !v.is_empty()).map(String::as_str).ok_or_else(|| invalid(&format!("missing {name}"))) };
    if required("specversion")? != "1.0" || required("datacontenttype")? != "application/octet-stream" { return Err(invalid("specversion or datacontenttype").into()); }
    let source = Birth::parse(required("source")?)?;
    let id = required("id")?.to_string();
    let kind = required("type")?.to_string();
    let subject = required("subject")?.to_string();
    let time_ms = date_ms(required("time")?)?;
    let sequence = a.get("sequence").cloned();
    if a.contains_key("sequencetype") { return Err(invalid("sequencetype must be unset in this binding").into()); }
    Ok(Header { source, id, kind, subject, time_ms, sequence, dataschema: a.get("dataschema").cloned(), data })
}

fn decode_event(bytes: &[u8], trace: &mut rdm_core::context::CallContext, dropped: &mut Vec<rdm_core::context::Dropped>) -> Result<Frame, DecodeError> {
    let Header { source, id, kind, subject, time_ms, sequence, mut data, .. } = decode_header(bytes, trace, dropped, crate::topology::MAX_FRAME)?;
    let index = TYPES.iter().position(|t| *t == kind);
    // Required context is checked before the named type outcome; no domain data is decoded.
    let Some(index) = index else { return Err(DecodeError::Unknown(kind)); };
    if index == 5 || (8..=17).contains(&index) {
        if sequence.is_some() { return Err(invalid("reserved type declares sequence").into()); }
        // Its historical page position (gossip.md "Reserved lifecycle frame positions": the reserved
        // types "retain their historical page positions"), which the types removed before them move not.
        // `rdm.fabric.state.committing` and `rdm.fabric.state.committed` are reserved since Q171,
        // `rdm.fabric.traffic.opening` and `rdm.fabric.traffic.opened` since Q174, and
        // `rdm.node.restarting` (position 7) since Q183.
        return Err(DecodeError::Reserved(kind, index + 2));
    }
    match index {
        0 | 1 | 2 => { scalar(sequence.as_deref().ok_or_else(|| invalid("missing sequence"))?)?; }
        6 | 7 => {
            let (r, t) = sequence.as_deref().and_then(|s| s.split_once('.')).ok_or_else(|| invalid("status sequence"))?;
            scalar(r)?; scalar(t)?;
        }
        _ if sequence.is_some() => return Err(invalid("fact declares sequence").into()),
        _ => (),
    }
    let mut publication = None;
    if index == 1 {
        let mut cursor = data.as_slice();
        let _: String = wire::take(&mut cursor)?;
        let _: PublisherId = wire::take(&mut cursor)?;
        let _: Option<String> = wire::take(&mut cursor)?;
        let _: u64 = wire::take(&mut cursor)?;
        let _: u64 = wire::take(&mut cursor)?;
        let start = data.len() - cursor.len();
        let (fabric, node, incarnation, counter): (FabricId, NodeId, IncarnationId, u64) = wire::take(&mut cursor)?;
        let mut old = data[..start].to_vec(); old.extend(wire::encode(&counter)?); old.extend(cursor);
        data = old; publication = Some((Birth { fabric, node, incarnation }, counter));
    }
    let mut tagged = vec![index as u8]; tagged.extend(data);
    let frame: Frame = wire::decode(&tagged)?;
    let context = Context { id, source, kind, subject, sequence, time_ms, publication, traceparent: trace.traceparent.clone(), tracestate: trace.tracestate.clone() };
    match &frame {
        Frame::NodeDeleting { publisher, .. } | Frame::NodeStopped { publisher, .. }
            if Birth::publisher(&context.source.fabric, publisher) != context.source => return Err(invalid("publisher and source disagree").into()),
        Frame::FabricStatus { fabric, .. } if fabric != &context.source.fabric => return Err(invalid("fabric and source disagree").into()),
        _ => (),
    }
    let owner = context.publication.as_ref().map(|(b, _)| b).unwrap_or(&context.source);
    let expected = Context::authored(&frame, owner, context.primary_revision().unwrap_or(0), time_ms);
    if context.id != expected.id || context.source != expected.source || context.subject != expected.subject || context.sequence != expected.sequence || context.time_ms != expected.time_ms {
        return Err(invalid("context and data disagree").into());
    }
    Ok(Frame::Authored { context, frame: Box::new(frame) })
}

/// Retained events carried by a directed join answer use the same envelope, preserving source
/// and status sequence through read installation without another context schema.
pub mod frames {
    use super::*;
    use serde::{Serializer, Deserializer, ser::Error as _, de::Error as _};
    /// Encode retained events, each with its original context.
    pub fn serialize<S: Serializer>(frames: &[Frame], serializer: S) -> Result<S::Ok, S::Error> {
        let events: Result<Vec<_>, _> = frames.iter().map(|f| {
            let c = f.context().ok_or_else(|| S::Error::custom("retained status has no authored context"))?;
            encode(c, f).map_err(S::Error::custom)
        }).collect();
        events?.serialize(serializer)
    }
    /// Read retained events using the sole envelope decoder.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Frame>, D::Error> {
        Vec::<Vec<u8>>::deserialize(deserializer)?.into_iter().map(|bytes| decode(&bytes).map_err(|e| D::Error::custom(format!("{e:?}")))).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn author() -> Birth {
        Birth { fabric: FabricId::parse("1123456789ab").unwrap(), node: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId::parse("yrz49j303yh4").unwrap() }
    }
    fn event() -> Frame {
        Frame::MeshStatus { mesh: "m".into(), status: "ready".into(), publisher: "m.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 1 }.authored(&author(), 2, 1)
    }
    fn parts() -> (Vec<(String, String)>, Vec<u8>) { wire::decode(&event().encode()).unwrap() }
    fn malformed(pairs: Vec<(String, String)>, body: Vec<u8>) {
        assert!(matches!(decode(&wire::encode(&(pairs, body)).unwrap()), Err(DecodeError::Malformed(_))));
    }
    #[test]
    fn context_is_order_independent_and_duplicate_names_are_refused() {
        let (mut attrs, data) = parts(); attrs.reverse();
        assert_eq!(decode(&wire::encode(&(attrs.clone(), data.clone())).unwrap()).unwrap().context(), event().context());
        attrs.push(attrs[0].clone()); malformed(attrs, data);
        let (mut attrs, data) = parts(); attrs.push(("traceparent".into(), "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".into()));
        assert!(decode(&wire::encode(&(attrs, data)).unwrap()).is_ok(), "the optional tracing attribute is accepted");
    }
    #[test]
    fn tracing_is_optional_sanitized_and_excluded_from_event_identity() {
        let (attrs, data) = parts();
        let parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        let baseline = event();
        for bad in ["", "not-a-parent", "00-00000000000000000000000000000000-0123456789abcdef-01", "00-0123456789abcdef0123456789abcdef-0000000000000000-01", "bad\nparent"] {
            let mut pairs = attrs.clone(); pairs.push(("traceparent".into(), bad.into())); pairs.push(("tracestate".into(), "rojo=123".into()));
            let received = decode_received(&wire::encode(&(pairs, data.clone())).unwrap());
            assert_eq!(received.dropped, vec![rdm_core::context::Dropped::Traceparent]);
            let frame = received.frame.unwrap(); assert_eq!(frame.context(), baseline.context());
        }
        for bad in ["", "no-equals", "rojo=a\n", "BadKey=a", "rojo=a,rojo=b", "orphan"] {
            let mut pairs = attrs.clone(); pairs.push(("traceparent".into(), parent.into())); pairs.push(("tracestate".into(), bad.into()));
            let received = decode_received(&wire::encode(&(pairs, data.clone())).unwrap());
            assert_eq!(received.dropped, vec![rdm_core::context::Dropped::Tracestate], "{bad:?}");
            assert_eq!(received.trace.traceparent.as_deref(), Some(parent));
            assert!(received.frame.unwrap().context().unwrap().tracestate.is_none());
        }
        let mut pairs = attrs; pairs.push(("traceparent".into(), parent.into())); pairs.push(("tracestate".into(), "rojo=123".into()));
        let frame = decode(&wire::encode(&(pairs, data)).unwrap()).unwrap();
        assert_eq!(frame.context().unwrap().id, baseline.context().unwrap().id);
        assert_eq!(frame.context().unwrap().traceparent.as_deref(), Some(parent));
        let forwarded = crate::topology::forward_of(frame.clone(), "mesh2.admin.1", "mesh2").unwrap();
        assert_eq!(forwarded.context(), frame.context(), "forwarder keeps the author's context");
        let retained = frames_roundtrip(&[forwarded]);
        assert_eq!(retained[0].context(), frame.context(), "directed replay keeps the author's context");
    }
    fn frames_roundtrip(events: &[Frame]) -> Vec<Frame> {
        #[derive(Serialize, Deserialize)]
        struct Retained { #[serde(with = "frames")] events: Vec<Frame> }
        let bytes = wire::encode(&Retained { events: events.to_vec() }).unwrap();
        wire::decode::<Retained>(&bytes).unwrap().events
    }

    #[test]
    fn reserved_and_unknown_types_are_refused_before_invalid_data_is_decoded() {
        for (index, kind) in ["rdm.mesh.concern", "rdm.node.connections.deleting", "rdm.node.leaving", "rdm.node.left", "rdm.node.connections.deleted", "rdm.fabric.primary.new"].into_iter().enumerate() {
            let (mut attrs, _) = parts(); attrs.retain(|(n, _)| n != "sequence");
            attrs.iter_mut().find(|(n, _)| n == "type").unwrap().1 = kind.into();
            assert!(matches!(decode(&wire::encode(&(attrs, vec![255u8, 255])).unwrap()), Err(DecodeError::Reserved(k, p)) if k == kind && p == index + 10));
        }
        for (kind, position) in [("rdm.fabric.state.committing", 16), ("rdm.fabric.state.committed", 17), ("rdm.fabric.traffic.opening", 18), ("rdm.fabric.traffic.opened", 19)] {
            let (mut attrs, _) = parts(); attrs.retain(|(n, _)| n != "sequence");
            attrs.iter_mut().find(|(n, _)| n == "type").unwrap().1 = kind.into();
            assert!(matches!(decode(&wire::encode(&(attrs, vec![255u8, 255])).unwrap()), Err(DecodeError::Reserved(k, p)) if k == kind && p == position), "{kind} is reserved (Q171, Q174)");
        }
        let (mut attrs, _) = parts(); attrs.iter_mut().find(|(n, _)| n == "type").unwrap().1 = "rdm.unknown".into();
        assert!(matches!(decode(&wire::encode(&(attrs, vec![255u8])).unwrap()), Err(DecodeError::Unknown(k)) if k == "rdm.unknown"));
    }
    /// CONTRACT (Q183, Luke 2026-10-26: node.restart and its NodeRestarting notice are removed):
    /// `rdm.node.restarting` keeps its historical page position, 7, and is refused before its data is
    /// decoded, as the other reserved types are. Must NOT happen: a NodeRestarting frame decoded.
    #[test]
    fn the_node_restarting_type_is_refused_before_its_data_is_decoded() {
        let (mut attrs, _) = parts(); attrs.retain(|(n, _)| n != "sequence");
        attrs.iter_mut().find(|(n, _)| n == "type").unwrap().1 = "rdm.node.restarting".into();
        let refused = decode(&wire::encode(&(attrs, vec![255u8, 255])).unwrap());
        assert!(matches!(&refused, Err(DecodeError::Reserved(k, 7)) if k == "rdm.node.restarting"), "rdm.node.restarting is reserved at position 7: {refused:?}");
    }
    #[test]
    fn malformed_required_context_never_reaches_domain_data() {
        for name in ["specversion", "id", "source", "type", "subject", "time", "datacontenttype", "sequence"] {
            let (mut attrs, data) = parts(); attrs.retain(|(n, _)| n != name); malformed(attrs, data);
            let (mut attrs, data) = parts(); attrs.iter_mut().find(|(n, _)| n == name).unwrap().1.clear(); malformed(attrs, data);
        }
        for (name, value) in [
            ("specversion", "0.3"), ("type", "invalid\n"), ("type", "invalid\u{fdd0}"), ("source", "rdm://m.admin.1"),
            ("source", "rdm://1123456789ab/0123456789ab/YRZ49J303YH4"),
            ("source", "rdm://1123456789ab/0123456789ab/yrz49j303yh4/extra"),
            ("sequence", "2.1"), ("sequence", "00000000000000000002.18446744073709551616"),
            ("sequence", "00000000000000000002.00000000000000000002"),
            ("time", "1970-01-01T00:00:00Z"), ("time", "1970-01-01T00:00:00.001+00:00"),
            ("time", "not-a-time"), ("datacontenttype", "application/json"),
            ("subject", "mesh/another/status"), ("id", "different"),
        ] {
            let (mut attrs, data) = parts(); attrs.iter_mut().find(|(n, _)| n == name).unwrap().1 = value.into(); malformed(attrs, data);
        }
        let (mut attrs, data) = parts(); attrs.push(("sequencetype".into(), "Integer".into())); malformed(attrs, data);
        let mut bytes = event().encode(); bytes.push(0); assert!(matches!(decode(&bytes), Err(DecodeError::Malformed(_))));
        assert!(matches!(decode(&[6, 1, 0xff]), Err(DecodeError::Malformed(_))));
        assert!(matches!(decode(&wire::encode(event().data()).unwrap()), Err(DecodeError::Malformed(_))), "the old domain enum has no gossip decoder");
    }
    #[test]
    fn operation_sequence_is_absent_and_source_survives_forwarding() {
        let op = rdm_core::LifecycleOp { build_id: "b".into(), attempt: 1, operation: "o".into(), node_id: author().node, incarnation: author().incarnation, name: "m.rpc.1".parse().unwrap(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        let event = Frame::NodeDeleted { op, forwarded_by: None }.authored(&author(), 0, 1);
        let forwarded = crate::topology::forward_of(event.clone(), "other.admin.1", "other").unwrap();
        assert_eq!(event.context(), forwarded.context());
        assert!(forwarded.context().unwrap().sequence.is_none());
        let (mut attrs, data): (Vec<(String, String)>, Vec<u8>) = wire::decode(&forwarded.encode()).unwrap();
        attrs.push(("sequence".into(), p(1))); malformed(attrs, data);
    }
}
