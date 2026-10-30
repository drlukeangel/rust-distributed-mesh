//! Independent postcard reference writer for gossip byte goldens. Expected context strings and
//! data bytes are assembled from the ruled grammar and literal data goldens, without the codec.
use rdm_core::{FabricId, NodeId, IncarnationId};
use rdm_transport::{envelope::Birth, topology::Frame};

pub fn author() -> Birth {
    Birth { fabric: FabricId::parse("04raj09p3zp7").unwrap(), node: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId::parse("yrz49j303yh4").unwrap() }
}
pub fn authored(f: &Frame) -> Frame {
    let author = match f.data() {
        Frame::Digest { digest } => Birth { fabric: digest.fabric_id.clone(), node: digest.node.node_id.clone(), incarnation: digest.node.incarnation.clone() },
        Frame::NodeDeleting { publisher, .. } | Frame::NodeRestarting { publisher, .. } | Frame::NodeStopped { publisher, .. } => Birth::publisher(&author().fabric, publisher),
        Frame::FabricStatus { fabric, .. } => Birth { fabric: fabric.clone(), ..author() },
        _ => author(),
    };
    f.clone().authored(&author, 2, 1)
}
pub fn data(f: &Frame) -> Vec<u8> {
    let (_, body): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&authored(f).encode()).unwrap(); body
}
fn number(mut n: u64, out: &mut Vec<u8>) {
    loop { let byte = (n & 127) as u8; n >>= 7; out.push(byte | if n != 0 { 128 } else { 0 }); if n == 0 { break; } }
}
fn text(s: &str, out: &mut Vec<u8>) { number(s.len() as u64, out); out.extend(s.as_bytes()); }
fn esc(s: impl ToString) -> String {
    s.to_string().bytes().map(|b| if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') { (b as char).to_string() } else { format!("%{b:02X}") }).collect()
}
fn joined(parts: Vec<String>) -> String { parts.into_iter().map(esc).collect::<Vec<_>>().join("/") }
fn stamp(ms: u64) -> String {
    assert!(ms < 86_400_000, "golden time is in the first UTC day");
    format!("1970-01-01T{:02}:{:02}:{:02}.{:03}Z", ms / 3_600_000, ms / 60_000 % 60, ms / 1000 % 60, ms % 1000)
}

/// A complete golden, including the metadata count, all literal names and both vector prefixes.
/// The literal data golden includes its historical enum byte solely to select the reference row (the
/// rows keep the historical page order, the removed types included); that byte is removed and never
/// appears in the expected wire message.
pub fn golden(f: &Frame, old: &[u8]) -> Vec<u8> { golden_with_trace(f, old, None, None) }

/// The optional standard pairs, supplied as literal expected W3C strings.
pub fn golden_with_trace(f: &Frame, old: &[u8], parent: Option<&str>, state: Option<&str>) -> Vec<u8> {
    let kinds = ["rdm.mesh.digest", "rdm.mesh.members", "rdm.mesh.members.delta", "rdm.node.deleting", "rdm.node.deleted", "rdm.node.restarting", "rdm.mesh.status", "rdm.fabric.status", "rdm.reserved.8", "rdm.mesh.concern", "rdm.node.connections.deleting", "rdm.node.leaving", "rdm.node.left", "rdm.node.connections.deleted", "rdm.fabric.primary.new", "rdm.fabric.state.committing", "rdm.fabric.state.committed", "rdm.fabric.traffic.opening", "rdm.fabric.traffic.opened", "rdm.node.stopped", "rdm.node.primary.updated", "rdm.node.primary.updating"];
    let kind = kinds[old[0] as usize];
    let a = author(); let mut source = format!("rdm://{}/{}/{}", a.fabric, a.node, a.incarnation);
    let mut seq = None; let mut ms = 1;
    let (subject, identity) = match f.data() {
        Frame::Digest { digest: d } => {
            source = format!("rdm://{}/{}/{}", d.fabric_id, d.node.node_id, d.node.incarnation); ms = d.emitted_at_rafka_ms; seq = Some(format!("{:020}", d.digest_seq));
            (format!("birth/{}/{}/{}", d.fabric_id, d.node.node_id, d.node.incarnation), vec![d.fabric_id.to_string(), d.node.node_id.to_string(), d.node.incarnation.to_string(), d.digest_seq.to_string()])
        }
        Frame::MeshTopology { mesh, publisher: p, topology_version: v, published_at_rafka_ms: t, snapshot_id: s, chunk_index: c, .. } => {
            source = format!("rdm://{}/{}/{}", a.fabric, p.node_id, p.incarnation); ms = *t; seq = Some(format!("{v:020}"));
            (format!("mesh/{}/publisher/{}/{}/{}", esc(mesh), a.fabric, p.node_id, p.incarnation), vec![mesh.clone(), a.fabric.to_string(), p.node_id.to_string(), p.incarnation.to_string(), v.to_string(), a.fabric.to_string(), a.node.to_string(), a.incarnation.to_string(), s.to_string(), c.to_string()])
        }
        Frame::MeshTopologyDelta { mesh, source_publisher: p, base_version: b, topology_version: v, published_at_rafka_ms: t, .. } => {
            source = format!("rdm://{}/{}/{}", a.fabric, p.node_id, p.incarnation); ms = *t; seq = Some(format!("{v:020}"));
            (format!("mesh/{}/publisher/{}/{}/{}", esc(mesh), a.fabric, p.node_id, p.incarnation), vec![mesh.clone(), a.fabric.to_string(), p.node_id.to_string(), p.incarnation.to_string(), b.to_string(), v.to_string()])
        }
        Frame::MeshStatus { mesh, changed_at_rafka_ms: t, .. } => {
            ms = *t; seq = Some(format!("00000000000000000002.{t:020}")); (format!("mesh/{}/status", esc(mesh)), vec![mesh.clone(), "2".into(), t.to_string()])
        }
        Frame::FabricStatus { fabric, changed_at_rafka_ms: t, .. } => {
            source = format!("rdm://{}/{}/{}", fabric, a.node, a.incarnation);
            ms = *t; seq = Some(format!("00000000000000000002.{t:020}")); (format!("fabric/{fabric}/status"), vec![fabric.to_string(), "2".into(), t.to_string()])
        }
        Frame::NodeDeleting { op: o, .. } | Frame::NodeDeleted { op: o, .. } | Frame::NodeRestarting { op: o, .. } | Frame::NodeStopped { op: o, .. } | Frame::NodeConnectionsDeleting { op: o, .. } | Frame::NodeConnectionsDeleted { op: o, .. } | Frame::NodeLeaving { op: o, .. } | Frame::NodeLeft { op: o, .. } => {
            ms = o.event_at_rafka_ms;
            if let Frame::NodeDeleting { publisher, .. } | Frame::NodeRestarting { publisher, .. } | Frame::NodeStopped { publisher, .. } = f.data() { source = format!("rdm://{}/{}/{}", a.fabric, publisher.node_id, publisher.incarnation); }
            (format!("operation/{}/{}/{}/birth/{}/{}", esc(&o.build_id), o.attempt, esc(&o.operation), o.node_id, o.incarnation), vec![o.build_id.clone(), o.attempt.to_string(), o.operation.clone(), o.node_id.to_string(), o.incarnation.to_string()])
        }
        Frame::Concern { node_id, incarnation, observer, .. } => (format!("birth/{}/{node_id}/{incarnation}", a.fabric), vec![node_id.to_string(), incarnation.to_string(), observer.clone()]),
        Frame::NewFabricPrimary { primary: h, .. } => (format!("fabric/{}/primary", a.fabric), vec![h.node_id.to_string(), h.incarnation.to_string()]),
        _ => unreachable!(),
    };
    let mut attrs = vec![("specversion", "1.0".into()), ("id", format!("{kind}/{}", joined(identity))), ("source", source), ("type", kind.into()), ("subject", subject)];
    if let Some(s) = seq { attrs.push(("sequence", s)); }
    attrs.push(("time", stamp(ms))); attrs.push(("datacontenttype", "application/octet-stream".into()));
    if let Some(p) = parent { attrs.push(("traceparent", p.into())); }
    if let Some(s) = state { attrs.push(("tracestate", s.into())); }
    let mut body = old[1..].to_vec();
    if let Frame::MeshTopology { mesh, publisher, topology_version, published_at_rafka_ms, snapshot_id, .. } = f.data() {
        let mut prefix = Vec::new(); text(mesh, &mut prefix); text(&publisher.node, &mut prefix); text(publisher.incarnation.as_str(), &mut prefix); text(publisher.node_id.as_str(), &mut prefix);
        if let Frame::MeshTopology { forwarded_by, .. } = f.data() { match forwarded_by { None => prefix.push(0), Some(by) => { prefix.push(1); text(by, &mut prefix); } } }
        number(*topology_version, &mut prefix); number(*published_at_rafka_ms, &mut prefix);
        assert!(body.starts_with(&prefix)); let start = prefix.len(); number(*snapshot_id, &mut prefix); let end = prefix.len();
        let mut expanded = body[..start].to_vec(); text(a.fabric.as_str(), &mut expanded); text(a.node.as_str(), &mut expanded); text(a.incarnation.as_str(), &mut expanded); expanded.extend(&body[start..end]); expanded.extend(&body[end..]); body = expanded;
    }
    let mut out = Vec::new(); number(attrs.len() as u64, &mut out); for (name, value) in attrs { text(name, &mut out); text(&value, &mut out); } number(body.len() as u64, &mut out); out.extend(body); out
}

/// Active kinds round trip; reserved types are refused before decoding their literal data.
pub fn assert_roundtrip(f: &Frame, golden: &[u8]) {
    match rdm_transport::envelope::decode(golden) {
        Ok(back) => { assert_eq!(back.encode(), golden); match (back.data(), f.data()) {
            (Frame::Digest { digest: a }, Frame::Digest { digest: b }) => assert_eq!(rdm_core::wire::WireDigest::from(a), rdm_core::wire::WireDigest::from(b)),
            _ => assert_eq!(format!("{:?}", back.data()), format!("{:?}", f.data())),
        } }
        Err(rdm_transport::envelope::DecodeError::Reserved(_, _)) => assert!(matches!(f.data(), Frame::NodeRestarting { .. } | Frame::Concern { .. } | Frame::NodeConnectionsDeleting { .. } | Frame::NodeConnectionsDeleted { .. } | Frame::NodeLeaving { .. } | Frame::NodeLeft { .. } | Frame::NewFabricPrimary { .. })),
        other => panic!("{other:?}"),
    }
}

pub trait WireGolden {
    fn golden_encode(&self) -> Vec<u8>;
    fn type_position(&self) -> u8;
}
impl WireGolden for Frame {
    fn golden_encode(&self) -> Vec<u8> { authored(self).encode() }
    fn type_position(&self) -> u8 {
        let (attrs, _): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&self.golden_encode()).unwrap();
        let kind = &attrs.iter().find(|(name, _)| name == "type").unwrap().1;
        rdm_transport::envelope::TYPES.iter().position(|k| k == kind).unwrap() as u8
    }
}
