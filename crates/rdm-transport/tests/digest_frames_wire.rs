//! Fixed bytes for a member's heartbeat and its mesh-primary's member projection on the membership
//! channels (gossip.md §3.1: the `Digest` carries "`node_state`" and "no status: a member's status
//! comes only from its mesh-primary"; `Members` and `MembersDelta` carry the mesh-primary's own-mesh
//! member projection, each entry its digest and its status). Postcard, positional, append-only. The
//! literals are derived from the postcard rules by hand. Never regenerate an expectation from the Rust
//! serializer to make a schema change pass.

use rdm_core::{IncarnationId, MemberStatus, MeshDigest, PublisherId};
use rdm_transport::membership::Frame;

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

/// mesh1.rpc.1's digest as JSON carries it (REST, files): no liveness field, so `node_state` takes its
/// default (`Joining`), and the status `status`.
fn digest(status: &str) -> MeshDigest {
    serde_json::from_str(&format!(
        r#"{{"fabric_id":"1123456789ab","node":{{"node_id":"0123456789ab","name":"mesh1.rpc.1","endpoint_id":"k","transport_addr":"127.0.0.1:80","incarnation":"yrz49j303yh4","supersedes":null}},"status":"{status}","admin_api_base":null,"digest_seq":7,"emitted_at_rafka_ms":9}}"#
    ))
    .unwrap()
}

/// The digest's wire shape: fabric id, node (node id, path.name, endpoint id, transport address
/// (variant V4, four address bytes, port), incarnation, supersedes, runtime), then `node_state` at
/// position 2 (`Joining` = 0), admin API base, digest_seq, emitted_at, data dir, mesh id, in flight,
/// config, load, gossip.
const WIRE_DIGEST: &str = "0c 313132333435363738396162 \
    0c 303132333435363738396162 0b 6d657368312e7270632e31 01 6b 00 7f000001 50 0c 79727a34396a333033796834 00 00 \
    00 \
    00 07 09 00 00 00 00 00 00";

/// CONTRACT (gossip.md §3.1 frame catalog): a heartbeat (`Digest`, variant 0) is the digest's wire
/// shape with the liveness `node_state` at position 2 and no status: a Draining digest and a
/// ready-for-traffic one encode alike. A member projection entry, in `Members` (variant 1) and
/// `MembersDelta` (variant 2), is the digest's wire shape then its status (`ReadyForTraffic` = 1), and
/// decodes to that status. What must NOT happen: a status on the heartbeat, a projection entry without
/// its status, or a field moved.
#[test]
fn a_heartbeat_carries_no_status_and_a_projection_entry_carries_its_digest_then_its_status() {
    let heartbeat = format!("00 {WIRE_DIGEST}");
    for status in ["draining", "ready-for-traffic"] {
        assert_eq!(Frame::Digest { digest: digest(status) }.encode(), bytes(&heartbeat), "a {status} heartbeat");
    }

    let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("p")).unwrap(), node_id: rdm_core::NodeId::parse("0123456789ab").unwrap() };
    let members = Frame::Members {
        mesh: "mesh1".into(),
        publisher: publisher.clone(),
        forwarded_by: None,
        topology_version: 3,
        published_at_rafka_ms: 5,
        snapshot_id: 9,
        chunk_index: 0,
        chunk_count: 1,
        digests: vec![digest("ready-for-traffic")],
        in_flight: vec![],
        departed: vec![],
    };
    let members_hex = format!("01 05 6d65736831 0d 6d657368312e61646d696e2e31 0c 79727a64396a333034326266 0c 303132333435363738396162 00 03 05 09 00 01 01 {WIRE_DIGEST} 01 00 00");
    assert_eq!(members.encode(), bytes(&members_hex), "Members: one projection entry, digest then status");
    let Frame::Members { digests, .. } = Frame::decode(&bytes(&members_hex)).expect("the Members frame decodes") else { panic!("not Members") };
    assert_eq!(digests.iter().map(|d| d.status).collect::<Vec<_>>(), vec![MemberStatus::ReadyForTraffic], "the entry's status is the projection's");

    let delta = Frame::MembersDelta {
        mesh: "mesh1".into(),
        source_publisher: publisher,
        base_version: 2,
        topology_version: 3,
        published_at_rafka_ms: 5,
        changed: vec![digest("ready-for-traffic")],
        removed: vec![],
        in_flight: vec![],
        departed: vec![],
    };
    let delta_hex = format!("02 05 6d65736831 0d 6d657368312e61646d696e2e31 0c 79727a64396a333034326266 0c 303132333435363738396162 02 03 05 01 {WIRE_DIGEST} 01 00 00 00");
    assert_eq!(delta.encode(), bytes(&delta_hex), "MembersDelta: one changed projection entry, digest then status");
    let Frame::MembersDelta { changed, .. } = Frame::decode(&bytes(&delta_hex)).expect("the MembersDelta frame decodes") else { panic!("not MembersDelta") };
    assert_eq!(changed.iter().map(|d| d.status).collect::<Vec<_>>(), vec![MemberStatus::ReadyForTraffic], "the entry's status is the projection's");
}
