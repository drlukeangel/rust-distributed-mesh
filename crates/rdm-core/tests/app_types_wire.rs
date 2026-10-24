//! The declared app-type list travels on JoinNode; a digest carries only its held-set hash.
//! These cells construct the appended request fields independently of JoinRequest's serializer.

use rdm_core::join::Join;
use rdm_core::protocol::NodeProtocol;
use rdm_core::wire::WireDigest;
use rdm_core::MeshDigest;
use serde_json::json;

fn held_hash() -> [u8; 32] {
    let sorted = ["rafka.entity.alpha", "rafka.entity.zeta"];
    *blake3::hash(&postcard::to_allocvec(&sorted).unwrap()).as_bytes()
}

fn digest() -> MeshDigest {
    serde_json::from_value(json!({
        "fabric_id": "1123456789ab",
        "node": {
            "node_id": "0123456789ab", "name": "mesh1.rpc.1",
            "endpoint_id": "endpoint", "transport_addr": "127.0.0.1:12345",
            "incarnation": "yrz49j303yh4", "supersedes": null, "runtime": null
        },
        "status": "pending", "node_state": "joining", "admin_api_base": null,
        "digest_seq": 1, "emitted_at_rafka_ms": 1, "data_dir": null,
        "mesh_id": null, "in_flight": null, "config": {}, "load": null,
        "gossip": null, "held_hash": held_hash()
    })).unwrap()
}

#[test]
fn a_join_carries_the_births_declared_types_after_its_digest() {
    let types = vec!["rafka.entity.alpha".to_string(), "rafka.entity.zeta".to_string()];
    // JoinNode's existing position is 0. Encoding the tuple writes exactly that position,
    // its digest, then the appended list and the listeners the node bound (Q151) after it, without
    // using the implementation's request shape.
    let listeners: Vec<(String, std::net::SocketAddr)> = vec![("kafka".into(), "127.0.0.1:9092".parse().unwrap())];
    let bytes = postcard::to_allocvec(&(0_u8, WireDigest::from(&digest()), &types, &listeners)).unwrap();
    let request = Join::decode_request(&bytes).expect("the declared types are part of JoinNode, not trailing bytes");
    let value = serde_json::to_value(&request).unwrap();
    assert_eq!(value["JoinNode"]["types"], json!(types), "the receiver retains the birth's declaration");
    assert_eq!(value["JoinNode"]["listeners"], json!(listeners), "and the listeners it bound, after its types");
    assert_eq!(Join::encode_request(&request).unwrap(), bytes, "the type declaration survives receipt and replay");
}

#[test]
fn a_digest_carries_the_held_set_hash_and_never_its_type_lists() {
    let wire = WireDigest::from(&digest());
    let bytes = postcard::to_allocvec(&wire).unwrap();
    let decoded: WireDigest = postcard::from_bytes(&bytes).unwrap();
    let value = serde_json::to_value(&decoded).unwrap();
    assert_eq!(value["held_hash"], json!(held_hash()), "the exact 32-byte held-set hash survives the wire");
    assert!(value.get("types").is_none() && value.get("held").is_none(), "type lists are read with node.get and never ride a digest");
    let projected = serde_json::to_value(MeshDigest::from(decoded)).unwrap();
    assert_eq!(projected["held_hash"], json!(held_hash()), "projection installation preserves the source's hash");
}
