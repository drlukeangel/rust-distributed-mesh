//! `node.roots.update` (fabric-ca-renew.md "node.roots.update, the primitive"): one exact birth
//! installs the fabric's trusted root set, the receipt `RootsUpdated`.

use rdm_core::certs::VerifyingKey;
use rdm_core::status::{RootsPhase, StatusReply};
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::connections::ConnectionsRegistry;

/// Install `roots` (each an ed25519 public key as hex) as the root set `registry`'s admissions are
/// judged on, for the birth `node` (`node_id`, `incarnation`) whose door the request reached: one or
/// two roots, never none, each refused by name when it is not a key (`RejectedCert`). A set that drops
/// a root this birth's own member cert still chains under is refused by name, `root-still-chained`
/// (`RejectedRootStillChained`), and nothing is installed: what keeps a rotation's retirement after
/// every cert chains to the new root. `secret` says whether the request carried the root secret (a
/// node-admin's), named on the span, never its value. One span,
/// `rdm.node.root.update.via-installed`, or `rdm.node.root.reject.via-update` on a refusal.
pub fn install_roots(registry: &ConnectionsRegistry, node: &str, node_id: &NodeId, incarnation: &IncarnationId, roots: &[String], phase: RootsPhase, secret: bool) -> StatusReply {
    let refused = |reason: String| {
        tracing::info_span!("rdm.node.root.reject.via-update", node, phase = ?phase, reason = %reason).in_scope(|| tracing::info!("node.roots.update refused"));
        reason
    };
    if roots.is_empty() || roots.len() > 2 {
        return rdm_core::reply::status::rejected_cert(refused(format!("{} roots: a fabric trusts one root, or two during a root rotation", roots.len())));
    }
    let mut set: Vec<VerifyingKey> = Vec::new();
    for hex in roots {
        let key = rdm_core::launch::decode_hex("a root", hex).ok().and_then(|b| <[u8; 32]>::try_from(b).ok()).and_then(|b| VerifyingKey::from_bytes(&b).ok());
        match key {
            Some(k) => set.push(k),
            None => return rdm_core::reply::status::rejected_cert(refused(format!("root `{hex}` is not an ed25519 public key"))),
        }
    }
    let cert = registry.member_cert();
    let own = (!cert.is_empty()).then(|| rdm_core::certs::cert_from_bytes(&cert).ok()).flatten();
    if let Some(own) = &own {
        for dropped in registry.roots().current().into_iter().filter(|held| !set.contains(held)) {
            if rdm_core::certs::verify_issuer(&own.issuer, &[dropped]).is_ok() {
                let root = rdm_core::launch::encode_hex(dropped.as_bytes());
                refused(format!("root-still-chained: this birth's member cert still chains under {root}"));
                return rdm_core::reply::status::root_still_chained(root);
            }
        }
    }
    registry.roots().replace(set);
    tracing::info_span!("rdm.node.root.update.via-installed", node, roots = %roots.join(","), phase = ?phase, secret = if secret { "carried" } else { "public-only" })
        .in_scope(|| tracing::info!("the fabric's trusted root set is installed"));
    StatusReply::RootsUpdated { node_id: node_id.clone(), incarnation: incarnation.clone(), roots: roots.to_vec() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connections_writer::ConnectionsWriter;
    use crate::storage::MemoryConnectionsStorage;
    use rdm_core::certs::{cert_to_bytes, issue, BirthIdentity, CaKey, IssuingKey};
    use rdm_core::connections::{ConnectionEnd, ConnectionsHeld};
    use rdm_core::{EndpointId, PathName};
    use rdm_transport::LiveNodeResolver;
    use std::sync::{Arc, Mutex};

    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:43
    /// "| Spans | `rdm.node_admin.root.update.via-caller` (phase, root_expires_at, previous_root_expires_at, nodes, outcome) on the fabric-primary; `rdm.node.root.update.via-installed` (node, roots, phase, secret ∈ {`carried`, `public-only`}) on each node |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:49
    /// "| Caller | The fabric-primary |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:50
    /// "| Target | One node, by exact `(node.id, incarnation)` |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:51
    /// "| Request | `UpdateRoots { node_id, incarnation, roots: [root public key, …], phase, secret? }` on [Status 0x1B](../node-rpc-envelope.md#status-op-0x1b-rdm); one or two roots, never none; the secret only to a node-admin |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:52
    /// "| Reply | `RootsUpdated { node_id, incarnation, roots }`, the receipt; a set that drops a root the node still admits a chain under is refused by name (`root-still-chained`), which is what keeps step 5 after step 4 |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:59
    /// "2. `node.roots.update { [old, new] }` to every node of the fabric, node-admins first. From its install a node admits a chain to either anchor. The row goes `distributed` on the last receipt."
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:64
    /// "4. The tick waits until every cert row chains to the new anchor: at most one member TTL after step 3. A member whose renewal has not reached it is the ordinary not-sent case, retried; the rotation waits rather than retiring an anchor something still chains to."
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:67
    /// "5. `node.roots.update { [new] }` to every node. The row goes `retired` on the last receipt, and the old secret is destroyed on every admin."
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:103
    /// "Trace propagation, step spans and correlation follow the [runtime spans](../node-rpc-envelope.md#runtime-spans). The cells, with the TTLs set to seconds in the test kit: an internal `fabric.ca.renew` with a dial admitted before, during and after and no `node.roots.update` span; a `fabric.root.update` through all five phases with a dial admitted at every phase, the spans and the roots row beside it, and a retire attempted before step 4 refused by name."
    #[test]
    fn install_roots_accepts_one_or_two_roots_and_refuses_dropped_chained_root_or_invalid_keys() {
        let dir = std::env::temp_dir().join(format!("node-roots-update-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "node-roots-update").unwrap();

        capture.run(1, async {
            let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
            let ep_id = EndpointId(endpoint.secret_key().public().to_string());
            let node_id = NodeId::mint();
            let incarnation = IncarnationId::mint();
            let name: PathName = "mesh1.rpc.1".parse().unwrap();
            let conn_store = Arc::new(MemoryConnectionsStorage::default());
            let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
            let own_end = ConnectionEnd { name: name.clone(), node_id: node_id.clone(), incarnation: Some(incarnation.clone()) };
            let writer = Arc::new(ConnectionsWriter::new(own_end, conn_store, held));
            let resolver = Arc::new(LiveNodeResolver::default());

            let (old, new, extra) = (rdm_core::certs::mint_key(), rdm_core::certs::mint_key(), rdm_core::certs::mint_key());
            let (old_pub, new_pub, extra_pub) = (old.verifying_key(), new.verifying_key(), extra.verifying_key());
            let hex = |k: &VerifyingKey| rdm_core::launch::encode_hex(k.as_bytes());
            let (old_hex, new_hex, extra_hex) = (hex(&old_pub), hex(&new_pub), hex(&extra_pub));

            let old_ca = CaKey::mint(&old, 3600, 1_000);
            let old_issuer = IssuingKey::mint(&old_ca, "mesh1", 1_000, 2_000_000).unwrap();
            let birth = BirthIdentity { node_id: node_id.clone(), incarnation: incarnation.clone(), name: name.clone(), mesh: "mesh1".into(), endpoint_key: ep_id };
            let old_cert = cert_to_bytes(&issue(&old_issuer, &birth, 1_000, 900_000).unwrap());

            let (registry, _handlers) = ConnectionsRegistry::new(endpoint, resolver, writer, &[], old_cert).unwrap();
            registry.roots().replace(vec![old_pub]);

            // Invalid counts (0 or 3 roots) and non-key hex are refused `RejectedCert`.
            assert!(matches!(install_roots(&registry, "mesh1.rpc.1", &node_id, &incarnation, &[], RootsPhase::Distributed, false), StatusReply::RejectedCert { .. }));
            assert!(matches!(install_roots(&registry, "mesh1.rpc.1", &node_id, &incarnation, &[old_hex.clone(), new_hex.clone(), extra_hex], RootsPhase::Distributed, false), StatusReply::RejectedCert { .. }));
            assert!(matches!(install_roots(&registry, "mesh1.rpc.1", &node_id, &incarnation, &["not-a-hex-key".into()], RootsPhase::Distributed, false), StatusReply::RejectedCert { .. }));

            // Phase 2 (`Distributed`): installing `[old, new]` succeeds and records `secret` (`public-only` or `carried`).
            let both = vec![old_hex.clone(), new_hex.clone()];
            assert_eq!(
                install_roots(&registry, "mesh1.rpc.1", &node_id, &incarnation, &both, RootsPhase::Distributed, false),
                StatusReply::RootsUpdated { node_id: node_id.clone(), incarnation: incarnation.clone(), roots: both.clone() }
            );
            assert_eq!(registry.roots().current(), vec![old_pub, new_pub]);
            assert_eq!(
                install_roots(&registry, "mesh1.admin.1", &node_id, &incarnation, &both, RootsPhase::Distributed, true),
                StatusReply::RootsUpdated { node_id: node_id.clone(), incarnation: incarnation.clone(), roots: both.clone() }
            );

            // Dropping `old` while the birth's member cert still chains under `old` is refused `root-still-chained`.
            assert_eq!(
                install_roots(&registry, "mesh1.rpc.1", &node_id, &incarnation, std::slice::from_ref(&new_hex), RootsPhase::Retired, false),
                StatusReply::RejectedRootStillChained { root: old_hex.clone() }
            );
            assert_eq!(registry.roots().current(), vec![old_pub, new_pub], "both roots stay installed on root-still-chained refusal");

            // Once the birth's member cert chains to `new`, installing `[new]` at `Retired` succeeds.
            let new_ca = CaKey::mint(&new, 3600, 2_000);
            let new_issuer = IssuingKey::mint(&new_ca, "mesh1", 2_000, 2_000_000).unwrap();
            let new_cert = cert_to_bytes(&issue(&new_issuer, &birth, 2_000, 900_000).unwrap());
            registry.set_member_cert(new_cert);
            assert_eq!(
                install_roots(&registry, "mesh1.rpc.1", &node_id, &incarnation, std::slice::from_ref(&new_hex), RootsPhase::Retired, false),
                StatusReply::RootsUpdated { node_id: node_id.clone(), incarnation: incarnation.clone(), roots: vec![new_hex.clone()] }
            );
            assert_eq!(registry.roots().current(), vec![new_pub]);
        });

        let spans = capture.spans().unwrap();
        let installed: Vec<_> = spans.iter().filter(|s| s["name"].as_str() == Some("rdm.node.root.update.via-installed")).collect();
        assert_eq!(installed.len(), 3, "expected 3 via-installed spans, got {installed:?}");
        assert_eq!(installed[0]["attributes"]["secret"].as_str(), Some("public-only"));
        assert_eq!(installed[1]["attributes"]["secret"].as_str(), Some("carried"));
        assert_eq!(installed[2]["attributes"]["phase"].as_str(), Some("Retired"));
        let rejected: Vec<_> = spans.iter().filter(|s| s["name"].as_str() == Some("rdm.node.root.reject.via-update")).collect();
        assert_eq!(rejected.len(), 4, "expected 4 reject.via-update spans, got {rejected:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
