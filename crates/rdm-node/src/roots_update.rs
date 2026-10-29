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
/// `rdm.node.roots.update.via-installed`, or `rdm.node.roots.reject.via-update` on a refusal.
pub fn install_roots(registry: &ConnectionsRegistry, node: &str, node_id: &NodeId, incarnation: &IncarnationId, roots: &[String], phase: RootsPhase, secret: bool) -> StatusReply {
    let refused = |reason: String| {
        tracing::info_span!("rdm.node.roots.reject.via-update", node, phase = ?phase, reason = %reason).in_scope(|| tracing::info!("node.roots.update refused"));
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
    tracing::info_span!("rdm.node.roots.update.via-installed", node, roots = %roots.join(","), phase = ?phase, secret = if secret { "carried" } else { "public-only" })
        .in_scope(|| tracing::info!("the fabric's trusted root set is installed"));
    StatusReply::RootsUpdated { node_id: node_id.clone(), incarnation: incarnation.clone(), roots: roots.to_vec() }
}
