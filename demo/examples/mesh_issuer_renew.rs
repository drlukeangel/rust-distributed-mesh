//! `mesh.issuer.renew` and `node.issuer.update`: minting a renewed mesh issuing key under the
//! fabric CA and distributing it directly to the peer mesh's primary, then via Forward `0x1A`
//! through that primary to the mesh's other node-admins
//! (`docs/architecture/lifecycles/mesh-issuer-renew.md:1`,
//! `docs/architecture/lifecycles/node-issuer-update.md:1`,
//! `crates/rdm-admin/src/cert_renewal.rs:365`, `crates/rdm-admin/src/certs.rs:391`).
//!
//! When a mesh's signing cert passes its `renew_at`, the fabric-primary mints a new [`IssuingKey`]
//! and [`IssuerMint`] under the fabric CA ([`CertIssuer::renew_issuer`]) and sends
//! [`StatusRequest::UpdateIssuer`] (`node.issuer.update`):
//! 1. directly (`call::<Status>`) to the peer mesh's mesh-primary first;
//! 2. once the mesh-primary answers [`StatusReply::IssuerUpdated`] confirming `issuer_expires_at`,
//!    forwarded through that mesh-primary (`call_via::<Status>`) to each other node-admin of the mesh.
//!
//! Build: `cargo build --example mesh_issuer_renew` in `demo/`.
//! Run against a live estate (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/mesh_issuer_renew [<mesh>]
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_admin::cert_renewal::{installed_signing_dates, signing_dates};
use rdm_admin::certs::CertChoice;
use rdm_admin::rafka_time::adopt_own_clock;
use rdm_client::{Filter, MeshNode, NodeStatus, Nodes, Peers};
use rdm_core::certs::{encode_issuing_cert, encode_key};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{CaKeyHex, IssuingKeyHex, Status, StatusReply, StatusRequest};

use rdm_transport::clock::{Clock, RafkaTime};
use rdm_transport::{CallOptions, NodeTarget};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-mesh-issuer-renew");
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    let entry = birth.name.clone();
    let peers = Peers::at(&birth).await.map_err(|e| anyhow!(e))?;
    let carrier = peers.carrier_at(entry.clone());
    let all = Nodes::new(carrier.clone()).get(&Filter::new()).await?;
    peers.learn(&all);

    let fp_row = all.iter().find(|n| n.name == entry).ok_or_else(|| anyhow!("no row for path {:?}, name {:?}", entry.path, entry.name))?;
    let peer_mesh = match std::env::args().nth(1) {
        Some(m) => m,
        None => all
            .iter()
            .find(|n| n.mesh != birth.mesh && n.kind == "node-admin" && n.status == NodeStatus::Ready)
            .map(|n| n.mesh.clone())
            .ok_or_else(|| anyhow!("no Ready peer mesh found in estate"))?,
    };
    let primary = all
        .iter()
        .find(|n| n.mesh == peer_mesh && n.kind == "node-admin" && n.is_primary)
        .ok_or_else(|| anyhow!("no mesh-primary in {peer_mesh}"))?;
    let replica = all
        .iter()
        .find(|n| n.mesh == peer_mesh && n.kind == "node-admin" && !n.is_primary)
        .ok_or_else(|| anyhow!("no secondary node-admin in {peer_mesh}"))?;

    let admin_dir = Path::new(fp_row.data_dir.as_deref().ok_or_else(|| anyhow!("no admin data_dir"))?);
    let choice = CertChoice::from_material(admin_dir, &birth.mesh, false, |_| None).map_err(|e| anyhow!(e))?;
    let time = RafkaTime::unadopted();
    adopt_own_clock(&tracing::Span::none(), &time, &birth.node_id.to_string(), "example");
    let _ = time.observe(time.now_rafka_ms() + 3_000);
    let issuer = choice.issuer(admin_dir, false, time).map_err(|e| anyhow!(e))?;

    let (key, mint) = issuer.renew_issuer(&peer_mesh).map_err(|e| anyhow!("{e}"))?;
    let dates = signing_dates(&mint);
    assert_eq!(installed_signing_dates(key.cert(), &issuer), dates);
    println!(
        "minted new issuer for {peer_mesh}: issued_at={} renew_at={} expires_at={}",
        mint.issued_at, mint.renew_at, mint.expires_at
    );

    // The CA the issuer is minted under rides with it (Q275), as cert_renewal sends it.
    let ca_key = CaKeyHex(issuer.ca_key_of(&key).unwrap_or_default());
    let primary_inc = primary.incarnation_id.clone().ok_or_else(|| anyhow!("no incarnation_id on path {:?}, name {:?}", primary.name.path, primary.name.name))?;
    let req_primary = StatusRequest::UpdateIssuer {
        node_id: primary.node_id.clone(),
        incarnation: primary_inc,
        key: IssuingKeyHex(encode_key(key.secret())),
        cert: encode_issuing_cert(key.cert()),
        ca_key: ca_key.clone(),
    };
    let (out_primary, _) = carrier
        .client()
        .call::<Status>(&NodeTarget::ExactNode(primary.node_id.clone()), &req_primary, &CallOptions::default())
        .await;
    let RpcOutcome::Reply(reply_primary) = out_primary else {
        return Err(anyhow!("direct UpdateIssuer to path {:?}, name {:?} failed: {out_primary:?}", primary.name.path, primary.name.name));
    };
    let StatusReply::IssuerUpdated { issuer_expires_at: exp_primary, .. } = reply_primary.into_value() else {
        return Err(anyhow!("unexpected StatusReply from path {:?}, name {:?}", primary.name.path, primary.name.name));
    };
    assert_eq!(exp_primary, mint.expires_at);
    println!(
        "step 1 direct node.issuer.update to mesh-primary path {:?}, name {:?} ({}): IssuerUpdated(expires_at={exp_primary})",
        primary.name.path, primary.name.name, primary.node_id
    );

    let replica_inc = replica.incarnation_id.clone().ok_or_else(|| anyhow!("no incarnation_id on path {:?}, name {:?}", replica.name.path, replica.name.name))?;
    let req_replica = StatusRequest::UpdateIssuer {
        node_id: replica.node_id.clone(),
        incarnation: replica_inc,
        key: IssuingKeyHex(encode_key(key.secret())),
        cert: encode_issuing_cert(key.cert()),
        ca_key: ca_key.clone(),
    };
    let (out_replica, _) = carrier
        .client()
        .call_via::<Status>(
            &NodeTarget::ExactNode(primary.node_id.clone()),
            &replica.node_id,
            &req_replica,
            &CallOptions::default(),
        )
        .await;
    let RpcOutcome::Reply(reply_replica) = out_replica else {
        return Err(anyhow!("forwarded UpdateIssuer to path {:?}, name {:?} failed: {out_replica:?}", replica.name.path, replica.name.name));
    };
    let StatusReply::IssuerUpdated { issuer_expires_at: exp_replica, .. } = reply_replica.into_value() else {
        return Err(anyhow!("unexpected StatusReply from path {:?}, name {:?}", replica.name.path, replica.name.name));
    };
    assert_eq!(exp_replica, mint.expires_at);
    println!(
        "step 2 forwarded node.issuer.update via path {:?}, name {:?} to path {:?}, name {:?} ({}): IssuerUpdated(expires_at={exp_replica})",
        primary.name.path, primary.name.name, replica.name.path, replica.name.name, replica.node_id
    );
    Ok(())
}
