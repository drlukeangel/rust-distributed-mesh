//! `node.cert.renew`: renewing a live member's certificate and verifying `CertRenewed`,
//! `installed_dates`, and `stalled` (`docs/architecture/lifecycles/node-cert-renew.md:1`,
//! `crates/rdm-admin/src/cert_renewal.rs:214`, `crates/rdm-admin/src/certs.rs:360`).
//!
//! Each birth's member cert dates (`issued_at`, `expires_at`, `issuer_expires_at`, `renew_at`) are
//! tracked on its node record. When a birth passes its `renew_at`, its issuer mints a fresh member
//! cert via [`CertIssuer::renewal`] and sends [`StatusRequest::RenewCert`] on Status `0x1B` to the
//! exact birth, which verifies the chain against its `RootSet`, installs the new cert, and answers
//! [`StatusReply::CertRenewed`] naming `expires_at`. A birth past `renew_at` by more than one
//! staleness floor without confirming a renewal is flagged by [`stalled`].
//!
//! Build: `cargo build --example node_cert_renew` in `demo/`.
//! Run against a live estate (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/node_cert_renew [<path.name>]
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_admin::cert_renewal::{dates_of, installed_dates, stalled};
use rdm_admin::certs::{BirthIdentity, CertChoice};
use rdm_admin::rafka_time::adopt_own_clock;
use rdm_client::{Filter, MeshNode, NodeStatus, Nodes, PathName, Peers};
use rdm_core::cadence::staleness_floor;
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{Status, StatusReply, StatusRequest};
use rdm_core::{EndpointId, NodeKind};
use rdm_transport::clock::{Clock, RafkaTime};
use rdm_transport::{CallOptions, NodeTarget};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-node-cert-renew");
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    let entry = birth.name.clone();
    let peers = Peers::at(&birth).await.map_err(|e| anyhow!(e))?;
    let carrier = peers.carrier_at(entry.clone());
    let all = Nodes::new(carrier.clone()).get(&Filter::new()).await?;
    peers.learn(&all);

    let admin_row = all.iter().find(|n| n.name == entry).ok_or_else(|| anyhow!("no row for {entry}"))?;
    let target_name: PathName = match std::env::args().nth(1) {
        Some(arg) => arg.parse().map_err(|e| anyhow!("{arg}: {e}"))?,
        None => all
            .iter()
            .find(|n| n.mesh == entry.mesh && n.kind != NodeKind::NodeAdmin && n.status == NodeStatus::Ready)
            .map(|n| n.name.clone())
            .ok_or_else(|| anyhow!("no Ready role member in {}", entry.mesh))?,
    };
    let member = all.iter().find(|n| n.name == target_name).ok_or_else(|| anyhow!("no row for {target_name}"))?;
    let before_cert = member.cert.as_ref().ok_or_else(|| anyhow!("{target_name} has no cert view"))?;
    println!(
        "before renewal on {}: issued_at={} expires_at={:?} issuer_expires_at={:?}",
        member.name, before_cert.issued_at, before_cert.expires_at, before_cert.issuer_expires_at
    );

    let admin_dir = Path::new(admin_row.data_dir.as_deref().ok_or_else(|| anyhow!("no admin data_dir"))?);
    let choice = CertChoice::from_material(admin_dir, &entry.mesh, false, |_| None).map_err(|e| anyhow!(e))?;
    let time = RafkaTime::unadopted();
    adopt_own_clock(&tracing::Span::none(), &time, &entry.to_string(), "example");
    let _ = time.observe(time.now_rafka_ms() + 2_000);
    let issuer = choice.issuer(admin_dir, false, time).map_err(|e| anyhow!(e))?;

    let inc = member.incarnation_id.clone().ok_or_else(|| anyhow!("no incarnation_id"))?;
    let ep = member.endpoint_id.clone().ok_or_else(|| anyhow!("no endpoint_id"))?;
    let identity = BirthIdentity {
        node_id: member.node_id.clone(),
        incarnation: inc.clone(),
        name: member.name.clone(),
        mesh: member.mesh.clone(),
        endpoint_key: EndpointId(ep),
    };
    let issue = issuer.renewal(&identity).map_err(|e| anyhow!("{e}"))?;
    let req = StatusRequest::RenewCert {
        node_id: member.node_id.clone(),
        incarnation: inc,
        cert: issue.bytes.clone(),
    };
    let (out, _) = carrier
        .client()
        .call::<Status>(&NodeTarget::ExactNode(member.node_id.clone()), &req, &CallOptions::default())
        .await;
    let RpcOutcome::Reply(reply) = out else {
        return Err(anyhow!("RenewCert failed: {out:?}"));
    };
    let StatusReply::CertRenewed { expires_at, .. } = reply.into_value() else {
        return Err(anyhow!("unexpected StatusReply for RenewCert"));
    };
    assert_eq!(expires_at, issue.expires_at);

    let dates = dates_of(&issue);
    assert_eq!(installed_dates(&req, &issuer).as_ref(), Some(&dates));
    let floor_ms = staleness_floor().as_millis() as u64;
    assert!(!stalled(&dates, dates.renew_at + floor_ms, floor_ms));
    assert!(stalled(&dates, dates.renew_at + floor_ms + 1, floor_ms));
    println!(
        "CertRenewed on {}: issued_at={} renew_at={} expires_at={expires_at} stalled_past_floor={}",
        member.name,
        dates.issued_at,
        dates.renew_at,
        stalled(&dates, dates.renew_at + floor_ms + 1, floor_ms)
    );
    Ok(())
}
