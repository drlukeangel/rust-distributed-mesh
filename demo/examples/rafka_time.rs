//! `rafka-time`: reading a live node's served rafka-time and verifying the adoption and
//! heartbeat-discipline ordering (`docs/architecture/fabric-node-hooks.md:145`,
//! `crates/rdm-transport/src/clock.rs:1`, `crates/rdm-node/src/rafka_time.rs:1`).
//!
//! Every RDM process composes one [`RafkaTime`]:
//! - the Day-0 root adopts its own OS clock once at boot (`adopt_own_clock`);
//! - every other process adopts from its pull (`JoinNode` or `GetTopology` op `0x1E`) through
//!   [`adopt_pulled`]: a `Puller::Member` adopts from whichever node answers, while a
//!   `Puller::MeshPrimary` adopts only from `PrimaryKind::FabricPrimary` (refusing
//!   `Refusal::ServedByAReplica` and `Refusal::AMeshPrimaryTakesOnlyTheFabricPrimary`);
//! - between pulls, [`Clock::observe`] steps forward on a newer authority stamp
//!   (`Observed::Stepped`) and ignores stamps before adoption (`Ignored::NotAdopted`) or not
//!   newer than the latest sample (`Ignored::NotNewer`).
//!
//! Build: `cargo build --example rafka_time` in `demo/`.
//! Run against a live estate (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/rafka_time
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_admin::rafka_time::{adopt_pulled, Puller, Refusal, Served};
use rdm_client::{Filter, MeshNode, NodeStatus, Nodes, Peers};
use rdm_core::topology::PrimaryKind;
use rdm_core::NodeKind;
use rdm_node::topology_read::read_topology;
use rdm_transport::clock::{Clock, Ignored, Observed, RafkaTime};
use rdm_transport::NodeTarget;

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-rafka-time");
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    let entry = birth.name.clone();
    let peers = Peers::at(&birth).await.map_err(|e| anyhow!(e))?;
    let carrier = peers.carrier_at(entry.clone());
    let all = Nodes::new(carrier.clone()).get(&Filter::new()).await?;
    peers.learn(&all);

    let fp_read = read_topology(
        carrier.client(),
        &NodeTarget::CurrentPath(entry.clone()),
        "example-rafka-time",
        Some(&entry.mesh),
        None,
    )
    .await
    .map_err(|e| anyhow!("{e:?}"))?;
    let fp_time = fp_read.rafka_time.ok_or_else(|| anyhow!("fabric-primary served no rafka_time"))?;
    println!(
        "live fabric-primary {entry} served rafka_time: ms={} kind={:?}",
        fp_time.ms, fp_time.kind
    );

    let member = all
        .iter()
        .find(|n| n.kind != NodeKind::NodeAdmin && n.status == NodeStatus::Ready)
        .ok_or_else(|| anyhow!("no Ready role member in estate"))?;
    let member_read = read_topology(
        carrier.client(),
        &NodeTarget::ExactNode(member.node_id.clone()),
        "example-rafka-time",
        Some(&member.mesh),
        None,
    )
    .await
    .map_err(|e| anyhow!("{e:?}"))?;
    let member_time = member_read
        .rafka_time
        .ok_or_else(|| anyhow!("{} served no rafka_time", member.name))?;
    println!(
        "live member {} served rafka_time: ms={} kind={:?} cert_issued_at={:?}",
        member.name,
        member_time.ms,
        member_time.kind,
        member.cert.as_ref().map(|c| c.issued_at)
    );

    let clock = RafkaTime::unadopted();
    assert_eq!(clock.try_now_ms(), None);
    assert_eq!(clock.observe(fp_time.ms), Observed::Ignored(Ignored::NotAdopted));

    let replica_refusal = adopt_pulled(
        &clock,
        Puller::MeshPrimary,
        "mesh2.admin.1",
        Served { via: "get-topology", served_by: &member.name.to_string(), ms: member_time.ms, kind: member_time.kind },
    )
    .unwrap_err();
    assert_eq!(replica_refusal, Refusal::ServedByAReplica);

    let mesh_primary_refusal = adopt_pulled(
        &clock,
        Puller::MeshPrimary,
        "mesh2.admin.1",
        Served { via: "get-topology", served_by: "mesh2.admin.2", ms: fp_time.ms, kind: Some(PrimaryKind::MeshPrimary) },
    )
    .unwrap_err();
    assert_eq!(mesh_primary_refusal, Refusal::AMeshPrimaryTakesOnlyTheFabricPrimary);
    println!("MeshPrimary pull refusals: replica={replica_refusal}, mesh-primary={mesh_primary_refusal}");

    let adopted = adopt_pulled(
        &clock,
        Puller::MeshPrimary,
        "mesh2.admin.1",
        Served { via: "get-topology", served_by: &entry.to_string(), ms: fp_time.ms, kind: fp_time.kind },
    )
    .map_err(|e| anyhow!("{e}"))?;
    let t0 = clock.now_rafka_ms();
    let future_stamp = t0 + 5_000;
    let stepped = clock.observe(future_stamp);
    let t1 = clock.now_rafka_ms();
    let stale = clock.observe(future_stamp - 1);
    assert!(matches!(stepped, Observed::Stepped { .. }));
    assert!(t1 >= future_stamp && t1 > t0);
    assert_eq!(stale, Observed::Ignored(Ignored::NotNewer));
    println!(
        "adopted reference_ms={}: t0={t0} -> observe({future_stamp})={stepped:?} -> t1={t1}; older stamp -> {stale:?}",
        adopted.reference_ms
    );
    Ok(())
}
