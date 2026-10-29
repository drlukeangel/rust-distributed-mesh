//! `node.connection.create`: the acceptor's verification of the member cert a dialer presents on
//! the first bi-stream of an inbound connection on a registered application ALPN
//! (`docs/architecture/lifecycles/node-connections-admit.md:1`,
//! `crates/rdm-transport/src/connections/admit.rs:1`, `crates/rdm-admin/src/certs.rs:1`).
//!
//! Before serving any application stream on a registered [`Alpn`], the acceptor verifies the
//! dialer's presented [`SignedCert`](rdm_core::certs::SignedCert) chain against its [`RootSet`] on
//! [`RafkaTime`] via [`CertAdmitter`]:
//! - `validate_alpns` accepts application ALPNs (`demo/echo`) and refuses RDM's own control-plane
//!   ALPN (`rdm_transport::ALPN`);
//! - `CertAdmitter::verify` returns `Ok((cert_expires_at_ms, "rafka-time"))` for a valid member
//!   cert, refuses empty bytes with `no-credential`, and refuses an untrusted root;
//! - an end-to-end `demo/echo` dial (`Dialer::open` -> `Acceptor::accept`) is admitted by the
//!   member cert and echoes its stream payload.
//!
//! Build: `cargo build --example node_connections_admit` in `demo/`.
//! Run against a live estate (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/node_connections_admit
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_admin::admin::{start_with, AdminConfig};
use rdm_admin::certs::{BirthIdentity, CertChoice};
use rdm_admin::rafka_time::adopt_own_clock;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_client::{Filter, MeshNode, NodeStatus, Nodes, Peers};
use rdm_core::{IncarnationId, NodeId, NodeKind};
use rdm_node::connections::Birth;
use rdm_transport::clock::RafkaTime;
use rdm_transport::connections::{validate_alpns, Admitter, Alpn, CertAdmitter, RootSet};
use rdm_transport::{NodeResolver, NodeTarget};
use std::path::Path;
use std::time::Duration;

const BOUND: Duration = Duration::from_secs(30);

fn config(dir: &Path) -> AdminConfig {
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.display().to_string()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("admin config")
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-node-connections-admit");
    validate_alpns(&[rshape_consumer::data_alpn()]).map_err(|e| anyhow!(e))?;
    let internal_refusal = validate_alpns(&[Alpn {
        name: rdm_transport::ALPN.to_vec(),
        proxy: false,
        carriers: vec![],
    }])
    .unwrap_err();
    println!("validate_alpns: demo/echo accepted; internal ALPN refused: {internal_refusal}");

    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    let peers = Peers::at(&birth).await.map_err(|e| anyhow!(e))?;
    let all = Nodes::new(peers.carrier_at(birth.name.clone())).get(&Filter::new()).await?;
    let admin_row = all.iter().find(|n| n.name == birth.name).ok_or_else(|| anyhow!("no row for {}", birth.name))?;
    let member = all
        .iter()
        .find(|n| n.kind != NodeKind::NodeAdmin && n.status == NodeStatus::Ready)
        .ok_or_else(|| anyhow!("no Ready role member in estate"))?;
    let admin_dir = Path::new(admin_row.data_dir.as_deref().ok_or_else(|| anyhow!("no admin data_dir"))?);

    let choice = CertChoice::from_material(admin_dir, &birth.name.mesh, false, |_| None).map_err(|e| anyhow!(e))?;
    let CertChoice::Ca { ref secret, .. } = choice else { return Err(anyhow!("expected CertChoice::Ca")) };
    let root_pub = secret.verifying_key();
    let time = RafkaTime::unadopted();
    adopt_own_clock(&tracing::Span::none(), &time, &birth.name.to_string(), "example");
    let issuer = choice.issuer(admin_dir, false, time.clone()).map_err(|e| anyhow!(e))?;
    let inc = member.incarnation_id.clone().ok_or_else(|| anyhow!("no incarnation_id"))?;
    let ep = member.endpoint_id.clone().ok_or_else(|| anyhow!("no endpoint_id"))?;
    let issue = issuer
        .issue_member(&BirthIdentity {
            node_id: member.node_id.clone(),
            incarnation: inc.clone(),
            name: member.name.clone(),
            mesh: member.mesh.clone(),
            endpoint_key: rdm_core::EndpointId(ep),
        })
        .map_err(|e| anyhow!("{e}"))?;
    let caller = Birth { name: member.name.clone(), node_id: member.node_id.clone(), incarnation: inc };
    let admitter = CertAdmitter::new(RootSet::new(vec![root_pub]), time.clone());
    let (expires_at, clock) = admitter.verify(&caller, b"demo/echo", &issue.bytes).map_err(|e| anyhow!(e))?;
    let empty_err = admitter.verify(&caller, b"demo/echo", &[]).unwrap_err();
    let untrusted = CertAdmitter::new(RootSet::new(vec![rdm_core::certs::mint_key().verifying_key()]), time)
        .verify(&caller, b"demo/echo", &issue.bytes)
        .unwrap_err();
    println!(
        "CertAdmitter::verify for {}: admitted (expires_at_ms={expires_at}, clock={clock}); empty={empty_err}; untrusted={untrusted}",
        caller.name
    );

    let dir = std::env::temp_dir().join(format!("demo-admit-{}", NodeId::mint()));
    let c = CertChoice::from_material(&dir, "mesh1", true, |_| None).map_err(|e| anyhow!(e))?;
    let CertChoice::Ca { ref secret, .. } = c else { unreachable!() };
    let root_key = secret.verifying_key();
    let root = start_with(
        config(&dir),
        Wiring { alpns: vec![rshape_consumer::data_alpn()], certs: c, fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() },
    )
    .await
    .map_err(|e| anyhow!(e))?;
    root.stop_reconciling().await;
    let d = root.digest.lock().unwrap().clone();
    let gw_dir = dir.join("mesh1.gateway.1");
    std::fs::create_dir_all(&gw_dir)?;
    let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("demo-admit")).map_err(|e| anyhow!(e))?;
    fact.write_record(&gw_dir).map_err(|e| anyhow!(e))?;
    let key = rdm_node::node::load_or_mint_key(&gw_dir).map_err(|e| anyhow!(e))?;
    let launch = rdm_core::launch::Launch {
        topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: config(&dir).fabric,
        fabric_id: d.fabric_id.clone(),
        name: "mesh1.gateway.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        declared: Vec::new(),
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(rdm_core::launch::Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: gw_dir.clone(),
        store_dir: dir.clone(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: vec![root_key],
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    };
    let _ = root.runner.joins.expect(rdm_admin::join::Deployed {
        listeners: Vec::new(), name: launch.name.clone(), node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(),
        supersedes: None, endpoint_id: rdm_core::EndpointId(key.public().to_string()), runtime: fact,
        data_dir: gw_dir.display().to_string(), last_accepted_name: None, held: false,
    });
    let gw = rdm_node::node::start_with_rafka_time_pre_node_ready_alpns_and(
        &launch, RafkaTime::unadopted(), rdm_node::node_hooks::NodeHooks::none(), |s| s, None, |b, _| b, &[], &[rshape_consumer::data_alpn()],
    ).await?;
    let (dialer, _) = root.connections(b"demo/echo").expect("admin demo/echo").split();
    let (_, mut acceptor) = gw.connections(b"demo/echo").expect("gateway demo/echo").split();
    let gw_node = gw.digest.node.clone();
    while root.node_rpc.resolver.resolve(&NodeTarget::ExactNode(gw_node.node_id.clone())).is_err() {
        tokio::task::yield_now().await;
    }
    let to = Birth { name: gw_node.name.clone(), node_id: gw_node.node_id.clone(), incarnation: gw_node.incarnation.clone() };
    let (conn, srv) = tokio::try_join!(
        async { tokio::time::timeout(BOUND, dialer.open(&to)).await?.map_err(|e| anyhow!("{e}")) },
        async { tokio::time::timeout(BOUND, acceptor.accept()).await?.ok_or_else(|| anyhow!("no accept")) }
    )?;
    let (mut c_send, mut c_recv) = conn.open_bi().await.map_err(|e| anyhow!("{e}"))?;
    c_send.write_all(b"hello-admitted").await?;
    c_send.finish().map_err(|e| anyhow!("{e}"))?;
    let (mut s_send, mut s_recv) = srv.accept_bi().await.map_err(|e| anyhow!("{e}"))?;
    let req = s_recv.read_to_end(256).await.map_err(|e| anyhow!("{e}"))?;
    s_send.write_all(format!("echo {}: {}", gw_node.name, String::from_utf8_lossy(&req)).as_bytes()).await?;
    s_send.finish().map_err(|e| anyhow!("{e}"))?;
    let reply = String::from_utf8(c_recv.read_to_end(256).await.map_err(|e| anyhow!("{e}"))?)?;
    println!("admitted demo/echo dial to {} ({:?}): {reply}", srv.peer().name, conn.kind());
    drop((conn, srv));
    gw.stop().await;
    root.topology_view.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
