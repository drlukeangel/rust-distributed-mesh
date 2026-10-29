//! `app-gossip-channels`: declared application publication channels over the CloudEvents gossip
//! envelope (`docs/architecture/gossip.md:1`, `crates/rdm-transport/src/gossip.rs:48`).
//!
//! A process declares the application event types it publishes at boot (`types`), and the mesh's
//! held-type union (`held_types`) lets any member join and receive a type held in its mesh via
//! `topology_view.join_channel(kind)`. Events carry `subject`, `sequence`, `dataschema`, and opaque
//! `data` bytes; RDM delivers them with their original author's [`rdm_node::gossip::Birth`] and a
//! receive span parented to the author's span. Joining or publishing a type not held in the mesh is
//! refused with [`ChannelError::UndeclaredType`].
//!
//! Build: `cargo build --example app_gossip_channels` in `demo/`.
//! Run against a live estate (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/app_gossip_channels
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_admin::admin::{start_with, AdminConfig};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_client::{Filter, MeshNode, Nodes, Peers};
use rdm_core::{IncarnationId, NodeId};
use rdm_node::gossip::{ChannelError, Event};
use std::time::Duration;

const KIND: &str = "demo.records";
const BOUND: Duration = Duration::from_secs(30);

fn config(dir: &std::path::Path) -> AdminConfig {
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.display().to_string()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("admin config")
}

fn main() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("demo-app-channel-{}", NodeId::mint()));
    let capture = rdm_transport::EvidenceCapture::start(&dir.join("evidence"), "example-app-gossip-channels").map_err(|e| anyhow!(e))?;
    let res = capture.run(4, async {
        if let Ok(json) = std::env::var("RDM_NODE_ADMIN_BIRTH") {
            let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
            let peers = Peers::at(&birth).await.map_err(|e| anyhow!(e))?;
            let rows = Nodes::new(peers.carrier_at(birth.name)).get(&Filter::new()).await?;
            let first = rows.first().ok_or_else(|| anyhow!("node.get returned no rows"))?;
            println!(
                "live estate node {}: types={:?} held={:?} held_hash={:?}",
                first.name, first.types, first.held, first.held_hash
            );
        }

        let root_dir = dir.join("admin");
        let broker_dir = dir.join("broker");

        let root = start_with(
            config(&root_dir),
            Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() },
        )
        .await
        .map_err(|e| anyhow!(e))?;
        root.stop_reconciling().await;

        let d = root.digest.lock().unwrap().clone();
        std::fs::create_dir_all(&broker_dir)?;
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("demo-app")).map_err(|e| anyhow!(e))?;
        fact.write_record(&broker_dir).map_err(|e| anyhow!(e))?;
        let key = rdm_node::node::load_or_mint_key(&broker_dir).map_err(|e| anyhow!(e))?;
        let launch = rdm_core::launch::Launch {
            topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: config(&root_dir).fabric,
            fabric_id: d.fabric_id.clone(),
            name: "mesh1.broker.1".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            declared: Vec::new(),
            seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
            launcher: Some(rdm_core::launch::Launcher {
                name: d.node.name.clone(),
                node_id: d.node.node_id.clone(),
                incarnation: d.node.incarnation.clone(),
            }),
            data_dir: broker_dir.clone(),
            store_dir: root_dir.clone(),
            mesh_id: d.mesh_id.clone(),
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        };
        let _ = root.runner.joins.expect(rdm_admin::join::Deployed {
            listeners: Vec::new(),
            name: launch.name.clone(),
            node_id: launch.node_id.clone(),
            incarnation: launch.incarnation.clone(),
            supersedes: None,
            endpoint_id: rdm_core::EndpointId(key.public().to_string()),
            runtime: fact,
            data_dir: broker_dir.display().to_string(),
            last_accepted_name: None,
            held: false,
        });
        let node = rdm_node::node::start_with_rafka_time_pre_node_ready_and(
            &launch,
            rdm_transport::clock::RafkaTime::unadopted(),
            rdm_node::node_hooks::NodeHooks::none(),
            |s| s,
            None,
            |b, _| b,
            &[KIND],
        )
        .await?;

        println!("broker app_types={:?}, admin held_types={:?}", node.topology_view.app_types(), root.topology_view.held_types());
        let refused = node.topology_view.join_channel("demo.undeclared").await.err();
        assert!(matches!(refused, Some(ChannelError::UndeclaredType { .. })));
        println!("undeclared channel refused: {}", refused.unwrap());

        let sender = node.topology_view.join_channel(KIND).await?;
        let mut receiver = root.topology_view.join_channel(KIND).await?;
        tokio::time::timeout(BOUND, async {
            while capture.spans().unwrap().iter().filter(|s| s["name"] == "rdm.mesh.channel.update.via-neighbour" && s["attributes"]["event_type"] == KIND).count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        sender
            .publish(Event {
                subject: "record/1".into(),
                sequence: Some(1),
                dataschema: Some("demo.row.v1".into()),
                data: b"hello-app-channel".to_vec().into(),
            })
            .await?;
        let received = tokio::time::timeout(BOUND, receiver.recv()).await?.ok_or_else(|| anyhow!("channel closed"))?;
        println!(
            "received on {KIND}: author={}/{} subject={} sequence={:?} dataschema={:?} payload={}",
            received.source.node,
            received.source.incarnation,
            received.subject,
            received.sequence,
            received.dataschema,
            String::from_utf8_lossy(&received.data)
        );
        node.stop().await;
        root.topology_view.stop().await;
        Ok(())
    });
    let _ = std::fs::remove_dir_all(&dir);
    res
}
