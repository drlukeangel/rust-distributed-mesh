//! The independent consumer uses only public RDM surfaces for live/pull reconciliation and loss.
use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::catalog::OpOwner;
use rdm_core::{IncarnationId, NodeId};
use rdm_node::gossip::{Channel, Event};
use rdm_test::pre_node_ready_probe::{PreNodeReadyPull, PullReply, PullRequest, Row};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
const KIND: &str = "demo.records";
const LARGE: &str = "demo.large-records";
const BOUND: Duration = Duration::from_secs(60);
fn config(dir: &std::path::Path) -> AdminConfig {
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.display().to_string()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .unwrap()
}
fn event(row: &Row) -> Event {
    Event {
        subject: "record/1".into(),
        sequence: Some(row.version),
        dataschema: Some("demo.row.v1".into()),
        data: postcard::to_allocvec(row).unwrap().into(),
    }
}
fn order(row: &Row) -> (u64, u64) {
    (
        row.value.split_once(':').unwrap().0.parse().unwrap(),
        row.version,
    )
}
fn apply(held: &mut Option<Row>, row: Row) {
    if held.as_ref().is_none_or(|old| order(&row) > order(old)) {
        *held = Some(row);
    }
}
async fn recv(channel: &mut Channel) -> Row {
    let event = tokio::time::timeout(BOUND, channel.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.dataschema.as_deref(), Some("demo.row.v1"));
    event
        .span
        .in_scope(|| postcard::from_bytes(&event.data).unwrap())
}
async fn pull(client: &rdm_transport::NodeRpcClient, root: &NodeId) -> Vec<Row> {
    let (out, _) = client
        .call::<PreNodeReadyPull>(
            &rdm_transport::NodeTarget::ExactNode(root.clone()),
            &PullRequest::Pull,
            &rdm_transport::CallOptions::default(),
        )
        .await;
    let Some(reply) = out.reply() else {
        panic!("snapshot: {out:?}")
    };
    let PullReply::Rows { rows } = reply.value() else {
        panic!("{reply:?}")
    };
    rows.clone()
}
async fn broker(
    root: &Running,
    dir: &std::path::Path,
    shared: &std::path::Path,
) -> rdm_node::node::RunningNode {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    let fact =
        rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("demo-app")).unwrap();
    fact.write_record(dir).unwrap();
    let key = rdm_node::node::load_or_mint_key(dir).unwrap();
    let launch = rdm_core::launch::Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: config(shared).fabric,
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
        data_dir: dir.into(),
        store_dir: shared.into(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    };
    let _ = root.runner.joins.expect(rdm_admin::join::Deployed { listeners: Vec::new(),
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rdm_core::EndpointId(key.public().to_string()),
        runtime: fact,
        data_dir: dir.display().to_string(),
        last_accepted_name: None,
        held: false,
    });
    rdm_node::node::start_with_rafka_time_pre_node_ready_and(
        &launch,
        rdm_transport::clock::RafkaTime::unadopted(),
        rdm_node::node_hooks::NodeHooks::none(),
        |s| s,
        None,
        |b, _| b,
        &[KIND, LARGE],
    )
    .await
    .unwrap()
}

#[test]
fn a_pinned_consumer_reconciles_live_and_pull_heals_both_loss_shapes() {
    let dir = std::env::temp_dir().join(format!("demo-app-{}", NodeId::mint()));
    let capture =
        rdm_transport::EvidenceCapture::start(&dir.join("evidence"), "demo-app-channels").unwrap();
    capture.run(4, async {
        let snapshot = Arc::new(Mutex::new(vec![Row {
            version: 100,
            value: "1:snapshot".into(),
        }]));
        let (entered, mut entered_rx) = tokio::sync::watch::channel(0u64);
        let (release, _) = tokio::sync::watch::channel(false);
        let serve = {
            let snapshot = snapshot.clone();
            let release = release.clone();
            Box::new(move |b: rdm_transport::ServerBuilder| {
                b.serve::<PreNodeReadyPull, _, _>(OpOwner::Testkit, move |_, _| {
                    let rows = snapshot.clone();
                    let release = release.clone();
                    let entered = entered.clone();
                    async move {
                        entered.send_modify(|n| *n += 1);
                        let _ = release.subscribe().wait_for(|v| *v).await;
                        Ok(PullReply::Rows {
                            rows: rows.lock().unwrap().clone(),
                        })
                    }
                })
            }) as rdm_admin::wiring::ServeApp
        };
        let root_dir = dir.join("admin");
        let root = start_with(
            config(&root_dir),
            Wiring {
                serve_app: Some(serve),
                fabric_hooks: FabricHooks::no_app_work(),
                ..Wiring::no_certs()
            },
        )
        .await
        .unwrap();
        root.stop_reconciling().await;
        assert!(root.topology_view.held_types().is_empty());
        let node = broker(&root, &dir.join("broker"), &root_dir).await;
        assert!(root.topology_view.app_types().is_empty());
        assert_eq!(
            root.topology_view.held_types(),
            [KIND.to_string(), LARGE.to_string()].into()
        );
        assert_eq!(node.topology_view.held_types(), node.topology_view.app_types());
        let sender = node.topology_view.join_channel(KIND).await.unwrap();
        let mut receiver = root.topology_view.join_channel(KIND).await.unwrap();
        let large_sender = node.topology_view.join_channel(LARGE).await.unwrap();
        let mut large_receiver = root.topology_view.join_channel(LARGE).await.unwrap();
        // Observe actual per-channel neighbour establishment; elapsed boot time proves nothing.
        tokio::time::timeout(BOUND, async {
            loop {
                let spans = capture.spans().unwrap();
                if spans
                    .iter()
                    .filter(|s| {
                        s["name"] == "rdm.mesh.channel.update.via-neighbour"
                            && s["attributes"]["event_type"] == KIND
                    })
                    .count()
                    >= 2
                    && spans
                        .iter()
                        .filter(|s| {
                            s["name"] == "rdm.mesh.channel.update.via-neighbour"
                                && s["attributes"]["event_type"] == LARGE
                        })
                        .count()
                        >= 2
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let root_id = root.birth().node_id;
        // A real directed pull is in flight when the lower-sequence write of another order arrives.
        let client = node.node_rpc.client.clone();
        let target = root_id.clone();
        let pending = tokio::spawn(async move { pull(&client, &target).await });
        tokio::time::timeout(BOUND, entered_rx.changed())
            .await
            .unwrap()
            .unwrap();
        let live = Row {
            version: 1,
            value: "2:live".into(),
        };
        sender.publish(event(&live)).await.unwrap();
        let mut held = None;
        apply(&mut held, recv(&mut receiver).await);
        release.send_replace(true);
        for row in pending.await.unwrap() {
            apply(&mut held, row);
        }
        assert_eq!(
            held,
            Some(live.clone()),
            "one comparator reconciles pull and live; order beats the higher sequence"
        );
        let mut reversed = None;
        for row in snapshot.lock().unwrap().clone() {
            apply(&mut reversed, row);
        }
        apply(&mut reversed, live);
        assert_eq!(reversed, held);
        // The incomplete-assembly shape runs on a quiet edge, before the burst: the burst leaves about
        // two thousand pushes in the publisher's per-peer gossip send queue (SEND_QUEUE_CAP 64), a
        // push into that full queue is dropped at the publisher, and on this one-edge topic no other
        // peer can IHave it back (gossip.md §7 repairs through the mesh, §8's pull is the recovery):
        // a record published into the draining queue would be lost for a reason the shape does not
        // measure (the :313 reds of 2026-10-19, read in i143-prd.md).
        // The testkit drops one incoming part; publishing, assembly expiry and recovery stay public.
        rdm_test::gossip_faults::drop_chunk_once(&root.topology_view, LARGE, 1)
            .await
            .unwrap();
        let mut incomplete = large_receiver.lost();
        let generation = *incomplete.borrow();
        large_sender
            .publish(event(&Row {
                version: 2,
                value: format!("3:{}", "x".repeat(9000)),
            }))
            .await
            .unwrap();
        tokio::time::timeout(
            rdm_core::cadence::staleness_floor() + Duration::from_secs(2),
            incomplete.wait_for(|n| *n > generation),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            large_receiver.dropped(),
            0,
            "the loss is assembly expiry, not queue overflow"
        );
        let repaired = Row {
            version: 1,
            value: "3:after-incomplete".into(),
        };
        *snapshot.lock().unwrap() = vec![repaired.clone()];
        for row in pull(&node.node_rpc.client, &root_id).await {
            apply(&mut held, row);
        }
        assert_eq!(held, Some(repaired));
        let complete = Row {
            version: 2,
            value: format!("3:{}", "y".repeat(9000)),
        };
        large_sender.publish(event(&complete)).await.unwrap();
        assert_eq!(
            recv(&mut large_receiver).await,
            complete,
            "the one-shot cut removed itself"
        );
        let mut lost = receiver.lost();
        let before = *lost.borrow();
        for version in 2..4098 {
            sender
                .publish(event(&Row {
                    version,
                    value: "2:burst".into(),
                }))
                .await
                .unwrap();
        }
        tokio::time::timeout(BOUND, lost.wait_for(|n| *n > before))
            .await
            .unwrap()
            .unwrap();
        assert!(receiver.dropped() > 0);
        let healed = Row {
            version: 1,
            value: "4:after-overflow".into(),
        };
        *snapshot.lock().unwrap() = vec![healed.clone()];
        tokio::time::timeout(BOUND, async {
            loop {
                let generation = *lost.borrow_and_update();
                for row in pull(&node.node_rpc.client, &root_id).await {
                    apply(&mut held, row);
                }
                if generation == *lost.borrow() {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(held, Some(healed));
        drop(sender);
        drop(receiver);
        drop(large_sender);
        drop(large_receiver);
        node.stop().await;
        root.leave().await;
        let spans = capture.spans().unwrap();
        assert!(spans
            .iter()
            .any(|s| s["name"] == "rdm.mesh.topology.reject.via-chunk-incomplete"));
        assert!(spans
            .iter()
            .any(|s| s["name"] == "rdm.mesh.channel.reject.via-queue-full"));
    });
}
