//! i143.e6.s11 acceptance (rafka-v2 #2899, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2899-process`, which exports `I143_ACCEPTANCE_DIR` (this
//! cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest, rpc ledger and every process's spans land under it) at the test cadence (staleness
//! 3 s, gossip 500 ms).
//!
//! CONTRACT: on a real fabric, the heartbeats a node publishes are ordered by `digest_seq` alone.
//! An observer on the mesh channel decodes the real frames: one birth's `digest_seq` rises with
//! every heartbeat it hears, each stamped with Rafka-time from the clock the binary composed (the
//! OS clock for an RDM executable), and a `MeshTopology` publication is stamped the same way. Then the
//! observer broadcasts a heartbeat of that very birth that is OLDER by sequence but stamped a year
//! ahead and saying `Joining` (a heartbeat carries liveness, never a status, gossip.md §3.1): the
//! node-admin that hears it holds the member as it was, because
//! time never ranks a heartbeat, and the live heartbeats that follow (the healthy control) keep
//! the member heard well past the silence window, so a stamp that runs ahead of the clock never
//! makes a live member look silent or `Dead`. Silence is the receiver's own monotonic age.

use bytes::Bytes;
use futures_lite::StreamExt as _;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::api::{Event, GossipSender};
use rdm_core::{FabricId, MeshDigest, MeshId, NodeState};
use rdm_transport::topology::{backbone_channel, learn_addresses, mesh_channel, Frame};
use rdm_client::{NodeStatus, NodeView};
use rdm_test::estate::{named, Estate, Owner};
use rdm_test::typed::until_arrival;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CELL: &str = "mesh_composition_uses_supplied_clock_keeps_silence_monotonic";
const NODE: &str = "mesh1.rpc.1";

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2899".into(),
        subfeature: "digest-seq".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: CELL.into(),
    }
}

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2899/process").join(CELL),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn os_now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// A frame an observer decoded, with the OS time it arrived at.
#[derive(Clone)]
struct Seen {
    heard_at_ms: u64,
    channel: &'static str,
    frame: Frame,
}

/// A member of the backbone and one mesh channel: it records every frame it decodes and can
/// broadcast one frame on the mesh channel.
struct Observer {
    author: rdm_transport::envelope::Birth,
    endpoint: Endpoint,
    _router: iroh::protocol::Router,
    seen: Arc<Mutex<Vec<Seen>>>,
    /// Woken by every frame the observer records.
    arrivals: Arc<tokio::sync::Notify>,
    mesh_sender: GossipSender,
}

impl Observer {
    async fn join(fabric: &FabricId, mesh_id: &MeshId, seeds: Vec<EndpointAddr>) -> Self {
        let transport = iroh::endpoint::QuicTransportConfig::builder().keep_alive_interval(Duration::from_secs(1)).max_idle_timeout(Some(Duration::from_secs(3).try_into().unwrap())).build();
        let endpoint = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
        learn_addresses(&endpoint, &seeds).unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        let router = iroh::protocol::Router::builder(endpoint.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let arrivals = Arc::new(tokio::sync::Notify::new());
        let peers: Vec<iroh::EndpointId> = seeds.iter().map(|a| a.id).collect();
        let mut mesh_sender = None;
        for (channel, topic) in [("backbone", backbone_channel(fabric)), ("mesh1", mesh_channel(fabric, mesh_id))] {
            let (sender, mut receiver) = gossip.subscribe(topic, peers.clone()).await.unwrap().split();
            if channel == "mesh1" {
                mesh_sender = Some(sender.clone());
            }
            let (seen, arrivals) = (seen.clone(), arrivals.clone());
            tokio::spawn(async move {
                let _keep = sender;
                while let Some(ev) = receiver.next().await {
                    if let Ok(Event::Received(m)) = ev {
                        if let Ok(frame) = Frame::decode(&Bytes::copy_from_slice(&m.content)) {
                            seen.lock().unwrap().push(Seen { heard_at_ms: os_now_ms(), channel, frame });
                            arrivals.notify_waiters();
                        }
                    }
                }
            });
        }
        Self { author: crate::gossip_author(fabric), endpoint, _router: router, seen, arrivals, mesh_sender: mesh_sender.unwrap() }
    }

    fn all(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// The digests of `node` heard on the mesh channel, in arrival order.
    fn heartbeats_of(&self, node: &str) -> Vec<(Seen, MeshDigest)> {
        self.all()
            .into_iter()
            .filter(|x| x.channel == "mesh1")
            .filter_map(|x| match x.frame.data() {
                Frame::Digest { digest } if digest.node.name.to_string() == node => Some((x.clone(), digest.clone())),
                _ => None,
            })
            .collect()
    }

    async fn broadcast(&self, f: &Frame) {
        self.mesh_sender.broadcast(Bytes::from(f.clone().authored(&self.author, 0, 1).encode())).await.unwrap();
    }
}

fn seeds_of(nodes: &[NodeView]) -> Vec<EndpointAddr> {
    nodes
        .iter()
        .filter(|n| n.kind == rdm_core::NodeKind::NodeAdmin)
        .filter_map(|n| {
            let key = n.endpoint_id.as_deref()?.parse::<iroh::PublicKey>().ok()?;
            Some(EndpointAddr::new(key).with_ip_addr(n.transport_addr?))
        })
        .collect()
}

fn row(h: &(Seen, MeshDigest)) -> Value {
    json!({ "heard_at_ms": h.0.heard_at_ms, "digest_seq": h.1.digest_seq, "emitted_at_rafka_ms": h.1.emitted_at_rafka_ms, "node_state": format!("{:?}", h.1.node_state), "incarnation": h.1.node.incarnation.as_str() })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mesh_composition_uses_supplied_clock_keeps_silence_monotonic() {
    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;

    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1)]).await;
    let nodes = estate.settled_views(&[("mesh1", 1, 1)]).await;
    let fabric_view = estate.fabric_view().await;
    let fabric = fabric_view.id.clone();
    let mesh1_id = fabric_view.meshes.iter().find(|m| m.name == "mesh1").and_then(|m| m.id.clone()).expect("the fabric view names mesh1's id");

    // The real frames: several heartbeats of the rpc node's one birth, and a MeshTopology publication.
    let observer = Observer::join(&fabric, &mesh1_id, seeds_of(&nodes)).await;
    until_arrival("the observer decodes six heartbeats of the rpc node and a MeshTopology publication", Duration::from_secs(60), &observer.arrivals, || {
        let members = observer.all().iter().any(|x| x.channel == "backbone" && matches!(x.frame.data(), Frame::MeshTopology { .. }));
        (observer.heartbeats_of(NODE).len() >= 6 && members).then_some(())
    })
    .await;
    let before = observer.heartbeats_of(NODE);
    let birth = before[0].1.node.incarnation.clone();
    assert!(before.iter().all(|h| h.1.node.incarnation == birth), "one birth in the window");
    let seqs: Vec<u64> = before.iter().map(|h| h.1.digest_seq).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "one birth's digest_seq rises with every heartbeat: {seqs:?}");
    assert!(seqs[0] >= 1, "a birth's sequence starts at 1: {seqs:?}");
    // The supplied clock is the OS clock for an RDM executable: each stamp is Rafka-time near the
    // moment the frame was heard.
    for h in &before {
        let skew = h.0.heard_at_ms.abs_diff(h.1.emitted_at_rafka_ms);
        assert!(skew < 5_000, "emitted_at_rafka_ms is the composed clock's reading, within 5 s of when the frame was heard: {:?}", row(h));
    }
    let mesh_topology_published: Vec<u64> = observer
        .all()
        .iter()
        .filter_map(|x| match x.frame.data() {
            Frame::MeshTopology { published_at_rafka_ms, .. } if x.channel == "backbone" => Some(x.heard_at_ms.abs_diff(*published_at_rafka_ms)),
            _ => None,
        })
        .collect();
    assert!(!mesh_topology_published.is_empty() && mesh_topology_published.iter().all(|skew| *skew < 5_000), "published_at_rafka_ms is Rafka-time from the composed clock: skews {mesh_topology_published:?}");

    // The adversary: the very birth, an OLDER sequence, a stamp a year ahead, saying Leaving.
    let held_before = estate.node_view(NODE).await;
    assert_eq!(held_before.status, NodeStatus::Ready);
    let last = before.last().unwrap().1.clone();
    let mut forged = last.clone();
    // A heartbeat carries liveness and no status (gossip.md §3.1 Digest row): the forged one says the
    // birth is not self-ready.
    forged.node_state = NodeState::Joining;
    forged.digest_seq = seqs[0];
    forged.emitted_at_rafka_ms = os_now_ms() + 365 * 24 * 3600 * 1000;
    assert!(forged.digest_seq < last.digest_seq && forged.emitted_at_rafka_ms > last.emitted_at_rafka_ms);
    let forged_at_ms = os_now_ms();
    observer.broadcast(&Frame::Digest { digest: forged.clone() }).await;
    // Well past the silence window (3 s) and many heartbeats (500 ms): the live heartbeats are the control.
    tokio::time::sleep(Duration::from_secs(9)).await;
    let held_after = estate.node_view(NODE).await;
    assert_eq!(held_after.status, NodeStatus::Ready, "the older heartbeat stamped ahead did not move the member, and its live heartbeats kept it heard past the silence window: {held_after:?}");
    assert_eq!(held_after.incarnation_id, held_before.incarnation_id, "the same birth");
    let live_after: Vec<(Seen, MeshDigest)> = observer.heartbeats_of(NODE).into_iter().filter(|h| h.0.heard_at_ms > forged_at_ms && h.1.node_state == NodeState::Alive).collect();
    assert!(live_after.len() >= 8, "the node kept publishing live heartbeats for the whole window ({} heard)", live_after.len());
    assert!(live_after.iter().all(|h| h.1.digest_seq > last.digest_seq), "every live heartbeat after the forged one carries a higher sequence than the last one before it");
    let silent_ms = live_after.windows(2).map(|w| w[1].0.heard_at_ms - w[0].0.heard_at_ms).max().unwrap_or(0);
    assert!(silent_ms < 3_000, "the node was never silent for the silence window: longest gap between its heartbeats {silent_ms} ms");

    estate.stop().await;
    let spans = estate.spans();
    let subscribed: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-subscribe").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == NODE).collect();
    assert!(!subscribed.is_empty(), "the rpc node's real topology view subscribed ({} spans read)", spans.len());
    let result = json!({
        "cell": CELL,
        "observer_heartbeats_before_forgery": before.iter().map(row).collect::<Vec<_>>(),
        "digest_seq_rises": seqs,
        "mesh_topology_publication_skews_ms": mesh_topology_published,
        "forged": { "digest_seq": forged.digest_seq, "emitted_at_rafka_ms": forged.emitted_at_rafka_ms, "node_state": "Joining", "last_real_digest_seq": last.digest_seq, "last_real_emitted_at_rafka_ms": last.emitted_at_rafka_ms },
        "member_before": { "status": held_before.status, "incarnation_id": held_before.incarnation_id },
        "member_after_9s": { "status": held_after.status, "incarnation_id": held_after.incarnation_id },
        "live_heartbeats_after_forgery": live_after.len(),
        "live_first_seq_after_forgery": live_after.first().map(|h| h.1.digest_seq),
        "longest_gap_between_live_heartbeats_ms": silent_ms,
        "subscribe_spans": subscribed.iter().map(|sp| json!({ "node": sp["attributes"]["node"], "trace_id": sp["trace_id"], "span_id": sp["span_id"] })).collect::<Vec<_>>(),
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    observer.endpoint.close().await;
}
