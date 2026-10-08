//! i143.e6.s13 acceptance (rafka-v2 #2901, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2901-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RAFKA_ARTIFACTS_DIR` (the
//! estate's manifest, rpc ledger and every process's spans land under it).
//!
//! CONTRACT: on a real fabric, an observer that joined the backbone and a mesh channel before a
//! status change decodes the frames the fabric puts there. A quiet fabric carries heartbeats and
//! aggregates and no status frame at all. A new mesh's primary publishes its mesh's status as its
//! own frame exactly five times, one per second, all at one instant, and then nothing. A member
//! that joins afterwards is told the held statuses by their holder, with the original publisher and
//! instant, and then hears no status frame. The topology change is a Build through the node-admin.

use bytes::Bytes;
use futures_lite::StreamExt as _;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::api::Event;
use rafka_mesh_entity::{FabricId, MeshId};
use rafka_mesh_transport::membership::{backbone_topic, learn_addresses, mesh_topic, Frame};
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const CELL: &str = "late_member_receives_held_status_on_join_without_status_heartbeat";

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2901".into(),
        subfeature: "status-frames".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: CELL.into(),
    }
}

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2901/process").join(CELL),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// One decoded frame, as an observer heard it.
#[derive(Clone)]
struct Seen {
    at: Instant,
    channel: &'static str,
    frame: Frame,
}

/// A passive gossip member of the backbone and one mesh channel: it broadcasts nothing and
/// records every frame it decodes.
struct Observer {
    endpoint: Endpoint,
    seen: Arc<Mutex<Vec<Seen>>>,
    started: Instant,
}

impl Observer {
    async fn join(fabric: &FabricId, mesh_id: &MeshId, seeds: Vec<EndpointAddr>) -> Self {
        let transport = iroh::endpoint::QuicTransportConfig::builder().keep_alive_interval(Duration::from_secs(1)).max_idle_timeout(Some(Duration::from_secs(3).try_into().unwrap())).build();
        let endpoint = rafka_node_rpc::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
        learn_addresses(&endpoint, &seeds).unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let peers: Vec<iroh::EndpointId> = seeds.iter().map(|a| a.id).collect();
        for (channel, topic) in [("backbone", backbone_topic(fabric)), ("mesh1", mesh_topic(fabric, mesh_id))] {
            let (_sender, mut receiver) = gossip.subscribe(topic, peers.clone()).await.unwrap().split();
            let seen = seen.clone();
            tokio::spawn(async move {
                let _keep = _sender;
                while let Some(ev) = receiver.next().await {
                    if let Ok(Event::Received(m)) = ev {
                        if let Some(frame) = Frame::decode(&Bytes::copy_from_slice(&m.content)) {
                            seen.lock().unwrap().push(Seen { at: Instant::now(), channel, frame });
                        }
                    }
                }
            });
        }
        Self { endpoint, seen, started: Instant::now() }
    }

    fn all(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    fn statuses(&self) -> Vec<Seen> {
        self.all().into_iter().filter(|x| is_status(&x.frame)).collect()
    }
}

fn is_status(f: &Frame) -> bool {
    matches!(f, Frame::MeshStatus { .. } | Frame::FabricStatus { .. })
}

/// (kind, scope, status, publisher, forwarded_by, instant)
fn parts(f: &Frame) -> (&'static str, String, String, String, Option<String>, u64) {
    match f {
        Frame::MeshStatus { mesh, status, publisher, forwarded_by, published_at_rafka_ms } => ("mesh-status", mesh.clone(), status.clone(), publisher.clone(), forwarded_by.clone(), *published_at_rafka_ms),
        Frame::FabricStatus { fabric, status, publisher, forwarded_by, published_at_rafka_ms } => ("fabric-status", fabric.to_string(), status.clone(), publisher.clone(), forwarded_by.clone(), *published_at_rafka_ms),
        _ => ("other", String::new(), String::new(), String::new(), None, 0),
    }
}

fn row(o: &Observer, x: &Seen) -> Value {
    let (kind, scope, status, publisher, forwarded_by, at) = parts(&x.frame);
    json!({ "at_ms": x.at.duration_since(o.started).as_millis() as u64, "channel": x.channel, "frame": kind, "scope": scope, "status": status, "publisher": publisher, "forwarded_by": forwarded_by, "published_at_rafka_ms": at })
}

fn seeds_of(nodes: &[Value]) -> Vec<EndpointAddr> {
    nodes
        .iter()
        .filter(|n| n["kind"] == "node_admin")
        .filter_map(|n| {
            let key = s(&n["endpoint_id"]).parse::<iroh::PublicKey>().ok()?;
            let addr = s(&n["transport_addr"]).parse::<std::net::SocketAddr>().ok()?;
            Some(EndpointAddr::new(key).with_ip_addr(addr))
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_member_receives_held_status_on_join_without_status_heartbeat() {
    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;

    // The fabric: mesh1, shaped by a Build through its node-admin.
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 1}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let nodes = estate.settled_shape(&[("mesh1", 1, 1)], Duration::from_secs(30)).await;
    let (_, fabric_view) = estate.get("/api/fabric").await;
    let fabric = FabricId::parse(&s(&fabric_view["id"])).expect("the fabric id");
    let mesh1_id = MeshId::parse(&s(&fabric_view["meshes"].as_array().unwrap().iter().find(|m| m["name"] == "mesh1").expect("mesh1")["id"])).expect("the mesh1 id");

    // The first observer joins before the change.
    let early = Observer::join(&fabric, &mesh1_id, seeds_of(&nodes)).await;
    wait_for("the early observer hears the backbone's aggregate and mesh1's heartbeat", Duration::from_secs(30), || async {
        let all = early.all();
        let members = all.iter().any(|x| x.channel == "backbone" && matches!(x.frame, Frame::Members { .. }));
        let digest = all.iter().any(|x| x.channel == "mesh1" && matches!(x.frame, Frame::Digest { .. }));
        (members && digest).then_some(())
    })
    .await;

    // The change: a Build adds mesh2. Its first (and only) admin becomes mesh2's primary and
    // publishes mesh2's status. The Build is the node-admin's rectifier; nothing is killed.
    let change_build_at = Instant::now();
    let shape = json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 1}, {"name": "mesh2", "node_admin": 1, "rpc_node": 1}]});
    let (status, a) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    estate.settled_shape(&[("mesh1", 1, 1), ("mesh2", 1, 1)], Duration::from_secs(60)).await;

    // The change's frames: mesh2's status, authored by mesh2's admin, on the backbone.
    let authored = |o: &Observer| -> Vec<Seen> {
        o.statuses().into_iter().filter(|x| x.channel == "backbone" && matches!(&x.frame, Frame::MeshStatus { mesh, publisher, forwarded_by: None, .. } if mesh == "mesh2" && publisher == "mesh2.admin.1")).collect()
    };
    wait_for("the observer heard the fifth send of mesh2's status", Duration::from_secs(30), || async { (authored(&early).len() >= 5).then_some(()) }).await;
    let sends = authored(&early);
    // No further send arrives: the fifth is the last.
    let last_at = sends.iter().map(|x| x.at).max().unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;
    let sends_after = authored(&early);
    assert_eq!(sends_after.len(), 5, "mesh2's status was sent exactly five times, then nothing: {:#?}", sends_after.iter().map(|x| row(&early, x)).collect::<Vec<_>>());
    let instants: Vec<u64> = sends_after.iter().map(|x| parts(&x.frame).5).collect();
    assert!(instants.iter().all(|i| *i == instants[0]), "all five sends carry the one instant of the change: {instants:?}");
    assert!(sends_after.iter().all(|x| parts(&x.frame).2 == "ready-for-traffic"), "the status the five sends carry");
    let gaps_ms: Vec<u64> = sends_after.windows(2).map(|w| w[1].at.duration_since(w[0].at).as_millis() as u64).collect();
    assert!(gaps_ms.iter().all(|g| (800..=1500).contains(g)), "one send per second: {gaps_ms:?}");
    assert!(last_at.duration_since(change_build_at) > Duration::from_secs(4));

    // The quiet window: nothing changes, the fabric primary stays the same, and no status frame
    // crosses either channel while the heartbeat and the aggregate keep going.
    let primary_before = estate.get("/api/fabric").await.1["fabric_primary"].clone();
    let window_start = Instant::now();
    let before = early.all().len();
    tokio::time::sleep(Duration::from_secs(10)).await;
    let window: Vec<Seen> = early.all().into_iter().filter(|x| x.at >= window_start).collect();
    let primary_after = estate.get("/api/fabric").await.1["fabric_primary"].clone();
    assert_eq!(primary_before, primary_after, "no seat moved during the quiet window");
    let status_in_window: Vec<&Seen> = window.iter().filter(|x| is_status(&x.frame)).collect();
    assert!(status_in_window.is_empty(), "a quiet fabric carries no status frame: {:#?}", status_in_window.iter().map(|x| row(&early, x)).collect::<Vec<_>>());
    let members_in_window = window.iter().filter(|x| x.channel == "backbone" && matches!(x.frame, Frame::Members { .. })).count();
    let digests_in_window = window.iter().filter(|x| x.channel == "mesh1" && matches!(x.frame, Frame::Digest { .. })).count();
    assert!(members_in_window > 0 && digests_in_window > 0, "the aggregate ({members_in_window}) and the heartbeat ({digests_in_window}) keep going");
    assert!(early.all().len() > before);
    // No Members frame carries a status: the frame has no such field.
    for x in early.all().iter().filter(|x| matches!(x.frame, Frame::Members { .. })) {
        let v: Value = serde_json::from_slice(&x.frame.encode()).unwrap();
        assert!(v.get("status").is_none() && v.get("mesh_status").is_none() && v.get("fabric_status").is_none(), "Members carries no status: {v}");
    }

    // The late member joins now, with everything already said and the five sends long over.
    let held_at: std::collections::BTreeMap<String, u64> = std::iter::once(("mesh2".to_string(), instants[0])).collect();
    let late_nodes = estate.nodes().await;
    let late = Observer::join(&fabric, &mesh1_id, seeds_of(&late_nodes)).await;
    wait_for("the late member is told the held statuses of mesh1, mesh2 and the fabric", Duration::from_secs(30), || async {
        let st = late.statuses();
        let has = |want: &str| st.iter().any(|x| { let p = parts(&x.frame); p.0 == want });
        let mesh = |m: &str| st.iter().any(|x| { let p = parts(&x.frame); p.0 == "mesh-status" && p.1 == m });
        (mesh("mesh1") && mesh("mesh2") && has("fabric-status")).then_some(())
    })
    .await;
    let told_at = Instant::now();
    let told: Vec<Seen> = late.statuses();
    let told_mesh2: Vec<&Seen> = told.iter().filter(|x| matches!(&x.frame, Frame::MeshStatus { mesh, .. } if mesh == "mesh2")).collect();
    for x in &told_mesh2 {
        let p = parts(&x.frame);
        assert_eq!(p.3, "mesh2.admin.1", "the replay keeps mesh2's primary as publisher: {:?}", row(&late, x));
        assert_eq!(p.5, held_at["mesh2"], "the replay carries the original instant of the change");
        assert_eq!(p.2, "ready-for-traffic");
        assert!(p.4.as_deref() != Some("mesh2.admin.1"), "a holder never names the author as its forwarder");
    }
    for x in &told {
        let p = parts(&x.frame);
        assert!(p.4.is_none() || p.4.as_deref() != Some(p.3.as_str()), "no replay restates a status as its own: {:?}", row(&late, x));
    }
    // And then no status heartbeat reaches the late member.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let after_told: Vec<Seen> = late.statuses().into_iter().filter(|x| x.at > told_at + Duration::from_secs(1)).collect();
    assert!(after_told.is_empty(), "the late member hears no status frame after its join replay: {:#?}", after_told.iter().map(|x| row(&late, x)).collect::<Vec<_>>());

    estate.stop().await;
    let spans = estate.spans();
    let roles: Vec<&Value> = named(&spans, "rdm.mesh.fabric.update.via-status-publisher");
    let early_rows: Vec<Value> = early.statuses().iter().map(|x| row(&early, x)).collect();
    let late_rows: Vec<Value> = late.statuses().iter().map(|x| row(&late, x)).collect();
    let result = json!({
        "cell": CELL,
        "fabric_id": fabric.to_string(),
        "mesh1_id": mesh1_id.to_string(),
        "early_observer_status_frames": early_rows,
        "mesh2_status_sends": { "count": sends_after.len(), "gaps_ms": gaps_ms, "published_at_rafka_ms": instants, "publisher": "mesh2.admin.1" },
        "quiet_window": {
            "seconds": 10,
            "status_frames": status_in_window.len(),
            "backbone_members_frames": members_in_window,
            "mesh_channel_digest_frames": digests_in_window,
            "fabric_primary": primary_after,
        },
        "late_member_status_frames": late_rows,
        "late_member_status_frames_after_join": after_told.len(),
        "status_publisher_role_spans": roles.iter().map(|sp| json!({ "node": sp["attributes"]["node"], "role": sp["attributes"]["role"], "trace_id": sp["trace_id"], "span_id": sp["span_id"] })).collect::<Vec<_>>(),
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    early.endpoint.close().await;
    late.endpoint.close().await;
}
