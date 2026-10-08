//! i143.e6.s13 acceptance (rafka-v2 #2901, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2901-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the
//! estate's manifest, rpc ledger and every process's spans land under it).
//!
//! CONTRACT: on a real fabric, an observer that joined the backbone and a mesh channel before a
//! status change decodes the frames the fabric puts there. A quiet fabric carries heartbeats and
//! aggregates and no status frame at all. A new mesh's primary sends its mesh's status five times,
//! one per second, as the SAME message (one `changed_at_rafka_ms`), and then nothing: the five
//! sends are proven at the SENDER by its `via-status-send` span for each send, never by counting
//! frames at an observer, which already holds the first copy and drops the identical rest. A member
//! that joins while the sends are running holds the status once, with the original publisher and
//! change instant. A member that joins afterwards is told the held statuses by their holder, with
//! the original publisher and instant, and then hears no status frame. The topology change is a
//! Build through the node-admin.

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
    _router: iroh::protocol::Router,
    seen: Arc<Mutex<Vec<Seen>>>,
    started: Instant,
}

impl Observer {
    async fn join(fabric: &FabricId, mesh_id: &MeshId, seeds: Vec<EndpointAddr>) -> Self {
        let transport = iroh::endpoint::QuicTransportConfig::builder().keep_alive_interval(Duration::from_secs(1)).max_idle_timeout(Some(Duration::from_secs(3).try_into().unwrap())).build();
        let endpoint = rafka_node_rpc::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
        learn_addresses(&endpoint, &seeds).unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        let router = iroh::protocol::Router::builder(endpoint.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let peers: Vec<iroh::EndpointId> = seeds.iter().map(|a| a.id).collect();
        for (channel, topic) in [("backbone", backbone_topic(fabric)), ("mesh1", mesh_topic(fabric, mesh_id))] {
            let (_sender, mut receiver) = gossip.subscribe(topic, peers.clone()).await.unwrap().split();
            let seen = seen.clone();
            tokio::spawn(async move {
                let _keep = _sender;
                while let Some(ev) = receiver.next().await {
                    if let Ok(Event::Received(m)) = ev {
                        if let Ok(frame) = Frame::decode(&Bytes::copy_from_slice(&m.content)) {
                            seen.lock().unwrap().push(Seen { at: Instant::now(), channel, frame });
                        }
                    }
                }
            });
        }
        Self { endpoint, _router: router, seen, started: Instant::now() }
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
        Frame::MeshStatus { mesh, status, publisher, forwarded_by, changed_at_rafka_ms } => ("mesh-status", mesh.clone(), status.clone(), publisher.clone(), forwarded_by.clone(), *changed_at_rafka_ms),
        Frame::FabricStatus { fabric, status, publisher, forwarded_by, changed_at_rafka_ms } => ("fabric-status", fabric.to_string(), status.clone(), publisher.clone(), forwarded_by.clone(), *changed_at_rafka_ms),
        _ => ("other", String::new(), String::new(), String::new(), None, 0),
    }
}

fn row(o: &Observer, x: &Seen) -> Value {
    let (kind, scope, status, publisher, forwarded_by, at) = parts(&x.frame);
    json!({ "at_ms": x.at.duration_since(o.started).as_millis() as u64, "channel": x.channel, "frame": kind, "scope": scope, "status": status, "publisher": publisher, "forwarded_by": forwarded_by, "changed_at_rafka_ms": at })
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
    let (anchor, anchor_unix_ns) = (Instant::now(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64);

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
    let shape = json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 1}, {"name": "mesh2", "node_admin": 1, "rpc_node": 1}]});
    let (status, a) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    estate.settled_shape(&[("mesh1", 1, 1), ("mesh2", 1, 1)], Duration::from_secs(60)).await;

    // The change's frames: mesh2's status, authored by mesh2's admin, on the backbone.
    let authored = |o: &Observer| -> Vec<Seen> {
        o.statuses().into_iter().filter(|x| x.channel == "backbone" && matches!(&x.frame, Frame::MeshStatus { mesh, publisher, forwarded_by: None, .. } if mesh == "mesh2" && publisher == "mesh2.admin.1")).collect()
    };
    wait_for("the early observer heard mesh2's status", Duration::from_secs(30), || async { (!authored(&early).is_empty()).then_some(()) }).await;
    // A member joins while the sends are still running (the first was just heard; the second is due
    // within a second): it holds the status once.
    let joiner = Observer::join(&fabric, &mesh1_id, seeds_of(&estate.nodes().await)).await;
    let joined_at = Instant::now();
    wait_for("the member that joined mid-window holds mesh2's status", Duration::from_secs(30), || async { (!authored(&joiner).is_empty()).then_some(()) }).await;
    let joiner_heard_at = joiner.statuses().iter().filter(|x| matches!(&x.frame, Frame::MeshStatus { mesh, .. } if mesh == "mesh2")).map(|x| x.at).min().unwrap();
    // Let the five sends finish, then keep watching: nothing more is sent.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let early_copies = authored(&early);
    let early_encoded: std::collections::BTreeSet<Vec<u8>> = early_copies.iter().map(|x| x.frame.encode()).collect();
    assert_eq!(early_encoded.len(), 1, "every copy the early observer holds is the one identical message: {:#?}", early_copies.iter().map(|x| row(&early, x)).collect::<Vec<_>>());
    let instants: Vec<u64> = early_copies.iter().map(|x| parts(&x.frame).5).collect();
    assert_eq!(parts(&early_copies[0].frame).2, "ready-for-traffic", "the status mesh2's primary sent");
    let joiner_copies = authored(&joiner);
    let joiner_encoded: std::collections::BTreeSet<Vec<u8>> = joiner_copies.iter().map(|x| x.frame.encode()).collect();
    assert_eq!(joiner_encoded, early_encoded, "the member that joined mid-window holds the very message the early observer holds");

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
        // The frame's own fields, not its wire bytes (postcard, R-W1).
        let v: Value = serde_json::to_value(&x.frame).unwrap()["Members"].clone();
        assert!(v.is_object(), "a Members frame: {v}");
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
    // The five sends, proven at the SENDER: one `via-status-send` span per send on mesh2's primary,
    // the first at once and the rest one second apart, all carrying the one changed_at_rafka_ms,
    // and no sixth however long the estate ran afterwards.
    let mut sender_sends: Vec<(u64, u64)> = named(&spans, "rdm.mesh.fabric.update.via-status-send")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == "mesh2.admin.1" && sp["attributes"]["scope"] == "mesh:mesh2")
        .map(|sp| (sp["start_unix_nano"].as_u64().unwrap(), s(&sp["attributes"]["changed_at_rafka_ms"]).parse::<u64>().unwrap()))
        .collect();
    sender_sends.sort();
    assert_eq!(sender_sends.len(), 5, "mesh2's primary sent its status exactly five times: {sender_sends:?}");
    assert!(sender_sends.iter().all(|(_, at)| *at == instants[0]), "all five sends are the one message, changed_at_rafka_ms {}: {sender_sends:?}", instants[0]);
    let send_offsets_ms: Vec<u64> = sender_sends.iter().map(|(t, _)| (t - sender_sends[0].0) / 1_000_000).collect();
    for (k, o) in send_offsets_ms.iter().enumerate() {
        assert!((k as u64 * 1_000 - (k as u64).min(1) * 100..k as u64 * 1_000 + 500).contains(o), "send {k} is due {k} s after the first: {send_offsets_ms:?}");
    }
    // Nothing on an unchanged status: no send of any status begins during the quiet window.
    let unix_ns = |t: Instant| -> u64 { anchor_unix_ns + t.duration_since(anchor).as_nanos() as u64 };
    let (quiet_from, quiet_to) = (unix_ns(window_start), unix_ns(window_start + Duration::from_secs(10)));
    let in_quiet: Vec<&Value> = named(&spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| (quiet_from..quiet_to).contains(&sp["start_unix_nano"].as_u64().unwrap())).collect();
    assert!(in_quiet.is_empty(), "no status is sent in the quiet window: {in_quiet:?}");
    // The mid-window joiner against the sender's own record: when it arrived, when the author's
    // NeighborUp for it fired, and when each send was made. Replay and send are the same bytes, so
    // the joiner cannot tell which copy it holds; the sender's record places both.
    let joiner_short = joiner.endpoint.id().fmt_short().to_string();
    let rel_ms = |ns: u64| -> i64 { (ns as i128 - sender_sends[0].0 as i128).div_euclid(1_000_000) as i64 };
    let author_neighbour_up_ms: Vec<i64> = named(&spans, "rdm.mesh.connection.update.via-neighbour-up")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == "mesh2.admin.1" && s(&sp["attributes"]["peer"]) == joiner_short)
        .map(|sp| rel_ms(sp["start_unix_nano"].as_u64().unwrap()))
        .collect();
    let joiner_heard_ms = rel_ms(unix_ns(joiner_heard_at));
    let joiner_joined_ms = rel_ms(unix_ns(joined_at));
    let early_rows: Vec<Value> = early.statuses().iter().map(|x| row(&early, x)).collect();
    let late_rows: Vec<Value> = late.statuses().iter().map(|x| row(&late, x)).collect();
    let result = json!({
        "cell": CELL,
        "fabric_id": fabric.to_string(),
        "mesh1_id": mesh1_id.to_string(),
        "early_observer_status_frames": early_rows,
        "mesh2_status_sends_at_the_sender": { "count": sender_sends.len(), "offsets_ms_from_first": send_offsets_ms, "changed_at_rafka_ms": instants[0], "publisher": "mesh2.admin.1" },
        "early_observer_mesh2_copies_held": early_copies.len(),
        "mid_window_joiner": { "joined_ms_after_first_send": joiner_joined_ms, "first_heard_ms_after_first_send": joiner_heard_ms, "author_neighbour_up_ms_after_first_send": author_neighbour_up_ms, "copies_held": joiner_copies.len(), "same_bytes_as_early_observer": true },
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
    joiner.endpoint.close().await;
}
