//! Application channels on real admins (gossip.md §8, Q362): RDM provides the channels, and an
//! application's nodes, its node-admin included, join the channels its own code names, each join
//! naming its first neighbours from the node's topology view.
use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::node_hooks::{HookOutcome, NodeHooks, NodeReadyCtx, PreNodeReady};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_client::{BuildCarrier, BuildId, Fabrics, MeshDesired, NodeStatus, Nodes, PathName};
use rdm_core::{IncarnationId, NodeId};
use rdm_test::estate::{bin_dir, Estate};
use rdm_test::executables::{Current, Executables};
use rdm_test::originate::{Originate, OriginateReply, OriginateRequest};
use rdm_test::test_app::{app_channel_peers, JoinsAppChannel, APP_CHANNEL, APP_CHANNEL_KNOB};
use rdm_transport::{CallOptions, NodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const MEMBERS: &str = "app.members";
const BOUND: Duration = Duration::from_secs(60);
const WRONG: Duration = Duration::from_secs(120);

/// An application's `pre_node_ready` in this process: it joins [`MEMBERS`] naming the peers the
/// testkit application names by `endpoint_id` (`app_channel_peers`: its mesh-primary from the
/// context, and for a node-admin every other node-admin its view holds), and keeps the channel and
/// the peers it named.
#[derive(Default)]
struct JoinsMembers {
    channel: Mutex<Option<rdm_gossip::gossip::Channel>>,
    named: Mutex<Option<Vec<String>>>,
}
#[async_trait::async_trait]
impl PreNodeReady for JoinsMembers {
    async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome {
        let peers = app_channel_peers(ctx);
        let addrs = peers.iter().map(|p| rdm_gossip::channel::peer(p).expect("a peer's endpoint id parses")).collect();
        ctx.channels.join(rdm_gossip::channel::ChannelId::app(ctx.topology_view.fabric_id(), MEMBERS), addrs).await.expect("the application's channel is joined");
        let channel = ctx.channels.app(MEMBERS).await.expect("the application's channel's receiver");
        *self.named.lock().unwrap() = Some(peers.iter().map(|p| p.0.clone()).collect());
        *self.channel.lock().unwrap() = Some(channel);
        HookOutcome::Ok
    }
}
fn config(dir: &Path) -> AdminConfig {
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.display().to_string()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .unwrap()
}

// @feature: gossip
/// CONTRACT (gossip.md §8, Q362): a node-admin's application and a node's application each join one
/// channel in their `pre_node_ready`, the node naming its mesh-primary from its context
/// (`accepting_primary`) by `endpoint_id` as its first neighbour and the node-admin, the Day-0 root
/// alone in its fabric, naming none. Once the channel's neighbours are up on both sides (a protocol event), an event
/// the node publishes is received by the node-admin's application, sourced from the node's birth.
/// Must NOT happen: a join refused for its name, or the node-admin naming a peer it does not hold.
#[test]
fn a_node_joins_a_channel_naming_its_node_admin_and_the_node_admins_application_receives_its_event() {
    let cell = "a_node_joins_a_channel_naming_its_node_admin_and_the_node_admins_application_receives_its_event";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("app-channel-{}", NodeId::mint()));
        let member_dir = dir.join("member");
        let admin_app = Arc::new(JoinsMembers::default());
        let root = start_with(config(&dir), Wiring { node_hooks: NodeHooks::new(admin_app.clone()), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.unwrap();
        root.stop_reconciling().await;
        tokio::time::timeout(BOUND, root.pre_node_ready().unwrap().settled()).await.unwrap();
        assert_eq!(admin_app.named.lock().unwrap().clone(), Some(Vec::new()), "the node-admin, alone in its mesh, names no peer");

        let d = root.digest.lock().unwrap().clone();
        std::fs::create_dir_all(&member_dir).unwrap();
        let runtime = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("app-channel")).unwrap();
        runtime.write_record(&member_dir).unwrap();
        let key = rdm_test::node::load_or_mint_key(&member_dir).unwrap();
        let launch = rdm_core::launch::Launch { initial_properties: Default::default(), topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: config(&dir).fabric,
            fabric_id: d.fabric_id.clone(),
            mesh: "mesh1".into(),
            r#type: "node".into(),
            path: "mesh1.node".into(), name: "1".into(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
            launcher: Some(rdm_core::launch::Launcher { mesh: d.node.mesh.clone(), path: d.node.path.clone(), name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
            data_dir: member_dir.clone(),
            store_dir: dir.clone(),
            mesh_id: d.mesh_id.clone(),
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
        };
        let _report = root.runner.joins.expect(rdm_admin::join::Deployed { mesh: launch.mesh.clone(), r#type: launch.r#type.clone(),
            path: launch.path.clone(), name: launch.name.clone(),
            node_id: launch.node_id.clone(),
            incarnation: launch.incarnation.clone(),
            supersedes: None,
            endpoint_id: rdm_core::EndpointId(key.public().to_string()),
            runtime,
            data_dir: member_dir.display().to_string(),
            last_accepted_name: None,
            held: false,
            listeners: Vec::new(),
        });
        let member_app = Arc::new(JoinsMembers::default());
        let member = rdm_node::node::start_with_rafka_time_pre_node_ready_alpns_and(&launch, rdm_transport::clock::RafkaTime::unadopted(), NodeHooks::new(member_app.clone()), |s| s, None, |b, _| b, &[])
            .await
            .expect("the member starts");
        assert_eq!(member_app.named.lock().unwrap().clone(), Some(vec![d.node.endpoint_id.0.clone()]), "the node named its mesh-primary, the node-admin that answered its join");

        // The channel's neighbours are up on both sides (a protocol event).
        tokio::time::timeout(BOUND, async {
            while spans.0.lock().unwrap().values().filter(|(n, _, f)| n == "rdm.mesh.channel.update.via-neighbour" && f.get("event_type").map(String::as_str) == Some(MEMBERS)).count() < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the app.members channel's neighbours are up on both sides");
        let (to_admin, _from_admin) = member_app.channel.lock().unwrap().take().expect("the member joined").split();
        let published = rdm_gossip::gossip::Event { subject: "members/1".into(), sequence: Some(1), dataschema: Some("members.v1".into()), data: bytes::Bytes::from_static(b"from the node") };
        to_admin.publish(published).await.expect("the member publishes");

        let mut channel = admin_app.channel.lock().unwrap().take().expect("the node-admin's application joined");
        let got = tokio::time::timeout(Duration::from_secs(10), channel.recv()).await.expect("the node's event is received").expect("the channel is open");
        assert_eq!((got.data.as_ref(), got.sequence), (&b"from the node"[..], Some(1)));
        assert_eq!(got.source.node, launch.node_id, "sourced from the node's birth");
        drop(channel);
        member.stop().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(dir);
    });
}

/// The runtimes the cell's admins launched (every process whose `RDM_STORE_DIR` is the cell's store
/// dir) end when the cell does, a cell that panics included.
struct Launched(PathBuf);

impl Drop for Launched {
    fn drop(&mut self) {
        let want = format!("RDM_STORE_DIR={}", self.0.display());
        for e in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
            let Some(pid) = e.file_name().to_str().and_then(|p| p.parse::<i32>().ok()) else { continue };
            let env = std::fs::read(e.path().join("environ")).unwrap_or_default();
            if env.split(|b| *b == 0).any(|kv| kv == want.as_bytes()) {
                unsafe { libc::kill(pid, libc::SIGKILL) };
            }
        }
    }
}

/// The cell's span files, kept in its artifacts (`<artifacts>/app-channels/<cell>/spans`) when the
/// cell ends, a cell that panics included, before its root is removed.
struct Kept {
    spans: PathBuf,
    to: PathBuf,
}

impl Drop for Kept {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.to);
        let _ = std::fs::create_dir_all(&self.to);
        for e in std::fs::read_dir(&self.spans).into_iter().flatten().flatten() {
            if let Err(why) = std::fs::copy(e.path(), self.to.join(e.file_name())) {
                eprintln!("{} was not kept: {why}", e.path().display());
            }
        }
        eprintln!("spans of this cell: {}", self.to.display());
    }
}

/// Every span line of every span file under `<dir>/spans`, by file.
fn span_files(dir: &Path) -> Vec<Vec<Value>> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir.join("spans")).into_iter().flatten().flatten() {
        let lines = std::fs::read_to_string(e.path()).unwrap_or_default();
        out.push(lines.lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()).collect());
    }
    out
}

/// The spans of the process whose testkit application joined [`APP_CHANNEL`] as `node_id`.
fn spans_of(files: &[Vec<Value>], node_id: &NodeId) -> Vec<Value> {
    let id = node_id.to_string();
    files
        .iter()
        .find(|f| f.iter().any(|s| s["name"] == "rdm.testkit.channel.update.via-join" && s["attributes"]["node_id"] == id.as_str()))
        .cloned()
        .unwrap_or_default()
}

/// `spans`' neighbours on [`APP_CHANNEL`] now: each one up, less each one down.
fn neighbours(spans: &[Value]) -> usize {
    let count = |name: &str| spans.iter().filter(|s| s["name"] == name && s["attributes"]["event_type"] == APP_CHANNEL).count();
    count("rdm.mesh.channel.update.via-neighbour").saturating_sub(count("rdm.mesh.channel.update.via-neighbour-down"))
}


/// The peers `spans`' testkit application named at its join of [`APP_CHANNEL`].
fn named_peers(spans: &[Value]) -> String {
    spans.iter().find(|s| s["name"] == "rdm.testkit.channel.update.via-join").and_then(|s| s["attributes"]["peers"].as_str()).unwrap_or_default().to_string()
}

/// The Day-0 node-admin of the cell's estate, run in this process as an embedding's own node-admin
/// is, its application the testkit's ([`JoinsAppChannel`]), and the objects a caller reaches it by.
struct Day0 {
    running: Running,
    carrier: BuildCarrier,
    nodes: Nodes,
    fabrics: Fabrics,
}

impl Day0 {
    async fn start(dir: &Path) -> Self {
        let (data, store) = (dir.join("admin"), dir.join("store"));
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(&store).unwrap();
        let bins = bin_dir().display().to_string();
        let config = AdminConfig::from_env(|k| match k {
            "RDM_DATA_DIR" => Some(data.display().to_string()),
            "RDM_STORE_DIR" => Some(store.display().to_string()),
            "RDM_BIN_DIR" => Some(bins.clone()),
            "MESH_SPAWN_TYPE" => Some("process".into()),
            // Every node the admins launch writes its spans here (the launch passes it through).
            "RDM_EVIDENCE_DIR" => Some(dir.join("spans").display().to_string()),
            _ => None,
        })
        .expect("a readable environment");
        let wiring = Wiring { node_hooks: NodeHooks::new(JoinsAppChannel::over(None)), certs: rdm_test::test_certs::root_choice(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() };
        let running = start_with(config, wiring).await.expect("the Day-0 admin comes up");
        let me = running.birth();
        let admin = ResolvedNode { mesh: me.mesh.clone(), r#type: me.r#type, node_id: me.node_id.clone(), path: me.path.clone(), name: me.name.clone(), endpoint_id: me.endpoint_id.0.parse().unwrap(), incarnation: me.incarnation.clone(), transport_addr: me.transport_addr };
        let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller's endpoint binds");
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(admin);
        let client = Arc::new(NodeRpcClient::new(endpoint, resolver).with_caller_system("rdm"));
        let carrier = BuildCarrier::new(client, rdm_core::PathName { path: me.path.clone(), name: me.name.clone() });
        let fabrics = Fabrics::new(carrier.clone());
        let day_zero = tokio::time::timeout(WRONG, async {
            loop {
                if let Some(id) = rdm_test::typed::the_fabric(&fabrics).await.ok().and_then(|f| f.build_id) {
                    return id;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the Day-0 admin holds its Day-0 Build as the current one");
        let day_zero = BuildId::parse(&day_zero).unwrap_or_else(|e| panic!("fabric.get names {day_zero}, not a Build id: {e}"));
        let mut stream = tokio::time::timeout(WRONG, async {
            loop {
                match carrier.resubmit(&day_zero).await {
                    Ok(s) => return s,
                    Err(e) if e.to_string().contains("not-fabric-primary") => tokio::time::sleep(Duration::from_millis(100)).await,
                    Err(e) => panic!("the Day-0 admin does not stream its Day-0 Build {day_zero}: {e}"),
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the Day-0 admin never held the fabric-primary to stream its Day-0 Build {day_zero}"));
        Estate::until_complete(&mut stream).await;
        Day0 { running, nodes: Nodes::new(carrier.clone()), fabrics, carrier }
    }

    /// Take the fabric to `meshes`, every node-admin the faulted node-admin and every node the
    /// testkit node (`binding_set`), and follow each Build to its end.
    async fn resize(&self, meshes: &[MeshDesired]) {
        let executables = Executables::Set(rdm_test::faults::binding_set(&rdm_test::faults::candidate_sha()));
        let current = Current::read(&self.carrier).await.unwrap_or_else(|e| panic!("the current topology: {e}"));
        let nodes = current.resized(meshes, true, &executables);
        for change in current.change_to(nodes) {
            let mut stream = self.carrier.create(change).await.unwrap_or_else(|e| panic!("the fabric-primary refused {meshes:?}: {e}"));
            Estate::until_complete(&mut stream).await;
        }
    }

    /// `node` listed ready by the admin's `node.get`.
    async fn until_ready(&self, node: &PathName) {
        tokio::time::timeout(WRONG, async {
            loop {
                let listed = self.nodes.get(&rdm_client::Filter::new().eq("path", [&node.path]).eq("name", [&node.name])).await.unwrap_or_else(|e| panic!("node.get [{node:?}]: {e}"));
                if listed.iter().any(|n| &n.location() == node && n.state == NodeStatus::Ready) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{node:?} never listed ready"));
    }

    /// The fabric's own status ready: a new mesh is created once the fabric first reached it.
    async fn until_fabric_ready(&self) {
        tokio::time::timeout(WRONG, async {
            loop {
                if rdm_test::typed::the_fabric(&self.fabrics).await.is_ok_and(|f| f.status == rdm_client::ScopeStatus::Ready) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the fabric never first reached ready");
    }

    /// The birth `node` holds now, as the Day-0 admin's resolver resolves it.
    async fn resolved(&self, node: &PathName) -> ResolvedNode {
        tokio::time::timeout(WRONG, async {
            loop {
                if let Ok(r) = self.running.node_rpc.resolver.resolve(&NodeTarget::CurrentPath(node.clone())) {
                    return r;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the Day-0 admin never resolved {node:?}"))
    }
}

// @feature: gossip
/// CONTRACT (gossip.md §8, Q362: "A node-admin is one of the application's nodes and joins the
/// channels its code names, as every node does; joined by nodes of two meshes, it carries the channel
/// between them"): in a two-mesh fabric every node and node-admin of the testkit application joins
/// its one channel before Ready, each naming its peers by `endpoint_id`: a node its mesh-primary from
/// its context (`accepting_primary`), a node-admin its own and every other node-admin its view
/// holds. Once each channel's neighbours are up, an event
/// mesh1's node publishes is received by mesh2's node's application, sourced from mesh1's node and in
/// its trace, carried to it by mesh2's node-admin, its one neighbour on the channel at the receipt,
/// which received it too: the neighbours read are those up before mesh2's node's `via-received`, the
/// event's possible carriers, since iroh-gossip adds neighbours later at will (rdm#324). (No node
/// names a peer of another mesh; HyParView's forwarded join may still make mesh2's node-admin a
/// neighbour of mesh1's node.) Must NOT happen: the event missing on mesh2's node, a node naming a
/// peer of another mesh, or mesh2's node with a neighbour other than its node-admin at the receipt.
#[test]
fn an_event_a_mesh1_node_publishes_reaches_a_mesh2_node_through_the_node_admins_its_application_joined() {
    let cell = "an_event_a_mesh1_node_publishes_reaches_a_mesh2_node_through_the_node_admins_its_application_joined";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let dir = std::env::temp_dir().join(format!("{cell}-{}", NodeId::mint()));
    std::fs::create_dir_all(dir.join("spans")).unwrap();
    std::fs::write(dir.join(APP_CHANNEL_KNOB), b"").unwrap();
    let _launched = Launched(dir.join("store"));
    let kept = Kept { spans: dir.join("spans"), to: rdm_test::estate::artifacts_root().join("app-channels").join(cell).join("spans") };
    let capture = rdm_transport::EvidenceCapture::start(&dir.join("spans"), "app-channels-cell").unwrap();
    capture.run(4, async {
        let d = Day0::start(&dir).await;
        let mesh = |m: &str| MeshDesired::of(m, [("node-admin", 1), ("node", 1)]);
        d.resize(&[mesh("mesh1")]).await;
        let m1_node: PathName = PathName::new("mesh1.node", "1").unwrap();
        d.until_ready(&m1_node).await;
        d.until_fabric_ready().await;
        d.resize(&[mesh("mesh1"), mesh("mesh2")]).await;
        let (m2_admin, m2_node): (PathName, PathName) = (PathName::new("mesh2.node-admin", "1").unwrap(), PathName::new("mesh2.node", "1").unwrap());
        d.until_ready(&m2_admin).await;
        d.until_ready(&m2_node).await;
        let day0 = d.running.birth().node_id.clone();
        let (m1n, m2a, m2n) = (d.resolved(&m1_node).await, d.resolved(&m2_admin).await, d.resolved(&m2_node).await);
        let (day0_key, m2a_key) = (d.running.birth().endpoint_id.0.clone(), m2a.endpoint_id.to_string());
        let (m1n, m2a, m2n) = (m1n.node_id, m2a.node_id, m2n.node_id);

        // Each join's first neighbours are up (a protocol event): mesh1's node and mesh2's node each with
        // its own node-admin, each node-admin with its node and the other node-admin.
        let files = tokio::time::timeout(WRONG, async {
            loop {
                capture.spans().expect("the cell's own spans are flushed");
                let files = span_files(&dir);
                if neighbours(&spans_of(&files, &m1n)) >= 1 && neighbours(&spans_of(&files, &m2n)) >= 1 && neighbours(&spans_of(&files, &m2a)) >= 2 && neighbours(&spans_of(&files, &day0)) >= 2 {
                    return files;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            let files = span_files(&dir);
            panic!(
                "the {APP_CHANNEL} neighbours never came up: mesh1.node.1 {} (named {:?}), mesh1.node-admin.1 {} (named {:?}), mesh2.node-admin.1 {} (named {:?}), mesh2.node.1 {} (named {:?})",
                neighbours(&spans_of(&files, &m1n)), named_peers(&spans_of(&files, &m1n)),
                neighbours(&spans_of(&files, &day0)), named_peers(&spans_of(&files, &day0)),
                neighbours(&spans_of(&files, &m2a)), named_peers(&spans_of(&files, &m2a)),
                neighbours(&spans_of(&files, &m2n)), named_peers(&spans_of(&files, &m2n)),
            )
        });
        assert_eq!(named_peers(&spans_of(&files, &m1n)), day0_key, "mesh1's node names its mesh-primary, mesh1.node-admin.1");
        assert_eq!(named_peers(&spans_of(&files, &m2n)), m2a_key, "mesh2's node names its mesh-primary, mesh2.node-admin.1");
        assert!(named_peers(&spans_of(&files, &m2a)).split(',').any(|p| p == day0_key), "mesh2's node-admin names mesh1's node-admin: {:?}", named_peers(&spans_of(&files, &m2a)));

        // mesh1's node publishes once, through its originate service.
        let req = OriginateRequest::Publish { subject: "testkit/app/1".into(), sequence: 1, data: b"from mesh1".to_vec() };
        let opts = CallOptions { send: rdm_transport::Send::with_time_limit(Duration::from_secs(30)), ..CallOptions::default() };
        let (out, _) = d.running.node_rpc.client.call::<Originate>(&NodeTarget::ExactNode(m1n.clone()), &req, &opts).await;
        match out {
            rdm_core::outcome::RpcOutcome::Reply(r) => match r.into_value() {
                OriginateReply::Published { outcome, .. } => assert_eq!(outcome, "handed-to-gossip", "mesh1's node handed its event to its channel's gossip"),
                other => panic!("mesh1.node.1's publish ended {other:?}"),
            },
            other => panic!("the originate service of mesh1.node.1 ended {other:?}"),
        }

        let received = |spans: &[Value]| spans.iter().find(|s| s["name"] == "rdm.testkit.channel.update.via-received" && s["attributes"]["subject"] == "testkit/app/1").cloned();
        let on_m2n = tokio::time::timeout(WRONG, async {
            loop {
                if let Some(s) = received(&spans_of(&span_files(&dir), &m2n)) {
                    return s;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("mesh2's node's application receives the event mesh1's node published");
        assert_eq!(on_m2n["attributes"]["source_node_id"], m1n.to_string().as_str(), "sourced from mesh1's node");
        // gossip.md (Q362): a node's `peers` are its first neighbours on the channel, and a node-admin
        // joined by nodes of two meshes carries the channel between them. iroh-gossip may also make
        // any two nodes on the channel neighbours, so mesh2's node's carrier is recorded, not required:
        // what the page states is that mesh2's node-admin, joined by both meshes, received the event.
        let frames_of = |node: &rdm_core::NodeId| -> Vec<String> {
            spans_of(&span_files(&dir), node)
                .iter()
                .filter(|s| s["name"] == "rdm.mesh.topology.update.via-frame" && s["attributes"]["subject"] == "testkit/app/1")
                .map(|s| s["attributes"]["delivered_from"].as_str().unwrap_or_default().to_string())
                .collect()
        };
        let at_admin = tokio::time::timeout(WRONG, async {
            loop {
                let carried = frames_of(&m2a);
                if !carried.is_empty() {
                    return carried;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("mesh2's node-admin, joined by both meshes' nodes, receives the event: it carries the channel between them");
        println!("carriers: mesh2's node-admin from {at_admin:?}; mesh2's node from {:?} (node-admin {m2a_key})", frames_of(&m2n));
        capture.spans().expect("the cell's own spans are flushed");
        // Each node-admin's spans reach its file on that process's own export batch: its receipt is
        // waited for, bounded, as mesh2's node's is above.
        for (who, id) in [("mesh1.node-admin.1", &day0), ("mesh2.node-admin.1", &m2a)] {
            let carried = tokio::time::timeout(WRONG, async {
                loop {
                    if let Some(s) = received(&spans_of(&span_files(&dir), id)) {
                        return s;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{who}'s application received the event it carried"));
            assert_eq!(carried["trace_id"], on_m2n["trace_id"], "{who}'s receipt is in the publisher's trace");
        }
        eprintln!("app channel trace: {}", on_m2n["trace_id"].as_str().unwrap_or_default());
        d.running.leave().await;
    });
    drop(capture);
    drop(kept);
    let _ = std::fs::remove_dir_all(&dir);
}
