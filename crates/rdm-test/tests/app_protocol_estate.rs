//! product=mesh, feature=connections, subfeature=app-protocol, rung=M1, provider=process.
//!
//! Why missed: no estate ever had a node-admin open or accept an app-protocol connection. An admin
//! executable (`rdm-node-admin`, `faulted-node-admin`, the demo's `rshape-node-admin`) runs
//! `rdm_admin::entry::run_on`, which never hands the embedding its `Running`, so no admin process can
//! take its app protocols' `Connections`; the testkit's rpc node registered no app protocol; and the
//! in-process cells that did have an admin on an app ALPN (`rdm-admin`'s `admin_app_alpns.rs`) had it
//! only accept, with no certs. The Day-0 root's missing member cert (Rafka 141: its dial refused
//! `no-credential`) was reachable by no cell.
//!
//! The estate: the Day-0 node-admin runs in this process (the only admin that can take its app
//! protocol's `Connections`, as an embedding's own node-admin does), holding the testkit's root, with the
//! testkit's app protocol registered (`rdm_test::node::APP_PROTOCOL`); every other node is a process a
//! Build created, its rpc nodes registering the same protocol through the root's knob file. Both
//! directions are dialed between the Day-0 admin and an rpc node, in a one-mesh fabric (the rpc node
//! restarted too) and in a two-mesh fabric (an rpc node of the mesh another admin leads).

use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_client::{BuildCarrier, BuildId, Change, Fabrics, LaunchKind, MeshDesired, NodeStatus, Nodes, PathName};
use rdm_test::estate::{bin_dir, Estate};
use rdm_test::executables::{Current, Executables};
use rdm_test::node::{app_protocol, echo_streams, APP_PROTOCOL, APP_PROTOCOL_KNOB};
use rdm_test::originate::{Originate, OriginateReply, OriginateRequest};
use rdm_transport::connections::{Birth, Dialer};
use rdm_transport::{CallOptions, NodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const WRONG: Duration = Duration::from_secs(120);

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

/// A Day-0 node-admin whose data dir is `<dir>/admin` and whose fabric's store dir is `<dir>/store`,
/// launching the built testkit executables on the process provider: every node it or another admin
/// launches has its data dir under `<dir>`, the root that holds the app protocol's knob.
fn day0_config(dir: &Path) -> AdminConfig {
    let (data, store) = (dir.join("admin"), dir.join("store"));
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&store).unwrap();
    std::fs::write(dir.join(APP_PROTOCOL_KNOB), b"").unwrap();
    let bins = bin_dir().display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" => Some(data.display().to_string()),
        "RDM_STORE_DIR" => Some(store.display().to_string()),
        "RDM_BIN_DIR" => Some(bins.clone()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        // Every node the admins launch writes its spans here (the launch passes it through).
        "RDM_EVIDENCE_DIR" => Some(dir.join("spans").display().to_string()),
        _ => None,
    })
    .expect("a readable environment")
}

/// Every `rdm.node.cert.update.via-join` the processes under `<dir>/spans` spanned for `node`: each
/// install's incarnation and the installed cert's length.
fn installs_of(dir: &Path, node: &str) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir.join("spans")).into_iter().flatten().flatten() {
        for l in std::fs::read_to_string(e.path()).unwrap_or_default().lines() {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(l) else { continue };
            let a = &v["attributes"];
            if v["name"] == "rdm.node.cert.update.via-join" && a["node"] == node {
                let len = a["cert_len"].as_u64().or_else(|| a["cert_len"].as_str().and_then(|x| x.parse().ok())).unwrap_or(0);
                out.push((a["incarnation_id"].as_str().unwrap_or_default().to_string(), len));
            }
        }
    }
    out
}

/// The Day-0 admin of the cell's estate, its Day-0 Build followed to its end, and the objects a
/// caller reaches it through.
struct Day0 {
    running: Running,
    dialer: Dialer,
    carrier: BuildCarrier,
    nodes: Nodes,
    fabrics: Fabrics,
    _echo: tokio::task::JoinHandle<()>,
}

async fn day0(dir: &Path) -> Day0 {
    let running = start_with(day0_config(dir), Wiring { alpns: vec![app_protocol()], certs: rdm_test::test_certs::root_choice(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() })
        .await
        .expect("the Day-0 admin comes up");
    let (dialer, acceptor) = running.connections(APP_PROTOCOL).expect("the app takes its protocol's Connections from the admin").split();
    let me = running.birth();
    let echo = echo_streams(acceptor, me.name.to_string());
    let admin = ResolvedNode { node_id: me.node_id.clone(), name: me.name.clone(), endpoint_id: me.endpoint_id.0.parse().unwrap(), incarnation: me.incarnation.clone(), transport_addr: me.transport_addr };
    let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller's endpoint binds");
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(admin);
    let client = Arc::new(NodeRpcClient::new(endpoint, resolver).with_caller_system("rdm"));
    let carrier = BuildCarrier::new(client, me.name.clone());
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
    // The admin names its Day-0 Build before it holds the fabric-primary: the stream is opened once it does.
    let mut stream = tokio::time::timeout(WRONG, async {
        loop {
            match carrier.resubmit(&day_zero, 1).await {
                Ok(s) => return s,
                Err(e) if e.to_string().contains("not-fabric-primary") => tokio::time::sleep(Duration::from_millis(100)).await,
                Err(e) => panic!("the admin does not stream its Day-0 Build {day_zero}: {e}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the Day-0 admin never held the fabric-primary to stream its Day-0 Build {day_zero}"));
    Estate::until_complete(&mut stream).await;
    Day0 { running, dialer, nodes: Nodes::new(carrier.clone()), fabrics, carrier, _echo: echo }
}

impl Day0 {
    /// Reconcile the fabric to `meshes` (each as the compiler's count form resizes it) and follow the
    /// Build to its end.
    async fn reconcile(&self, meshes: &[MeshDesired]) {
        let executables = Executables::testkit("process");
        let current = Current::read(&self.carrier).await.unwrap_or_else(|e| panic!("the current topology: {e}"));
        let nodes = current.resized(meshes, true, &executables);
        let mut stream = self.carrier.create(Change::Reconcile { topology_storage_provider: None, based_on: current.based_on.clone(), nodes }).await.unwrap_or_else(|e| panic!("the fabric-primary refused {meshes:?}: {e}"));
        Estate::until_complete(&mut stream).await;
    }

    /// `node` listed ready by the admin's `mesh.nodes.get` of its mesh.
    async fn until_ready(&self, node: &PathName) {
        tokio::time::timeout(WRONG, async {
            loop {
                let listed = rdm_test::typed::mesh_nodes(&self.nodes, &node.mesh).await.unwrap_or_else(|e| panic!("node.get [mesh: {}]: {e}", node.mesh));
                if listed.iter().any(|n| &n.name == node && n.status == NodeStatus::Ready) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{node} never listed ready"));
    }

    /// The fabric's own status ready: a node.restart and a new mesh are run once the fabric
    /// first reached it (states.md, "The initial fabric before ready").
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
    async fn resolved(&self, node: &PathName, not: Option<&rdm_core::IncarnationId>) -> ResolvedNode {
        tokio::time::timeout(WRONG, async {
            loop {
                if let Ok(r) = self.running.node_rpc.resolver.resolve(&NodeTarget::CurrentPath(node.clone())) {
                    if Some(&r.incarnation) != not {
                        return r;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the Day-0 admin never resolved {node}"))
    }

    /// The Day-0 admin dials `node` on the app protocol: admitted, its stream echoed by the node's app.
    async fn dials(&self, node: &ResolvedNode) {
        let to = Birth { name: node.name.clone(), node_id: node.node_id.clone(), incarnation: node.incarnation.clone() };
        let conn = match tokio::time::timeout(WRONG, self.dialer.open(&to)).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => panic!("the Day-0 admin's dial to {} on an app protocol is refused: {e:?}", node.name),
            Err(_) => panic!("the Day-0 admin's dial to {} never ended", node.name),
        };
        let (mut send, mut recv) = conn.open_bi().await.unwrap_or_else(|e| panic!("a stream to {}: {e:?}", node.name));
        send.write_all(b"from the Day-0 admin").await.unwrap();
        send.finish().unwrap();
        let answered = tokio::time::timeout(WRONG, recv.read_to_end(1 << 16)).await.expect("the node answers").expect("the answer reads whole");
        assert_eq!(String::from_utf8_lossy(&answered), format!("echo {}: from the Day-0 admin", node.name), "{}'s app answered the Day-0 admin's admitted connection", node.name);
    }

    /// `node` dials the Day-0 admin on the app protocol through its originate door: admitted, its stream
    /// echoed by the admin's app.
    async fn dialed_by(&self, node: &ResolvedNode) {
        let me = self.running.birth().name.to_string();
        let req = OriginateRequest::Stream { destination: me.clone(), payload: format!("from {}", node.name).into_bytes() };
        let opts = CallOptions { budget: rdm_transport::Budget::Overall(Duration::from_secs(30)), ..CallOptions::default() };
        let (out, _) = self.running.node_rpc.client.call::<Originate>(&NodeTarget::ExactNode(node.node_id.clone()), &req, &opts).await;
        match out {
            rdm_core::outcome::RpcOutcome::Reply(r) => match r.into_value() {
                OriginateReply::Streamed { answered, peer, .. } => {
                    assert_eq!(peer.node, me, "{} reached the Day-0 admin", node.name);
                    assert_eq!(String::from_utf8_lossy(&answered), format!("echo {me}: from {}", node.name), "the Day-0 admin's app answered {}'s admitted connection", node.name);
                }
                other => panic!("{}'s dial to the Day-0 admin on an app protocol failed: {other:?}", node.name),
            },
            other => panic!("the originate door of {} ended {other:?}", node.name),
        }
    }

    async fn both_ways(&self, node: &ResolvedNode) {
        self.dials(node).await;
        self.dialed_by(node).await;
    }
}

fn run(cell: &str, body: impl FnOnce(PathBuf) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>) {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    let dir = std::env::temp_dir().join(format!("{cell}-{}", rdm_core::NodeId::mint()));
    let _launched = Launched(dir.join("store"));
    rt.block_on(body(dir.clone()));
    drop(rt);
    let _ = std::fs::remove_dir_all(&dir);
}

// @feature: connections
/// CONTRACT (fabric-certs.md §When a cert is issued, §On a connection; node-connections-admit.md): in a
/// one-mesh fabric the Day-0 node-admin dials an rpc node of its mesh on a registered app protocol and is
/// admitted (its stream answered by the node's app), and the rpc node's dial to the Day-0 admin is
/// admitted; after the rpc node's `node.restart` (the same birth, its start's join issuing it a fresh cert, and its drop record of the Day-0 admin dialled again, connections.md §7) both
/// directions are admitted again. A second node-admin stopped and started as the same birth installs a
/// fresh member cert at its start's join. Must NOT happen: the Day-0 admin refused `no-credential` (Rafka 141).
#[test]
fn the_day0_admin_and_an_rpc_node_dial_each_other_on_an_app_protocol_in_a_one_mesh_fabric() {
    run("app-protocol-one-mesh", |dir| {
        Box::pin(async move {
            let d = day0(&dir).await;
            // Two rpc nodes: a restart of the type primary hands the role to the other first (node-stop.md "Primary": a sole one is refused no-secondary).
            d.reconcile(&[MeshDesired::of("mesh1", [(LaunchKind::NodeAdmin, 2), (LaunchKind::RpcNode, 2)])]).await;
            let rpc: PathName = "mesh1.rpc.1".parse().unwrap();
            d.until_ready(&rpc).await;
            d.until_fabric_ready().await;
            // A launched admin restarted as the same birth (node.stop, then node.start: same node.id and
            // incarnation) is issued a fresh member cert by its start's join and installs it. A process
            // admin takes no app protocol (`entry::run_on`), so its dials are proved in process
            // (`rdm-admin`'s `app_protocol_dials`); here the install is read from its evidence.
            let admin2: PathName = "mesh1.admin.2".parse().unwrap();
            d.until_ready(&admin2).await;
            let before = d.resolved(&admin2, None).await;
            Estate::workflow("node.stop of mesh1.admin.2", d.nodes.stop(&admin2).await).await;
            Estate::workflow("node.start of mesh1.admin.2", d.nodes.start(&admin2).await).await;
            d.until_ready(&admin2).await;
            let same = before.incarnation.as_str().to_string();
            let installs = tokio::time::timeout(WRONG, async {
                loop {
                    let of_birth: Vec<(String, u64)> = installs_of(&dir, "mesh1.admin.2").into_iter().filter(|(i, l)| *i == same && *l > 0).collect();
                    if of_birth.len() >= 2 {
                        return of_birth;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("mesh1.admin.2 ({same}) installed no member cert at its start after its stop: {:?}", installs_of(&dir, "mesh1.admin.2")));
            assert!(installs.len() >= 2, "a cert at its create and another at its start, same birth: {installs:?}");
            let first = d.resolved(&rpc, None).await;
            d.both_ways(&first).await;

            d.until_fabric_ready().await;
            Estate::workflow("node.stop of mesh1.rpc.1", d.nodes.stop(&rpc).await).await;
            Estate::workflow("node.start of mesh1.rpc.1", d.nodes.start(&rpc).await).await;
            d.until_ready(&rpc).await;
            // A stop then a start keep the birth (same node.id and incarnation; node-stop.md "Kept",
            // node-start.md): its start's join issues it a fresh member cert, and the dials are admitted
            // again on that birth.
            let again = d.resolved(&rpc, None).await;
            assert_eq!(again.incarnation, first.incarnation, "a stop then a start keep the birth");
            d.both_ways(&again).await;

            d.running.leave().await;
        })
    });
}

// @feature: connections
/// CONTRACT (fabric-certs.md §When a cert is issued: "a new mesh's first node-admin is deployed"; §On a
/// connection): in a two-mesh fabric the Day-0 node-admin (mesh1's, the fabric-primary) and an rpc node
/// of mesh2, launched by mesh2's own admin, dial each other on a registered app protocol and are
/// admitted both ways; so are the Day-0 admin and mesh1's rpc node. Must NOT happen: a dial across the
/// meshes refused for a cert of another chain, or the Day-0 admin refused `no-credential`.
#[test]
fn the_day0_admin_and_rpc_nodes_of_both_meshes_dial_each_other_on_an_app_protocol_in_a_two_mesh_fabric() {
    run("app-protocol-two-mesh", |dir| {
        Box::pin(async move {
            let d = day0(&dir).await;
            let mesh = |m: &str| MeshDesired::of(m, [(LaunchKind::NodeAdmin, 1), (LaunchKind::RpcNode, 1)]);
            d.reconcile(&[mesh("mesh1")]).await;
            d.until_ready(&"mesh1.rpc.1".parse().unwrap()).await;
            d.until_fabric_ready().await;
            d.reconcile(&[mesh("mesh1"), mesh("mesh2")]).await;
            for node in ["mesh2.rpc.1", "mesh1.rpc.1"] {
                let node: PathName = node.parse().unwrap();
                d.until_ready(&node).await;
                let resolved = d.resolved(&node, None).await;
                d.both_ways(&resolved).await;
            }
            d.running.leave().await;
        })
    });
}
