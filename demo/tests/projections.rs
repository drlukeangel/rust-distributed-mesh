//! The independent consumer's projection chain, on public RDM surfaces only: node-admin declares
//! every kind and keeps a cache of what arrives on each channel; a node born later hydrates its own
//! kinds from that cache through its `pre_node_ready` hook before it reports ready; what is published
//! after the pull arrives live. The gateway holds three kinds, the compute two: one shared with the
//! gateway and one its own (gossip.md §8: node-admin holds every channel as the app; "Entry ends
//! with a pull"; fabric-node-hooks.md `pre_node_ready`).
use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::catalog::OpOwner;
use rdm_core::{IncarnationId, NodeId};
use rdm_node::node_hooks::{PreNodeReady, NodeReadyCtx, NodeHooks, HookOutcome};
use rdm_node::gossip::{Channel, Event};
use rdm_test::pre_node_ready_probe::{PreNodeReadyPull, PullReply, PullRequest, Row};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

const BOUND: Duration = Duration::from_secs(60);
/// The gateway's three kinds and the compute's two; `SHARED` is held by both.
const GATEWAY_ONLY: [&str; 2] = ["demo.principal", "demo.path-config"];
const SHARED: &str = "demo.topic";
const COMPUTE_ONLY: &str = "demo.task";
const GATEWAY_KINDS: [&str; 3] = ["demo.principal", "demo.path-config", "demo.topic"];
const COMPUTE_KINDS: [&str; 2] = ["demo.topic", "demo.task"];
const ALL_KINDS: [&str; 4] = ["demo.principal", "demo.path-config", "demo.topic", "demo.task"];

/// The cache: the latest row per subject (`<kind>/<key>`), by version; a version already held
/// changes nothing. node-admin's and each node's own are this one shape.
type Cache = Arc<Mutex<BTreeMap<String, (u64, String)>>>;

fn install(cache: &Cache, subject: &str, version: u64, value: &str) -> bool {
    let mut c = cache.lock().unwrap();
    match c.get(subject) {
        Some((held, _)) if *held >= version => false,
        _ => {
            c.insert(subject.to_string(), (version, value.to_string()));
            true
        }
    }
}
fn kind_of(subject: &str) -> &str {
    subject.split_once('/').map(|(k, _)| k).unwrap_or(subject)
}
fn event(subject: &str, version: u64, value: &str) -> Event {
    Event {
        subject: subject.into(),
        sequence: Some(version),
        dataschema: Some("demo.row.v1".into()),
        data: value.as_bytes().to_vec().into(),
    }
}
fn config(dir: &std::path::Path) -> AdminConfig {
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.display().to_string()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .unwrap()
}

/// node-admin's side: every kind's channel feeds the one cache.
fn feed(cache: &Cache, mut channel: Channel) {
    let cache = cache.clone();
    tokio::spawn(async move {
        while let Some(received) = channel.recv().await {
            let value = String::from_utf8_lossy(&received.data).to_string();
            let version = received.sequence.unwrap_or(0);
            if install(&cache, &received.subject, version, &value) {
                received.span.in_scope(|| {
                    tracing::info_span!(target: "rshape", "rafka.mesh.projection.update.via-channel", subject = %received.subject, version)
                        .in_scope(|| tracing::info!("node-admin's cache took the row"))
                });
            }
        }
    });
}

/// A node's side: the hook pulls every row the accepting admin holds and installs its own kinds,
/// before the node reports ready.
struct Hydrate {
    kinds: Vec<&'static str>,
    held: Cache,
}
#[async_trait::async_trait]
impl PreNodeReady for Hydrate {
    async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome {
        let Some(accepting_primary) = ctx.accepting_primary.accepted() else {
            return HookOutcome::Failed { reason: "this consumer's nodes are all launched: each has an accepting admin".into() };
        };
        let (out, _) = ctx
            .client
            .call::<PreNodeReadyPull>(&accepting_primary.target, &PullRequest::Pull, &rdm_transport::CallOptions::default())
            .await;
        let Some(reply) = out.reply() else {
            return HookOutcome::Failed { reason: format!("the pull was not answered: {out:?}") };
        };
        match reply.value() {
            PullReply::Rows { rows } => {
                let mut installed = 0u64;
                for row in rows {
                    let Some((subject, value)) = row.value.split_once('=') else { continue };
                    if self.kinds.contains(&kind_of(subject)) && install(&self.held, subject, row.version, value) {
                        installed += 1;
                    }
                }
                tracing::info_span!(target: "rshape", "rafka.mesh.entry.update.via-projection-pulled", node = %ctx.birth.name, accepting_primary = %accepting_primary.name, entity_rows = rows.len() as u64, raw_applied = installed)
                    .in_scope(|| tracing::info!("hydrated from node-admin's cache before ready"));
                HookOutcome::Ok
            }
            PullReply::NotReady { reason } => ctx.primary_not_ready(reason.clone()),
            other => HookOutcome::Failed { reason: format!("{other:?}") },
        }
    }
}

/// A member launched by the root admin, declaring `kinds`, hydrating through `hook`.
async fn member(root: &Running, dir: &std::path::Path, shared: &std::path::Path, name: &str, kinds: &[&str], hook: NodeHooks) -> rdm_node::node::RunningNode {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("demo-proj")).unwrap();
    fact.write_record(dir).unwrap();
    let key = rdm_node::node::load_or_mint_key(dir).unwrap();
    let launch = rdm_core::launch::Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: config(shared).fabric,
        fabric_id: d.fabric_id.clone(),
        name: name.parse().unwrap(),
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
        hook,
        |s| s,
        None,
        |b, _| b,
        kinds,
    )
    .await
    .unwrap()
}

async fn recv(channel: &mut Channel) -> (String, u64, String) {
    let e = tokio::time::timeout(BOUND, channel.recv()).await.unwrap().unwrap();
    (e.subject.clone(), e.sequence.unwrap_or(0), String::from_utf8_lossy(&e.data).to_string())
}

#[test]
fn node_admin_caches_every_kind_and_a_node_born_later_hydrates_its_own_kinds_before_ready() {
    let dir = std::env::temp_dir().join(format!("demo-proj-{}", NodeId::mint()));
    let capture = rdm_transport::EvidenceCapture::start(&dir.join("evidence"), "demo-projections").unwrap();
    capture.run(4, async {
        // node-admin: declares every kind, serves the pull from its cache.
        let cache: Cache = Default::default();
        let serve = {
            let cache = cache.clone();
            Box::new(move |b: rdm_transport::ServerBuilder| {
                b.serve::<PreNodeReadyPull, _, _>(OpOwner::Testkit, move |_, _| {
                    let cache = cache.clone();
                    async move {
                        let rows = cache
                            .lock()
                            .unwrap()
                            .iter()
                            .map(|(subject, (version, value))| Row { version: *version, value: format!("{subject}={value}") })
                            .collect();
                        Ok(PullReply::Rows { rows })
                    }
                })
            }) as rdm_admin::wiring::ServeApp
        };
        let root_dir = dir.join("admin");
        let root = start_with(
            config(&root_dir),
            Wiring {
                types: ALL_KINDS.iter().map(|k| k.to_string()).collect(),
                serve_app: Some(serve),
                fabric_hooks: FabricHooks::no_app_work(),
                ..Wiring::no_certs()
            },
        )
        .await
        .unwrap();
        root.stop_reconciling().await;
        assert_eq!(root.topology_view.held_types(), ALL_KINDS.iter().map(|k| k.to_string()).collect());
        for kind in ALL_KINDS {
            feed(&cache, root.topology_view.join_channel(kind).await.unwrap());
        }

        // The first gateway and compute: born into an empty cache, they publish.
        let g1_held: Cache = Default::default();
        let c1_held: Cache = Default::default();
        let g1 = member(&root, &dir.join("gateway.1"), &root_dir, "mesh1.gateway.1", &GATEWAY_KINDS, NodeHooks::new(Arc::new(Hydrate { kinds: GATEWAY_KINDS.to_vec(), held: g1_held.clone() }))).await;
        let c1 = member(&root, &dir.join("compute.1"), &root_dir, "mesh1.compute.1", &COMPUTE_KINDS, NodeHooks::new(Arc::new(Hydrate { kinds: COMPUTE_KINDS.to_vec(), held: c1_held.clone() }))).await;
        assert!(g1_held.lock().unwrap().is_empty() && c1_held.lock().unwrap().is_empty(), "nothing was published yet");
        assert_eq!(g1.topology_view.held_types(), GATEWAY_KINDS.iter().map(|k| k.to_string()).collect());
        assert_eq!(c1.topology_view.held_types(), COMPUTE_KINDS.iter().map(|k| k.to_string()).collect());
        let mut g1_channels: BTreeMap<&str, Channel> = BTreeMap::new();
        for kind in GATEWAY_KINDS {
            g1_channels.insert(kind, g1.topology_view.join_channel(kind).await.unwrap());
        }
        let mut c1_channels: BTreeMap<&str, Channel> = BTreeMap::new();
        for kind in COMPUTE_KINDS {
            c1_channels.insert(kind, c1.topology_view.join_channel(kind).await.unwrap());
        }
        // Each channel has node-admin as a neighbour before anything is published: the admin's
        // channel count of the kind reaches one.
        tokio::time::timeout(BOUND, async {
            loop {
                let spans = capture.spans().unwrap();
                let up = |kind: &str| spans.iter().filter(|s| s["name"] == "rdm.mesh.channel.update.via-neighbour" && s["attributes"]["event_type"] == kind).count();
                if GATEWAY_ONLY.iter().all(|k| up(k) >= 2) && up(SHARED) >= 4 && up(COMPUTE_ONLY) >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        // Rows: the gateway writes its own kinds and the shared one; the compute writes the shared
        // one and its own. Versions change per subject.
        let rows: Vec<(&str, u64, &str)> = vec![
            ("demo.principal/alice", 1, "alice@v1"),
            ("demo.path-config/root", 1, "path:/"),
            ("demo.topic/orders", 1, "orders@v1 from the gateway"),
            ("demo.topic/orders", 2, "orders@v2 from the compute"),
            ("demo.task/t1", 1, "t1 running"),
            ("demo.principal/alice", 2, "alice@v2"),
        ];
        for (subject, version, value) in &rows {
            let publisher = match kind_of(subject) {
                k if k == COMPUTE_ONLY => &c1_channels[k],
                k if k == SHARED && *version == 2 => &c1_channels[k],
                k => &g1_channels[k],
            };
            publisher.publish(event(subject, *version, value)).await.unwrap();
        }
        // node-admin's cache holds the latest version of every subject, fed by the channels alone.
        let expected: BTreeMap<String, (u64, String)> = [
            ("demo.principal/alice", (2, "alice@v2")),
            ("demo.path-config/root", (1, "path:/")),
            ("demo.topic/orders", (2, "orders@v2 from the compute")),
            ("demo.task/t1", (1, "t1 running")),
        ]
        .into_iter()
        .map(|(s, (v, x))| (s.to_string(), (v, x.to_string())))
        .collect();
        tokio::time::timeout(BOUND, async {
            while *cache.lock().unwrap() != expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("node-admin's cache converges on the latest row per subject");

        // A gateway and a compute born now hydrate their own kinds from the cache, before ready.
        let g2_held: Cache = Default::default();
        let c2_held: Cache = Default::default();
        let g2 = member(&root, &dir.join("gateway.2"), &root_dir, "mesh1.gateway.2", &GATEWAY_KINDS, NodeHooks::new(Arc::new(Hydrate { kinds: GATEWAY_KINDS.to_vec(), held: g2_held.clone() }))).await;
        let c2 = member(&root, &dir.join("compute.2"), &root_dir, "mesh1.compute.2", &COMPUTE_KINDS, NodeHooks::new(Arc::new(Hydrate { kinds: COMPUTE_KINDS.to_vec(), held: c2_held.clone() }))).await;
        let g2_rows = g2_held.lock().unwrap().clone();
        let c2_rows = c2_held.lock().unwrap().clone();
        assert_eq!(g2_rows.keys().map(|s| kind_of(s).to_string()).collect::<std::collections::BTreeSet<_>>(), GATEWAY_KINDS.iter().map(|k| k.to_string()).collect(), "the gateway hydrated exactly its three kinds");
        assert_eq!(c2_rows.keys().map(|s| kind_of(s).to_string()).collect::<std::collections::BTreeSet<_>>(), COMPUTE_KINDS.iter().map(|k| k.to_string()).collect(), "the compute hydrated exactly its two kinds");
        assert_eq!(g2_rows["demo.topic/orders"], (2, "orders@v2 from the compute".to_string()));
        assert_eq!(c2_rows["demo.topic/orders"], (2, "orders@v2 from the compute".to_string()), "the shared kind is the same row on both");
        assert_eq!(g2_rows["demo.principal/alice"], (2, "alice@v2".to_string()));
        assert_eq!(c2_rows["demo.task/t1"], (1, "t1 running".to_string()));
        assert!(!c2_rows.contains_key("demo.principal/alice") && !c2_rows.contains_key("demo.path-config/root"), "a compute holds no gateway-only kind");

        // What is published after the pull arrives live on both, and node-admin's cache follows.
        let mut g2_topic = g2.topology_view.join_channel(SHARED).await.unwrap();
        let mut c2_topic = c2.topology_view.join_channel(SHARED).await.unwrap();
        tokio::time::timeout(BOUND, async {
            loop {
                let spans = capture.spans().unwrap();
                if spans.iter().filter(|s| s["name"] == "rdm.mesh.channel.update.via-neighbour" && s["attributes"]["event_type"] == SHARED).count() >= 8 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        c1_channels[SHARED].publish(event("demo.topic/orders", 3, "orders@v3 live")).await.unwrap();
        assert_eq!(recv(&mut g2_topic).await, ("demo.topic/orders".to_string(), 3, "orders@v3 live".to_string()));
        assert_eq!(recv(&mut c2_topic).await, ("demo.topic/orders".to_string(), 3, "orders@v3 live".to_string()));
        tokio::time::timeout(BOUND, async {
            while cache.lock().unwrap().get("demo.topic/orders").map(|(v, _)| *v) != Some(3) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        drop(g1_channels);
        drop(c1_channels);
        drop(g2_topic);
        drop(c2_topic);
        for n in [g2, c2, c1, g1] {
            n.stop().await;
        }
        root.leave().await;
        let spans = capture.spans().unwrap();
        assert_eq!(spans.iter().filter(|s| s["name"] == "rafka.mesh.entry.update.via-projection-pulled").count(), 4, "every member hydrated through its hook (gossip.md §8, the consumer's span)");
        assert!(spans.iter().any(|s| s["name"] == "rafka.mesh.entry.update.via-projection-pulled" && s["attributes"]["raw_applied"] == "3"), "the gateway hydrated its three kinds");
        assert!(spans.iter().any(|s| s["name"] == "rafka.mesh.entry.update.via-projection-pulled" && s["attributes"]["raw_applied"] == "2"), "the compute hydrated its two kinds");
        // Five subjects' latest rows plus the live one; an older version overtaken in flight (orders@v1 behind v2) is
        // never installed, so the count is at least five and at most seven.
        let took = spans.iter().filter(|s| s["name"] == "rafka.mesh.projection.update.via-channel").count();
        assert!((5..=7).contains(&took), "node-admin's cache took each newer row once: {took}");
    });
}
