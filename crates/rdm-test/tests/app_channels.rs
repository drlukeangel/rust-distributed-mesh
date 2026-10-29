//! Public app declarations, joins before the pre_node_ready hook, and held-set discovery on real admins.
use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::node_hooks::{HookOutcome, PreNodeReady, NodeReadyCtx, NodeHooks};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::status::{StatusReply, StatusRequest};
use rdm_core::{IncarnationId, NodeId};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};
const BASE: &str = "app.base";
const LATE: &str = "app.late";
const BOUND: Duration = Duration::from_secs(60);
#[derive(Default)]
struct Hook {
    channel: Mutex<Option<rdm_transport::gossip::Channel>>,
    observed: Mutex<Option<BTreeSet<String>>>,
}
#[async_trait::async_trait]
impl PreNodeReady for Hook {
    async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome {
        let types = ctx.topology_view.app_types();
        assert_eq!(types.len(), 1);
        assert!(
            types.is_subset(&ctx.topology_view.held_types()),
            "the role joined its declaration before its pre_node_ready hook"
        );
        let channel = ctx
            .topology_view
            .join_channel(types.first().unwrap())
            .await
            .expect("the own declared channel is already joined");
        *self.observed.lock().unwrap() = Some(ctx.topology_view.held_types());
        *self.channel.lock().unwrap() = Some(channel);
        HookOutcome::Ok
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
async fn until_held(r: &Running, expected: &BTreeSet<String>) {
    let mut changes = r.topology_view.digest_view.digest_changes();
    tokio::time::timeout(BOUND, async {
        loop {
            if expected.is_subset(&r.topology_view.held_types()) {
                return;
            }
            changes.changed().await.unwrap();
        }
    })
    .await
    .expect("the declared held set is learned within the read bound");
}
async fn own_view(r: &Running) -> rdm_core::views::NodeView {
    let mut changes = r.topology_view.digest_view.digest_changes();
    tokio::time::timeout(BOUND, async {
        loop {
            let answer = r
                .status_door()
                .unwrap()
                .apply(
                    None,
                    &StatusRequest::Get {
                        entity: rdm_core::filter::Entity::Nodes,
                        node: None,
                        filter: rdm_core::filter::Filter::new().eq("id", [r.birth().node_id.to_string()]),
                    },
                )
                .await;
            // Before this admin holds its own primary records it is not a primary: it forwards the read to
            // the mesh-primary its view names and, naming none yet, refuses it not-ready (query.md §1;
            // Q243). The read is not answered yet, as an empty list is not.
            match answer {
                StatusReply::Nodes { mut nodes } if nodes.len() == 1 => return nodes.remove(0),
                StatusReply::Nodes { nodes } => assert!(nodes.is_empty()),
                StatusReply::NotReady { .. } => {}
                other => panic!("{other:?}"),
            }
            changes.changed().await.unwrap();
        }
    })
    .await
    .expect("the admin's own projection is installed within the read bound")
}
#[test]
fn a_node_admin_joins_its_wiring_types_before_its_pre_node_ready_hook_and_node_get_answers_its_own_declaration() {
    let cell="a_node_admin_joins_its_wiring_types_before_its_pre_node_ready_hook_and_node_get_answers_its_own_declaration";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    cap.run(crate::common::Threads::Workers(4), async {
        let dir = std::env::temp_dir().join(format!("app-admin-{}", NodeId::mint()));
        let hook = Arc::new(Hook::default());
        let root = start_with(
            config(&dir),
            Wiring {
                types: vec![BASE.into()],
                node_hooks: NodeHooks::new(hook.clone()),
                fabric_hooks: FabricHooks::no_app_work(),
                ..Wiring::no_certs()
            },
        )
        .await
        .unwrap();
        root.stop_reconciling().await;
        tokio::time::timeout(BOUND, root.pre_node_ready().unwrap().settled())
            .await
            .unwrap();
        assert_eq!(
            hook.observed.lock().unwrap().as_ref(),
            Some(&BTreeSet::from([BASE.into()]))
        );
        let view = own_view(&root).await;
        assert_eq!(view.types, BTreeSet::from([BASE.into()]));
        assert_eq!(view.held, view.types);
        assert_eq!(view.held_hash, rdm_core::app_types::hash(&view.held));
        hook.channel.lock().unwrap().take();
        root.leave().await;
        let _ = std::fs::remove_dir_all(dir);
    });
}
#[test]
fn a_late_declaration_joins_before_the_admin_answers_and_a_peer_hash_reads_node_get_to_grow_held() {
    let cell="a_late_declaration_joins_before_the_admin_answers_and_a_peer_hash_reads_node_get_to_grow_held";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    cap.run(crate::common::Threads::Workers(4), async {
        let root_dir = std::env::temp_dir().join(format!("app-discovery-a-{}", NodeId::mint()));
        let joiner_dir = std::env::temp_dir().join(format!("app-discovery-b-{}", NodeId::mint()));
        let root = start_with(
            config(&root_dir),
            Wiring {
                types: vec![BASE.into()],
                fabric_hooks: FabricHooks::no_app_work(),
                ..Wiring::no_certs()
            },
        )
        .await
        .unwrap();
        root.stop_reconciling().await;
        let root_digest = root.digest.lock().unwrap().clone();
        std::fs::create_dir_all(&joiner_dir).unwrap();
        let runtime =
            rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("app-discovery"))
                .unwrap();
        runtime.write_record(&joiner_dir).unwrap();
        let key = rdm_test::node::load_or_mint_key(&joiner_dir).unwrap();
        let launch = rdm_core::launch::Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: config(&root_dir).fabric,
            fabric_id: root_digest.fabric_id.clone(),
            name: "mesh1.admin.2".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![("control".into(), "127.0.0.1:0".parse().unwrap())],
            seeds: vec![(
                root_digest.node.endpoint_id.0.clone(),
                root_digest.node.transport_addr,
            )],
            launcher: Some(rdm_core::launch::Launcher {
                name: root_digest.node.name.clone(),
                node_id: root_digest.node.node_id.clone(),
                incarnation: root_digest.node.incarnation.clone(),
            }),
            data_dir: joiner_dir.clone(),
            store_dir: root_dir.clone(),
            mesh_id: root_digest.mesh_id.clone(),
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
        };
        let _report = root.runner.joins.expect(rdm_admin::join::Deployed {
            name: launch.name.clone(),
            node_id: launch.node_id.clone(),
            incarnation: launch.incarnation.clone(),
            supersedes: None,
            endpoint_id: rdm_core::EndpointId(key.public().to_string()),
            runtime,
            data_dir: joiner_dir.display().to_string(),
            last_accepted_name: None,
            held: false,
            listeners: Vec::new(),
        });
        let env = launch.to_env();
        let hook = Arc::new(Hook::default());
        let joiner = start_with(
            AdminConfig::from_env(|k| env.get(k).cloned()).unwrap(),
            Wiring {
                types: vec![LATE.into()],
                node_hooks: NodeHooks::new(hook.clone()),
                fabric_hooks: FabricHooks::no_app_work(),
                ..Wiring::no_certs()
            },
        )
        .await
        .unwrap();
        joiner.stop_reconciling().await;
        assert!(
            root.topology_view.held_types().contains(LATE),
            "the launching admin joined the new type before returning JoinNode"
        );
        let expected = BTreeSet::from([BASE.into(), LATE.into()]);
        until_held(&joiner, &expected).await;
        let first = own_view(&root).await;
        // What the joiner holds itself (query.md §2): it is not a primary, so node.get sent to it is
        // forwarded to its mesh-primary (query.md §1; Q243) and answers that primary's record of it.
        let second = &joiner.topology_view;
        assert_eq!(first.types, BTreeSet::from([BASE.into()]));
        assert_eq!(second.app_types(), BTreeSet::from([LATE.into()]));
        assert_eq!(first.held, expected);
        assert_eq!(second.held_types(), expected);
        assert_eq!(second.held_hash(), rdm_core::app_types::hash(&expected));
        tokio::time::timeout(BOUND, joiner.pre_node_ready().unwrap().settled())
            .await
            .unwrap();
        hook.channel.lock().unwrap().take();
        joiner.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(joiner_dir);
        let _ = std::fs::remove_dir_all(root_dir);
    });
    cap.flush().unwrap();
    assert!(
        cap.spans
            .0
            .lock()
            .unwrap()
            .values()
            .any(|(name, _, _)| name == "rdm.node_admin.node.update.via-held-types"),
        "a changed backbone hash drove a peer node.get, not a digest list"
    );
    assert!(
        !cap.spans
            .0
            .lock()
            .unwrap()
            .values()
            .any(|(name, _, _)| name == "rdm.mesh.topology.reject.via-inconsistent"),
        "GetTopology and Frame::MeshTopology carry the same held_hash at the same (publisher, topology_version)"
    );
}


const MEMBERS: &str = "app.members";

// @feature: gossip
/// CONTRACT (gossip.md app channels: node-admin joins every type a member declares, with the entity
/// framework as a Channel subscriber; i141 e11's receive wiring): a node-admin's app watches its held
/// set (`TopologyView::held_type_changes`). A member it deployed declares a type the admin does not
/// declare; its join grows the admin's held set, and the watch reports the whole new set. An event the
/// member publishes on that type once the channel's neighbours are up, before the app joins the
/// channel, is buffered by the admin's subscription and received by the app's `join_channel`.
#[test]
fn a_node_admin_reports_a_members_new_type_on_its_held_watch_and_its_app_receives_an_event_published_before_the_join() {
    let cell = "a_node_admin_reports_a_members_new_type_on_its_held_watch_and_its_app_receives_an_event_published_before_the_join";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("app-held-watch-{}", NodeId::mint()));
        let member_dir = dir.join("member");
        let root = start_with(config(&dir), Wiring { types: vec![BASE.into()], fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.unwrap();
        root.stop_reconciling().await;
        let mut held = root.topology_view.held_type_changes();
        assert_eq!(*held.borrow_and_update(), BTreeSet::from([BASE.to_string()]), "the watch starts at the set held now");

        let d = root.digest.lock().unwrap().clone();
        std::fs::create_dir_all(&member_dir).unwrap();
        let runtime = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("app-held-watch")).unwrap();
        runtime.write_record(&member_dir).unwrap();
        let key = rdm_test::node::load_or_mint_key(&member_dir).unwrap();
        let launch = rdm_core::launch::Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: config(&dir).fabric,
            fabric_id: d.fabric_id.clone(),
            name: "mesh1.rpc.1".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
            launcher: Some(rdm_core::launch::Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
            data_dir: member_dir.clone(),
            store_dir: dir.clone(),
            mesh_id: d.mesh_id.clone(),
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
        };
        let _report = root.runner.joins.expect(rdm_admin::join::Deployed {
            name: launch.name.clone(),
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
        let member = rdm_node::node::start_with_rafka_time_pre_node_ready_alpns_and(&launch, rdm_transport::clock::RafkaTime::unadopted(), rdm_node::node_hooks::NodeHooks::none(), |s| s, None, |b, _| b, &[MEMBERS], &[])
            .await
            .expect("the member declaring app.members starts");

        let grown = tokio::time::timeout(BOUND, async {
            loop {
                held.changed().await.expect("the admin's held watch stays open");
                let set = held.borrow_and_update().clone();
                if set.contains(MEMBERS) {
                    return set;
                }
            }
        })
        .await
        .expect("the admin's held watch reports the member's new type");
        assert_eq!(grown, BTreeSet::from([BASE.to_string(), MEMBERS.to_string()]), "the whole held set is sent");
        assert_eq!(grown, root.topology_view.held_types(), "the snapshot read agrees with the watch");

        // The member's channel and the admin's subscription see each other (a protocol event).
        let (to_admin, _from_admin) = member.topology_view.join_channel(MEMBERS).await.expect("the member joins its declared channel").split();
        tokio::time::timeout(BOUND, async {
            while spans.0.lock().unwrap().values().filter(|(n, _, f)| n == "rdm.mesh.channel.update.via-neighbour" && f.get("event_type").map(String::as_str) == Some(MEMBERS)).count() < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the app.members channel's neighbours are up on both sides");
        let published = rdm_transport::gossip::Event { subject: "members/1".into(), sequence: Some(1), dataschema: Some("members.v1".into()), data: bytes::Bytes::from_static(b"before the join") };
        to_admin.publish(published).await.expect("the member publishes");

        let mut channel = root.topology_view.join_channel(MEMBERS).await.expect("the admin's app joins the type its held watch reported");
        let got = tokio::time::timeout(Duration::from_secs(10), channel.recv()).await.expect("the event published before the join is received").expect("the channel is open");
        assert_eq!((got.data.as_ref(), got.sequence), (&b"before the join"[..], Some(1)));
        drop(channel);
        member.stop().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(dir);
    });
}
