//! `FetchBuildFacts` (op `0x1F`) and the Ready check's recovery of missing control facts (i143 R-G6),
//! over real Node RPC between in-process node-admins: what a responder serves from its own Build log,
//! what a caller counts as hydration, and how the Hydrator rotates, backs off and repeats.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology};
use rafka_node_admin_core::admin::hydration_blocker;
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_facts_read::{fetch_build_facts, BuildFactsDoor, FetchFailure};
use rafka_node_admin_core::build_state::{AttemptOpened, AttemptReason, AttemptOutcome, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, LocalBuildLog, MemoryBuildStateAdapter};
use rafka_node_admin_core::fabric_storage::{FabricIdentity, FabricRecord, FabricStorage, MemoryFabricStorage};
use rafka_node_admin_core::hydrate::{HydrationBlocker, Hydrator, Responder, Responders};
use rafka_node_admin_core::model::{FabricId, PathName};
use rafka_node_rpc::{NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::build_facts::{BuildFacts, BuildFactsReply};
use rafka_mesh_entity::{IncarnationId, NodeId};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A node-admin's Build state: its log, its pointer holder and its entry floor.
struct Admin {
    name: PathName,
    log: Arc<MemoryBuildStateAdapter>,
    accepted: Arc<AcceptedStore>,
    floor: Arc<Mutex<Option<(BuildId, u32)>>>,
}

async fn admin(name: &str, fabric: &FabricId) -> Admin {
    let storage = Arc::new(MemoryFabricStorage::new());
    storage.put_identity(&FabricIdentity { fabric_id: fabric.clone(), name: "fabric1".into() }).await.unwrap();
    Admin { name: name.parse().unwrap(), log: Arc::new(MemoryBuildStateAdapter::new()), accepted: Arc::new(AcceptedStore::new(storage, name)), floor: Arc::default() }
}

/// `a` holds Build `id` through attempt `attempts`, converged, and points at it.
async fn holds(a: &Admin, id: &BuildId, attempts: u32, point: bool) {
    a.log.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
    for attempt in 1..=attempts {
        if attempt > 1 {
            a.log.open_attempt(&AttemptOpened { build_id: id.clone(), attempt, reason: AttemptReason::Restart, action: None, opened_by: "x".into(), opened_at_ms: 0 }).await.unwrap();
        }
        a.log.claim_attempt(&BuildAttemptClaim { build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
        a.log.append_attempt_receipt(&BuildAttemptReceipt { build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
    }
    if point {
        a.accepted.point(id, 0, "test").await.unwrap();
    }
}

/// One node on the wire: its endpoint, a client that resolves the nodes it is given, and what it answers.
struct Node {
    ep: iroh::Endpoint,
    client: Arc<NodeRpcClient>,
    resolver: Arc<StaticResolver>,
    resolved: ResolvedNode,
    calls: Arc<AtomicU32>,
    _router: Router,
}

/// A node serving `FetchBuildFacts` with `serve`, counting the reads it is asked.
async fn node_serving<F, Fut>(name: &str, serve: F) -> Node
where
    F: Fn(rafka_node_rpc_contract::build_facts::BuildFactsRequest, rafka_node_rpc::stream::ReplySink<BuildFacts, rafka_node_rpc::stream::NotStarted>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<BuildFactsReply, rafka_node_rpc::HandlerFault>> + Send + 'static,
{
    let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    let client = Arc::new(NodeRpcClient::new(ep.clone(), resolver.clone()));
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let calls = Arc::new(AtomicU32::new(0));
    let counted = calls.clone();
    let server = ServerBuilder::new()
        .serve_stream::<BuildFacts, _, _>(rafka_node_rpc_contract::catalog::OpOwner::Product("rdm".into()), move |_peer, req, sink| {
            counted.fetch_add(1, Ordering::SeqCst);
            serve(req, sink)
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .expect("the catalog seals");
    let router = Router::builder(ep.clone()).accept(rafka_node_rpc::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id, name: name.parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation };
    Node { ep, client, resolver, resolved, calls, _router: router }
}

/// A node answering from `a`'s own Build state, as a node-admin does.
async fn node_of(a: &Admin) -> Node {
    let door = Arc::new(BuildFactsDoor { me: a.name.clone(), builds: a.log.clone(), accepted: a.accepted.clone(), floor: a.floor.clone() });
    node_serving(&a.name.to_string(), move |req, sink| {
        let door = door.clone();
        async move { Ok(door.serve(req, sink).await) }
    })
    .await
}

fn responders(list: Vec<&Node>) -> Responders {
    let list: Vec<Responder> = list.into_iter().map(|n| Responder { name: n.resolved.name.clone(), node_id: n.resolved.node_id.clone() }).collect();
    Arc::new(move || {
        let list = list.clone();
        Box::pin(async move { list })
    })
}

/// The caller: its own state and a client that resolves `peers`.
async fn caller(name: &str, fabric: &FabricId, peers: &[&Node]) -> (Admin, Node) {
    let a = admin(name, fabric).await;
    let n = node_serving(name, |_, _| async { Ok(BuildFactsReply::NotReady { reason: "a caller".into() }) }).await;
    for p in peers {
        n.resolver.insert(p.resolved.clone());
    }
    (a, n)
}

fn hydrator(c: &(Admin, Node), responders: Responders, entry: Option<rafka_node_admin_core::hydrate::EntryRepeat>) -> Hydrator {
    Hydrator::new(c.0.name.clone(), c.1.client.clone(), c.0.log.clone() as Arc<dyn LocalBuildLog>, c.0.log.clone(), c.0.accepted.clone(), responders, entry)
}

/// Drive the Ready check's turn until it finds no blocker, recording each distinct blocker kind.
async fn drive(a: &Admin, h: &mut Hydrator, within: Duration) -> Result<Vec<&'static str>, Vec<&'static str>> {
    let until = Instant::now() + within;
    let mut seen: Vec<&'static str> = Vec::new();
    loop {
        let floor = a.floor.lock().unwrap().clone();
        let blocker = hydration_blocker(&a.name, &a.accepted, &*a.log, floor).await;
        if let Some(b) = &blocker {
            if seen.last() != Some(&b.kind()) {
                seen.push(b.kind());
            }
        }
        h.tick(blocker.as_ref()).await;
        if blocker.is_none() {
            return Ok(seen);
        }
        if Instant::now() > until {
            return Err(seen);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn record_of(fabric: &FabricId, id: &BuildId) -> FabricRecord {
    FabricRecord { fabric_id: fabric.clone(), name: "fabric1".into(), build_id: Some(id.clone()) }
}

// @feature: node-lifecycle
/// CONTRACT: a joiner holds the Fabric record its entry reply supplied and none of the Build's facts
/// (its pointer is parked as wanted). The Ready check names the typed blocker (a parked pointer),
/// FetchBuildFacts retrieves the Build's facts from the responder, they are absorbed into the local
/// Build log, the parked pointer resolves and the blocker clears.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_pointer_is_resolved_by_fetching_the_builds_facts() {
    let fabric = FabricId::mint();
    let id = BuildId::mint();
    let full = admin("mesh1.admin.1", &fabric).await;
    holds(&full, &id, 2, true).await;
    let responder = node_of(&full).await;
    let joiner = caller("mesh1.admin.2", &fabric, &[&responder]).await;
    joiner.0.accepted.learn(record_of(&fabric, &id), &*joiner.0.log, "entry").await;
    *joiner.0.floor.lock().unwrap() = Some((id.clone(), 2));
    assert_eq!(joiner.0.accepted.build_id().await, None, "the pointer is parked, not installed");
    let b = hydration_blocker(&joiner.0.name, &joiner.0.accepted, &*joiner.0.log, joiner.0.floor.lock().unwrap().clone()).await.unwrap();
    assert!(matches!(&b, HydrationBlocker::NoPointer { wanted: Some(w), .. } if *w == id), "{b:?}");

    let mut h = hydrator(&joiner, responders(vec![&responder]), None);
    let seen = drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("hydrates");
    assert_eq!(seen, vec!["no-pointer-wanted"], "the one blocker the joiner named");
    assert_eq!(joiner.0.accepted.build_id().await, Some(id.clone()), "the parked pointer resolved");
    assert_eq!(joiner.0.log.read_build(&id).await.unwrap().attempt, 2, "the joiner's own log holds the Build through the floor");
    assert_eq!(responder.calls.load(Ordering::SeqCst), 1, "one read");
    for n in [&responder, &joiner.1] {
        n.ep.close().await;
    }
}

// @feature: node-lifecycle
/// CONTRACT: the stream is a snapshot of the responder's facts for the one Build asked, `End` is
/// explicit about completeness, and a Build it holds no fact of is a typed refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_responder_streams_one_builds_facts_and_names_an_unknown_build() {
    let fabric = FabricId::mint();
    let (id, other) = (BuildId::mint(), BuildId::mint());
    let full = admin("mesh1.admin.1", &fabric).await;
    holds(&full, &id, 3, true).await;
    holds(&full, &other, 1, false).await;
    let responder = node_of(&full).await;
    let c = caller("mesh1.admin.2", &fabric, &[&responder]).await;
    let t = NodeTarget::ExactNode(responder.resolved.node_id.clone());

    let got = fetch_build_facts(&c.1.client, &t, &id).await.expect("the read completes");
    assert!(got.complete, "a Ready responder holding the Build whole says so");
    assert!(got.facts.iter().all(|f| *f.build_id() == id), "only the Build asked for");
    assert_eq!(got.facts.len(), 1 + 3 + 2 + 3, "accepted, three claims, two opens, three receipts");
    let none = fetch_build_facts(&c.1.client, &t, &BuildId::mint()).await.unwrap_err();
    assert!(matches!(none, FetchFailure::UnknownBuild(_)), "{none}");
    for n in [&responder, &c.1] {
        n.ep.close().await;
    }
}

// @feature: node-lifecycle
/// CONTRACT: a responder whose own holdings are not whole (it holds the Build's facts but no pointer
/// of its own) says `complete = false` though the facts it holds would reach the floor; the caller
/// absorbs nothing from it and rotates to the next responder; only the full holdings of the second count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_incomplete_responder_is_passed_over_and_only_full_holdings_count() {
    let fabric = FabricId::mint();
    let id = BuildId::mint();
    let partial = admin("mesh1.admin.1", &fabric).await;
    holds(&partial, &id, 2, false).await;
    let full = admin("mesh1.admin.2", &fabric).await;
    holds(&full, &id, 2, true).await;
    let (first, second) = (node_of(&partial).await, node_of(&full).await);
    let joiner = caller("mesh1.admin.3", &fabric, &[&first, &second]).await;
    joiner.0.accepted.learn(record_of(&fabric, &id), &*joiner.0.log, "entry").await;
    *joiner.0.floor.lock().unwrap() = Some((id.clone(), 2));

    let got = fetch_build_facts(&joiner.1.client, &NodeTarget::ExactNode(first.resolved.node_id.clone()), &id).await.unwrap();
    assert!(!got.complete, "the first responder says its holdings are not whole");
    assert!(joiner.0.log.facts().await.unwrap().is_empty(), "a read alone absorbs nothing");

    let mut h = hydrator(&joiner, responders(vec![&first, &second]), None);
    drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("hydrates from the second");
    assert_eq!(first.calls.load(Ordering::SeqCst), 2, "the first responder was asked (once by the read above, once by the Hydrator) and passed over");
    assert_eq!(second.calls.load(Ordering::SeqCst), 1, "the second responder's full holdings hydrated it");
    assert_eq!(joiner.0.log.read_build(&id).await.unwrap().attempt, 2);
    for n in [&first, &second, &joiner.1] {
        n.ep.close().await;
    }
}

// @feature: node-lifecycle
/// CONTRACT: a stream that ends before its `End` is never hydration: nothing is absorbed, the blocker
/// stands, the target is left alone for its backoff, and the same request is made again from the
/// beginning and then counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_stream_never_counts_and_the_read_is_made_again() {
    let fabric = FabricId::mint();
    let id = BuildId::mint();
    let full = admin("mesh1.admin.1", &fabric).await;
    holds(&full, &id, 1, true).await;
    let door = Arc::new(BuildFactsDoor { me: full.name.clone(), builds: full.log.clone(), accepted: full.accepted.clone(), floor: full.floor.clone() });
    let asked = Arc::new(AtomicU32::new(0));
    let responder = {
        let (door, asked) = (door.clone(), asked.clone());
        node_serving("mesh1.admin.1", move |req, sink| {
            let (door, n) = (door.clone(), asked.fetch_add(1, Ordering::SeqCst));
            async move {
                if n == 0 {
                    // The first read: Started and one chunk, then the stream is cut.
                    let mut sink = sink.started(BuildFactsReply::Started).await.map_err(|e| rafka_node_rpc::HandlerFault::invariant_broken(format!("{e:?}")))?;
                    let (messages, _) = rafka_node_admin_core::fabric_builds::encode_chunks(full_facts(&door).await);
                    sink.data(BuildFactsReply::Facts { build_id: id_of(&req), chunk_index: 0, chunk_count: messages.len() as u32 + 1, facts: messages[0].to_vec() })
                        .await
                        .map_err(|e| rafka_node_rpc::HandlerFault::invariant_broken(format!("{e:?}")))?;
                    return Err(rafka_node_rpc::HandlerFault::invariant_broken("the stream is cut"));
                }
                Ok(door.serve(req, sink).await)
            }
        })
        .await
    };
    let joiner = caller("mesh1.admin.2", &fabric, &[&responder]).await;
    joiner.0.accepted.learn(record_of(&fabric, &id), &*joiner.0.log, "entry").await;

    let cut = fetch_build_facts(&joiner.1.client, &NodeTarget::ExactNode(responder.resolved.node_id.clone()), &id).await.unwrap_err();
    assert!(matches!(cut, FetchFailure::Interrupted(_)), "{cut}");
    assert!(joiner.0.log.facts().await.unwrap().is_empty(), "an interrupted stream absorbs nothing");

    let mut h = hydrator(&joiner, responders(vec![&responder]), None);
    drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("the repeated read hydrates");
    assert_eq!(asked.load(Ordering::SeqCst), 2, "the cut read, then the read made again");
    assert_eq!(joiner.0.accepted.build_id().await, Some(id));
    for n in [&responder, &joiner.1] {
        n.ep.close().await;
    }
}

async fn full_facts(door: &BuildFactsDoor) -> Vec<rafka_node_admin_core::build_state::BuildFact> {
    door.builds.facts().await.unwrap()
}

fn id_of(req: &rafka_node_rpc_contract::build_facts::BuildFactsRequest) -> String {
    let rafka_node_rpc_contract::build_facts::BuildFactsRequest::FetchBuildFacts { build_id } = req;
    build_id.clone()
}

// @feature: node-lifecycle
/// CONTRACT: the fabric-primary is asked first. When it cannot answer (it refuses NotReady, or its
/// peer does not resolve), the next known node-admin is asked, and its holdings hydrate the joiner. The
/// fabric-primary is not killed: it is made unreachable by what it answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreachable_fabric_primary_rotates_to_another_node_admin() {
    let fabric = FabricId::mint();
    let id = BuildId::mint();
    let other = admin("mesh1.admin.2", &fabric).await;
    holds(&other, &id, 1, true).await;
    let not_ready = node_serving("mesh1.admin.1", |_, _| async { Ok(BuildFactsReply::NotReady { reason: "the fabric-primary holds no Build state yet".into() }) }).await;
    let second = node_of(&other).await;
    let joiner = caller("mesh1.admin.3", &fabric, &[&not_ready, &second]).await;
    joiner.0.accepted.learn(record_of(&fabric, &id), &*joiner.0.log, "entry").await;
    let mut h = hydrator(&joiner, responders(vec![&not_ready, &second]), None);
    drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("hydrates from the second");
    assert_eq!(not_ready.calls.load(Ordering::SeqCst), 1, "the fabric-primary was asked first, once");
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);

    // Its peer does not resolve at all: the read ends Unreached and the next responder answers.
    let ghost = Responder { name: "mesh1.admin.9".parse().unwrap(), node_id: NodeId::mint() };
    let joiner2 = caller("mesh1.admin.4", &fabric, &[&second]).await;
    joiner2.0.accepted.learn(record_of(&fabric, &id), &*joiner2.0.log, "entry").await;
    let list = vec![ghost, Responder { name: second.resolved.name.clone(), node_id: second.resolved.node_id.clone() }];
    let responders: Responders = Arc::new(move || {
        let list = list.clone();
        Box::pin(async move { list })
    });
    let mut h2 = hydrator(&joiner2, responders, None);
    drive(&joiner2.0, &mut h2, Duration::from_secs(10)).await.expect("hydrates past the unresolvable fabric-primary");
    for n in [&not_ready, &second, &joiner.1, &joiner2.1] {
        n.ep.close().await;
    }
}

// @feature: node-lifecycle
/// CONTRACT: a same-key restart whose storage holds `Fabric.build_id` but not the Build's facts names
/// the blocker (the pointed Build cannot be read) and fetches the facts; nothing is owed a floor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_that_lost_the_builds_facts_fetches_them() {
    let fabric = FabricId::mint();
    let id = BuildId::mint();
    let full = admin("mesh1.admin.1", &fabric).await;
    holds(&full, &id, 2, true).await;
    let responder = node_of(&full).await;
    let restarted = caller("mesh1.admin.2", &fabric, &[&responder]).await;
    restarted.0.accepted.point(&id, 0, "restart").await.unwrap();
    let mut h = hydrator(&restarted, responders(vec![&responder]), None);
    let seen = drive(&restarted.0, &mut h, Duration::from_secs(10)).await.expect("hydrates");
    assert_eq!(seen, vec!["unreadable-build"]);
    assert_eq!(restarted.0.log.read_build(&id).await.unwrap().attempt, 2);
    for n in [&responder, &restarted.1] {
        n.ep.close().await;
    }
}

// @feature: node-lifecycle
/// CONTRACT: a local Build behind the entry's attempt floor names that blocker; the responder's
/// complete holdings reach the floor and clear it. A responder behind the floor is passed over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_behind_the_entry_floor_is_fetched_up_to_it() {
    let fabric = FabricId::mint();
    let id = BuildId::mint();
    let behind = admin("mesh1.admin.1", &fabric).await;
    holds(&behind, &id, 1, true).await;
    let ahead = admin("mesh1.admin.2", &fabric).await;
    holds(&ahead, &id, 3, true).await;
    let (first, second) = (node_of(&behind).await, node_of(&ahead).await);
    let joiner = caller("mesh1.admin.3", &fabric, &[&first, &second]).await;
    holds(&joiner.0, &id, 1, true).await;
    *joiner.0.floor.lock().unwrap() = Some((id.clone(), 3));
    let mut h = hydrator(&joiner, responders(vec![&first, &second]), None);
    let seen = drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("hydrates");
    assert_eq!(seen, vec!["behind-floor"]);
    assert_eq!(joiner.0.log.read_build(&id).await.unwrap().attempt, 3);
    assert_eq!(first.calls.load(Ordering::SeqCst), 1, "the responder behind the floor gave facts that did not clear it, and was not asked again");
    for n in [&first, &second, &joiner.1] {
        n.ep.close().await;
    }
}

// @feature: node-lifecycle
/// CONTRACT: with no pointer and no BuildId parked, nothing is fetched: the control entry is
/// retrieved again, and only once that names a Build is its facts read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unidentified_build_is_never_fetched_the_entry_is_retrieved_again() {
    let fabric = FabricId::mint();
    let id = BuildId::mint();
    let full = admin("mesh1.admin.1", &fabric).await;
    holds(&full, &id, 1, true).await;
    let responder = node_of(&full).await;
    let joiner = caller("mesh1.admin.2", &fabric, &[&responder]).await;
    let entries = Arc::new(AtomicU32::new(0));
    let entry: rafka_node_admin_core::hydrate::EntryRepeat = {
        let (entries, accepted, log, fabric, id) = (entries.clone(), joiner.0.accepted.clone(), joiner.0.log.clone(), fabric.clone(), id.clone());
        Arc::new(move || {
            let (entries, accepted, log, fabric, id) = (entries.clone(), accepted.clone(), log.clone(), fabric.clone(), id.clone());
            Box::pin(async move {
                // The first retrievals find the maker without a pointer; the third names the Build.
                if entries.fetch_add(1, Ordering::SeqCst) >= 2 {
                    accepted.learn(record_of(&fabric, &id), &*log, "entry-repeat").await;
                }
                Ok(())
            })
        })
    };
    let mut h = hydrator(&joiner, responders(vec![&responder]), Some(entry));
    let seen = drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("hydrates once the entry names the Build");
    assert_eq!(seen, vec!["no-pointer", "no-pointer-wanted"]);
    assert!(entries.load(Ordering::SeqCst) >= 3, "the entry was retrieved until it named a Build");
    assert_eq!(responder.calls.load(Ordering::SeqCst), 1, "nothing was fetched before the Build was identified");
    for n in [&responder, &joiner.1] {
        n.ep.close().await;
    }
}

#[test]
fn the_backoff_is_bounded() {
    use rafka_node_admin_core::hydrate::{BACKOFF_BASE, BACKOFF_CAP};
    assert!(BACKOFF_BASE < BACKOFF_CAP);
    assert!(BACKOFF_CAP <= Duration::from_secs(5));
}
