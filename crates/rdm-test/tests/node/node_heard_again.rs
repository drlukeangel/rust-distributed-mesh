//! Q87 (Luke, 2026-10-20): every node coming to ready from any other state connects to
//! its node-admin for its cert, its time and its cache, and a ready node may do the same; the
//! `pre_node_ready` hook is where it does that; the one exception is the Day-0 fabric-primary, with no
//! other node to connect to. The pages: node-recovery.md step 1 ("A birth heard again runs its
//! `pre_node_ready` hook again before it is ready"), node-tickle.md ("on `reached` for a node
//! that is off its channel it commands node.mesh.join"), node-mesh-join.md step 10 (the commanded join
//! continues into node.start steps 3 to 5), fabric-node-hooks.md "Where it runs".
//!
//! CONTRACT: in a one-mesh estate (two node-admins, two rpc nodes) an rpc node and the node-admin that
//! is not the mesh-primary each have their exact runtime hung (every task but the leader in a tracer's
//! stop, so the provider reads it `running`: a SIGSTOP reads `os-stopped`, which node investigation's
//! inspect takes as its verdict and recovery re-creates, Q273) past the staleness floor and released. While held, the view marks the birth `PendingReconnect` (or
//! `Dead`, the ladder's TRUE OFFLINE); nothing re-births it or terminates it (silence is not death).
//! Once released, the mesh-primary's tickle reaches it, the mesh-primary begins the birth's next
//! cycle (`via-cycle-begun`) and commands `node.mesh.join` (`via-mesh-join-commanded`, answered
//! `mesh-joined`); the node runs the start steps of node-start.md under one
//! `rdm.mesh.node.update.via-start` root, the same steps as its own first start ran under its boot
//! span and in the same order (a birth whose kind registered no hook has no hook attempt at either
//! start), each step after the one before it closed; the accepting
//! authority issues it a fresh member cert at that join (`cert.create.via-join` after the release);
//! the view holds the birth Pending and then Ready again, the same NodeId and incarnation.
//! What must NOT happen: a new birth at the path, a terminate (`via-path-investigation outcome=unreached`: the previous birth is deleted, its runtime terminated), a
//! `node.create` for it, a start step missing, outside its root or out of order, or a ready
//! with no hook attempt before it.
use rdm_client::NodeStatus;
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::process_faults::{ExactRuntime, Fault};
use serde_json::Value;
use std::path::Path;
use std::time::Duration;

/// The start steps, in the order every start path runs them (node-start.md steps 2-5).
const STEPS: [&str; 7] = [
    "rdm.mesh.node.update.via-join-admitted",
    "rdm.mesh.node.update.via-topology-joined",
    "rdm.mesh.node.update.via-pre-node-ready-attempt",
    "rdm.mesh.node.update.via-topology-taken",
    "rdm.mesh.node.update.via-build-floor",
    "rdm.mesh.node.update.via-rafka-time-refreshed",
    "rdm.mesh.node.update.via-ready",
];
const START_ROOT: &str = "rdm.mesh.node.update.via-start";
const FIRST_ROOT: &str = "rdm.mesh.node.create.via-boot";

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "node-recovery".into(),
        subfeature: "heard-again".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_held_node_heard_again_is_commanded_back_through_the_whole_start_sequence".into(),
    }
}

fn owner_of(test: &str) -> Owner {
    Owner { test: test.into(), ..owner() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}
fn at(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}
fn end(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(u64::MAX)
}
fn now_nano() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

/// Hold `node`'s exact runtime past the floor, then release it; the instant of the release.
async fn hold_past_the_floor_then_release(estate: &Estate, node: &str, floor: Duration) -> u64 {
    let before = estate.node_view(node).await;
    assert_eq!(before.status, NodeStatus::Ready, "{node} is ready before the hold: {before:?}");
    let rt = ExactRuntime::published(Path::new(&estate.data_dir(node).await)).expect("the node's published runtime");
    let hung = rt.hang().expect("the hang is acknowledged by the OS");
    estate
        .until_node(node, &format!("{node} is marked unheard in the view"), floor * 3 + Duration::from_secs(30), |n| {
            n.filter(|n| matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead)).cloned()
        })
        .await;
    tokio::time::sleep(floor).await;
    let during = estate.node_view(node).await;
    assert_eq!((&during.node_id, &during.incarnation_id), (&before.node_id, &before.incarnation_id), "{node} is the same birth while held: silence is not death");
    let release_at = now_nano();
    hung.release();
    // The released birth's first digest still says Ready (its own stale state): the re-entry
    // is the mesh-primary's commanded join, which begins the birth's next cycle; wait for that, then
    // for the birth ready again as the same birth.
    wait_for(&format!("the mesh-primary begins {node}'s next cycle after the release"), floor * 8 + Duration::from_secs(60), || async {
        named(&estate.spans(), "rdm.node_admin.status.update.via-cycle-begun").into_iter().any(|sp| s(&sp["attributes"]["node"]) == node && at(sp) >= release_at).then_some(())
    })
    .await;
    let back = estate
        .until_node(node, &format!("{node} is ready again as the same birth"), floor * 8 + Duration::from_secs(60), |n| {
            n.filter(|n| n.status == NodeStatus::Ready && n.node_id == before.node_id && n.incarnation_id == before.incarnation_id).cloned()
        })
        .await;
    assert_eq!(back.incarnation_id, before.incarnation_id, "{node} came back as itself");
    release_at
}

/// The start steps under `node`'s start roots named `root` that began after `since`, in start order,
/// with each step's (name, start, end).
fn start_steps_after<'a>(spans: &'a [Value], root: &str, node: &str, since: u64) -> (Vec<&'a Value>, Vec<(String, u64, u64)>) {
    let roots: Vec<&Value> = named(spans, root).into_iter().filter(|sp| s(&sp["attributes"]["node"]) == node && at(sp) >= since).collect();
    let mut steps: Vec<(String, u64, u64)> = roots
        .iter()
        .flat_map(|root| spans.iter().filter(move |sp| sp["parent_span_id"] == root["span_id"] && STEPS.contains(&s(&sp["name"]).as_str())).map(|sp| (s(&sp["name"]), at(sp), end(sp))))
        .collect();
    steps.sort_by_key(|(_, start, _)| *start);
    (roots, steps)
}

fn after(spans: &[Value], name: &str, node: &str, since: u64) -> Vec<Value> {
    named(spans, name).into_iter().filter(|sp| s(&sp["attributes"]["node"]) == node && at(sp) >= since).cloned().collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_node_heard_again_is_commanded_back_through_the_whole_start_sequence() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    estate.settled_views(&[("mesh1", 2, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let floor = rdm_transport::topology::staleness_floor();

    // An rpc node, then the node-admin that is not the mesh-primary: every node follows the path.
    let rpc_node_id = estate.node_view("mesh1.rpc.2").await.node_id.to_string();
    let rpc_released_at = hold_past_the_floor_then_release(&estate, "mesh1.rpc.2", floor).await;
    let admin_released_at = hold_past_the_floor_then_release(&estate, "mesh1.admin.2", floor).await;

    estate.stop().await;
    let spans = estate.spans();
    for (node, since) in [("mesh1.rpc.2", rpc_released_at), ("mesh1.admin.2", admin_released_at)] {
        // The mesh-primary: the tickle answered, the next cycle begun, node.mesh.join commanded and answered.
        let begun = after(&spans, "rdm.node_admin.status.update.via-cycle-begun", node, since);
        assert!(!begun.is_empty(), "{node}: the mesh-primary begins the heard-again birth's next cycle (via-cycle-begun) after the release");
        let commanded = after(&spans, "rdm.node_admin.node.update.via-mesh-join-commanded", node, since);
        assert!(
            commanded.iter().any(|sp| s(&sp["attributes"]["outcome"]) == "mesh-joined"),
            "{node}: the mesh-primary commands node.mesh.join to the heard-again birth and it answers mesh-joined (node-tickle.md); commanded: {commanded:?}"
        );
        // The node: the whole start sequence under one start root, the steps its first start ran, in
        // order, each step after the last closed.
        let (first_roots, first) = start_steps_after(&spans, FIRST_ROOT, node, 0);
        assert_eq!(first_roots.len(), 1, "{node}: one first start ({FIRST_ROOT})");
        let first_names: Vec<&str> = first.iter().map(|(n, _, _)| n.as_str()).collect();
        assert!(first_names.len() >= 6 && first_names.iter().all(|n| STEPS.contains(n)), "{node}: the first start ran the start steps: {first_names:?}");
        let (roots, steps) = start_steps_after(&spans, START_ROOT, node, since);
        assert_eq!(roots.len(), 1, "{node}: one start root ({START_ROOT}) after the release: {}", roots.len());
        let names: Vec<&str> = steps.iter().map(|(n, _, _)| n.as_str()).collect();
        assert_eq!(names, first_names, "{node}: the heard-again start runs the start steps its first start ran, in their order (node-start.md); got {names:?}");
        let overlaps: Vec<String> = steps.windows(2).filter(|w| w[1].1 < w[0].2).map(|w| format!("{} began before {} closed", w[1].0, w[0].0)).collect();
        assert!(overlaps.is_empty(), "{node}: each start step begins only after the one before it closed: {overlaps:?}");
        // The authority: the birth's next cycle begins at its join, never before the command reached it
        // (node-start.md step 2: no state is written before the command is accepted), and the birth is
        // published Pending at that join and ready only once its hook has passed
        // (node-mesh-join.md step 10): its peers hold it off ready while its gate is closed.
        let joins = after(&spans, "rdm.node_admin.node.update.via-join", node, since);
        let joined_at = joins.iter().filter(|sp| s(&sp["attributes"]["outcome"]) == "installed").map(at).min().unwrap_or_else(|| panic!("{node}: the heard-again join is served (via-join installed) after the release: {joins:?}"));
        let begun_at = begun.iter().map(at).min().unwrap();
        assert!(begun_at >= joined_at, "{node}: the next cycle begins at the join the command made ({joined_at}), not before it ({begun_at}): no state is written before the command is accepted (node-start.md step 2)");
        let admitted = after(&spans, "rdm.node_admin.status.update.via-mesh-join-admitted", node, since);
        let pending_at = admitted.iter().filter(|sp| matches!(s(&sp["attributes"]["outcome"]).as_str(), "applied" | "already-applied")).map(at).min();
        // The Pending publish is the admission's (the join's report reaches the authority before its
        // install span closes the serve), so it is read after the command, not against the install's clock.
        assert!(pending_at.is_some_and(|p| p >= since), "{node}: the authority publishes the heard-again birth Pending at its join (node-mesh-join.md step 10; via-mesh-join-admitted applied, after the release {since}): {admitted:?}");
        let _ = joined_at;
        // The authority's record of the birth's own ready declaration (its `sender`).
        let ready_at = named(&spans, "rdm.node_admin.status.update.via-declaration").into_iter().filter(|sp| s(&sp["attributes"]["sender"]) == node && s(&sp["attributes"]["state"]) == "Ready" && s(&sp["attributes"]["outcome"]) == "applied" && at(sp) >= since).map(at).min();
        assert!(ready_at.is_some_and(|r| r > pending_at.unwrap()), "{node}: ready is declared only after the Pending publish: pending {pending_at:?}, ready {ready_at:?}");
        // What a peer holds of the birth never goes back. Gossip is eventual and unordered (a cell never
        // asserts when a copy arrives, c6a8211f), so a copy of an older digest of the birth can reach a
        // peer late, before or after the re-entry: the receive rule refuses it (`via-stale` below the
        // held digest_seq, `via-duplicate` at it) and installs only a newer one. At each peer, for the
        // birth's incarnation, the digest_seq of every digest it installed rises in the order it
        // installed them, and every late copy it refused names an offered sequence below the one it held.
        if node == "mesh1.rpc.2" {
            let seq_of = |v: &str| v.rsplit('=').next().unwrap_or_default().parse::<u64>().unwrap_or(0);
            let mut installed: std::collections::BTreeMap<(String, String), Vec<(u64, u64)>> = Default::default();
            for sp in named(&spans, "rdm.mesh.topology.update.via-digest").into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == rpc_node_id) {
                let seq = s(&sp["attributes"]["digest_seq"]).parse::<u64>().unwrap_or(0);
                installed.entry((s(&sp["attributes"]["node"]), s(&sp["attributes"]["incarnation_id"]))).or_default().push((at(sp), seq));
            }
            assert!(!installed.is_empty(), "{node}: its peers installed its digests (via-digest naming node_id {rpc_node_id})");
            let back: Vec<String> = installed
                .iter_mut()
                .flat_map(|((peer, incarnation), seqs)| {
                    seqs.sort();
                    seqs.windows(2).filter(|w| w[1].1 <= w[0].1).map(|w| format!("{peer} ({incarnation}): digest_seq {} installed after {}", w[1].1, w[0].1)).collect::<Vec<_>>()
                })
                .collect();
            assert!(back.is_empty(), "{node}: a late copy of an older digest never takes back what a peer holds: {back:?}");
            let subject = format!("{rpc_node_id}/");
            let refused: Vec<(String, u64, u64)> = named(&spans, "rdm.mesh.topology.reject.via-stale")
                .into_iter()
                .filter(|sp| s(&sp["attributes"]["frame"]) == "digest" && s(&sp["attributes"]["subject"]).starts_with(&subject))
                .map(|sp| (s(&sp["attributes"]["node"]), seq_of(&s(&sp["attributes"]["held_key"])), seq_of(&s(&sp["attributes"]["frame_key"]))))
                .collect();
            assert!(refused.iter().all(|(_, held, offered)| offered < held), "{node}: each late copy a peer refused as stale offered a digest_seq below the one it held: {refused:?}");
            let copies = named(&spans, "rdm.mesh.topology.update.via-duplicate").into_iter().filter(|sp| s(&sp["attributes"]["frame"]) == "digest" && s(&sp["attributes"]["identity"]).starts_with(&subject)).count();
            eprintln!("{node}: late copies refused, stale {} and duplicate {copies}; installed per (peer, incarnation): {:?}", refused.len(), installed.iter().map(|(k, v)| (k.clone(), v.len())).collect::<Vec<_>>());
        }
        // The cert: issued again for the same birth at the heard-again join.
        let certs = after(&spans, "rdm.node_admin.cert.create.via-join", node, since);
        assert!(!certs.is_empty(), "{node}: the accepting authority issues a fresh member cert at the heard-again join");
        // Must NOT: a terminate, a new birth, a create.
        let terminated = named(&spans, "rdm.node_admin.deployment.delete.via-path-investigation").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == node && s(&sp["attributes"]["outcome"]) == "unreached").count();
        assert_eq!(terminated, 0, "{node}: a held runtime is never terminated");
        let created = after(&spans, "rdm.node_admin.node.create.via-build", node, since);
        assert!(created.is_empty(), "{node}: no new birth at the path: {created:?}");
    }
}

/// CONTRACT (node-mesh-join.md step 10, Q97, Q99: one Pending per accepted join report, per join and
/// not per birth): a birth held past the floor and released is commanded back and ready again; held
/// and released a second time in the same Build attempt, it is commanded back again and is ready
/// again. Each command back is its own command: the second join's ready declaration is
/// applied, never answered AlreadyApplied from the first join's, so the birth never stays Pending
/// after its second return. What must NOT happen: the birth left Pending after its second return.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_held_twice_is_commanded_back_and_ready_after_each_release() {
    let mut estate = Estate::bootstrap(owner_of("a_node_held_twice_is_commanded_back_and_ready_after_each_release"), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    estate.settled_views(&[("mesh1", 2, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let floor = rdm_transport::topology::staleness_floor();
    let first = hold_past_the_floor_then_release(&estate, "mesh1.rpc.2", floor).await;
    let second = hold_past_the_floor_then_release(&estate, "mesh1.rpc.2", floor).await;
    estate.stop().await;
    let spans = estate.spans();
    let applied_after = |since: u64| {
        named(&spans, "rdm.node_admin.status.update.via-declaration")
            .into_iter()
            .any(|sp| s(&sp["attributes"]["sender"]) == "mesh1.rpc.2" && s(&sp["attributes"]["state"]) == "Ready" && s(&sp["attributes"]["outcome"]) == "applied" && at(sp) >= since)
    };
    assert!(applied_after(first), "mesh1.rpc.2: its ready declaration after the first release is applied");
    assert!(applied_after(second), "mesh1.rpc.2: its ready declaration after the second release is applied, not answered from the first join's");
}

/// CONTRACT (node-recovery.md step 2, "never to a birth with a command of its authority open or in
/// flight"): a command is open from its send to its completion, not for the life of the admin. An rpc
/// node stopped and started by Builds (node.stop then node.start, the same node.id and incarnation)
/// and, once ready again, held past the staleness floor and released, is commanded back through
/// node.mesh.join like any other heard-again birth: its stop and start completed long before. What must
/// NOT happen: the command refused `command-open` for a completed stop or start, or the birth left on
/// its channels with its hook never run again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_birth_stopped_and_started_by_builds_and_held_later_is_still_commanded_back() {
    let mut estate = Estate::bootstrap(owner_restarted(), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    estate.settled_views(&[("mesh1", 2, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let floor = rdm_transport::topology::staleness_floor();
    let node = "mesh1.rpc.2";
    let before = estate.node_view(node).await;
    estate.stop_then_start(&node.parse().unwrap()).await;
    let restarted = estate
        .until_node(node, &format!("{node} is ready again after its stop and start"), Duration::from_secs(120), |n| n.filter(|n| n.status == NodeStatus::Ready).cloned())
        .await;
    assert_eq!((&restarted.node_id, &restarted.incarnation_id), (&before.node_id, &before.incarnation_id), "a stop then a start keep the node.id and the incarnation");
    tokio::time::sleep(floor).await;
    let released_at = hold_past_the_floor_then_release(&estate, node, floor).await;
    estate.stop().await;
    let spans = estate.spans();
    let commanded = after(&spans, "rdm.node_admin.node.update.via-mesh-join-commanded", node, released_at);
    let outcomes: Vec<String> = commanded.iter().map(|sp| s(&sp["attributes"]["outcome"])).collect();
    assert!(outcomes.iter().any(|o| o == "mesh-joined"), "{node}: commanded back after its stop and start completed and it was heard again; outcomes {outcomes:?}");
    assert!(!outcomes.iter().any(|o| o.starts_with("command-open")), "{node}: the stop's and the start's commands completed long before: none is open (node-recovery.md step 2); outcomes {outcomes:?}");
}

fn owner_restarted() -> Owner {
    Owner { test: "a_birth_stopped_and_started_by_builds_and_held_later_is_still_commanded_back".into(), ..owner() }
}

/// The pid of the one process of this estate launched as `node` (its environment names the node
/// and the estate), `None` while it is not yet launched.
fn launched_pid(node: &str, root: &Path) -> Option<u32> {
    let name = format!("RDM_NODE_NAME={node}");
    std::fs::read_dir("/proc").ok()?.flatten().find_map(|e| {
        let pid = e.file_name().to_str()?.parse::<u32>().ok()?;
        let env = std::fs::read(format!("/proc/{pid}/environ")).ok()?;
        (env.split(|b| *b == 0).any(|var| var == name.as_bytes()) && rdm_test::estate::environ_names_estate(&env, root)).then_some(pid)
    })
}

/// CONTRACT (Q87: a birth with a lifecycle command of its authority in flight is that command's, never
/// commanded back; node-create.md step 3: the create composes the whole node.start, and `node.created`
/// follows it): a node being created is its create's. The mesh-primary's tickle lists a planned birth
/// silent until its JoinNode is admitted; a launch held at its start (the provider's `Stop`, as a slow
/// launch is) is planned and silent through several ticks, and its report, once released, is the
/// heard change the tickle runs on. That birth is not commanded node.mesh.join while its first start
/// runs, before its first `via-ready`: nothing is sent to it, and any look at it names the create in
/// flight. Must NOT happen: a `via-mesh-join-commanded` for the birth before its first `via-ready` whose
/// outcome is not the create named in flight (a join commanded to a birth whose first start is
/// running, which then ends without node.started).
// @feature: node-lifecycle
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_being_created_is_not_commanded_back_while_its_first_start_runs() {
    let mut estate = Estate::bootstrap_with_env(owner_of("a_node_being_created_is_not_commanded_back_while_its_first_start_runs"), "fabric1", "mesh1", &[("RDM_GOSSIP_INTERVAL_MS", "200")]).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    estate.settled_views(&[("mesh1", 2, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let node = "mesh1.rpc.3";
    let root = estate.bootstrap_data_dir("mesh1").parent().expect("the estate root holds the bootstrap admin's data dir").to_path_buf();
    // The launch under test is held the instant it exists, before its JoinNode, through five ticks of
    // the mesh-primary (200 ms each): a planned birth in its view, silent; then released.
    let hold = async {
        let pid = loop {
            if let Some(pid) = launched_pid(node, &root) {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        };
        let rt = ExactRuntime { deployment_id: String::new(), control_domain: rdm_test::process_faults::local_control_domain(), pid, start: rdm_test::process_faults::start_token(pid).expect("the launched process has a start token") };
        rt.apply(Fault::Stop).expect("the hold is acknowledged by the provider");
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        rt.apply(Fault::Continue).expect("the release is acknowledged by the provider");
    };
    let (_, ()) = tokio::join!(estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]), hold);
    estate.until_node(node, &format!("{node} is ready"), Duration::from_secs(120), |n| n.filter(|n| n.status == NodeStatus::Ready).cloned()).await;
    estate.stop().await;
    let spans = estate.spans();
    let first_ready = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == node).map(at).min().unwrap_or_else(|| panic!("{node}: its first start reached node.started (via-ready)"));
    let looked: Vec<String> = named(&spans, "rdm.node_admin.node.update.via-mesh-join-commanded").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == node && at(sp) < first_ready).map(|sp| s(&sp["attributes"]["outcome"])).collect();
    let returned = named(&spans, "rdm.node_admin.node.update.via-offline-returned").len();
    eprintln!("{node}: via-mesh-join-commanded before its first via-ready: {looked:?}; via-offline-returned spans in the run: {returned}");
    let sent: Vec<&String> = looked.iter().filter(|outcome| !outcome.starts_with("create-in-flight")).collect();
    assert!(sent.is_empty(), "{node}: no node.mesh.join is commanded to a birth whose first start is running (its create is in flight); commanded before its first via-ready with outcomes {sent:?}");
}
