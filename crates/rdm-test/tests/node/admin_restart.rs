//! Node-admin restart: product=mesh, feature=node-lifecycle, subfeature=admin-restart, rung=MM.
//! Spec: `docs/architecture/lifecycles/node-restart.md`, Master Event
//! Matrix (restart: executing Mesh admin = owning mesh-admin; dissemination = own Mesh channel,
//! then the backbone aggregate), §13, §17 "Restart Evidence".

use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, Door};
use serde_json::Value;
use std::collections::BTreeSet;
use std::time::Duration;

fn owner() -> Owner {
    owner_of("a_peer_mesh_admin_restart_is_published_by_its_own_mesh_primary_and_reaches_the_other_mesh_through_the_aggregate")
}

fn owner_of(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "node-lifecycle".into(),
        subfeature: "admin-restart".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn mesh(name: &str) -> MeshDesired {
    MeshDesired::of(name, [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)])
}

/// Every span the process `pid` wrote.
fn spans_of(estate: &Estate, pid: u64) -> Vec<Value> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(&estate.evidence).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with(".spans.jsonl") && name.contains(&format!(".{pid}-")) {
            out.extend(std::fs::read_to_string(e.path()).unwrap_or_default().lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()));
        }
    }
    out
}

/// CONTRACT: a node-admin of a mesh that does not hold the fabric seat is restarted through the
/// `node.restart` call to the fabric-primary. The restart is executed by that mesh's own primary admin, never by the
/// fabric primary: the primary of the target's mesh is the only process that publishes
/// `NodeRestarting` for it (on its own mesh channel). The fabric primary's mesh hears the target
/// mesh's aggregate on the backbone, and its members hear it from their primary's forward.
///
/// Adversarial (R-NS2: "Final aggregates and leadership announcements are best-effort dissemination.
/// A stop does not wait for their delivery. Committed lifecycle facts remain recoverable after the
/// sender cuts gossip, and the executing authority updates its own view from the matching completion
/// and publishes that fact as its author: the mesh-primary for a member"): every aggregate of the
/// target's mesh that carries the restart in flight is withheld from the backbone (the fault door's
/// `announcement` cut on the target's mesh primary), as a backbone neighbour's stop loses what gossip
/// had not yet pushed. The other mesh converges on the restart's outcome from the mesh primary's own
/// later aggregate: the target the same birth, live again, the restart no longer in flight.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_mesh_admin_restart_is_published_by_its_own_mesh_primary_and_reaches_the_other_mesh_through_the_aggregate() {
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1"), mesh("mesh2")] }).await;
    let names: BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| [format!("{m}.admin.1"), format!("{m}.admin.2"), format!("{m}.rpc.1"), format!("{m}.rpc.2")]).collect();
    let before = Estate::values(&estate.settles_to_named(&names, Duration::from_secs(60)).await);
    // The target's mesh is the one that does not hold the fabric seat.
    let fabric_primary = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let (home, away) = if fabric_primary.starts_with("mesh1.") { ("mesh1", "mesh2") } else { ("mesh2", "mesh1") };
    let primary_of = |m: &str| before.iter().find(|n| n["mesh"] == m && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).expect("a mesh primary");
    let (away_primary, home_primary) = (primary_of(away), primary_of(home));
    let target = before.iter().find(|n| n["mesh"] == away && n["kind"] == "node_admin" && s(&n["name"]) != away_primary).map(|n| s(&n["name"])).expect("a non-primary admin");
    let target_birth = before.iter().find(|n| s(&n["name"]) == target).map(|n| (s(&n["node_id"]), s(&n["incarnation_id"]))).expect("the target's birth");
    assert_ne!(away_primary, fabric_primary, "the target's mesh primary is not the fabric primary");

    // The restart outruns gossip, so every node must hold the target before it begins: each rpc node reports,
    // through its held view, that it holds the target's mesh including the target.
    {
        let peers = estate.peers().await;
        let views = estate.node_views().await;
        let target_view = views.iter().find(|n| n.name.to_string() == target).expect("node.get lists the target").clone();
        for peer in views.iter().filter(|n| n.name != target_view.name && n.kind == rdm_client::LaunchKind::RpcNode) {
            peers.fact(peer, &target_view.mesh, "it holds the restart target's mesh including the target", |v| v.birth(&target_view.node_id).is_some()).await;
        }
    }
    // The initial fabric refuses a node restart until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let mut pids = std::collections::BTreeMap::new();
    for n in &names {
        let pid = match estate.bootstrap_pid() {
            Some(p) if n == "mesh1.admin.1" => u64::from(p),
            _ => estate.pid_of(n).await,
        };
        pids.insert(n.clone(), pid);
    }
    // The target's mesh primary withholds from the backbone every aggregate of its mesh that carries
    // a lifecycle operation in flight: no version carrying the restart in flight reaches a peer mesh.
    let door = Door::at(&estate, &away_primary).await;
    door.arm("aggregate-in-flight", serde_json::json!({"kind": "announcement", "announcement": "aggregate-in-flight", "channel": "backbone"})).await;
    let mut restart = estate.nodes_obj().await.restart(&target.parse().unwrap_or_else(|e| panic!("{target} is not a path.name: {e:?}"))).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.restart of {target}: {e}"));
    Estate::complete(&mut restart).await;
    let operation = format!("node.restart:{target}");
    // The restart's outcome reaches the other mesh from its mesh primary's own later aggregate: every
    // ordinary node of the other mesh holds the target as the same birth, with no restart in flight.
    {
        let peers = estate.peers().await;
        let views = estate.node_views().await;
        let target_view = views.iter().find(|n| n.name.to_string() == target).expect("node.get lists the target").clone();
        assert_eq!((target_view.node_id.to_string(), target_view.incarnation_id.as_ref().map(|i| i.as_str().to_string()).unwrap_or_default()), target_birth.clone(), "the restart keeps the target's birth");
        for peer in views.iter().filter(|n| n.mesh == home && n.kind == rdm_client::LaunchKind::RpcNode) {
            peers
                .fact(peer, &target_view.mesh, "it holds the restarted target as the same birth, live, with no restart in flight", |v| {
                    v.birth(&target_view.node_id).is_some_and(|d| d.node.incarnation.as_str() == target_birth.1 && d.status == rdm_core::MemberStatus::ReadyForTraffic) && v.overlay(&target_view.node_id, "node.restart:").is_none()
                })
                .await;
        }
    }
    // The other mesh's primary holds the target mesh's aggregate past the restart frame's version, with nothing in
    // flight, before the estate stops: the restart's end is a material change its mesh primary sends on the backbone
    // at once, and anti-entropy sends it again every backbone interval (gossip.md §3.2). The ordinary nodes' fact
    // above does not wait for it: the door withheld every aggregate carrying the restart, so they never held it in
    // flight. Bounded by the staleness floor.
    let frame_version: u64 = wait_for(&format!("{away_primary}'s NodeRestarting frame of {operation} is in its evidence"), Duration::from_secs(10), || {
        let spans = spans_of(&estate, pids[&away_primary]);
        let version = named(&spans, "rdm.mesh.membership.update.via-status-event")
            .into_iter()
            .find(|x| x["attributes"]["frame"] == "node-restarting" && x["attributes"]["operation"] == operation.as_str())
            .and_then(|x| x["attributes"]["topology_version"].as_str().and_then(|v| v.parse().ok()));
        async move { version }
    })
    .await;
    {
        let peers = estate.peers().await;
        let views = estate.node_views().await;
        let home_view = views.iter().find(|n| n.name.to_string() == home_primary).unwrap_or_else(|| panic!("node.get lists {home_primary}")).clone();
        let carrier = peers.carrier_at(home_view.name.clone());
        let rpc = rdm_client::NodeRpc::new(carrier.client());
        let at = rdm_transport::NodeTarget::ExactNode(home_view.node_id.clone());
        wait_for(&format!("{home_primary} holds {away}'s aggregate past v{frame_version} with nothing in flight"), rdm_transport::membership::staleness_floor(), || async {
            let frames = rpc.topology_get(&at, Some(away.to_string()), None, &rdm_transport::CallOptions::default()).await.ok()?;
            frames
                .iter()
                .any(|f| matches!(f, rdm_core::topology::TopologyReply::Snapshot { mesh, topology_version, in_flight, .. } if mesh.as_str() == away && *topology_version > frame_version && in_flight.is_empty()))
                .then_some(())
        })
        .await;
    }
    door.release("aggregate-in-flight").await;
    estate.stop().await;

    let start = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or_default();
    let of_op = |sp: &[Value], name: &str| -> Vec<Value> { named(sp, name).into_iter().filter(|x| x["attributes"]["operation"] == operation.as_str()).cloned().collect() };

    // Publisher: the target mesh's primary, alone (Matrix: restart / owning mesh-admin; §17 "PUBLISHER").
    let published: Vec<(String, Value)> =
        pids.iter().flat_map(|(n, pid)| of_op(&spans_of(&estate, *pid), "rdm.node_admin.node.update.via-node-restarting").into_iter().map(|x| (n.clone(), x)).collect::<Vec<_>>()).collect();
    assert_eq!(published.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), vec![away_primary.as_str()], "only {away_primary}, the primary of {away}, publishes NodeRestarting for {target}: {published:?}");
    let publish = &published[0].1;
    assert_eq!(publish["attributes"]["node"], target.as_str());
    let trace_id = s(&publish["trace_id"]);
    estate.record_trace_url(&trace_id);
    eprintln!("RESTART-TRACE {trace_id} publisher={away_primary} target={target} fabric_primary={fabric_primary}");

    // The withheld aggregates: the target's mesh primary offered its mesh's aggregate with the restart in
    // flight, and none of it reached the backbone.
    let withheld: Vec<Value> = named(&spans_of(&estate, pids[&away_primary]), "rdm.mesh.backbone.update.via-announcement-withheld").into_iter().filter(|x| x["attributes"]["announcement"] == "aggregate-in-flight").cloned().collect();
    assert!(!withheld.is_empty(), "{away_primary} withheld its mesh's aggregate carrying the restart in flight from the backbone");
    // Own mesh channel: the target mesh's rpc nodes hear it.
    for n in names.iter().filter(|n| n.starts_with(away) && n.contains(".rpc.")) {
        let heard = of_op(&spans_of(&estate, pids[n]), "rdm.mesh.membership.update.via-node-restarting");
        assert_eq!(heard.len(), 1, "{n} (mesh {away}) hears NodeRestarting once on its own mesh channel: {heard:?}");
    }
    // The backbone carries it as the target mesh's aggregate. The order is the doc's (gossip.md NodeRestarting: `topology_version`
    // is "the version of the subject's mesh at which the subject changed"): the restart step's receipt puts the hold into the
    // primary's book, the aggregate takes the version at which the subject changed and forwards, and the frame is published
    // carrying that version. The version bump therefore precedes the publisher's span, so every later check anchors on the
    // frame's version, never on wall-clock order against the publisher's span.
    let away_spans = spans_of(&estate, pids[&away_primary]);
    let frames: Vec<&Value> = named(&away_spans, "rdm.mesh.membership.update.via-status-event").into_iter().filter(|x| x["attributes"]["frame"] == "node-restarting" && x["attributes"]["operation"] == operation.as_str()).collect();
    assert_eq!(frames.len(), 1, "{away_primary} publishes the NodeRestarting frame of {operation} once: {frames:?}");
    let version = s(&frames[0]["attributes"]["topology_version"]);
    assert!(!version.is_empty(), "the NodeRestarting frame carries the version of {away}'s aggregate at which {target} changed: {}", frames[0]);
    // The frame's version is taken by the publish itself, under the version lock (membership.rs `publish_status_event`), which
    // emits no `via-topology-version` span; that span belongs to the round path. So the proof that the frame's version is
    // the aggregate's is the other primary installing and forwarding the aggregate at exactly that version, below.
    // The other mesh's primary installs that mesh's aggregate from the backbone and forwards it onto its own mesh at a
    // version later than the frame's: the restart's outcome, from its own mesh primary's later aggregate, with nothing in
    // flight. No version carrying the restart in flight reached it (each was withheld above).
    let after = |x: &Value| x["attributes"]["topology_version"].as_str().and_then(|v| v.parse::<u64>().ok()).zip(version.parse::<u64>().ok()).is_some_and(|(v, at)| v > at);
    let carries = |x: &Value| x["attributes"]["in_flight"].as_str().is_some_and(|n| n != "0");
    let home_spans = spans_of(&estate, pids[&home_primary]);
    let installed: Vec<&Value> = named(&home_spans, "rdm.mesh.membership.update.via-snapshot-installed").into_iter().filter(|x| x["attributes"]["mesh"] == away && x["attributes"]["channel"] == "backbone").collect();
    let converged: Vec<&Value> = installed.iter().copied().filter(|x| after(x) && !carries(x)).collect();
    assert!(!converged.is_empty(), "{home_primary} installs {away}'s aggregate from the backbone at a version after {version}, the restart no longer in flight: {installed:?}");
    let in_flight: Vec<&Value> = installed.iter().copied().filter(|x| carries(x) && start(x) >= start(publish)).collect();
    assert!(in_flight.is_empty(), "no aggregate carrying the restart in flight reached {home_primary} from the backbone: {in_flight:?}");
    let forwarded: Vec<&Value> = named(&home_spans, "rdm.mesh.membership.update.via-forwarded-delta").into_iter().filter(|x| x["attributes"]["mesh"] == away && after(x)).collect();
    assert!(!forwarded.is_empty(), "{home_primary} forwards {away}'s delta at a version after {version} onto {home}'s channel");
    eprintln!("RESTART-CONVERGED installed v{} trace {} forwarded v{} trace {} withheld {}", s(&converged[0]["attributes"]["topology_version"]), s(&converged[0]["trace_id"]), s(&forwarded[0]["attributes"]["topology_version"]), s(&forwarded[0]["trace_id"]), withheld.len());
}

/// CONTRACT (node-primary-update.md failure contract, "The successor is unheard after the update": the
/// ordinary election then names whoever it names, the old primary included, and the successor's node
/// goes through node recovery like any unheard member): a mesh-primary node-admin restarted through
/// `node.restart` first hands the mesh-primary role to its mesh's other admin by node.primary.update.
/// When that successor's process is then gone, the election names the restarted admin again, and the
/// successor's recovery completes under it: a new node stands ready at the successor's path.name.
///
/// What must NOT happen (the twelve soaks at 768dfaee, `mock_soak_primaries_preserve_continuous_invariants`
/// round 17, mesh2.admin.2-n88v5zcrvdkd): the restarted primary's mesh-row writes refused, and no
/// re-birth in 150 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_mesh_primary_elected_again_after_its_successor_dies_writes_its_mesh_rows() {
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner_of("a_restarted_mesh_primary_elected_again_after_its_successor_dies_writes_its_mesh_rows"), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"])
        .await
        .expect("the faulted-admin binding set is accepted");
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1"), mesh("mesh2")] }).await;
    let names: BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| [format!("{m}.admin.1"), format!("{m}.admin.2"), format!("{m}.rpc.1"), format!("{m}.rpc.2")]).collect();
    let before = Estate::values(&estate.settles_to_named(&names, Duration::from_secs(60)).await);
    // The initial fabric refuses a restart until it has first reached ready-for-traffic; a kill before
    // that is refused by the estate (Q74).
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // The target mesh is the one that does not hold the fabric seat: its primary (the holder) and its
    // other admin (the successor the holder's restart hands the seat to).
    let fabric_primary = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let away = if fabric_primary.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
    let holder = before.iter().find(|n| n["mesh"] == away && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).expect("a mesh primary");
    let successor = before.iter().find(|n| n["mesh"] == away && n["kind"] == "node_admin" && s(&n["name"]) != holder).map(|n| s(&n["name"])).expect("the other admin");
    let successor_birth = before.iter().find(|n| s(&n["name"]) == successor).map(|n| (s(&n["node_id"]), s(&n["incarnation_id"]))).expect("the successor's birth");
    let holder_birth = before.iter().find(|n| s(&n["name"]) == holder).map(|n| (s(&n["node_id"]), s(&n["incarnation_id"]))).expect("the holder's birth");

    // 1. The holder's restart: its seat goes to the successor by node.primary.update, then the same
    //    birth starts again.
    let mut restart = estate.nodes_obj().await.restart(&holder.parse().unwrap_or_else(|e| panic!("{holder} is not a path.name: {e:?}"))).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.restart of {holder}: {e}"));
    Estate::complete(&mut restart).await;
    wait_for(&format!("{successor} holds {away}'s mesh seat and {holder} is back as the same birth, not the primary"), Duration::from_secs(60), || async {
        let nodes = estate.node_values().await;
        let s_ok = nodes.iter().any(|n| s(&n["name"]) == successor && n["is_primary"] == true && n["status"] == "ready-for-traffic");
        let h_ok = nodes.iter().any(|n| s(&n["name"]) == holder && n["is_primary"] == false && n["status"] == "ready-for-traffic" && (s(&n["node_id"]), s(&n["incarnation_id"])) == holder_birth);
        (s_ok && h_ok).then_some(())
    })
    .await;
    let holder_pid = estate.pid_of(&holder).await;
    let handed = wait_for(&format!("{holder}'s evidence records its restart's node.primary.update"), Duration::from_secs(10), || {
        let spans = spans_of(&estate, holder_pid);
        let found = named(&spans, "rdm.node_admin.primary.update.via-update-primary").into_iter().find(|x| s(&x["attributes"]["operation"]).ends_with(&format!("node.restart:{holder}")) && x["attributes"]["outcome"] == "updated").cloned();
        async move { found }
    })
    .await;

    // 2. The successor's process is gone: SIGKILL of its exact runtime, provider-confirmed on the next
    //    inspection.
    let successor_pid = estate.pid_of(&successor).await;
    estate.kill_pid(successor_pid as u32);
    let killed_at = std::time::Instant::now();

    // 3. The ordinary rule seats the holder again, and the successor's recovery completes under it: a
    //    new node ready at the successor's path.name (a new node.id). The bound is the ladder's (a floor
    //    to mark it silent, the tickle, a floor to the second no-path, the provider's proof, the create).
    let recovered = wait_for(&format!("{holder} is {away}'s mesh primary again and a new node stands ready at {successor}"), Duration::from_secs(120), || async {
        let nodes = estate.node_values().await;
        let h_ok = nodes.iter().any(|n| s(&n["name"]) == holder && n["is_primary"] == true && n["status"] == "ready-for-traffic");
        let reborn = nodes.iter().find(|n| s(&n["name"]) == successor && n["status"] == "ready-for-traffic" && s(&n["node_id"]) != successor_birth.0).cloned();
        if h_ok { reborn } else { None }
    })
    .await;
    let recovered_ms = killed_at.elapsed().as_millis();
    estate.stop().await;

    // The holder's own record: the election named it mesh-primary again after its restart's update.
    let spans = spans_of(&estate, holder_pid);
    let start = |x: &Value| x["start_unix_nano"].as_u64().unwrap_or_default();
    let election = named(&spans, "rdm.mesh.seat.update.via-announcement").into_iter().filter(|x| start(x) > start(&handed) && x["attributes"]["seat"] == "mesh-primary" && x["attributes"]["via"] == "own" && x["attributes"]["outcome"] == "held").cloned().collect::<Vec<_>>();
    assert!(!election.is_empty(), "{holder} holds itself {away}'s mesh-primary again by the election after its restart's update: {}", serde_json::to_string(&named(&spans, "rdm.mesh.seat.update.via-announcement")).unwrap_or_default());
    let trace_id = s(&election[0]["trace_id"]);
    estate.record_trace_url(&trace_id);
    eprintln!("SEATED-AGAIN-TRACE {trace_id} holder={holder} successor={successor} reborn={} recovered_ms={recovered_ms}", s(&recovered["node_id"]));
}
