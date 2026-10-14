//! Ruling R-A2 process cells: the fabric-primary seat stays in its mesh until that mesh is proven
//! unable to hold it, and a silent holder is looked at, never replaced for being silent.
//!
//! Each cell runs a two-mesh fabric of two node-admins and three rpc nodes per mesh. A cell that
//! kills the fabric primary is permitted by R-P1; no other cell kills a fabric-primary node-admin.

use rafka_node_admin_client::{BuildSpec, FabricDesired, LaunchKind as NodeKind, MeshDesired};
use rafka_test_scenario::estate::{descends_from, iroh_observation_shape, named, wait_for, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha, Door};
use rafka_test_scenario::netfault::{udp_ports, Partition};
use serde_json::Value;
use std::collections::BTreeSet;
use std::process::Command;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "sticky-seat".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn attr(sp: &Value, k: &str) -> String {
    s(&sp["attributes"][k])
}

/// A numeric span attribute (exported as a string).
fn num(sp: &Value, k: &str) -> Option<i64> {
    attr(sp, k).parse().ok()
}

fn names() -> BTreeSet<String> {
    ["mesh1", "mesh2"].iter().flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain((1..=3).map(move |i| format!("{m}.rpc.{i}")))).collect()
}

/// The caller of a Build: the objects it submitted over, kept so that it can follow the Build to the
/// fabric-primary a stream that ended `SeatMoved` names (node-rpc-envelope.md TakeSeats: "the caller
/// resubmits the same `build_id` to N, and N resumes the same attempt").
struct Caller {
    peers: std::sync::Arc<rafka_node_admin_client::Peers>,
    entry: rafka_node_admin_client::PathName,
    kind: rafka_node_admin_client::WorkflowKind,
    accepted: rafka_node_admin_client::Accepted,
}

/// What the caller read of one Build: its first stream, and, when that ended `SeatMoved`, the Build
/// followed on to its terminal.
struct Followed {
    /// The frames of the stream the call was submitted over, and how it broke before its terminal.
    first: Vec<rafka_node_admin_client::Frame>,
    broke: Option<String>,
    /// The frames of each stream resubmitted after a `SeatMoved`, and the Build's receipts read at the
    /// admin that ended it; the reason when the caller could not follow.
    onward: Result<(Vec<Vec<rafka_node_admin_client::Frame>>, rafka_node_admin_client::BuildReceipts), String>,
    kind: rafka_node_admin_client::WorkflowKind,
    accepted: rafka_node_admin_client::Accepted,
}

/// The admin a stream ended `SeatMoved` names, when it did.
fn seat_moved_to(frames: &[rafka_node_admin_client::Frame]) -> Option<Option<String>> {
    match frames.last() {
        Some(rafka_node_admin_client::Frame::SeatMoved { fabric_primary }) => Some(fabric_primary.clone()),
        _ => None,
    }
}

impl Caller {
    /// The Build followed to its terminal after `first`, the stream that ended `SeatMoved`: the same
    /// `build_id` is resubmitted at the admin each `SeatMoved` names the moment it is named, never a
    /// second Build, until a stream ends with a terminal frame. Returns the frames of each resubmitted
    /// stream and the Build's receipts (`build.get`) read at the admin that ended it.
    async fn follow(&self, first: &[rafka_node_admin_client::Frame]) -> Result<(Vec<Vec<rafka_node_admin_client::Frame>>, rafka_node_admin_client::BuildReceipts), String> {
        use rafka_node_admin_client::{Builds, Frame, Nodes};
        let mut seat = seat_moved_to(first);
        let mut streams = Vec::new();
        let mut at = None;
        while let Some(named) = seat {
            let to: rafka_mesh_entity::PathName = named.ok_or_else(|| format!("a stream ended SeatMoved naming no fabric-primary: {first:?} {streams:?}"))?.parse().map_err(|e| format!("SeatMoved names {e:?}, which is not a path.name"))?;
            let carrier = self.peers.carrier_following(&self.entry, &to).await.map_err(|e| format!("the caller cannot reach {to}, the fabric-primary SeatMoved names: {e}"))?;
            let mut stream = Nodes::new(carrier.clone()).resume(self.kind.clone(), &self.accepted).await.map_err(|e| format!("Build {} was not resumed at {to}: {e}", self.accepted.build_id))?;
            let mut frames: Vec<Frame> = Vec::new();
            seat = loop {
                match stream.next().await {
                    Some(Ok(f)) => {
                        frames.push(f);
                        if frames.last().is_some_and(Frame::is_terminal) {
                            break None;
                        }
                        if let Some(moved) = seat_moved_to(&frames) {
                            break Some(moved);
                        }
                    }
                    Some(Err(e)) => return Err(format!("the stream resumed at {to} broke before its terminal: {e}; its frames: {frames:?}")),
                    None => return Err(format!("the stream resumed at {to} ended with no terminal; its frames: {frames:?}")),
                }
            };
            streams.push(frames);
            at = Some(carrier);
        }
        let carrier = at.ok_or_else(|| format!("the first stream did not end SeatMoved: {first:?}"))?;
        let receipts = Builds::new(carrier).get(&self.accepted.build_id).await.map_err(|e| format!("build.get of {} failed: {e}", self.accepted.build_id))?;
        Ok((streams, receipts))
    }
}

/// The replace recipe of `p` (lifecycles/node-replace.md: one Build of node primitives) submitted to
/// the fabric-primary the admin at `entry` names, over the `node` objects: the Build it runs on, and
/// the caller's reply stream read to its end on a task of its own: its frames, how a stream that
/// broke before its terminal ended, and, when the stream ended `SeatMoved`, the Build followed on at
/// the admin it named as soon as it is named.
async fn replace_by_recipe(estate: &Estate, entry: &rafka_node_admin_client::PathName, p: &str) -> (String, tokio::task::JoinHandle<Followed>) {
    use rafka_node_admin_client::{NodeSelector, Nodes};
    use rafka_node_rpc_contract::build::RecipeStep;
    let peers = estate.peers().await;
    let nodes = Nodes::new(peers.carrier_at(entry.clone()));
    for mesh in ["mesh1", "mesh2"] {
        if let Ok(held) = nodes.get(&NodeSelector::Mesh(mesh.into())).await {
            peers.learn_known(&held);
        }
    }
    let node: rafka_mesh_entity::PathName = p.parse().unwrap();
    let recipe = vec![
        RecipeStep::Create { node: node.successor() },
        RecipeStep::Stop { node: node.clone() },
        RecipeStep::Update { node: node.clone(), name: node.renamed() },
        RecipeStep::Stop { node: node.successor() },
        RecipeStep::Update { node: node.successor(), name: node.clone() },
        RecipeStep::Start { node: node.clone() },
        RecipeStep::Delete { node: node.renamed() },
    ];
    let mut stream = nodes.recipe(recipe).await.expect("the replace recipe was accepted");
    let build = stream.accepted().build_id.to_string();
    let caller = Caller { peers, entry: entry.clone(), kind: stream.kind().clone(), accepted: stream.accepted().clone() };
    let frames = tokio::spawn(async move {
        let mut out = Vec::new();
        loop {
            match stream.next().await {
                Some(Ok(f)) => {
                    let end = f.is_terminal() || matches!(f, rafka_node_admin_client::Frame::SeatMoved { .. });
                    out.push(f);
                    if end {
                        return finished(out, None, caller).await;
                    }
                }
                Some(Err(e)) => return finished(out, Some(format!("{e:?}")), caller).await,
                None => return finished(out, Some("the stream ended with no frame after these".into()), caller).await,
            }
        }
    });
    (build, frames)
}

/// The first stream is over: when it ended `SeatMoved` the caller follows the Build at once.
async fn finished(first: Vec<rafka_node_admin_client::Frame>, broke: Option<String>, caller: Caller) -> Followed {
    let onward = if seat_moved_to(&first).is_some() { caller.follow(&first).await } else { Err("the first stream did not end SeatMoved".into()) };
    Followed { first, broke, onward, kind: caller.kind, accepted: caller.accepted }
}

async fn fabric(cell: &str) -> (Estate, Vec<Value>) {
    fabric_skewing(cell, None).await
}

/// The OS-clock skew one ordinary node of a cell runs under: one hour ahead.
const OS_CLOCK_SKEW_MS: i64 = 3_600_000;

/// [`fabric`] with every node-admin the testkit's faulted node-admin (`faults::binding_set`): its fault
/// door (testkit Node RPC op 0x75) withholds an announcement the cell arms, and nothing else of it
/// differs.
async fn faulted_fabric(cell: &str) -> (Estate, Vec<Value>) {
    let sha = candidate_sha();
    let estate = Estate::bootstrap_external(owner(cell), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    born(estate, None).await
}

/// [`fabric`], with the OS clock of the ordinary node `skewed` (if any) an hour ahead from its start.
async fn fabric_skewing(cell: &str, skewed: Option<&str>) -> (Estate, Vec<Value>) {
    born(Estate::bootstrap(owner(cell), "fabric1", "mesh1").await, skewed).await
}

async fn born(estate: Estate, skewed: Option<&str>) -> (Estate, Vec<Value>) {
    if let Some(node) = skewed {
        let dir = rafka_node_rpc_testkit::os_clock_skew_dir(&estate.root);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(node), OS_CLOCK_SKEW_MS.to_string()).unwrap();
    }
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    let nodes = Estate::values(&estate.settles_to_named(&names(), Duration::from_secs(30)).await);
    // The initial fabric refuses a node replace until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    (estate, nodes)
}

fn node<'a>(nodes: &'a [Value], name: &str) -> &'a Value {
    nodes.iter().find(|n| n["name"] == name).unwrap_or_else(|| panic!("{name} is not in the view"))
}

fn fabric_primary(nodes: &[Value]) -> Option<String> {
    nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"]))
}

async fn kill(estate: &mut Estate, name: &str) {
    if name == "mesh1.admin.1" {
        estate.kill_bootstrap();
    } else {
        let pid = estate.pid_of(name).await;
        assert!(Command::new("kill").args(["-9", &pid.to_string()]).status().unwrap().success(), "kill -9 {pid}");
    }
}

/// CONTRACT: the fabric primary dies while another admin of its mesh survives. The seat goes to
/// that admin, the lowest Ready NodeId of the SAME mesh, whatever the other mesh's primary's id
/// is, and the new holder says so (`rdm.mesh.seat.update.via-announce`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_fabric_primary_hands_the_seat_to_the_surviving_admin_of_its_own_mesh() {
    // One ordinary node of each mesh runs on an OS clock an hour ahead: rafka-time is adopted from
    // the authority and no stamp of any node reads the OS clock.
    let (mut estate, nodes) = fabric_skewing("a_dead_fabric_primary_hands_the_seat_to_the_surviving_admin_of_its_own_mesh", Some("mesh2.rpc.1")).await;
    let old = fabric_primary(&nodes).expect("one fabric primary");
    let mesh = old.split('.').next().unwrap().to_string();
    let heir = if old.ends_with(".1") { format!("{mesh}.admin.2") } else { format!("{mesh}.admin.1") };
    let other_mesh = if mesh == "mesh1" { "mesh2" } else { "mesh1" };
    let other_primary = nodes.iter().find(|n| n["mesh"] == other_mesh && n["is_primary"] == true).map(|n| (s(&n["name"]), s(&n["node_id"]))).unwrap();
    let heir_id = s(&node(&nodes, &heir)["node_id"]);
    let discriminating = other_primary.1 < heir_id;
    estate.enter(&Estate::path(&heir)).await;
    kill(&mut estate, &old).await;
    let seen = estate
        .until_view("the surviving admin of the fabric primary's mesh holds the seat", Duration::from_secs(90), |views| {
            let ns = Estate::values(views);
            (fabric_primary(&ns).as_deref() == Some(heir.as_str())).then_some(ns)
        })
        .await;
    // The seat stays with the heir: the other mesh's primary does not take it back from it.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let later = estate.node_values().await;
    estate.stop().await;
    assert_eq!(fabric_primary(&later).as_deref(), Some(heir.as_str()), "the heir keeps the seat: {later:#?}");
    assert_eq!(seen.iter().filter(|n| n["is_fabric_primary"] == true).count(), 1);
    let spans: Vec<Value> = estate.spans();
    assert!(
        named(&spans, "rdm.mesh.seat.update.via-announce").iter().any(|sp| attr(sp, "node") == heir && attr(sp, "seat") == "fabric-primary" && attr(sp, "holder").contains(&heir_id)),
        "the heir announced the fabric-primary seat (the other mesh's primary id is {} the heir's: {})",
        if discriminating { "below" } else { "above" },
        other_primary.1
    );

    // rafka-time is one lineage across the fabric.
    let adopted = named(&spans, "rdm.mesh.entry.update.via-rafka-time-adopted");
    // The Day-0 root adopted its own OS clock, once, and says why.
    let own: Vec<&&Value> = adopted.iter().filter(|sp| attr(sp, "source") == "own-clock").collect();
    assert_eq!(own.len(), 1, "exactly one own-clock adoption in the fabric: {own:?}");
    assert_eq!((attr(own[0], "node").as_str(), attr(own[0], "reason").as_str()), ("mesh1.admin.1", "day0-root"));
    // Every other node adopted from the answer to the join it made, served by the admin that deployed it.
    let joined: BTreeSet<String> = adopted.iter().filter(|sp| attr(sp, "source") == "pull" && attr(sp, "via") == "join").map(|sp| attr(sp, "node")).collect();
    let expected: BTreeSet<String> = names().into_iter().filter(|n| n != "mesh1.admin.1").collect();
    assert!(expected.is_subset(&joined), "every node but the root adopted at its join; missing {:?}", expected.difference(&joined).collect::<Vec<_>>());
    // The skewed node ran an hour ahead (its boot span says so), and every heartbeat of every node
    // carries rafka-time less the host clock: none is off by the skew, and the nodes stay within a
    // second of one another.
    let boot = named(&spans, "rdm.mesh.node.create.via-deployment");
    assert!(
        boot.iter().any(|sp| attr(sp, "node") == "mesh2.rpc.1" && num(sp, "os_clock_skew_ms") == Some(OS_CLOCK_SKEW_MS)),
        "mesh2.rpc.1 ran on an OS clock {OS_CLOCK_SKEW_MS} ms ahead"
    );
    let beats = named(&spans, "rdm.mesh.node.update.via-heartbeat");
    let skew_of = |sp: &&Value| num(sp, "clock_skew_ms").unwrap_or(i64::MAX);
    assert!(beats.iter().any(|sp| attr(sp, "node") == "mesh2.rpc.1"), "mesh2.rpc.1 published heartbeats");
    let (lo, hi) = (beats.iter().map(skew_of).min().unwrap(), beats.iter().map(skew_of).max().unwrap());
    assert!(lo > -1_000 && hi < 1_000, "every node's rafka-time stands within 1 s of the host clock (the skewed node's OS clock was {OS_CLOCK_SKEW_MS} ms ahead): {lo}..{hi} ms over {} heartbeats", beats.len());
    // The fabric seat moved to the heir: the other mesh's primary pulled once from it and adopted
    // its time under the mesh-primary rule.
    let moved: Vec<&Value> = named(&spans, "rdm.mesh.entry.resolve.via-fabric-seat-moved").into_iter().filter(|sp| attr(sp, "node") == other_primary.0).collect();
    assert_eq!(moved.len(), 1, "the other mesh's primary pulled once when the fabric seat moved: {moved:?}");
    assert_eq!((attr(moved[0], "outcome").as_str(), attr(moved[0], "to").as_str()), ("adopted", heir_id.as_str()), "{:?}", moved[0]);
    let child = adopted.iter().find(|a| rafka_test_scenario::estate::descends_from(&spans, a, moved[0])).expect("the adoption is a child of the pull");
    assert_eq!(
        (attr(child, "source").as_str(), attr(child, "via").as_str(), attr(child, "seat").as_str(), attr(child, "served_by").as_str(), attr(child, "node").as_str()),
        ("pull", "get-topology", "fabric-primary", heir.as_str(), other_primary.0.as_str())
    );
}

/// CONTRACT: the primary of the mesh that does not hold the fabric seat dies; its mesh's other
/// admin takes the mesh-primary seat and announces it, and an admin of the OTHER mesh holds the
/// record (it reached the backbone).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_change_reaches_the_other_mesh_over_the_backbone() {
    let (mut estate, nodes) = fabric("a_mesh_primary_change_reaches_the_other_mesh_over_the_backbone").await;
    let fp = fabric_primary(&nodes).expect("one fabric primary");
    let mesh = if fp.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
    let old = nodes.iter().find(|n| n["mesh"] == mesh && n["is_primary"] == true).map(|n| s(&n["name"])).unwrap();
    let heir = if old.ends_with(".1") { format!("{mesh}.admin.2") } else { format!("{mesh}.admin.1") };
    let heir_id = s(&node(&nodes, &heir)["node_id"]);
    kill(&mut estate, &old).await;
    estate.until_view("the surviving admin is its mesh's primary", Duration::from_secs(90), |views| views.iter().any(|n| n.name.to_string() == heir && n.is_primary).then_some(())).await;
    let watchers: Vec<String> = nodes.iter().filter(|n| n["kind"] == "node_admin" && !n["mesh"].as_str().unwrap_or("").eq(mesh) && n["name"] != old.as_str()).map(|n| s(&n["name"])).collect();
    let heard = wait_for("an admin of the other mesh heard the new mesh primary", Duration::from_secs(60), || {
        let spans = estate.spans();
        let found = named(&spans, "rdm.mesh.seat.update.via-announcement").into_iter().find(|sp| attr(sp, "seat") == "mesh-primary" && attr(sp, "holder").contains(&heir_id) && watchers.contains(&attr(sp, "node"))).cloned();
        async move { found }
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();
    assert!(named(&spans, "rdm.mesh.seat.update.via-announce").iter().any(|sp| attr(sp, "node") == heir && attr(sp, "seat") == "mesh-primary"), "the new mesh primary announced itself");
    assert_eq!(attr(&heard, "outcome"), "held", "{heard:#?}");
}

/// CONTRACT: every UDP path between the fabric primary's mesh and the other mesh is cut while
/// all processes run. The other mesh's primary says the fabric primary's exact birth is silent (a
/// Concern), the incumbent mesh's admins look at it, and the seat does not move on either side. The
/// cut is healed and the seat is still where it was. The Concern and the investigation carry
/// iroh's local view of the holder (A1), in its shape and read as nothing more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_off_fabric_primary_that_runs_is_looked_at_and_keeps_the_seat() {
    let (mut estate, nodes) = fabric("a_cut_off_fabric_primary_that_runs_is_looked_at_and_keeps_the_seat").await;
    let fp = fabric_primary(&nodes).expect("one fabric primary");
    let fp_node = node(&nodes, &fp).clone();
    let (mine, theirs): (Vec<String>, Vec<String>) = {
        let side = |incumbent: bool| nodes.iter().filter(|n| n["kind"] == "node_admin" && (n["mesh"] == fp_node["mesh"]) == incumbent).map(|n| s(&n["name"])).collect::<Vec<_>>();
        (side(true), side(false))
    };
    let other_admin = nodes.iter().find(|n| n["kind"] == "node_admin" && n["mesh"] != fp_node["mesh"]).unwrap().clone();
    let cut = match Partition::start(&udp_ports(&nodes, &mine), &udp_ports(&nodes, &theirs)) {
        Ok(p) => p,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() != Ok("1") => {
            eprintln!("SKIPPED by name: the host cannot cut UDP between node-admins: {why}");
            estate.stop().await;
            return;
        }
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: {why}"),
    };
    let fp_id = s(&fp_node["node_id"]);
    let concern = wait_for("the other mesh's primary says the fabric primary's birth is silent", Duration::from_secs(120), || {
        let spans = estate.spans();
        let found = named(&spans, "rdm.mesh.seat.update.via-concern").into_iter().find(|sp| attr(sp, "holder_node_id") == fp_id).cloned();
        async move { found }
    })
    .await;
    let looked = wait_for("an admin looked at the exact silent birth", Duration::from_secs(60), || {
        let spans = estate.spans();
        let found = named(&spans, "rdm.node_admin.seat.update.via-investigation").into_iter().find(|sp| attr(sp, "suspect_node_id") == fp_id).cloned();
        async move { found }
    })
    .await;
    // Both sides still name the same holder while the cut stands.
    let from_other = estate.node_values_at(&s(&other_admin["name"])).await;
    let from_incumbent = estate.node_values_at(&fp).await;
    drop(cut);
    tokio::time::sleep(Duration::from_secs(8)).await;
    let healed = estate.node_values().await;
    estate.stop().await;
    for (side, ns) in [("the other mesh", &from_other), ("the incumbent mesh", &from_incumbent), ("after the heal", &healed)] {
        assert_eq!(fabric_primary(ns).as_deref(), Some(fp.as_str()), "{side} names the unchanged holder: {ns:#?}");
    }
    assert_eq!(attr(&concern, "seat"), "fabric-primary");
    assert_ne!(attr(&looked, "finding"), "exit-proven", "a running birth is not a loss: {looked:#?}");
    // A1: the Concern and the investigation carry iroh's local view of the holder.
    for sp in [&concern, &looked] {
        iroh_observation_shape(sp).unwrap_or_else(|why| panic!("{why}"));
    }
}

fn at_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

/// CONTRACT (the approved proposal docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md
/// Part C1, Luke yes 2026-10-12: "One writer, one row, one point of no return"; Q1: "the one row of C1
/// commits both seats to NA2 at their next epochs"; step 8: "Only then does the commanding Build
/// continue ... The successor is now the holder's primary"): the fabric-primary P is replaced by the
/// replace recipe (lifecycles/node-replace.md: one Build of node primitives the rectifier executes;
/// never a signal). Its stop of P is executed by its same-mesh successor H (node-admin-lifecycle.md
/// §1.3), the admin P's mesh seats once P is gone: the lowest Ready NodeId of P's mesh (elections §3),
/// the other admin N or P's successor born `<P>.new`. P holds both seats and is its mesh's row writer,
/// so it moves them itself before its stop runs:
/// 1. P claims the attempt the update runs in for itself (C1 step 1: "The holder is commanded from
///    above, as a step of the Build"; `build.update.via-claim-decision` at P, won, before its row), says
///    "primary updating" (`via-announce-primary-updating`, sent on the backbone and its mesh channel),
///    cuts off the writes of both seats (`via-writes-cut-off`, the fabric seat included) and writes the
///    ONE committed row (`seat.update.via-seat-row-written` at P: holder P, successor H, both seats at
///    their next epochs, the attempt P claimed).
/// 2. H, P's same-mesh successor, installs from the row and answers node.primary.updated
///    (`seat.update.via-primary-updated` at H, not a replay, under P's update, taking P's claim of the
///    attempt into its Build log: `claim` adopted).
/// 3. P says "primary updated" (C1 step 7: `via-announce-primary-updated` at P, sent on the backbone and
///    its mesh channel), and only then does its attempt end, handed off to H (`build.update.via-dispatch`
///    at P for its own attempt, handed-off). P's drive ends at the row (`seat-lost`;
///    `build.reject.via-fenced` naming H), and P decides no claim after its row.
/// 4. Only then does the commanding Build continue (step 8; build.md: "A live run is reattached, never
///    re-claimed"): H claims the next attempt and dispatches the stop of P; H says nothing for P's row
///    (P said it; a `seat.update.via-row-acknowledged` at H for P's row, if any, announced nothing). The
///    recipe's steps on P, its stop first, run after P's "primary updated".
/// Must NOT happen: a view showing two fabric-primaries; an observer resolving P's mesh or the fabric
/// with no primary between the replace and the fabric's ready-for-traffic; H writing a seat row for P or
/// saying "primary updated" before P's stop cut its transport; H claiming or dispatching, or a step on P
/// running, before P's "primary updated"; a birth made twice; an attempt with two winners. The fabric is
/// ready again under the same Build.
///
/// Adversarial (R-NS2: "Final aggregates and leadership announcements are best-effort dissemination.
/// A stop does not wait for their delivery. Committed lifecycle facts remain recoverable after the
/// sender cuts gossip"): P's "primary updated" is withheld on both channels (the fault door's
/// `announcement` cut) and the replace stops P right behind it. No observer is left without a
/// primary: every node holds the committed seats naming H through the read, the replay or the
/// surviving publisher.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_hand_over_moves_both_seats_in_the_old_holders_one_row_before_the_new_one_acts() {
    let cell = "a_planned_hand_over_moves_both_seats_in_the_old_holders_one_row_before_the_new_one_acts";
    let (mut estate, nodes) = faulted_fabric(cell).await;
    let p = fabric_primary(&nodes).expect("one fabric primary");
    let p_node = node(&nodes, &p).clone();
    let p_id = s(&p_node["node_id"]);
    let mesh = s(&p_node["mesh"]);
    // The heir is the other admin of P's mesh; control stays on it.
    let n_name = if p.ends_with(".1") { format!("{mesh}.admin.2") } else { format!("{mesh}.admin.1") };
    let n_id = s(&node(&nodes, &n_name)["node_id"]);
    let n_path = Estate::path(&n_name);
    estate.enter(&n_path).await;
    // P's "primary updated" never leaves it, on either channel.
    Door::at(&estate, &p).await.arm("primary-updated", serde_json::json!({"kind": "announcement", "announcement": "primary-updated", "channel": "any"})).await;
    let replaced_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let (build, outer) = replace_by_recipe(&estate, &n_path, &p).await;
    // Sampled while the hand-over runs: no admin's view ever shows two fabric primaries.
    let mut worst = 0usize;
    let until = std::time::Instant::now() + Duration::from_secs(120);
    let mut successor_id = None;
    while std::time::Instant::now() < until && successor_id.is_none() {
        let views = estate.node_views_at(&n_path).await.unwrap_or_else(|| panic!("{n_path} does not answer node.get while the hand-over runs"));
        let r = Estate::values(&views);
        let held = r.iter().filter(|x| x["is_fabric_primary"] == true).count();
        worst = worst.max(held);
        successor_id = r.iter().find(|x| x["name"] == p.as_str() && x["incarnation_id"] != p_node["incarnation_id"] && x["status"] == "ready-for-traffic").filter(|_| held == 1).map(|x| s(&x["node_id"]));
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let successor_id = successor_id.expect("the replaced admin's path is ready again under a new birth");
    // H: the lowest NodeId of N and P's successor, which was born `<P>.new` and took P's name.
    let (h_name, h_id) = if successor_id < n_id { (p.parse::<rafka_mesh_entity::PathName>().unwrap().successor().to_string(), successor_id.clone()) } else { (n_name.clone(), n_id.clone()) };
    assert!(worst <= 1, "a view showed {worst} fabric primaries");
    // The fabric is ready-for-traffic once its planned births are all ready and the state-sync,
    // state-commit and open-traffic rounds have run under the new fabric-primary.
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let ready_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    // The caller's stream ended with the seat move and the caller followed the recipe to the new
    // seat the moment it was named (node-rpc-envelope.md TakeSeats): the same build id resubmitted
    // at H, read to its terminal. The recipe is complete when that terminal is `Complete` and
    // `build.get` says no step is left, never when a span says so; the fabric is stopped only after.
    let Followed { first, broke, onward, kind, accepted } = tokio::time::timeout(Duration::from_secs(120), outer).await.expect("the caller read the recipe to its terminal within 120 s").expect("the caller's stream task");
    estate.stop().await;
    let spans = estate.spans();
    let one = |name: &str, f: &dyn Fn(&Value) -> bool| -> Value {
        let found: Vec<&Value> = named(&spans, name).into_iter().filter(|sp| f(sp)).collect();
        assert_eq!(found.len(), 1, "one {name}: {found:#?}");
        found[0].clone()
    };
    // C1 steps 2-5 at P: the pre-check named H, "primary updating", the cut-off, the ONE row.
    let updating = one("rdm.node_admin.seat.update.via-primary-updating", &|sp| attr(sp, "node") == p && attr(sp, "build_id") == build);
    assert_eq!((attr(&updating, "seat").as_str(), attr(&updating, "successor").as_str(), attr(&updating, "outcome").as_str()), ("mesh-primary+fabric-primary", h_name.as_str(), "updated"), "P moved both its seats to H ({h_name}): {updating:#?}");
    let said_updating = one("rdm.mesh.seat.update.via-announce-primary-updating", &|sp| attr(sp, "node") == p && attr(sp, "build_id") == build);
    assert_eq!((attr(&said_updating, "backbone").as_str(), attr(&said_updating, "mesh_channel").as_str()), ("sent", "sent"), "{said_updating:#?}");
    let cut_off = one("rdm.node_admin.seat.update.via-writes-cut-off", &|sp| attr(sp, "node") == p && attr(sp, "build_id") == build);
    assert_eq!(attr(&cut_off, "fabric_seat"), "true", "the fabric seat's writes were cut off too: {cut_off:#?}");
    let row = one("rdm.node_admin.seat.update.via-seat-row-written", &|sp| attr(sp, "build_id") == build && attr(sp, "node") == p);
    assert!(attr(&row, "holder").contains(&p_id) && attr(&row, "successor").contains(&h_id), "the row moves P's seats to H ({h_name}, {h_id}): {row:#?}");
    assert_eq!((num(&row, "mesh_epoch").is_some(), num(&row, "fabric_epoch").is_some()), (true, true), "both seats moved in the one row: {row:#?}");
    let row_at = at_ns(&row);
    let transfer_attempt = num(&row, "attempt").unwrap();
    // C1 step 6: H installs from the row and answers node.primary.updated, under P's update call.
    let installed = one("rdm.node_admin.seat.update.via-primary-updated", &|sp| attr(sp, "node") == h_name && attr(sp, "build_id") == build && attr(sp, "mesh_epoch") == attr(&row, "mesh_epoch") && attr(sp, "replay") == "false");
    assert_eq!(attr(&installed, "outcome"), "held", "H installed P's row: {installed:#?}");
    assert_eq!((attr(&installed, "mesh_epoch"), attr(&installed, "fabric_epoch")), (attr(&row, "mesh_epoch"), attr(&row, "fabric_epoch")), "H installed the row's epochs: {installed:#?}");
    assert!(descends_from(&spans, &installed, &updating), "H installs under P's node.primary.update call: {installed:#?}");
    // C1 step 1: the update is a step of the attempt P claimed for itself before its row: the row's attempt.
    let p_claim = one("rdm.node_admin.build.update.via-claim-decision", &|sp| attr(sp, "node") == p && attr(sp, "build_id") == build && num(sp, "attempt") == Some(transfer_attempt));
    assert_eq!((attr(&p_claim, "outcome").as_str(), attr(&p_claim, "executor").as_str()), ("won", p.as_str()), "P claimed the attempt its update runs in for itself: {p_claim:#?}");
    assert!(sp_end(&p_claim) <= at_ns(&said_updating), "P claimed its attempt before it said primary updating: {p_claim:#?}");
    let p_attempt = one("rdm.node_admin.build.update.via-dispatch", &|sp| attr(sp, "node") == p && attr(sp, "build_id") == build && num(sp, "attempt") == Some(transfer_attempt));
    assert_eq!((attr(&p_attempt, "executor").as_str(), attr(&p_attempt, "outcome").as_str()), (p.as_str(), "handed-off"), "P's own attempt ended handed off: {p_attempt:#?}");
    assert!(descends_from(&spans, &updating, &p_attempt), "P's update runs under its own attempt: {updating:#?}");
    assert_eq!(attr(&installed, "claim"), "adopted", "H took P's claim of the attempt into its Build log as it installed: {installed:#?}");
    // C1 step 7: P says "primary updated" once H answered. The successor neither says it before P's stop nor writes a row for P.
    let p_said = one("rdm.mesh.seat.update.via-announce-primary-updated", &|sp| attr(sp, "node") == p && attr(sp, "build_id") == build);
    assert_eq!((attr(&p_said, "backbone").as_str(), attr(&p_said, "mesh_channel").as_str()), ("withheld", "withheld"), "P's primary updated was withheld on both channels: {p_said:#?}");
    // H, P's next writer, says primary updated for P's transfer only once P's stop completed and P can
    // say nothing more (R-NS2; C1 failure contract: "the next writer to read the row publishes
    // 'updated'"), under the span naming the stopped holder, never before P's stop cut its transport.
    let by_h: Vec<&Value> = named(&spans, "rdm.mesh.seat.update.via-announce-primary-updated").into_iter().filter(|sp| attr(sp, "node") == h_name && num(sp, "attempt") == Some(transfer_attempt)).collect();
    let p_cut = named(&spans, "rdm.node_admin.node.update.via-stop-transport-cut").into_iter().filter(|sp| attr(sp, "node") == p).map(sp_end).min();
    for said in &by_h {
        let after_stop = named(&spans, "rdm.mesh.seat.update.via-holder-stopped").into_iter().find(|h| attr(h, "node") == h_name && attr(h, "holder") == p && descends_from(&spans, said, h)).cloned();
        let after_stop = after_stop.unwrap_or_else(|| panic!("the successor said primary updated other than after the holder's stop: {said:#?}"));
        assert!(p_cut.is_some_and(|cut| cut <= at_ns(&after_stop)), "the successor said primary updated only after P's stop cut its transport ({p_cut:?}): {after_stop:#?}");
        eprintln!("H said primary updated after P's stop: trace {}", s(&after_stop["trace_id"]));
    }
    let h_acks: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-row-acknowledged").into_iter().filter(|sp| attr(sp, "node") == h_name && attr(sp, "build_id") == build && attr(sp, "operation") == attr(&row, "operation")).collect();
    assert!(h_acks.iter().all(|sp| attr(sp, "announced") == "false"), "H's acknowledgement of P's row announced nothing: {h_acks:#?}");
    let h_rows: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-seat-row-written").into_iter().filter(|sp| attr(sp, "node") != p && attr(sp, "holder").contains(&p_id)).collect();
    assert!(h_rows.is_empty(), "the successor wrote a seat row for P: {h_rows:#?}");
    // The order of the sequence (C1 steps 3-8).
    assert!(at_ns(&said_updating) <= at_ns(&cut_off) && sp_end(&cut_off) <= row_at, "P said primary updating, then cut off its writes, then wrote the row");
    assert!(sp_end(&row) <= at_ns(&installed), "H installed only after P's row");
    assert!(sp_end(&installed) <= at_ns(&p_said), "P said primary updated only after H answered");
    assert!(sp_end(&p_said) <= sp_end(&p_attempt), "P's attempt ended only after it said primary updated");
    // Step 8: H claims the next attempt only after P's "primary updated", and dispatches the replace.
    let h_claim = one("rdm.node_admin.build.update.via-claim-decision", &|sp| attr(sp, "node") == h_name && attr(sp, "build_id") == build && num(sp, "attempt") == Some(transfer_attempt + 1) && attr(sp, "outcome") == "won");
    assert_eq!(attr(&h_claim, "executor"), h_name, "H claimed the attempt after P's: {h_claim:#?}");
    assert!(sp_end(&p_said) <= at_ns(&h_claim), "H claimed only after P said primary updated: {h_claim:#?}");
    let h_dispatch = one("rdm.node_admin.build.update.via-dispatch", &|sp| attr(sp, "node") == h_name && attr(sp, "build_id") == build && num(sp, "attempt") == Some(transfer_attempt + 1));
    assert!(sp_end(&p_said) <= at_ns(&h_dispatch), "H dispatched the replace only after P said primary updated: {h_dispatch:#?}");
    // The replace's steps on P, its stop first, run from H's attempt, after P's "primary updated".
    let p_steps: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| attr(sp, "build_id") == build && attr(sp, "node") == p && num(sp, "attempt").is_some_and(|a| a >= transfer_attempt)).collect();
    assert!(!p_steps.is_empty() && num(p_steps[0], "attempt") == Some(transfer_attempt + 1), "the replace's steps on P ran from attempt {}: {p_steps:#?}", transfer_attempt + 1);
    assert!(p_steps.iter().all(|sp| num(sp, "attempt").is_some_and(|a| a > transfer_attempt) && at_ns(sp) >= sp_end(&p_said)), "no step on P ran before P said primary updated, nor in P's own attempt: {p_steps:#?}");
    // No seatless window: from the replace until the fabric is ready again (the fabric's own
    // shutdown after that is not this cell's story), no observer resolves P's mesh or the fabric
    // with nobody.
    let vacant: Vec<&Value> = named(&spans, "rdm.mesh.election.resolve.via-mesh-primary")
        .into_iter()
        .filter(|sp| attr(sp, "mesh") == mesh && attr(sp, "winner_path").is_empty() && attr(sp, "previous_node_id") != "" && (replaced_at..ready_at).contains(&at_ns(sp)))
        .chain(named(&spans, "rdm.mesh.election.resolve.via-fabric-recompute").into_iter().filter(|sp| attr(sp, "winner_path").is_empty() && attr(sp, "previous_node_id") != "" && (replaced_at..ready_at).contains(&at_ns(sp))))
        .collect();
    assert!(vacant.is_empty(), "a seat was vacant during the hand-over: {vacant:#?}");
    // Every other node holds the committed seats naming H though P's frame never left it: through the
    // replay or the surviving publisher (each node's seat record, whichever path brought it).
    for other in names().into_iter().filter(|x| *x != p && *x != h_name) {
        let heard: Vec<&Value> = named(&spans, "rdm.mesh.seat.update.via-primary-updated").into_iter().filter(|sp| attr(sp, "node") == other && attr(sp, "successor").contains(&h_id)).collect();
        let seated: Vec<&Value> = named(&spans, "rdm.mesh.seat.update.via-announcement").into_iter().filter(|sp| attr(sp, "node") == other && attr(sp, "holder").contains(&h_id)).collect();
        let held = heard.iter().chain(seated.iter()).any(|sp| matches!(attr(sp, "outcome").as_str(), "held" | "same"));
        assert!(held, "{other} holds the committed seats naming H ({h_name}, {h_id}) with P's frame withheld: primary updated {heard:#?}, seated {seated:#?}");
        eprintln!("seat at {other}: {:?}", heard.iter().chain(seated.iter()).map(|sp| format!("{} via {} {} trace {}", s(&sp["name"]), attr(sp, "via"), attr(sp, "outcome"), s(&sp["trace_id"]))).collect::<Vec<_>>());
    }
    // P was the fabric-primary driving the Build: its drive ends at its row (seat-lost), its committed
    // seat check naming H (build_drive.rs `may_act`), a stream attached there ends with SeatMoved naming
    // H, and H drives the rest of the recipe.
    let p_drive: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-drive").into_iter().filter(|sp| attr(sp, "node") == p && attr(sp, "build_id") == build).collect();
    assert!(p_drive.iter().any(|sp| attr(sp, "outcome") == "seat-lost"), "P's drive ends at its seat row: {p_drive:#?}");
    assert!(matches!(first.last(), Some(rafka_node_admin_client::Frame::SeatMoved { fabric_primary: Some(to) }) if *to == h_name), "the caller's stream ends SeatMoved naming H: {first:?}, broke: {broke:?}");
    let p_refused: Vec<&Value> = named(&spans, "rdm.node_admin.build.reject.via-fenced").into_iter().filter(|sp| attr(sp, "node") == p && attr(sp, "build_id") == build).collect();
    assert!(!p_refused.is_empty() && p_refused.iter().all(|sp| attr(sp, "to") == h_name), "P's drive lost the seat at the committed record naming H: {p_refused:#?}");
    // When H is P's successor `<P>.new`, the recipe's stop of `.new` moves the seats once more, to
    // N, its own same-mesh successor; no SeatMoved of the Build names anyone else.
    let moved: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-seat-moved").into_iter().filter(|sp| attr(sp, "build_id") == build).collect();
    assert!(moved.iter().any(|sp| attr(sp, "to") == h_name) && moved.iter().all(|sp| attr(sp, "to") == h_name || attr(sp, "to") == n_name), "every SeatMoved of the Build names H, then N: {moved:#?}");
    // A planned move leaves no fault anywhere in the run (the fault span of a handler is a root of its
    // own trace, so the whole run is read).
    let faults = named(&spans, "rdm.node_rpc.handler.reject.via-internal-rpc-failure");
    assert!(faults.is_empty(), "a planned seat move left a handler fault: {faults:#?}");
    // The run that stopped P hands back to the fabric primary its ending view seats: no attempt
    // after the transfer's is claimed for P, and the driver never dispatches to a lost executor.
    let stale: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| attr(sp, "build_id") == build && attr(sp, "outcome") == "won" && num(sp, "attempt").is_some_and(|a| a > transfer_attempt) && attr(sp, "executor") == p).collect();
    assert!(stale.is_empty(), "an attempt after the transfer was claimed for the stopped P: {stale:#?}");
    let lost: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().filter(|sp| attr(sp, "build_id") == build && attr(sp, "outcome") == "executor-lost").collect();
    assert!(lost.is_empty(), "the drive dispatched to a lost executor: {lost:#?}");
    // The recipe runs as attempts of the accepted Build and completes once: the terminal of the
    // caller's last stream is `Complete`, and the Build's receipts say it is complete with no step of
    // the recipe left to run.
    let (followed, receipts) = onward.unwrap_or_else(|why| panic!("the caller could not follow the recipe past the seat move: {why}"));
    assert!(matches!(followed.last().and_then(|frames| frames.last()), Some(rafka_node_admin_client::Frame::Complete)), "the caller's resubmitted stream ends Complete: {followed:?}");
    assert_eq!(receipts.phase, rafka_node_rpc_contract::build::BuildPhase::Complete, "build.get: the recipe's Build is complete: {receipts:?}");
    let resume = rafka_node_admin_client::Resume::from_receipts(&kind, &receipts, accepted.attempt).expect("the receipts fold into the recipe's steps");
    let step_keys: Vec<String> = {
        let (p, succ) = (p.parse::<rafka_mesh_entity::PathName>().unwrap(), p.parse::<rafka_mesh_entity::PathName>().unwrap().successor());
        vec![format!("create-node:{succ}"), format!("stop-node:{p}"), format!("update-node:{p}"), format!("stop-node:{succ}"), format!("update-node:{succ}"), format!("start-node:{p}"), format!("retire-node:{}", p.renamed())]
    };
    for key in &step_keys {
        assert!(resume.completed.iter().any(|(op, step)| op == key && step == "Complete"), "build.get holds the Complete receipt of {key}: {resume:?}");
    }
    // The Build facts were handed on when P's record moved.
    let hand_over: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-seat-hand-over").into_iter().filter(|sp| attr(sp, "node") == p).collect();
    assert!(hand_over.iter().any(|sp| sp["attributes"]["facts"].as_str().and_then(|f| f.parse::<u64>().ok()).is_some()), "the hand-over span names the facts handed on: {hand_over:#?}");
    // No claim decided by P after its row.
    let late: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| attr(sp, "node") == p && at_ns(sp) > row_at && matches!(attr(sp, "outcome").as_str(), "won" | "lost" | "not-open")).collect();
    assert!(late.is_empty(), "P decided a claim after its row: {late:#?}");
    // One winner per attempt; no birth made twice.
    let mut won: std::collections::BTreeMap<(String, String), std::collections::BTreeSet<String>> = Default::default();
    let mut made: std::collections::BTreeMap<(String, String, String), u32> = Default::default();
    for sp in &spans {
        match sp["name"].as_str() {
            Some("rdm.node_admin.build.update.via-claim-decision") if attr(sp, "outcome") == "won" => {
                won.entry((attr(sp, "build_id"), attr(sp, "attempt"))).or_default().insert(attr(sp, "executor"));
            }
            Some("rdm.node_admin.node.create.via-build") => *made.entry((attr(sp, "build_id"), attr(sp, "attempt"), attr(sp, "node"))).or_default() += 1,
            _ => {}
        }
    }
    assert!(won.values().all(|w| w.len() == 1), "conflicting winners: {won:#?}");
    assert!(made.values().all(|c| *c == 1), "a birth made twice: {made:#?}");
    estate.record_trace_url(row["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (the approved proposal Part C1 step 5: "The writer writes the one seat row at the next
/// revision(s) naming the successor. This is the commit. From this write the holder is no longer
/// primary"): the same hand-over right behind a Build whose claims the fabric-primary P decided
/// (mesh2 grows by two rpc nodes, then P is replaced by the replace recipe). The recipe is submitted
/// once the grow completed: build.md, "A second topology-changing request while the current Build is
/// actively reconciling is refused with a named build-in-progress refusal". P writes its ONE row under
/// its Build-log decision lock (`seat.update.via-seat-row-written` at P), before its successor installs
/// it (`seat.update.via-primary-updated`): a claim P won committed wholly before the row, and P wins
/// none after it. One winner per attempt, no birth twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_in_flight_commits_before_the_old_holders_seat_row_and_none_is_decided_after() {
    let cell = "a_claim_in_flight_commits_before_the_old_holders_seat_row_and_none_is_decided_after";
    let (mut estate, nodes) = fabric(cell).await;
    let p = fabric_primary(&nodes).expect("one fabric primary");
    let mesh = s(&node(&nodes, &p)["mesh"]);
    let p_birth = node(&nodes, &p)["incarnation_id"].clone();
    let p_id = s(&node(&nodes, &p)["node_id"]);
    let n_name = if p.ends_with(".1") { format!("{mesh}.admin.2") } else { format!("{mesh}.admin.1") };
    let n_path = Estate::path(&n_name);
    estate.enter(&n_path).await;
    let grow = FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 3)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 5)])] };
    // The Build this cell started first is the grow: it is followed to its terminal frame before the replace
    // is submitted, because the fabric-primary accepts one change of the accepted Build at a time.
    let growing = estate.submit(&BuildSpec::Fabric(grow)).await;
    estate.follow(growing).await;
    let (replace_id, outer) = replace_by_recipe(&estate, &n_path, &p).await;
    let all: BTreeSet<String> = names().into_iter().chain(["mesh2.rpc.4".to_string(), "mesh2.rpc.5".to_string()]).collect();
    // The replace is part of the story: the fabric is stopped only once P's path is ready again under
    // a new birth, so the fabric's own shutdown never races the seat transfer.
    let settled = estate.until_view("the grown fabric is settled under the new holder and P's path is a new birth", Duration::from_secs(180), |views| {
        let ns = Estate::values(views);
        let replaced = ns.iter().any(|x| x["name"] == p.as_str() && x["incarnation_id"] != p_birth && x["status"] == "ready-for-traffic");
        (replaced && all.iter().all(|n| ns.iter().any(|x| x["name"] == n.as_str() && x["status"] == "ready-for-traffic")) && ns.iter().filter(|x| x["is_fabric_primary"] == true).count() == 1).then_some(())
    });
    let (followed, ()) = tokio::join!(outer, settled);
    let _ = followed.expect("the caller's stream task");
    estate.stop().await;
    let spans = estate.spans();
    // C1 step 5: P's ONE row is the commit, written under its Build-log decision lock
    // (fabric_handover.rs `update_primary_in`), before its successor installs it.
    // P's own seat row: the recipe moves the seats once more when P's successor `<P>.new` takes
    // them first (its stop then hands them to N), and that row is `.new`'s, not P's.
    let row: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-seat-row-written").into_iter().filter(|sp| attr(sp, "build_id") == replace_id && attr(sp, "node") == p && attr(sp, "holder").contains(&p_id)).collect();
    assert_eq!(row.len(), 1, "P wrote one seat row for the replace: {row:#?}");
    assert_eq!(attr(row[0], "node"), p, "the holder P wrote the row: {:#?}", row[0]);
    let installed = named(&spans, "rdm.node_admin.seat.update.via-primary-updated").into_iter().filter(|sp| attr(sp, "build_id") == replace_id && attr(sp, "replay") == "false" && attr(sp, "mesh_epoch") == attr(row[0], "mesh_epoch")).map(at_ns).min().expect("the successor installed the committed row");
    assert!(sp_end(row[0]) <= installed, "P's row was written before the successor installed it");
    // A decision P won committed wholly before the row moved its record; none after it.
    let row_at = at_ns(row[0]);
    let mine: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| attr(sp, "node") == p && at_ns(sp) < sp_end(row[0]) + 3_000_000_000 && attr(sp, "outcome") == "won").collect();
    for sp in &mine {
        assert!(sp_end(sp) <= row_at, "P won a claim that did not commit before its seat row: {sp:#?}");
    }
    let mut won: std::collections::BTreeMap<(String, String), BTreeSet<String>> = Default::default();
    for sp in named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| attr(sp, "outcome") == "won") {
        won.entry((attr(sp, "build_id"), attr(sp, "attempt"))).or_default().insert(attr(sp, "executor"));
    }
    assert!(won.values().all(|w| w.len() == 1), "conflicting winners: {won:#?}");
    estate.record_trace_url(row[0]["trace_id"].as_str().unwrap_or(""));
}

fn sp_end(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(0)
}
