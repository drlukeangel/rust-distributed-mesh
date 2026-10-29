//! product=mesh, feature=fabric-build, subfeature=build-op, rung=MM, provider=process.
//!
//! R-ON8 = A′: the Build family on op `0x20`, with the fabric-primary the one driver of every
//! Build. Every cell drives the estate through the typed objects of `rafka-node-admin-client`
//! (`Builds`, `Nodes`) over Node RPC and awaits their reply streams. The one exception is cell 6,
//! which sends `build.attempt.run` itself because that call is the fabric-primary's own, never a
//! caller's. The one read made outside the client's typed objects is cell 8's precondition: the surviving
//! admin's own Build log, read over `FetchBuildFacts` (op `0x1F`) and awaited through the shared
//! `build_log::log_fact`. The spans an estate exports are the flame of each cell; no cell asserts behaviour
//! from them.
//!
//! The estate: `mesh1` (two node-admins, one rpc node) holds the fabric-primary seat, and `mesh2`
//! (two node-admins, two rpc nodes) is run by its own primary, so a node of `mesh2` is executed by
//! an admin that is not the fabric-primary and every Build crosses a real `build.attempt.run`.

use iroh::SecretKey;
use rdm_core::{NodeKind, PathName};
use rdm_client::{BuildCarrier, BuildFrame, BuildId, BuildStream, Builds, CallEnd, FabricDesired, Fabrics, Frame, MeshDesired, Meshes, NodeEvent, NodeStatus, Nodes, Peers, StartDisposition, WorkflowKind, WorkflowStream};
use rdm_admin::build_state::{BuildFact, StepOutcome};
use rdm_transport::{Budget, CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rdm_core::build::{Build, BuildReply, BuildRequest, StepResult};
use rdm_core::context::CallContext;
use rdm_core::outcome::RpcOutcome;
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, Door};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const FABRIC: &str = "fabric1";
const SHAPE: [(&str, u32, u32); 2] = [("mesh1", 2, 1), ("mesh2", 2, 2)];
/// The events a stop's stream carries, in order.
const STOP_EVENTS: [NodeEvent; 2] = [NodeEvent::ConnectionsDeleted, NodeEvent::Stopped];

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "fabric-build".into(), subfeature: "build-op".into(), rung: "MM".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn path(name: &str) -> PathName {
    name.parse().unwrap()
}

/// A caller outside the fabric: its own endpoint, and the node-admins it reaches by path.
pub(crate) struct Caller {
    /// The estate's peers, the node-admin the caller enters at, and the Fabric: every read is `fabric.get` / `node.get` there.
    peers: Arc<Peers>,
    entry: PathName,
    fabric_id: String,
    client: Arc<NodeRpcClient>,
    resolver: Arc<StaticResolver>,
    /// The control API each node-admin advertises, by path.name, as the answering admin's view held it.
    bases: std::sync::Mutex<std::collections::BTreeMap<String, String>>,
    /// The node id each node-admin's path.name held, as the answering admin's view held it.
    ids: std::sync::Mutex<std::collections::BTreeMap<String, String>>,
}

impl Caller {
    /// A caller that learns the fabric from the estate's entry node-admin, over Node RPC.
    pub(crate) async fn of(estate: &Estate) -> Caller {
        let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller binds an endpoint");
        let resolver = Arc::new(StaticResolver::new());
        let client = Arc::new(NodeRpcClient::new(ep, resolver.clone()).with_caller_system("rdm"));
        let caller = Caller { peers: estate.peers().await, entry: estate.entry().await, fabric_id: estate.fabric_id.clone(), client, resolver, bases: Default::default(), ids: Default::default() };
        caller.learn().await;
        caller
    }

    /// Every node-admin the answering admin's view holds becomes reachable by its path.
    async fn learn(&self) {
        for n in rdm_test::typed::node_views_of(&self.peers, &self.entry, &self.fabric_id).await.expect("the entry admin's view") {
            if n.kind != NodeKind::NodeAdmin {
                continue;
            }
            if let Some(base) = n.admin_api_base.clone() {
                self.bases.lock().unwrap().insert(n.name.to_string(), base);
            }
            self.ids.lock().unwrap().insert(n.name.to_string(), n.node_id.to_string());
            let (Some(ep), Some(addr), Some(inc)) = (n.endpoint_id.as_deref(), n.transport_addr, n.incarnation_id.clone()) else { continue };
            self.resolver.insert(ResolvedNode { node_id: n.node_id.clone(), name: n.name.clone(), endpoint_id: ep.parse().expect("an iroh key"), transport_addr: addr, incarnation: inc });
        }
    }

    fn nodes(&self, carrier: &BuildCarrier) -> Nodes {
        Nodes::new(carrier.clone())
    }
}

/// Read a Build stream to its end; a failed step or a broken stream is a defect of the cell. The
/// frames read, `Complete` last.
async fn until_complete(stream: &mut BuildStream) -> Vec<BuildFrame> {
    let mut frames = Vec::new();
    while let Some(frame) = stream.next().await {
        let frame = frame.expect("the stream does not break");
        frames.push(frame.clone());
        match frame {
            BuildFrame::Complete { .. } => return frames,
            BuildFrame::Failed { operation, step, reason, .. } => panic!("the Build failed at {operation} {step}: {reason}"),
            _ => {}
        }
    }
    panic!("the stream ended without a terminal frame");
}

/// Birth the fabric's shape through `build.create`: one call, awaited to its `Complete`.
async fn born(estate: &Estate, caller: &Caller, shape: &[(&str, u32, u32)]) -> BuildCarrier {
    born_framed(estate, caller, shape).await.0
}

/// [`born`], with every frame the Build that birthed the shape streamed. The estate's birth followed
/// the Day-0 Build to its end, so the shape is accepted at once.
async fn born_framed(estate: &Estate, caller: &Caller, shape: &[(&str, u32, u32)]) -> (BuildCarrier, Vec<BuildFrame>) {
    let carrier = estate.carrier().await;
    let desired = FabricDesired { fabric: FABRIC.into(), meshes: shape.iter().map(|(m, a, r)| MeshDesired::of(*m, [(NodeKind::NodeAdmin, *a), (NodeKind::RpcNode, *r)])).collect() };
    let mut stream = carrier.create(estate.shape_change_at(&carrier, &desired).await).await.expect("the fabric-primary accepts the shape");
    let frames = until_complete(&mut stream).await;
    estate.settles_to(shape, Duration::from_secs(90)).await;
    caller.learn().await;
    // A workflow a caller submits is refused until the initial fabric has first reached ready
    // (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(90)).await;
    (carrier, frames)
}

/// Every frame of a workflow stream, to its end.
async fn frames_of(stream: &mut WorkflowStream) -> Vec<Frame> {
    let mut frames = Vec::new();
    while let Some(f) = stream.next().await {
        frames.push(f.expect("the stream does not break"));
    }
    frames
}

fn events(frames: &[Frame]) -> Vec<NodeEvent> {
    frames.iter().filter_map(|f| if let Frame::Event(e) = f { Some(*e) } else { None }).collect()
}

/// The spans of the estate named `name`.
fn spans_named<'a>(spans: &'a [Value], name: &str) -> Vec<&'a Value> {
    named(spans, name)
}

/// The rpc node of `mesh2` that holds no seat: executed by `mesh2`'s own primary with no preceding
/// seat move, so its stop is one attempt (`accepted.attempt`).
async fn secondary_node(estate: &Estate) -> PathName {
    estate.node_views().await.into_iter().filter(|n| n.name.to_string().starts_with("mesh2.rpc.") && !n.is_primary).map(|n| n.name).min().expect("an rpc node of mesh2 holding no seat")
}

/// CONTRACT (acceptance 1): `node.stop` over `0x20` is one call. The stream is `Started`, a frame
/// per step as its `Complete` receipt becomes durable on the admin that wrote it, and `Complete`,
/// pushed by the executor through the fabric-primary: the node is executed by `mesh2`'s own primary,
/// so the Build crosses a real `build.attempt.run`. Every event the stop names arrives once, in
/// order, and nothing polls a Build to learn it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_over_the_build_family_is_one_call_that_pushes_every_step_frame_and_completes() {
    let test = "a_stop_over_the_build_family_is_one_call_that_pushes_every_step_frame_and_completes";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = secondary_node(&estate).await;

    let mut stream = caller.nodes(&carrier).stop(&node).await.expect("the fabric-primary accepts the stop");
    let accepted = stream.accepted().clone();
    let frames = frames_of(&mut stream).await;
    assert_eq!(frames.first(), Some(&Frame::Started { build_id: accepted.build_id.clone(), attempt: accepted.attempt }));
    assert_eq!(frames.last(), Some(&Frame::Complete), "the stream ends with its terminal frame: {frames:?}");
    assert_eq!(events(&frames), STOP_EVENTS, "every step event, once, in order: {frames:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Failed { .. } | Frame::Blocked { .. })), "{frames:?}");
    assert_eq!(stream.disposition(), StartDisposition::Created);

    estate.stop().await;
    let spans = estate.spans();
    // The flame: the call, the fabric-primary's drive and dispatch, and the executor's run are one trace.
    let build = &accepted.build_id.to_string();
    let reconcile = spans_named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["attempt"]) == accepted.attempt.to_string())
        .expect("the executor's run of the attempt");
    assert_eq!(s(&reconcile["attributes"]["executor"]), "mesh2.admin.1", "the node of mesh2 is executed by mesh2's primary, not the fabric-primary");
    assert_eq!(s(&reconcile["attributes"]["outcome"]), "converged");
    let dispatch = spans_named(&spans, "rdm.node_admin.build.update.via-dispatch")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["attempt"]) == accepted.attempt.to_string())
        .expect("the fabric-primary's dispatch");
    assert_eq!(s(&dispatch["attributes"]["node"]), "mesh1.admin.1");
    assert_eq!(s(&dispatch["attributes"]["executor"]), "mesh2.admin.1");
    assert_eq!(dispatch["trace_id"], reconcile["trace_id"], "the run is in the call's trace");
    assert!(spans_named(&spans, "rdm.node_admin.build.resolve.via-call").is_empty(), "nothing read a Build to learn a step");
    assert!(
        spans_named(&spans, "rdm.node_admin.build.update.via-rest").into_iter().chain(spans_named(&spans, "rdm.node_admin.build.create.via-rest")).all(|sp| s(&sp["attributes"]["build_id"]) != *build),
        "the Build was not accepted through a route"
    );
    estate.record_trace_url(reconcile["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (acceptance 4): cutting the outer stream leaves the Build running. The caller drops its
/// stream right after `Started`; the Build is driven to its end by the fabric-primary with no one
/// attached. A re-submit by the Build id attaches to that run and streams it to `Complete`, and a
/// second re-submit after the end answers `AlreadyApplied` with every frame and the terminal frame:
/// the Build is never created twice and no completed step has a second receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_outer_stream_leaves_the_build_running_and_a_resubmit_after_the_end_is_already_applied() {
    let test = "a_cut_outer_stream_leaves_the_build_running_and_a_resubmit_after_the_end_is_already_applied";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = secondary_node(&estate).await;
    let nodes = caller.nodes(&carrier);

    let mut cut = nodes.stop(&node).await.expect("the fabric-primary accepts the stop");
    let accepted = cut.accepted().clone();
    // The first frame is the call's `Started`; the cut follows it, before any step is reported.
    assert!(matches!(cut.next().await, Some(Ok(Frame::Started { .. }))));
    drop(cut);

    let mut attached = nodes.resume(WorkflowKind::Stop(node.clone()), &accepted).await.expect("the re-submit is accepted");
    assert_eq!(attached.disposition(), StartDisposition::Attached, "the Build the cut left running is the one the re-submit attaches to");
    assert_eq!(attached.accepted().build_id, accepted.build_id, "never a second Build");
    let first = frames_of(&mut attached).await;
    assert_eq!(first.last(), Some(&Frame::Complete));
    assert_eq!(events(&first), STOP_EVENTS, "{first:?}");

    let mut again = nodes.resume(WorkflowKind::Stop(node), &accepted).await.expect("the second re-submit is accepted");
    assert_eq!(again.disposition(), StartDisposition::AlreadyApplied);
    let second = frames_of(&mut again).await;
    assert_eq!(second.last(), Some(&Frame::Complete), "the terminal frame follows AlreadyApplied");
    assert_eq!(events(&second), STOP_EVENTS, "the receipts held are replayed as frames: {second:?}");

    let receipts = carrier.clone().build(&accepted.build_id).await.expect("build.get [id]");
    let mut keys: Vec<(u32, String, String)> = receipts.steps.iter().filter(|r| r.attempt >= accepted.attempt).map(|r| (r.attempt, r.operation.clone(), r.step.clone())).collect();
    let before = keys.len();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), before, "no step has two receipts");
    assert_eq!(receipts.attempt, accepted.attempt, "the cut opened no second attempt");

    estate.stop().await;
    let spans = estate.spans();
    let build = &accepted.build_id.to_string();
    let drives: Vec<_> = spans_named(&spans, "rdm.node_admin.build.update.via-drive").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build).collect();
    assert!(!drives.is_empty());
    assert!(drives.iter().any(|sp| s(&sp["attributes"]["outcome"]) == "terminal"), "the drive ended the Build with no caller attached: {drives:?}");
    let reconcile = spans_named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["attempt"]) == accepted.attempt.to_string())
        .expect("the run");
    estate.record_trace_url(reconcile["trace_id"].as_str().unwrap_or(""));
}

/// What a `build.attempt.run` sent to `to` for `attempt` answers, whole: the refusal, or the first frame.
/// CONTRACT (i146; build.md: "a live run is reattached, never re-claimed"; Q22: the reattach after a
/// `Budget::Stream` connection closes is the owner's; node-rpc-envelope.md "Frames, durable receipts and
/// reattachment": "`build.get [id: build_id]` for the receipts, then the outer stream again, resuming from the
/// receipts"): a watched Build whose connection closes mid-stream is reattached by its build id, each
/// reattachment a `build.get [id]` and then the outer stream again, and ends `Complete`, the run it watched
/// left running: no second attempt, no step receipted twice. The Build grows mesh2 by an rpc node; its
/// watcher reads the first frame, then every connection of the watching client is closed, the stream's
/// with them. Must NOT happen: the watcher ending on the break, a reattachment that reads no receipts,
/// a second attempt, a step run twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watched_build_whose_connection_closes_mid_stream_is_reattached_and_completes_with_no_step_run_twice() {
    let test = "a_watched_build_whose_connection_closes_mid_stream_is_reattached_and_completes_with_no_step_run_twice";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let grown = FabricDesired { fabric: FABRIC.into(), meshes: [("mesh1", 2, 1), ("mesh2", 2, 3)].iter().map(|(m, a, r)| MeshDesired::of(*m, [(NodeKind::NodeAdmin, *a), (NodeKind::RpcNode, *r)])).collect() };
    let mut stream = carrier.create(estate.shape_change_at(&carrier, &grown).await).await.expect("the fabric-primary accepts the change");
    let (build_id, attempt) = (stream.build_id().clone(), stream.attempt());
    let first = stream.next().await.expect("a frame").expect("the stream reads its first frame");
    assert!(!matches!(first, BuildFrame::Complete { .. } | BuildFrame::Failed { .. }), "the Build is still running when its connection closes: {first:?}");

    let closed = carrier.client().close_pooled("the cell closes the watcher's connection");
    assert!(closed >= 1, "the watching client held the stream's connection");
    let followed = carrier.follow(stream).await.expect("the watcher follows the Build to its terminal");
    assert!(followed.reattached >= 1, "the watcher reattached after the close: {followed:?}");
    assert!(matches!(followed.frames.last(), Some(BuildFrame::Complete { .. })), "the Build ends Complete: {followed:?}");

    let receipts = carrier.clone().build(&build_id).await.expect("build.get [id]");
    // The Build put the fabric back to pending, so it is Complete only once its ready state is set (Q154): when the
    // fabric is ready before the run ends the run completes it in the watched attempt; otherwise it hands back
    // and the one attempt after it, claimed once the fabric is ready, completes it and runs no step. The close
    // opened none: every step ran in the attempt the watcher watched.
    assert!(receipts.attempt == attempt || receipts.attempt == attempt + 1, "the close opened no second attempt; the Complete is the watched attempt's or the one after the fabric's change: {receipts:?}");
    assert!(receipts.steps.iter().all(|r| r.attempt == attempt), "every step ran in attempt {attempt}, the one the watcher watched: {receipts:?}");
    let mut keys: Vec<(u32, String, String)> = receipts.steps.iter().map(|r| (r.attempt, r.operation.clone(), r.step.clone())).collect();
    let all = keys.len();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), all, "no step has two receipts: {receipts:?}");

    estate.stop().await;
    let spans = estate.spans();
    let build = build_id.to_string();
    // Each reattachment read the Build's receipts before its stream again; the cell's own read is one more.
    let gets = spans_named(&spans, "rdm.node_admin.build.serve.via-node-rpc").into_iter().filter(|sp| s(&sp["attributes"]["call"]) == "build.get").count();
    assert!(gets as u32 > followed.reattached, "each reattachment read build.get [id] ({} reattachment(s), {gets} build.get read(s) with the cell's own)", followed.reattached);
    let reconcile = spans_named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build)
        .expect("the run");
    println!("watched Build reattached {} time(s), Complete; flame http://192.168.68.99:16687/trace/{}", followed.reattached, s(&reconcile["trace_id"]));
    estate.record_trace_url(reconcile["trace_id"].as_str().unwrap_or(""));
}

async fn attempt_run(
    caller: &Caller,
    to: &str,
    build: &BuildId,
    attempt: u32,
    executor: &str,
) -> BuildReply {
    let req = BuildRequest::AttemptRun {
        build_id: build.to_string(),
        attempt,
        executor: executor.into(),
        context: CallContext::default(),
        intent: Vec::new(),
        operations: Vec::new(),
        hand_off_to: None,
        mesh: None,
    };
    let opts = CallOptions {
        budget: Budget::Stream {
            send: Duration::from_secs(10),
        },
        ..CallOptions::default()
    };
    match caller
        .client
        .call_stream::<Build>(&NodeTarget::CurrentPath(path(to)), &req, &opts)
        .await
        .map(|(s, e)| (s.read_only(), e))
    {
        Err((RpcOutcome::Reply(r), _)) => r.into_value(),
        Err((other, _)) => panic!("the call to {to} ended {other:?}"),
        Ok((mut stream, _)) => match stream.next().await {
            Some(rdm_transport::stream::StreamItem::Frame(_, r)) => r,
            other => panic!("the stream of {to} gave {other:?}"),
        },
    }
}

/// CONTRACT (acceptance 6): `build.attempt.run` is refused by name when the recipient is not the
/// executor the claim names, and when the claim is not the current folded attempt of the recipient's
/// projection; a replaced primary is given a new claim, never the stale one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_attempt_run_to_a_node_the_claim_does_not_name_or_with_a_stale_claim_is_refused_by_name() {
    let test = "an_attempt_run_to_a_node_the_claim_does_not_name_or_with_a_stale_claim_is_refused_by_name";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = secondary_node(&estate).await;
    let mut stream = caller.nodes(&carrier).stop(&node).await.expect("the fabric-primary accepts the stop");
    let accepted = stream.accepted().clone();
    assert_eq!(frames_of(&mut stream).await.last(), Some(&Frame::Complete));

    // The claim names mesh2.admin.1; the call goes to the fabric-primary.
    let wrong = attempt_run(&caller, "mesh1.admin.1", &accepted.build_id, accepted.attempt, "mesh2.admin.1").await;
    assert_eq!(wrong, BuildReply::NotExecutor { named: "mesh2.admin.1".into(), recipient: "mesh1.admin.1".into() });

    // The right recipient, a claim ahead of the Build and one behind it: neither is its current folded attempt.
    let ahead = attempt_run(&caller, "mesh2.admin.1", &accepted.build_id, accepted.attempt + 5, "mesh2.admin.1").await;
    assert_eq!(ahead, BuildReply::StaleClaim { held_attempt: accepted.attempt, held_executor: Some("mesh2.admin.1".into()), carried_attempt: accepted.attempt + 5 });
    let behind = attempt_run(&caller, "mesh2.admin.1", &accepted.build_id, accepted.attempt - 1, "mesh2.admin.1").await;
    assert_eq!(behind, BuildReply::StaleClaim { held_attempt: accepted.attempt, held_executor: Some("mesh2.admin.1".into()), carried_attempt: accepted.attempt - 1 });

    estate.stop().await;
    let spans = estate.spans();
    assert!(!spans_named(&spans, "rdm.node_admin.build.reject.via-not-executor").is_empty(), "the refusal is spanned");
    assert!(!spans_named(&spans, "rdm.node_admin.build.reject.via-stale-claim").is_empty());
    let refused = spans_named(&spans, "rdm.node_admin.build.reject.via-stale-claim").into_iter().next().unwrap();
    estate.record_trace_url(refused["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (acceptance 3): the executor is lost in the middle of its run. The fabric-primary's inner
/// call ends indeterminate; it claims the next attempt for the successor only after it holds proof
/// of the executor's departure (its exact runtime inspected and exited), never on silence; and the
/// caller's one stream carries the successor's frames to `Complete`. No event is delivered twice.
///
/// The node is stopped first (node.stop, complete). The executor is lost at the same step every run of
/// its node.start: its own Door holds it in the start, the node's `start-node` answered `Started` and
/// its `StartNode` receipt not yet durable, and it is killed while held. The successor reissues the
/// start, which the node settles `AlreadyApplied`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_executor_ends_the_inner_call_indeterminate_and_the_successor_is_claimed_only_after_proof() {
    let test = "a_lost_executor_ends_the_inner_call_indeterminate_and_the_successor_is_claimed_only_after_proof";
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let mut estate = Estate::bootstrap_on(owner(test), FABRIC, "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = secondary_node(&estate).await;

    let mut stopped = caller.nodes(&carrier).stop(&node).await.expect("the fabric-primary accepts the stop");
    assert_eq!(frames_of(&mut stopped).await.last(), Some(&Frame::Complete), "the node is stopped first");
    // The executor, mesh2's primary, is held in the middle of the start: the start of the stopped node
    // answered, its receipt not durable.
    const EXECUTOR: &str = "mesh2.admin.1";
    let door = Door::at(&estate, EXECUTOR).await;
    door.arm("lost", json!({"kind": "receipt", "step": "StartNode", "operation": "node.start", "node": node.to_string()})).await;
    let mut stream = caller.nodes(&carrier).start(&node).await.expect("the fabric-primary accepts the start");
    let accepted = stream.accepted().clone();
    let held = door.wait_held("lost").await;
    assert_eq!(s(&held["hit"]["build_id"]), accepted.build_id.as_str(), "{held}");
    assert_eq!(held["hit"]["attempt"], json!(accepted.attempt), "the executor is held in the attempt the start was accepted at: {held}");
    estate.kill_node(EXECUTOR).await;
    let frames = frames_of(&mut stream).await;
    assert_eq!(frames.last(), Some(&Frame::Complete), "the caller's stream carries the successor's frames to the end: {frames:?}");
    let seen = events(&frames);
    let mut once = seen.clone();
    once.dedup();
    assert_eq!(seen, once, "no event is delivered twice: {frames:?}");
    assert_eq!(seen.last(), Some(&NodeEvent::Started), "{seen:?}");

    estate.stop().await;
    let spans = estate.spans();
    let build = &accepted.build_id.to_string();
    let proofs: Vec<_> = spans_named(&spans, "rdm.node_admin.build.update.via-departure-proven").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["attempt"]) == accepted.attempt.to_string()).collect();
    assert!(!proofs.is_empty(), "the departure was proven before the next attempt was claimed: {proofs:?}");
    assert_eq!(s(&proofs[0]["attributes"]["executor"]), "mesh2.admin.1");
    let claims: Vec<_> = spans_named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build && sp["attributes"]["attempt"].as_str().and_then(|a| a.parse::<u32>().ok()) > Some(accepted.attempt)).collect();
    let successor = claims.iter().find(|sp| s(&sp["attributes"]["executor"]) == "mesh2.admin.2").expect("the next attempt was claimed for the successor");
    assert!(successor["start_unix_nano"].as_u64() > proofs[0]["start_unix_nano"].as_u64(), "the claim for the successor came after the proof");
    let lost = spans_named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["outcome"]) == "executor-lost").expect("the inner call to the lost executor ended");
    assert!(!s(&lost["attributes"]["reason"]).is_empty(), "the executor-lost dispatch names why the inner call ended: {lost}");
    estate.record_trace_url(lost["trace_id"].as_str().unwrap_or(""));
}

/// The executor every os-stopped cell holds: mesh2's primary, which runs the start of a node of mesh2.
const SUSPENDED: &str = "mesh2.admin.1";

/// An executor the operating system holds suspended in the middle of its attempt: `SUSPENDED` runs the
/// node.start of `node`, its own Door holds it in the start (the node answered `Started`, its
/// `StartNode` receipt not yet durable), and its exact runtime is then stopped (SIGSTOP, acknowledged
/// `T`), so it neither answers nor exits.
struct Suspended {
    estate: Estate,
    carrier: BuildCarrier,
    node: PathName,
    accepted: rdm_client::Accepted,
    stream: WorkflowStream,
    door: Door,
    rt: rdm_test::process_faults::ExactRuntime,
    /// The suspended birth's node id and data dir.
    node_id: String,
    data_dir: String,
    /// Its CPU ticks at the stop.
    ticks: u64,
}

async fn suspended_executor(test: &str) -> Suspended {
    suspended_executor_withholding(test, false).await
}

/// [`suspended_executor`], with `withhold`: the executor publishes none of its mesh's topology, on the
/// backbone or into its mesh, from before the start (the door's `aggregate` cut), so the clear of the birth's stop mark
/// it makes at the start's join is never published before it is held.
async fn suspended_executor_withholding(test: &str, withhold: bool) -> Suspended {
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let estate = Estate::bootstrap_on(owner(test), FABRIC, "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = secondary_node(&estate).await;
    let mut stopped = caller.nodes(&carrier).stop(&node).await.expect("the fabric-primary accepts the stop");
    assert_eq!(frames_of(&mut stopped).await.last(), Some(&Frame::Complete), "the node is stopped first");
    let executor = estate.node_value(SUSPENDED).await;
    assert_eq!(executor["is_primary"], true, "{SUSPENDED} is mesh2's primary, the executor of mesh2's node: {executor}");
    let node_id = s(&executor["node_id"]);
    let data_dir = estate.data_dir(SUSPENDED).await;
    let rt = rdm_test::process_faults::ExactRuntime::published(std::path::Path::new(&data_dir)).unwrap_or_else(|r| panic!("{SUSPENDED}: no exact runtime published in {data_dir}: {r:?}"));
    let door = Door::at(&estate, SUSPENDED).await;
    if withhold {
        door.arm("unpublished", json!({"kind": "announcement", "announcement": "aggregate", "channel": "any"})).await;
    }
    door.arm("held", json!({"kind": "receipt", "step": "StartNode", "operation": "node.start", "node": node.to_string()})).await;
    let stream = caller.nodes(&carrier).start(&node).await.expect("the fabric-primary accepts the start");
    let accepted = stream.accepted().clone();
    let held = door.wait_held("held").await;
    assert_eq!(s(&held["hit"]["build_id"]), accepted.build_id.as_str(), "{held}");
    assert_eq!(held["hit"]["attempt"], json!(accepted.attempt), "the executor is held in the attempt the start was accepted at: {held}");
    let stop = rt.apply(rdm_test::process_faults::Fault::Stop).expect("the exact runtime is suspended");
    assert_eq!(stop.state_after, Some('T'), "the OS holds {SUSPENDED}: {stop:?}");
    let ticks = rdm_test::process_faults::cpu_ticks(rt.pid);
    Suspended { estate, carrier, node, accepted, stream, door, rt, node_id, data_dir, ticks }
}

/// CONTRACT (node-start.md step 2, Q249): mesh2's primary carries a start of the stopped mesh2 rpc node,
/// clears the birth's stop mark at the start's join, and is held before it publishes the clear (it
/// withholds its mesh's topology while it carries the start), then os-stopped. The next attempt's
/// executor, mesh2.admin.2, reissues the start; the birth answers it `AlreadyApplied` (no `NodeJoined`),
/// and the executor, whose overlay holds the birth stopped, clears the mark at that answer and publishes
/// it as the mesh's primary: the birth's digests are taken again, the start completes, and the Build
/// with it. Must NOT happen: the birth held stopped by every view but the held executor's, its digests
/// refused `via-stopped-birth` until the start fails waiting for one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replayed_start_clears_the_stop_mark_its_held_executor_never_published() {
    let test = "a_replayed_start_clears_the_stop_mark_its_held_executor_never_published";
    let Suspended { mut estate, mut stream, rt, node, .. } = suspended_executor_withholding(test, true).await;
    started_to_the_end(&mut stream, "the replayed start completes at the next executor").await;
    rt.apply(rdm_test::process_faults::Fault::Kill(estate.fabric_ready())).expect("the cell kills the suspended runtime it made");
    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let cleared = spans_named(&spans, "rdm.node_admin.node.update.via-command-frame")
        .into_iter()
        .find(|sp| attr(sp, "recorder") == "mesh2.admin.2" && attr(sp, "node") == node.to_string() && attr(sp, "outcome") == "stop-mark-cleared")
        .cloned()
        .unwrap_or_else(|| panic!("mesh2.admin.2 clears {node}'s stop mark at the replayed start's answer"));
    let birth = attr(&cleared, "incarnation_id");
    let at = cleared["start_unix_nano"].as_u64().unwrap_or(0);
    let late: Vec<&Value> = spans_named(&spans, "rdm.mesh.topology.reject.via-stopped-birth").into_iter().filter(|sp| attr(sp, "incarnation_id") == birth && sp["start_unix_nano"].as_u64().unwrap_or(0) > at + 5_000_000_000).collect();
    assert!(late.is_empty(), "no view refuses {node}'s digests as a stopped birth once the clear is published: {late:#?}");
}

/// The caller's stream of the start read to its end within the bound of a node recovery: `Complete`,
/// every event once, `Started` last.
async fn started_to_the_end(stream: &mut WorkflowStream, why: &str) {
    let frames = tokio::time::timeout(crate::node_recovery_due() * 2, frames_of(stream)).await.unwrap_or_else(|_| panic!("{why}: the caller's stream of the start has no terminal within {:?}", crate::node_recovery_due() * 2));
    assert_eq!(frames.last(), Some(&Frame::Complete), "the caller's stream carries the next attempt's frames to the end: {frames:?}");
    let seen = events(&frames);
    let mut once = seen.clone();
    once.dedup();
    assert_eq!(seen, once, "no event is delivered twice: {frames:?}");
    assert_eq!(seen.last(), Some(&NodeEvent::Started), "{seen:?}");
}

/// `path` re-created as a new birth (a node id other than `old`), ready, within the bound of
/// a node recovery.
async fn re_created(estate: &Estate, path: &str, old: &str) -> Value {
    wait_for(&format!("{path} re-created as a new birth"), crate::node_recovery_due() * 2, || async { estate.node_value_opt(path).await.filter(|n| n["status"] == "ready" && s(&n["node_id"]) != old) }).await
}

/// The spans one process of the estate wrote, by its pid: the evidence file `<service>.<pid>-*.spans.jsonl`.
fn spans_of_pid(estate: &Estate, pid: u32) -> Vec<Value> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(&estate.evidence).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with(".spans.jsonl") && name.contains(&format!(".{pid}-")) {
            out.extend(std::fs::read_to_string(e.path()).unwrap_or_default().lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()));
        }
    }
    out.sort_by_key(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0));
    out
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// CONTRACT (build.md: "when the executor is lost with the attempt unfinished, after the provider's
/// inspection of the executor's exact runtime answers `exited` or `os-stopped`"; node-recovery.md step
/// 3: TerminateRuntime "never skipped for what it reads, except `os-stopped`, which recovery leaves as
/// it is"; Q239 A narrowed): mesh2's primary is the executor of a node.start and the operating system
/// holds it suspended in the middle of it. The fabric-primary's inner call ends executor-lost, the
/// provider reads the exact runtime os-stopped, which proves its departure, and the next attempt is
/// claimed for mesh2.admin.2, which takes the start to `Complete` on the caller's one stream. The
/// fabric-primary's one investigation of the unheard mesh-primary finds it os-stopped and its recovery
/// is admitted once the next attempt ran (a submit while that attempt holds the mesh answers
/// BuildInProgress, node-recovery.md step 1): the path is re-created as a new birth, and the
/// suspended runtime is left as it is: still `T`, no CPU used, its TerminateRuntime reading os-stopped.
/// Must NOT happen: the attempt held on os-stopped while the mesh's recovery is refused BuildInProgress
/// behind it (gate run gates-4-41c1b4003: attempt 4 executor-lost, then fourteen refusals), a signal to
/// the suspended runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_os_stopped_executors_unfinished_attempt_is_claimed_by_the_next_and_its_mesh_recovered_leaving_the_runtime_stopped() {
    let test = "an_os_stopped_executors_unfinished_attempt_is_claimed_by_the_next_and_its_mesh_recovered_leaving_the_runtime_stopped";
    let Suspended { mut estate, carrier, accepted, mut stream, rt, node_id, data_dir, ticks, .. } = suspended_executor(test).await;
    started_to_the_end(&mut stream, "the os-stopped executor's attempt is claimed by the next").await;
    let recovered = re_created(&estate, SUSPENDED, &node_id).await;
    assert_ne!(estate.data_dir(SUSPENDED).await, data_dir, "a newly allocated data dir, never the suspended runtime's");
    assert_eq!(rdm_test::process_faults::proc_state(rt.pid), Some('T'), "the suspended runtime is sent nothing: still stopped");
    assert_eq!(rdm_test::process_faults::cpu_ticks(rt.pid), ticks, "the suspended runtime used no CPU");
    // The cell's own cleanup: the suspended runtime the recovery left is no estate's.
    rt.apply(rdm_test::process_faults::Fault::Kill(estate.fabric_ready())).expect("the cell kills the suspended runtime it made");

    estate.stop().await;
    let spans = estate.spans();
    let fp = carrier.fabric_primary().to_string();
    let build = &accepted.build_id.to_string();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let of_build = |name: &str| -> Vec<Value> { spans_named(&spans, name).into_iter().filter(|sp| attr(sp, "build_id") == *build).cloned().collect() };
    let lost = of_build("rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| attr(sp, "attempt") == accepted.attempt.to_string() && attr(sp, "outcome") == "executor-lost").expect("the inner call to the suspended executor ended executor-lost");
    let proof = of_build("rdm.node_admin.build.update.via-departure-proven").into_iter().find(|sp| attr(sp, "attempt") == accepted.attempt.to_string()).unwrap_or_else(|| panic!("the os-stopped executor's departure is proven: {:#?}", of_build("rdm.node_admin.fabric.update.via-blocked")));
    assert_eq!(attr(&proof, "executor"), SUSPENDED, "{proof}");
    let claim = of_build("rdm.node_admin.build.update.via-claim-decision")
        .into_iter()
        .find(|sp| attr(sp, "node") == fp && attr(sp, "outcome") == "won" && attr(sp, "attempt").parse::<u32>().ok() > Some(accepted.attempt))
        .expect("the next attempt is claimed");
    assert_eq!(attr(&claim, "executor"), "mesh2.admin.2", "the next attempt is the other node-admin of mesh2's: {claim}");
    assert!(claim["start_unix_nano"].as_u64() > proof["start_unix_nano"].as_u64(), "the claim follows the proof");
    // The mesh: the fabric-primary's investigation of the unheard mesh-primary finds it os-stopped, and the
    // recovery it opens is admitted, never refused behind the held attempt.
    let investigations: Vec<Value> = spans_named(&spans, "rdm.node_admin.primary.update.via-investigation").into_iter().filter(|sp| attr(sp, "node") == fp && attr(sp, "suspect_node_id") == node_id).cloned().collect();
    assert!(!investigations.is_empty() && investigations.iter().all(|sp| attr(sp, "finding") == "os-stopped"), "the fabric-primary's investigation finds the suspended mesh-primary os-stopped: {investigations:#?}");
    let recoveries: Vec<Value> = spans_named(&spans, "rdm.node_admin.build.update.via-recovery").into_iter().filter(|sp| attr(sp, "node_id") == node_id).cloned().collect();
    // A submit while an attempt holds the mesh answers BuildInProgress (node-recovery.md step 1); the
    // suspended executor's attempt holds it no longer than the next attempt's run.
    let next_ran = of_build("rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| attr(sp, "attempt") == attr(&claim, "attempt") && attr(sp, "outcome") == "complete").unwrap_or_else(|| panic!("the next attempt {} completes", attr(&claim, "attempt")));
    let ended = next_ran["end_unix_nano"].as_u64().unwrap_or(0);
    let refused: Vec<&Value> = recoveries.iter().filter(|sp| attr(sp, "outcome") == "build-in-progress" && sp["start_unix_nano"].as_u64().unwrap_or(0) > ended).collect();
    assert!(refused.is_empty(), "the recovery of the mesh-primary is never refused behind the suspended executor's attempt once the next attempt ran: {refused:#?}");
    let admitted = recoveries.iter().find(|sp| attr(sp, "outcome") == "admitted").unwrap_or_else(|| panic!("the recovery is admitted: {recoveries:#?}"));
    assert!(admitted["start_unix_nano"].as_u64() > proof["start_unix_nano"].as_u64(), "the recovery is admitted after the departure proof: {admitted}");
    assert_eq!(investigations.len(), 1, "the mesh is recovered within one investigation: {investigations:#?}");
    let at_delete = spans_named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").into_iter().find(|sp| attr(sp, "node_id") == node_id && attr(sp, "caller") == "terminate").cloned().expect("the recovery's TerminateRuntime reads the exact runtime at its instant");
    assert_eq!(attr(&at_delete, "verdict"), "os-stopped", "{at_delete}");
    let signalled: Vec<&Value> = spans_named(&spans, "rdm.node_admin.deployment.reject.via-lost-executor").into_iter().filter(|sp| attr(sp, "node") == SUSPENDED).collect();
    assert!(signalled.is_empty(), "the recovery stops no runtime of the suspended birth: {signalled:#?}");
    println!("recovered {SUSPENDED}: {} -> {}", node_id, s(&recovered["node_id"]));
    println!("executor lost: http://192.168.68.99:16687/trace/{}", s(&lost["trace_id"]));
    println!("departure proven os-stopped: http://192.168.68.99:16687/trace/{}", s(&proof["trace_id"]));
    println!("investigation: http://192.168.68.99:16687/trace/{}", s(&investigations[0]["trace_id"]));
    println!("runtime left os-stopped: http://192.168.68.99:16687/trace/{}", s(&at_delete["trace_id"]));
    estate.record_trace_url(&s(&proof["trace_id"]));
}

/// CONTRACT (build.md: "an os-stopped executor that resumes after the next attempt is claimed is a
/// deleted birth whose writes every reader refuses"; node-recovery.md step 3: "A birth that comes back
/// after it was re-created is a deleted birth"; Q239 addendum 0): the os-stopped executor of a
/// node.start is resumed (SIGCONT) at once after the fabric-primary claimed the next attempt for
/// mesh2.admin.2, while its mesh's recovery still runs, and its Door lets the held start go on. Every
/// receipt and frame it makes for the superseded attempt lands nowhere: its run reports them up a
/// stream no fabric-primary reads, the Build's journal holds no `StartNode` receipt and no verdict of
/// the superseded attempt, and the next attempt's verdict is the Build's. The re-created birth at its
/// path.name holds. Its own record, which the resumed birth writes whole from its copy older than the
/// recovery's `Deleted` write, is ignored on read (build.md: "a deleted birth whose writes every
/// reader refuses"; Q109: "a row that does not follow the fold is ignored by every reader, refused by
/// no storage"): the record carries its writer's birth, the recovery's claim reads the `Deleted`
/// record its writer holds as the previous record at the name, so the re-created birth names the
/// resumed one its predecessor, and every reader of the mesh's records ignores the version the
/// resumed birth wrote, naming it on `rdm.node_admin.node.reject.via-deleted-birth`.
/// Must NOT happen: a record of the superseded attempt in the journal, the Build decided by it, the
/// re-created birth displaced, or the resumed birth's own write read as its record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_os_stopped_executor_resumed_after_the_next_attempts_claim_lands_no_receipt_of_its_attempt() {
    let test = "an_os_stopped_executor_resumed_after_the_next_attempts_claim_lands_no_receipt_of_its_attempt";
    let Suspended { mut estate, carrier, node, accepted, mut stream, door, rt, node_id, .. } = suspended_executor(test).await;
    let fp = carrier.fabric_primary().to_string();
    let build = accepted.build_id.to_string();
    let superseded = accepted.attempt;
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    // The mesh-primary's recovery (the investigation ended os-stopped) is held by its executor's own
    // Door once its delete wrote the suspended birth's row Deleted and before its create: the window
    // after the next attempt's claim in which the recovery runs. Then at once the resume and the
    // suspended executor's release, and the recovery goes on once the resumed run of the superseded
    // attempt has ended (its last frame written to a stream no fabric-primary reads).
    let recovering = Door::at(&estate, "mesh2.admin.2").await;
    recovering.arm("recovery", json!({"kind": "receipt", "step": "NodeDeleted", "operation": "node.delete", "node": SUSPENDED})).await;
    let cut = recovering.wait_held("recovery").await;
    let (claim, recovery) = wait_for("the next attempt and the suspended birth's recovery claimed", crate::node_recovery_due(), || async {
        let spans = estate.spans();
        let won = |attempt: &str| spans_named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().find(|sp| attr(sp, "build_id") == build && attr(sp, "node") == fp && attr(sp, "outcome") == "won" && attr(sp, "attempt") == attempt).cloned();
        let recovery = spans_named(&spans, "rdm.node_admin.build.update.via-recovery").into_iter().find(|sp| attr(sp, "node_id") == node_id && attr(sp, "outcome") == "admitted").cloned()?;
        let next = won(&(superseded + 1).to_string())?;
        won(&attr(&recovery, "attempt")).map(|_| (next, recovery))
    })
    .await;
    assert_eq!(cut["hit"]["attempt"], json!(attr(&recovery, "attempt").parse::<u32>().unwrap()), "the recovery's attempt is held: {cut}");
    let resumed_at = now_ns();
    let resumed = rt.apply(rdm_test::process_faults::Fault::Continue).expect("the suspended runtime is resumed");
    assert_ne!(resumed.state_after, Some('T'), "{resumed:?}");
    door.release("held").await;
    let at_resume = estate.node_value_opt(SUSPENDED).await;
    assert!(at_resume.as_ref().is_none_or(|n| s(&n["node_id"]) == node_id || n["status"] != "ready"), "the resume falls while the recovery still runs: {at_resume:?}");
    assert_eq!(attr(&claim, "executor"), "mesh2.admin.2", "{claim}");
    wait_for("the resumed run of the superseded attempt ended", crate::node_recovery_due(), || async {
        spans_of_pid(&estate, rt.pid).iter().any(|sp| sp["name"] == "rdm.node_rpc.stream.serve.via-terminal" && sp["start_unix_nano"].as_u64().unwrap_or(0) >= resumed_at).then_some(())
    })
    .await;
    recovering.release("recovery").await;
    println!("resumed after the next attempt {} and the recovery attempt {} were claimed", attr(&claim, "attempt"), attr(&recovery, "attempt"));

    started_to_the_end(&mut stream, "the next attempt's start").await;
    let recovered = re_created(&estate, SUSPENDED, &node_id).await;
    // The resumed birth is a deleted birth: its mesh-primary reports it and the fabric-primary
    // terminates its runtime.
    wait_for("the resumed birth's runtime ended", crate::node_recovery_due() * 2, || async { rt.check().is_err().then_some(()) }).await;
    let held = estate.node_value(SUSPENDED).await;
    assert_eq!((s(&held["node_id"]), s(&held["status"])), (s(&recovered["node_id"]), "ready".to_string()), "the re-created birth holds its path.name: {held}");
    assert_eq!(estate.node_value(&node.to_string()).await["status"], "ready", "the started node stands");
    // The journal: nothing of the superseded attempt from its executor.
    let facts = crate::build_log::log_read(&carrier, &build).await.unwrap_or_else(|e| panic!("the Build log of {fp}: {e}"));
    let landed: Vec<&BuildFact> = facts
        .iter()
        .filter(|f| match f {
            BuildFact::Step(r) => r.attempt == superseded && r.step == "StartNode",
            BuildFact::Attempt(a) => a.attempt == superseded && matches!(a.outcome, rdm_admin::build_state::AttemptOutcome::Converged | rdm_admin::build_state::AttemptOutcome::HandedOff { .. }),
            _ => false,
        })
        .collect();
    let projection = crate::build_log::projection(&facts, &build).expect("the Build's acceptance");
    let reported = projection.reported.clone();
    // The decision ends once its terminate report is answered: owed while an attempt holds the mesh
    // (the recovery that re-created the birth), so it is waited for, never cut by the estate's stop.
    wait_for("the resumed birth is decided and reported", crate::node_recovery_due() * 2, || async {
        spans_named(&estate.spans(), "rdm.node_admin.node.update.via-refused-caller").into_iter().find(|sp| attr(sp, "node_id") == node_id && attr(sp, "outcome") == "terminate-reported").map(|_| ())
    })
    .await;

    estate.stop().await;
    let spans = estate.spans();
    // What the resumed process wrote after its resume, by span: its receipt, its frame and its rows.
    let after: Vec<Value> = spans_of_pid(&estate, rt.pid).into_iter().filter(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0) >= resumed_at).collect();
    let mut seen: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for sp in &after {
        let n = s(&sp["name"]);
        if n.contains(".reject.") || n.contains("via-node-row") || n.contains("receipt") || n.contains("via-attempt-run") || n.contains("via-stale") || n.contains("via-superseded") || n.contains("via-deleted-birth") {
            seen.entry(n).or_default().push(format!("{} {} {}", attr(sp, "outcome"), attr(sp, "build_id"), attr(sp, "attempt")));
        }
    }
    println!("the resumed birth's spans after its resume ({} in all): {seen:#?}", after.len());
    // The mesh's records as a reader reads them from the store, and every version the store holds of
    // the resumed birth's own record, with the birth that wrote it.
    let records = {
        let root = rdm_node::store_root::StoreRoot::open(&estate.store_dir(), &rdm_core::FabricId::parse(&estate.fabric_id).unwrap()).expect("the fabric's store root opens");
        let writer = rdm_node::store_log::LogWriter::mint();
        let meshes = rdm_admin::storage::FileMeshStorage::open(&root, &writer).await.expect("the mesh storage opens");
        let rows = rdm_admin::storage::FileNodesStorage::open(&estate.store_dir(), &root, "mesh2", &writer).await.expect("the nodes storage opens");
        rdm_admin::storage::records(&meshes, &rows, "mesh2").await.expect("the mesh's records read")
    };
    let versions: Vec<rdm_admin::storage::NodeRow> = rdm_node::store_log::read_file(&estate.store_dir().join(&estate.fabric_id).join(rdm_node::store_root::NODES_FILE))
        .expect("the fabric's nodes file reads")
        .into_iter()
        .filter(|l| l.kind() == "node-record" && l.key == node_id)
        .map(|l| serde_json::from_value(l.record).expect("a node record"))
        .collect();
    let born = versions.iter().find_map(|r| r.newest_birth().map(|b| b.incarnation_id.clone())).expect("the resumed birth's record keeps its birth");
    let own_writes: Vec<&rdm_admin::storage::NodeRow> = versions.iter().filter(|r| r.writer.as_ref().is_some_and(|w| w.node_id.as_str() == node_id)).collect();
    for r in &versions {
        println!("version of {} ({node_id}): {:?} revision {} written by {:?}", r.name, r.status, r.revision, r.writer.as_ref().map(|w| format!("{}@{}", w.node_id, w.incarnation_id.as_str())));
    }
    let successor = records.iter().find(|r| r.node_id.as_str() == s(&recovered["node_id"])).cloned().expect("the re-created birth's record reads");
    // A read of mesh2's records that met a version the resumed birth wrote after its delete (its own,
    // or another node's over a record the reader held) names it ignored.
    let ignored: Vec<Value> = spans_named(&spans, "rdm.node_admin.node.reject.via-deleted-birth").into_iter().filter(|sp| attr(sp, "writer").starts_with(&format!("{node_id}@"))).cloned().collect();
    for sp in &ignored {
        println!("a version the resumed birth wrote ignored on read ({}): {} ({}) {} revision {}, held {}: trace http://192.168.68.99:16687/trace/{}", attr(sp, "read"), attr(sp, "subject"), attr(sp, "node_id"), attr(sp, "status"), attr(sp, "revision"), attr(sp, "held"), s(&sp["trace_id"]));
    }
    let refusals: Vec<Value> = spans.iter().filter(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0) >= resumed_at && (attr(sp, "build_id") == build || attr(sp, "node_id") == node_id) && s(&sp["name"]).contains(".reject.")).cloned().collect();
    for sp in &refusals {
        println!("refusal after the resume: {} {} at {} attempt {} subject {} trace http://192.168.68.99:16687/trace/{}", s(&sp["name"]), attr(sp, "outcome"), attr(sp, "node"), attr(sp, "attempt"), attr(sp, "subject"), s(&sp["trace_id"]));
    }
    assert!(landed.is_empty(), "nothing of the superseded attempt {superseded} lands in the Build's journal: {landed:#?}");
    // The resumed birth is a caller its mesh-primary does not hold (Q207), whichever door it comes back
    // through: its Digest, refused once a receiver holds it deleted, or its join answering the
    // mesh-primary's command, refused by name once its path.name holds the re-created birth (rdm#153,
    // gate gates-6-9af200cc4: resumed before `NodeDeleted`, it joined, was refused, and left its mesh
    // silent). Either way it is decided deleted and reported, and the fabric-primary opens its
    // termination.
    let decided = spans_named(&spans, "rdm.node_admin.node.update.via-refused-caller").into_iter().find(|sp| attr(sp, "node_id") == node_id && attr(sp, "outcome") == "terminate-reported").cloned().unwrap_or_else(|| {
        panic!("the resumed birth is decided and reported: {:#?} {:#?}", spans_named(&spans, "rdm.node_admin.node.update.via-refused-caller"), spans_named(&spans, "rdm.node_admin.node.reject.via-join-mismatch"))
    });
    let terminated = spans_named(&spans, "rdm.node_admin.build.update.via-terminate-returned").into_iter().find(|sp| attr(sp, "node_id") == node_id).cloned().expect("the fabric-primary opens the resumed birth's termination");
    println!("refused caller decided at its {} door: http://192.168.68.99:16687/trace/{}", attr(&decided, "door"), s(&decided["trace_id"]));
    println!("termination opened: http://192.168.68.99:16687/trace/{}", s(&terminated["trace_id"]));
    let next: u32 = attr(&claim, "attempt").parse().unwrap();
    assert!(reported.contains(&next) && !reported.contains(&superseded), "the next attempt {next}'s verdict is the Build's, and the superseded attempt {superseded} has none: {reported:?}");
    assert!(own_writes.iter().all(|r| r.writer.as_ref().is_some_and(|w| w.incarnation_id == born)) && !own_writes.is_empty(), "every version of its own record the birth wrote names it, its node and incarnation, as its writer (Q109): {versions:#?}");
    assert_eq!(successor.predecessor.as_ref().map(|p| (p.node_id.as_str().to_string(), p.incarnation_id.clone())), Some((node_id.clone(), born.clone())), "the re-created birth names the resumed birth its predecessor: the recovery's claim read the Deleted record, never the resumed birth's version: {successor:?}");
    let held: Vec<&rdm_admin::storage::NodeRow> = records.iter().filter(|r| r.node_id.as_str() == node_id).collect();
    assert!(held.iter().all(|r| r.status == rdm_core::model::NodeStatus::Deleted), "a reader of mesh2's records never holds the resumed birth live: its own version is ignored on read: {held:#?}");
    println!("next attempt claimed: http://192.168.68.99:16687/trace/{}", s(&claim["trace_id"]));
    estate.record_trace_url(&s(&ignored.first().unwrap_or(&claim)["trace_id"]));
}

/// CONTRACT (acceptance 2): the fabric-primary dies after claiming an attempt and before dispatching
/// it. Fabric recovery restores the node it re-creates at its path.name (Q122), which resumes the Build from its projection: it calls the executor the
/// claim names with `build.attempt.run`, which starts the claimed attempt, and the caller's re-submit
/// by the Build id streams it to `Complete`. No step has a second receipt, and no attempt beyond the
/// claimed one is opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_fabric_primary_starts_the_attempt_its_predecessor_claimed_and_no_step_runs_twice() {
    let test = "a_restored_fabric_primary_starts_the_attempt_its_predecessor_claimed_and_no_step_runs_twice";
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let mut estate = Estate::bootstrap_on(owner(test), FABRIC, "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = secondary_node(&estate).await;
    let fp = carrier.fabric_primary().to_string();

    // The cut: the fabric-primary's claim is durable and broadcast, and the drive has not dispatched.
    let door = Door::at(&estate, &fp).await;
    door.arm("claimed", json!({"kind": "claimed-attempt", "attempt": null})).await;
    let mut stream = caller.nodes(&carrier).stop(&node).await.expect("the fabric-primary accepts the stop");
    let accepted = stream.accepted().clone();
    let held = door.wait_held("claimed").await;
    assert_eq!(s(&held["hit"]["build_id"]), accepted.build_id.as_str(), "{held}");
    assert_eq!(s(&held["hit"]["executor"]), "mesh2.admin.1", "the claim names mesh2's primary, which is alive");
    // The fabric-primary dies at that cut.
    estate.kill_bootstrap();
    let end = loop {
        match stream.next().await {
            Some(Ok(_)) => continue,
            Some(Err(end)) => break end,
            None => panic!("the stream ended without saying how"),
        }
    };
    assert!(matches!(end, CallEnd::Indeterminate { .. }), "a dead fabric-primary ends the caller's stream indeterminate, never as a failed step: {end:?}");

    // Fabric recovery restores the node it re-creates at the fabric-primary's path.name (Q122); the caller
    // re-submits there by the Build id.
    let restored = caller.restored_fabric_primary(&fp).await;
    let carrier = caller.carrier_at(&restored);
    let mut resumed = caller.nodes(&carrier).resume(WorkflowKind::Stop(node), &accepted).await.expect("the restored fabric-primary accepts the re-submit");
    assert_eq!(resumed.accepted().build_id, accepted.build_id, "never a second Build");
    let frames = frames_of(&mut resumed).await;
    assert_eq!(frames.last(), Some(&Frame::Complete), "{frames:?}");
    assert_eq!(events(&frames), STOP_EVENTS, "{frames:?}");

    let receipts = carrier.clone().build(&accepted.build_id).await.expect("build.get [id]");
    let mut keys: Vec<(u32, String, String)> = receipts.steps.iter().filter(|r| r.attempt >= accepted.attempt).map(|r| (r.attempt, r.operation.clone(), r.step.clone())).collect();
    let before = keys.len();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), before, "no step has two receipts");
    assert_eq!(receipts.attempt, accepted.attempt, "the restored fabric-primary started the attempt that was claimed and opened no other");

    // The estate stops through the admin that holds the seat now.
    // A surviving admin's view names the restored birth: the estate dials it from then on.
    let _ = estate.node_views_at(&path(&caller.survivor_of(&fp))).await;
    estate.enter(&path(&restored)).await;
    estate.stop().await;
    let spans = estate.spans();
    let build = &accepted.build_id.to_string();
    let run = spans_named(&spans, "rdm.node_admin.build.serve.via-attempt-run")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["attempt"]) == accepted.attempt.to_string())
        .expect("the executor was called with the claimed attempt");
    assert_eq!(s(&run["attributes"]["executor"]), "mesh2.admin.1");
    assert_eq!(s(&run["attributes"]["outcome"]), "started");
    estate.record_trace_url(run["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (acceptance 5; the approved proposal node-primary-update-fabric-mesh-and-node-type.md,
/// Part C1 step 8: "Only then does the commanding Build continue, and only once the successor's
/// installation is acknowledged"): a planned seat move mid-Build ends the caller's outer stream with
/// the typed new-seat frame, never `Indeterminate`. `mesh1` holds the fabric-primary seat, and the
/// Build deletes `mesh1`: before the delete is claimed, the fabric-primary moves the fabric seat alone
/// to `mesh2`'s primary by node.fabric-primary.update (op `0x21`, mesh-delete.md step 1), and the Build
/// goes on at the new fabric-primary. The successor installs the election's result the call carries
/// (`primary.update.via-primary-updated` at the new fabric-primary, `held`) before the old fabric-primary's drive refuses in its
/// span and names the new seat, so the caller's re-submit by the Build id at the seat the frame names
/// is accepted and streams to `Complete`.
/// Must NOT happen: the new seat named to the caller before it installed (its re-submit refused
/// `not-fabric-primary`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_handover_mid_build_ends_the_outer_stream_with_the_new_fabric_primary_and_the_old_one_dispatches_nothing() {
    let test = "a_planned_handover_mid_build_ends_the_outer_stream_with_the_new_fabric_primary_and_the_old_one_dispatches_nothing";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let old_seat = carrier.fabric_primary().to_string();
    assert!(old_seat.starts_with("mesh1."), "mesh1 holds the fabric-primary seat: {old_seat}");

    // The Build deletes mesh1; the seat leaves with it, by a planned handover inside the Build.
    let keep = FabricDesired { fabric: FABRIC.into(), meshes: vec![MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)])] };
    let mut stream = carrier.create(estate.shape_change_at(&carrier, &keep).await).await.expect("the fabric-primary accepts the delete of mesh1");
    let (build_id, attempt) = (stream.build_id().clone(), stream.attempt());
    let new_seat = loop {
        match stream.next().await {
            Some(Ok(BuildFrame::FabricPrimaryMoved { fabric_primary })) => break fabric_primary.expect("the old seat names the new one"),
            Some(Ok(BuildFrame::Complete { .. })) => panic!("the old fabric-primary cannot finish a delete of its own mesh"),
            Some(Ok(BuildFrame::Failed { operation, step, reason, .. })) => panic!("the Build failed at {operation} {step}: {reason}"),
            Some(Ok(_)) => continue,
            Some(Err(end)) => panic!("a planned seat move ends the outer stream with the typed new-seat frame, never as {end:?}"),
            None => panic!("the stream ended without a terminal frame"),
        }
    };
    assert!(new_seat.starts_with("mesh2."), "the seat moved to mesh2's primary: {new_seat}");
    assert_ne!(new_seat, old_seat);

    // The caller re-submits the same Build at the seat the frame names.
    let at_new_seat = BuildCarrier::new(caller.client.clone(), path(&new_seat));
    let mut resumed = at_new_seat.resubmit(&build_id, attempt).await.expect("the new seat accepts the re-submit by build_id");
    assert_eq!(resumed.build_id(), &build_id, "never a second Build");
    until_complete(&mut resumed).await;

    estate.enter(&path(&new_seat)).await;
    estate.stop().await;
    let spans = estate.spans();
    let build = &build_id.as_str();
    let handover_calls = spans_named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().filter(|sp| sp["attributes"]["op"].as_str().and_then(|o| o.parse::<u64>().ok()).or_else(|| sp["attributes"]["op"].as_u64()) == Some(0x21)).count();
    assert!(handover_calls >= 1, "the seat moved by the fabric-primary handover, op 0x21");
    let yielded: Vec<_> = spans_named(&spans, "rdm.node_admin.build.reject.via-fabric-primary-changed").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build).collect();
    assert!(!yielded.is_empty(), "the old fabric-primary's dispatch after its seat moved was refused, SeatYielded in its span");
    assert!(yielded.iter().all(|sp| s(&sp["attributes"]["node"]) == old_seat), "{yielded:?}");
    let installed: Vec<_> = spans_named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["node"]) == new_seat && s(&sp["attributes"]["outcome"]) == "held").collect();
    assert_eq!(installed.len(), 1, "the new fabric-primary installed the election's result once: {installed:?}");
    let installed_at = installed[0]["start_unix_nano"].as_u64().unwrap_or(u64::MAX);
    let early: Vec<_> = yielded.iter().filter(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0) <= installed_at).collect();
    assert!(early.is_empty(), "C1 step 8: the old seat's drive names the new seat only once the successor installed ({installed_at}): {early:?}");
    estate.record_trace_url(yielded[0]["trace_id"].as_str().unwrap_or(""));
}

impl Caller {
    /// The node-admins other than `dead`, as the answering admin's view held them before it died.
    fn admins_except(&self, dead: &str) -> Vec<String> {
        self.bases.lock().unwrap().keys().filter(|name| name.as_str() != dead).cloned().collect()
    }

    /// The carrier that starts at node-admin `at`: its reads answer from that admin's own view.
    fn carrier_at(&self, at: &str) -> BuildCarrier {
        BuildCarrier::new(self.client.clone(), path(at))
    }

    /// The first node-admin that survives `dead`.
    fn survivor_of(&self, dead: &str) -> String {
        self.admins_except(dead).into_iter().next().unwrap_or_else(|| panic!("no node-admin survives {dead}"))
    }

    /// The node-admin that is fabric-primary once `dead`, the fabric-primary, has died: nobody takes the role
    /// meanwhile, and fabric recovery restores the node it re-creates at `dead`'s path.name (Q122). A
    /// surviving admin's view is read until it names a Ready fabric-primary other than `dead`'s
    /// birth; that birth is made reachable here, and its path.name returned.
    async fn restored_fabric_primary(&self, dead: &str) -> String {
        let asked = path(&self.survivor_of(dead));
        let dead_id = self.ids.lock().unwrap().get(dead).cloned().unwrap_or_default();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        loop {
            if let Some(views) = rdm_test::typed::node_views_of(&self.peers, &asked, &self.fabric_id).await {
                let restored = views.into_iter().find(|n| n.kind == NodeKind::NodeAdmin && n.is_fabric_primary && n.status == NodeStatus::Ready && n.node_id.to_string() != dead_id);
                if let Some(n) = restored {
                    if let (Some(ep), Some(addr), Some(inc)) = (n.endpoint_id.as_deref(), n.transport_addr, n.incarnation_id.clone()) {
                        if let Ok(old) = rdm_core::NodeId::parse(&dead_id) {
                            self.resolver.remove_gone(&old);
                        }
                        self.resolver.insert(ResolvedNode { node_id: n.node_id.clone(), name: n.name.clone(), endpoint_id: ep.parse().expect("an iroh key"), transport_addr: addr, incarnation: inc });
                        if let Some(base) = n.admin_api_base.clone() {
                            self.bases.lock().unwrap().insert(n.name.to_string(), base);
                        }
                        // The restored node's own view names it fabric-primary once it installed the roles.
                        let name = n.name.to_string();
                        Fabrics::new(self.carrier_at(&name))
                            .fact(&format!("{name} names itself the fabric-primary"), Duration::from_secs(60), |f| f.fabric_primary.as_ref().is_some_and(|p| p.to_string() == name))
                            .await
                            .unwrap_or_else(|end| panic!("{end}"));
                        return name;
                    }
                }
            }
            assert!(tokio::time::Instant::now() < deadline, "{asked}'s view names no Ready fabric-primary other than {dead}'s birth {dead_id} within 120s");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// The next `Blocked` frame of `stream`. A terminal frame or a broken stream before it is a defect of
/// the cell; frames before it are returned beside it.
async fn next_blocked(stream: &mut BuildStream) -> (Vec<BuildFrame>, (String, String)) {
    let mut before = Vec::new();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(90), stream.next()).await.expect("a frame within the bound").expect("the stream goes on").expect("the stream does not break");
        match frame {
            BuildFrame::Blocked { step, reason, .. } => return (before, (step, reason)),
            BuildFrame::Complete { .. } | BuildFrame::Failed { .. } => panic!("the Build ended before the Blocked frame: {frame:?} after {before:?}"),
            other => before.push(other),
        }
    }
}

/// Whether `frame` is a Blocked frame of a step in flight: the Build's own Blocked naming the fabric's
/// change, which a Build that put the fabric back to pending reports once its operations are done
/// (Q154; states.md "Resize and executable replacement"), is no birth's.
fn birth_blocked(frame: &BuildFrame) -> bool {
    matches!(frame, BuildFrame::Blocked { step, .. } if step != "fabric-change")
}

/// How many attempts of `node`'s knob hook (`rdm_test::pre_node_ready_probe::KnobPreNodeReady`)
/// ended blocked for `reason`.
fn knob_attempts(knob: &std::path::Path, node: &str, reason: &str) -> usize {
    std::fs::read_to_string(knob.join(format!("{node}.attempts"))).unwrap_or_default().lines().filter(|l| *l == format!("blocked {reason}")).count()
}

/// CONTRACT (node-start.md step 3; node-rpc-envelope.md frame table, Blocked "emitted on a new
/// blocker or reason change, never on a timer"): a birth whose `pre_node_ready` is Blocked
/// answers its executor's `ProbeNodeState` with Status `Blocked { node_id, incarnation, reason,
/// retry_on }`, and the Build stream carries `Blocked { WaitForNodeReady, reason }` once per reason. The hook here runs again
/// every time its authority addresses it, so it is blocked again for the same reason many times;
/// none of those reruns frames anything. A changed reason frames once more; when the hook passes the
/// birth is Ready, the step completes and the Build is `Complete` with no further Blocked. The Builds
/// that birthed the shape, whose births register no hook (plain Pending until Ready, a node-admin's
/// door answering `NotReady` until its authority is filled), frame no Blocked: a `NotReady` is the
/// door not serving yet and never a blocker. What must NOT happen: a Blocked frame for a plain
/// Pending birth or a door's `NotReady`, a repeat for an unchanged reason, or a Blocked frame after
/// the hook passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blocked_birth_is_framed_once_per_reason_and_its_build_completes_when_the_hook_passes() {
    let test = "a_blocked_birth_is_framed_once_per_reason_and_its_build_completes_when_the_hook_passes";
    const SHAPE1: [(&str, u32, u32); 1] = [("mesh1", 2, 1)];
    const BLOCKED: &str = "mesh1.rpc.2";
    const A: &str = "the rows of orders are not yet published";
    const B: &str = "the authority of orders is connections-deleting";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let (carrier, shape_frames) = born_framed(&estate, &caller, &SHAPE1).await;
    assert!(!shape_frames.iter().any(birth_blocked), "births with no hook are plain Pending until Ready and frame no Blocked: {shape_frames:?}");

    // The next birth's hook is blocked for A from its first attempt.
    let knob = rdm_test::pre_node_ready_knob_dir(&estate.root);
    rdm_test::write_pre_node_ready_knob(&estate.root, BLOCKED, A).unwrap();
    let grown = FabricDesired { fabric: FABRIC.into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)])] };
    let mut stream = carrier.create(estate.shape_change_at(&carrier, &grown).await).await.expect("the fabric-primary accepts the grown shape");
    let (before_a, (step, reason)) = next_blocked(&mut stream).await;
    assert_eq!(step, "WaitForNodeReady", "the blocked step: {reason}");
    assert!(reason.contains(BLOCKED) && reason.contains(A) && reason.contains("primary-addressed"), "the frame names the birth, the hook's reason and its retry event: {reason}");
    assert!(!before_a.iter().any(|f| matches!(f, BuildFrame::Blocked { .. })));

    // Reruns blocked for the same reason: the executor's probes address the birth, so its hook runs
    // again on each; wait until it has been blocked for A three more times.
    wait_for("three more attempts blocked for A", Duration::from_secs(60), || async { (knob_attempts(&knob, BLOCKED, A) >= 4).then_some(()) }).await;
    rdm_test::write_pre_node_ready_knob(&estate.root, BLOCKED, B).unwrap();
    let (before_b, (step_b, reason_b)) = next_blocked(&mut stream).await;
    assert_eq!(step_b, "WaitForNodeReady");
    assert!(reason_b.contains(B) && !reason_b.contains(A), "the next Blocked frame is the changed reason, never A again: {reason_b}");
    assert!(!before_b.iter().any(|f| matches!(f, BuildFrame::Step { step, .. } if step == "WaitForNodeReady")), "the step is still in flight: {before_b:?}");

    wait_for("an attempt blocked for B", Duration::from_secs(60), || async { (knob_attempts(&knob, BLOCKED, B) >= 2).then_some(()) }).await;
    rdm_test::write_pre_node_ready_knob(&estate.root, BLOCKED, "pass").unwrap();
    let rest = until_complete(&mut stream).await;
    assert!(!rest.iter().any(birth_blocked), "nothing of the birth is framed Blocked once the hook passed: {rest:?}");
    assert!(rest.iter().any(|f| matches!(f, BuildFrame::Step { step, .. } if step == "WaitForNodeReady")), "WaitForNodeReady completed: {rest:?}");
    let a_reruns = knob_attempts(&knob, BLOCKED, A);

    estate.stop().await;
    let spans = estate.spans();
    // The flame: the blocked birth's probe answers, each naming its blocker.
    let probes: Vec<&Value> = spans_named(&spans, "rdm.node_admin.status.update.via-probe").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == BLOCKED && s(&sp["attributes"]["state"]) == "blocked").collect();
    assert!(probes.iter().any(|sp| s(&sp["attributes"]["blocker"]).contains(A)) && probes.iter().any(|sp| s(&sp["attributes"]["blocker"]).contains(B)), "the birth answered both blockers Blocked");
    assert!(probes.iter().all(|sp| s(&sp["attributes"]["retry_on"]) == "primary-addressed"), "every Blocked answer names the hook's retry event");
    estate.artifact("blocked-probes", &json!({ "a_reruns": a_reruns, "blocked_probe_answers": probes.len() }));
    estate.record_trace_url(probes[0]["trace_id"].as_str().unwrap_or(""));
}

const SHAPE1: [(&str, u32, u32); 1] = [("mesh1", 2, 1)];

/// What losing the fabric-primary in the middle of a Build that creates mesh2 leaves behind.
struct Lost {
    /// The Build that creates mesh2.
    build_id: BuildId,
    /// The attempt the lost fabric-primary's run executed.
    attempt: u32,
    /// The admin that holds the fabric-primary seat after it.
    restored: String,
    /// The carrier at the restored fabric-primary, after it streamed the Build to `Complete`.
    carrier: BuildCarrier,
}

/// The fabric reached its first ready (a claim is written to the store root at once, Q171).
/// A fabric-primary grows the fabric by mesh2 through `build.create` and is lost at `cut` (the
/// fabric-primary's own Door holds it there), after its claim of mesh2's name landed: mesh2's row is on
/// the store root. Fabric recovery restores the node it re-creates at the fabric-primary's path.name (Q122), and the caller re-submits the Build there by its id
/// and follows it to `Complete`. mesh2 is then the mesh the lost fabric-primary's claim wrote.
///
/// `survivor_holds` is a precondition of the cut: the operation whose `Complete` receipt, at the lost
/// attempt, the surviving mesh1 admin's own Build log must hold before the fabric-primary is lost. The
/// receipt reaches that admin by the Build topic's push; the loss comes within milliseconds of the
/// receipt, so a cell that reads the receipt at the restored fabric-primary awaits the push first. A cell that
/// cuts before the receipt exists passes `None`.
async fn lose_the_fabric_primary_creating_mesh2(estate: &mut Estate, caller: &Caller, carrier: &BuildCarrier, cut: Value, survivor_holds: Option<&str>) -> Lost {
    let fp = carrier.fabric_primary().to_string();
    let root = estate.fabric_store_root();
    // Ready: the Day-0 Build is Complete and a growth may be accepted (Q170).
    estate.until_fabric_ready(Duration::from_secs(90)).await;

    let door = Door::at(&estate, &fp).await;
    door.arm("cut", cut).await;
    let grown = FabricDesired {
        fabric: FABRIC.into(),
        meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 1)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)])],
    };
    let mut stream = carrier.create(estate.shape_change_at(&carrier, &grown).await).await.expect("the fabric-primary accepts mesh2");
    let (build_id, attempt) = (stream.build_id().clone(), stream.attempt());
    let held = door.wait_held("cut").await;
    assert_eq!(s(&held["hit"]["build_id"]), build_id.as_str(), "{held}");
    // The claim landed before the executor was lost: mesh2's row is on the store root.
    let name_claim: Value = serde_json::from_slice(&std::fs::read(root.join("locks/name/mesh2")).expect("the fabric-primary's claim of mesh2 wrote its name claim to the store root")).unwrap();
    let row: Value = rdm_admin::store_log::read_record(&root.join(rdm_admin::store_root::MESHES_FILE), "mesh-record", Some(&s(&name_claim["record"]["mesh_id"]))).expect("the fabric-primary's claim of mesh2 wrote its record to the store root");
    let claimed = s(&row["record"]["mesh_id"]);
    assert!(!claimed.is_empty() && row["record"]["name"] == "mesh2", "{row}");
    // The receipt reached the surviving admin before the fabric-primary is lost: its own Build log holds the
    // operation's `Complete` receipt of the lost attempt. The Build family is the fabric-primary's alone, so
    // the survivor's log is read over `FetchBuildFacts`, which every node-admin serves from its own.
    if let Some(operation) = survivor_holds {
        let survivor = caller.survivor_of(&fp);
        crate::build_log::log_fact(&caller.carrier_at(&survivor), &build_id.as_str(), &format!("{operation} Complete at attempt {attempt}"), Duration::from_secs(60), |facts| {
                facts.iter().any(|f| matches!(f, BuildFact::Step(r) if r.operation == operation && r.step == "Complete" && r.attempt == attempt && r.outcome == StepOutcome::Complete))
            })
            .await
            .unwrap_or_else(|end| panic!("{end}"));
    }
    // The fabric-primary dies at that cut.
    estate.kill_bootstrap();
    let end = loop {
        match stream.next().await {
            Some(Ok(_)) => continue,
            Some(Err(end)) => break end,
            None => panic!("the stream ended without saying how"),
        }
    };
    assert!(matches!(end, CallEnd::Indeterminate { .. }), "a dead fabric-primary ends the caller's stream indeterminate: {end:?}");

    // Fabric recovery restores the node it re-creates at the fabric-primary's path.name (Q122); the caller
    // re-submits there by the Build id.
    let restored = caller.restored_fabric_primary(&fp).await;
    let carrier = caller.carrier_at(&restored);
    let mut resumed = carrier.resubmit(&build_id, attempt).await.expect("the restored fabric-primary accepts the re-submit by build_id");
    assert_eq!(resumed.build_id(), &build_id, "never a second Build");
    until_complete(&mut resumed).await;
    // The estate is read and stopped through the admin that holds the seat now.
    // A surviving admin's view names the restored birth: the estate dials it from then on.
    let _ = estate.node_views_at(&path(&caller.survivor_of(&fp))).await;
    estate.enter(&path(&restored)).await;
    estate.settles_to(&[("mesh1", 2, 1), ("mesh2", 1, 1)], Duration::from_secs(90)).await;
    let mesh2 = rdm_test::typed::one_mesh(&Meshes::new(carrier.clone()), "mesh2").await.expect("mesh2's view at the restored fabric-primary").expect("the restored fabric-primary holds mesh2");
    assert_eq!(mesh2.id.map(|id| id.to_string()), Some(claimed.clone()), "mesh2 is the mesh the lost fabric-primary's claim wrote");
    Lost { build_id, attempt, restored, carrier }
}

/// `create-mesh:mesh2`'s receipts in `lost`'s Build, as the restored fabric-primary reads them: `(attempt, step, complete)`.
async fn create_mesh_receipts(lost: &Lost) -> Vec<(u32, String, bool)> {
    let receipts = lost.carrier.clone().build(&lost.build_id).await.expect("build.get [id] at the restored fabric-primary");
    receipts.steps.iter().filter(|r| r.operation == "create-mesh:mesh2").map(|r| (r.attempt, r.step.clone(), matches!(r.result, StepResult::Complete))).collect()
}

/// The trace of `create-mesh:mesh2` in `spans`: each claim of mesh2's name and each receipt step, by node
/// and outcome. The flame of the cell, never its assertion. The trace id of the restored fabric-primary's span.
fn create_mesh_flame(spans: &[Value], lost: &Lost) -> Option<String> {
    let claims = spans_named(spans, "rdm.node_admin.mesh.create.via-mesh-row").into_iter().filter(|c| c["attributes"]["mesh"] == "mesh2" && c["attributes"]["moment"] == "claim");
    let steps = spans_named(spans, "rdm.node_admin.mesh.create.via-receipt").into_iter().filter(|c| c["attributes"]["mesh"] == "mesh2");
    let mut restored = Vec::new();
    for c in claims.chain(steps) {
        let (name, node) = (c["name"].as_str().unwrap_or_default().to_string(), s(&c["attributes"]["node"]));
        eprintln!("trace {}: {name} mesh=mesh2 node={node} mesh_id={} attempt={} outcome={}", s(&c["trace_id"]), s(&c["attributes"]["mesh_id"]), s(&c["attributes"]["attempt"]), s(&c["attributes"]["outcome"]));
        if node == lost.restored {
            restored.push(s(&c["trace_id"]));
        }
    }
    restored.into_iter().next()
}

/// CONTRACT (node-rpc-envelope.md, Build op 0x20: "completed step receipts are read and replayed, never
/// executed again"; node-identity-and-state.md §2.7: the same MeshId re-claiming its name is idempotent): a
/// claim is written to the store root at once (Q171). The fabric-primary
/// accepts mesh2, minting mesh2's id at that acceptance, runs `create-mesh:mesh2` (the claim of mesh2's
/// name writes its row, and the step is receipted `Complete`) and is lost before the next step's receipt is
/// durable: the first admin's `AllocateIdentity`. The loss waits for the receipt to reach the surviving
/// mesh1 admin: its own Build log holds `create-mesh:mesh2` `Complete` at the lost attempt. The
/// restored fabric-primary resumes the same Build: it
/// reads the `create-mesh:mesh2` receipt and does not run the step again, and streams the Build to
/// `Complete`. mesh2 is the mesh the lost fabric-primary's claim wrote. The Build holds exactly one
/// `Complete` receipt of `create-mesh:mesh2`, written by the run that executed it. Must NOT happen: the
/// restored fabric-primary claiming mesh2's name again, or a second receipt of the step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_fabric_primary_reads_the_create_mesh_receipt_and_does_not_claim_the_mesh_again() {
    let test = "a_restored_fabric_primary_reads_the_create_mesh_receipt_and_does_not_claim_the_mesh_again";
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let mut estate = Estate::bootstrap_on(owner(test), FABRIC, "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE1).await;
    // The cut: mesh2's first admin has its identity allocated and that receipt is not durable; the
    // fabric-primary ran `create-mesh:mesh2` before it.
    let lost = lose_the_fabric_primary_creating_mesh2(&mut estate, &caller, &carrier, json!({"kind": "receipt", "step": "AllocateIdentity", "operation": "node.create", "node": "mesh2.admin.1"}), Some("create-mesh:mesh2")).await;
    let receipts = create_mesh_receipts(&lost).await;

    estate.stop().await;
    let spans = estate.spans();
    if let Some(trace) = create_mesh_flame(&spans, &lost) {
        estate.record_trace_url(&trace);
    }
    assert_eq!(receipts, vec![(lost.attempt, "Complete".to_string(), true)], "create-mesh:mesh2 is receipted once, by the run that executed it; the restored fabric-primary reads that receipt and runs nothing: {receipts:?}");
}

/// CONTRACT (node-rpc-envelope.md, Build carrier: "Settle an idempotent uncertain step by its natural key";
/// node-identity-and-state.md §2.7: "The same MeshId re-claiming its own name, on a retry or from a
/// replaced primary, is idempotent and accepted"): a claim is written to the store root at once. The
/// fabric-primary accepts mesh2, claims mesh2's name with the MeshId the Build carries (its row lands on
/// the store root) and is lost before it receipts `create-mesh:mesh2`. The restored fabric-primary resumes
/// the same Build: the step has no receipt, so it is uncertain and runs by its natural key; the claim
/// finds the mesh's own row (`row-held-by-mesh`) and the step is receipted `Complete` by the restored fabric-primary's
/// run. mesh2 is the mesh the lost fabric-primary's claim wrote. The Build holds exactly one `Complete`
/// receipt of `create-mesh:mesh2`, written after the cut. Must NOT happen: a refused claim
/// (`name-held`) by the mesh's own row, a second MeshId, or a step that ends with no receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_fabric_primary_settles_a_create_mesh_with_no_receipt_by_its_natural_key_and_receipts_it() {
    let test = "a_restored_fabric_primary_settles_a_create_mesh_with_no_receipt_by_its_natural_key_and_receipts_it";
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let mut estate = Estate::bootstrap_on(owner(test), FABRIC, "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE1).await;
    // The cut: the claim of mesh2's name landed and `create-mesh:mesh2`'s own receipt is not durable.
    let lost = lose_the_fabric_primary_creating_mesh2(&mut estate, &caller, &carrier, json!({"kind": "receipt", "step": "Complete", "operation": "create-mesh", "node": "mesh2"}), None).await;
    let receipts = create_mesh_receipts(&lost).await;

    estate.stop().await;
    let spans = estate.spans();
    if let Some(trace) = create_mesh_flame(&spans, &lost) {
        estate.record_trace_url(&trace);
    }
    assert!(matches!(receipts.as_slice(), [(at, step, true)] if *at >= lost.attempt && step == "Complete"), "create-mesh:mesh2 is receipted once, by the restored fabric-primary's run, after the cut: {receipts:?}");
}

/// CONTRACT (Q30): a launch whose executable hash outlasts the QUIC idle timeout keeps every open
/// connection of its admin alive. `rpc_node` is bound to an executable that `exec`s the rpc node and
/// is padded to 8 GiB (sparse: its zeros cost the hash CPU, not disk), so the launching admin's re-hash of it
/// (`rdm.node_admin.deployment.resolve.via-executable-hash`) runs longer than the staleness floor,
/// the QUIC idle timeout (node-rpc.md §21), on a bootstrap admin whose runtime has one worker. The
/// hash runs off the runtime's workers: no member goes
/// stale at that admin while it runs (no `via-refeed` "stale-held-member"), and the caller's Build
/// stream ends `Complete`. Must NOT happen: a stream `ReplyLost` "reset by peer", a member stale
/// during the hash. The before: trace 055c493a9ba3750bffc1854699ce7829 (R24's gate on `1959eb2b`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_launch_whose_executable_hash_outlasts_the_idle_timeout_keeps_every_connection_alive() {
    use std::os::unix::fs::PermissionsExt;
    let test = "a_launch_whose_executable_hash_outlasts_the_idle_timeout_keeps_every_connection_alive";
    let sha = candidate_sha();
    let mut set = binding_set(&sha);
    let real = set.bindings.iter().find(|b| b.launch_id == "rpc_node").expect("the rpc node is bound").executable.clone();
    let dir = std::env::temp_dir().join(format!("q30-bound-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir_all(&dir).unwrap();
    let wrapper = dir.join("rdm-rpc-node-padded");
    std::fs::write(&wrapper, format!("#!/bin/sh\nexec \"{}\" \"$@\"\n", real.display())).unwrap();
    std::fs::OpenOptions::new().write(true).open(&wrapper).unwrap().set_len(8 << 30).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let sha256 = rdm_core::binding::sha256_file(&wrapper).unwrap();
    for b in set.bindings.iter_mut().filter(|b| b.launch_id == "rpc_node") {
        b.executable = wrapper.clone();
        b.sha256 = sha256.clone();
    }
    // One worker: a hash held on it holds every task of the admin, its endpoint's included, so the
    // cell fails whenever the hash runs on a worker, never only when a woken task happens to sit in
    // the hashing worker's LIFO slot (with ~20 workers the defect needs that; R24's gate had it).
    let mut estate = Estate::bootstrap_on_with_env(owner(test), FABRIC, "mesh1", &set, &sha, &["rpc_node"], &[("TOKIO_WORKER_THREADS", "1")])
        .await
        .expect("the padded binding set is accepted");
    let caller = Caller::of(&estate).await;
    born(&estate, &caller, &[("mesh1", 1, 1)]).await;
    estate.stop().await;
    let _ = std::fs::remove_dir_all(&dir);

    let spans = estate.spans();
    let floor_ms = rdm_core::cadence::staleness_floor().as_millis() as u64;
    let hashes: Vec<&Value> = spans_named(&spans, "rdm.node_admin.deployment.resolve.via-executable-hash").into_iter().filter(|sp| s(&sp["attributes"]["launch_id"]) == "rpc_node").collect();
    assert!(!hashes.is_empty(), "the rpc node's launch re-hashed its executable");
    for h in &hashes {
        let elapsed: u64 = s(&h["attributes"]["elapsed_ms"]).parse().unwrap_or(0);
        assert!(elapsed > floor_ms, "the hash outlasts the idle timeout ({floor_ms} ms), or the cell proves nothing: {h}");
        let (from, to) = (h["start_unix_nano"].as_u64().unwrap_or(0), h["end_unix_nano"].as_u64().unwrap_or(0));
        let stale: Vec<&Value> = spans_named(&spans, "rdm.mesh.connection.update.via-refeed")
            .into_iter()
            .filter(|sp| s(&sp["attributes"]["reason"]) == "stale-held-member" && (from..=to).contains(&sp["start_unix_nano"].as_u64().unwrap_or(0)))
            .collect();
        assert!(stale.is_empty(), "no member went stale while the launch hashed its executable: {stale:#?}");
    }
    estate.record_trace_url(hashes[0]["trace_id"].as_str().unwrap_or(""));
    println!("executable hash flame: http://192.168.68.99:16687/trace/{}", s(&hashes[0]["trace_id"]));
}


/// i146 e2b: an uncertain closure is a new invocation on resumption; a closure with its Build
/// receipt is reused. Neither case depends on a receipt on the subject node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_connections_delete_resumes_from_the_builds_receipts_and_its_reissue_settles_it() {
    async fn at_cut(cut: &str) {
        let test = format!("an_interrupted_connections_delete_resumes_from_the_builds_receipts_and_its_reissue_settles_it_{cut}");
        let sha = candidate_sha();
        let mut estate = Estate::bootstrap_on(owner(&test), FABRIC, "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.unwrap();
        let caller = Caller::of(&estate).await;
        let carrier = born(&estate, &caller, &SHAPE).await;
        let node = secondary_node(&estate).await;
        let before = estate.node_value(&node.to_string()).await;
        let door = Door::at(&estate, "mesh2.admin.1").await;
        door.arm("closure", json!({"kind": "receipt", "step": cut, "operation": "node.connection.delete", "node": node.to_string()})).await;
        let mut stream = caller.nodes(&carrier).delete_connections(&node).await.expect("the Build accepts the closure");
        let accepted = stream.accepted().clone();
        let held = door.wait_held("closure").await;
        assert_eq!(s(&held["hit"]["build_id"]), accepted.build_id.to_string());
        // The surviving executor has heard all receipts that precede this cut.
        let operation = format!("node.connection.delete:{node}");
        if cut == "Complete" {
            crate::build_log::build_fact(&estate.carrier_at(&path("mesh2.admin.2")).await, accepted.build_id.as_str(), "the completed closure is recoverable on the successor before its executor is lost", Duration::from_secs(30), |b| {
                b.steps.iter().any(|r| r.operation == operation && r.step == "DeleteConnections" && serde_json::to_value(r).unwrap()["outcome"] == "complete")
            }).await.unwrap_or_else(|e| panic!("{e}"));
        }
        wait_for("the held executor exported the progress record before it is killed", Duration::from_secs(30), || async {
            named(&estate.spans(), "rdm.node_admin.node.update.via-connections-deleting").into_iter().find(|s| s["attributes"]["node"] == node.to_string()).cloned()
        }).await;
        estate.kill_node("mesh2.admin.1").await;
        let frames = frames_of(&mut stream).await;
        assert_eq!(frames.last(), Some(&Frame::Complete), "the successor settles the closure at {cut}: {frames:?}");
        let receipts = carrier.build(&accepted.build_id).await.expect("Build receipts");
        let closure: Vec<_> = receipts.steps.iter().filter(|r| r.operation == operation && r.step == "DeleteConnections" && matches!(r.result, StepResult::Complete)).collect();
        assert_eq!(closure.len(), 1, "one durable completed step across attempts: {closure:?}");
        let after = estate.node_value(&node.to_string()).await;
        assert_eq!((after["node_id"].clone(), after["incarnation_id"].clone()), (before["node_id"].clone(), before["incarnation_id"].clone()), "the same subject birth executes the reissue");
        // states.md "Who sets each state": the completion carrying `Established` sets connections-deleted,
        // and a read answers the row's status as written (Q233).
        assert_eq!(after["status"], "connections-deleted", "a completed closure leaves its row connections-deleted");
        estate.stop().await;
        let spans = estate.spans();
        let executions: Vec<_> = named(&spans, "rdm.node_admin.connection.delete.via-delete-connections").into_iter().filter(|s| s["attributes"]["node"] == node.to_string()).collect();
        assert_eq!(executions.len(), if cut == "DeleteConnections" { 2 } else { 1 }, "an uncertain closure runs again; a receipted closure never runs again: {executions:?}");
        let records: Vec<_> = named(&spans, "rdm.node_admin.node.update.via-connections-deleting").into_iter().filter(|s| s["attributes"]["node"] == node.to_string()).collect();
        assert_eq!(records.len(), executions.len());
        let ids: std::collections::BTreeSet<_> = records.iter().map(|s| s["attributes"]["invocation_id"].as_str().expect("every record names its invocation")).collect();
        assert_eq!(ids.len(), executions.len(), "every reissue is a fresh invocation");
        if cut == "Complete" {
            assert!(named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().any(|s| s["attributes"]["node"] == node.to_string() && s["attributes"]["step"] == "DeleteConnections" && s["attributes"]["outcome"] == "reused"), "the successor reads the completed step receipt");
        }
        let trace = executions.last().unwrap()["trace_id"].as_str().unwrap();
        estate.record_trace_url(trace);
        println!("node.connection.delete {cut} recovery: http://192.168.68.99:16687/trace/{trace}");
    }
    tokio::join!(at_cut("DeleteConnections"), at_cut("Complete"));
}

/// CONTRACT (docs/architecture/lifecycles/node-investigation.md: "A caller whose record is deleted is
/// deleted, its runtime terminated, never re-admitted"; Q217; node-recovery.md: "A birth that comes back
/// after it was re-created is a deleted birth: every view refuses its Digest by name"): a node-admin
/// (`mesh2.admin.1`) is os-stopped, its successor (`mesh2.admin.2`) takes `mesh2`'s primary role and, on the
/// inspect's `os-stopped` verdict (no tickle is sent, Q273), runs its recovery's `node.delete` to
/// `NodeDeleted` (its row written `Deleted`, before `node.create` writes a re-created birth). Resumed there,
/// with the recovery then let on, `mesh2.admin.1` is refused by name at a door of `mesh2.admin.2` (its
/// Digest, once the view holds it deleted; a declaration it still owed, refused `SubjectUnknown`; or a
/// join, refused `JoinMismatch` on `status`) and
/// decided as `delete-deleted-birth`, never `re-admit`, reported to the fabric-primary
/// (`via-returned-reported` outcome `opened`), and its runtime is terminated while the re-created birth
/// holds `mesh2.admin.1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_os_stopped_node_admin_recovered_and_resumed_is_refused_by_name_and_deleted() {
    let test = "an_os_stopped_node_admin_recovered_and_resumed_is_refused_by_name_and_deleted";
    let Suspended { mut estate, mut stream, door, rt, node_id, .. } = suspended_executor(test).await;
    let recovering = Door::at(&estate, "mesh2.admin.2").await;
    recovering.arm("recovery", json!({"kind": "receipt", "step": "NodeDeleted", "operation": "node.delete", "node": SUSPENDED})).await;

    let _cut = recovering.wait_held("recovery").await;
    let resumed = rt.apply(rdm_test::process_faults::Fault::Continue).expect("the suspended runtime is resumed");
    assert_ne!(resumed.state_after, Some('T'), "{resumed:?}");
    door.release("held").await;

    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    recovering.release("recovery").await;

    started_to_the_end(&mut stream, "the next attempt completes the held start").await;
    let recovered = re_created(&estate, SUSPENDED, &node_id).await;
    wait_for("the resumed birth's runtime ended", crate::node_recovery_due() * 2, || async { rt.check().is_err().then_some(()) }).await;
    let held = estate.node_value(SUSPENDED).await;
    assert_eq!((s(&held["node_id"]), s(&held["status"])), (s(&recovered["node_id"]), "ready".to_string()), "the re-created birth holds its path.name: {held}");
    // The decision ends once its terminate report is answered: owed while an attempt holds the mesh
    // (the recovery that re-created the birth), so it is waited for, never cut by the estate's stop.
    wait_for("the refused caller's decision is reported to the fabric-primary", crate::node_recovery_due() * 2, || async {
        spans_named(&estate.spans(), "rdm.node_admin.node.update.via-refused-caller").into_iter().find(|d| attr(d, "node_id") == node_id && attr(d, "decision") == "delete-deleted-birth" && attr(d, "outcome") == "terminate-reported").map(|_| ())
    })
    .await;

    estate.stop().await;
    let spans = estate.spans();
    let decisions: Vec<Value> = spans_named(&spans, "rdm.node_admin.node.update.via-refused-caller")
        .into_iter()
        .filter(|sp| attr(sp, "node_id") == node_id)
        .cloned()
        .collect();
    assert!(!decisions.is_empty(), "the refused caller is decided: {decisions:#?}");
    assert!(decisions.iter().all(|d| attr(d, "decision") != "re-admit"), "a deleted birth is never re-admitted: {decisions:#?}");
    let refused_decided = decisions
        .iter()
        .find(|d| ["digest", "declaration", "join"].contains(&attr(d, "door").as_str()) && attr(d, "decision") == "delete-deleted-birth" && attr(d, "outcome") == "terminate-reported")
        .unwrap_or_else(|| panic!("expected a digest, declaration or join door decision delete-deleted-birth with terminate-reported: {decisions:#?}"));
    assert_eq!(attr(refused_decided, "own"), "delete-deleted-birth", "{refused_decided}");

    let reported = spans_named(&spans, "rdm.node_admin.node.update.via-returned-reported")
        .into_iter()
        .find(|sp| attr(sp, "node_id") == node_id && attr(sp, "outcome") == "opened")
        .cloned()
        .unwrap_or_else(|| panic!("expected via-returned-reported outcome opened for {node_id}"));
    assert_eq!(attr(&reported, "returned"), SUSPENDED, "{reported}");
    let terminated = spans_named(&spans, "rdm.node_admin.build.update.via-terminate-returned")
        .into_iter()
        .find(|sp| attr(sp, "node_id") == node_id)
        .cloned()
        .expect("the fabric-primary opens the resumed birth's termination");
    estate.record_trace_url(&s(&terminated["trace_id"]));
}

/// CONTRACT (docs/architecture/build.md: "A stop, a start or a recovery opens the next attempt of the
/// current Build with its action"; crates/rdm-core/src/build.rs `BuildRequest::AttemptRun`: `intent`
/// carries the Build's acceptance, every attempt opened, claimed and ended, and the step receipts of
/// the operations this run carries, never the Build's other receipts): a Build that lives through 100
/// `node.stop` / `node.start` pairs on `mesh2` (200 attempts dispatched across `build.attempt.run` to
/// `mesh2.admin.1`) dispatches and completes every attempt within the Build op's 256 KiB request
/// ceiling, with zero `Malformed(TooLarge)` refusals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_that_lives_through_one_hundred_stop_and_start_pairs_dispatches_every_attempt_run() {
    let test = "a_build_that_lives_through_one_hundred_stop_and_start_pairs_dispatches_every_attempt_run";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = secondary_node(&estate).await;
    let nodes = caller.nodes(&carrier);

    let mut build_id = None;
    let mut last_attempt = 0;
    for pair in 1..=100 {
        let mut stop_stream = nodes.stop(&node).await.unwrap_or_else(|e| panic!("pair {pair} stop accepted: {e:?}"));
        let stop_acc = stop_stream.accepted().clone();
        if let Some(ref id) = build_id {
            assert_eq!(&stop_acc.build_id, id, "pair {pair} stop stays on the one Build");
        } else {
            build_id = Some(stop_acc.build_id.clone());
        }
        assert!(stop_acc.attempt > last_attempt, "pair {pair} stop advances the attempt ({} > {last_attempt})", stop_acc.attempt);
        last_attempt = stop_acc.attempt;
        let stop_frames = frames_of(&mut stop_stream).await;
        assert_eq!(stop_frames.last(), Some(&Frame::Complete), "pair {pair} stop completes: {stop_frames:?}");

        let mut start_stream = nodes.start(&node).await.unwrap_or_else(|e| panic!("pair {pair} start accepted: {e:?}"));
        let start_acc = start_stream.accepted().clone();
        assert_eq!(Some(&start_acc.build_id), build_id.as_ref(), "pair {pair} start stays on the one Build");
        assert!(start_acc.attempt > last_attempt, "pair {pair} start advances the attempt ({} > {last_attempt})", start_acc.attempt);
        last_attempt = start_acc.attempt;
        let start_frames = frames_of(&mut start_stream).await;
        assert_eq!(start_frames.last(), Some(&Frame::Complete), "pair {pair} start completes: {start_frames:?}");
    }

    estate.stop().await;
    let spans = estate.spans();
    let build = build_id.expect("100 pairs ran").to_string();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let dispatches: Vec<&Value> = spans_named(&spans, "rdm.node_admin.build.update.via-dispatch")
        .into_iter()
        .filter(|sp| attr(sp, "build_id") == build && attr(sp, "executor") == "mesh2.admin.1" && attr(sp, "outcome") == "complete")
        .collect();
    assert!(dispatches.len() >= 200, "every stop and start attempt on mesh2 dispatched and completed at mesh2.admin.1: {}", dispatches.len());
    let malformed: Vec<&Value> = spans
        .iter()
        .filter(|sp| s(&sp["attributes"]["outcome"]).contains("malformed") || s(&sp["attributes"]["reason"]).contains("too-large") || s(&sp["attributes"]["detail"]).contains("TooLarge"))
        .collect();
    assert!(malformed.is_empty(), "no AttemptRun was refused Malformed(TooLarge): {malformed:#?}");
    estate.record_trace_url(&attr(dispatches.last().unwrap(), "trace_id"));
}
