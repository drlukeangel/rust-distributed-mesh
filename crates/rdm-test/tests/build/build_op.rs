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
use rdm_client::{BuildCarrier, BuildFrame, BuildId, BuildSpec, BuildStream, Builds, CallEnd, FabricDesired, Fabrics, Frame, MeshDesired, MeshSelector, Meshes, NodeEvent, Nodes, Peers, ScopeStatus, StartDisposition, WorkflowKind, WorkflowStream};
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
/// The events a restart's stream carries, in order.
const RESTART_EVENTS: [NodeEvent; 6] = [
    NodeEvent::Restarting,
    NodeEvent::ConnectionsDeleted,
    NodeEvent::Stopped,
    NodeEvent::Joined,
    NodeEvent::Started,
    NodeEvent::Restarted,
];

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
}

impl Caller {
    /// A caller that learns the fabric from the estate's entry node-admin, over Node RPC.
    pub(crate) async fn of(estate: &Estate) -> Caller {
        let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller binds an endpoint");
        let resolver = Arc::new(StaticResolver::new());
        let client = Arc::new(NodeRpcClient::new(ep, resolver.clone()).with_caller_system("rdm"));
        let caller = Caller { peers: estate.peers().await, entry: estate.entry().await, fabric_id: estate.fabric_id.clone(), client, resolver, bases: Default::default() };
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
    let mut stream = Builds::new(carrier.clone()).create(&BuildSpec::Fabric(desired)).await.expect("the fabric-primary accepts the shape");
    let frames = until_complete(&mut stream).await;
    estate.settles_to(shape, Duration::from_secs(90)).await;
    caller.learn().await;
    // A workflow a caller submits is refused until the initial fabric has first reached ready-for-traffic
    // (states.md "The initial fabric before ready-for-traffic").
    Fabrics::new(carrier.clone())
        .fact("ready-for-traffic: the initial fabric has climbed", Duration::from_secs(90), |f| f.status == ScopeStatus::ReadyForTraffic)
        .await
        .unwrap_or_else(|end| panic!("{end}"));
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
/// seat move, so its restart is one attempt (`accepted.attempt`).
async fn seatless_node(estate: &Estate) -> PathName {
    estate.node_views().await.into_iter().filter(|n| n.name.to_string().starts_with("mesh2.rpc.") && !n.is_primary).map(|n| n.name).min().expect("an rpc node of mesh2 holding no seat")
}

/// CONTRACT (acceptance 1): `node.restart` over `0x20` is one call. The stream is `Started`, a frame
/// per step as its `Complete` receipt becomes durable on the admin that wrote it, and `Complete`,
/// pushed by the executor through the fabric-primary: the node is executed by `mesh2`'s own primary,
/// so the Build crosses a real `build.attempt.run`. Every event the restart names arrives once, in
/// order, and nothing polls a Build to learn it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_over_the_build_family_is_one_call_that_pushes_every_step_frame_and_completes() {
    let test = "a_restart_over_the_build_family_is_one_call_that_pushes_every_step_frame_and_completes";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = seatless_node(&estate).await;

    let mut stream = caller.nodes(&carrier).restart(&node).await.expect("the fabric-primary accepts the restart");
    let accepted = stream.accepted().clone();
    let frames = frames_of(&mut stream).await;
    assert_eq!(frames.first(), Some(&Frame::Started { build_id: accepted.build_id.clone(), attempt: accepted.attempt }));
    assert_eq!(frames.last(), Some(&Frame::Complete), "the stream ends with its terminal frame: {frames:?}");
    assert_eq!(events(&frames), RESTART_EVENTS, "every step event, once, in order: {frames:?}");
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
    assert!(spans_named(&spans, "rdm.node_admin.build.get.via-call").is_empty(), "nothing read a Build to learn a step");
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
    let node = seatless_node(&estate).await;
    let nodes = caller.nodes(&carrier);

    let mut cut = nodes.restart(&node).await.expect("the fabric-primary accepts the restart");
    let accepted = cut.accepted().clone();
    // The first frame is the call's `Started`; the cut follows it, before any step is reported.
    assert!(matches!(cut.next().await, Some(Ok(Frame::Started { .. }))));
    drop(cut);

    let mut attached = nodes.resume(WorkflowKind::Restart(node.clone()), &accepted).await.expect("the re-submit is accepted");
    assert_eq!(attached.disposition(), StartDisposition::Attached, "the Build the cut left running is the one the re-submit attaches to");
    assert_eq!(attached.accepted().build_id, accepted.build_id, "never a second Build");
    let first = frames_of(&mut attached).await;
    assert_eq!(first.last(), Some(&Frame::Complete));
    assert_eq!(events(&first), RESTART_EVENTS, "{first:?}");

    let mut again = nodes.resume(WorkflowKind::Restart(node), &accepted).await.expect("the second re-submit is accepted");
    assert_eq!(again.disposition(), StartDisposition::AlreadyApplied);
    let second = frames_of(&mut again).await;
    assert_eq!(second.last(), Some(&Frame::Complete), "the terminal frame follows AlreadyApplied");
    assert_eq!(events(&second), RESTART_EVENTS, "the receipts held are replayed as frames: {second:?}");

    let receipts = Builds::new(carrier.clone()).get(&accepted.build_id).await.expect("build.get");
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
/// reattachment": "`build.get` for the receipts, then the outer stream again, resuming from the
/// receipts"): a watched Build whose connection closes mid-stream is reattached by its build id, each
/// reattachment a `build.get` and then the outer stream again, and ends `Complete`, the run it watched
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
    let mut stream = Builds::new(carrier.clone()).create(&BuildSpec::Fabric(grown)).await.expect("the fabric-primary accepts the change");
    let (build_id, attempt) = (stream.build_id().clone(), stream.attempt());
    let first = stream.next().await.expect("a frame").expect("the stream reads its first frame");
    assert!(!matches!(first, BuildFrame::Complete { .. } | BuildFrame::Failed { .. }), "the Build is still running when its connection closes: {first:?}");

    let closed = carrier.client().close_pooled("the cell closes the watcher's connection");
    assert!(closed >= 1, "the watching client held the stream's connection");
    let followed = carrier.follow(stream).await.expect("the watcher follows the Build to its terminal");
    assert!(followed.reattached >= 1, "the watcher reattached after the close: {followed:?}");
    assert!(matches!(followed.frames.last(), Some(BuildFrame::Complete { .. })), "the Build ends Complete: {followed:?}");

    let receipts = Builds::new(carrier.clone()).get(&build_id).await.expect("build.get");
    assert_eq!(receipts.attempt, attempt, "the close opened no second attempt: {receipts:?}");
    let mut keys: Vec<(u32, String, String)> = receipts.steps.iter().map(|r| (r.attempt, r.operation.clone(), r.step.clone())).collect();
    let all = keys.len();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), all, "no step has two receipts: {receipts:?}");

    estate.stop().await;
    let spans = estate.spans();
    let build = build_id.to_string();
    // Each reattachment read the Build's receipts before its stream again; the cell's own read is one more.
    let gets = spans_named(&spans, "rdm.node_admin.build.serve.via-node-rpc").into_iter().filter(|sp| s(&sp["attributes"]["call"]) == "get").count();
    assert!(gets as u32 > followed.reattached, "each reattachment read build.get ({} reattachment(s), {gets} build.get read(s) with the cell's own)", followed.reattached);
    let reconcile = spans_named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build)
        .expect("the run");
    println!("watched Build reattached {} time(s), Complete; flame http://192.168.68.99:16687/trace/{}", followed.reattached, s(&reconcile["trace_id"]));
    estate.record_trace_url(reconcile["trace_id"].as_str().unwrap_or(""));
}

async fn attempt_run(caller: &Caller, to: &str, build: &BuildId, attempt: u32, executor: &str) -> BuildReply {
    let req = BuildRequest::AttemptRun { build_id: build.to_string(), attempt, executor: executor.into(), context: CallContext::default(), intent: Vec::new(), operations: Vec::new(), hand_off_to: None };
    let opts = CallOptions { budget: Budget::Stream { send: Duration::from_secs(10) }, ..CallOptions::default() };
    match caller.client.call_stream::<Build>(&NodeTarget::CurrentPath(path(to)), &req, &opts).await.map(|(s, e)| (s.read_only(), e)) {
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
    let node = seatless_node(&estate).await;
    let mut stream = caller.nodes(&carrier).restart(&node).await.expect("the fabric-primary accepts the restart");
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
/// The executor is lost at the same step every run: its own Door holds it in the restart's start, the
/// node's `start-node` answered `Started` and its `StartNode` receipt not yet durable, and it is killed
/// while held. The successor replays the receipted stop and reissues the start, which the node settles
/// `AlreadyApplied`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_executor_ends_the_inner_call_indeterminate_and_the_successor_is_claimed_only_after_proof() {
    let test = "a_lost_executor_ends_the_inner_call_indeterminate_and_the_successor_is_claimed_only_after_proof";
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let mut estate = Estate::bootstrap_external(owner(test), FABRIC, "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = seatless_node(&estate).await;

    // The executor, mesh2's primary, is held in the middle of the restart: the start of the stopped node
    // answered, its receipt not durable.
    const EXECUTOR: &str = "mesh2.admin.1";
    let door = Door::at(&estate, EXECUTOR).await;
    door.arm("lost", json!({"kind": "receipt", "step": "StartNode", "operation": "node.restart", "node": node.to_string()})).await;
    let mut stream = caller.nodes(&carrier).restart(&node).await.expect("the fabric-primary accepts the restart");
    let accepted = stream.accepted().clone();
    let held = door.wait_held("lost").await;
    assert_eq!(s(&held["hit"]["build_id"]), accepted.build_id.as_str(), "{held}");
    assert_eq!(held["hit"]["attempt"], json!(accepted.attempt), "the executor is held in the attempt the restart was accepted at: {held}");
    estate.kill_node(EXECUTOR).await;
    let frames = frames_of(&mut stream).await;
    assert_eq!(frames.last(), Some(&Frame::Complete), "the caller's stream carries the successor's frames to the end: {frames:?}");
    let seen = events(&frames);
    let mut once = seen.clone();
    once.dedup();
    assert_eq!(seen, once, "no event is delivered twice: {frames:?}");
    assert_eq!(seen.last(), Some(&NodeEvent::Restarted), "{seen:?}");

    estate.stop().await;
    let spans = estate.spans();
    let build = &accepted.build_id.to_string();
    let proofs: Vec<_> = spans_named(&spans, "rdm.node_admin.build.update.via-departure-proven").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["attempt"]) == accepted.attempt.to_string()).collect();
    assert_eq!(proofs.len(), 1, "the departure was proven once, before the next attempt was claimed: {proofs:?}");
    assert_eq!(s(&proofs[0]["attributes"]["executor"]), "mesh2.admin.1");
    let claims: Vec<_> = spans_named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build && sp["attributes"]["attempt"].as_str().and_then(|a| a.parse::<u32>().ok()) > Some(accepted.attempt)).collect();
    let successor = claims.iter().find(|sp| s(&sp["attributes"]["executor"]) == "mesh2.admin.2").expect("the next attempt was claimed for the successor");
    assert!(successor["start_unix_nano"].as_u64() > proofs[0]["start_unix_nano"].as_u64(), "the claim for the successor came after the proof");
    let lost = spans_named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["outcome"]) == "executor-lost").expect("the inner call to the lost executor ended");
    assert!(!s(&lost["attributes"]["reason"]).is_empty(), "the executor-lost dispatch names why the inner call ended: {lost}");
    estate.record_trace_url(lost["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (acceptance 2): the fabric-primary dies after claiming an attempt and before dispatching
/// it. The successor fabric-primary resumes the Build from its projection: it calls the executor the
/// claim names with `build.attempt.run`, which starts the claimed attempt, and the caller's re-submit
/// by the Build id streams it to `Complete`. No step has a second receipt, and no attempt beyond the
/// claimed one is opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_fabric_primary_starts_the_attempt_its_predecessor_claimed_and_no_step_runs_twice() {
    let test = "a_successor_fabric_primary_starts_the_attempt_its_predecessor_claimed_and_no_step_runs_twice";
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let mut estate = Estate::bootstrap_external(owner(test), FABRIC, "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = seatless_node(&estate).await;
    let fp = carrier.fabric_primary().to_string();

    // The cut: the fabric-primary's claim is durable and broadcast, and the drive has not dispatched.
    let door = Door::at(&estate, &fp).await;
    door.arm("claimed", json!({"kind": "claimed-attempt", "attempt": null})).await;
    let mut stream = caller.nodes(&carrier).restart(&node).await.expect("the fabric-primary accepts the restart");
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

    // A seat moves to a surviving admin; the caller re-submits there by the Build id.
    let successor = caller.successor_of(&fp).await;
    let carrier = caller.carrier_at(&successor);
    let mut resumed = caller.nodes(&carrier).resume(WorkflowKind::Restart(node), &accepted).await.expect("the successor accepts the re-submit");
    assert_eq!(resumed.accepted().build_id, accepted.build_id, "never a second Build");
    let frames = frames_of(&mut resumed).await;
    assert_eq!(frames.last(), Some(&Frame::Complete), "{frames:?}");
    assert_eq!(events(&frames), RESTART_EVENTS, "{frames:?}");

    let receipts = Builds::new(carrier.clone()).get(&accepted.build_id).await.expect("build.get");
    let mut keys: Vec<(u32, String, String)> = receipts.steps.iter().filter(|r| r.attempt >= accepted.attempt).map(|r| (r.attempt, r.operation.clone(), r.step.clone())).collect();
    let before = keys.len();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), before, "no step has two receipts");
    assert_eq!(receipts.attempt, accepted.attempt, "the successor started the attempt that was claimed and opened no other");

    // The estate stops through the admin that holds the seat now.
    estate.enter(&path(&successor)).await;
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
/// Build retires `mesh1`: before the retire is claimed, the fabric-primary moves the fabric seat alone
/// to `mesh2`'s primary by node.fabric-primary.update (op `0x21`, mesh-delete.md step 1), and the Build
/// goes on at the new seat. The successor installs the committed row (`seat.update.via-primary-updated`
/// at the new seat, not a replay) before the old fabric-primary's drive refuses with `Fenced` in its
/// span and names the new seat, so the caller's re-submit by the Build id at the seat the frame names
/// is accepted and streams to `Complete`.
/// Must NOT happen: the new seat named to the caller before it installed (its re-submit refused
/// `not-fabric-primary`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_seat_handover_mid_build_ends_the_outer_stream_with_the_new_seat_and_the_old_seat_dispatches_nothing() {
    let test = "a_planned_seat_handover_mid_build_ends_the_outer_stream_with_the_new_seat_and_the_old_seat_dispatches_nothing";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let old_seat = carrier.fabric_primary().to_string();
    assert!(old_seat.starts_with("mesh1."), "mesh1 holds the fabric-primary seat: {old_seat}");

    // The Build retires mesh1; the seat leaves with it, by a planned handover inside the Build.
    let keep = FabricDesired { fabric: FABRIC.into(), meshes: vec![MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)])] };
    let mut stream = Builds::new(carrier.clone()).create(&BuildSpec::Fabric(keep)).await.expect("the fabric-primary accepts the retire");
    let (build_id, attempt) = (stream.build_id().clone(), stream.attempt());
    let new_seat = loop {
        match stream.next().await {
            Some(Ok(BuildFrame::SeatMoved { fabric_primary })) => break fabric_primary.expect("the old seat names the new one"),
            Some(Ok(BuildFrame::Complete { .. })) => panic!("the old fabric-primary cannot finish a retire of its own mesh"),
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
    let fenced: Vec<_> = spans_named(&spans, "rdm.node_admin.build.reject.via-fenced").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build).collect();
    assert!(!fenced.is_empty(), "the old fabric-primary's dispatch after the fence was refused, Fenced in its span");
    assert!(fenced.iter().all(|sp| s(&sp["attributes"]["node"]) == old_seat), "{fenced:?}");
    let installed: Vec<_> = spans_named(&spans, "rdm.node_admin.seat.update.via-primary-updated").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["node"]) == new_seat && s(&sp["attributes"]["replay"]) == "false").collect();
    assert_eq!(installed.len(), 1, "the new seat installed the committed row once, not a replay: {installed:?}");
    let installed_at = installed[0]["start_unix_nano"].as_u64().unwrap_or(u64::MAX);
    let early: Vec<_> = fenced.iter().filter(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0) <= installed_at).collect();
    assert!(early.is_empty(), "C1 step 8: the old seat's drive names the new seat only once the successor installed ({installed_at}): {early:?}");
    estate.record_trace_url(fenced[0]["trace_id"].as_str().unwrap_or(""));
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

    /// The node-admin that holds the fabric-primary seat once `dead` has died: the first surviving
    /// admin's own view of the fabric is awaited until it names a fabric-primary other than `dead`.
    async fn successor_of(&self, dead: &str) -> String {
        let asked = self.survivor_of(dead);
        let view = Fabrics::new(self.carrier_at(&asked))
            .fact(&format!("a fabric-primary other than {dead}"), Duration::from_secs(60), |f| f.fabric_primary.as_ref().is_some_and(|p| p.to_string() != dead))
            .await
            .unwrap_or_else(|end| panic!("{end}"));
        view.fabric_primary.expect("the awaited view names a fabric-primary").to_string()
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

/// How many attempts of `node`'s knob hook (`rdm_test::hydrate_probe::KnobHydrator`)
/// ended blocked for `reason`.
fn knob_attempts(knob: &std::path::Path, node: &str, reason: &str) -> usize {
    std::fs::read_to_string(knob.join(format!("{node}.attempts"))).unwrap_or_default().lines().filter(|l| *l == format!("blocked {reason}")).count()
}

/// CONTRACT (node-start.md step 3; node-rpc-envelope.md frame table, Blocked "emitted on a new
/// blocker or reason change, never on a timer"): a birth whose `hydrate_before_ready` is Blocked
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
    const B: &str = "the authority of orders is draining";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let (carrier, shape_frames) = born_framed(&estate, &caller, &SHAPE1).await;
    assert!(!shape_frames.iter().any(|f| matches!(f, BuildFrame::Blocked { .. })), "births with no hook are plain Pending until Ready and frame no Blocked: {shape_frames:?}");

    // The next birth's hook is blocked for A from its first attempt.
    let knob = rdm_test::hydrate_knob_dir(&estate.root);
    rdm_test::write_hydrate_knob(&estate.root, BLOCKED, A).unwrap();
    let grown = FabricDesired { fabric: FABRIC.into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)])] };
    let mut stream = Builds::new(carrier.clone()).create(&BuildSpec::Fabric(grown)).await.expect("the fabric-primary accepts the grown shape");
    let (before_a, (step, reason)) = next_blocked(&mut stream).await;
    assert_eq!(step, "WaitForNodeReady", "the blocked step: {reason}");
    assert!(reason.contains(BLOCKED) && reason.contains(A) && reason.contains("authority-addressed"), "the frame names the birth, the hook's reason and its retry event: {reason}");
    assert!(!before_a.iter().any(|f| matches!(f, BuildFrame::Blocked { .. })));

    // Reruns blocked for the same reason: the executor's probes address the birth, so its hook runs
    // again on each; wait until it has been blocked for A three more times.
    wait_for("three more attempts blocked for A", Duration::from_secs(60), || async { (knob_attempts(&knob, BLOCKED, A) >= 4).then_some(()) }).await;
    rdm_test::write_hydrate_knob(&estate.root, BLOCKED, B).unwrap();
    let (before_b, (step_b, reason_b)) = next_blocked(&mut stream).await;
    assert_eq!(step_b, "WaitForNodeReady");
    assert!(reason_b.contains(B) && !reason_b.contains(A), "the next Blocked frame is the changed reason, never A again: {reason_b}");
    assert!(!before_b.iter().any(|f| matches!(f, BuildFrame::Step { step, .. } if step == "WaitForNodeReady")), "the step is still in flight: {before_b:?}");

    wait_for("an attempt blocked for B", Duration::from_secs(60), || async { (knob_attempts(&knob, BLOCKED, B) >= 2).then_some(()) }).await;
    rdm_test::write_hydrate_knob(&estate.root, BLOCKED, "pass").unwrap();
    let rest = until_complete(&mut stream).await;
    assert!(!rest.iter().any(|f| matches!(f, BuildFrame::Blocked { .. })), "nothing is framed Blocked once the hook passed: {rest:?}");
    assert!(rest.iter().any(|f| matches!(f, BuildFrame::Step { step, .. } if step == "WaitForNodeReady")), "WaitForNodeReady completed: {rest:?}");
    let a_reruns = knob_attempts(&knob, BLOCKED, A);

    estate.stop().await;
    let spans = estate.spans();
    // The flame: the blocked birth's probe answers, each naming its blocker.
    let probes: Vec<&Value> = spans_named(&spans, "rdm.node_admin.status.update.via-probe").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == BLOCKED && s(&sp["attributes"]["state"]) == "blocked").collect();
    assert!(probes.iter().any(|sp| s(&sp["attributes"]["blocker"]).contains(A)) && probes.iter().any(|sp| s(&sp["attributes"]["blocker"]).contains(B)), "the birth answered both blockers Blocked");
    assert!(probes.iter().all(|sp| s(&sp["attributes"]["retry_on"]) == "authority-addressed"), "every Blocked answer names the hook's retry event");
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
    successor: String,
    /// The carrier at that successor, after it streamed the Build to `Complete`.
    carrier: BuildCarrier,
}

/// The fabric is past its first state-commit, which published the rows its store held in the scratchpad:
/// a claim from here on is written to the store root, never held in a scratchpad that dies with its
/// writer. A fabric-primary grows the fabric by mesh2 through `build.create` and is lost at `cut` (the
/// fabric-primary's own Door parks it there), after its claim of mesh2's name landed: mesh2's row is on
/// the store root. A seat moves to a surviving admin, and the caller re-submits the Build there by its id
/// and follows it to `Complete`. mesh2 is then the mesh the lost fabric-primary's claim wrote.
///
/// `survivor_holds` is a precondition of the cut: the operation whose `Complete` receipt, at the lost
/// attempt, the surviving mesh1 admin's own Build log must hold before the fabric-primary is lost. The
/// receipt reaches that admin by the Build topic's push; the loss comes within milliseconds of the
/// receipt, so a cell that reads the receipt at its successor awaits the push first. A cell that
/// cuts before the receipt exists passes `None`.
async fn lose_the_fabric_primary_creating_mesh2(estate: &mut Estate, caller: &Caller, carrier: &BuildCarrier, cut: Value, survivor_holds: Option<&str>) -> Lost {
    let fp = carrier.fabric_primary().to_string();
    let root = estate.fabric_store_root();
    Fabrics::new(carrier.clone())
        .fact("ready-for-traffic: its first state-commit has published the rows held in the scratchpad", Duration::from_secs(90), |f| f.status == ScopeStatus::ReadyForTraffic)
        .await
        .unwrap_or_else(|end| panic!("{end}"));

    let door = Door::at(&estate, &fp).await;
    door.arm("cut", cut).await;
    let grown = FabricDesired {
        fabric: FABRIC.into(),
        meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 1)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)])],
    };
    let mut stream = Builds::new(carrier.clone()).create(&BuildSpec::Fabric(grown)).await.expect("the fabric-primary accepts mesh2");
    let (build_id, attempt) = (stream.build_id().clone(), stream.attempt());
    let held = door.wait_held("cut").await;
    assert_eq!(s(&held["hit"]["build_id"]), build_id.as_str(), "{held}");
    // The claim landed before the executor was lost: mesh2's row is on the store root.
    let row: Value = serde_json::from_slice(&std::fs::read(root.join("meshes/mesh2.json")).expect("the fabric-primary's claim of mesh2 wrote its row to the store root")).unwrap();
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

    // A seat moves to a surviving admin; the caller re-submits there by the Build id.
    let successor = caller.successor_of(&fp).await;
    let carrier = caller.carrier_at(&successor);
    let mut resumed = carrier.resubmit(&build_id, attempt).await.expect("the successor accepts the re-submit by build_id");
    assert_eq!(resumed.build_id(), &build_id, "never a second Build");
    until_complete(&mut resumed).await;
    // The estate is read and stopped through the admin that holds the seat now.
    estate.enter(&path(&successor)).await;
    estate.settles_to(&[("mesh1", 2, 1), ("mesh2", 1, 1)], Duration::from_secs(90)).await;
    let mesh2 = Meshes::new(carrier.clone()).get(&MeshSelector::Mesh("mesh2".into())).await.expect("mesh2's view at the successor").pop().expect("the successor's view holds mesh2");
    assert_eq!(mesh2.id.map(|id| id.to_string()), Some(claimed.clone()), "mesh2 is the mesh the lost fabric-primary's claim wrote");
    Lost { build_id, attempt, successor, carrier }
}

/// `create-mesh:mesh2`'s receipts in `lost`'s Build, as the successor reads them: `(attempt, step, complete)`.
async fn create_mesh_receipts(lost: &Lost) -> Vec<(u32, String, bool)> {
    let receipts = Builds::new(lost.carrier.clone()).get(&lost.build_id).await.expect("build.get at the successor");
    receipts.steps.iter().filter(|r| r.operation == "create-mesh:mesh2").map(|r| (r.attempt, r.step.clone(), matches!(r.result, StepResult::Complete))).collect()
}

/// The trace of `create-mesh:mesh2` in `spans`: each claim of mesh2's name and each receipt step, by node
/// and outcome. The flame of the cell, never its assertion. The trace id of the successor's span.
fn create_mesh_flame(spans: &[Value], lost: &Lost) -> Option<String> {
    let claims = spans_named(spans, "rdm.node_admin.mesh.create.via-mesh-row").into_iter().filter(|c| c["attributes"]["mesh"] == "mesh2" && c["attributes"]["moment"] == "claim");
    let steps = spans_named(spans, "rdm.node_admin.mesh.create.via-receipt").into_iter().filter(|c| c["attributes"]["mesh"] == "mesh2");
    let mut successors = Vec::new();
    for c in claims.chain(steps) {
        let (name, node) = (c["name"].as_str().unwrap_or_default().to_string(), s(&c["attributes"]["node"]));
        eprintln!("trace {}: {name} mesh=mesh2 node={node} mesh_id={} attempt={} outcome={}", s(&c["trace_id"]), s(&c["attributes"]["mesh_id"]), s(&c["attributes"]["attempt"]), s(&c["attributes"]["outcome"]));
        if node == lost.successor {
            successors.push(s(&c["trace_id"]));
        }
    }
    successors.into_iter().next()
}

/// CONTRACT (node-rpc-envelope.md, Build op 0x20: "completed step receipts are read and replayed, never
/// executed again"; node-identity-and-state.md §2.7: the same MeshId re-claiming its name is idempotent): the
/// fabric is past its first state-commit, so a claim is written to its store root. The fabric-primary
/// accepts mesh2, minting mesh2's id at that acceptance, runs `create-mesh:mesh2` (the claim of mesh2's
/// name writes its row, and the step is receipted `Complete`) and is lost before the next step's receipt is
/// durable: the first admin's `AllocateIdentity`. The loss waits for the receipt to reach the surviving
/// mesh1 admin: its own Build log holds `create-mesh:mesh2` `Complete` at the lost attempt. The
/// successor fabric-primary resumes the same Build: it
/// reads the `create-mesh:mesh2` receipt and does not run the step again, and streams the Build to
/// `Complete`. mesh2 is the mesh the lost fabric-primary's claim wrote. The Build holds exactly one
/// `Complete` receipt of `create-mesh:mesh2`, written by the run that executed it. Must NOT happen: the
/// successor claiming mesh2's name again, or a second receipt of the step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_fabric_primary_reads_the_create_mesh_receipt_and_does_not_claim_the_mesh_again() {
    let test = "a_successor_fabric_primary_reads_the_create_mesh_receipt_and_does_not_claim_the_mesh_again";
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let mut estate = Estate::bootstrap_external(owner(test), FABRIC, "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
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
    assert_eq!(receipts, vec![(lost.attempt, "Complete".to_string(), true)], "create-mesh:mesh2 is receipted once, by the run that executed it; the successor reads that receipt and runs nothing: {receipts:?}");
}

/// CONTRACT (node-rpc-envelope.md, Build carrier: "Settle an idempotent uncertain step by its natural key";
/// node-identity-and-state.md §2.7: "The same MeshId re-claiming its own name, at state-commit, on a retry or
/// from a replaced primary, is idempotent and accepted"): the fabric is past its first state-commit. The
/// fabric-primary accepts mesh2, claims mesh2's name with the MeshId the Build carries (its row lands on
/// the store root) and is lost before it receipts `create-mesh:mesh2`. The successor fabric-primary resumes
/// the same Build: the step has no receipt, so it is uncertain and runs by its natural key; the claim
/// finds the mesh's own row (`row-held-by-mesh`) and the step is receipted `Complete` by the successor's
/// run. mesh2 is the mesh the lost fabric-primary's claim wrote. The Build holds exactly one `Complete`
/// receipt of `create-mesh:mesh2`, written after the cut. Must NOT happen: a refused claim
/// (`name-held`) by the mesh's own row, a second MeshId, or a step that ends with no receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_fabric_primary_settles_a_create_mesh_with_no_receipt_by_its_natural_key_and_receipts_it() {
    let test = "a_successor_fabric_primary_settles_a_create_mesh_with_no_receipt_by_its_natural_key_and_receipts_it";
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let mut estate = Estate::bootstrap_external(owner(test), FABRIC, "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
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
    assert!(matches!(receipts.as_slice(), [(at, step, true)] if *at >= lost.attempt && step == "Complete"), "create-mesh:mesh2 is receipted once, by the successor's run, after the cut: {receipts:?}");
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
    let wrapper = dir.join("rafka-rpc-node-padded");
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
    let mut estate = Estate::bootstrap_external_with_env(owner(test), FABRIC, "mesh1", &set, &sha, &["rpc_node"], &[("TOKIO_WORKER_THREADS", "1")])
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
        let mut estate = Estate::bootstrap_external(owner(&test), FABRIC, "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.unwrap();
        let caller = Caller::of(&estate).await;
        let carrier = born(&estate, &caller, &SHAPE).await;
        let node = seatless_node(&estate).await;
        let before = estate.node_value(&node.to_string()).await;
        let door = Door::at(&estate, "mesh2.admin.1").await;
        door.arm("closure", json!({"kind": "receipt", "step": cut, "operation": "node.connections.delete", "node": node.to_string()})).await;
        let mut stream = caller.nodes(&carrier).delete_connections(&node).await.expect("the Build accepts the closure");
        let accepted = stream.accepted().clone();
        let held = door.wait_held("closure").await;
        assert_eq!(s(&held["hit"]["build_id"]), accepted.build_id.to_string());
        // The surviving executor has heard all receipts that precede this cut.
        let operation = format!("node.connections.delete:{node}");
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
        let receipts = Builds::new(carrier).get(&accepted.build_id).await.expect("Build receipts");
        let closure: Vec<_> = receipts.steps.iter().filter(|r| r.operation == operation && r.step == "DeleteConnections" && matches!(r.result, StepResult::Complete)).collect();
        assert_eq!(closure.len(), 1, "one durable completed step across attempts: {closure:?}");
        let after = estate.node_value(&node.to_string()).await;
        assert_eq!((after["node_id"].clone(), after["incarnation_id"].clone()), (before["node_id"].clone(), before["incarnation_id"].clone()), "the same subject birth executes the reissue");
        assert_eq!(after["status"], "connections-deleting", "a completed closure leaves connections-deleting");
        estate.stop().await;
        let spans = estate.spans();
        let executions: Vec<_> = named(&spans, "rdm.node_admin.node.connections.delete.via-delete-connections").into_iter().filter(|s| s["attributes"]["node"] == node.to_string()).collect();
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
        println!("connections.delete {cut} recovery: http://192.168.68.99:16687/trace/{trace}");
    }
    tokio::join!(at_cut("DeleteConnections"), at_cut("Complete"));
}
