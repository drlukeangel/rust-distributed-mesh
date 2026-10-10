//! product=mesh, feature=fabric-build, subfeature=build-op, rung=MM, provider=process.
//!
//! R-ON8 = A′: the Build family on op `0x20`, with the fabric-primary the one driver of every
//! Build. Every cell drives the estate through the typed objects of `rafka-node-admin-client`
//! (`Builds`, `Nodes`) over Node RPC and awaits their reply streams. The one exception is cell 6,
//! which sends `build.attempt.run` itself because that call is the fabric-primary's own, never a
//! caller's. The spans an estate exports are the flame of each cell; no cell asserts behaviour from
//! them.
//!
//! The estate: `mesh1` (two node-admins, one rpc node) holds the fabric-primary seat, and `mesh2`
//! (two node-admins, two rpc nodes) is run by its own primary, so a node of `mesh2` is executed by
//! an admin that is not the fabric-primary and every Build crosses a real `build.attempt.run`.

use iroh::SecretKey;
use rafka_mesh_entity::{NodeKind, PathName};
use rafka_node_admin_client::{BuildCarrier, BuildFrame, BuildId, BuildSpec, BuildStream, Builds, CallEnd, FabricDesired, Frame, MeshDesired, NodeAdminClient, NodeEvent, Nodes, StartDisposition, WorkflowKind, WorkflowStream};
use rafka_node_rpc::{Budget, CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rafka_node_rpc_contract::build::{Build, BuildReply, BuildRequest};
use rafka_node_rpc_contract::context::CallContext;
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha, Door};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const FABRIC: &str = "fabric1";
const SHAPE: [(&str, u32, u32); 2] = [("mesh1", 2, 1), ("mesh2", 2, 2)];
/// A restart of an rpc node of `mesh2`: executed by `mesh2`'s own primary, not the fabric-primary.
const NODE: &str = "mesh2.rpc.1";
/// The events a restart's stream carries, in order.
const RESTART_EVENTS: [NodeEvent; 10] = [
    NodeEvent::Restarting,
    NodeEvent::Draining,
    NodeEvent::Drained,
    NodeEvent::Left,
    NodeEvent::Stopped,
    NodeEvent::Created,
    NodeEvent::Started,
    NodeEvent::Joined,
    NodeEvent::Ready,
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
struct Caller {
    admin: NodeAdminClient,
    client: Arc<NodeRpcClient>,
    resolver: Arc<StaticResolver>,
    /// The control API each node-admin advertises, by path.name, as the answering admin's view held it.
    bases: std::sync::Mutex<std::collections::BTreeMap<String, String>>,
}

impl Caller {
    /// A caller that learns the fabric from the admin serving `base`.
    async fn of(base: &str) -> Caller {
        let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller binds an endpoint");
        let resolver = Arc::new(StaticResolver::new());
        let client = Arc::new(NodeRpcClient::new(ep, resolver.clone()).with_caller_system("rdm"));
        let caller = Caller { admin: NodeAdminClient::new(base), client, resolver, bases: Default::default() };
        caller.learn().await;
        caller
    }

    /// Every node-admin the answering admin's view holds becomes reachable by its path.
    async fn learn(&self) {
        for n in self.admin.nodes().await.expect("the admin's view") {
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

    async fn carrier(&self) -> BuildCarrier {
        let fp = self.admin.fabric().await.expect("the answering admin's fabric").fabric_primary.expect("a fabric-primary");
        BuildCarrier::new(self.client.clone(), fp)
    }

    fn nodes(&self, carrier: &BuildCarrier) -> Nodes {
        Nodes::new(self.admin.clone(), carrier.clone())
    }
}

/// Read a Build stream to its end; a failed step or a broken stream is a defect of the cell.
async fn until_complete(stream: &mut BuildStream) {
    while let Some(frame) = stream.next().await {
        match frame.expect("the stream does not break") {
            BuildFrame::Complete { .. } => return,
            BuildFrame::Failed { operation, step, reason, .. } => panic!("the Build failed at {operation} {step}: {reason}"),
            _ => {}
        }
    }
    panic!("the stream ended without a terminal frame");
}

/// Birth the fabric's shape through `build.create`: one call, awaited to its `Complete`.
async fn born(estate: &Estate, caller: &Caller, shape: &[(&str, u32, u32)]) -> BuildCarrier {
    let carrier = caller.carrier().await;
    let desired = FabricDesired { fabric: FABRIC.into(), meshes: shape.iter().map(|(m, a, r)| MeshDesired::of(*m, [(NodeKind::NodeAdmin, *a), (NodeKind::RpcNode, *r)])).collect() };
    let builds = Builds::new(carrier.clone());
    let mut stream = match builds.create(&BuildSpec::Fabric(desired.clone())).await {
        // The Build the fabric accepted on its Day 0 is still reconciling: its own stream says when it
        // is done, and the shape is accepted after it.
        Err(CallEnd::BuildInProgress { current }) => {
            let mut day_zero = carrier.resubmit(&current, 1).await.expect("the fabric-primary streams the Build in flight");
            until_complete(&mut day_zero).await;
            builds.create(&BuildSpec::Fabric(desired)).await.expect("the fabric-primary accepts the shape once the Day-0 Build is done")
        }
        other => other.expect("the fabric-primary accepts the shape"),
    };
    until_complete(&mut stream).await;
    estate.settled_shape(shape, Duration::from_secs(90)).await;
    caller.learn().await;
    carrier
}

/// Every frame of a workflow stream, to its end.
async fn drain(stream: &mut WorkflowStream) -> Vec<Frame> {
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

/// The trace id of the first `build` stream a node served (op 0x20).
fn build_serve_traces(spans: &[Value]) -> Vec<(String, String)> {
    spans_named(spans, "rdm.node_rpc.stream.serve.via-direct")
        .into_iter()
        .filter(|sp| s(&sp["attributes"]["op"]) == "32")
        .map(|sp| (s(&sp["trace_id"]), s(&sp["span_id"])))
        .collect()
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
    let caller = Caller::of(&estate.admin).await;
    let carrier = born(&estate, &caller, &SHAPE).await;

    let mut stream = caller.nodes(&carrier).restart(&path(NODE)).await.expect("the fabric-primary accepts the restart");
    let accepted = stream.accepted().clone();
    let frames = drain(&mut stream).await;
    assert_eq!(frames.first(), Some(&Frame::Started { build_id: accepted.build_id.clone(), attempt: accepted.attempt }));
    assert_eq!(frames.last(), Some(&Frame::Complete), "the stream ends with its terminal frame: {frames:?}");
    assert_eq!(events(&frames), RESTART_EVENTS, "every step event, once, in order: {frames:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Failed { .. } | Frame::Blocked { .. })), "{frames:?}");
    assert_eq!(stream.disposition(), StartDisposition::Created);

    estate.stop().await;
    let spans = estate.spans();
    // The flame: the call, the fabric-primary's drive and dispatch, and the executor's run are one trace.
    let build = &accepted.build_id.0;
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
    let caller = Caller::of(&estate.admin).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let nodes = caller.nodes(&carrier);

    let mut cut = nodes.restart(&path(NODE)).await.expect("the fabric-primary accepts the restart");
    let accepted = cut.accepted().clone();
    // The first frame is the call's `Started`; the cut follows it, before any step is reported.
    assert!(matches!(cut.next().await, Some(Ok(Frame::Started { .. }))));
    drop(cut);

    let mut attached = nodes.resume(WorkflowKind::Restart(path(NODE)), &accepted).await.expect("the re-submit is accepted");
    assert_eq!(attached.disposition(), StartDisposition::Attached, "the Build the cut left running is the one the re-submit attaches to");
    assert_eq!(attached.accepted().build_id, accepted.build_id, "never a second Build");
    let first = drain(&mut attached).await;
    assert_eq!(first.last(), Some(&Frame::Complete));
    assert_eq!(events(&first), RESTART_EVENTS, "{first:?}");

    let mut again = nodes.resume(WorkflowKind::Restart(path(NODE)), &accepted).await.expect("the second re-submit is accepted");
    assert_eq!(again.disposition(), StartDisposition::AlreadyApplied);
    let second = drain(&mut again).await;
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
    let build = &accepted.build_id.0;
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
async fn attempt_run(caller: &Caller, to: &str, build: &BuildId, attempt: u32, executor: &str) -> BuildReply {
    let req = BuildRequest::AttemptRun { build_id: build.0.clone(), attempt, executor: executor.into(), context: CallContext::default(), intent: Vec::new() };
    let opts = CallOptions { budget: Budget::Stream { send: Duration::from_secs(10) }, ..CallOptions::default() };
    match caller.client.call_stream::<Build>(&NodeTarget::CurrentPath(path(to)), &req, &opts).await {
        Err((RpcOutcome::Reply(r), _)) => r.into_value(),
        Err((other, _)) => panic!("the call to {to} ended {other:?}"),
        Ok((mut stream, _)) => match stream.next().await {
            Some(rafka_node_rpc::stream::StreamItem::Frame(_, r)) => r,
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
    let caller = Caller::of(&estate.admin).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let mut stream = caller.nodes(&carrier).restart(&path(NODE)).await.expect("the fabric-primary accepts the restart");
    let accepted = stream.accepted().clone();
    assert_eq!(drain(&mut stream).await.last(), Some(&Frame::Complete));

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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_executor_ends_the_inner_call_indeterminate_and_the_successor_is_claimed_only_after_proof() {
    let test = "a_lost_executor_ends_the_inner_call_indeterminate_and_the_successor_is_claimed_only_after_proof";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate.admin).await;
    let carrier = born(&estate, &caller, &SHAPE).await;

    let mut stream = caller.nodes(&carrier).restart(&path(NODE)).await.expect("the fabric-primary accepts the restart");
    let accepted = stream.accepted().clone();
    let mut frames = Vec::new();
    // The executor, mesh2's primary, is lost once the restart is under way: its first step frame.
    while !frames.contains(&Frame::Event(NodeEvent::Restarting)) {
        frames.push(stream.next().await.expect("the stream goes on").expect("the stream does not break"));
    }
    estate.kill_node("mesh2.admin.1").await;
    frames.extend(drain(&mut stream).await);
    assert_eq!(frames.last(), Some(&Frame::Complete), "the caller's stream carries the successor's frames to the end: {frames:?}");
    let seen = events(&frames);
    let mut once = seen.clone();
    once.dedup();
    assert_eq!(seen, once, "no event is delivered twice: {frames:?}");
    assert_eq!(seen.last(), Some(&NodeEvent::Restarted), "{seen:?}");

    estate.stop().await;
    let spans = estate.spans();
    let build = &accepted.build_id.0;
    let proofs: Vec<_> = spans_named(&spans, "rdm.node_admin.build.update.via-departure-proven").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["attempt"]) == accepted.attempt.to_string()).collect();
    assert_eq!(proofs.len(), 1, "the departure was proven once, before the next attempt was claimed: {proofs:?}");
    assert_eq!(s(&proofs[0]["attributes"]["executor"]), "mesh2.admin.1");
    let claims: Vec<_> = spans_named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build && sp["attributes"]["attempt"].as_str().and_then(|a| a.parse::<u32>().ok()) > Some(accepted.attempt)).collect();
    let successor = claims.iter().find(|sp| s(&sp["attributes"]["executor"]) == "mesh2.admin.2").expect("the next attempt was claimed for the successor");
    assert!(successor["start_unix_nano"].as_u64() > proofs[0]["start_unix_nano"].as_u64(), "the claim for the successor came after the proof");
    let lost = spans_named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["outcome"]) == "executor-lost").expect("the inner call to the lost executor ended");
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
    let caller = Caller::of(&estate.admin).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let fp = carrier.fabric_primary().to_string();

    // The cut: the fabric-primary's claim is durable and broadcast, and the drive has not dispatched.
    let door = Door::open(&estate.root, &fp, &estate.admin).await;
    door.arm("claimed", json!({"kind": "claimed-attempt", "attempt": null})).await;
    let mut stream = caller.nodes(&carrier).restart(&path(NODE)).await.expect("the fabric-primary accepts the restart");
    let accepted = stream.accepted().clone();
    let held = door.wait_held("claimed").await;
    assert_eq!(s(&held["hit"]["build_id"]), accepted.build_id.0, "{held}");
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
    let survivors: Vec<String> = caller.bases_except(&fp);
    let (successor, base) = wait_for("an admin other than the dead one holds the fabric-primary seat", Duration::from_secs(60), || async {
        for base in &survivors {
            if let Ok(f) = NodeAdminClient::new(base.as_str()).fabric().await {
                if let Some(p) = f.fabric_primary.filter(|p| p.to_string() != fp) {
                    return Some((p, base.clone()));
                }
            }
        }
        None
    })
    .await;
    let after = Caller::of(&base).await;
    let carrier = after.carrier().await;
    assert_eq!(carrier.fabric_primary(), &successor);
    let mut resumed = after.nodes(&carrier).resume(WorkflowKind::Restart(path(NODE)), &accepted).await.expect("the successor accepts the re-submit");
    assert_eq!(resumed.accepted().build_id, accepted.build_id, "never a second Build");
    let frames = drain(&mut resumed).await;
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
    estate.admin = base.clone();
    estate.stop().await;
    let spans = estate.spans();
    let build = &accepted.build_id.0;
    let run = spans_named(&spans, "rdm.node_admin.build.serve.via-attempt-run")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["attempt"]) == accepted.attempt.to_string())
        .expect("the executor was called with the claimed attempt");
    assert_eq!(s(&run["attributes"]["executor"]), "mesh2.admin.1");
    assert_eq!(s(&run["attributes"]["outcome"]), "started");
    estate.record_trace_url(run["trace_id"].as_str().unwrap_or(""));
    let _ = build_serve_traces(&spans);
}

impl Caller {
    /// The control API bases of the node-admins other than `dead`, as the answering admin's view
    /// held them before it died.
    fn bases_except(&self, dead: &str) -> Vec<String> {
        self.bases.lock().unwrap().iter().filter(|(name, _)| name.as_str() != dead).map(|(_, base)| base.clone()).collect()
    }
}
