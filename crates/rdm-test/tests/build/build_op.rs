//! product=mesh, feature=fabric-build, subfeature=build-op, rung=MM, provider=process.
//!
//! R-ON8 = A′: the Build family on op `0x20`, with the fabric-primary the one driver of every
//! Build. Every cell drives the estate through the typed objects of `rafka-node-admin-client`
//! (`Builds`, `Nodes`) over Node RPC and awaits their reply streams. The one exception is cell 6,
//! which sends `build.section.run` itself because that call is the fabric-primary's own, never a
//! caller's. The one read made outside the client's typed objects is cell 8's precondition: the fabric's
//! Build records, read from its store root (Q292) and awaited through the shared `build_log::log_fact`. The spans an estate exports are the flame of each cell; no cell asserts behaviour
//! from them.
//!
//! The estate: `mesh1` (two node-admins, one node) holds the fabric-primary role, and `mesh2`
//! (two node-admins, two nodes) is run by its own primary, so a node of `mesh2` is executed by
//! an admin that is not the fabric-primary and every Build crosses a real `build.section.run`.

use iroh::SecretKey;
use rdm_core::{PathName};
use rdm_client::{BuildCarrier, BuildFrame, BuildId, BuildStream, Builds, CallEnd, FabricDesired, Fabrics, Frame, MeshDesired, Meshes, NodeEvent, NodeStatus, Nodes, Peers, StartDisposition, WorkflowKind, WorkflowStream};
use rdm_admin::build_state::{BuildFact, StepOutcome};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rdm_core::build::{Build, BuildReply, BuildRequest, StepResult};
use rdm_core::context::CallContext;
use rdm_core::outcome::RpcOutcome;
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, FaultClient};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const FABRIC: &str = "fabric1";
const SHAPE: [(&str, u32, u32); 2] = [("mesh1", 2, 1), ("mesh2", 2, 2)];
/// The events a stop's stream carries, in order.
const STOP_EVENTS: [NodeEvent; 2] = [NodeEvent::ConnectionsDeleted, NodeEvent::Stopped];
/// A live node's `node.delete` (node-delete.md): its composed stop's events, then its deletion. A node
/// command (a stop, a start) is never a Build (Q342), so the Build cells' one-node vehicle is the delete.
const DELETE_EVENTS: [NodeEvent; 3] = [NodeEvent::ConnectionsDeleted, NodeEvent::Stopped, NodeEvent::Deleted];

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "fabric-build".into(), subfeature: "build-op".into(), rung: "MM".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn birth_of(node: &rdm_client::NodeView) -> rdm_core::framing::Birth {
    rdm_core::framing::Birth { node_id: node.node_id.clone(), incarnation: node.incarnation.clone().expect("launched node") }
}

fn at_node(span: &Value, location: &PathName) -> bool {
    span["attributes"]["node_path"] == location.path && span["attributes"]["node_name"] == location.name
}

/// A caller outside the fabric: its own endpoint, and the node-admins it reaches by path.
pub(crate) struct Caller {
    /// The estate's peers, the node-admin the caller enters at, and the Fabric: every read is `fabric.get` / `node.get` there.
    peers: Arc<Peers>,
    entry: PathName,
    fabric_id: String,
    client: Arc<NodeRpcClient>,
    resolver: Arc<StaticResolver>,
    /// Observed admin records, indexed by globally unique node ID.
    admins: std::sync::Mutex<std::collections::BTreeMap<String, rdm_client::NodeView>>,
}

impl Caller {
    /// A caller that learns the fabric from the estate's entry node-admin, over Node RPC.
    pub(crate) async fn of(estate: &Estate) -> Caller {
        let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller binds an endpoint");
        let resolver = Arc::new(StaticResolver::new());
        let client = Arc::new(NodeRpcClient::new(ep, resolver.clone()).with_caller_system("rdm"));
        let caller = Caller { peers: estate.peers().await, entry: estate.entry().await, fabric_id: estate.fabric_id.clone(), client, resolver, admins: Default::default() };
        caller.learn().await;
        caller
    }

    /// Every node-admin the answering admin's view holds becomes reachable by its path.
    async fn learn(&self) {
        for n in rdm_test::typed::node_views_of(&self.peers, &self.entry, &self.fabric_id).await.expect("the entry admin's view") {
            if n.r#type != "node-admin" {
                continue;
            }
            self.admins.lock().unwrap().insert(n.node_id.to_string(), n.clone());
            let (Some(ep), Some(addr), Some(inc)) = (n.endpoint_id.as_deref(), n.transport_addr, n.incarnation.clone()) else { continue };
            self.resolver.insert(ResolvedNode { mesh: n.mesh().clone(), r#type: n.r#type, node_id: n.node_id.clone(), path: n.path.clone(), name: n.name.clone(), endpoint_id: ep.parse().expect("an iroh key"), transport_addr: addr, incarnation: inc });
        }
    }

    fn location(&self, node_id: &str) -> PathName {
        self.admins.lock().unwrap().get(node_id).expect("observed admin ID").location()
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
            BuildFrame::Failed { reason, .. } => panic!("the Build could not start: {reason}"),
            BuildFrame::Incomplete { sections } => panic!("the Build ended incomplete: {sections:?}"),
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
    let desired = FabricDesired { fabric: FABRIC.into(), meshes: shape.iter().map(|(m, a, r)| MeshDesired::of(*m, [("node-admin", *a), ("node", *r)])).collect() };
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

/// The node of `mesh2` that holds no role: executed by `mesh2`'s own primary with no preceding
/// role move.
async fn secondary_node(estate: &Estate) -> PathName {
    estate.node_views().await.into_iter().filter(|n| n.mesh() == "mesh2" && n.r#type == "node" && !n.is_primary).map(|n| n.location()).min().expect("a node of mesh2 holding no primary role")
}

/// CONTRACT (acceptance 1): `node.delete` over `0x20` is one call. The stream is `Started`, a frame
/// per step once its `rdm.build.step` event is written by the admin that ran it, and `Complete`,
/// pushed by mesh2's primary through the fabric-primary: mesh2's section is one call to mesh2's own
/// primary, so the Build crosses a real `build.section.run`. Every event the stop names arrives once, in
/// order, and nothing polls a Build to learn it. A stop is a node command, never a Build (Q342): the
/// one-node Build is the delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_over_the_build_family_is_one_call_that_pushes_every_step_frame_and_completes() {
    let test = "a_delete_over_the_build_family_is_one_call_that_pushes_every_step_frame_and_completes";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = secondary_node(&estate).await;
    let views = estate.node_views().await;
    let executor = views.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.is_primary).expect("mesh2 primary");
    let fabric_primary = views.iter().find(|n| n.is_fabric_primary).expect("fabric primary");

    let mut stream = caller.nodes(&carrier).delete(vec![estate.node_view(&node).await.node_id]).await.expect("the fabric-primary accepts the delete");
    let accepted = stream.accepted().cloned().expect("the call opened a Build");
    let frames = frames_of(&mut stream).await;
    assert_eq!(frames.first(), Some(&Frame::Started { build_id: accepted.build_id.clone(), }));
    assert_eq!(frames.last(), Some(&Frame::Complete), "the stream ends with its terminal frame: {frames:?}");
    assert_eq!(events(&frames), DELETE_EVENTS, "every step event, once, in order: {frames:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Failed { .. } | Frame::Pending { .. })), "{frames:?}");
    assert_eq!(stream.disposition(), Some(StartDisposition::Created));

    estate.stop().await;
    let spans = estate.spans();
    // The flame: the call, the fabric-primary's drive and its dispatch of mesh2's section, and mesh2's
    // primary's run of the section are one trace.
    let build = &accepted.build_id.to_string();
    let section = spans_named(&spans, "rdm.node_admin.build.serve.via-section-run")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["mesh"]) == "mesh2")
        .expect("mesh2's section call");
    assert!(at_node(section, &executor.location()), "mesh2's section runs on mesh2's primary, not the fabric-primary: {section}");
    let dispatched = spans_named(&spans, "rdm.node_admin.build.update.via-dispatched")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["mesh"]) == "mesh2")
        .expect("the fabric-primary's dispatch of mesh2's section");
    assert!(at_node(dispatched, &fabric_primary.location()), "the dispatch is the fabric primary's: {dispatched}");
    assert_eq!(dispatched["trace_id"], section["trace_id"], "the section is in the call's trace");
    let reconcile = section;
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
/// the Build is never created twice and no step has a second `complete` event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_outer_stream_leaves_the_build_running_and_a_resubmit_after_the_end_is_already_applied() {
    let test = "a_cut_outer_stream_leaves_the_build_running_and_a_resubmit_after_the_end_is_already_applied";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = secondary_node(&estate).await;
    let nodes = caller.nodes(&carrier);

    // The node.id the Delete names, read once: the node is gone from the view once the Build ends.
    let node_id = estate.node_view(&node).await.node_id;
    let mut cut = nodes.delete(vec![node_id.clone()]).await.expect("the fabric-primary accepts the delete");
    let accepted = cut.accepted().cloned().expect("the call opened a Build");
    // The first frame is the call's `Started`; the cut follows it, before any step is reported.
    assert!(matches!(cut.next().await, Some(Ok(Frame::Started { .. }))));
    drop(cut);

    let mut attached = nodes.resume(WorkflowKind::Delete(vec![node_id.clone()]), &accepted).await.expect("the re-submit is accepted");
    assert_eq!(attached.disposition(), Some(StartDisposition::Attached), "the Build the cut left running is the one the re-submit attaches to");
    assert_eq!(attached.accepted().expect("the call opened a Build").build_id, accepted.build_id, "never a second Build");
    let first = frames_of(&mut attached).await;
    assert_eq!(first.last(), Some(&Frame::Complete));
    assert_eq!(events(&first), DELETE_EVENTS, "{first:?}");

    let mut again = nodes.resume(WorkflowKind::Delete(vec![node_id.clone()]), &accepted).await.expect("the second re-submit is accepted");
    assert_eq!(again.disposition(), Some(StartDisposition::AlreadyApplied));
    let second = frames_of(&mut again).await;
    assert_eq!(second.last(), Some(&Frame::Complete), "the terminal frame follows AlreadyApplied");
    assert_eq!(events(&second), DELETE_EVENTS, "the receipts held are replayed as frames: {second:?}");

    let held = carrier.clone().build(&accepted.build_id).await.expect("build.get [id]");
    let mut keys = held.completed_steps();
    let before = keys.len();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), before, "no step has two complete events: {:?}", held.events);
    assert_eq!(estate.starts_of_build(&accepted.build_id.to_string()).await, 1, "the cut and the re-submits started no second run");

    estate.stop().await;
    let spans = estate.spans();
    let build = &accepted.build_id.to_string();
    let drives: Vec<_> = spans_named(&spans, "rdm.node_admin.build.update.via-drive").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build).collect();
    assert!(!drives.is_empty());
    assert!(drives.iter().any(|sp| s(&sp["attributes"]["outcome"]) == "terminal"), "the drive ended the Build with no caller attached: {drives:?}");
    let reconcile = spans_named(&spans, "rdm.node_admin.build.serve.via-section-run")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == *build && s(&sp["attributes"]["mesh"]) == "mesh2")
        .expect("mesh2's section run");
    estate.record_trace_url(reconcile["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (i146; build.md: "a live run is reattached, never re-claimed"; Q22: the reattach after a
/// Build stream's connection closes is the owner's (`StreamingProtocol::ENDS_WITH_ITS_CONNECTION`); node-rpc-envelope.md "Frames, durable receipts and
/// reattachment": "`build.get [id: build_id]` for the receipts, then the outer stream again, resuming from the
/// receipts"): a watched Build whose connection closes mid-stream is reattached by its build id, each
/// reattachment a `build.get [id]` and then the outer stream again, and ends `Complete`, the run it watched
/// left running: no second run, no step with two `complete` events. The Build grows mesh2 by a node;
/// its watcher reads the first frame, then every connection of the watching client is closed, the
/// stream's with them. Must NOT happen: the watcher ending on the break, a reattachment that reads no
/// step events, a second run, a step run twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watched_build_whose_connection_closes_mid_stream_is_reattached_and_completes_with_no_step_run_twice() {
    let test = "a_watched_build_whose_connection_closes_mid_stream_is_reattached_and_completes_with_no_step_run_twice";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let grown = FabricDesired { fabric: FABRIC.into(), meshes: [("mesh1", 2, 1), ("mesh2", 2, 3)].iter().map(|(m, a, r)| MeshDesired::of(*m, [("node-admin", *a), ("node", *r)])).collect() };
    let mut stream = carrier.create(estate.shape_change_at(&carrier, &grown).await).await.expect("the fabric-primary accepts the change");
    let build_id = stream.build_id().clone();
    let first = stream.next().await.expect("a frame").expect("the stream reads its first frame");
    assert!(!matches!(first, BuildFrame::Complete { .. } | BuildFrame::Failed { .. } | BuildFrame::Incomplete { .. }), "the Build is still running when its connection closes: {first:?}");

    let closed = carrier.client().close_pooled("the cell closes the watcher's connection");
    assert!(closed >= 1, "the watching client held the stream's connection");
    let followed = carrier.follow(stream).await.expect("the watcher follows the Build to its terminal");
    assert!(followed.reattached >= 1, "the watcher reattached after the close: {followed:?}");
    assert!(matches!(followed.frames.last(), Some(BuildFrame::Complete { .. })), "the Build ends Complete: {followed:?}");

    let held = carrier.clone().build(&build_id).await.expect("build.get [id]");
    // The close started no second run: every step ran in the run the watcher watched, each once.
    assert_eq!(estate.starts_of_build(&build_id.to_string()).await, 1, "the close started no second run");
    let mut keys = held.completed_steps();
    let all = keys.len();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), all, "no step has two complete events: {:?}", held.events);

    estate.stop().await;
    let spans = estate.spans();
    let build = build_id.to_string();
    // Each reattachment read the Build's receipts before its stream again; the cell's own read is one more.
    let gets = spans_named(&spans, "rdm.node_admin.build.serve.via-node-rpc").into_iter().filter(|sp| s(&sp["attributes"]["call"]) == "build.get").count();
    assert!(gets as u32 > followed.reattached, "each reattachment read build.get [id] ({} reattachment(s), {gets} build.get read(s) with the cell's own)", followed.reattached);
    let reconcile = spans_named(&spans, "rdm.node_admin.build.serve.via-section-run")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["mesh"]) == "mesh2")
        .expect("mesh2's section run");
    println!("watched Build reattached {} time(s), Complete; flame http://192.168.68.99:16687/trace/{}", followed.reattached, s(&reconcile["trace_id"]));
    estate.record_trace_url(reconcile["trace_id"].as_str().unwrap_or(""));
}

/// What a `build.section.run` of `mesh`'s section of `build` sent to `to` answers, whole: the refusal,
/// or the first frame.
async fn section_run(caller: &Caller, to: &rdm_core::NodeId, build: &BuildId, mesh: &str) -> BuildReply {
    let req = BuildRequest::SectionRun { build_id: build.to_string(), mesh: mesh.to_string(), context: CallContext::default() };
    let opts = CallOptions {
        send: rdm_transport::Send::with_time_limit(Duration::from_secs(10)),
        ..CallOptions::default()
    };
    match caller
        .client
        .call_stream::<Build>(&NodeTarget::ExactNode(to.clone()), &req, &opts)
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

/// CONTRACT (acceptance 6; build-envelope "build.section.run": the recipient is the mesh's primary):
/// `build.section.run` of a mesh's section is refused by name, `NotExecutor`, when the recipient is not
/// that mesh's primary; it names the mesh's primary the recipient sees. A Build has no claims and no
/// attempts (Q344), so nothing else is carried to refuse.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_section_run_to_a_node_that_is_not_the_meshs_primary_is_refused_by_name() {
    let test = "a_section_run_to_a_node_that_is_not_the_meshs_primary_is_refused_by_name";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let node = secondary_node(&estate).await;
    let mut stream = caller.nodes(&carrier).delete(vec![estate.node_view(&node).await.node_id]).await.expect("the fabric-primary accepts the delete");
    let accepted = stream.accepted().cloned().expect("the call opened a Build");
    assert_eq!(frames_of(&mut stream).await.last(), Some(&Frame::Complete));

    let views = estate.node_views().await;
    let mesh2_primary = birth_of(views.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.is_primary).expect("mesh2's primary"));
    let recipient = views.iter().find(|n| n.is_fabric_primary).expect("fabric primary");
    let recipient_birth = birth_of(recipient);
    assert_ne!(mesh2_primary.node_id, recipient_birth.node_id, "the cell calls a node that is not mesh2's primary");
    let wrong = section_run(&caller, &recipient.node_id, &accepted.build_id, "mesh2").await;
    assert_eq!(wrong, BuildReply::NotExecutor { named: mesh2_primary, recipient: recipient_birth });

    estate.stop().await;
    let spans = estate.spans();
    let refused = spans_named(&spans, "rdm.node_admin.build.reject.via-not-executor").into_iter().next().expect("the refusal is spanned");
    estate.record_trace_url(refused["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (acceptance 5; the approved proposal node-primary-update-fabric-mesh-and-node-type.md,
/// Part C1 step 8: "Only then does the commanding Build continue, and only once the successor's
/// installation is acknowledged"): a planned role move mid-Build ends the caller's outer stream with
/// the typed new-role frame, never `Indeterminate`. `mesh1` holds the fabric-primary role, and the
/// Build deletes `mesh1`: before the delete is claimed, the fabric-primary moves the fabric-primary role alone
/// to `mesh2`'s primary by node.fabric-primary.update (op `0x21`, mesh-delete.md step 1), and the Build
/// goes on at the new fabric-primary. The successor installs the election's result the call carries
/// (`primary.update.via-primary-updated` at the new fabric-primary, `held`) before the old fabric-primary's drive refuses in its
/// span and names the new role, so the caller's re-submit by the Build id at the role the frame names
/// is accepted and streams to `Complete`.
/// Must NOT happen: the new role named to the caller before it installed (its re-submit refused
/// `not-fabric-primary`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_handover_mid_build_ends_the_outer_stream_with_the_new_fabric_primary_and_the_old_one_dispatches_nothing() {
    let test = "a_planned_handover_mid_build_ends_the_outer_stream_with_the_new_fabric_primary_and_the_old_one_dispatches_nothing";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let carrier = born(&estate, &caller, &SHAPE).await;
    let old_primary = estate.node_views().await.into_iter().find(|n| n.is_fabric_primary).expect("fabric primary");
    let old_role = old_primary.node_id.clone();
    assert_eq!(old_primary.mesh(), "mesh1", "mesh1 holds the fabric-primary role: {old_role}");

    // The Build deletes mesh1; the role leaves with it, by a planned handover inside the Build.
    let keep = FabricDesired { fabric: FABRIC.into(), meshes: vec![MeshDesired::of("mesh2", [("node-admin", 2), ("node", 2)])] };
    let mut stream = carrier.create(estate.shape_change_at(&carrier, &keep).await).await.expect("the fabric-primary accepts the delete of mesh1");
    let build_id = stream.build_id().clone();
    let new_role = loop {
        match stream.next().await {
            Some(Ok(BuildFrame::FabricPrimaryMoved { fabric_primary })) => break fabric_primary.expect("the old role names the new one"),
            Some(Ok(BuildFrame::Complete { .. })) => panic!("the old fabric-primary cannot finish a delete of its own mesh"),
            Some(Ok(BuildFrame::Failed { reason, .. })) => panic!("the Build could not start: {reason}"),
            Some(Ok(BuildFrame::Incomplete { sections })) => panic!("the Build ended incomplete: {sections:?}"),
            Some(Ok(_)) => continue,
            Some(Err(end)) => panic!("a planned role move ends the outer stream with the typed new-primary frame, never as {end:?}"),
            None => panic!("the stream ended without a terminal frame"),
        }
    };
    assert_eq!(caller.admins.lock().unwrap().get(new_role.as_str()).expect("new primary record").mesh(), "mesh2", "the role moved to mesh2's primary: {new_role}");
    assert_ne!(new_role, old_role);

    // The caller re-submits the same Build at the role the frame names.
    let at_new_role = BuildCarrier::at_node(caller.client.clone(), new_role.clone());
    let mut resumed = at_new_role.resubmit(&build_id).await.expect("the new role accepts the re-submit by build_id");
    assert_eq!(resumed.build_id(), &build_id, "never a second Build");
    until_complete(&mut resumed).await;

    estate.enter(&caller.location(new_role.as_str())).await;
    estate.stop().await;
    let spans = estate.spans();
    let build = &build_id.as_str();
    let handover_calls = spans_named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().filter(|sp| sp["attributes"]["op"].as_str().and_then(|o| o.parse::<u64>().ok()).or_else(|| sp["attributes"]["op"].as_u64()) == Some(0x21)).count();
    assert!(handover_calls >= 1, "the role moved by the fabric-primary handover, op 0x21");
    let yielded: Vec<_> = spans_named(&spans, "rdm.node_admin.build.reject.via-fabric-primary-changed").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build).collect();
    assert!(!yielded.is_empty(), "the old fabric-primary's dispatch after its role moved was refused, RoleYielded in its span");
    assert!(yielded.iter().all(|sp| at_node(sp, &rdm_core::PathName { path: old_primary.path.clone(), name: old_primary.name.clone() })), "{yielded:?}");
    let installed: Vec<_> = spans_named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == *build && at_node(sp, &caller.location(new_role.as_str())) && s(&sp["attributes"]["outcome"]) == "held").collect();
    assert_eq!(installed.len(), 1, "the new fabric-primary installed the election's result once: {installed:?}");
    let installed_at = installed[0]["start_unix_nano"].as_u64().unwrap_or(u64::MAX);
    let early: Vec<_> = yielded.iter().filter(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0) <= installed_at).collect();
    assert!(early.is_empty(), "C1 step 8: the old role's drive names the new role only once the successor installed ({installed_at}): {early:?}");
    estate.record_trace_url(yielded[0]["trace_id"].as_str().unwrap_or(""));
}

impl Caller {
    /// The node-admins other than `dead`, as the answering admin's view held them before it died.
    fn admins_except(&self, dead: &str) -> Vec<String> {
        self.admins.lock().unwrap().keys().filter(|name| name.as_str() != dead).cloned().collect()
    }

    /// The carrier that starts at node-admin `at`: its reads answer from that admin's own view.
    fn carrier_at(&self, at: &str) -> BuildCarrier {
        BuildCarrier::at_node(self.client.clone(), at.parse().expect("canonical admin ID"))
    }

    /// The first node-admin that survives `dead`.
    fn survivor_of(&self, dead: &str) -> String {
        self.admins_except(dead).into_iter().next().unwrap_or_else(|| panic!("no node-admin survives {dead}"))
    }

    /// The node-admin that is fabric-primary once `dead`, the fabric-primary, has died: nobody takes the role
    /// meanwhile, and fabric recovery restores the node it re-creates at `dead`'s (path, name) (Q122). A
    /// surviving admin's view is read until it names a Ready fabric-primary other than `dead`'s
    /// birth; that birth is made reachable here, and its (path, name) returned.
    async fn restored_fabric_primary(&self, dead: &str) -> String {
        let asked = self.location(&self.survivor_of(dead));
        let dead_id = dead.to_string();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        loop {
            if let Some(views) = rdm_test::typed::node_views_of(&self.peers, &asked, &self.fabric_id).await {
                let restored = views.into_iter().find(|n| n.r#type == "node-admin" && n.is_fabric_primary && n.state == NodeStatus::Ready && n.node_id.to_string() != dead_id);
                if let Some(n) = restored {
                    if let (Some(ep), Some(addr), Some(inc)) = (n.endpoint_id.as_deref(), n.transport_addr, n.incarnation.clone()) {
                        if let Ok(old) = rdm_core::NodeId::parse(&dead_id) {
                            self.resolver.remove_gone(&old);
                            self.admins.lock().unwrap().remove(&dead_id);
                        }
                        self.resolver.insert(ResolvedNode { mesh: n.mesh().clone(), r#type: n.r#type.clone(), node_id: n.node_id.clone(), path: n.path.clone(), name: n.name.clone(), endpoint_id: ep.parse().expect("an iroh key"), transport_addr: addr, incarnation: inc });
                        self.admins.lock().unwrap().insert(n.node_id.to_string(), n.clone());
                        // The restored node's own view names it fabric-primary once it installed the roles.
                        let node_id = n.node_id.to_string();
                        Fabrics::new(self.carrier_at(&node_id))
                            .fact(&format!("{node_id} names itself the fabric-primary"), Duration::from_secs(60), |f| f.fabric_primary.as_ref().is_some_and(|p| p.as_str() == node_id))
                            .await
                            .unwrap_or_else(|end| panic!("{end}"));
                        return node_id;
                    }
                }
            }
            assert!(tokio::time::Instant::now() < deadline, "{asked:?}'s view names no Ready fabric-primary other than {dead}'s birth {dead_id} within 120s");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// The next `Pending` frame of `stream`. A terminal frame or a broken stream before it is a defect of
/// the cell; frames before it are returned beside it.
async fn next_pending(stream: &mut BuildStream) -> (Vec<BuildFrame>, (String, String, String)) {
    let mut before = Vec::new();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(90), stream.next()).await.expect("a frame within the bound").expect("the stream goes on").expect("the stream does not break");
        match frame {
            BuildFrame::Pending { step, reason, operation, .. } => return (before, (step, reason, operation)),
            BuildFrame::Complete { .. } | BuildFrame::Failed { .. } | BuildFrame::Incomplete { .. } => panic!("the Build ended before the Pending frame: {frame:?} after {before:?}"),
            other => before.push(other),
        }
    }
}

/// Whether `frame` is a Pending frame of a step in flight: its only source is a subject's Status
/// `Pending` (node-rpc-envelope.md, Build "Frames").
fn birth_pending(frame: &BuildFrame) -> bool {
    matches!(frame, BuildFrame::Pending { step, .. })
}

/// How many attempts of `node`'s knob hook (`rdm_test::pre_node_ready_probe::KnobPreNodeReady`)
/// answered `Pending` for `reason`.
fn knob_attempts(knob: &std::path::Path, node: &rdm_core::PathName, reason: &str) -> usize {
    std::fs::read_to_string(rdm_test::location_knob_file(knob, node).with_extension("attempts")).unwrap_or_default().lines().filter(|l| *l == format!("pending {reason}")).count()
}

/// CONTRACT (fabric-node-hooks.md "Outcomes and the gate"; node-rpc-envelope.md frame table, Pending
/// "emitted when its reason changes"): a node whose `pre_node_ready` answers `Pending` answers its
/// executor's `ProbeNodeState` with Status `Pending { node_id, incarnation, reason }`, and the Build
/// stream carries `Pending { WaitForNodeReady, reason }` once per reason. The hook runs again on its
/// `Send`, so it answers Pending for the same reason many times; none of those reruns frames anything.
/// A changed reason frames once more; when the hook passes the node is Ready, the step completes and
/// the Build is `Complete` with no further Pending. The Builds that birthed the shape, whose nodes
/// register no hook (plain Pending until Ready, a node-admin's readiness gate answering `NotReady` until its
/// authority is filled), frame no Pending: a `NotReady` is the readiness gate not open yet and never a hook's
/// answer. What must NOT happen: a Pending frame for a plain Pending node or a readiness gate's `NotReady`, a
/// repeat for an unchanged reason, or a Pending frame after the hook passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pending_node_is_framed_once_per_reason_and_its_build_completes_when_the_hook_passes() {
    let test = "a_pending_node_is_framed_once_per_reason_and_its_build_completes_when_the_hook_passes";
    const SHAPE1: [(&str, u32, u32); 1] = [("mesh1", 2, 1)];
    let blocked = rdm_core::PathName::new("mesh1.node", "2").unwrap();
    const A: &str = "the rows of orders are not yet published";
    const B: &str = "the authority of orders is connections-deleting";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let (carrier, shape_frames) = born_framed(&estate, &caller, &SHAPE1).await;
    assert!(!shape_frames.iter().any(birth_pending), "nodes with no hook are plain Pending until Ready and frame no Pending: {shape_frames:?}");

    // The next node's hook answers Pending for A from its first attempt.
    let knob = rdm_test::pre_node_ready_knob_dir(&estate.root);
    rdm_test::write_pre_node_ready_knob(&estate.root, &blocked, A).unwrap();
    let grown = FabricDesired { fabric: FABRIC.into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 2), ("node", 2)])] };
    let mut stream = carrier.create(estate.shape_change_at(&carrier, &grown).await).await.expect("the fabric-primary accepts the grown shape");
    let (before_a, (step, reason, operation)) = next_pending(&mut stream).await;
    assert_eq!(step, "WaitForNodeReady", "the pending step: {reason}");
    // The frame's operation is the node's create, keyed by the node.id the Build record minted for it
    // (D58); its reason names the node by node id and the hook's reason.
    assert!(reason.contains(": pre_node_ready answered Pending: ") && reason.contains(A), "the frame names the node and the hook's reason: {reason}");
    let blocked_id = reason.split(':').next().expect("the reason opens with the birth's node id").to_string();
    assert_eq!(operation, format!("node.create:{blocked_id}"), "the frame names the birth's create by the node.id it runs with: {operation:?}");
    assert!(!before_a.iter().any(|f| matches!(f, BuildFrame::Pending { .. })));

    // Reruns pending for the same reason: the hook runs again on its Send; wait until it has
    // answered Pending for A three more times.
    wait_for("three more attempts pending for A", Duration::from_secs(60), || async { (knob_attempts(&knob, &blocked, A) >= 4).then_some(()) }).await;
    rdm_test::write_pre_node_ready_knob(&estate.root, &blocked, B).unwrap();
    let (before_b, (step_b, reason_b, _)) = next_pending(&mut stream).await;
    assert_eq!(step_b, "WaitForNodeReady");
    assert!(reason_b.contains(B) && !reason_b.contains(A), "the next Pending frame is the changed reason, never A again: {reason_b}");
    assert!(!before_b.iter().any(|f| matches!(f, BuildFrame::Step { step, .. } if step == "WaitForNodeReady")), "the step is still in flight: {before_b:?}");

    wait_for("an attempt pending for B", Duration::from_secs(60), || async { (knob_attempts(&knob, &blocked, B) >= 2).then_some(()) }).await;
    rdm_test::write_pre_node_ready_knob(&estate.root, &blocked, "pass").unwrap();
    let rest = until_complete(&mut stream).await;
    assert!(!rest.iter().any(birth_pending), "nothing of the node is framed Pending once the hook passed: {rest:?}");
    assert!(rest.iter().any(|f| matches!(f, BuildFrame::Step { step, .. } if step == "WaitForNodeReady")), "WaitForNodeReady completed: {rest:?}");
    let a_reruns = knob_attempts(&knob, &blocked, A);

    estate.stop().await;
    let spans = estate.spans();
    // The flame: the pending node's probe answers, each naming the hook's reason.
    // The node's own probe answers name it by node id.
    let probes: Vec<&Value> = spans_named(&spans, "rdm.node_admin.status.update.via-probe").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == blocked_id && s(&sp["attributes"]["state"]) == "pending").collect();
    assert!(probes.iter().any(|sp| s(&sp["attributes"]["reason"]).contains(A)) && probes.iter().any(|sp| s(&sp["attributes"]["reason"]).contains(B)), "the node answered both reasons Pending");
    estate.artifact("pending-probes", &json!({ "a_reruns": a_reruns, "pending_probe_answers": probes.len() }));
    estate.record_trace_url(probes[0]["trace_id"].as_str().unwrap_or(""));
}

const SHAPE1: [(&str, u32, u32); 1] = [("mesh1", 2, 1)];

/// CONTRACT (fabric-node-hooks.md "Outcomes and the gate": "`Failed { reason }`, or the attempts spent |
/// the start ends `Failed` with the hook step and the last reason, back to whoever started the node";
/// "Where the failure lands": "a Build (Create) | the step's `rdm.build.step` event, `failed { reason }`;
/// its mesh's section ends, and the Build ends `incomplete` (Q346)"; the executor "ends its
/// `WaitForNodeReady` step failed from its Status `StartFailed { step: node.pre-node-ready, reason }`
/// reply: the node stays `Pending` and its process keeps running"): a Build grows mesh1 by one node
/// whose hook answers `Pending` on every attempt of a three-attempt `Send`. The hook is called three
/// times, its start ends Failed with the last reason, the node's probe answers `StartFailed`, the
/// create's `WaitForNodeReady` ends failed with that reason, mesh1's section ends there and the Build
/// ends `incomplete` naming it. The node stays Pending, its process running, and nothing calls its
/// hook again. Must NOT happen: a fourth call, the Build `complete` or `failed`, the step waiting out
/// its own bound, or the node's process ending.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_created_node_whose_hook_spends_its_attempts_fails_its_step_and_the_build_ends_incomplete() {
    let test = "a_created_node_whose_hook_spends_its_attempts_fails_its_step_and_the_build_ends_incomplete";
    const SHAPE1: [(&str, u32, u32); 1] = [("mesh1", 2, 1)];
    const A: &str = "the rows of orders are not yet published";
    let held = rdm_core::PathName::new("mesh1.node", "2").unwrap();
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let caller = Caller::of(&estate).await;
    let (carrier, _) = born_framed(&estate, &caller, &SHAPE1).await;

    // The next node's hook answers Pending for A on each of three attempts.
    let knob = rdm_test::pre_node_ready_knob_dir(&estate.root);
    rdm_test::write_pre_node_ready_knob(&estate.root, &held, A).unwrap();
    rdm_test::write_pre_node_ready_knob_attempts(&estate.root, &held, 3).unwrap();
    let grown = FabricDesired { fabric: FABRIC.into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 2), ("node", 2)])] };
    let submitted = std::time::Instant::now();
    let mut stream = carrier.create(estate.shape_change_at(&carrier, &grown).await).await.expect("the fabric-primary accepts the grown shape");
    let build_id = stream.build_id().clone();
    let mut frames = Vec::new();
    let sections = loop {
        let frame = tokio::time::timeout(Duration::from_secs(120), stream.next()).await.expect("a frame within the bound").expect("the stream goes on").expect("the stream does not break");
        frames.push(frame.clone());
        match frame {
            BuildFrame::Incomplete { sections } => break sections,
            BuildFrame::Complete { .. } | BuildFrame::Failed { .. } => panic!("the Build ran and a step failed: it ends incomplete, never {frame:?}; frames {frames:?}"),
            _ => {}
        }
    };
    let took = submitted.elapsed();
    let section = sections.iter().find(|u| u.mesh == "mesh1").unwrap_or_else(|| panic!("the end names mesh1's section: {sections:?}"));
    assert_eq!(section.step, "WaitForNodeReady", "mesh1's section ended at the create's ready wait: {section:?}");
    assert!(section.reason.contains(A) && section.reason.contains("node.pre-node-ready") && section.reason.contains("3 attempts"), "the step failed with the hook's last reason and step: {section:?}");
    assert!(took < Duration::from_secs(90), "the step ended on the StartFailed answer, not on a bound: {took:?}");
    assert!(frames.iter().any(|f| matches!(f, BuildFrame::Pending { step, reason, .. } if step == "WaitForNodeReady" && reason.contains(A))), "the node's Pending was framed before its start ended Failed: {frames:?}");
    let ended = crate::build_log::build_fact(&estate, build_id.as_str(), "the Build ended short of complete", Duration::from_secs(60), |p| p.state.ended_short()).await.unwrap_or_else(|end| panic!("{end}"));
    assert_eq!(ended.state, rdm_admin::build_state::BuildState::Incomplete, "the Build ran, so it is incomplete, never failed: {ended:#?}");
    assert!(ended.steps.iter().any(|st| st.mesh == "mesh1" && st.step == "WaitForNodeReady" && matches!(st.outcome, StepOutcome::Failed { .. })), "the step's rdm.build.step event ended failed: {:?}", ended.steps);

    // The node stays Pending, its process running, and nothing calls its hook again.
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(knob_attempts(&knob, &held, A), 3, "three attempts and no fourth");
    let view = estate.node_views().await.into_iter().find(|n| n.path == held.path && n.name == held.name).unwrap_or_else(|| panic!("{held:?} is still in the view"));
    assert_eq!(view.state, NodeStatus::Pending, "the node stays Pending: {view:?}");

    estate.stop().await;
    let spans = estate.spans();
    let failed = spans_named(&spans, "rdm.mesh.node.reject.via-start-step-failed").into_iter().find(|sp| at_node(sp, &held) && s(&sp["attributes"]["step"]) == "node.pre-node-ready").cloned().unwrap_or_else(|| panic!("{held:?}'s start named its failed step"));
    let answered = spans_named(&spans, "rdm.node_admin.status.update.via-probe").into_iter().filter(|sp| s(&sp["attributes"]["state"]) == "start-failed" && s(&sp["attributes"]["reason"]).contains(A)).count();
    assert!(answered >= 1, "the node's probe answered StartFailed with the hook's reason");
    let step = spans_named(&spans, "rdm.node_admin.build.update.via-step-event").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == build_id.as_str() && s(&sp["attributes"]["step"]) == "WaitForNodeReady").cloned();
    println!("flame (the start ended Failed at its hook): http://192.168.68.99:16687/trace/{}", s(&failed["trace_id"]));
    if let Some(step) = &step {
        println!("flame (WaitForNodeReady failed, the Build incomplete): http://192.168.68.99:16687/trace/{}", s(&step["trace_id"]));
    }
    estate.record_trace_url(step.as_ref().map(|sp| sp["trace_id"].as_str().unwrap_or("")).unwrap_or(""));
}

/// CONTRACT (Q30): a launch whose executable hash outlasts the QUIC idle timeout keeps every open
/// connection of its admin alive. `rpc_node` is bound to an executable that `exec`s the node and
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
    let real = set.bindings.iter().find(|b| b.launch_id == "node").expect("the node is bound").executable.clone();
    let dir = std::env::temp_dir().join(format!("q30-bound-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir_all(&dir).unwrap();
    let wrapper = dir.join("testkit-node-padded");
    std::fs::write(&wrapper, format!("#!/bin/sh\nexec \"{}\" \"$@\"\n", real.display())).unwrap();
    std::fs::OpenOptions::new().write(true).open(&wrapper).unwrap().set_len(8 << 30).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let sha256 = rdm_core::binding::sha256_file(&wrapper).unwrap();
    for b in set.bindings.iter_mut().filter(|b| b.launch_id == "node") {
        b.executable = wrapper.clone();
        b.sha256 = sha256.clone();
    }
    // One worker: a hash held on it holds every task of the admin, its endpoint's included, so the
    // cell fails whenever the hash runs on a worker, never only when a woken task happens to sit in
    // the hashing worker's LIFO slot (with ~20 workers the defect needs that; R24's gate had it).
    let mut estate = Estate::bootstrap_on_with_env(owner(test), FABRIC, "mesh1", &set, &sha, &["node"], &[("TOKIO_WORKER_THREADS", "1")])
        .await
        .expect("the padded binding set is accepted");
    let caller = Caller::of(&estate).await;
    born(&estate, &caller, &[("mesh1", 1, 1)]).await;
    estate.stop().await;
    let _ = std::fs::remove_dir_all(&dir);

    let spans = estate.spans();
    let floor_ms = rdm_core::cadence::staleness_floor().as_millis() as u64;
    let hashes: Vec<&Value> = spans_named(&spans, "rdm.node_admin.deployment.resolve.via-executable-hash").into_iter().filter(|sp| s(&sp["attributes"]["launch_id"]) == "node").collect();
    assert!(!hashes.is_empty(), "the node's launch re-hashed its executable");
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

