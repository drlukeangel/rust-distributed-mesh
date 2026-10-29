//! `build.get` (Q167 R-RD5; node-rpc-envelope.md "Build, op `0x20`": `GetBuilds`), over real Node RPC:
//! the fabric-primary's Build log answers every Build whose header matches the filter, newest first,
//! each whole (its header, its topology, its receipts), the current one selected once and marked, and
//! only `End` completes the read.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_admin::accepted::FabricTopology;
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{
    AttemptOutcome, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, BuildStepReceipt, MemoryBuildStateAdapter, StepOutcome,
};
use rdm_admin::model::{MeshId, PathName};
use rdm_client::{BuildCarrier, CallEnd, Filter};
use rdm_core::build::{Build, BuildPhase, BuildReply, BuildRequest, StepResult};
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::stream::StreamItem;
use rdm_transport::{NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// A topology of mesh1's admin and `rpcs` rpc nodes, each carrying a config of 256 bytes, so the
/// topology takes more than one reply frame's chunk.
fn topology(rpcs: u32) -> FabricTopology {
    let mut t = FabricTopology::root("fabric1", "mesh1", MeshId::parse("meshd0000001").unwrap());
    let mesh = t.meshes.get_mut("mesh1").unwrap();
    for i in 1..=rpcs {
        let path: PathName = format!("mesh1.rpc.{i}").parse().unwrap();
        let mut spec = rdm_core::meta::NodeSpec::default_for(path.clone());
        spec.config.insert("note".into(), "n".repeat(256));
        spec.tags.insert(format!("t{}", i % 7));
        mesh.nodes.insert(path.clone());
        mesh.spec_by_path.insert(path, spec);
    }
    t
}

/// Build `id`, accepted at `at` over `t`; its attempt 1 claimed by mesh1.admin.1, `receipts` written,
/// and its verdict.
async fn build(log: &MemoryBuildStateAdapter, id: &BuildId, at: u64, t: FabricTopology, receipts: &[(&str, StepOutcome)], verdict: AttemptOutcome) {
    log.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: t, submitted_change: None, submitted_at_ms: at }).await.unwrap();
    log.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    for (operation, outcome) in receipts {
        let receipt = BuildStepReceipt { build_id: id.clone(), attempt: 1, operation: operation.to_string(), step: "Complete".into(), outcome: outcome.clone(), output: None, executor: Some("mesh1.admin.1".into()), mesh: None };
        log.append_step_receipt(&receipt).await.unwrap();
    }
    log.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt: 1, outcome: verdict }).await.unwrap();
}

/// The fabric-primary on the wire: `build.get` served from `log`, at `mesh1.admin.1`.
async fn fabric_primary(log: Arc<dyn BuildStateAdapter>) -> (iroh::Endpoint, ResolvedNode, Router) {
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let served = me.clone();
    let server = ServerBuilder::new()
        .serve_stream::<Build, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_peer, req, sink| {
            let (log, me) = (log.clone(), served.clone());
            async move {
                match req {
                    BuildRequest::GetBuilds { filter } => rdm_admin::build_op::serve_builds(&me, &*log, filter, sink).await,
                    other => Ok(rdm_core::reply::build::rejected("unserved".into(), format!("this cell serves build.get only, asked {}", other.op()))),
                }
            }
        })
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .expect("the catalog is built");
    let router = Router::builder(ep.clone()).accept(rdm_transport::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    (ep.clone(), ResolvedNode { node_id, name: me, endpoint_id: ep.id(), transport_addr: addr, incarnation }, router)
}

async fn caller(fp: &ResolvedNode) -> (Arc<NodeRpcClient>, iroh::Endpoint) {
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(fp.clone());
    (Arc::new(NodeRpcClient::new(ep.clone(), resolver).with_caller_system("rdm")), ep)
}

fn ids(builds: &[rdm_client::Build]) -> Vec<String> {
    builds.iter().map(|b| b.build_id.to_string()).collect()
}

// @feature: fabric-build
/// CONTRACT (Q167 R-RD5; node-rpc-envelope.md "Build, op `0x20`", `build.get`): a fabric-primary
/// whose Build log holds three Builds, b1 (complete), b2 (complete, accepted after b1, so the
/// current Build) and b3 (accepted last, failed part-way: one step Complete, the next Failed), answers
/// `build.get []` with all three, newest first (b3, b2, b1), each whole: its header (phase, attempt,
/// executor, reason, last failure) as its log folds it, its topology (every node's create spec,
/// a topology of 301 nodes taking several Topology chunks), and its receipts; only b2 is marked
/// current. `[current: true]` streams b2 alone, `[phase: failed]` b3 alone, `[id: b1]` b1 alone, an
/// id the log does not hold the empty list. A pair naming no property of the Build header (`zone`) is
/// refused INVALID_ARGUMENT (`RejectedFilter`) naming the pair. The stream, read on the wire, opens
/// with Started naming the current Build, and only `End` closes it. Must NOT happen: the failed
/// Build marked current or left out, a Build answered without its topology or receipts, an unknown
/// property read as matching nothing.
#[test]
fn builds_get_answers_every_matching_build_whole_newest_first_the_current_one_marked() {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap().block_on(async {
        let log = Arc::new(MemoryBuildStateAdapter::new());
        let (b1, b2, b3) = (BuildId::mint(), BuildId::mint(), BuildId::mint());
        build(&log, &b1, 1, topology(1), &[("node.create:mesh1.rpc.1", StepOutcome::Complete)], AttemptOutcome::Converged).await;
        build(&log, &b2, 2, topology(300), &[("node.create:mesh1.rpc.2", StepOutcome::Complete)], AttemptOutcome::Converged).await;
        let failed = StepOutcome::Failed { reason: "the launch of mesh1.rpc.4 exited 1".into(), code: rdm_core::outcome::Code::FailedPrecondition };
        build(&log, &b3, 3, topology(4), &[("node.create:mesh1.rpc.3", StepOutcome::Complete), ("node.create:mesh1.rpc.4", failed)], AttemptOutcome::Failed { reason: "node.create:mesh1.rpc.4 failed".into(), code: rdm_core::outcome::Code::FailedPrecondition }).await;
        assert_eq!(log.current_build().await.unwrap().into_build_id(), Some(b2.clone()), "the setup: b2 is the current Build, the failed b3 skipped");

        let (ep, fp, _router) = fabric_primary(log.clone()).await;
        let (client, caller_ep) = caller(&fp).await;
        let carrier = BuildCarrier::new(client.clone(), fp.name.clone());
        let read = |f: Filter| {
            let carrier = carrier.clone();
            async move { tokio::time::timeout(Duration::from_secs(30), carrier.builds(&f)).await.unwrap_or_else(|_| panic!("build.get {f:?} answered nothing in 30 s")) }
        };

        // No pair: every Build, newest first, each whole, only the current one marked.
        let every = read(Filter::new()).await.unwrap_or_else(|e| panic!("build.get []: {e}"));
        assert_eq!(ids(&every), vec![b3.to_string(), b2.to_string(), b1.to_string()], "every Build, newest first");
        assert_eq!(every.iter().map(|b| b.current).collect::<Vec<_>>(), vec![false, true, false], "only b2, the current Build, is marked");
        for got in &every {
            let held = log.read_build(&got.build_id).await.unwrap();
            let phase = match held.state {
                rdm_admin::build_state::BuildState::Pending => BuildPhase::Pending,
                rdm_admin::build_state::BuildState::Running => BuildPhase::Running,
                rdm_admin::build_state::BuildState::Complete => BuildPhase::Complete,
                rdm_admin::build_state::BuildState::Failed => BuildPhase::Failed,
            };
            assert_eq!((got.phase, got.attempt, got.executor.clone(), got.reason.clone(), got.last_failure.clone()), (phase, held.attempt, held.executor.clone(), held.reason.as_str().to_string(), held.last_failure.clone()), "{}'s header is its log's", got.build_id);
            let specs: Vec<rdm_core::meta::NodeSpec> = held.topology.meshes.values().flat_map(|m| m.spec_by_path.values().cloned()).collect();
            assert_eq!(got.nodes, specs, "{}'s topology is every node's create spec, whole", got.build_id);
            let receipts: Vec<(String, bool)> = got.steps.iter().map(|s| (s.operation.clone(), s.result == StepResult::Complete)).collect();
            let held_receipts: Vec<(String, bool)> = held.steps.iter().map(|s| (s.operation.clone(), s.outcome == StepOutcome::Complete)).collect();
            assert_eq!(receipts, held_receipts, "{}'s receipts are its log's", got.build_id);
        }
        assert_eq!(every[1].nodes.len(), 301, "b2's topology: mesh1.admin.1 and 300 rpc nodes");
        assert_eq!((every[0].phase, every[0].steps.len()), (BuildPhase::Failed, 2), "b3 failed part-way, both its receipts read: {:?}", every[0]);

        // One pair each: the current Build, the failed one, one by id, and an id not held.
        assert_eq!(ids(&read(Filter::new().eq("current", ["true"])).await.unwrap()), vec![b2.to_string()], "[current: true] is the current Build alone");
        assert_eq!(ids(&read(Filter::new().eq("phase", ["failed"])).await.unwrap()), vec![b3.to_string()], "[phase: failed]");
        assert_eq!(ids(&read(Filter::new().eq("id", [b1.to_string()])).await.unwrap()), vec![b1.to_string()], "[id: b1]");
        assert_eq!(ids(&read(Filter::new().eq("phase", ["complete"]).eq("current", ["false"])).await.unwrap()), vec![b1.to_string()], "pairs AND");
        assert!(read(Filter::new().eq("id", [BuildId::mint().to_string()])).await.unwrap().is_empty(), "an id the log does not hold is the empty list");

        // An unknown property is INVALID_ARGUMENT naming the pair, through the carrier and on the wire.
        match read(Filter::new().eq("zone", ["eu"])).await {
            Err(CallEnd::Rejected { reason, detail }) => assert!(reason == "rejected-filter" && detail.contains("the pair (zone, [eu])"), "refused naming the pair: {reason}: {detail}"),
            other => panic!("an unknown property is refused by name, not {other:?}"),
        }
        let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Overall(Duration::from_secs(10)), ..Default::default() };
        let refused = client.call_stream::<Build>(&NodeTarget::CurrentPath(fp.name.clone()), &BuildRequest::GetBuilds { filter: Filter::new().eq("zone", ["eu"]) }, &opts).await;
        let first = match refused {
            Err((rdm_core::outcome::RpcOutcome::Reply(r), _)) => r.into_value(),
            Err((other, _)) => panic!("the refusal reached the caller: {other:?}"),
            Ok((s, _)) => match s.read_only().next().await {
                Some(StreamItem::Frame(_, f)) => f,
                other => panic!("a refusal frame: {other:?}"),
            },
        };
        match first {
            BuildReply::RejectedFilter { reason } => assert!(reason.contains("build.get: the pair (zone, [eu])"), "{reason}"),
            other => panic!("RejectedFilter, not {other:?}"),
        }
        assert_eq!(<Build as rdm_core::protocol::NodeProtocol>::classify_reply(&BuildReply::RejectedFilter { reason: String::new() }).code(), rdm_core::outcome::Code::InvalidArgument);

        // On the wire: Started names the current Build, then data frames, then End alone closes it.
        let (stream, _) = client.call_stream::<Build>(&NodeTarget::CurrentPath(fp.name.clone()), &BuildRequest::GetBuilds { filter: Filter::new() }, &opts).await.unwrap_or_else(|(o, _)| panic!("the read streamed: {o:?}"));
        let mut stream = stream.read_only();
        let mut frames = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                StreamItem::Frame(_, f) => frames.push(f),
                StreamItem::Failed(f) => panic!("the stream failed before its End: {f:?}"),
            }
        }
        match frames.first() {
            Some(BuildReply::Started { build_id, attempt, .. }) => assert_eq!((build_id.as_str(), *attempt), (b2.to_string().as_str(), 1), "Started names the current Build"),
            other => panic!("the stream opens with Started: {other:?}"),
        }
        assert!(matches!(frames.last(), Some(BuildReply::End { builds: 3 })), "End closes the read, counting three Builds: {:?}", frames.last());
        let topology_chunks = frames.iter().filter(|f| matches!(f, BuildReply::Topology { build_id, .. } if *build_id == b2.to_string())).count();
        assert!(topology_chunks >= 2, "b2's topology takes more than one chunk: {topology_chunks}");
        let headers: Vec<&BuildReply> = frames.iter().filter(|f| matches!(f, BuildReply::Build { .. })).collect();
        assert_eq!(headers.len(), 3, "one header closes each Build");

        for e in [ep, caller_ep] {
            e.close().await;
        }
    })
}
