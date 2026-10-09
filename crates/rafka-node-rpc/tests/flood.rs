//! A sustained flood of concurrent calls over real Iroh streams: the floor it must meet, and what
//! it does at the in-flight limit. One bi-directional stream carries each call.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::admission::Limits;
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, ServerStats, StaticResolver};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::{ReplyKind, RpcOutcome};
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The flood's width: concurrent callers, each with one call in flight at a time.
const CALLERS: usize = 32;
/// The floor: round trips the flood must complete in its window.
const FLOOR_ROUND_TRIPS: u64 = 200;
const WINDOW: Duration = Duration::from_secs(10);

struct Rig {
    _router: Router,
    stats: Arc<ServerStats>,
    handled: Arc<AtomicU64>,
    client: Arc<NodeRpcClient>,
    target: NodeTarget,
    release: tokio::sync::watch::Sender<bool>,
}

async fn rig(limits: Option<Limits>) -> Rig {
    let handled = Arc::new(AtomicU64::new(0));
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let h = handled.clone();
    let (release, released) = tokio::sync::watch::channel(false);
    let mut b = ServerBuilder::new().serve::<Ping, _, _>(OpOwner::Core, move |_peer, req: PingRequest| {
        let h = h.clone();
        let mut released = released.clone();
        async move {
            h.fetch_add(1, Ordering::SeqCst);
            let PingRequest::Ping { payload, .. } = req;
            if payload == b"hold" {
                let _ = released.wait_for(|r| *r).await;
            }
            Ok(PingReply::Pong { payload })
        }
    });
    if let Some(l) = limits {
        b = b.limits(Ping::OP, l);
    }
    let server = b.seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() }).unwrap();
    let stats = server.stats();
    let key = SecretKey::generate();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode { node_id: node_id.clone(), name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation });
    let cep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    Rig { _router: router, stats, handled, client: Arc::new(NodeRpcClient::new(cep, resolver)), target: NodeTarget::ExactNode(node_id), release }
}

fn ping(p: &[u8]) -> PingRequest {
    PingRequest::Ping { payload: p.to_vec() }
}

/// CONTRACT: 32 concurrent callers each send 1 KiB calls back to back for 10 s over one client
/// endpoint. Every call is answered `Pong` with its own payload (no error, no Busy), at least 200
/// complete, the server dispatched exactly the calls the callers completed, and nothing is left in
/// flight. What must NOT happen: a stalled stream, a refused call below the limit, or a handler run
/// that no caller saw answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_flood_of_32_callers_for_10_seconds_round_trips_without_error() {
    let r = Arc::new(rig(None).await);
    let deadline = tokio::time::Instant::now() + WINDOW;
    let (total, errors) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let mut workers = Vec::new();
    for _ in 0..CALLERS {
        let (r, total, errors) = (r.clone(), total.clone(), errors.clone());
        workers.push(tokio::spawn(async move {
            let payload = vec![0xAB_u8; 1024];
            while tokio::time::Instant::now() < deadline {
                match r.client.call::<Ping>(&r.target, &ping(&payload), &CallOptions::default()).await.0 {
                    RpcOutcome::Reply(rep) if matches!(rep.value(), PingReply::Pong { payload: p } if *p == payload) => {
                        total.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    for w in workers {
        w.await.unwrap();
    }
    let (done, failed) = (total.load(Ordering::Relaxed), errors.load(Ordering::Relaxed));
    eprintln!("flood: round_trips={done} errors={failed} callers={CALLERS} window={WINDOW:?}");
    assert_eq!(failed, 0, "a call below the in-flight limit was not answered with its own echo");
    assert!(done >= FLOOR_ROUND_TRIPS, "sustained throughput below the floor: {done} round trips in {WINDOW:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), done, "the server ran exactly the calls the callers saw answered");
    assert_eq!(ServerStats::get(&r.stats.busy), 0, "no call was refused Busy below the limit");
    assert_eq!(ServerStats::get(&r.stats.in_flight), 0, "nothing is left in flight");
}

/// CONTRACT: with the op bounded to 4 in flight, 32 callers at once get exactly 4 admitted (held in
/// the handler) and 28 immediate typed `Busy` replies whose handler never ran (the server's
/// `rdm.node_rpc.request.reject.via-busy` refusal, counted in `stats.busy`); released, the 4 are
/// answered, no permit is left held, and the next call is served. What must NOT happen: a queued
/// call, a handler run for a refused call, or a permit that outlives its call.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_flood_over_the_in_flight_limit_is_refused_busy_and_the_handler_never_runs_for_the_refused() {
    const LIMIT: usize = 4;
    let r = Arc::new(rig(Some(Limits { node_wide: LIMIT, per_caller: LIMIT })).await);
    let mut calls = Vec::new();
    for _ in 0..CALLERS {
        let r = r.clone();
        calls.push(tokio::spawn(async move {
            let opts = CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(10), reply: Duration::from_secs(10) }, ..Default::default() };
            r.client.call::<Ping>(&r.target, &ping(b"hold"), &opts).await.0
        }));
    }
    let until = tokio::time::Instant::now() + Duration::from_secs(10);
    while (r.handled.load(Ordering::SeqCst) < LIMIT as u64 || ServerStats::get(&r.stats.busy) < (CALLERS - LIMIT) as u64) && tokio::time::Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(r.handled.load(Ordering::SeqCst), LIMIT as u64, "only the admitted calls entered the handler");
    assert_eq!(ServerStats::get(&r.stats.busy), (CALLERS - LIMIT) as u64, "every other call was refused Busy");
    assert_eq!(ServerStats::get(&r.stats.in_flight), LIMIT as u64, "the admitted calls are the ones in flight");
    r.release.send(true).unwrap();
    let (mut busy, mut pong) = (0, 0);
    for c in calls {
        match c.await.unwrap() {
            RpcOutcome::Reply(rep) if rep.class() == ReplyKind::Busy => busy += 1,
            RpcOutcome::Reply(rep) if matches!(rep.value(), PingReply::Pong { .. }) => pong += 1,
            other => panic!("a flooded call is answered Busy or Pong, not {}", other.name()),
        }
    }
    assert_eq!((busy, pong), (CALLERS - LIMIT, LIMIT), "28 refused, 4 served");
    assert_eq!(r.handled.load(Ordering::SeqCst), LIMIT as u64, "no refused call ran");
    assert_eq!(ServerStats::get(&r.stats.in_flight), 0, "no permit outlives its call");
    let (after, _) = r.client.call::<Ping>(&r.target, &ping(b"after"), &CallOptions::default()).await;
    assert!(matches!(after, RpcOutcome::Reply(ref rep) if matches!(rep.value(), PingReply::Pong { .. })), "the next call is served: {after:?}");
}
