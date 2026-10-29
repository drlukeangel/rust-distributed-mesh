//! `rdm-rpc-probe`: one proof-store call over real Node RPC, for scenarios
//! (PRD §6: no in-process shortcut).
//!
//! ```text
//! rdm-rpc-probe --admin-birth <birth> get    --target exact:<node_id>|path:<path.name> --key <k>
//! rdm-rpc-probe --admin-birth <birth> put    --target ... --key <k> --value <v>
//! rdm-rpc-probe --admin-birth <birth> delete --target ... --key <k>
//! rdm-rpc-probe --admin-birth <birth> cas    --target ... --key <k> [--expected <v>] [--value <v>]
//! rdm-rpc-probe --admin-birth <birth> topology --target ... [--mesh <name>] [--since <node>|<node_id>|<incarnation>|<version>]
//! rdm-rpc-probe --admin-birth <birth> fabric-delete --target path:<the entry admin>
//! rdm-rpc-probe --admin-birth <birth> node-delete   --target path:<path.name>
//! ```
//!
//! `<birth>` is the entry node-admin's birth, the JSON it prints on its birth line
//! (`rdm_client::BIRTH_LINE`). The probe reads that admin's view over Node RPC
//! (`fabric.get`, then `mesh.nodes.get` for each mesh the fabric lists) and resolves the target from it:
//! `exact:` is exactly that logical node and never follows a replacement;
//! `path:` is whoever holds the path now. `cas` without `--expected` expects
//! the key absent; without `--value` it deletes. `fabric-delete` submits fabric.delete at the entry
//! admin and reads its stream to the terminal; `node-delete` submits node.delete of the target's path
//! and reads its stream to its last reply (fabric-delete.md step 4: the fabric-primary's own
//! node.delete, called from outside the fabric, whose node.deleted is its last reply): the demo
//! estate's teardown. It prints one JSON line:
//! `{"outcome": "Reply"|"NotSent"|"Unserved"|"Indeterminate", ...}`; a
//! `Reply` carries the executing node's provenance and the typed result.

use rdm_core::{IncarnationId, MeshNode, NodeId, PathName};
use rdm_client::{BuildCarrier, Fabrics, NodeView, Nodes};
use rdm_transport::{CallOptions, NodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rdm_core::outcome::RpcOutcome;
use rdm_test::proof_store::{ProofReply, ProofRequest, ProofStore};
use serde_json::{json, Value};
use std::sync::Arc;

struct Args {
    /// The entry node-admin's birth: the probe reads that admin's view.
    admin_birth: MeshNode,
    op: String,
    target: String,
    key: String,
    value: Option<String>,
    expected: Option<String>,
    /// Carry the call through this node (`path:<path.name>` or `exact:<node_id>`) to the exact
    /// target: the `ViaPeer` route, executed as the composition seam does.
    via: Option<String>,
    /// Execute the `NoActiveRoute` choice: the seam sends nothing and answers by name.
    no_route: bool,
    /// Failpoint: write half the request, then reset it with 499 (`NotSent(FrameNotSent)`).
    cut_before_finish: bool,
    /// `--budget-ms`: the call's overall budget, when it is not the default.
    budget_ms: Option<u64>,
    /// `declare`: the authority the node declares to (`path:`/`exact:`), the state, and the
    /// testkit-only overrides.
    to: Option<String>,
    state: Option<String>,
    as_node_id: Option<String>,
    as_incarnation: Option<String>,
    /// `originate`: the destination path the target node calls over its own held projection.
    destination: Option<String>,
    /// `fault`: how many index writes and history appends the target node refuses next, or
    /// `--release`.
    refuse_index: u32,
    refuse_history: u32,
    pass_history: u32,
    release: bool,
    /// `record-proxy`: the carrier path, and how many Direct Failed observations precede the Proxy.
    carrier: Option<String>,
    failed_attempts: u32,
    /// `topology`: the one mesh asked for, and the source version already held for it.
    mesh: Option<String>,
    since: Option<String>,
}

fn parse(mut it: impl Iterator<Item = String>) -> Result<Args, String> {
    let (mut admin_birth, mut op, mut target, mut key, mut value, mut expected, mut via) = (None, None, None, None, None, None, None);
    let mut no_route = false;
    let mut cut_before_finish = false;
    let mut budget_ms = None;
    let (mut to, mut state, mut as_node_id, mut as_incarnation) = (None, None, None, None);
    let (mut destination, mut refuse_index, mut refuse_history, mut release) = (None, 0u32, 0u32, false);
    let mut pass_history = 0u32;
    let (mut carrier, mut failed_attempts) = (None, 0u32);
    let (mut mesh, mut since) = (None, None);
    while let Some(a) = it.next() {
        let mut take = |name: &str| it.next().ok_or_else(|| format!("{name} needs a value"));
        match a.as_str() {
            "--admin-birth" => admin_birth = Some(take("--admin-birth")?),
            "--target" => target = Some(take("--target")?),
            "--key" => key = Some(take("--key")?),
            "--value" => value = Some(take("--value")?),
            "--expected" => expected = Some(take("--expected")?),
            "--query" => key = Some(take("--query")?),
            "--via" => via = Some(take("--via")?),
            "--no-route" => no_route = true,
            "--cut-before-finish" => cut_before_finish = true,
            "--budget-ms" => budget_ms = Some(take("--budget-ms")?.parse().map_err(|e| format!("--budget-ms: {e}"))?),
            "--to" => to = Some(take("--to")?),
            "--state" => state = Some(take("--state")?),
            "--as-node-id" => as_node_id = Some(take("--as-node-id")?),
            "--as-incarnation" => as_incarnation = Some(take("--as-incarnation")?),
            "--destination" => destination = Some(take("--destination")?),
            "--refuse-index" => refuse_index = take("--refuse-index")?.parse().map_err(|e| format!("--refuse-index: {e}"))?,
            "--refuse-history" => refuse_history = take("--refuse-history")?.parse().map_err(|e| format!("--refuse-history: {e}"))?,
            "--pass-history" => pass_history = take("--pass-history")?.parse().map_err(|e| format!("--pass-history: {e}"))?,
            "--release" => release = true,
            "--mesh" => mesh = Some(take("--mesh")?),
            "--since" => since = Some(take("--since")?),
            "--carrier" => carrier = Some(take("--carrier")?),
            "--failed-attempts" => failed_attempts = take("--failed-attempts")?.parse().map_err(|e| format!("--failed-attempts: {e}"))?,
            "get" | "put" | "delete" | "cas" | "resolve" | "declare" | "originate" | "fault" | "snapshot" | "record-proxy" | "dial" | "stream" | "block-direct" | "stop-transport" | "topology" | "nodes" | "fabric-delete" | "node-delete" | "own-config" if op.is_none() => op = Some(a),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    let is_declare = matches!(op.as_deref(), Some("declare" | "topology" | "nodes" | "stream" | "fabric-delete" | "node-delete" | "own-config"));
    let admin_birth = admin_birth.ok_or("--admin-birth is required")?;
    let admin_birth: MeshNode = serde_json::from_str(&admin_birth).map_err(|e| format!("--admin-birth {admin_birth:?} is not a node-admin's birth: {e}"))?;
    Ok(Args {
        admin_birth,
        op: op.ok_or("an op (get, put, delete, cas, resolve, declare) is required")?,
        target: target.ok_or("--target is required")?,
        key: if is_declare { key.unwrap_or_default() } else { key.ok_or("--key is required")? },
        value,
        expected,
        via,
        no_route,
        cut_before_finish,
        budget_ms,
        to,
        state,
        as_node_id,
        as_incarnation,
        destination,
        refuse_index,
        refuse_history,
        pass_history,
        release,
        carrier,
        failed_attempts,
        mesh,
        since,
    })
}

/// `declare`: ask the node at `--target` to declare `--state` for its own birth to `--to`.
async fn run_declare(a: &Args, target: &NodeTarget) -> Result<Value, String> {
    use rdm_core::status::NodeState;
    use rdm_test::declare_probe::{DeclareProbe, DeclareReply, DeclareRequest, DeclareTarget};
    let to = a.to.as_deref().ok_or("declare needs --to <path:..|exact:..>")?;
    let to = match to.split_once(':') {
        Some(("exact", id)) => DeclareTarget::Exact(id.to_string()),
        Some(("path", p)) => DeclareTarget::Path(p.to_string()),
        _ => return Err(format!("--to {to:?} is neither exact:<node_id> nor path:<path.name>")),
    };
    let state = match a.state.as_deref().ok_or("declare needs --state")? {
        "pending" => NodeState::Pending,
        "ready" => NodeState::Ready,
        "connections-deleting" => NodeState::ConnectionsDeleting,
        "stopped" => NodeState::Stopped,
        other => return Err(format!("--state {other:?} is not a node state")),
    };
    let (client, _) = enter(&a.admin_birth).await?;
    let req = DeclareRequest::Declare { to, state, node_id: a.as_node_id.clone(), incarnation: a.as_incarnation.clone() };
    let (out, _) = client.call::<DeclareProbe>(target, &req, &CallOptions::default()).await;
    // The probe's one socket is closed before the process ends: an endpoint dropped open aborts the
    // process ungracefully and its span file is lost (a call that never left the resolver is the case).
    client.endpoint().close().await;
    Ok(match &out {
        RpcOutcome::Reply(r) => match r.value() {
            DeclareReply::Answered { outcome, reply, reason } => json!({
                "outcome": "Reply",
                "declared": {
                    "outcome": outcome,
                    "reply": reply.as_ref().map(|s| s.name()),
                    "detail": reply.as_ref().map(|s| format!("{s:?}")),
                    "reason": reason,
                }
            }),
            other => json!({"outcome": "Reply", "refused": format!("{other:?}")}),
        },
        RpcOutcome::NotSent(n) => json!({"outcome": out.name(), "reason": format!("{:?}", n.reason())}),
        other => json!({"outcome": other.name()}),
    })
}

fn target(spec: &str) -> Result<NodeTarget, String> {
    match spec.split_once(':') {
        Some(("exact", id)) => NodeId::parse(id).map(NodeTarget::ExactNode).map_err(|e| format!("exact target {id:?}: {e}")),
        Some(("path", p)) => p.parse::<PathName>().map(NodeTarget::CurrentPath).map_err(|e| format!("path target {p:?}: {e}")),
        _ => Err(format!("target {spec:?} is neither exact:<node_id> nor path:<path.name>")),
    }
}

/// The probe's one client, entered at the node-admin `admin`: the endpoint is bound, the admin's birth
/// is put in the resolver, and then every node the admin's view holds that can be dialled
/// ([`views_of`], [`resolved`]). A birth whose endpoint is no Iroh key is refused by name before
/// anything is bound. A read that fails, or a view node with a value its field does not read, refuses
/// the call by name, and the endpoint is closed first: an endpoint dropped open aborts the process
/// ungracefully and its span file is lost.
async fn enter(admin: &MeshNode) -> Result<(Arc<NodeRpcClient>, Arc<StaticResolver>), String> {
    let endpoint_id = admin.endpoint_id.0.parse::<iroh::PublicKey>().map_err(|e| format!("--admin-birth names {} (node {}) at the endpoint {:?}, no Iroh key: {e}", admin.name, admin.node_id, admin.endpoint_id.0))?;
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode { node_id: admin.node_id.clone(), name: admin.name.clone(), endpoint_id, transport_addr: admin.transport_addr, incarnation: admin.incarnation.clone() });
    let client = Arc::new(NodeRpcClient::new(bind_endpoint().await?, resolver.clone()).with_caller_system("rdm"));
    let read = async { resolved(&views_of(&client, &admin.name).await?) };
    match read.await {
        Ok(nodes) => {
            for n in nodes {
                resolver.insert(n);
            }
            Ok((client, resolver))
        }
        Err(e) => {
            client.endpoint().close().await;
            Err(e)
        }
    }
}

/// Every node the node-admin `admin` holds in its view: `fabric.get`, then `node.get [mesh: m]` for each mesh the
/// fabric lists with nodes, read from that admin. A read that does not answer is refused naming the
/// admin, the read and how it ended.
async fn views_of(client: &Arc<NodeRpcClient>, admin: &PathName) -> Result<Vec<NodeView>, String> {
    let carrier = BuildCarrier::new(client.clone(), admin.clone());
    let fabric = rdm_test::typed::the_fabric(&Fabrics::new(carrier.clone())).await.map_err(|e| format!("fabric.get at the entry admin {admin}: {e}"))?;
    let nodes = Nodes::new(carrier);
    let mut all = Vec::new();
    for mesh in fabric.meshes.iter().filter(|m| !m.nodes.is_empty()) {
        all.extend(rdm_test::typed::mesh_nodes(&nodes, &mesh.name).await.map_err(|e| format!("node.get [mesh: {}] at the entry admin {admin}: {e}", mesh.name))?);
    }
    Ok(all)
}

/// Every node of `views` that can be dialled.
///
/// A node the admin holds with no endpoint, address or incarnation yet (created, not born) is not
/// dialable and is left out, as an event naming it and the fields it lacks. An endpoint that is present
/// and is no Iroh key refuses the whole call naming the node, the field, the value and the parse error:
/// a reader that does not recognise a value never drops it.
fn resolved(views: &[NodeView]) -> Result<Vec<ResolvedNode>, String> {
    let mut out = Vec::new();
    for n in views {
        let endpoint_id = match n.endpoint_id.as_deref() {
            None => None,
            Some(v) => Some(v.parse::<iroh::PublicKey>().map_err(|e| format!("view node {} ({}): endpoint_id {v:?} does not parse: {e}", n.node_id, n.name))?),
        };
        match (endpoint_id, n.transport_addr, n.incarnation_id.clone()) {
            (Some(endpoint_id), Some(transport_addr), Some(incarnation)) => out.push(ResolvedNode { node_id: n.node_id.clone(), name: n.name.clone(), endpoint_id, transport_addr, incarnation }),
            (endpoint_id, transport_addr, incarnation) => {
                let absent: Vec<&str> = [(endpoint_id.is_none(), "endpoint_id"), (transport_addr.is_none(), "transport_addr"), (incarnation.is_none(), "incarnation_id")]
                    .into_iter()
                    .filter_map(|(is_absent, field)| is_absent.then_some(field))
                    .collect();
                tracing::info!(node_id = %n.node_id, node = %n.name, absent = absent.join(","), "the admin's view holds this node with no value yet for these fields: it cannot be dialled");
            }
        }
    }
    Ok(out)
}

fn request(a: &Args) -> Result<ProofRequest, String> {
    let key = a.key.as_bytes().to_vec();
    let bytes = |v: &Option<String>| v.as_ref().map(|s| s.as_bytes().to_vec());
    Ok(match a.op.as_str() {
        "get" => ProofRequest::Get { key },
        "put" => ProofRequest::Put { key, value: bytes(&a.value).ok_or("put needs --value")? },
        "delete" => ProofRequest::Delete { key },
        "cas" => ProofRequest::CompareAndSwap { key, expected: bytes(&a.expected), new: bytes(&a.value) },
        other => return Err(format!("unknown op {other:?}")),
    })
}

fn text(v: &[u8]) -> Value {
    match std::str::from_utf8(v) {
        Ok(s) => json!(s),
        Err(_) => json!({ "hex": hex::encode(v) }),
    }
}

fn reply(r: &ProofReply) -> Value {
    let result = match r {
        ProofReply::Value { value, .. } => json!({"found": true, "value": text(value)}),
        ProofReply::Absent { .. } => json!({"found": false}),
        ProofReply::Stored { .. } => json!({"stored": true}),
        ProofReply::Deleted { .. } => json!({"deleted": true}),
        ProofReply::Swapped { .. } => json!({"swapped": true}),
        ProofReply::Mismatch { current, .. } => json!({"swapped": false, "current": current.as_deref().map(text)}),
        ProofReply::TooLarge { field, limit, got, .. } => json!({"refused": "too-large", "field": field, "limit": limit, "got": got}),
        ProofReply::StoreFailed { reason, .. } => json!({"refused": "store-failed", "reason": reason}),
        other => json!({"refused": format!("{other:?}")}),
    };
    let mut v = json!({ "result": result });
    if let Some(at) = r.provenance() {
        let o = v.as_object_mut().unwrap();
        o.insert("executing_node".into(), json!(at.node_id));
        o.insert("node".into(), json!(at.node));
        o.insert("mesh".into(), json!(at.mesh));
        o.insert("incarnation_id".into(), json!(at.incarnation_id));
        o.insert("op".into(), json!(at.op.as_str()));
    }
    v
}

async fn run(a: Args) -> Result<(Value, Option<iroh::Endpoint>), String> {
    let target = target(&a.target)?;
    if a.op == "nodes" {
        let (client, _) = enter(&a.admin_birth).await?;
        let nodes = views_of(&client, &a.admin_birth.name).await;
        client.endpoint().close().await;
        return nodes.map(|nodes| (json!({"outcome": "Reply", "nodes": nodes}), None));
    }
    if a.op == "resolve" {
        return run_resolve(&a, &target).await.map(|value| (value, None));
    }
    if matches!(a.op.as_str(), "fabric-delete" | "node-delete") {
        return run_delete(&a).await.map(|value| (value, None));
    }
    if a.op == "declare" {
        return run_declare(&a, &target).await.map(|value| (value, None));
    }
    if a.op == "topology" {
        return run_topology(&a, &target).await.map(|value| (value, None));
    }
    if matches!(a.op.as_str(), "originate" | "fault" | "snapshot" | "record-proxy" | "dial" | "stream" | "block-direct" | "stop-transport" | "own-config") {
        return run_originate(&a, &target).await.map(|value| (value, None));
    }
    let req = request(&a)?;
    let (client, resolver) = enter(&a.admin_birth).await?;
    // The exact final target first; `--via` only decides how it is reached.
    // The exact final target first (a `path:` is the current holder); a target the view does
    // not hold is the client's own `NotSent(Resolve(..))`, never a refusal here.
    // A carrier the view does not hold (it left the fabric while the call was aimed at it) is the same
    // fact as a target it does not hold: the call is not sent, and the client says so.
    let route: Result<rdm_transport::RouteChoice, _> = match (&a.via, a.no_route) {
        (_, true) => Ok(rdm_transport::RouteChoice::NoActiveRoute),
        (None, false) => Ok(rdm_transport::RouteChoice::Direct),
        (Some(v), false) => {
            let carrier = self::target(v)?;
            resolver.resolve(&carrier).map(|c| rdm_transport::RouteChoice::ViaPeer { carrier: c.node_id, path: c.name })
        }
    };
    let mut opts = CallOptions { cut_before_finish: a.cut_before_finish, ..CallOptions::default() };
    if let Some(ms) = a.budget_ms {
        opts.budget = rdm_transport::Budget::Overall(std::time::Duration::from_millis(ms));
    }
    let (out, leg): (RpcOutcome<ProofReply>, &str) = match route {
        Err(f) => (rdm_core::outcome::PreCommit::begin(<ProofStore as rdm_core::protocol::NodeProtocol>::OP).not_sent(rdm_core::outcome::NotSentReason::Resolve(f)), "via-peer"),
        Ok(route) => match resolver.resolve(&target) {
            Ok(n) => {
                let (out, _, leg) = client.call_routed::<ProofStore>(&n.node_id, &route, &req, &opts).await;
                (out, leg.token())
            }
            Err(_) if matches!(route, rdm_transport::RouteChoice::Direct) => (client.call::<ProofStore>(&target, &req, &opts).await.0, "direct"),
            Err(f) => (rdm_core::outcome::PreCommit::begin(<ProofStore as rdm_core::protocol::NodeProtocol>::OP).not_sent(rdm_core::outcome::NotSentReason::Resolve(f)), route.token()),
        },
    };
    // The reply is observable as soon as the call ends. The caller still owns
    // this endpoint and closes it before the runtime and telemetry guard end.
    let value = match &out {
        RpcOutcome::Reply(r) => json!({"outcome": out.name(), "route": leg, "reply": reply(r.value())}),
        RpcOutcome::NotSent(n) => json!({"outcome": out.name(), "route": leg, "reason": format!("{:?}", n.reason())}),
        RpcOutcome::Indeterminate(i) => json!({"outcome": out.name(), "route": leg, "reason": format!("{:?}", i.reason())}),
        RpcOutcome::Unserved(u) => json!({"outcome": out.name(), "route": leg, "reason": format!("{u:?}")}),
        RpcOutcome::RejectedStale(r) => json!({"outcome": out.name(), "route": leg, "target_node_id": r.target_node_id()}),
    };
    Ok((value, Some(client.endpoint().clone())))
}

/// A refusal keeps its evidence: the process returns its exit status from `main`, so the telemetry
/// guard drops (flushes) before the process ends, and the refusal is a span with its reason.
///
/// The runtime ends BEFORE the guard drops. The call's connection work (tasks the dial and the
/// stream leave winding down after the endpoint closes) holds spans that descend from the root, and a
/// span is exported only when its last descendant has closed: the root closes when the runtime drops
/// those tasks. A guard dropped while they are still alive flushes a file with the call's spans and
/// no root.
fn main() -> std::process::ExitCode {
    // One invocation runs one async call sequence. Its I/O tasks share this runtime;
    // creating a worker for every host CPU adds no call concurrency.
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("the probe's runtime");
    let telemetry = runtime.block_on(async { rdm_transport::init_evidence_telemetry_for_one_call("rdm-rpc-probe") });
    let code = runtime.block_on(probe());
    drop(runtime);
    drop(telemetry);
    code
}

async fn probe() -> std::process::ExitCode {
    let args = match parse(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            tracing::info_span!("rdm.node_rpc.proof_store.reject.via-probe-arguments", reason = %e).in_scope(|| {
                rdm_transport::report_init_steps_here();
                tracing::info!("the probe refused its arguments")
            });
            println!("{}", json!({"outcome": "Refused", "reason": e}));
            return std::process::ExitCode::from(2);
        }
    };
    let span = tracing::info_span!("rdm.node_rpc.proof_store.resolve.via-probe", op = %args.op, target = %args.target);
    // The invocation is one trace: the process's init steps are reported inside its root.
    span.in_scope(rdm_transport::report_init_steps_here);
    let out = {
        use tracing::Instrument;
        run(args).instrument(span.clone()).await
    };
    match out {
        Ok((mut v, endpoint)) => {
            // The invocation's trace, named by the probe itself: a scenario reads it here, never from the
            // span file, whose root is exported only when its last descendant has closed.
            if let Some(tp) = span.in_scope(rdm_transport::current_traceparent) {
                v["traceparent"] = json!(tp);
            }
            println!("{v}");
            if let Some(endpoint) = endpoint { endpoint.close().await; }
            std::process::ExitCode::SUCCESS
        }
        Err(e) => {
            span.in_scope(|| tracing::info!(refusal = %e, "the probe refused the call"));
            println!("{}", json!({"outcome": "Refused", "reason": e}));
            std::process::ExitCode::from(2)
        }
    }
}

/// `resolve --target <node> --query exact:<id>|path:<p>`: what the target node's own live
/// resolver says about the query.
async fn run_resolve(a: &Args, target: &NodeTarget) -> Result<Value, String> {
    use rdm_test::resolve_probe::{ProbeTarget, ResolveProbe, ResolveReply, ResolveRequest};
    let query = match a.key.split_once(':') {
        Some(("exact", id)) => ProbeTarget::Exact(id.to_string()),
        Some(("path", p)) => ProbeTarget::Path(p.to_string()),
        _ => return Err(format!("--query {:?} is neither exact:<node_id> nor path:<path.name>", a.key)),
    };
    let (client, _) = enter(&a.admin_birth).await?;
    let req = ResolveRequest::Resolve { target: query };
    let (out, _) = client.call::<ResolveProbe>(target, &req, &CallOptions::default()).await;
    // The probe's one socket is closed before the process ends: an endpoint dropped open aborts the
    // process ungracefully and its span file is lost (a call that never left the resolver is the case).
    client.endpoint().close().await;
    Ok(match &out {
        RpcOutcome::Reply(r) => {
            let v = r.value();
            let mut reply = json!({"resolution": v.resolution()});
            if let ResolveReply::Found { node_id, name, incarnation_id, .. } = v {
                reply["node_id"] = json!(node_id);
                reply["name"] = json!(name);
                reply["incarnation_id"] = json!(incarnation_id);
            }
            json!({"outcome": out.name(), "reply": reply})
        }
        RpcOutcome::NotSent(n) => json!({"outcome": out.name(), "reason": format!("{:?}", n.reason())}),
        RpcOutcome::Indeterminate(i) => json!({"outcome": out.name(), "reason": format!("{:?}", i.reason())}),
        RpcOutcome::Unserved(u) => json!({"outcome": out.name(), "reason": format!("{u:?}")}),
        RpcOutcome::RejectedStale(r) => json!({"outcome": out.name(), "target_node_id": r.target_node_id()}),
    })
}

/// The probe's one endpoint.
async fn bind_endpoint() -> Result<iroh::Endpoint, String> {
    rdm_transport::endpoint::bind(iroh::SecretKey::generate(), probe_bind()).await.map_err(|e| format!("binding the probe's endpoint: {e}"))
}

/// Where the probe binds its one socket: `RDM_PROBE_BIND` (an address on the fabric's network,
/// e.g. a container fabric's gateway), else loopback.
fn probe_bind() -> std::net::SocketAddr {
    let ip: std::net::IpAddr = std::env::var("RDM_PROBE_BIND").ok().and_then(|v| v.parse().ok()).unwrap_or(std::net::IpAddr::from([127, 0, 0, 1]));
    std::net::SocketAddr::new(ip, 0)
}

/// `originate --target <source> --destination path:<dest> get|put --key k [--value v]`,
/// `fault --target <source> --refuse-index N --refuse-history M | --release`, and
/// `snapshot --target <source>`: the testkit's originate door on the source node (op 0x73).
async fn run_originate(a: &Args, target: &NodeTarget) -> Result<Value, String> {
    use rdm_test::originate::{Originate, OriginateReply, OriginateRequest, ProofOp};
    let req = match a.op.as_str() {
        "originate" => {
            let destination = a.destination.clone().ok_or("originate needs --destination path:<path.name>")?;
            let destination = destination.strip_prefix("path:").map(str::to_string).ok_or("--destination is path:<path.name>")?;
            let key = a.key.as_bytes().to_vec();
            let op = match a.value.as_ref() {
                Some(v) => ProofOp::Put { key, value: v.as_bytes().to_vec() },
                None => ProofOp::Get { key },
            };
            OriginateRequest::Call { destination, op }
        }
        "dial" => OriginateRequest::Dial { destination: a.destination.as_deref().and_then(|d| d.strip_prefix("path:")).map(str::to_string).ok_or("dial needs --destination path:<path.name>")? },
        "stream" => OriginateRequest::Stream {
            destination: a.destination.as_deref().and_then(|d| d.strip_prefix("path:")).map(str::to_string).ok_or("stream needs --destination path:<path.name>")?,
            payload: a.value.clone().unwrap_or_default().into_bytes(),
        },
        "record-proxy" => {
            let path = |what: &str, v: &Option<String>| -> Result<String, String> {
                v.as_deref().and_then(|d| d.strip_prefix("path:")).map(str::to_string).ok_or(format!("record-proxy needs {what} path:<path.name>"))
            };
            OriginateRequest::RecordProxy { destination: path("--destination", &a.destination)?, carrier: path("--carrier", &a.carrier)?, failed_attempts: a.failed_attempts }
        }
        "stop-transport" => OriginateRequest::StopTransport { reason: "testkit stop-transport".to_string() },
        "block-direct" => OriginateRequest::BlockDirect {
            destination: a.destination.as_deref().and_then(|d| d.strip_prefix("path:")).map(str::to_string).ok_or("block-direct needs --destination path:<path.name>")?,
            blocked: !a.release,
        },
        // `own-config --target <node> --value k=v[,k=v...]`: the node puts its own config, the whole map.
        "own-config" => OriginateRequest::UpdateOwnConfig {
            config: a.value.as_deref().unwrap_or_default().split(',').filter(|kv| !kv.is_empty()).map(|kv| kv.split_once('=').map(|(k, v)| (k.to_string(), v.to_string())).ok_or(format!("own-config --value is k=v[,k=v...], not {kv:?}"))).collect::<Result<_, String>>()?,
        },
        "fault" if a.release => OriginateRequest::ReleaseFault,
        "fault" => OriginateRequest::ArmFault { refuse_index: a.refuse_index, refuse_history: a.refuse_history, pass_history: a.pass_history },
        _ => OriginateRequest::Snapshot,
    };
    let (client, _) = enter(&a.admin_birth).await?;
    let opts = CallOptions { budget: rdm_transport::Budget::Overall(std::time::Duration::from_secs(20)), ..CallOptions::default() };
    let (out, _) = client.call::<Originate>(target, &req, &opts).await;
    // The probe's one socket is closed before the process ends: an endpoint dropped open aborts the
    // process ungracefully and its span file is lost (a call that never left the resolver is the case).
    client.endpoint().close().await;
    Ok(match &out {
        RpcOutcome::Reply(r) => match r.value() {
            OriginateReply::Called { by, destination_node_id, route, carrier, deleted, outcome, reply, pooled } => json!({
                "outcome": "Reply", "by": by.node, "destination_node_id": destination_node_id, "route": route, "carrier": carrier,
                "deleted": deleted, "call_outcome": outcome, "reply": reply.as_ref().map(self::reply), "pooled": pooled,
            }),
            OriginateReply::FaultArmed { by, refuse_index, refuse_history } => json!({"outcome": "Reply", "by": by.node, "fault": "armed", "refuse_index": refuse_index, "refuse_history": refuse_history}),
            OriginateReply::FaultReleased { by, refused } => json!({"outcome": "Reply", "by": by.node, "fault": "released", "refused": refused}),
            OriginateReply::Snapshot { by, own_active_proxies, own_latest_directs, active_len, active_directs, owed, fault_refused } => json!({
                "outcome": "Reply", "by": by.node, "own_active_proxies": own_active_proxies, "own_latest_directs": own_latest_directs,
                "active_len": active_len, "active_directs": active_directs, "owed": owed, "fault_refused": fault_refused,
            }),
            OriginateReply::TransportStopMarked { by, reason } => json!({"outcome": "Reply", "by": by.node, "transport_stop_marked": reason}),
            OriginateReply::Dialed { by, destination_node_id, outcome } => json!({"outcome": "Reply", "by": by.node, "dialed": outcome, "destination_node_id": destination_node_id}),
            OriginateReply::Streamed { by, peer, kind, connection_id, answered } => json!({
                "outcome": "Reply", "by": by.node, "streamed": true, "kind": kind, "connection_id": connection_id, "peer": peer.node, "peer_node_id": peer.node_id,
                "peer_incarnation": peer.incarnation_id, "answered": String::from_utf8_lossy(answered),
            }),
            OriginateReply::StreamFailed { by, reason } => json!({"outcome": "Reply", "by": by.node, "streamed": false, "reason": reason}),
            OriginateReply::DirectBlocked { by, destination, blocked } => json!({"outcome": "Reply", "by": by.node, "direct_blocked": blocked, "destination": destination}),
            OriginateReply::OwnConfigUpdated { by, outcome, config, revision } => json!({"outcome": "Reply", "by": by.node, "own_config": outcome, "config": config, "revision": revision}),
            OriginateReply::ProxyRecorded { by, destination_node_id, destination_incarnation, carrier_node_id, carrier_incarnation, failed_attempts } => json!({
                "outcome": "Reply", "by": by.node, "proxy_recorded": true, "destination_node_id": destination_node_id, "destination_incarnation": destination_incarnation,
                "carrier_node_id": carrier_node_id, "carrier_incarnation": carrier_incarnation, "failed_attempts": failed_attempts,
            }),
            other => json!({"outcome": "Reply", "refused": format!("{other:?}")}),
        },
        RpcOutcome::NotSent(n) => json!({"outcome": out.name(), "reason": format!("{:?}", n.reason())}),
        RpcOutcome::Indeterminate(i) => json!({"outcome": out.name(), "reason": format!("{:?}", i.reason())}),
        RpcOutcome::Unserved(u) => json!({"outcome": out.name(), "reason": format!("{u:?}")}),
        RpcOutcome::RejectedStale(r) => json!({"outcome": out.name(), "target_node_id": r.target_node_id()}),
    })
}

/// `topology --target <node> [--mesh <name>] [--since <node>|<node_id>|<incarnation>|<version>]`: one
/// `GetTopology` (op `0x1E`) read, every frame of the stream reported in order. The probe installs
/// nothing: it has no topology view.
/// `fabric-delete`: fabric.delete at the entry admin, its stream read to the terminal (`Complete`,
/// `Failed` or `FabricPrimaryMoved`). `node-delete --target path:<p>`: node.delete of `p`, its stream
/// read to its last reply: the terminal, or the end of the stream after it, which is how the
/// fabric-primary's own node.delete ends once node.deleted is sent (its process serves nothing
/// after it). Each prints the replies in order and how long the call took.
async fn run_delete(a: &Args) -> Result<Value, String> {
    let (client, _) = enter(&a.admin_birth).await?;
    let carrier = BuildCarrier::new(client.clone(), a.admin_birth.name.clone());
    let begun = std::time::Instant::now();
    let mut replies = Vec::new();
    let result = if a.op == "fabric-delete" {
        match carrier.create(rdm_client::Change::DeleteFabric).await {
            Err(end) => json!({"outcome": "Rejected", "reason": end.to_string()}),
            Ok(mut stream) => {
                let build_id = stream.build_id().to_string();
                let mut terminal = None;
                while let Some(frame) = stream.next().await {
                    match frame {
                        Ok(frame) => {
                            let end = matches!(frame, rdm_client::BuildFrame::Complete { .. } | rdm_client::BuildFrame::Failed { .. } | rdm_client::BuildFrame::FabricPrimaryMoved { .. });
                            replies.push(format!("{frame:?}"));
                            if end {
                                terminal = Some(match frame {
                                    rdm_client::BuildFrame::Complete { .. } => "Complete",
                                    rdm_client::BuildFrame::Failed { .. } => "Failed",
                                    _ => "FabricPrimaryMoved",
                                });
                                break;
                            }
                        }
                        Err(e) => {
                            replies.push(format!("ended: {e}"));
                            break;
                        }
                    }
                }
                json!({"outcome": "Reply", "build_id": build_id, "terminal": terminal, "replies": replies, "elapsed_ms": begun.elapsed().as_millis() as u64})
            }
        }
    } else {
        let path: PathName = a.target.strip_prefix("path:").ok_or_else(|| format!("node-delete needs --target path:<path.name>, not {:?}", a.target))?.parse().map_err(|e| format!("--target {:?}: {e:?}", a.target))?;
        match Nodes::new(carrier).delete(&path).await {
            Err(end) => json!({"outcome": "Rejected", "reason": end.to_string()}),
            Ok(mut stream) => {
                let mut deleted = false;
                while let Some(frame) = stream.next().await {
                    match frame {
                        Ok(frame) => {
                            deleted |= frame == rdm_client::Frame::Event(rdm_client::NodeEvent::Deleted);
                            let end = frame.is_terminal();
                            replies.push(format!("{frame:?}"));
                            if end {
                                break;
                            }
                        }
                        Err(e) => {
                            replies.push(format!("ended: {e}"));
                            break;
                        }
                    }
                }
                json!({"outcome": "Reply", "node": path.to_string(), "deleted": deleted, "replies": replies, "elapsed_ms": begun.elapsed().as_millis() as u64})
            }
        }
    };
    client.endpoint().close().await;
    Ok(result)
}

async fn run_topology(a: &Args, target: &NodeTarget) -> Result<Value, String> {
    use rdm_transport::stream::StreamItem;
    use rdm_core::topology::{SourceVersion, Topology, TopologyReply, TopologyRequest};
    let since = match &a.since {
        None => None,
        Some(s) => {
            let parts: Vec<&str> = s.split('|').collect();
            let [node, node_id, incarnation, version] = parts[..] else { return Err(format!("--since {s:?} is not <node>|<node_id>|<incarnation>|<version>")) };
            Some(SourceVersion {
                publisher: rdm_core::PublisherId { node: node.to_string(), incarnation: IncarnationId::parse(incarnation).map_err(|e| format!("--since incarnation: {e}"))?, node_id: rdm_core::NodeId::parse(node_id).map_err(|e| format!("--since node_id: {e}"))? },
                topology_version: version.parse().map_err(|e| format!("--since version {version:?}: {e}"))?,
            })
        }
    };
    let (client, _) = enter(&a.admin_birth).await?;
    let req = TopologyRequest::GetTopology { mesh: a.mesh.clone(), since };
    // The probe records nothing of the frames it prints: reading one is its recovery.
    let started = client.call_stream::<Topology>(target, &req, &CallOptions::default()).await.map(|(s, e)| (s.read_only(), e));
    let mut frames = Vec::new();
    let result = match started {
        Err((out, _)) => json!({"outcome": out.name()}),
        Ok((mut stream, _)) => {
            while let Some(item) = stream.next().await {
                match item {
                    StreamItem::Frame(_, f) => frames.push(match f {
                        TopologyReply::Snapshot { mesh, publisher, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, deleted } => json!({
                            "frame": "snapshot", "mesh": mesh, "publisher_node": publisher.node, "publisher_node_id": publisher.node_id.as_str(), "publisher_incarnation": publisher.incarnation.as_str(), "topology_version": topology_version,
                            "snapshot_id": snapshot_id, "chunk_index": chunk_index, "chunk_count": chunk_count,
                            "members": digests.into_iter().map(|d| rdm_core::MeshDigest::from(d).node.name.to_string()).collect::<Vec<_>>(),
                            "in_flight": in_flight.len(), "deleted": deleted.len(),
                        }),
                        TopologyReply::Unchanged { mesh, publisher, topology_version } => json!({"frame": "unchanged", "mesh": mesh, "publisher_node": publisher.node, "publisher_node_id": publisher.node_id.as_str(), "publisher_incarnation": publisher.incarnation.as_str(), "topology_version": topology_version}),
                        TopologyReply::End { meshes } => json!({"frame": "end", "meshes": meshes}),
                        other => json!({"frame": other.name(), "detail": format!("{other:?}")}),
                    }),
                    StreamItem::Failed(f) => frames.push(json!({"frame": "failed", "detail": format!("{f:?}")})),
                }
            }
            json!({"outcome": "Reply", "frames": frames})
        }
    };
    client.endpoint().close().await;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::resolved;
    use rdm_core::{IncarnationId, NodeId, PathName};
    use rdm_client::NodeView;
    use rdm_core::views::NodeStatus;

    /// A node of `name` as an admin's view holds it: born when `endpoint`, `addr` and `incarnation`
    /// are all given.
    fn view(name: &str, endpoint: Option<&str>, addr: Option<&str>, incarnation: Option<IncarnationId>) -> NodeView {
        let name: PathName = name.parse().unwrap();
        NodeView { cert: None, executable: None, storage_persistence: None, topology_storage_provider: None, types: Default::default(), held: Default::default(), held_hash: Default::default(),
            kind: name.kind,
            mesh: name.mesh.clone(),
            name,
            node_id: NodeId::mint(),
            endpoint_id: endpoint.map(str::to_string),
            incarnation_id: incarnation,
            deployment_id: None,
            provider: None,
            data_dir: None,
            status: NodeStatus::Ready,
            is_primary: false,
            is_fabric_primary: false,
            admin_api_base: None,
            transport_addr: addr.map(|a| a.parse().unwrap()),
            listeners: Vec::new(),
            declared: None,
            stopped: false,
            load: None,
            gossip: None,
            tags: Default::default(),
            config: Default::default(),
            data_dir_lost: None,
            data_dir_unreadable: None,
        }
    }

    fn born(name: &str) -> NodeView {
        view(name, Some(&iroh::SecretKey::generate().public().to_string()), Some("127.0.0.1:9"), Some(IncarnationId::mint()))
    }

    /// CONTRACT: a view whose nodes are all born resolves every one of them, in order.
    #[test]
    fn view_with_born_nodes_resolves_each() {
        let views = [born("mesh1.rpc.1"), born("mesh1.rpc.2")];
        let ids: Vec<NodeId> = resolved(&views).unwrap().into_iter().map(|n| n.node_id).collect();
        assert_eq!(ids, views.iter().map(|v| v.node_id.clone()).collect::<Vec<_>>());
    }

    /// CONTRACT: a node whose endpoint is present and is no Iroh key refuses the whole call, and the
    /// refusal names the node's id and path.name, the field, the value and the parse error. What must
    /// NOT happen: the node vanishing while the readable ones resolve.
    #[test]
    fn view_node_with_unparseable_endpoint_refuses_naming_node_field_value_and_error() {
        let bad = view("mesh1.rpc.2", Some("not-an-endpoint-id"), Some("127.0.0.1:9"), Some(IncarnationId::mint()));
        let err = resolved(&[born("mesh1.rpc.1"), bad.clone()]).unwrap_err();
        for part in [bad.node_id.to_string(), "mesh1.rpc.2".into(), "endpoint_id".into(), "\"not-an-endpoint-id\"".into(), "does not parse: ".into()] {
            assert!(err.contains(&part), "the refusal names {part:?}: {err}");
        }
    }

    /// CONTRACT: a node the admin holds as created and not yet born (endpoint, address and incarnation
    /// all absent) is not dialable and is left out; it does not refuse the call, and the born nodes
    /// beside it still resolve.
    #[test]
    fn view_with_an_unborn_node_leaves_it_out_and_resolves_the_rest() {
        let readable = born("mesh1.rpc.1");
        let nodes = resolved(&[view("mesh1.rpc.2", None, None, None), readable.clone()]).unwrap();
        assert_eq!(nodes.into_iter().map(|n| n.node_id).collect::<Vec<_>>(), vec![readable.node_id]);
    }

    /// CONTRACT: an absent value beside an unreadable one does not hide it: a node with no incarnation
    /// yet and an endpoint that is no Iroh key still refuses.
    #[test]
    fn view_node_with_an_absent_field_and_an_unparseable_one_refuses() {
        let bad = view("mesh1.rpc.2", Some("not-an-endpoint-id"), Some("127.0.0.1:9"), None);
        let err = resolved(&[bad.clone()]).unwrap_err();
        assert!(err.contains(&bad.node_id.to_string()) && err.contains("endpoint_id"), "{err}");
    }
}
