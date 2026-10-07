//! `rafka-rpc-probe`: one proof-store call over real Node RPC, for scenarios
//! (PRD §6: no in-process shortcut).
//!
//! ```text
//! rafka-rpc-probe --admin <base> get    --target exact:<node_id>|path:<path.name> --key <k>
//! rafka-rpc-probe --admin <base> put    --target ... --key <k> --value <v>
//! rafka-rpc-probe --admin <base> delete --target ... --key <k>
//! rafka-rpc-probe --admin <base> cas    --target ... --key <k> [--expected <v>] [--value <v>]
//! ```
//!
//! The target is resolved from the admin's node view at the dial cut:
//! `exact:` is exactly that logical node and never follows a replacement;
//! `path:` is whoever holds the path now. `cas` without `--expected` expects
//! the key absent; without `--value` it deletes. It prints one JSON line:
//! `{"outcome": "Reply"|"NotSent"|"Unserved"|"Indeterminate", ...}`; a
//! `Reply` carries the executing node's provenance and the typed result.

use rafka_mesh_entity::{IncarnationId, NodeId, PathName};
use rafka_node_rpc::{CallOptions, NodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_testkit::proof_store::{ProofReply, ProofRequest, ProofStore};
use serde_json::{json, Value};
use std::sync::Arc;

struct Args {
    admin: String,
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
    /// `declare`: the authority the node declares to (`path:`/`exact:`), the state, and the
    /// testkit-only overrides.
    to: Option<String>,
    state: Option<String>,
    as_node_id: Option<String>,
    as_incarnation: Option<String>,
}

fn parse(mut it: impl Iterator<Item = String>) -> Result<Args, String> {
    let (mut admin, mut op, mut target, mut key, mut value, mut expected, mut via) = (None, None, None, None, None, None, None);
    let mut no_route = false;
    let (mut to, mut state, mut as_node_id, mut as_incarnation) = (None, None, None, None);
    while let Some(a) = it.next() {
        let mut take = |name: &str| it.next().ok_or_else(|| format!("{name} needs a value"));
        match a.as_str() {
            "--admin" => admin = Some(take("--admin")?),
            "--target" => target = Some(take("--target")?),
            "--key" => key = Some(take("--key")?),
            "--value" => value = Some(take("--value")?),
            "--expected" => expected = Some(take("--expected")?),
            "--query" => key = Some(take("--query")?),
            "--via" => via = Some(take("--via")?),
            "--no-route" => no_route = true,
            "--to" => to = Some(take("--to")?),
            "--state" => state = Some(take("--state")?),
            "--as-node-id" => as_node_id = Some(take("--as-node-id")?),
            "--as-incarnation" => as_incarnation = Some(take("--as-incarnation")?),
            "get" | "put" | "delete" | "cas" | "resolve" | "declare" if op.is_none() => op = Some(a),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    let is_declare = op.as_deref() == Some("declare");
    Ok(Args {
        admin: admin.ok_or("--admin is required")?,
        op: op.ok_or("an op (get, put, delete, cas, resolve, declare) is required")?,
        target: target.ok_or("--target is required")?,
        key: if is_declare { key.unwrap_or_default() } else { key.ok_or("--key is required")? },
        value,
        expected,
        via,
        no_route,
        to,
        state,
        as_node_id,
        as_incarnation,
    })
}

/// `declare`: ask the node at `--target` to declare `--state` for its own birth to `--to`.
async fn run_declare(a: &Args, target: &NodeTarget) -> Result<Value, String> {
    use rafka_node_rpc_contract::status::NodeState;
    use rafka_node_rpc_testkit::declare_probe::{DeclareProbe, DeclareReply, DeclareRequest, DeclareTarget};
    let to = a.to.as_deref().ok_or("declare needs --to <path:..|exact:..>")?;
    let to = match to.split_once(':') {
        Some(("exact", id)) => DeclareTarget::Exact(id.to_string()),
        Some(("path", p)) => DeclareTarget::Path(p.to_string()),
        _ => return Err(format!("--to {to:?} is neither exact:<node_id> nor path:<path.name>")),
    };
    let state = match a.state.as_deref().ok_or("declare needs --state")? {
        "pending" => NodeState::Pending,
        "ready-for-traffic" => NodeState::ReadyForTraffic,
        "draining" => NodeState::Draining,
        "leaving" => NodeState::Leaving,
        other => return Err(format!("--state {other:?} is not a node state")),
    };
    let url = format!("{}/api/nodes", a.admin.trim_end_matches('/'));
    let view: Value = reqwest::get(&url).await.map_err(|e| format!("GET {url}: {e}"))?.json().await.map_err(|e| format!("GET {url}: {e}"))?;
    let resolver = Arc::new(StaticResolver::new());
    for n in resolved(&view) {
        resolver.insert(n);
    }
    let ep = rafka_node_rpc::endpoint::bind(iroh::SecretKey::generate(), probe_bind()).await.map_err(|e| format!("binding the probe's endpoint: {e}"))?;
    let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm");
    let req = DeclareRequest::Declare { to, state, node_id: a.as_node_id.clone(), incarnation: a.as_incarnation.clone() };
    let (out, _) = client.call::<DeclareProbe>(target, &req, &CallOptions::default()).await;
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

/// Every node in the admin's view that can be dialled.
fn resolved(view: &Value) -> Vec<ResolvedNode> {
    view["nodes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|n| {
            Some(ResolvedNode {
                node_id: NodeId::parse(n["node_id"].as_str()?).ok()?,
                name: n["name"].as_str()?.parse().ok()?,
                endpoint_id: n["endpoint_id"].as_str()?.parse().ok()?,
                transport_addr: n["transport_addr"].as_str()?.parse().ok()?,
                incarnation: IncarnationId(n["incarnation_id"].as_str()?.to_string()),
            })
        })
        .collect()
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

async fn run(a: Args) -> Result<Value, String> {
    let target = target(&a.target)?;
    if a.op == "resolve" {
        return run_resolve(&a, &target).await;
    }
    if a.op == "declare" {
        return run_declare(&a, &target).await;
    }
    let req = request(&a)?;
    let url = format!("{}/api/nodes", a.admin.trim_end_matches('/'));
    let view: Value = reqwest::get(&url).await.map_err(|e| format!("GET {url}: {e}"))?.json().await.map_err(|e| format!("GET {url}: {e}"))?;
    let resolver = Arc::new(StaticResolver::new());
    for n in resolved(&view) {
        resolver.insert(n);
    }
    let ep = rafka_node_rpc::endpoint::bind(iroh::SecretKey::generate(), probe_bind())
        .await
        .map_err(|e| format!("binding the probe's endpoint: {e}"))?;
    let client = NodeRpcClient::new(ep, resolver.clone()).with_caller_system("rdm");
    // The exact final target first; `--via` only decides how it is reached.
    // The exact final target first (a `path:` is the current holder); a target the view does
    // not hold is the client's own `NotSent(Resolve(..))`, never a refusal here.
    let route = match (&a.via, a.no_route) {
        (_, true) => rafka_node_rpc::RouteChoice::NoActiveRoute,
        (None, false) => rafka_node_rpc::RouteChoice::Direct,
        (Some(v), false) => {
            let carrier = self::target(v)?;
            let c = resolver.resolve(&carrier).map_err(|f| format!("the carrier {v} does not resolve: {f:?}"))?;
            rafka_node_rpc::RouteChoice::ViaPeer { carrier: c.node_id, path: c.name }
        }
    };
    let (out, leg): (RpcOutcome<ProofReply>, &str) = match resolver.resolve(&target) {
        Ok(n) => {
            let (out, _, leg) = client.call_routed::<ProofStore>(&n.node_id, &route, &req, &CallOptions::default()).await;
            (out, leg.token())
        }
        Err(_) if matches!(route, rafka_node_rpc::RouteChoice::Direct) => (client.call::<ProofStore>(&target, &req, &CallOptions::default()).await.0, "direct"),
        Err(f) => (rafka_node_rpc_contract::outcome::PreCommit::begin(<ProofStore as rafka_node_rpc_contract::protocol::NodeProtocol>::OP).not_sent(rafka_node_rpc_contract::outcome::NotSentReason::Resolve(f)), route.token()),
    };
    Ok(match &out {
        RpcOutcome::Reply(r) => json!({"outcome": out.name(), "route": leg, "reply": reply(r.value())}),
        RpcOutcome::NotSent(n) => json!({"outcome": out.name(), "route": leg, "reason": format!("{:?}", n.reason())}),
        RpcOutcome::Indeterminate(i) => json!({"outcome": out.name(), "route": leg, "reason": format!("{:?}", i.reason())}),
        RpcOutcome::Unserved(u) => json!({"outcome": out.name(), "route": leg, "reason": format!("{u:?}")}),
        RpcOutcome::RejectedStale(r) => json!({"outcome": out.name(), "route": leg, "target_node_id": r.target_node_id()}),
    })
}

#[tokio::main]
async fn main() {
    let _telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-rpc-probe");
    let args = match parse(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            println!("{}", json!({"outcome": "Refused", "reason": e}));
            std::process::exit(2);
        }
    };
    let span = tracing::info_span!("rdm.node_rpc.proof_store.resolve.via-probe", op = %args.op, target = %args.target);
    let out = {
        use tracing::Instrument;
        run(args).instrument(span).await
    };
    match out {
        Ok(v) => println!("{v}"),
        Err(e) => {
            println!("{}", json!({"outcome": "Refused", "reason": e}));
            std::process::exit(2);
        }
    }
}

/// `resolve --target <node> --query exact:<id>|path:<p>`: what the target node's own live
/// resolver says about the query.
async fn run_resolve(a: &Args, target: &NodeTarget) -> Result<Value, String> {
    use rafka_node_rpc_testkit::resolve_probe::{ProbeTarget, ResolveProbe, ResolveReply, ResolveRequest};
    let query = match a.key.split_once(':') {
        Some(("exact", id)) => ProbeTarget::Exact(id.to_string()),
        Some(("path", p)) => ProbeTarget::Path(p.to_string()),
        _ => return Err(format!("--query {:?} is neither exact:<node_id> nor path:<path.name>", a.key)),
    };
    let url = format!("{}/api/nodes", a.admin.trim_end_matches('/'));
    let view: Value = reqwest::get(&url).await.map_err(|e| format!("GET {url}: {e}"))?.json().await.map_err(|e| format!("GET {url}: {e}"))?;
    let resolver = Arc::new(StaticResolver::new());
    for n in resolved(&view) {
        resolver.insert(n);
    }
    let ep = rafka_node_rpc::endpoint::bind(iroh::SecretKey::generate(), probe_bind())
        .await
        .map_err(|e| format!("binding the probe's endpoint: {e}"))?;
    let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm");
    let req = ResolveRequest::Resolve { target: query };
    let (out, _) = client.call::<ResolveProbe>(target, &req, &CallOptions::default()).await;
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

/// Where the probe binds its one socket: `RAFKA_PROBE_BIND` (an address on the fabric's network,
/// e.g. a container fabric's gateway), else loopback.
fn probe_bind() -> std::net::SocketAddr {
    let ip: std::net::IpAddr = std::env::var("RAFKA_PROBE_BIND").ok().and_then(|v| v.parse().ok()).unwrap_or(std::net::IpAddr::from([127, 0, 0, 1]));
    std::net::SocketAddr::new(ip, 0)
}
