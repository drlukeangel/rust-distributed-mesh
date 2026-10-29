//! Fabric certs, tracked and renewed (i145 e3; fabric-certs.md §Tracking; node-cert-renew.md,
//! mesh-issuer-renew.md, node-issuer-update.md): every issue puts the birth's cert row; the
//! issuing authority's tick renews a member at its row's point (`node.cert.renew`), the
//! fabric-primary's tick re-mints a mesh's issuer at its row's point and carries it to the mesh's
//! admins (`mesh.issuer.renew`, `node.issuer.update`); the policy is the bootstrap admin's, carried
//! to every later admin in its entry pull; a stall is surfaced on `fabric.get`.
//!
//! The cells run a real Day-0 node-admin in this process, holding the testkit's fabric root, and
//! births deployed by hand (the admin reconciles nothing): its ticks and Status door are the
//! product's. Members register a product ALPN and are launched with the testkit's root set, so
//! their dials are admitted by `node.connections.admit` on it. A cell that needs a member's Node RPC
//! to fail cuts its admin's calls to it (the client's admission block, its pooled connection to the
//! member closed): the member gossips on, so nothing but the renewal is told about it.

use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::certs::CertChoice;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::certs::{self, SignedCert};
use rdm_core::launch::{Launch, Launcher};
use rdm_core::outcome::RpcOutcome;
use rdm_core::runtime::RuntimeFact;
use rdm_core::filter::{Entity, Filter};
use rdm_core::status::{Status, StatusReply, StatusRequest};
use rdm_core::views::{FabricView, NodeCertView, NodeStatus, NodeView, Renewals};
use rdm_core::{IncarnationId, NodeId};
use rdm_node::node::RunningNode;
use rdm_test::test_certs::{root_choice, test_root_public};
use rdm_transport::connections::{Alpn, Birth};
use rdm_transport::{CallOptions, NodeTarget};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// How long any one fact of these cells may take to appear.
const BOUND: Duration = Duration::from_secs(45);
/// The product ALPN every member registers.
const ALPN: &[u8] = b"cert-renewal/1";
/// The member TTL the renewal cells' fabric is bootstrapped with (`RDM_MEMBER_CERT_TTL_SECS`).
const MEMBER_TTL_SECS: u64 = 12;
/// The renew point's floor and ceiling as a share of a cert's life (fabric-certs.md §Tracking: the
/// default `RDM_CERT_RENEW_AT` of 0.67, plus a fixed jitter of up to 5 %), in thousandths.
const RENEW_FROM: u64 = 670;
const RENEW_TO: u64 = 720;
/// What a tick adds past a renew point before the renewal is issued: one gossip interval of the
/// cadence the cells run at, and the call.
const TICK_SLACK_MS: u64 = 2_000;

fn app_alpn() -> Alpn {
    Alpn { name: ALPN.to_vec(), proxy: false, carriers: vec![rdm_core::NodeKind::RpcNode] }
}

fn cell_dir(what: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("cert-renewal-{what}-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The cert policy the cells bootstrap with: the member TTL [`MEMBER_TTL_SECS`] and `more`.
fn policy(more: &[(&'static str, u64)]) -> Vec<(&'static str, String)> {
    let mut p = vec![(certs::ENV_MEMBER_CERT_TTL_SECS, MEMBER_TTL_SECS.to_string())];
    p.extend(more.iter().map(|(k, v)| (*k, v.to_string())));
    p
}

/// The Day-0 admin's environment: its dirs, a process fabric, and the cert policy `policy`.
fn day0_config(dir: &Path, policy: &[(&'static str, String)]) -> AdminConfig {
    let dir_s = dir.display().to_string();
    let policy: BTreeMap<&str, String> = policy.iter().cloned().collect();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir_s.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        k => policy.get(k).cloned(),
    })
    .expect("a readable environment")
}

/// The Day-0 admin of `fabric1`, holding the testkit's fabric root, its fabric committed (its store
/// root is the cells' shared store, `dir`), reconciling nothing.
async fn boot_root(dir: &Path, policy: &[(&'static str, String)]) -> Running {
    let cfg = day0_config(dir, policy);
    let identity = dir.join(cfg.fabric_id.to_string()).join(rdm_admin::store_root::FABRIC_FILE);
    let root = start_with(cfg, Wiring { certs: root_choice(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() }).await.expect("the Day-0 admin comes up");
    tokio::time::timeout(BOUND, async {
        while rdm_admin::store_log::read_record(&identity, "fabric-record", None).is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the Day-0 admin writes the fabric identity");
    root.stop_reconciling().await;
    root
}

/// The launch of a birth at `name` deployed by `by` (the admin that answers its JoinNode), with the
/// testkit's root set, on the fabric's shared `store`.
fn launch_by(by: &Running, store: &Path, dir: &Path, name: &str) -> Launch {
    let d = by.digest.lock().unwrap().clone();
    Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: "fabric1".into(),
        fabric_id: d.fabric_id.clone(),
        name: name.parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: dir.to_path_buf(),
        store_dir: store.to_path_buf(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: vec![test_root_public()],
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    }
}

/// `launch` told to `by` as the exact birth it deployed, the birth's runtime record and key in its dir.
fn deployed(by: &Running, launch: &Launch) {
    let fact = RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cert-renewal")).unwrap();
    fact.write_record(&launch.data_dir).unwrap();
    let key = rdm_node::node::load_or_mint_key(&launch.data_dir).unwrap();
    let _ = by.runner.joins.expect(rdm_admin::join::Deployed {
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rdm_core::EndpointId(key.public().to_string()),
        runtime: fact,
        data_dir: launch.data_dir.display().to_string(),
        last_accepted_name: None,
        held: false,
        listeners: Vec::new(),
    });
}

/// A member at `name`, deployed by `by`, with the product ALPN registered, started and joined.
async fn member(by: &Running, store: &Path, name: &str) -> RunningNode {
    let dir = cell_dir(name);
    let launch = launch_by(by, store, &dir, name);
    deployed(by, &launch);
    rdm_node::node::start_with_rafka_time_pre_node_ready_alpns_and(&launch, rdm_transport::clock::RafkaTime::unadopted(), rdm_node::node_hooks::NodeHooks::none(), |s| s, None, |b, _| b, &[], &[app_alpn()])
        .await
        .unwrap_or_else(|e| panic!("{name} starts: {e}"))
}

/// `mesh1.admin.2`, launched by `root` with its mesh's issuing material and the root beside it, the
/// testkit's root set and the fabric's shared store. Its environment is its launch's alone: it holds
/// no cert policy but the one its entry pull carries. Returns it with that environment.
async fn launched_admin(root: &Running, store: &Path) -> (Running, BTreeMap<String, String>) {
    let dir = cell_dir("mesh1.admin.2");
    let mut launch = launch_by(root, store, &dir, "mesh1.admin.2");
    launch.listeners = vec![("control".into(), "127.0.0.1:0".parse().unwrap())];
    launch.issuing = root.runner.issuer.issuing_material("mesh1").expect("the root holder hands mesh1's issuing material");
    deployed(root, &launch);
    let env = launch.to_env();
    let cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
    let certs = CertChoice::from_material(&dir, "mesh1", false, |k| env.get(k).cloned()).expect("the launch carries the admin's cert material");
    let admin = start_with(cfg, Wiring { certs, fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() }).await.expect("mesh1.admin.2 comes up");
    admin.stop_reconciling().await;
    (admin, env)
}

fn birth_of(n: &RunningNode) -> Birth {
    Birth { name: n.digest.node.name.clone(), node_id: n.digest.node.node_id.clone(), incarnation: n.digest.node.incarnation.clone() }
}

fn signed(bytes: &[u8]) -> SignedCert {
    certs::cert_from_bytes(bytes).expect("a signed member cert")
}

/// `node.get [id: node]` on `admin`'s Status authority.
async fn node_get(admin: &Running, node: &NodeId) -> Option<NodeView> {
    match admin.status_authority()?.apply(None, &StatusRequest::Get { entity: Entity::Nodes, node: None, filter: Filter::new().eq("id", [node.to_string()]) }).await {
        StatusReply::Nodes { mut nodes } => nodes.pop(),
        _ => None,
    }
}

/// `fabric.get` on `admin`'s Status authority.
async fn fabric_get(admin: &Running) -> Option<FabricView> {
    match admin.status_authority()?.apply(None, &StatusRequest::Get { entity: Entity::Fabrics, node: None, filter: Filter::new() }).await {
        StatusReply::Fabrics { mut fabrics } => fabrics.pop(),
        _ => None,
    }
}

/// `admin`'s Status call `req` to the exact birth `to`: its reply, or the outcome that is none.
async fn call(admin: &Running, to: &RunningNode, req: &StatusRequest) -> Result<StatusReply, String> {
    let (out, _) = admin.node_rpc.client.call::<Status>(&NodeTarget::ExactNode(to.digest.node.node_id.clone()), req, &CallOptions::default()).await;
    match out {
        RpcOutcome::Reply(r) => Ok(r.into_value()),
        other => Err(format!("{other:?}")),
    }
}

/// `admin`'s Node RPC calls to `to` end NotSent from now on: the client opens no new connection and
/// its pooled one to `to` is closed. `to` gossips on.
fn cut_calls(admin: &Running, to: &RunningNode) {
    let key: iroh::PublicKey = to.digest.node.endpoint_id.0.parse().expect("an endpoint key");
    admin.node_rpc.client.block_new_connections("the cell cuts this admin's calls");
    admin.node_rpc.client.close_pooled_to(&key, "the cell cuts this admin's calls");
}

/// Every span named `name` with its fields, in the order they began.
fn named(spans: &crate::common::Spans, name: &str) -> Vec<BTreeMap<String, String>> {
    let held = spans.0.lock().unwrap();
    let mut found: Vec<(u64, BTreeMap<String, String>)> = held.iter().filter(|(_, (n, _, _))| n == name).map(|(k, (_, _, f))| (*k, f.clone())).collect();
    found.sort_by_key(|(k, _)| *k);
    found.into_iter().map(|(_, f)| f).collect()
}

fn num(f: &BTreeMap<String, String>, k: &str) -> u64 {
    f.get(k).and_then(|v| v.parse().ok()).unwrap_or_else(|| panic!("no number `{k}` in {f:?}"))
}

/// The member renewals the authority spanned for `node` whose outcome is recorded, in order.
fn renewals_of(spans: &crate::common::Spans, node: &str) -> Vec<BTreeMap<String, String>> {
    named(spans, "rdm.node_admin.cert.update.via-renewal").into_iter().filter(|f| f.get("node").map(String::as_str) == Some(node) && f.contains_key("outcome")).collect()
}

/// The join issue of `node`, as its authority spanned it.
/// The latest cert a join issued `node`: every join of a birth is issued a fresh one (Q87), so a join
/// its node made again (an answer lost under load) leaves the row holding the later issue.
fn join_issue(spans: &crate::common::Spans, node: &str) -> BTreeMap<String, String> {
    named(spans, "rdm.node_admin.cert.create.via-join")
        .into_iter()
        .filter(|f| f.get("node").map(String::as_str) == Some(node) && f.get("outcome").map(String::as_str) == Some("issued"))
        .max_by_key(|f| f.get("issued_at_rafka_ms").and_then(|v| v.parse::<u64>().ok()).unwrap_or(0))
        .unwrap_or_else(|| panic!("no join issue spanned for {node}"))
}

/// Poll `f` until it answers, within `bound`; `what` names the fact on a timeout.
/// `f`, within [`BOUND`]: a wait that outlasts it fails by name, so a hang is a red line and never a
/// killed binary.
async fn bounded<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(BOUND, f).await.unwrap_or_else(|_| panic!("{what}: not within {BOUND:?}"))
}

async fn until<T>(what: &str, bound: Duration, mut f: impl AsyncFnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + bound;
    loop {
        if let Some(t) = f().await {
            return t;
        }
        assert!(tokio::time::Instant::now() < deadline, "{what}: not within {bound:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The renew window of a cert issued at `issued_at` expiring at `expires_at`: the policy's point plus
/// at most 5 % of its life, and the tick that issues it.
fn renew_window(issued_at: u64, expires_at: u64) -> std::ops::RangeInclusive<u64> {
    let life = expires_at - issued_at;
    issued_at + life * RENEW_FROM / 1000..=issued_at + life * RENEW_TO / 1000 + TICK_SLACK_MS
}

async fn stop_all(nodes: Vec<RunningNode>, admins: Vec<Running>) {
    for n in nodes {
        n.stop().await;
    }
    for a in admins {
        a.leave().await;
    }
}

// @feature: node-lifecycle
/// CONTRACT (fabric-certs.md §Tracking: "The issuer writes a cert row for the exact birth at every
/// issue and renew ... `node.get` surfaces it as `cert { issued_at, expires_at, issuer_expires_at }`"):
/// a JoinNode the admin accepts puts the birth's cert row before the answer: its node id, path.name,
/// incarnation, issued_at, expires_at, issuer_expires_at and renew_at, the renew point the policy's
/// fraction of the cert's life plus at most 5 %. `node.get` of the birth shows the row's three times,
/// and they are the cert's the birth holds.
#[test]
fn a_join_writes_the_births_cert_row_and_node_get_shows_it() {
    let cell = "a_join_writes_the_births_cert_row_and_node_get_shows_it";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = cell_dir("row");
        let root = boot_root(&dir, &[]).await;
        let a = member(&root, &dir, "mesh1.rpc.1").await;
        let issued = join_issue(&spans, "mesh1.rpc.1");
        let held = signed(&a.member_cert);
        let issued_at = num(&issued, "issued_at_rafka_ms");

        let shown = until("node.get of mesh1.rpc.1 shows its cert row", BOUND, async || node_get(&root, &a.digest.node.node_id).await.and_then(|v| v.cert)).await;
        assert_eq!(shown, NodeCertView { issued_at, expires_at: held.cert.expiry_ms, issuer_expires_at: held.issuer.cert.expiry_ms }, "node.get shows the cert the birth holds");

        let record = root.runner.nodes_storage.node(&a.digest.node.node_id).await.expect("the record read").expect("mesh1.rpc.1's record");
        let birth = record.birth(&a.digest.node.incarnation).unwrap_or_else(|| panic!("no facts of the exact birth on mesh1.rpc.1's record: {record:?}"));
        let row = birth.cert.clone().unwrap_or_else(|| panic!("no cert dates on mesh1.rpc.1's record: {record:?}"));
        assert_eq!(
            (birth.name.to_string(), row.issued_at, row.expires_at, row.issuer_expires_at),
            ("mesh1.rpc.1".to_string(), issued_at, held.cert.expiry_ms, held.issuer.cert.expiry_ms),
            "the record names the exact birth and its cert"
        );
        let life = row.expires_at - row.issued_at;
        assert!((row.issued_at + life * RENEW_FROM / 1000..=row.issued_at + life * RENEW_TO / 1000).contains(&row.renew_at), "renewed at the policy's point plus at most 5 %: {row:?}");
        stop_all(vec![a], vec![root]).await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}

// @feature: node-lifecycle
/// CONTRACT (node-cert-renew.md, When and the Observability cell: "a member whose TTL is set to seconds
/// in the test kit dials past its first expiry and is admitted, with both spans and the admission
/// fact"): under a 12 s member TTL the mesh-primary renews a member at its row's point, spanned
/// `via-renewal` (`installed`, the new expiry, the previous one) on the authority and `via-installed`
/// on the member. Past its first cert's expiry and the floor of skew, the member dials a peer that
/// admits on the fabric's root set and is admitted on the renewed cert.
#[test]
fn a_member_is_renewed_at_the_policys_point_and_dials_past_its_first_expiry_admitted() {
    let cell = "a_member_is_renewed_at_the_policys_point_and_dials_past_its_first_expiry_admitted";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = cell_dir("renewed");
        let root = boot_root(&dir, &policy(&[])).await;
        let a = member(&root, &dir, "mesh1.rpc.1").await;
        let b = member(&root, &dir, "mesh1.rpc.2").await;
        let first = signed(&a.member_cert);
        let issued_at = num(&join_issue(&spans, "mesh1.rpc.1"), "issued_at_rafka_ms");
        assert_eq!(first.cert.expiry_ms - issued_at, MEMBER_TTL_SECS * 1000, "the join's cert lives the policy's member TTL");

        let renewal = until("mesh1.rpc.1 is renewed", BOUND, async || renewals_of(&spans, "mesh1.rpc.1").into_iter().find(|f| f["outcome"] == "installed")).await;
        assert!(renew_window(issued_at, first.cert.expiry_ms).contains(&num(&renewal, "issued_at")), "renewed at its row's point: {renewal:?}");
        assert_eq!((num(&renewal, "expires_at") - num(&renewal, "issued_at"), num(&renewal, "previous_expires_at")), (MEMBER_TTL_SECS * 1000, first.cert.expiry_ms), "{renewal:?}");
        assert_eq!(renewal.get("incarnation_id"), Some(&a.digest.node.incarnation.to_string()), "renewed for the exact birth");
        until("mesh1.rpc.1 spans its install", BOUND, async || named(&spans, "rdm.node.cert.update.via-installed").into_iter().find(|f| f.get("node").map(String::as_str) == Some("mesh1.rpc.1"))).await;

        let past = first.cert.expiry_ms + rdm_core::cadence::staleness_floor().as_millis() as u64 + 200;
        until("rafka-time passes the first cert's expiry and its skew", BOUND, async || (a.rafka_time.now_ms() > past && b.rafka_time.now_ms() > past).then_some(())).await;
        a.data_connections_for_testkit().probe_direct(&birth_of(&b), ALPN).await.expect("the renewed member is admitted past its first cert's expiry");
        let admitted = named(&spans, "rdm.node.connections.create.via-admitted").into_iter().rfind(|f| f.get("source").map(String::as_str) == Some("mesh1.rpc.1")).expect("the admission fact");
        assert!(num(&admitted, "cert_expires_at") > first.cert.expiry_ms, "admitted on a cert expiring after the first: {admitted:?}");
        stop_all(vec![a, b], vec![root]).await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}

// @feature: node-lifecycle
/// CONTRACT, the control (node-cert-renew.md: "nothing is terminated, nothing reconnects, no status
/// changes and no gossip frame is published. A renewal is invisible to every other node"): a member
/// holding an open Direct connection is renewed. The connection is the same one after the install
/// and carries a new stream; what the member holds (`node.connection.get`) is unchanged; no
/// connection span is begun between the renewal and the install; the member's view on its admin and
/// its digest in its peer's view keep their status, incarnation, name and config.
#[test]
fn a_renewal_terminates_nothing_and_publishes_nothing() {
    let cell = "a_renewal_terminates_nothing_and_publishes_nothing";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = cell_dir("control");
        let root = boot_root(&dir, &policy(&[])).await;
        let a = member(&root, &dir, "mesh1.rpc.1").await;
        let b = member(&root, &dir, "mesh1.rpc.2").await;
        let (opener, _) = a.connections(ALPN).unwrap().split();
        let (_, mut accepts) = b.connections(ALPN).unwrap().split();
        // One open, paired with its accept by the connection row's id (the same at both ends). An open
        // that fails is named here, never retried blind: a retry would leave the acceptor holding the
        // first connection while the echo writes to the second, and the echo would wait on a stream
        // that never comes. Every wait is bounded, so a hang is a red line, never a killed binary.
        let destination = birth_of(&b);
        // The opener resolves the destination from its own view, which takes mesh1.rpc.2 eventually
        // (its gossip or its next read): the open waits for that fact under the cell's bound, and an
        // open before it is NotSent peer-unresolved, a red of the cell's order, not of the renewal.
        until("mesh1.rpc.1's view holds mesh1.rpc.2", BOUND, async || a.node_rpc.resolver.resolve_at(&NodeTarget::ExactNode(destination.node_id.clone()), std::time::Instant::now()).ok().map(|_| ())).await;
        let (opened, accepted) = tokio::join!(
            bounded("mesh1.rpc.1 opens a Direct connection to mesh1.rpc.2", opener.open(&destination)),
            bounded("mesh1.rpc.2 accepts a connection", accepts.accept())
        );
        let conn = opened.unwrap_or_else(|e| panic!("mesh1.rpc.1's open of a Direct connection to mesh1.rpc.2 failed: {e} ({e:?})"));
        let mut server = accepted.expect("the acceptor serves the connection");
        // An accepted connection that is not the opened one is dropped (closed) before the echo, and
        // the acceptor is read again for the opened one.
        while server.id() != conn.id() {
            drop(server);
            server = bounded("mesh1.rpc.2 accepts the opened connection", accepts.accept()).await.expect("the acceptor serves the opened connection");
        }
        let echo = async |label: &[u8]| {
            bounded(&format!("the echo of {:?} over the one connection", String::from_utf8_lossy(label)), async {
                let (mut send, mut recv) = conn.open_bi().await.unwrap();
                send.write_all(label).await.unwrap();
                send.finish().unwrap();
                let (mut s_send, mut s_recv) = server.accept_bi().await.unwrap();
                assert_eq!(s_recv.read_to_end(64).await.unwrap(), label);
                s_send.write_all(label).await.unwrap();
                s_send.finish().unwrap();
                assert_eq!(recv.read_to_end(64).await.unwrap(), label);
            })
            .await;
        };
        echo(b"before").await;
        // The open answers before its row lands (Q169): mesh1.rpc.1 holds the connection once its write
        // lands, and the renewal is compared against that.
        let opened_id = conn.id().to_string();
        let held_before = until("node.connection.get of mesh1.rpc.1 holds the open connection before the renewal", BOUND, async || {
            let held = call(&root, &a, &StatusRequest::Get { entity: Entity::Connections, node: Some((a.digest.node.node_id.clone(), a.digest.node.incarnation.clone())), filter: Filter::new() }).await;
            matches!(&held, Ok(StatusReply::Connections { connections }) if connections.connections.iter().any(|c| c.id == opened_id)).then_some(held)
        })
        .await;
        // The renewal is compared against a settled member: its admin holds it ready (a member's
        // first ready declaration can reach the admin before the admin's own start ends, and is owed again),
        // and mesh1.rpc.2 holds its digest, which it takes eventually, as mesh1.rpc.1 took its own above.
        let view_before = until("node.get of mesh1.rpc.1 holds it ready before the renewal", BOUND, async || node_get(&root, &a.digest.node.node_id).await.filter(|v| v.status == NodeStatus::Ready)).await;
        // The digest in the peer's view moves Pending to ready when the mesh-primary's projected
        // status reaches it (rdm.mesh.topology.update.via-projected-status), after the admin's row is written:
        // the renewal is compared against the peer's settled view of it, never its first digest.
        let (digest_before, _) = until("mesh1.rpc.2's view holds mesh1.rpc.1's digest ready", BOUND, async || b.topology_view.digest_view.get(a.digest.node.node_id.as_str()).filter(|(d, _)| d.status == rdm_core::digest::MemberStatus::Ready)).await;
        let connection_spans = |spans: &crate::common::Spans| spans.0.lock().unwrap().values().filter(|(n, _, _)| n.starts_with("rdm.node.connections.")).count();
        let before = connection_spans(&spans);

        until("mesh1.rpc.1 is renewed and installs it", BOUND, async || {
            let installed = renewals_of(&spans, "mesh1.rpc.1").iter().any(|f| f["outcome"] == "installed");
            let on_node = named(&spans, "rdm.node.cert.update.via-installed").iter().any(|f| f.get("node").map(String::as_str) == Some("mesh1.rpc.1"));
            (installed && on_node).then_some(())
        })
        .await;
        assert_eq!(connection_spans(&spans), before, "no connection was closed, refused or opened by the renewal");
        echo(b"after").await;
        assert_eq!(conn.id(), server.id(), "the same connection carries the stream after the install");
        let held_after = bounded("node.connection.get of mesh1.rpc.1 after the renewal", call(&root, &a, &StatusRequest::Get { entity: Entity::Connections, node: Some((a.digest.node.node_id.clone(), a.digest.node.incarnation.clone())), filter: Filter::new() })).await;
        assert_eq!(held_after, held_before, "what the node holds is unchanged");
        let view_after = bounded("node.get of mesh1.rpc.1 after the renewal", node_get(&root, &a.digest.node.node_id)).await.expect("mesh1.rpc.1 in the view");
        assert_eq!(
            (view_after.status, &view_after.incarnation_id, &view_after.name, &view_after.config, view_after.stopped),
            (view_before.status, &view_before.incarnation_id, &view_before.name, &view_before.config, view_before.stopped),
            "no status set, nothing restarted"
        );
        let (digest_after, _) = b.topology_view.digest_view.get(a.digest.node.node_id.as_str()).expect("mesh1.rpc.2 still holds mesh1.rpc.1's digest");
        assert_eq!(
            (digest_after.status, &digest_after.node.incarnation, &digest_after.node.name),
            (digest_before.status, &digest_before.node.incarnation, &digest_before.node.name),
            "the renewal is invisible to the peer"
        );
        bounded("the members and the root admin stop", stop_all(vec![a, b], vec![root])).await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}

// @feature: node-lifecycle
/// CONTRACT (node-cert-renew.md step 3 and Failure: "`NotSent` or `Indeterminate` is spanned and
/// retried on the next tick, until the old cert expires; nothing else happens ... A renewal that does
/// not reach the node is retried every tick and marks the node nothing"): while the mesh-primary's
/// calls to a member end NotSent, each tick spans one `via-renewal` `not-sent` for it, at least two
/// of them; the member stays ready in the view at the same incarnation, nothing held-running,
/// os-stopped or data-dir-lost is recorded of it, and its cert row is still the join's (Q141: a renewal
/// not answered writes nothing). Once the calls reach it,
/// the next tick installs the renewal.
#[test]
fn a_renewal_that_is_not_sent_is_retried_each_tick_and_marks_nothing() {
    let cell = "a_renewal_that_is_not_sent_is_retried_each_tick_and_marks_nothing";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = cell_dir("not-sent");
        // A 24 s TTL: the renewal is retried from its point to the held cert's expiry, which leaves
        // seconds to send it again once the calls reach the member.
        let root = boot_root(&dir, &policy(&[(certs::ENV_MEMBER_CERT_TTL_SECS, 24)])).await;
        let b = member(&root, &dir, "mesh1.rpc.1").await;
        cut_calls(&root, &b);
        let tries = until("two not-sent renewals of mesh1.rpc.1", BOUND, async || {
            let not_sent: Vec<_> = renewals_of(&spans, "mesh1.rpc.1").into_iter().filter(|f| f["outcome"] == "not-sent").collect();
            (not_sent.len() >= 2).then_some(not_sent)
        })
        .await;
        assert!(renewals_of(&spans, "mesh1.rpc.1").iter().all(|f| f["outcome"] == "not-sent"), "nothing reached it, nothing was installed");
        let view = node_get(&root, &b.digest.node.node_id).await.expect("mesh1.rpc.1 in the view");
        assert_eq!((view.status, view.incarnation_id.as_ref()), (NodeStatus::Ready, Some(&b.digest.node.incarnation)), "the member is marked nothing: {view:?}");
        assert!(view.data_dir_lost.is_none(), "no fact is recorded of it: {view:?}");
        let row = root.runner.nodes_storage.node(&b.digest.node.node_id).await.unwrap().and_then(|r| r.birth(&b.digest.node.incarnation).and_then(|f| f.cert.clone())).expect("the member's cert dates");
        assert_eq!(row.issued_at, num(&join_issue(&spans, "mesh1.rpc.1"), "issued_at_rafka_ms"), "a renewal not answered writes nothing: the row is still the join's after {tries:?}");

        root.node_rpc.client.admit_new_connections();
        let installed = until("the renewal is installed once the calls reach the member", BOUND, async || renewals_of(&spans, "mesh1.rpc.1").into_iter().find(|f| f["outcome"] == "installed")).await;
        assert_eq!(installed.get("incarnation_id"), Some(&b.digest.node.incarnation.to_string()));
        stop_all(vec![b], vec![root]).await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}

// @feature: node-lifecycle
/// CONTRACT (node-cert-renew.md Target: "a cert naming another birth is refused by name"; Reply: "A node
/// that cannot decode the cert answers a named refusal and keeps the one it holds"): `RenewCert` to a
/// member's exact birth carrying another birth's cert is answered `rejected-cert` naming both births,
/// and carrying bytes that are no cert, `rejected-cert` naming that; the member installs neither.
#[test]
fn a_cert_for_another_birth_is_refused_by_name() {
    let cell = "a_cert_for_another_birth_is_refused_by_name";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = cell_dir("another-birth");
        let root = boot_root(&dir, &[]).await;
        let a = member(&root, &dir, "mesh1.rpc.1").await;
        let b = member(&root, &dir, "mesh1.rpc.2").await;
        let to_a = |cert: Vec<u8>| StatusRequest::RenewCert { node_id: a.digest.node.node_id.clone(), incarnation: a.digest.node.incarnation.clone(), cert };

        match call(&root, &a, &to_a(b.member_cert.clone())).await {
            Ok(StatusReply::RejectedCert { reason }) => {
                assert!(reason.contains(b.digest.node.node_id.as_str()) && reason.contains(a.digest.node.node_id.as_str()), "the refusal names the birth the cert names and this one: {reason}");
            }
            other => panic!("another birth's cert must be refused by name: {other:?}"),
        }
        match call(&root, &a, &to_a(vec![1, 2, 3])).await {
            Ok(StatusReply::RejectedCert { reason }) => assert!(!reason.is_empty(), "the refusal names why"),
            other => panic!("bytes that are no cert must be refused by name: {other:?}"),
        }
        assert!(
            !named(&spans, "rdm.node.cert.update.via-installed").iter().any(|f| f.get("node").map(String::as_str) == Some("mesh1.rpc.1")),
            "the member installed neither"
        );
        stop_all(vec![a, b], vec![root]).await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// The estate cell's owner (the successor cell runs node-admin processes: a seat moves only between
/// births the product can stop).
fn estate_owner(test: &str) -> rdm_test::estate::Owner {
    rdm_test::estate::Owner {
        product: "mesh".into(),
        feature: "fabric-certs".into(),
        subfeature: "renewal".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn attr(sp: &serde_json::Value, k: &str) -> String {
    sp["attributes"][k].as_str().map(str::to_string).unwrap_or_else(|| sp["attributes"][k].to_string())
}

fn attr_num(sp: &serde_json::Value, k: &str) -> u64 {
    attr(sp, k).parse().unwrap_or_else(|_| panic!("no number `{k}` in {sp}"))
}

// @feature: node-lifecycle
/// CONTRACT (node-cert-renew.md Failure: "A successor authority folds the cert rows and continues the
/// schedule from them"; fabric-certs.md §Tracking: "nothing about renewal lives only in memory";
/// node-stop.md Seat holder: "A target holding a seat has it moved first by node.primary.update ...
/// and is stopped by its successor"): under a 60 s member TTL, `node.stop` of mesh1.admin.1, the
/// holder of both seats, moves them to mesh1.admin.2 and stops it. mesh1.rpc.1's next renewal is
/// mesh1.admin.2's, at the point its newest cert row names (the renew point of the issue it held at
/// the move, the policy's two thirds plus its fixed jitter), or at once when that point had passed:
/// never a schedule of its own from the move.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_mesh_primary_continues_the_schedule_from_the_rows() {
    use rdm_test::estate::{named, Estate};
    const CELL: &str = "a_successor_mesh_primary_continues_the_schedule_from_the_rows";
    const TTL_SECS: u64 = 60;
    let mut estate = Estate::bootstrap_with_env(estate_owner(CELL), "fabric1", "mesh1", &[(certs::ENV_MEMBER_CERT_TTL_SECS, "60")]).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1)]).await;
    let names: std::collections::BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1"].into_iter().map(str::to_string).collect();
    estate.settles_to_named(&names, Duration::from_secs(30)).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let member = estate.node_view("mesh1.rpc.1").await;
    let incarnation = member.incarnation_id.clone().expect("the member's birth");

    estate.enter(&Estate::path("mesh1.admin.2")).await;
    let mut stop = estate.nodes_obj().await.stop(&Estate::path("mesh1.admin.1")).await.expect("node.stop of mesh1.admin.1 is accepted");
    let accepted = stop.accepted().clone();
    if matches!(Estate::frames_of(&mut stop).await.last(), Some(rdm_client::Frame::FabricPrimaryMoved { .. })) {
        estate.resume(&accepted.build_id, accepted.attempt).await;
    }
    estate.until_node("mesh1.admin.2", "mesh1.admin.2 holds both seats", Duration::from_secs(90), |n| n.filter(|n| n.is_primary && n.is_fabric_primary).map(|_| ())).await;
    let moved_at_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;

    // The issue the member held at the move: its join's, or the newest renewal it received.
    let held = |spans: &[serde_json::Value]| -> (u64, u64) {
        let joined = named(spans, "rdm.node_admin.cert.create.via-join").into_iter().filter(|sp| attr(sp, "node") == "mesh1.rpc.1" && attr(sp, "incarnation_id") == incarnation.to_string() && attr(sp, "outcome") == "issued").map(|sp| (attr_num(sp, "issued_at_rafka_ms"), attr_num(sp, "expires_at_rafka_ms")));
        let renewed = named(spans, "rdm.node_admin.cert.update.via-renewal").into_iter().filter(|sp| attr(sp, "node") == "mesh1.rpc.1" && attr(sp, "outcome") == "installed" && sp["start_unix_nano"].as_u64().unwrap_or(0) < moved_at_ns).map(|sp| (attr_num(sp, "issued_at"), attr_num(sp, "expires_at")));
        joined.chain(renewed).max().expect("the member was issued a cert")
    };
    let next = wait_for_renewal(&estate, moved_at_ns, Duration::from_secs(TTL_SECS + 30)).await;
    let spans = estate.spans();
    let (issued_at, expires_at) = held(&spans);
    let point = certs::renew_at(&member.node_id, &incarnation, issued_at, expires_at, 670);
    let at = attr_num(&next, "issued_at");
    assert_eq!(attr_num(&next, "previous_expires_at"), expires_at, "the successor continues from the issue the member held: {next}");
    assert!(at >= point, "never before the point the row names ({point}): {next}");
    // rafka-time is the Day-0 root's OS clock lineage: the move's instant on it is the host's, within
    // the slack.
    let moved_ms = moved_at_ns / 1_000_000;
    assert!(at <= point.max(moved_ms) + TICK_SLACK_MS + 2_000, "at the row's point ({point}), or at once when it had passed by the move ({moved_ms}): {next}");
    estate.stop().await;
}

/// The first `via-renewal` of mesh1.rpc.1 installed after `moved_at_ns`: the successor's.
async fn wait_for_renewal(estate: &rdm_test::estate::Estate, moved_at_ns: u64, within: Duration) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let spans = estate.spans();
        if let Some(sp) = rdm_test::estate::named(&spans, "rdm.node_admin.cert.update.via-renewal").into_iter().find(|sp| attr(sp, "node") == "mesh1.rpc.1" && attr(sp, "outcome") == "installed" && sp["start_unix_nano"].as_u64().unwrap_or(0) > moved_at_ns) {
            return sp.clone();
        }
        assert!(tokio::time::Instant::now() < deadline, "mesh1.admin.2 renewed mesh1.rpc.1 within {within:?} of taking the seats");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// @feature: node-lifecycle
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:5
/// "`mesh.issuer.renew` re-mints a mesh's issuing cert before it expires and carries it to that mesh's node-admins. It is run by the fabric-primary on its own tick, once the issuing cert has lived `RDM_CERT_RENEW_AT` (default two thirds) of its TTL (`RDM_ISSUING_CERT_TTL_SECS`, default 30 days). It is not a Build and not a caller op. Member certs issued after it carry the new issuer; the old issuer, and every member cert it signed, stay valid to their own expiry, so nothing is cut and nothing reconnects."
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:15
/// "| Runner | The fabric-primary, which holds the fabric root |"
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:16
/// "| Target | One mesh; every node-admin of it, exact births, in the view |"
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:18
/// "| Order | The mesh-primary first (the fabric-primary's own mesh: installed in-process, with no call, spanned `via-installed`), then the others; a member renewal due in the window is signed with the new key once its own admin holds it |"
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:20
/// "| Never | A member cert re-issued by this workflow (that is [node.cert.renew](node-cert-renew.md)'s on its own schedule), the root rotated (a fabric re-birth), a status set, a frame published |"
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:21
/// "| Spans | `rdm.node_admin.issuer.create.via-renewal` (mesh, issuer_expires_at, previous_issuer_expires_at, admins, outcome) on the fabric-primary, and each `node.issuer.update`'s spans |"
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:25
/// "1. The fabric-primary's tick reads the signing cert dates it holds in its cache, the dates it issued and each admin's `IssuerUpdated` confirmed; a mesh with a row whose `renew_at` has passed, and no newer issuing cert held, is renewed. It mints the issuing cert for that mesh with the root, on rafka-time, and holds it in process."
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:30
/// "3. Each admin signs member certs with the new key from its install. [node.cert.renew](node-cert-renew.md) runs on its own schedule; a member whose cert would outlive the old issuer is renewed early, cut to the issuer's expiry (`MemberOutlivesIssuer`), which is what forces this workflow to run first."
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:33
/// "4. Each admin's record holds the dates it answered, written by its mesh-primary. The old issuer expires on its own."
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:61
/// "Trace propagation, step spans and correlation follow the [runtime spans](../node-rpc-envelope.md#runtime-spans)."
/// CONTRACT (mesh-issuer-renew.md; node-issuer-update.md; node-cert-renew.md "members renewed from then
/// on carry the new issuer, and members not yet due keep the old one until their own turn";
/// fabric-certs.md §chain): under a 48 s issuing TTL the fabric-primary re-mints mesh1's issuer at its
/// row's point (`rdm.node_admin.issuer.create.via-renewal`) and carries it to mesh1.admin.2
/// (`UpdateIssuer`, its `via-renewal` `installed`, the admin's `via-installed`). A member that admin
/// deploys afterwards is signed by the new issuer; a member that joined before holds the old one's
/// cert (its renewal does not reach it), and admits the new member's dial on the fabric's root set.
#[test]
fn an_issuer_is_rotated_and_a_member_signed_by_the_new_issuer_is_admitted_by_a_peer_holding_the_old() {
    let cell = "an_issuer_is_rotated_and_a_member_signed_by_the_new_issuer_is_admitted_by_a_peer_holding_the_old";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        const ISSUING_TTL_SECS: u64 = 48;
        let dir = cell_dir("rotation");
        let root = boot_root(&dir, &policy(&[(certs::ENV_ISSUING_CERT_TTL_SECS, ISSUING_TTL_SECS), (certs::ENV_MEMBER_CERT_TTL_SECS, 120)])).await;
        // The mesh's first join mints its issuer (`mesh.issuer.create`) and puts the issuer row.
        let first = member(&root, &dir, "mesh1.rpc.3").await;
        let (admin2, env) = launched_admin(&root, &dir).await;
        let old = certs::decode_issuing_cert(&env[rdm_core::launch::ENV_ISSUING_CERT]).expect("mesh1's issuing cert");
        let minted_at = old.cert.expiry_ms - ISSUING_TTL_SECS * 1000;
        // The holder of the old issuer's cert joins halfway through its issuer's life. The issuer is
        // re-minted at 67-72 % of its life (RDM_CERT_RENEW_AT plus a jitter of up to 5 %), so the join
        // has 17-22 % of 48 s (8-10 s) to finish under the old issuer. The holder's own cert, capped at
        // the issuer's expiry, is renewed at 67-72 % of what is left (84-86 % of the issuer's life),
        // after its calls are cut below.
        until("rafka-time is 50 % into the old issuer's life", BOUND, async || (root.rafka_time.now_ms() >= minted_at + ISSUING_TTL_SECS * 500).then_some(())).await;
        let holder = member(&root, &dir, "mesh1.rpc.1").await;
        assert_eq!(signed(&holder.member_cert).issuer, old, "it joined under the old issuer");

        let rotated = until("the fabric-primary re-mints mesh1's issuer", BOUND, async || named(&spans, "rdm.node_admin.issuer.create.via-renewal").into_iter().find(|f| f.get("mesh").map(String::as_str) == Some("mesh1") && f.contains_key("outcome"))).await;
        assert_eq!(num(&rotated, "previous_issuer_expires_at"), old.cert.expiry_ms, "{rotated:?}");
        until("mesh1.admin.2 installs the new issuer", BOUND, async || named(&spans, "rdm.node_admin.issuer.update.via-installed").into_iter().find(|f| f.get("node").map(String::as_str) == Some("mesh1.admin.2"))).await;
        // From here the old issuer's holder is not reached by its own renewal.
        cut_calls(&root, &holder);
        let update = until("the fabric-primary's update of mesh1.admin.2 is received", BOUND, async || {
            named(&spans, "rdm.node_admin.issuer.update.via-renewal").into_iter().find(|f| f.get("node").map(String::as_str) == Some("mesh1.admin.2") && f.get("outcome").map(String::as_str) == Some("installed"))
        })
        .await;
        assert_eq!(num(&update, "issuer_expires_at"), num(&rotated, "issuer_expires_at"));

        let newer = member(&admin2, &dir, "mesh1.rpc.2").await;
        let new_issuer = signed(&newer.member_cert).issuer;
        assert_ne!(new_issuer.cert.issuer_pub, old.cert.issuer_pub, "mesh1.admin.2 signs with the new issuing key");
        assert_eq!((new_issuer.cert.expiry_ms, new_issuer.cert.mesh.as_str()), (num(&rotated, "issuer_expires_at"), "mesh1"));
        assert_eq!(certs::verify_issuer(&new_issuer, &[test_root_public()]), Ok(()), "the new issuer chains to the root");
        newer.data_connections_for_testkit().probe_direct(&birth_of(&holder), ALPN).await.expect("the old issuer's holder admits the new issuer's member");
        assert!(!named(&spans, "rdm.node.cert.update.via-installed").iter().any(|f| f.get("node").map(String::as_str) == Some("mesh1.rpc.1")), "the holder still holds the old issuer's cert");
        let admitted = named(&spans, "rdm.node.connections.create.via-admitted").into_iter().rfind(|f| f.get("destination").map(String::as_str) == Some("mesh1.rpc.1")).expect("the admission fact");
        assert_eq!(num(&admitted, "cert_expires_at"), signed(&newer.member_cert).cert.expiry_ms);
        stop_all(vec![holder, newer, first], vec![admin2, root]).await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}

// @feature: node-lifecycle
/// CONTRACT (fabric-certs.md §Tracking: "The TTLs and the renewal point are fabric policy, read once by
/// the bootstrap admin and fixed for the fabric, carried to every later admin in its entry pull the way
/// `MESH_SPAWN_TYPE` is"): the bootstrap admin is started with a 12 s member TTL; mesh1.admin.2's
/// environment is its launch's alone and names no policy. Each admin issues the member it deploys a
/// cert that lives 12 s.
#[test]
fn two_admins_read_the_same_policy_from_the_entry_pull() {
    let cell = "two_admins_read_the_same_policy_from_the_entry_pull";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = cell_dir("policy");
        let root = boot_root(&dir, &policy(&[])).await;
        let (admin2, env) = launched_admin(&root, &dir).await;
        for k in [certs::ENV_MEMBER_CERT_TTL_SECS, certs::ENV_ISSUING_CERT_TTL_SECS, certs::ENV_CA_CERT_TTL_SECS, certs::ENV_CERT_RENEW_AT] {
            assert!(!env.contains_key(k), "mesh1.admin.2's launch names no policy ({k})");
        }
        let by_root = member(&root, &dir, "mesh1.rpc.1").await;
        let by_admin2 = member(&admin2, &dir, "mesh1.rpc.2").await;
        for (node, by) in [("mesh1.rpc.1", "mesh1.admin.1"), ("mesh1.rpc.2", "mesh1.admin.2")] {
            let issued = join_issue(&spans, node);
            assert_eq!(num(&issued, "expires_at_rafka_ms") - num(&issued, "issued_at_rafka_ms"), MEMBER_TTL_SECS * 1000, "{by} issued {node} a cert of the bootstrap admin's member TTL: {issued:?}");
        }
        stop_all(vec![by_root, by_admin2], vec![admin2, root]).await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}

// @feature: node-lifecycle
/// CONTRACT (fabric-certs.md §Tracking: "`fabric.get` shows the next expiry across the fabric, and a
/// mesh whose renewals stall (a row past `renew_at` by more than a floor with no `node.cert.renewed`) is
/// surfaced on `fabric.get` as `renewals: stalled`"): with one member, `fabric.get` shows as the next
/// expiry the sooner of its cert's and the Day-0 root's own (fabric-certs.md, When a cert is issued:
/// the first node-admin of a mesh, by its maker), and mesh1's renewals not stalled. While the
/// mesh-primary's calls to the member end NotSent, once its row is past its renew point by more than
/// a floor, mesh1 is shown `stalled` naming it alone (the root renews itself in process), and the
/// member's term of the next expiry is still its join cert's. Once the renewal reaches it, the stall
/// is gone and the member's term is the renewed cert's.
#[test]
fn a_mesh_whose_renewals_stall_has_its_row_surfaced_on_fabric_get() {
    let cell = "a_mesh_whose_renewals_stall_has_its_row_surfaced_on_fabric_get";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = cell_dir("stall");
        // A 40 s TTL: the stall shows one floor past the renew point, and the renewal is still retried
        // until the held cert expires, seconds later.
        let root = boot_root(&dir, &policy(&[(certs::ENV_MEMBER_CERT_TTL_SECS, 40)])).await;
        let b = member(&root, &dir, "mesh1.rpc.1").await;
        let joined = signed(&b.member_cert).cert.expiry_ms;
        let mesh1 = |f: &FabricView| f.meshes.iter().find(|m| m.name == "mesh1").map(|m| m.renewals.clone());
        let me = root.birth();
        // The next expiry across the fabric with the member's cert expiring at `member`: the sooner of
        // it and the Day-0 root's own, as its record holds it now.
        let next = async |member: u64| {
            let own = root.runner.nodes_storage.node(&me.node_id).await.unwrap().and_then(|r| r.birth(&me.incarnation).and_then(|f| f.cert.clone())).expect("the Day-0 root's own cert dates");
            Some(member.min(own.expires_at))
        };
        let shown = until("fabric.get shows the next expiry across the member's and the root's certs", BOUND, async || {
            let f = fabric_get(&root).await?;
            (f.next_expiry == next(joined).await).then_some(f)
        })
        .await;
        assert_eq!(mesh1(&shown), Some(None), "nothing stalls");
        cut_calls(&root, &b);

        let row = root.runner.nodes_storage.node(&b.digest.node.node_id).await.unwrap().and_then(|r| r.birth(&b.digest.node.incarnation).and_then(|f| f.cert.clone())).expect("the member's cert dates");
        let stalled = until("fabric.get shows mesh1's renewals stalled", BOUND, async || {
            let f = fabric_get(&root).await?;
            (matches!(mesh1(&f), Some(Some(_))) && f.next_expiry == next(joined).await).then_some(f)
        })
        .await;
        let floor = rdm_core::cadence::staleness_floor().as_millis() as u64;
        assert!(root.rafka_time.now_ms() > row.renew_at + floor, "stalled only past the renew point by more than a floor");
        assert_eq!(mesh1(&stalled), Some(Some(Renewals::Stalled { nodes: vec!["mesh1.rpc.1".parse().unwrap()] })), "the member alone stalls: {stalled:?}");

        root.node_rpc.client.admit_new_connections();
        let renewed = until("the renewal reaches the member", BOUND, async || renewals_of(&spans, "mesh1.rpc.1").into_iter().find(|f| f["outcome"] == "installed")).await;
        let renewed_expiry = num(&renewed, "expires_at");
        until("the stall is gone from fabric.get, the member's term the renewed cert's", BOUND, async || {
            let f = fabric_get(&root).await?;
            (mesh1(&f) == Some(None) && f.next_expiry == next(renewed_expiry).await).then_some(f)
        })
        .await;
        stop_all(vec![b], vec![root]).await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// The first span of `estate` named `name` that `wanted` says yes to, within `within`.
async fn estate_span(estate: &rdm_test::estate::Estate, name: &str, what: &str, within: Duration, wanted: impl Fn(&serde_json::Value) -> bool) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let spans = estate.spans();
        if let Some(sp) = rdm_test::estate::named(&spans, name).into_iter().find(|sp| wanted(sp)) {
            return sp.clone();
        }
        assert!(tokio::time::Instant::now() < deadline, "{what}: not within {within:?}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// @feature: node-lifecycle
/// CONTRACT (node-cert-renew.md Caller: "its mesh-primary for a member, the fabric-primary for a
/// mesh-primary"; Row: "A mesh-primary's own cert, issued by the fabric-primary, it writes on its own
/// record when it installs it"; Q141, Q241: the row holds the cert's own dates, never a clock's): under
/// a 30 s member TTL, the fabric-primary (mesh1.admin.1) renews mesh2's primary, spanned `via-renewal`
/// `installed` for it. `node.get` shows that issue's dates exactly, issued_at and expires_at, at the
/// fabric-primary and at mesh2's primary, which wrote them on its own row: two admins on two clocks,
/// one set of dates (rdm#112: mesh2's row held its own clock, 1 ms behind, as issued_at).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fabric_primary_renews_another_meshs_primary_and_puts_its_rows_through_that_meshs_door() {
    use rdm_test::estate::Estate;
    const CELL: &str = "the_fabric_primary_renews_another_meshs_primary_and_puts_its_rows_through_that_meshs_door";
    let estate = Estate::bootstrap_with_env(estate_owner(CELL), "fabric1", "mesh1", &[(certs::ENV_MEMBER_CERT_TTL_SECS, "30")]).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 0), ("mesh2", 1, 0)]).await;
    let names: std::collections::BTreeSet<String> = ["mesh1.admin.1", "mesh2.admin.1"].into_iter().map(str::to_string).collect();
    estate.settles_to_named(&names, Duration::from_secs(30)).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let primary = estate.node_view("mesh2.admin.1").await;
    assert!(primary.is_primary && !primary.is_fabric_primary, "mesh2.admin.1 is mesh2's primary and not the fabric's: {primary:?}");

    let renewed = estate_span(&estate, "rdm.node_admin.cert.update.via-renewal", "the fabric-primary renews mesh2.admin.1", Duration::from_secs(60), |sp| attr(sp, "node") == "mesh2.admin.1" && attr(sp, "outcome") == "installed").await;
    let (issued_at, expires_at) = (attr_num(&renewed, "issued_at"), attr_num(&renewed, "expires_at"));
    for at in ["mesh1.admin.1", "mesh2.admin.1"] {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let shown = estate.node_views_at(&Estate::path(at)).await.and_then(|vs| vs.into_iter().find(|v| v.name.to_string() == "mesh2.admin.1")).and_then(|v| v.cert);
            if shown.as_ref().is_some_and(|c| (c.issued_at, c.expires_at) == (issued_at, expires_at)) {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "node.get at {at} shows mesh2.admin.1's renewed issue's dates ({issued_at}, {expires_at}): {shown:?}");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    let mut estate = estate;
    estate.stop().await;
}

// @feature: node-lifecycle
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:15
/// "| Runner | The fabric-primary, which holds the fabric root |"
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:16
/// "| Target | One mesh; every node-admin of it, exact births, in the view |"
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:17
/// "| Steps | Mint the issuing cert, then [node.issuer.update](node-issuer-update.md) to the mesh-primary, then to each other admin as a command forwarded through the mesh-primary; the step keys are `mesh.issuer.renew:<mesh>` and `node.issuer.update:<path.name>` |"
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:18
/// "| Order | The mesh-primary first (the fabric-primary's own mesh: installed in-process, with no call, spanned `via-installed`), then the others; a member renewal due in the window is signed with the new key once its own admin holds it |"
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:19
/// "| Receipt | On each admin's `IssuerUpdated` its mesh-primary writes its signing cert issue date and expiry on the admin's record (its own at its install); the workflow is complete when every admin in the view holds the new dates |"
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:21
/// "| Spans | `rdm.node_admin.issuer.create.via-renewal` (mesh, issuer_expires_at, previous_issuer_expires_at, admins, outcome) on the fabric-primary, and each `node.issuer.update`'s spans |"
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:28
/// "2. [node.issuer.update](node-issuer-update.md) to the mesh-primary, then to each other node-admin of the mesh as a command forwarded through the mesh-primary ([Forward 0x1A](../node-rpc-envelope.md#forward-op-0x1a-core), the mesh-primary the carrier). An admin not reached is retried on the next tick until the old issuer expires."
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:33
/// "4. Each admin's record holds the dates it answered, written by its mesh-primary. The old issuer expires on its own."
/// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:61
/// "Trace propagation, step spans and correlation follow the [runtime spans](../node-rpc-envelope.md#runtime-spans)."
/// CONTRACT (fabric-authority.md §1.1: "a command the fabric-primary originates for a mesh's node
/// reaches it as a forwarded command through its mesh-primary"; node-issuer-update.md; Luke
/// 2026-10-19): under a 45 s issuing TTL the fabric-primary (mesh1.admin.1) re-mints mesh2's issuer and
/// sends `node.issuer.update` to mesh2's primary directly; once that receipt is held it sends the same
/// update to mesh2's other admin as a Forward (0x1A) whose exact carrier is mesh2's primary and whose
/// inner call is Status (0x1B) `UpdateIssuer`. The fabric-primary originates and records both: each hop's
/// `via-renewal` names it as `sender` and the receiver `installed`. The secondary's install names mesh2's
/// primary as its `sender` (the carrier); the forward and the carried inner call are in the trace of the
/// fabric-primary's update to the secondary, and mesh2's primary, the secondary's writer, writes the
/// signing cert dates the secondary answered on its record as they return through it (`via-carried`
/// `written`). Must NOT happen: an update originated by mesh2's primary
/// alone, or the fabric-primary calling mesh2's secondary admin directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_issuer_update_reaches_a_secondary_admin_through_its_mesh_primary() {
    use rdm_test::estate::{named, Estate};
    const CELL: &str = "an_issuer_update_reaches_a_secondary_admin_through_its_mesh_primary";
    let estate = Estate::bootstrap_with_env(estate_owner(CELL), "fabric1", "mesh1", &[(certs::ENV_ISSUING_CERT_TTL_SECS, "45")]).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 0), ("mesh2", 2, 0)]).await;
    let names: std::collections::BTreeSet<String> = ["mesh1.admin.1", "mesh2.admin.1", "mesh2.admin.2"].into_iter().map(str::to_string).collect();
    let views = estate.settles_to_named(&names, Duration::from_secs(30)).await;
    let mesh2_admins: Vec<_> = views.iter().filter(|v| v.mesh == "mesh2").collect();
    let primary = mesh2_admins.iter().find(|v| v.is_primary).cloned().expect("mesh2 holds a primary");
    let secondary = mesh2_admins.iter().find(|v| !v.is_primary).cloned().expect("mesh2 holds a secondary admin");
    let (primary_name, secondary_name) = (primary.name.to_string(), secondary.name.to_string());
    let (primary_id, secondary_id) = (primary.node_id.to_string(), secondary.node_id.to_string());

    let rotated = estate_span(&estate, "rdm.node_admin.issuer.create.via-renewal", "the fabric-primary re-mints mesh2's issuer", Duration::from_secs(90), |sp| attr(sp, "mesh") == "mesh2" && attr(sp, "outcome") == "minted").await;
    let expiry = attr(&rotated, "issuer_expires_at");
    let installed = |node: String, sender: String| {
        let expiry = expiry.clone();
        move |sp: &serde_json::Value| attr(sp, "node") == node && attr(sp, "sender") == sender && attr(sp, "issuer_expires_at") == expiry
    };
    let received_from_the_fabric_primary = |node: String| {
        let expiry = expiry.clone();
        move |sp: &serde_json::Value| attr(sp, "node") == node && attr(sp, "sender") == "mesh1.admin.1" && attr(sp, "outcome") == "installed" && attr(sp, "issuer_expires_at") == expiry
    };
    estate_span(&estate, "rdm.node_admin.issuer.update.via-installed", "mesh2's primary installs the update the fabric-primary sent", Duration::from_secs(30), installed(primary_name.clone(), "mesh1.admin.1".into())).await;
    estate_span(&estate, "rdm.node_admin.issuer.update.via-renewal", "the fabric-primary records mesh2's primary's receipt", Duration::from_secs(30), received_from_the_fabric_primary(primary_name.clone())).await;
    estate_span(&estate, "rdm.node_admin.issuer.update.via-installed", "mesh2's secondary installs the update its primary carried", Duration::from_secs(30), installed(secondary_name.clone(), primary_name.clone())).await;
    let to_secondary = estate_span(&estate, "rdm.node_admin.issuer.update.via-renewal", "the fabric-primary records the secondary's receipt", Duration::from_secs(30), received_from_the_fabric_primary(secondary_name.clone())).await;
    let trace = to_secondary["trace_id"].as_str().unwrap_or_default().to_string();
    // Each process writes its own spans: the carrier's are on disk when it flushes them, not when the
    // fabric-primary's receipt is, so each is waited for, bounded, in the update's trace.
    let in_this_trace = |want: &'static str, op: &'static str, peer: String| {
        let trace = trace.clone();
        move |sp: &serde_json::Value| sp["trace_id"].as_str() == Some(trace.as_str()) && attr(sp, want) == op && attr(sp, "target").contains(&peer)
    };
    estate_span(&estate, "rdm.node_rpc.request.update.via-call", "the fabric-primary's update to the secondary is a Forward (0x1A) whose carrier is mesh2's primary", Duration::from_secs(10), in_this_trace("op", "26", primary_id.clone())).await;
    estate_span(&estate, "rdm.node_rpc.request.serve.via-carried-inner", "mesh2's primary carries the Status (0x1B) UpdateIssuer to the secondary", Duration::from_secs(10), in_this_trace("inner_op", "27", secondary_id.clone())).await;
    // The carrier is the secondary's writer: the dates its IssuerUpdated answered are written on the
    // secondary's record by mesh2's primary as they return through it (node-issuer-update.md step 4).
    estate_span(&estate, "rdm.node_admin.issuer.update.via-carried", "mesh2's primary writes the dates the secondary answered on its record", Duration::from_secs(10), {
        let (secondary_name, primary_name, expiry) = (secondary_name.clone(), primary_name.clone(), expiry.clone());
        move |sp: &serde_json::Value| attr(sp, "node") == secondary_name && attr(sp, "carrier") == primary_name && attr(sp, "issuer_expires_at") == expiry && attr(sp, "outcome") == "written"
    }).await;
    let spans = estate.spans();
    let alone: Vec<_> = named(&spans, "rdm.node_admin.issuer.update.via-renewal").into_iter().filter(|sp| attr(sp, "node") == secondary_name && attr(sp, "sender") != "mesh1.admin.1").collect();
    assert!(alone.is_empty(), "no update to the secondary is originated by anyone but the fabric-primary: {alone:#?}");
    let direct: Vec<_> = named(&spans, "rdm.node_admin.issuer.update.via-installed").into_iter().filter(|sp| attr(sp, "node") == secondary_name && attr(sp, "sender") == "mesh1.admin.1").collect();
    assert!(direct.is_empty(), "the fabric-primary never calls mesh2's secondary admin directly: {direct:#?}");
    estate.record_trace_url(&trace);
    let mut estate = estate;
    estate.stop().await;
}

// @feature: node-lifecycle
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:5
/// "Two levels sit above a mesh's issuers ([the chain](../fabric-certs.md#the-chain)): the **fabric root**, the trust anchor the embedding app passed in, which signs nothing but RDM's CA; and the **fabric CA**, RDM's own intermediate, signed by the root, which signs every mesh's issuing cert."
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:10
/// "- `fabric.ca.renew` rotates the CA internally, on policy, with nothing pushed to any node but the ordinary issuer and member renewals: the new CA chains to the same anchor, so every verifier's trust set is unchanged."
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:13
/// "- `fabric.root.update` rotates the anchor itself, only on the app's or a person's call with a new root, through an overlap in which every node trusts both anchors (`node.roots.update`), and retires the old one once nothing chains to it."
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:17
/// "Nothing is cut and nothing reconnects at any step of either."
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:23
/// "| Runner | The fabric-primary, which holds the root secret (as every node-admin does, [custody](../fabric-certs.md#custody)) |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:24
/// "| When | Its own tick, once the CA has lived `RDM_CERT_RENEW_AT` of its TTL (`RDM_CA_CERT_TTL_SECS`, default 365 days); also run by `fabric.root.update` step 3, under the new anchor |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:25
/// "| Steps | 1 mint a new CA keypair and sign its cert with the root; write the CA row; 2 [mesh.issuer.renew](mesh-issuer-renew.md) for every mesh under the new CA, each `node.issuer.update` carrying the new CA material; members renew under the new issuers at their own points |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:28
/// "| Receipt | The CA row carries each mesh's issuer receipt; complete when every mesh in the view has one |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:30
/// "| Spans | `rdm.node_admin.ca.update.via-renewal` (ca_expires_at, previous_ca_expires_at, meshes, outcome) on the fabric-primary, and each `mesh.issuer.renew`'s spans |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:36
/// "| Caller | The embedding app or a person, to the fabric-primary; a single call |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:37
/// "| Request | `fabric.root.update { secret }`, the new anchor's secret |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:38
/// "| Reply | `fabric.root.updated` once the rotation is accepted (the roots row `minted`); its progress is `fabrics.get`'s `roots { phase, expires_at }`. Refused by name while a rotation is in flight (`rotation-in-progress`) and for a root already trusted (`root-already-trusted`) |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:39
/// "| Phases | `minted` → `distributed` (both anchors trusted everywhere) → `ca-rotated` → `retired` (one anchor again); the roots row is durable and a successor continues from it |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:40
/// "| Steps | 1 write the new root secret beside the old one on the fabric-primary; row `minted`; 2 `node.roots.update { roots: [old, new] }` to every node, admins first (the secret to admins, the public set to every other node); row `distributed`; 3 `fabric.ca.renew` under the new anchor, which re-mints every issuer; row `ca-rotated`; 4 wait until every cert row chains to the new anchor (at most one member TTL after step 3); 5 `node.roots.update { roots: [new] }` to every node; row `retired`; the old secret is destroyed everywhere |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:41
/// "| Bound | The old anchor's own expiry: a rotation not retired by then is surfaced on `fabrics.get` as `roots: stalled` |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:42
/// "| Never | A cert cut short, a status set, a frame published, a connection closed; a node that has not yet received a phase keeps the set it holds, which admits both anchors' chains throughout |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:43
/// "| Spans | `rdm.node_admin.root.update.via-caller` (phase, root_expires_at, previous_root_expires_at, nodes, outcome) on the fabric-primary; `rdm.node.roots.update.via-installed` (node, roots, phase, secret ∈ {`carried`, `public-only`}) on each node |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:49
/// "| Caller | The fabric-primary |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:50
/// "| Target | One node, by exact `(node.id, incarnation)` |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:51
/// "| Request | `UpdateRoots { node_id, incarnation, roots: [root public key, …], phase, secret? }` on [Status 0x1B](../node-rpc-envelope.md#status-op-0x1b-rdm); one or two roots, never none; the secret only to a node-admin |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:52
/// "| Reply | `RootsUpdated { node_id, incarnation, roots }`, the receipt; a set that drops a root the node still admits a chain under is refused by name (`root-still-chained`), which is what keeps step 5 after step 4 |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:53
/// "| Launch | A launched node is written the current set at launch (`RDM_ROOTS`), the way `RDM_MESH_ID` is, so a node born mid-overlap trusts both; a launched node-admin is also written the root secret (`RDM_CA_SECRET`) and the current CA material (`RDM_FABRIC_CA_KEY`; the CA's cert travels inside the issuing cert) |"
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:57
/// "1. `fabric.root.update { secret }` arrives at the fabric-primary. It writes the new secret beside the old one, writes the roots row `minted`, and answers `fabric.root.updated`."
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:59
/// "2. `node.roots.update { [old, new] }` to every node of the fabric, node-admins first. From its install a node admits a chain to either anchor. The row goes `distributed` on the last receipt."
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:61
/// "3. `fabric.ca.renew` under the new anchor: a new CA signed by the new root, every mesh's issuer re-minted under it ([mesh.issuer.renew](mesh-issuer-renew.md)). The row goes `ca-rotated`. Members renew under the new issuers at their own points ([node.cert.renew](node-cert-renew.md))."
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:64
/// "4. The tick waits until every cert row chains to the new anchor: at most one member TTL after step 3. A member whose renewal has not reached it is the ordinary not-sent case, retried; the rotation waits rather than retiring an anchor something still chains to."
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:67
/// "5. `node.roots.update { [new] }` to every node. The row goes `retired` on the last receipt, and the old secret is destroyed on every admin."
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:96
/// "A node not reached in a phase is retried every tick; the phase does not advance without it, and nothing expires meanwhile that the overlap does not cover. A successor fabric-primary continues from the roots row and the CA row, and from its own copy of the root secret, which every node-admin holds ([custody](../fabric-certs.md#custody)). A successor that does not hold the CA the CA row names (a renewal whose step 2 had not reached it) never mints under a CA it does not hold: it runs `fabric.ca.renew` again from the root secret, and step 2 reaches every admin under the new CA."
/// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:103
/// "Trace propagation, step spans and correlation follow the [runtime spans](../node-rpc-envelope.md#runtime-spans). The cells, with the TTLs set to seconds in the test kit: an internal `fabric.ca.renew` with a dial admitted before, during and after and no `node.roots.update` span; a `fabric.root.update` through all five phases with a dial admitted at every phase, the spans and the roots row beside it, and a retire attempted before step 4 refused by name."
#[test]
fn a_root_update_rotates_the_fabric_to_the_new_anchor_through_its_four_phases() {
    let cell = "a_root_update_rotates_the_fabric_to_the_new_anchor_through_its_four_phases";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        use rdm_core::status::{RootSecretHex, RootsPhase};
        let dir = cell_dir("root-rotation");
        let root = boot_root(&dir, &policy(&[])).await;
        let (second, second_env) = launched_admin(&root, &dir).await;
        let a = member(&root, &dir, "mesh1.rpc.1").await;
        let door = root.status_authority().expect("the fabric-primary's Status door");
        let new = certs::mint_key();
        let hex = |k: &certs::VerifyingKey| rdm_core::launch::encode_hex(k.as_bytes());
        // The call goes to the fabric-primary once its view names it so.
        until("mesh1.admin.1's view names it the fabric-primary", BOUND, async || fabric_get(&root).await.and_then(|f| f.fabric_primary).filter(|p| p.to_string() == "mesh1.admin.1")).await;
        let answered = door.apply(None, &StatusRequest::UpdateRoot { secret: RootSecretHex(certs::encode_key(&new)) }).await;
        assert_eq!(answered, StatusReply::RootUpdated { phase: RootsPhase::Minted }, "the rotation is accepted at minted");
        let same = door.apply(None, &StatusRequest::UpdateRoot { secret: RootSecretHex(certs::encode_key(&new)) }).await;
        assert_eq!(same, StatusReply::RejectedRootAlreadyTrusted { root: hex(&new.verifying_key()) }, "the same root is trusted already");
        let other = door.apply(None, &StatusRequest::UpdateRoot { secret: RootSecretHex(certs::encode_key(&certs::mint_key())) }).await;
        assert!(matches!(other, StatusReply::RejectedRotationInProgress { .. }), "another root while one is in flight: {other:?}");

        let moved = |phase: &'static str| {
            let spans = spans.clone();
            async move || named(&spans, "rdm.node_admin.root.update.via-caller").into_iter().find(|f| f.get("phase").map(String::as_str) == Some(phase) && f.get("outcome").map(String::as_str) == Some("moved"))
        };
        until("the rotation is distributed", BOUND, moved("Distributed")).await;
        until("the rotation is ca-rotated", BOUND, moved("CaRotated")).await;
        until("the rotation is retired", BOUND * 2, moved("Retired")).await;

        // fabric.get carries the rotation's progress and the CA under the new root.
        let shown = until("fabric.get shows the rotation retired", BOUND, async || fabric_get(&root).await.filter(|f| f.roots.as_ref().is_some_and(|r| r.phase == RootsPhase::Retired))).await;
        assert!(!shown.roots.as_ref().unwrap().stalled, "a rotation retired is not stalled: {:?}", shown.roots);
        assert!(shown.ca.is_some(), "fabric.get carries the CA row: {shown:?}");
        let alone = vec![hex(&new.verifying_key())];
        for node in ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1"] {
            until(&format!("{node} installs the new root alone"), BOUND, async || {
                named(&spans, "rdm.node.roots.update.via-installed").into_iter().find(|f| f.get("node").map(String::as_str) == Some(node) && f.get("roots").is_some_and(|r| r.contains(&alone[0]) && !r.contains(',')))
            })
            .await;
        }
        let second_dir = PathBuf::from(second_env.get("RDM_DATA_DIR").expect("the launch names its data dir"));
        for (who, data) in [("mesh1.admin.1", dir.clone()), ("mesh1.admin.2", second_dir)] {
            assert_eq!(std::fs::read_to_string(data.join(rdm_admin::certs::CA_SECRET_FILE)).unwrap().trim(), certs::encode_key(&new), "{who} keeps the new root as ca-secret");
            assert!(!data.join(rdm_admin::certs::NEXT_SECRET_FILE).exists(), "{who} keeps no ca-secret-next");
        }
        // `retired` moves only once every member's cert row names an issuer under the new CA
        // (`every_cert_under_new_ca`): the member was renewed and installed it on the way.
        assert!(!renewals_of(&spans, "mesh1.rpc.1").into_iter().filter(|f| f.get("outcome").map(String::as_str) == Some("installed")).collect::<Vec<_>>().is_empty(), "the member was renewed under the new issuer before the old root retired");
        stop_all(vec![a], vec![root, second]).await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}
