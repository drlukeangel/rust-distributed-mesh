//! i143.e12.s22, i145 e1 process E2E: fabric certs (`fabric-certs.md`), on the testkit's own node-admin
//! executable, which holds the fabric's cert chain as every RDM node-admin does.
//!
//! - every JoinNode is issued a member cert for exactly its birth, signed by its mesh's issuing key and
//!   verifying to the fabric's root: a node restarted through the Build is issued a fresh one;
//! - every launched node-admin is launched with its mesh's issuing material, minted once at the
//!   mesh's birth by an admin holding the root.
//!
//! Evidence: the issuing admins' `rdm.node_admin.cert.create.via-join` and `...via-mesh-birth`
//! spans, and the testkit's `rdm.testkit.cert.resolve.via-running-node` / `...via-launch` spans, in
//! which each launched process records the cert it holds (hex) and the issuing cert it was launched with.

use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, NodeStatus, PathName};
use rdm_core::certs::{self, SignedCert, SignedIssuingCert, VerifyingKey};
use rdm_test::estate::{named, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha};
use serde_json::{json, Value};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-certs".into(),
        subfeature: "issuance".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_member_cert_verifies_to_its_issuer_and_the_issuer_to_the_root".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn s_u64(v: &Value) -> u64 {
    v.as_str().and_then(|x| x.parse().ok()).or_else(|| v.as_u64()).unwrap_or(0)
}

/// The member certs the nodes named `node` recorded holding (the testkit's span carries the bytes as
/// hex), in the order they started, with the incarnation each span names.
fn held_by(spans: &[Value], node: &str) -> Vec<(String, SignedCert)> {
    let mut found: Vec<(u64, String, SignedCert)> = named(spans, "rdm.testkit.cert.resolve.via-running-node")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == node)
        .map(|sp| {
            let cert = certs::decode_cert(&s(&sp["attributes"]["member_cert"])).unwrap_or_else(|e| panic!("{node} holds no signed member cert: {e}: {sp}"));
            (sp["start_unix_nano"].as_u64().unwrap_or(0), s(&sp["attributes"]["incarnation_id"]), cert)
        })
        .collect();
    found.sort_by_key(|f| f.0);
    found.into_iter().map(|(_, inc, c)| (inc, c)).collect()
}

/// CONTRACT (fabric-certs.md "The chain", "Where a node-admin's signing material comes from" and
/// "When a cert is issued"; i145 e1, e5 R13): the bootstrap admin holds the fabric's root (minted on
/// day 0 and kept as its `ca-secret`) and the fabric CA under it. The admin that accepts a JoinNode
/// issues a member cert for exactly that birth (path.name, mesh, node id, incarnation, endpoint key),
/// signed by its mesh's issuing key, and the birth holds it: it verifies to its issuer, the issuer to
/// the CA and the CA to the root. A restart is a new JoinNode and is issued a fresh cert. Every
/// launched node-admin is launched with the fabric's root and the CA's key beside its mesh's issuing
/// material (`RDM_CA_SECRET`, `RDM_FABRIC_CA_KEY`, `RDM_ISSUING_KEY`, `RDM_ISSUING_CERT`; §Custody), the
/// issuing cert signed by the CA and carrying the CA's cert; the mesh's issuer is minted once, at the
/// mesh's birth, and every admin of the mesh carries that one, a launched admin that holds the root
/// included. Must NOT happen: a cert that does not verify to the root, a cert naming another birth, a
/// launched admin with no issuing material, another mesh's, no root or CA beside it, or a mesh given a
/// second issuer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_cert_verifies_to_its_issuer_and_the_issuer_to_the_root() {
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let mesh1 = MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)]);
    let mesh2 = MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 1)]);
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh1, mesh2] }).await;
    let want: std::collections::BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh1.rpc.2", "mesh2.admin.1", "mesh2.admin.2", "mesh2.rpc.1"].into_iter().map(str::to_string).collect();
    let views = estate.settles_to_named(&want, Duration::from_secs(30)).await;

    // Stop then start mesh1.rpc.1 through the Build: a new JoinNode of the same node.
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let before = estate.node_view("mesh1.rpc.1").await;
    let path: PathName = "mesh1.rpc.1".parse().unwrap();
    estate.stop_then_start(&path).await;
    estate.until_node("mesh1.rpc.1", "mesh1.rpc.1 ready after its stop and start", SETTLE, |n| n.filter(|n| n.status == NodeStatus::Ready && !n.stopped && n.incarnation_id == before.incarnation_id).map(|_| ())).await;
    let ca = estate.bootstrap_data_dir("mesh1").join("ca-secret");
    estate.stop().await;
    let spans = estate.spans();
    let root: VerifyingKey = certs::decode_key(&std::fs::read_to_string(&ca).unwrap_or_else(|e| panic!("the bootstrap admin keeps the fabric's root at {}: {e}", ca.display()))).unwrap_or_else(|e| panic!("{}: {e}", ca.display())).verifying_key();

    // Every rpc node holds a cert for exactly its birth, verifying to its issuer and the issuer to the root.
    let issued = named(&spans, "rdm.node_admin.cert.create.via-join");
    for node in ["mesh1.rpc.1", "mesh2.rpc.1"] {
        let held = held_by(&spans, node);
        // The process holds a cert its birth was issued at a join.
        assert_eq!(held.len(), 1, "{node} booted once: {held:?}");
        let (inc, cert) = &held[0];
        let view = views.iter().find(|v| v.name.to_string() == node).expect("the node is in the view");
        assert_eq!(
            (cert.cert.name.to_string(), cert.cert.mesh.as_str(), cert.cert.node_id.to_string(), cert.cert.incarnation.to_string()),
            (node.to_string(), node.split('.').next().unwrap(), view.node_id.to_string(), inc.clone()),
            "the cert names the birth that holds it"
        );
        // Every JoinNode issues a fresh member cert (fabric-certs.md, When a cert is issued): a join the
        // admin served again for the same birth (a resend under load) issues another, and the process
        // holds the one its last answered join carried. The held cert is one this birth was issued.
        let joins: Vec<&serde_json::Value> = issued
            .iter()
            .filter(|sp| sp["attributes"]["node"] == node && sp["attributes"]["incarnation_id"] == inc.as_str() && sp["attributes"]["outcome"] == "issued")
            .copied()
            .collect();
        assert!(!joins.is_empty(), "no issuance span for {node} {inc}");
        let held_issue = joins
            .iter()
            .find(|sp| s_u64(&sp["attributes"]["expires_at_rafka_ms"]) == cert.cert.expiry_ms)
            .unwrap_or_else(|| panic!("the process holds a cert its birth was issued at one of its joins: {} expiring {}, issued {joins:?}", cert.cert.name, cert.cert.expiry_ms));
        let at = s_u64(&held_issue["attributes"]["issued_at_rafka_ms"]);
        assert_eq!(certs::verify(cert, &[root], at, 0), Ok(()), "{node}'s cert verifies to its issuer, the issuer to the fabric CA and the CA to the root");
        assert_eq!(cert.issuer.cert.mesh, node.split('.').next().unwrap(), "signed by its own mesh's issuer");
    }
    // The rejoin (start after stop) is a second JoinNode of the same birth, issued a fresh cert on the later rafka-time.
    let rejoins: Vec<&Value> = issued.iter().copied().filter(|sp| sp["attributes"]["node"] == "mesh1.rpc.1" && sp["attributes"]["incarnation_id"] == before.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default().as_str()).collect();
    assert_eq!(rejoins.len(), 2, "one issuance for the birth's join and one for its rejoin: {rejoins:?}");

    // Every launched admin carries its mesh's issuing material, signed by the root; a mesh's issuer is
    // minted once, at its birth, and every admin of the mesh carries that one.
    let launches = named(&spans, "rdm.testkit.cert.resolve.via-launch");
    let issuing_of = |node: &str| -> SignedIssuingCert {
        let sp = launches.iter().find(|sp| sp["attributes"]["node"] == node).unwrap_or_else(|| panic!("no launch span for {node}"));
        certs::decode_issuing_cert(&s(&sp["attributes"]["issuing_cert"])).unwrap_or_else(|e| panic!("{node} was launched with no issuing cert: {e}: {sp}"))
    };
    for (node, mesh) in [("mesh1.admin.2", "mesh1"), ("mesh2.admin.1", "mesh2"), ("mesh2.admin.2", "mesh2")] {
        let issuing = issuing_of(node);
        assert_eq!(issuing.cert.mesh, mesh, "{node} carries its own mesh's issuing cert");
        assert_eq!(certs::verify_issuer(&issuing, &[root]), Ok(()), "{node}'s issuing cert is signed by the fabric CA, and the CA by the root");
        let sp = launches.iter().find(|sp| sp["attributes"]["node"] == node).unwrap();
        assert_eq!(sp["attributes"]["root"], "true", "{node} was launched with the root beside its issuing material: {sp}");
        // R13: the fabric CA sits between the root and every mesh's issuer; a launched admin holds it.
        assert!(!s(&sp["attributes"]["ca_cert"]).is_empty(), "{node} was launched with the fabric CA's cert beside its root and issuing material (R13): {sp}");
        let ca = certs::decode_ca_cert(&s(&sp["attributes"]["ca_cert"])).unwrap_or_else(|e| panic!("{node}'s launch carries no CA cert: {e}: {sp}"));
        assert_eq!(ca, issuing.ca, "{node}'s CA is its issuing cert's");
        assert_eq!(sp["attributes"]["ca_key"], "true", "{node} was launched with the fabric CA's key beside the root: {sp}");
    }
    assert_eq!(issuing_of("mesh2.admin.1"), issuing_of("mesh2.admin.2"), "mesh2's admins carry the one issuer minted at mesh2's birth");
    let minted = named(&spans, "rdm.node_admin.cert.create.via-mesh-birth").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh2").collect::<Vec<_>>();
    assert_eq!(minted.len(), 1, "one mesh birth minted mesh2's issuer: {minted:?}");
    assert_eq!(minted[0]["attributes"]["outcome"], "issued", "{}", minted[0]);

    estate.artifact("trace-ids.json", &json!({
        "mesh1.rpc.1 issuances": issued.iter().filter(|sp| sp["attributes"]["node"] == "mesh1.rpc.1").map(|sp| json!({"trace_id": sp["trace_id"], "span_id": sp["span_id"], "incarnation_id": sp["attributes"]["incarnation_id"], "issued_at_rafka_ms": sp["attributes"]["issued_at_rafka_ms"]})).collect::<Vec<_>>(),
        "mesh2 mesh-birth issuance": {"trace_id": minted[0]["trace_id"], "span_id": minted[0]["span_id"]},
    }));
    estate.record_trace_url(&s(&minted[0]["trace_id"]));
}
