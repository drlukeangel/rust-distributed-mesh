//! i143.e12.s22 process E2E: fabric certs (`fabric-certs.md`), on the testkit's own node-admin
//! executable, which signs with the deterministic test signer.
//!
//! - every JoinNode is issued a member cert for exactly its birth, on the issuing admin's
//!   rafka-time: a node restarted through the Build is issued a fresh one (new incarnation, later
//!   issue time);
//! - the maker issues a new mesh's issuing material and passes it in the launch of that mesh's first
//!   node-admin, and in no other launch.
//!
//! Evidence: the issuing admins' `rdm.node_admin.cert.create.via-join` and `...via-mesh-birth`
//! spans, and the testkit's `rdm.testkit.cert.resolve.via-running-node` / `...via-launch` spans, in
//! which each launched process records the bytes it holds.

use rafka_node_rpc_testkit::test_certs::{TestMemberCert, TestMeshIssuer};
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha};
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
        test: "every_join_is_issued_a_fresh_member_cert_and_a_new_meshs_first_admin_is_launched_with_its_issuer".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// The member certs the nodes named `node` recorded holding, in the order they started, with the
/// incarnation each span names.
fn s_u64(v: &Value) -> u64 {
    v.as_str().and_then(|x| x.parse().ok()).or_else(|| v.as_u64()).unwrap_or(0)
}

fn held_by(spans: &[Value], node: &str) -> Vec<(String, TestMemberCert)> {
    let mut found: Vec<(u64, String, TestMemberCert)> = named(spans, "rdm.testkit.cert.resolve.via-running-node")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == node)
        .map(|sp| {
            let cert = TestMemberCert::decode(s(&sp["attributes"]["member_cert"]).as_bytes()).unwrap_or_else(|e| panic!("{node}: {e}: {sp}"));
            (sp["start_unix_nano"].as_u64().unwrap_or(0), s(&sp["attributes"]["incarnation_id"]), cert)
        })
        .collect();
    found.sort_by_key(|f| f.0);
    found.into_iter().map(|(_, inc, c)| (inc, c)).collect()
}

/// CONTRACT: the admin that accepts a JoinNode issues a member cert for exactly that birth (node
/// id, incarnation, path.name, mesh, endpoint key) on its rafka-time and the birth holds those
/// bytes; a restart is a new JoinNode and is issued a fresh cert. The maker issues a created mesh's
/// issuing material and launches that mesh's first admin with it; the mesh's second admin, launched
/// while the first is live, is launched with none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_join_is_issued_a_fresh_member_cert_and_a_new_meshs_first_admin_is_launched_with_its_issuer() {
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_external(owner(), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let mesh = |m: &str| json!({"name": m, "node_admin": 2, "rpc_node": 1});
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    let create = s(&a["build_id"]);
    estate.await_build(&create, SETTLE).await;
    let want: std::collections::BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| [format!("{m}.admin.1"), format!("{m}.admin.2"), format!("{m}.rpc.1")]).collect();
    estate.settled(&want, Duration::from_secs(30)).await;

    // Restart mesh1.rpc.1 through the Build: a new JoinNode of the same node.
    let before = estate.node("mesh1.rpc.1").await;
    let (status, restart) = estate.post("/api/nodes/mesh1.rpc.1/restart", &json!({})).await;
    assert_eq!(status, 202, "{restart}");
    estate.await_attempt(&s(&restart["build_id"]), Estate::attempt_of(&restart), SETTLE).await;
    wait_for("mesh1.rpc.1 ready after its restart", SETTLE, || async {
        let n = estate.node_opt("mesh1.rpc.1").await?;
        (n["status"] == "ready-for-traffic" && n["parked"] == false && n["incarnation_id"] == before["incarnation_id"]).then_some(())
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();

    // Every rpc node holds a cert for exactly its birth.
    let first = held_by(&spans, "mesh1.rpc.1");
    // The process holds the cert its birth was issued; the rejoin (start after stop) is a second JoinNode
    // of the same birth, issued a fresh cert on the later rafka-time.
    assert_eq!(first.len(), 1, "mesh1.rpc.1 booted once: {first:?}");
    let (inc, cert) = &first[0];
    assert_eq!((cert.name.as_str(), cert.mesh.as_str(), cert.incarnation.as_str()), ("mesh1.rpc.1", "mesh1", inc.as_str()), "the cert names the birth that holds it");
    let issuances: Vec<&Value> = named(&spans, "rdm.node_admin.cert.create.via-join").into_iter().filter(|sp| sp["attributes"]["node"] == "mesh1.rpc.1" && sp["attributes"]["incarnation_id"] == inc.as_str()).collect();
    assert_eq!(issuances.len(), 2, "one issuance for the birth's join and one for its rejoin: {issuances:?}");
    let issued_at = |sp: &Value| s_u64(&sp["attributes"]["issued_at_rafka_ms"]);
    let (a, b) = (issuances.iter().map(|sp| issued_at(sp)).min().unwrap(), issuances.iter().map(|sp| issued_at(sp)).max().unwrap());
    assert!(b > a, "the rejoin's cert is issued later on rafka-time: {a} then {b}");
    assert_eq!(a, cert.issued_at_ms, "the process holds the cert of its first join");
    let second_mesh = held_by(&spans, "mesh2.rpc.1");
    assert_eq!(second_mesh.len(), 1);
    assert_eq!((second_mesh[0].1.name.as_str(), second_mesh[0].1.mesh.as_str()), ("mesh2.rpc.1", "mesh2"));

    // The issuing admins' spans: one issuance per join, on rafka-time, naming the birth.
    let issued = named(&spans, "rdm.node_admin.cert.create.via-join");
    for (inc, cert) in first.iter().chain(second_mesh.iter()) {
        let sp = issued
            .iter()
            .find(|sp| sp["attributes"]["node"] == cert.name.as_str() && sp["attributes"]["incarnation_id"] == inc.as_str() && sp["attributes"]["issued_at_rafka_ms"] == cert.issued_at_ms.to_string().as_str())
            .unwrap_or_else(|| panic!("no issuance span for {} {inc}", cert.name));
        assert_eq!(sp["attributes"]["outcome"], "issued", "{sp}");
        assert_eq!(sp["attributes"]["issued_at_rafka_ms"], cert.issued_at_ms.to_string().as_str(), "the span's issue time is the cert's: {sp}");
    }

    // A new mesh's first admin is launched with the issuer the maker issued, and no other admin is.
    let minted = named(&spans, "rdm.node_admin.cert.create.via-mesh-birth").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh2").collect::<Vec<_>>();
    assert_eq!(minted.len(), 1, "one mesh birth issued mesh2's material: {minted:?}");
    assert_eq!(minted[0]["attributes"]["outcome"], "issued", "{}", minted[0]);
    let launches = named(&spans, "rdm.testkit.cert.resolve.via-launch");
    let launch_of = |node: &str| launches.iter().find(|sp| sp["attributes"]["node"] == node).unwrap_or_else(|| panic!("no launch span for {node}"));
    let first_admin = launch_of("mesh2.admin.1");
    let material = TestMeshIssuer::decode(s(&first_admin["attributes"]["mesh_issuer"]).as_bytes()).expect("mesh2.admin.1 was launched with the signer's issuing material");
    assert_eq!(material.mesh, "mesh2");
    assert_eq!(minted[0]["attributes"]["issued_at_rafka_ms"], material.issued_at_ms.to_string().as_str(), "the launch carries exactly what the maker issued");
    assert_eq!(first_admin["attributes"]["mesh_issuer_len"], minted[0]["attributes"]["cert_len"], "{first_admin}");
    assert_eq!(launch_of("mesh2.admin.2")["attributes"]["mesh_issuer_len"], "0", "the second admin of a mesh whose first is live is launched with no issuer");
    assert_eq!(launch_of("mesh1.admin.2")["attributes"]["mesh_issuer_len"], "0", "nor is an admin joining the Day-0 mesh");

    estate.artifact("trace-ids.json", &json!({
        "mesh1.rpc.1 restart issuance": issued.iter().filter(|sp| sp["attributes"]["node"] == "mesh1.rpc.1").map(|sp| json!({"trace_id": sp["trace_id"], "span_id": sp["span_id"], "incarnation_id": sp["attributes"]["incarnation_id"], "issued_at_rafka_ms": sp["attributes"]["issued_at_rafka_ms"]})).collect::<Vec<_>>(),
        "mesh2 mesh-birth issuance": {"trace_id": minted[0]["trace_id"], "span_id": minted[0]["span_id"], "cert_len": minted[0]["attributes"]["cert_len"]},
    }));
}
