//! Functional: the probe enters the fabric at a node-admin's birth and reads that admin's view over
//! Node RPC.
//!
//! RDM's only surface is node-RPC (ops-naming-proposal.md "RDM has no REST"): the probe is given the
//! entry admin's birth (`--admin-birth`, the JSON an admin prints on its birth line), reads the fabric
//! (`fabric.get`) and each mesh's nodes (`node.get` by mesh) from that admin, and resolves its target
//! from what it read. The admin here serves Node RPC only; there is no HTTP listener to fall back on.

use crate::common::admin_view::{self, node};
use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::launch::Launch;
use rdm_core::{FabricId, IncarnationId, NodeId};
use rdm_transport::{ServedBirth, ServerBuilder};
use rdm_core::filter::{Entity, Filter};
use rdm_core::status::StatusRequest;
use rdm_test::proof_store::{self, FileProofStore};
use std::sync::Arc;

/// CONTRACT: a probe given `--admin-birth` of a node-admin that serves Node RPC only asks that admin
/// `fabric.get`, then `node.get [mesh: m]` for the one mesh the fabric lists, resolves its `exact:` target from
/// the nodes it read and makes its proof call: one `Reply` executed by the target node. A node the view
/// holds unborn (no endpoint, address or incarnation yet) is left out and refuses nothing. The probe's
/// span file holds the `via-probe` root and, in its trace, the `rdm.node_admin.fabric.resolve.via-call` and
/// `rdm.node_admin.node.resolve.via-call` reads, each ended `read`. What must NOT happen: a `Refused` line
/// (an argument the probe does not know, or a read it could not make), any request to the admin but
/// those two reads, or a read outside the probe's trace.
#[test]
fn a_probe_given_an_admin_birth_resolves_its_target_from_that_admins_view_over_node_rpc() {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let data = std::env::temp_dir().join(format!("rdm-probe-entry-data-{}", std::process::id()));
    // The probe's span files stay after the cell, in the cell's own directory: its trace is the proof.
    let evidence = std::env::temp_dir().join("i143-testkit/a_probe_given_an_admin_birth_resolves_its_target_from_that_admins_view_over_node_rpc");
    let _ = std::fs::remove_dir_all(&data);
    let _ = std::fs::remove_dir_all(&evidence);
    std::fs::create_dir_all(&data).unwrap();
    let (admin, target, router) = rt.block_on(async {
        let key = SecretKey::generate();
        let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            name: "mesh1.rpc.1".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: Vec::new(),
            seeds: Vec::new(),
            launcher: None,
            data_dir: data.clone(),
            store_dir: data.clone(),
            mesh_id: None,
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
        };
        let store = Arc::new(FileProofStore::open(&launch.data_dir).unwrap());
        let server = proof_store::serve(ServerBuilder::new(), store, &launch, &rdm_test::held_name::HeldName::of_birth(launch.name.clone()))
            .carry::<rdm_test::proof_store::ProofStore>()
            .build(ServedBirth { node_id: launch.node_id.to_string(), incarnation: launch.incarnation.to_string() })
            .unwrap();
        let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
        let admin = admin_view::serve(vec![
            node("mesh1.rpc.1", &launch.node_id, Some(key.public().to_string()), Some(addr), Some(launch.incarnation.clone())),
            node("mesh1.rpc.2", &NodeId::mint(), None, None, None),
        ])
        .await;
        (admin, launch.node_id.to_string(), router)
    });
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rdm-rpc-probe"))
        .args(["--admin-birth", &admin.birth_arg(), "get", "--target", &format!("exact:{target}"), "--key", "k"])
        .env("RDM_EVIDENCE_DIR", &evidence)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| panic!("the probe printed no JSON ({e}): {stdout} / {stderr}"));
    assert_eq!((v["outcome"].as_str(), v["route"].as_str()), (Some("Reply"), Some("direct")), "the call reaches the target the admin's view names: {v}");
    assert_eq!(v["reply"]["executing_node"].as_str(), Some(target.as_str()), "the target node executed the call: {v}");
    let read = |entity, filter| rdm_core::read::ReadRequest::Get { entity, filter };
    assert_eq!(*admin.seen.lock().unwrap(), vec![read(Entity::Fabrics, Filter::new()), read(Entity::Nodes, Filter::new().eq("mesh", ["mesh1"]))], "the probe read the fabric, then the one mesh it lists, and nothing else");
    let spans: Vec<serde_json::Value> = std::fs::read_dir(&evidence)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("rdm-rpc-probe.") && e.file_name().to_string_lossy().ends_with(".spans.jsonl"))
        .flat_map(|e| std::fs::read_to_string(e.path()).unwrap_or_default().lines().filter_map(|l| serde_json::from_str(l).ok()).collect::<Vec<serde_json::Value>>())
        .collect();
    let root = spans.iter().find(|s| s["name"] == "rdm.node_rpc.proof_store.resolve.via-probe").unwrap_or_else(|| panic!("the probe left no via-probe span: {spans:?}"));
    let trace = root["trace_id"].as_str().unwrap_or_default();
    for read in ["rdm.node_admin.fabric.resolve.via-call", "rdm.node_admin.node.resolve.via-call"] {
        let s = spans.iter().find(|s| s["name"] == read).unwrap_or_else(|| panic!("the probe left no {read} span: {spans:?}"));
        assert_eq!((s["trace_id"].as_str(), s["attributes"]["outcome"].as_str()), (Some(trace), Some("read")), "{read} is a read of the probe's own trace: {s}");
    }
    std::fs::write(evidence.join("trace.txt"), format!("probe_entry_admin trace_id={trace} reads=fabric.get,node.get[mesh:mesh1] outcome={} executing_node={target}\n", v["outcome"])).unwrap();
    rt.block_on(async {
        admin.shutdown().await;
        let _ = router.shutdown().await;
    });
    let _ = std::fs::remove_dir_all(&data);
}
