//! The estate's handover to a node-admin it starts: the admin prints the exact birth its Node RPC
//! door serves under, and a caller that holds only that line reaches it over Node RPC.

use rdm_core::MeshNode;
use rdm_client::{BuildCarrier, Fabrics};
use rdm_transport::{NodeRpcClient, ResolvedNode, StaticResolver};
use rdm_test::estate::{artifacts_root, bin_dir, binary, estate_root, estate_store_dir, named, wait_for, EstateScope};
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

const TEST: &str = "a_started_admin_is_reached_by_the_birth_it_printed";

/// The node-admin the cell started: stopped by SIGTERM, and waited for, when the cell ends, passing or not.
struct Started(Child);

impl Drop for Started {
    fn drop(&mut self) {
        let _ = Command::new("kill").args(["-TERM", &self.0.id().to_string()]).status();
        let until = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < until && self.0.try_wait().ok().flatten().is_none() {
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Every span the admin wrote under `evidence`.
fn spans_in(evidence: &std::path::Path) -> Vec<Value> {
    std::fs::read_dir(evidence)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "jsonl"))
        .flat_map(|e| std::fs::read_to_string(e.path()).unwrap_or_default().lines().filter_map(|l| serde_json::from_str(l).ok()).collect::<Vec<Value>>())
        .collect()
}

/// CONTRACT (node-rpc-envelope.md, "Node RPC readiness gate": "The router refuses every op, Ping
/// included, with typed NotReady until the Node RPC door is ready. A node-admin waits until its Status
/// authority is filled and its view holds it"): a node-admin prints, beside its control base, the birth
/// it runs under (`RDM_NODE_BIRTH=`: node id, path.name, endpoint, address and incarnation). It is the
/// exact birth its boot span names and the one its door opens for
/// (`rdm.node_admin.node.serve.via-view-holds-birth`), and a caller that holds only that line reaches it
/// over Node RPC: its `fabric.get`, refused NotReady until the door opens, names it the fabric-primary.
/// The estate hands its typed objects to an admin it started this way; the admin's view lists the admin
/// only once its door opens, which can be after the admin printed its base.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_started_admin_is_reached_by_the_birth_it_printed() {
    let artifacts = artifacts_root().join("estate-handover").join(TEST);
    let _ = std::fs::remove_dir_all(&artifacts);
    let evidence = artifacts.join("spans");
    std::fs::create_dir_all(&evidence).unwrap();
    let root = estate_root(TEST);
    std::fs::create_dir_all(estate_store_dir(&root)).unwrap();
    let _scope = EstateScope::begin(&root);
    let mut child = Command::new(binary("rdm-node-admin"))
        .envs([
            ("MESH_SPAWN_TYPE", "process".to_string()),
            ("RDM_FABRIC", "fabric1".into()),
            ("RDM_MESH", "mesh1".into()),
            ("RDM_DATA_DIR", root.join("mesh1.admin.1").display().to_string()),
            ("RDM_STORE_DIR", estate_store_dir(&root).display().to_string()),
            ("RDM_BIN_DIR", bin_dir().display().to_string()),
            ("RDM_EVIDENCE_DIR", evidence.display().to_string()),
            ("RDM_ESTATE_ROOT", root.display().to_string()),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("the node-admin starts");
    let stdout = child.stdout.take().unwrap();
    let admin = Started(child);
    // Every line the admin prints, up to its control base.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut lines = Vec::new();
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let base = line.starts_with("RDM_NODE_ADMIN_API_BASE=");
            lines.push(line);
            if base {
                let _ = tx.send(lines.clone());
            }
        }
    });
    let printed = rx.recv_timeout(Duration::from_secs(30)).expect("the node-admin prints its control base within 30 s");
    std::fs::write(artifacts.join("printed.txt"), printed.join("\n")).unwrap();
    let line = printed.iter().find_map(|l| l.strip_prefix("RDM_NODE_BIRTH=")).unwrap_or_else(|| panic!("the node-admin prints no birth beside its control base; it printed {printed:?}"));
    let birth: MeshNode = serde_json::from_str(line).unwrap_or_else(|e| panic!("the birth line is a birth: {e}: {line}"));

    // A caller that holds nothing but the line.
    let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller binds an endpoint");
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode { node_id: birth.node_id.clone(), name: birth.name.clone(), endpoint_id: birth.endpoint_id.0.parse().expect("the birth's endpoint is an iroh key"), transport_addr: birth.transport_addr, incarnation: birth.incarnation.clone() });
    let fabrics = Fabrics::new(BuildCarrier::new(Arc::new(NodeRpcClient::new(endpoint.clone(), resolver).with_caller_system("rdm")), birth.name.clone()));
    let fabric = wait_for(&format!("{} answers fabric.get naming itself the fabric-primary", birth.name), Duration::from_secs(30), || async { rdm_test::typed::the_fabric(&fabrics).await.ok().filter(|f| f.fabric_primary.as_ref() == Some(&birth.name)) }).await;
    std::fs::write(artifacts.join("fabric.json"), serde_json::to_vec_pretty(&fabric).unwrap()).unwrap();
    endpoint.close().await;
    drop(admin);

    let spans = spans_in(&evidence);
    let boot = named(&spans, "rdm.mesh.node.create.via-boot");
    let [boot] = boot.as_slice() else { panic!("one boot span, found {}", boot.len()) };
    assert_eq!((boot["attributes"]["node_id"].as_str(), boot["attributes"]["incarnation_id"].as_str()), (Some(birth.node_id.to_string().as_str()), Some(birth.incarnation.as_str())), "the printed birth is the one the boot names: {birth:?}");
    let door = named(&spans, "rdm.node_admin.node.serve.via-view-holds-birth");
    let [door] = door.as_slice() else { panic!("the door opens once, found {}", door.len()) };
    assert_eq!((door["attributes"]["node_id"].as_str(), door["attributes"]["incarnation_id"].as_str(), door["attributes"]["node"].as_str()), (Some(birth.node_id.to_string().as_str()), Some(birth.incarnation.as_str()), Some(birth.name.to_string().as_str())), "the door opens for the printed birth: {birth:?}");
    std::fs::write(artifacts.join("trace-id.txt"), boot["trace_id"].as_str().unwrap_or_default()).unwrap();
}
