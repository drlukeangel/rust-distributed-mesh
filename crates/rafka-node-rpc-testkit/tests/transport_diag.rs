//! Functional: the transport diagnostic sink (R-QH1). A handshake that fails leaves, in each end's
//! own node log directory, a file with that end's run, identity, bound address, error chain and
//! the handshake's packets, and none of it reaches span export.
//!
//! The maker is this process (an endpoint and a Node RPC server, accepting as the router does);
//! the joining end is the `rafka-rpc-probe` process, which dials the maker's address expecting
//! another endpoint's key. The maker answers the Initial, the probe verifies the maker's key, finds
//! it is not the one it dialled and closes: a failed handshake with work on both ends.

use iroh::protocol::ProtocolHandler;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{ServedBirth, ServerBuilder};
use std::io::{Read, Write};
use std::net::TcpListener;

/// The diagnostic file each process writes in its node log directory.
const DIAG: &str = "transport-diag.log";

/// A one-route admin: every request is answered with the same node view.
fn serve_view(view: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut stream = stream;
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = write!(stream, "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{view}", view.len());
        }
    });
    base
}

fn line_with<'a>(file: &'a str, parts: &[&str]) -> Option<&'a str> {
    file.lines().find(|l| parts.iter().all(|p| l.contains(p)))
}

/// CONTRACT: a dial whose handshake fails (the maker's key is not the key the dialler expects)
/// leaves `transport-diag.log` in BOTH node log directories, the maker's and the dialler's. Each
/// file opens with a header naming the same run (`RDM_EVIDENCE_DIR`) and its own node; each holds
/// its endpoint's identity and bound UDP address; the dialler's holds the connect failure with the
/// peer key it expected, the maker's address, its own local socket and the full error chain; the
/// maker's holds the accept failure with the remote address and its error chain; and the packets
/// of the handshake are on both sides: the dialler's Initial sent and the maker's reply received,
/// the maker's first Initial received and its reply sent. What must NOT happen: a diagnostic line,
/// a noq or iroh line in either process's span evidence (R-TL1), or a side with no file. The
/// dialler's connect failure names the trace of its exported dial span.
#[test]
fn a_failed_handshake_leaves_both_ends_transport_diagnostics_in_their_node_log_dirs() {
    if crate::own_process::delegated(module_path!(), "a_failed_handshake_leaves_both_ends_transport_diagnostics_in_their_node_log_dirs") {
        return;
    }
    let root = std::env::temp_dir().join(format!("rdm-transport-diag-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let (run, maker_dir, dialler_dir) = (root.join("spans"), root.join("mesh1.admin.1"), root.join("mesh1.rpc.9-diag"));
    std::fs::create_dir_all(&run).unwrap();
    std::env::set_var("RDM_EVIDENCE_DIR", &run);
    std::env::set_var("RDM_DATA_DIR", &maker_dir);
    std::env::set_var("RDM_NODE_NAME", "mesh1.admin.1");
    std::env::set_var("RDM_FABRIC", "diag");
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let telemetry = rt.block_on(async { rafka_mesh_telemetry::init_evidence_telemetry("rafka-node-admin") });
    let maker_key = iroh::SecretKey::generate();
    let expected = iroh::SecretKey::generate().public();
    let (maker_addr, accepted, out) = rt.block_on(async {
        let server = ServerBuilder::new().seal(ServedBirth { node_id: NodeId::mint().to_string(), incarnation: IncarnationId::mint().0 }).unwrap();
        let ep = rafka_node_rpc::endpoint::bind(maker_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let maker_addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let target = NodeId::mint().to_string();
        let view = serde_json::json!({"nodes": [{
            "node_id": target, "name": "mesh1.admin.1", "endpoint_id": expected.to_string(), "transport_addr": maker_addr.to_string(), "incarnation_id": "c0ffee00c0ffee00c0ffee00c0ffee00",
        }]})
        .to_string();
        let admin = serve_view(view);
        let (run, dialler_dir) = (run.clone(), dialler_dir.clone());
        let dialler = tokio::task::spawn_blocking(move || {
            std::process::Command::new(env!("CARGO_BIN_EXE_rafka-rpc-probe"))
                .args(["--admin", &admin, "get", "--target", &format!("exact:{target}"), "--key", "k"])
                .env("RDM_EVIDENCE_DIR", &run)
                .env("RDM_DATA_DIR", &dialler_dir)
                .env("RDM_NODE_NAME", "mesh1.rpc.9")
                .output()
                .unwrap()
        });
        // As the router accepts: the Initial, the ALPN, then the protocol's own accepting.
        let incoming = ep.accept().await.expect("the dialler's Initial reached the maker");
        let mut accepting = incoming.accept().expect("the maker accepts the incoming connection");
        let accepted = match accepting.alpn().await {
            Ok(_) => server.on_accepting(accepting).await.map(|_| ()).map_err(|e| format!("{e:#}")),
            Err(e) => Err(format!("alpn: {e:#}")),
        };
        let out = dialler.await.unwrap();
        ep.close().await;
        (maker_addr, accepted, out)
    });
    drop(rt);
    drop(telemetry);
    assert!(accepted.is_err(), "the maker's accept completed though the dialler refused the maker's key");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("\"NotSent\""), "the dialler's call was sent: {stdout} / {}", String::from_utf8_lossy(&out.stderr));

    let read = |dir: &std::path::Path| std::fs::read_to_string(dir.join(DIAG)).unwrap_or_else(|e| panic!("{} has no {DIAG}: {e}", dir.display()));
    let (maker, dialler) = (read(&maker_dir), read(&dialler_dir));
    let run_field = format!("run={:?}", run.display().to_string());
    for (side, file, node) in [("maker", &maker, "mesh1.admin.1"), ("dialler", &dialler, "mesh1.rpc.9")] {
        let header = file.lines().next().unwrap_or_default();
        assert!(header.starts_with("transport-diag ") && header.contains(&run_field) && header.contains(&format!("node={node:?}")), "the {side}'s header names another run or node: {header}");
        assert!(line_with(file, &["step=\"endpoint-bound\"", "endpoint_id=", "bound="]).is_some(), "the {side} logged no bound endpoint:\n{file}");
    }
    let maker_id = maker_key.public().to_string();
    assert!(line_with(&maker, &["step=\"endpoint-bound\"", &format!("endpoint_id={maker_id}"), &maker_addr.to_string()]).is_some(), "the maker's bound line names another key or address:\n{maker}");
    let failed = line_with(&dialler, &["step=\"connect-failed\""]).unwrap_or_else(|| panic!("the dialler logged no connect failure:\n{dialler}"));
    assert!(failed.contains(&format!("peer={expected}")) && failed.contains(&format!("addr={maker_addr}")) && failed.contains("local=") && failed.contains("error="), "the connect failure lacks the expected key, the maker's address, the local socket or the chain: {failed}");
    let refused = line_with(&maker, &["step=\"accept-failed\""]).unwrap_or_else(|| panic!("the maker logged no accept failure:\n{maker}"));
    assert!(refused.contains("remote_addr=") && refused.contains("error="), "the accept failure lacks the remote address or the chain: {refused}");
    assert!(line_with(&dialler, &["noq.packet sent", "space=Initial"]).is_some(), "the dialler's Initial is not in its file:\n{dialler}");
    assert!(line_with(&dialler, &["noq.packet received"]).is_some(), "the maker's reply, received by the dialler, is not in its file:\n{dialler}");
    assert!(line_with(&maker, &["noq.packet first-received"]).is_some(), "the dialler's first Initial, received by the maker, is not in its file:\n{maker}");
    assert!(line_with(&maker, &["noq.packet sent", "space=Initial"]).is_some(), "the maker's reply is not in its file:\n{maker}");

    let spans: String = std::fs::read_dir(&run).unwrap().flatten().map(|e| std::fs::read_to_string(e.path()).unwrap_or_default()).collect();
    assert!(!spans.is_empty(), "neither process exported a span");
    for leaked in ["noq.packet", "\"connect-failed\"", "\"accept-failed\"", "\"endpoint-bound\"", "transport_diag", "noq_proto", "iroh::"] {
        assert!(!spans.contains(leaked), "{leaked:?} reached span evidence");
    }
    // The dial's line names its trace, and that trace is the dial span the dialler exported.
    let trace = failed.split("trace=00-").nth(1).and_then(|t| t.split('-').next()).unwrap_or_else(|| panic!("the connect failure names no trace: {failed}"));
    assert!(spans.contains(&format!("\"trace_id\":\"{trace}\"")) && spans.contains("rdm.node_rpc.connection.create.via-dial"), "the connect failure's trace {trace} is not the exported dial's");
    let _ = std::fs::remove_dir_all(&root);
}
