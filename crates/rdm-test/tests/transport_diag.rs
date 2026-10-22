//! Functional: the transport diagnostic sink (R-QH1). A handshake that fails leaves, in each end's
//! own node log directory, a file with that end's run, identity, bound address, error chain and
//! the handshake's packets, and none of it reaches span export.
//!
//! The maker is this process (an endpoint and a Node RPC server, accepting as the router does);
//! the joining end is the `rafka-rpc-probe` process, which dials the maker's address expecting
//! another endpoint's key. The maker answers the Initial, the probe verifies the maker's key, finds
//! it is not the one it dialled and closes: a failed handshake with work on both ends.
//!
//! The subject of both cells is the process-wide telemetry init itself, `init_evidence_telemetry`: the
//! diagnostic layer it installs from the process environment (`RDM_EVIDENCE_DIR`, `RDM_DATA_DIR`,
//! `RDM_NODE_NAME`, `RDM_MESH`), the callsites that layer enables or leaves off, and the file it opens.
//! A thread-scoped `EvidenceCapture` installs no diagnostic layer and reads none of that environment,
//! so a cell on one would pass without exercising what these cells prove. Each cell is therefore its
//! own process, started with the environment its subject reads, as the estate starts a node: the cell
//! never sets a variable.

use crate::common::admin_view::{self, node};
use iroh::protocol::ProtocolHandler;
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::{ServedBirth, ServerBuilder};

/// The diagnostic file each process writes in its node log directory.
const DIAG: &str = "transport-diag.log";

/// The directory a cell owns: both of its processes compute the same one.
fn cell_dir(cell: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit").join(cell)
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
/// dialler's connect failure names the trace of its exported dial span. The maker is launched as the
/// estate launches the bootstrap admin (a mesh and a data dir, no node name), and its header still
/// names it: `node="mesh1.admin.1"`, `mesh="mesh1"`.
#[test]
fn a_failed_handshake_leaves_both_ends_transport_diagnostics_in_their_node_log_dirs() {
    let cell = "a_failed_handshake_leaves_both_ends_transport_diagnostics_in_their_node_log_dirs";
    let root = cell_dir(cell);
    let (run, maker_dir, dialler_dir) = (root.join("spans"), root.join("mesh1.admin.1"), root.join("mesh1.rpc.9-diag"));
    // As the estate launches the bootstrap admin: an evidence run, a mesh and a data dir, no node name.
    let env = [
        ("RDM_EVIDENCE_DIR", Some(run.as_os_str())),
        ("RDM_DATA_DIR", Some(maker_dir.as_os_str())),
        ("RDM_NODE_NAME", None),
        ("RDM_MESH", Some(std::ffi::OsStr::new("mesh1"))),
        ("RDM_FABRIC", Some(std::ffi::OsStr::new("diag"))),
    ];
    if crate::own_process::delegated_with_env(module_path!(), cell, &env) {
        return;
    }
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&run).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let telemetry = rt.block_on(async { rdm_transport::init_evidence_telemetry("rafka-node-admin") });
    let maker_key = iroh::SecretKey::generate();
    let expected = iroh::SecretKey::generate().public();
    let (maker_addr, accepted, out) = rt.block_on(async {
        let server = ServerBuilder::new().build(ServedBirth { node_id: NodeId::mint().to_string(), incarnation: IncarnationId::mint().to_string() }).unwrap();
        let ep = rdm_transport::endpoint::bind(maker_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let maker_addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let target = NodeId::mint();
        // The entry admin the probe reads its view from: a third endpoint of this process, whose view
        // names the target at the maker's address under another endpoint's key.
        let served = admin_view::serve(vec![node("mesh1.admin.1", &target, Some(expected.to_string()), Some(maker_addr), Some(IncarnationId::mint()))]).await;
        let admin = served.birth_arg();
        let (run, dialler_dir) = (run.clone(), dialler_dir.clone());
        let mut dialler = tokio::task::spawn_blocking(move || {
            std::process::Command::new(env!("CARGO_BIN_EXE_rafka-rpc-probe"))
                .args(["--admin-birth", &admin, "get", "--target", &format!("exact:{target}"), "--key", "k"])
                .env("RDM_EVIDENCE_DIR", &run)
                .env("RDM_DATA_DIR", &dialler_dir)
                .env("RDM_NODE_NAME", "mesh1.rpc.9")
                .output()
                .unwrap()
        });
        // As the router accepts: the Initial, the ALPN, then the protocol's own accepting.
        let incoming = tokio::select! {
            incoming = ep.accept() => incoming.expect("the dialler's Initial reached the maker"),
            out = &mut dialler => {
                let out = out.unwrap();
                panic!("the dialler ended without sending an Initial to the maker: {} / {}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
            }
        };
        let mut accepting = incoming.accept().expect("the maker accepts the incoming connection");
        let accepted = match accepting.alpn().await {
            Ok(_) => server.on_accepting(accepting).await.map(|_| ()).map_err(|e| format!("{e:#}")),
            Err(e) => Err(format!("alpn: {e:#}")),
        };
        let out = dialler.await.unwrap();
        served.shutdown().await;
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
        assert!(header.starts_with("transport-diag ") && header.contains(&run_field) && header.contains(&format!(" node={node:?}")), "the {side}'s header names another run or node: {header}");
        assert!(line_with(file, &["step=\"endpoint-bound\"", "endpoint_id=", "bound="]).is_some(), "the {side} logged no bound endpoint:\n{file}");
    }
    assert!(maker.lines().next().unwrap_or_default().contains(" mesh=\"mesh1\""), "the maker's header names no mesh: {}", maker.lines().next().unwrap_or_default());
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
    // A diagnostic line in span evidence is an event of the diagnostic target (`"target":"transport_diag"`),
    // never the bare word: this cell's thread, which exports spans of its own, is named after its module.
    for leaked in ["noq.packet", "\"connect-failed\"", "\"accept-failed\"", "\"endpoint-bound\"", "\"target\":\"transport_diag\"", "noq_proto", "iroh::"] {
        assert!(!spans.contains(leaked), "{leaked:?} reached span evidence");
    }
    // The dial's line names its trace, and that trace is the dial span the dialler exported.
    let trace = failed.split("trace=00-").nth(1).and_then(|t| t.split('-').next()).unwrap_or_else(|| panic!("the connect failure names no trace: {failed}"));
    assert!(spans.contains(&format!("\"trace_id\":\"{trace}\"")) && spans.contains("rdm.node_rpc.connection.create.via-dial"), "the connect failure's trace {trace} is not the exported dial's");
    let _ = std::fs::remove_dir_all(&root);
}

/// CONTRACT: a process that is not part of an evidence run (it has a node log dir, `RDM_DATA_DIR`,
/// and no `RDM_EVIDENCE_DIR`: every production node) installs no transport diagnostic layer. After
/// telemetry is initialised and the endpoint bound, the noq-proto endpoint's trace callsites and the
/// `transport_diag` target are disabled, and the data dir holds no `transport-diag.log`. What must
/// NOT happen: noq or iroh handshake tracing switched on in production (hot-path tracing is
/// debug-only), or a diagnostic file written outside a gate run.
#[test]
fn a_process_outside_an_evidence_run_installs_no_transport_diagnostics() {
    let cell = "a_process_outside_an_evidence_run_installs_no_transport_diagnostics";
    let data_dir = cell_dir(cell);
    // A node outside an evidence run: a node log dir and a node name, no evidence dir.
    let env = [("RDM_EVIDENCE_DIR", None), ("RUST_LOG", None), ("RDM_DATA_DIR", Some(data_dir.as_os_str())), ("RDM_NODE_NAME", Some(std::ffi::OsStr::new("mesh1.rpc.1")))];
    if crate::own_process::delegated_with_env(module_path!(), cell, &env) {
        return;
    }
    let _ = std::fs::remove_dir_all(&data_dir);
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let telemetry = rt.block_on(async { rdm_transport::init_evidence_telemetry("rafka-rpc-node") });
    let (noq_trace, diag_target) = (tracing::enabled!(target: "noq_proto::endpoint", tracing::Level::TRACE), tracing::enabled!(target: "transport_diag", tracing::Level::INFO));
    rt.block_on(async {
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        ep.close().await;
    });
    drop(rt);
    drop(telemetry);
    rdm_transport::flush_before_exit();
    assert!(!noq_trace, "noq-proto endpoint trace is enabled in a process outside an evidence run");
    assert!(!diag_target, "the transport_diag target is enabled in a process outside an evidence run");
    let file = data_dir.join(DIAG);
    assert!(!file.exists(), "a process outside an evidence run wrote {}: {}", file.display(), std::fs::read_to_string(&file).unwrap_or_default());
    let _ = std::fs::remove_dir_all(&data_dir);
}
