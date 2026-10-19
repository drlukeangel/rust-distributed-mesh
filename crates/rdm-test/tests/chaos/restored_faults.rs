//! The chaos kit's burst kill, disk fill, partition subset, link flap and inbound drop, each
//! applied end to end on a real process estate: the kit's fault span, the OS or view consequence,
//! and the heal.
//!
//! The fabric-primary is never a target: each cell passes it as protected. The network cells need
//! `nft` (root, or `sudo -n`); where the host cannot, the fault answers
//! `LinkRefusal::Unavailable` and the cell skips by that reason, or fails with it when
//! `RDM_REQUIRE_NETFAULT=1`.
//!
//! The spans a cell asserts are this process's own: the kit's fault spans. Each cell captures them
//! with its own `EvidenceCapture` on the runtime it runs the estate on, in a directory of its own
//! beside the estate's artifacts, and reads back that file and no other.

use rdm_core::NodeId;
use rdm_client::{NodeStatus, NodeView};
use rdm_test::disk_faults::DiskFull;
use rdm_test::elections::advertised_fabric_primaries;
use rdm_test::estate::{artifacts_root, named, wait_for, Estate, Owner};
use serde_json::json;
use rdm_test::link_faults::{flap_link, FirewallInbound, LinkRefusal, PartitionSubset};
use rdm_test::process_faults::{burst_kill, gone, ExactRuntime};
use serde_json::Value;
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "chaos-kit".into(),
        subfeature: "restored-faults".into(),
        rung: "MN".into(),
        provider: "process".into(),
        test: test.into(),
    }
}

fn ready(n: &NodeView) -> bool {
    n.status == NodeStatus::ReadyForTraffic
}

fn attr(sp: &Value, k: &str) -> String {
    sp["attributes"][k].as_str().unwrap_or_default().to_string()
}

/// This cell's capture of the kit's spans, in `<artifacts>/chaos-kit/<test>.capture`: the estate
/// replaces its own artifacts directory when it is born, so the capture's file is not under it.
fn capture(test: &str) -> rdm_transport::EvidenceCapture {
    let dir = artifacts_root().join("chaos-kit").join(format!("{test}.capture"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    rdm_transport::EvidenceCapture::start(&dir, "rafka-chaos-kit").expect("the cell's evidence file can be created")
}

/// mesh1 with two node-admins and three rpc nodes, all ready.
async fn estate(test: &str) -> Estate {
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settles_to(&[("mesh1", 2, 3)], SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    estate
}

async fn published(estate: &Estate, node: &str) -> ExactRuntime {
    let dir = estate.data_dir(node).await;
    ExactRuntime::published(std::path::Path::new(&dir)).unwrap_or_else(|r| panic!("{node}: no exact runtime published in {dir}: {r:?}"))
}

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// Skip a network cell by the host's named reason; any other refusal is a failure.
fn host_cannot(why: &LinkRefusal) -> bool {
    match why {
        LinkRefusal::Unavailable { reason } if std::env::var("RDM_REQUIRE_NETFAULT").is_err() => {
            eprintln!("skip: {reason}");
            true
        }
        other => panic!("the fault was refused: {other:?}"),
    }
}

async fn finish(mut estate: Estate) {
    estate.stop().await;
}

async fn back_ready(estate: &Estate, node: &str, node_id: &NodeId) -> NodeView {
    estate.until_node(node, &format!("{node} ready again as the same node"), SETTLE, |n| n.filter(|n| ready(n) && n.node_id == *node_id).cloned()).await
}

/// CONTRACT: a burst kill signals two rpc nodes' exact runtimes together, each acknowledged by its
/// exit; the third rpc node and the fabric-primary node-admin (protected) are untouched; the view
/// stops holding either victim's birth as ready. What must NOT happen: a signal to a protected or
/// unnamed process.
#[test]
fn burst_kill_takes_two_exact_runtimes_together_and_spares_the_rest() {
    let capture = capture("burst_kill_takes_two_exact_runtimes_together_and_spares_the_rest");
    capture.run(4, async {
        let estate = estate("burst_kill_takes_two_exact_runtimes_together_and_spares_the_rest").await;
        let views = estate.node_views().await;
        let nodes = Estate::values(&views);
        let fp = advertised_fabric_primaries(&nodes).into_iter().next().expect("a fabric primary is advertised");
        // The fabric primary's runtime: its published fact, or (the self-adopted bootstrap admin
        // publishes none) the OS's own record of the process this estate started.
        let protected = match ExactRuntime::published(std::path::Path::new(&estate.data_dir(&fp).await)) {
            Ok(r) => r,
            Err(_) => {
                let pid = estate.bootstrap_pid().expect("the bootstrap admin runs");
                ExactRuntime { deployment_id: String::new(), control_domain: rdm_test::process_faults::local_control_domain(), pid, start: rdm_test::process_faults::start_token(pid).expect("it runs") }
            }
        };
        let (a, b, spared) = (published(&estate, "mesh1.rpc.1").await, published(&estate, "mesh1.rpc.2").await, published(&estate, "mesh1.rpc.3").await);
        let births: Vec<_> = ["mesh1.rpc.1", "mesh1.rpc.2"].iter().map(|n| views.iter().find(|x| x.name.to_string() == *n).unwrap().incarnation_id.clone()).collect();
        let fabric_ready = estate.fabric_ready();
        let done = tokio::task::spawn_blocking({
            let (targets, protected) = (vec![a.clone(), b.clone()], vec![protected.clone()]);
            move || burst_kill(&targets, &protected, fabric_ready)
        })
        .await
        .unwrap()
        .expect("the burst applies");
        assert!(done.killed.iter().all(|k| k.exited), "{done:?}");
        assert!(gone(a.pid) && gone(b.pid), "both victims exited");
        assert!(!gone(spared.pid) && !gone(protected.pid), "the spared rpc node and the protected fabric primary still run");
        for (name, birth) in [("mesh1.rpc.1", &births[0]), ("mesh1.rpc.2", &births[1])] {
            estate.until_node(name, &format!("{name}'s birth no longer ready in the view"), SETTLE, |n| n.filter(|n| !ready(n) || n.incarnation_id != *birth).map(|_| ())).await;
        }
        finish(estate).await;
        let spans = capture.spans().expect("the cell's spans can be read");
        let burst: Vec<&Value> = named(&spans, "rdm.testkit.fault.update.via-process-burst");
        assert_eq!(burst.len(), 1, "one burst span: {burst:?}");
        assert_eq!(attr(burst[0], "targets"), "2");
        assert!(attr(burst[0], "outcome").contains("\"applied\":\"kill\""), "{}", attr(burst[0], "outcome"));
    });
}

/// CONTRACT: a disk fill really allocates its bounded filler inside ONE node's own data dir, the
/// node keeps serving (a bounded filler is not an exhausted host), and the heal removes the file.
#[test]
fn disk_fill_allocates_in_one_nodes_data_dir_and_heals_by_removal() {
    let capture = capture("disk_fill_allocates_in_one_nodes_data_dir_and_heals_by_removal");
    capture.run(4, async {
        let estate = estate("disk_fill_allocates_in_one_nodes_data_dir_and_heals_by_removal").await;
        let dir = std::path::PathBuf::from(estate.data_dir("mesh1.rpc.2").await);
        let other = std::path::PathBuf::from(estate.data_dir("mesh1.rpc.1").await);
        let fill = DiskFull::fill(&dir, 16 << 20).expect("16 MiB fits");
        assert!(fill.allocated >= 16 << 20, "{fill:?}");
        assert!(fill.path.starts_with(&dir) && !other.join(rdm_test::disk_faults::FILLER).exists(), "only the named node's dir holds a filler");
        let held = estate.node_view("mesh1.rpc.2").await;
        assert!(ready(&held), "a bounded filler leaves the node serving: {held:?}");
        let path = fill.path.clone();
        drop(fill);
        assert!(!path.exists(), "the heal removed the filler");
        finish(estate).await;
        let spans = capture.spans().expect("the cell's spans can be read");
        let fills = named(&spans, "rdm.testkit.fault.update.via-disk-fill");
        assert!(fills.iter().any(|sp| attr(sp, "outcome").starts_with("allocated")), "{fills:?}");
        assert!(named(&spans, "rdm.testkit.fault.remove.via-heal").iter().any(|sp| attr(sp, "fault") == "disk-fill"));
    });
}

/// CONTRACT: a partition subset cuts two rpc nodes from every other node: while it stands the view
/// stops holding either as ready; the fabric-primary is protected and refused by name; when the cut
/// is dropped both are ready again as the same nodes.
#[test]
fn partition_subset_cuts_a_node_set_from_the_rest_until_healed() {
    let capture = capture("partition_subset_cuts_a_node_set_from_the_rest_until_healed");
    capture.run(4, async {
        let estate = estate("partition_subset_cuts_a_node_set_from_the_rest_until_healed").await;
        let views = estate.node_views().await;
        let nodes = Estate::values(&views);
        let fp = advertised_fabric_primaries(&nodes);
        let ids: Vec<NodeId> = ["mesh1.rpc.1", "mesh1.rpc.2"].iter().map(|n| views.iter().find(|x| x.name.to_string() == *n).unwrap().node_id.clone()).collect();
        assert_eq!(PartitionSubset::start(&nodes, &fp, &fp).err(), Some(LinkRefusal::Protected { name: fp[0].clone() }), "the fabric primary is never a target");
        let cut = match PartitionSubset::start(&nodes, &names(&["mesh1.rpc.1", "mesh1.rpc.2"]), &fp) {
            Ok(c) => c,
            Err(why) => {
                host_cannot(&why);
                finish(estate).await;
                return;
            }
        };
        for n in ["mesh1.rpc.1", "mesh1.rpc.2"] {
            estate.until_node(n, &format!("{n} not ready in the view under the cut"), SETTLE, |x| x.filter(|x| !ready(x)).map(|_| ())).await;
        }
        drop(cut);
        back_ready(&estate, "mesh1.rpc.1", &ids[0]).await;
        back_ready(&estate, "mesh1.rpc.2", &ids[1]).await;
        finish(estate).await;
        let spans = capture.spans().expect("the cell's spans can be read");
        assert!(named(&spans, "rdm.testkit.fault.update.via-partition-subset").iter().any(|sp| attr(sp, "outcome").starts_with("cut:")));
        assert!(named(&spans, "rdm.testkit.fault.remove.via-heal").iter().any(|sp| attr(sp, "fault") == "partition-subset"));
    });
}

/// CONTRACT: a link between one rpc node and the rest flaps three times, each cut shorter than the
/// staleness floor and healed between; afterwards the same node is ready with the same node id and
/// the estate holds no ghost birth. Each cut and each heal leaves a span.
#[test]
fn link_flap_cuts_and_heals_on_schedule_and_leaves_the_same_birth() {
    let capture = capture("link_flap_cuts_and_heals_on_schedule_and_leaves_the_same_birth");
    capture.run(4, async {
        let estate = estate("link_flap_cuts_and_heals_on_schedule_and_leaves_the_same_birth").await;
        let views = estate.node_views().await;
        let nodes = Estate::values(&views);
        let fp = advertised_fabric_primaries(&nodes);
        let victim = "mesh1.rpc.1";
        let rest: Vec<String> = nodes.iter().filter_map(|n| n["name"].as_str()).filter(|n| *n != victim).map(String::from).collect();
        let before = views.iter().find(|n| n.name.to_string() == victim).unwrap().clone();
        let out = tokio::task::spawn_blocking({
            let (nodes, rest) = (nodes.clone(), rest.clone());
            move || flap_link(&nodes, &names(&[victim]), &rest, &fp, 3, Duration::from_millis(1200), Duration::from_millis(800))
        })
        .await
        .unwrap();
        let flapped = match out {
            Ok(f) => f,
            Err(why) => {
                host_cannot(&why);
                finish(estate).await;
                return;
            }
        };
        assert_eq!(flapped.cycles, 3);
        let back = back_ready(&estate, victim, &before.node_id).await;
        assert_eq!(back.incarnation_id, before.incarnation_id, "a flap shorter than the floor leaves the same birth");
        assert_eq!(estate.node_views().await.len(), nodes.len(), "no ghost birth");
        finish(estate).await;
        let spans = capture.spans().expect("the cell's spans can be read");
        assert_eq!(named(&spans, "rdm.testkit.fault.update.via-link-flap").len(), 1);
        assert_eq!(named(&spans, "rdm.testkit.fault.remove.via-heal").iter().filter(|sp| attr(sp, "fault") == "link-flap").count(), 3, "one heal per cycle");
    });
}

/// CONTRACT: inbound UDP to one rpc node is dropped while its own sends are not: the view stops
/// holding it as ready; when the drop is removed it is ready again as the same node.
#[test]
fn firewall_inbound_deafens_one_node_until_healed() {
    let capture = capture("firewall_inbound_deafens_one_node_until_healed");
    capture.run(4, async {
        let estate = estate("firewall_inbound_deafens_one_node_until_healed").await;
        let views = estate.node_views().await;
        let nodes = Estate::values(&views);
        let fp = advertised_fabric_primaries(&nodes);
        let victim = "mesh1.rpc.2";
        let id = views.iter().find(|n| n.name.to_string() == victim).unwrap().node_id.clone();
        let drop_in = match FirewallInbound::start(&nodes, victim, &fp) {
            Ok(f) => f,
            Err(why) => {
                host_cannot(&why);
                finish(estate).await;
                return;
            }
        };
        estate.until_node(victim, &format!("{victim} not ready under the inbound drop"), SETTLE, |x| x.filter(|x| !ready(x)).map(|_| ())).await;
        drop(drop_in);
        back_ready(&estate, victim, &id).await;
        finish(estate).await;
        let spans = capture.spans().expect("the cell's spans can be read");
        assert!(named(&spans, "rdm.testkit.fault.update.via-inbound-drop").iter().any(|sp| attr(sp, "target") == victim));
        assert!(named(&spans, "rdm.testkit.fault.remove.via-heal").iter().any(|sp| attr(sp, "fault") == "inbound-drop"));
    });
}

/// Who owns the inbound drop in [`firewall_inbound_ends_with_the_process_that_installed_it`]: the
/// copy of the cell that installs it. It writes this file once the drop stands.
const OWNER_MARKER: &str = "RDM_FAULT_OWNER_MARKER";
/// The loopback UDP port the owner drops inbound traffic to.
const OWNER_PORT: &str = "RDM_FAULT_OWNER_PORT";

/// Whether one datagram sent to `to` reaches `rx` within the probe window.
fn delivered(rx: &std::net::UdpSocket, to: std::net::SocketAddr) -> bool {
    let tx = std::net::UdpSocket::bind("127.0.0.1:0").expect("a loopback sender");
    tx.send_to(b"fault-probe", to).expect("the probe is sent");
    rx.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    rx.recv_from(&mut [0u8; 32]).is_ok()
}

/// Whether process `pid` has exited, waiting up to `within` for it to: a pidfd polls readable at exit.
fn exited(pid: u32, within: Duration) -> bool {
    // SAFETY: pidfd_open(2) with a pid and no flags names no memory; poll(2) reads one pollfd on
    // the stack; the fd it returned is closed.
    unsafe {
        let fd = libc::syscall(libc::SYS_pidfd_open, pid as libc::c_int, 0 as libc::c_uint) as libc::c_int;
        if fd < 0 {
            return std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
        }
        let mut watch = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
        let ready = libc::poll(&mut watch, 1, within.as_millis() as libc::c_int);
        libc::close(fd);
        ready == 1
    }
}

/// The fault owner process: killed when the cell ends, so a failed cell leaves none running.
struct Owned {
    child: std::process::Child,
}

impl Drop for Owned {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// CONTRACT: inbound UDP dropped to one node stops being dropped when the process that installed
/// the drop dies, however it dies. A fault owner is killed with SIGKILL, so no destructor runs; once
/// the helper process that carries its packet-filter table has exited, a datagram to the port it was
/// dropping reaches the port's socket. What must NOT happen: a drop left on the host by a dead owner,
/// deafening whichever process binds that port next.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn firewall_inbound_ends_with_the_process_that_installed_it() {
    const CELL: &str = "firewall_inbound_ends_with_the_process_that_installed_it";
    let evidence = rdm_test::estate::artifacts_root().join("chaos-kit").join(CELL);
    std::fs::create_dir_all(&evidence).unwrap();
    if crate::own_process::delegated_with_env(
        module_path!(),
        CELL,
        &[("RDM_EVIDENCE_DIR", Some(evidence.as_os_str()))],
    ) {
        return;
    }
    let guard = rdm_transport::init_evidence_telemetry("rafka-chaos-kit");
    if let Ok(marker) = std::env::var(OWNER_MARKER) {
        let port = std::env::var(OWNER_PORT).expect("the owner is told its port");
        let nodes = vec![json!({"name": "mesh1.rpc.1", "transport_addr": format!("127.0.0.1:{port}")})];
        let said = match FirewallInbound::start(&nodes, "mesh1.rpc.1", &[]) {
            Ok(fault) => {
                std::fs::write(format!("{marker}.tmp"), format!("installed {}", fault.holder_pid())).unwrap();
                std::fs::rename(format!("{marker}.tmp"), &marker).unwrap();
                drop(guard);
                loop {
                    std::thread::park();
                }
            }
            Err(why) => format!("refused: {}", serde_json::to_string(&why).unwrap()),
        };
        std::fs::write(format!("{marker}.tmp"), said).unwrap();
        std::fs::rename(format!("{marker}.tmp"), &marker).unwrap();
        return;
    }
    let rx = std::net::UdpSocket::bind("127.0.0.1:0").expect("a loopback receiver");
    let to = rx.local_addr().unwrap();
    assert!(delivered(&rx, to), "control: with no fault a datagram to {to} is delivered");
    let marker = std::env::temp_dir().join(format!("rdm-fault-owner-{}-{}", std::process::id(), to.port()));
    let _ = std::fs::remove_file(&marker);
    let full = format!("{}::{CELL}", module_path!().split_once("::").map(|(_, m)| m).unwrap_or(""));
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([full.as_str(), "--exact", "--nocapture", "--test-threads=1"])
        .env(OWNER_MARKER, &marker)
        .env(OWNER_PORT, to.port().to_string())
        .spawn()
        .expect("the fault owner starts");
    let mut owned = Owned { child };
    let said = wait_for("the owner installed the drop", Duration::from_secs(60), || async { std::fs::read_to_string(&marker).ok() }).await;
    let _ = std::fs::remove_file(&marker);
    let Some(holder) = said.strip_prefix("installed ").and_then(|p| p.parse::<u32>().ok()) else {
        eprintln!("skip: {said}");
        assert!(std::env::var("RDM_REQUIRE_NETFAULT").is_err(), "the fault was refused: {said}");
        return;
    };
    assert!(!delivered(&rx, to), "control: while its owner lives the drop holds a datagram to {to} back");
    let span = tracing::info_span!("rdm.testkit.fault.remove.via-owner-exit", fault = "inbound-drop", owner_pid = owned.child.id(), holder_pid = holder, port = to.port(), outcome = tracing::field::Empty);
    let _in = span.enter();
    let killed = std::time::Instant::now();
    owned.child.kill().expect("SIGKILL to the owner");
    owned.child.wait().expect("the owner is reaped");
    assert!(exited(holder, Duration::from_secs(30)), "holder {holder} still runs 30 s after its owner was killed with SIGKILL");
    let held_ms = killed.elapsed().as_millis();
    let back = delivered(&rx, to);
    span.record("outcome", tracing::field::display(format!("owner killed with SIGKILL; holder {holder} exited {held_ms} ms later; datagram to {to} delivered={back}")));
    drop(_in);
    drop(span);
    drop(guard);
    assert!(back, "the owner was killed with SIGKILL, its holder {holder} exited, and a datagram to {to} is still dropped: the drop outlived the process that installed it");
}
