//! The launcher half of a functional fabric: an in-process node-admin side (a gossip seed, a Node
//! RPC client, the `JoinNode` and `GetTopology` doors a launched node calls first) and the observer
//! the deployment pipeline waits on. Behind the `rig` feature: test crates share this one rig.
#![allow(missing_docs)]

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId};
use rdm_transport::topology::{Backbone, TopologyView};
use rdm_admin::deployment::pipeline::NodeObserver;
use rdm_admin::model::Node;
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingReply, PingRequest};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// Joined = the node's own digest for exactly this birth reached the admin's
/// the topology view; ready = the admin side holds the birth ready, set from the node.started
/// it reported (as the node-admin's own observer decides it), and a Ping is answered;
/// admission closed = no Ping runs any more.
pub struct LiveMesh {
    pub topology_view: TopologyView,
    pub client: NodeRpcClient,
    pub resolver: Arc<StaticResolver>,
    /// The commands this admin side sent and awaits a completion call for.
    pub commands: Arc<rdm_admin::node_commands::CommandView>,
}

impl LiveMesh {
    pub fn echo_target(&self, node: &Node) -> Result<NodeTarget, String> {
        let endpoint_id = node.endpoint_id.as_ref().ok_or("no fabric id")?.0.parse::<iroh::PublicKey>().map_err(|e| e.to_string())?;
        self.resolver.insert(ResolvedNode {
            node_id: node.node_id.clone(),
            name: node.name.clone(),
            endpoint_id,
            incarnation: node.incarnation_id.clone().ok_or("no incarnation")?,
            transport_addr: node.transport_addr.ok_or("no transport address")?,
        });
        Ok(NodeTarget::ExactNode(node.node_id.clone()))
    }

    /// One Ping. On loopback a live endpoint answers in milliseconds; the
    /// budget bounds a dial to an endpoint that is gone.
    pub async fn echo(&self, target: &NodeTarget) -> RpcOutcome<PingReply> {
        let opts = CallOptions {
            budget: rdm_transport::Budget::Overall(Duration::from_millis(500)),
            ..CallOptions::default()
        };
        let req = PingRequest::Ping { payload: b"ready?".to_vec() };
        self.client.call::<Ping>(target, &req, &opts).await.0
    }
}

#[async_trait::async_trait]
impl NodeObserver for LiveMesh {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<rdm_admin::deployment::pipeline::Publication> {
        self.topology_view
            .digest_view
            .get(node_id.as_str())
            .filter(|(d, _)| &d.node.incarnation == incarnation)
            .map(|(d, _)| rdm_admin::deployment::pipeline::Publication { runtime: d.node.runtime, data_dir: d.data_dir })
    }

    async fn ready(&self, node: &Node) -> Result<(), String> {
        // A joined node answers Ping while it is still Pending (its hook runs, it serves status and
        // the lifecycle commands): ready is the status this admin side set from its node.started,
        // never the Ping alone and never its heartbeat.
        match self.topology_view.digest_view.get(node.node_id.as_str()).filter(|(d, _)| node.incarnation_id.as_ref() == Some(&d.node.incarnation)) {
            Some((d, _)) if d.status == rdm_core::digest::MemberStatus::Ready => {}
            Some((d, _)) => return Err(format!("{} is held {:?}, not ready: the member status the admin side holds for the birth is {:?}", node.name, d.status, self.topology_view.digest_view.member_status(&node.node_id, &d.node.incarnation))),
            None => return Err(format!("no digest from {} for this birth", node.name)),
        }
        let target = self.echo_target(node)?;
        {
            match self.echo(&target).await {
                RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Pong { .. }) => {}
                other => return Err(format!("{} answered {}", node.name, other.name())),
            }
        }
        Ok(())
    }

    async fn ready_blocker(&self, node: &Node) -> Option<String> {
        // The same probe the node-admin's own observer sends (node-start.md step 3), to the birth
        // this launcher resolves by its own digest.
        self.echo_target(node).ok()?;
        rdm_admin::admin::probed_ready_blocker(&self.client, &self.topology_view.digest_view, node).await
    }

    async fn send_command(&self, node: &Node, cmd: rdm_admin::node_commands::NodeCommand, ctx: &rdm_admin::deployment::pipeline::CommandContext) -> rdm_admin::deployment::pipeline::CommandAdmission {
        let target = match self.echo_target(node) {
            Ok(t) => t,
            Err(reason) => return rdm_admin::deployment::pipeline::CommandAdmission::NotSent { reason, code: rdm_core::outcome::Code::Unavailable },
        };
        if matches!(cmd, rdm_admin::node_commands::NodeCommand::Stop | rdm_admin::node_commands::NodeCommand::Start) {
            // Use the real mesh-primary frame owner; the fixture supplies its held view and stores.
            let birth = self.client.delivery().me().clone();
            let mut me = Node::allocated(self.topology_view.node_name().parse().expect("the fixture admin's path"));
            me.node_id = birth.node_id.clone();
            me.incarnation_id = Some(birth.incarnation.clone());
            me.is_primary = true;
            let fabric_id = self.topology_view.digest_view.get(node.node_id.as_str()).expect("the fixture holds its commanded birth").0.fabric_id;
            let door = rdm_admin::status_rpc::StatusDoor {
                certs: None,
                app_channels: None,
                me: me.name.clone(), node_id: me.node_id.clone(), fabric_id: fabric_id.clone(),
                declaration_seam: None, command_frame_seam: None, operation_effects: Default::default(), stopped: Default::default(),
                topology: Arc::new(tokio::sync::RwLock::new(rdm_admin::topology::Topology {
                    fabric: rdm_admin::model::Fabric { id: fabric_id, name: "fabric1".into(), status: rdm_admin::model::ScopeStatus::Ready, provider: rdm_admin::model::ProviderKind::Process },
                    meshes: vec![], nodes: vec![me, node.clone()],
                })),
                declared: Default::default(), meshes_storage: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()), fabric_storage: Arc::new(rdm_admin::fabric_storage::MemoryFabricStorage::new()),
                nodes_storage: Arc::new(rdm_admin::storage::MemoryNodesStorage::default()), mesh_ids: Arc::new(Default::default), durable: Default::default(),
                caches: Default::default(),
                commands: self.commands.clone(), own: Default::default(), mesh_deleter: Default::default(),
                hold_next_reply: Default::default(), wake: Default::default(), fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(), configs: Default::default(),
                republish: Default::default(), configure: Default::default(), pre_node_ready: Default::default(), rename: Default::default(), row_moved: Default::default(), loads: None,
                digest_view: Some(self.topology_view.digest_view.clone()), builds: Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
            };
            assert!(door.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(Arc::new(self.client.clone()), Default::default(), Default::default()))).is_ok());
            return door.carry_lifecycle(node, cmd == rdm_admin::node_commands::NodeCommand::Start, ctx).await;
        }
        rdm_admin::node_commands::send_command(&self.client, &self.commands, &target, node, cmd, ctx).await
    }

    async fn await_completion(&self, node: &Node, cmd: rdm_admin::node_commands::NodeCommand, ctx: &rdm_admin::deployment::pipeline::CommandContext, within: Duration) -> rdm_admin::deployment::pipeline::Completion {
        rdm_admin::node_commands::await_completion(&self.commands, node, cmd, ctx, within).await
    }
}

/// Every runtime a process provider launched under `data_root` (each birth's data dir carries the
/// `deployment.json` the provider writes at spawn: its pid and start token) is stopped when this is
/// dropped, however the cell ends: a cell that panics before its own terminate leaves no node behind.
/// The drop returns once each of them has exited. A pid whose start token no longer matches is
/// another process (or none) and is left alone. [`AdminSide`] holds one over the data root of every
/// launch from its template.
pub struct ReapOnDrop(pub std::path::PathBuf);

impl Drop for ReapOnDrop {
    fn drop(&mut self) {
        let Ok(dirs) = std::fs::read_dir(&self.0) else { return };
        // Every birth still running is signalled first, then each exit is awaited: the births stop
        // together.
        let signalled: Vec<Pidfd> = dirs.flatten().filter_map(|dir| Pidfd::kill_birth(&dir.path())).collect();
        for birth in signalled {
            birth.await_exit(Duration::from_secs(5));
        }
    }
}

/// One process this reap signalled, held by a pidfd: the fd names that exact process for its whole
/// life (a recycled pid is another process and never matches it), and reads ready once every task of
/// it has exited, so its sockets are released.
struct Pidfd {
    fd: std::os::fd::OwnedFd,
    pid: u32,
    dir: std::path::PathBuf,
}

impl Pidfd {
    /// SIGKILL the runtime `dir`'s `deployment.json` names, when that exact process still runs.
    fn kill_birth(dir: &std::path::Path) -> Option<Self> {
        use std::os::fd::FromRawFd;
        let record = std::fs::read_to_string(dir.join("deployment.json")).ok()?;
        let record = serde_json::from_str::<serde_json::Value>(&record).ok()?;
        let pid = record["pid"].as_u64().and_then(|p| u32::try_from(p).ok())?;
        let start = record["start"].as_u64()?;
        // The fd is opened before the start token is read: a token that matches then names the
        // process the fd holds, and the signal below cannot reach a process that took the pid later.
        // SAFETY: pidfd_open(2) with a pid and no flags; no memory is touched.
        let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::c_int, 0 as libc::c_uint) };
        if raw < 0 {
            let e = std::io::Error::last_os_error();
            // ESRCH: no such process, its exit already waited for by the provider.
            if e.raw_os_error() != Some(libc::ESRCH) {
                eprintln!("reap: {} names pid {pid}, which cannot be held to stop it: pidfd_open: {e}", dir.display());
            }
            return None;
        }
        // SAFETY: `raw` is a pidfd this call just opened and nothing else owns.
        let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(raw as libc::c_int) };
        if rdm_core::runtime::process_start_token(pid) != Some(start) {
            return None;
        }
        eprintln!("reap: {} still runs as pid {pid} after the cell: killed", dir.display());
        // SAFETY: pidfd_send_signal(2) on the pidfd held above, no siginfo, no flags.
        let sent = unsafe { libc::syscall(libc::SYS_pidfd_send_signal, std::os::fd::AsRawFd::as_raw_fd(&fd), libc::SIGKILL, std::ptr::null::<libc::siginfo_t>(), 0 as libc::c_uint) };
        if sent < 0 {
            eprintln!("reap: SIGKILL to pid {pid} ({}) failed: {}", dir.display(), std::io::Error::last_os_error());
            return None;
        }
        Some(Self { fd, pid, dir: dir.to_path_buf() })
    }

    /// Wait until the process has exited, at most `within`; a process still running after that is
    /// named.
    fn await_exit(self, within: Duration) {
        let mut ready = libc::pollfd { fd: std::os::fd::AsRawFd::as_raw_fd(&self.fd), events: libc::POLLIN, revents: 0 };
        loop {
            // SAFETY: poll(2) over the one pollfd above, which outlives the call.
            let n = unsafe { libc::poll(&mut ready, 1, within.as_millis() as libc::c_int) };
            match n {
                1.. => return,
                0 => {
                    eprintln!("reap: pid {} ({}) has not exited {within:?} after SIGKILL", self.pid, self.dir.display());
                    return;
                }
                _ => {
                    let e = std::io::Error::last_os_error();
                    if e.kind() != std::io::ErrorKind::Interrupted {
                        eprintln!("reap: awaiting the exit of pid {} ({}) failed: poll: {e}", self.pid, self.dir.display());
                        return;
                    }
                }
            }
        }
    }
}

/// The id of the one mesh these functional fabrics hold (`mesh1`).
pub const TEST_MESH_ID: &str = "meshd0000001";

/// The admin side of a fabric: one endpoint serving gossip on `ip`, used as
/// the nodes' topology seed, and the observer over it. It is the launcher of every node a cell
/// launches from its template, and owns their teardown: dropped, it stops every node launched under
/// its data root and ends its publications.
pub struct AdminSide {
    pub observer: LiveMesh,
    pub seed: (String, SocketAddr),
    /// The fabric this admin side serves.
    pub fabric: FabricId,
    /// The births this admin side deployed and awaits a `JoinNode` from.
    pub joins: Arc<rdm_admin::join::Joins>,
    /// This admin side's nodes.storage: where its join door keeps an admitted birth's report durable
    /// (the birth's runtime row). A restarted admin side over the same store reads it back.
    pub nodes_storage: Arc<dyn rdm_admin::storage::NodesStorage>,
    /// The admin every launch from `template` names as its launcher.
    pub launcher: rdm_core::launch::Launcher,
    /// Every `node.config.update` a node sent this admin side, with the endpoint the transport proved
    /// sent it: the admin side answers each `ConfigUpdated` for the birth it names, as the fabric-primary
    /// answers once the command completed.
    pub config_updates: Arc<std::sync::Mutex<Vec<(iroh::PublicKey, rdm_core::status::StatusRequest)>>>,
    /// The node rows this admin side holds, its atlas (Q234): its join door reads the row at a
    /// joiner's `path.name` from it to hand the joiner its config, as a mesh-primary does.
    rows: Arc<rdm_admin::atlas::Atlas>,
    /// The digest this admin side publishes as itself, on the topology cadence as a node-admin does.
    digest: Arc<std::sync::Mutex<rdm_core::digest::MeshDigest>>,
    /// Over the data root where every launch from this admin side's template keeps its births'
    /// data dirs and the fabric's store root: stops every node launched under it, before the
    /// admin's endpoint closes.
    reap: ReapOnDrop,
    /// Its publications as its mesh's primary (`None` for a silent admin side).
    _publications: Option<AbortOnDrop>,
    /// Its own digest, published on the topology cadence (`None` for a silent admin side).
    _publisher: Option<AbortOnDrop>,
    _router: Router,
}

/// How an admin side takes its place in its mesh.
enum SideRole {
    /// Its mesh's primary: it says its own digest on the topology cadence and publishes its
    /// member projection on its own mesh channel.
    Primary,
    /// A node-admin of the mesh that says nothing of itself: no digest on the mesh channel and no
    /// member projection. It enters the channel through `seed` (its primary's gossip address), holds
    /// the projection its primary publishes there and serves that topology.
    Silent { seed: iroh::EndpointAddr },
}

/// An admin side's digest held in another admin side's view, re-heard on the topology cadence
/// while this lives: the other's member projection carries it ([`AdminSide::carry_in_projection`]).
pub struct Carried(AbortOnDrop);

impl AdminSide {
    /// The data root every launch from this admin side's template uses: the nodes under it are
    /// stopped when the admin side drops.
    pub fn data_root(&self) -> &std::path::Path {
        &self.reap.0
    }

    /// Say `status` as this admin side's own digest: how a test moves its authority from Pending
    /// to Ready.
    pub async fn publish_status(&self, status: rdm_core::digest::MemberStatus) {
        let d = {
            let mut held = self.digest.lock().unwrap();
            held.status = status;
            held.clone()
        };
        self.observer.topology_view.publish(&d).await.expect("the admin's digest publishes");
    }

    /// Register the birth `launch` describes, as this admin deployed it, before the node starts:
    /// the node's own `JoinNode` is then verified against it. `key` is the node's fabric key.
    pub fn deployed(&self, launch: &rdm_core::launch::Launch, key: &SecretKey) {
        self.register(launch, key, launch.name.clone(), None, false);
    }

    /// [`Self::deployed`] of a birth this admin side already holds: its row now names `name`, and
    /// `last_accepted` is the name its held stopped digest carries (R-JN1).
    pub fn deployed_as(&self, launch: &rdm_core::launch::Launch, key: &SecretKey, name: rdm_core::PathName, last_accepted: Option<rdm_core::PathName>) {
        self.register(launch, key, name, last_accepted, true);
    }

    fn register(&self, launch: &rdm_core::launch::Launch, key: &SecretKey, name: rdm_core::PathName, last_accepted: Option<rdm_core::PathName>, held: bool) {
        let runtime = rdm_core::runtime::await_own_record(&launch.data_dir, Duration::from_secs(10)).expect("the birth's runtime record is written");
        let _ = self.joins.expect(rdm_admin::join::Deployed {
            name,
            node_id: launch.node_id.clone(),
            incarnation: launch.incarnation.clone(),
            supersedes: launch.supersedes.clone(),
            endpoint_id: rdm_core::EndpointId(key.public().to_string()),
            runtime,
            data_dir: launch.data_dir.display().to_string(),
            last_accepted_name: last_accepted,
            held,
            listeners: Vec::new(),
        });
    }

    /// Hold `row` as the node row at its `path.name`, a blind put of its own key.
    pub fn put_row(&self, row: rdm_admin::storage::NodeRow) {
        self.rows.write_record(&row);
    }

    /// The digest this admin side holds of itself.
    pub fn own_digest(&self) -> rdm_core::digest::MeshDigest {
        self.digest.lock().unwrap().clone()
    }

    /// Hold `other`'s digest in this admin side's view as heard, again on the topology cadence while
    /// the answer lives: this admin side, its mesh's primary, carries `other` in the member projection
    /// it publishes, while `other` itself says nothing on the mesh channel.
    pub fn carry_in_projection(&self, other: &AdminSide) -> Carried {
        let (digest_view, mut digest) = (self.observer.topology_view.digest_view.clone(), other.own_digest());
        Carried(AbortOnDrop(tokio::spawn(async move {
            loop {
                digest.digest_seq += 1;
                digest.emitted_at_rafka_ms += 1;
                digest_view.record(digest.clone());
                tokio::time::sleep(rdm_transport::topology::gossip_interval()).await;
            }
        })))
    }
}

pub async fn admin_side(ip: std::net::IpAddr, fabric: &FabricId) -> AdminSide {
    admin_side_taking_joins(ip, fabric, "mesh1.admin.1", true, None, |b| b, rdm_core::digest::MemberStatus::Ready, no_certs()).await
}

/// [`admin_side`] whose rafka-time is `reference_ms` (adopted once, as a Day-0 root adopts its own
/// clock) instead of the OS clock: a node it admits adopts that lineage, so a reading that matches
/// it cannot have come from an OS clock.
pub async fn admin_side_on_rafka_time(ip: std::net::IpAddr, fabric: &FabricId, reference_ms: u64) -> AdminSide {
    admin_side_taking_joins(ip, fabric, "mesh1.admin.1", true, Some(reference_ms), |b| b, rdm_core::digest::MemberStatus::Ready, no_certs()).await
}

/// [`admin_side`] that also serves what `serve` adds (an app's ops) and publishes itself as
/// `status` to start with.
pub async fn admin_side_serving(ip: std::net::IpAddr, fabric: &FabricId, status: rdm_core::digest::MemberStatus, serve: impl FnOnce(rdm_transport::ServerBuilder) -> rdm_transport::ServerBuilder) -> AdminSide {
    admin_side_taking_joins(ip, fabric, "mesh1.admin.1", true, None, serve, status, no_certs()).await
}

/// [`admin_side_serving`] as the node-admin `name` of `mesh1` (`mesh1.admin.<n>`): a second admin
/// side of the same fabric and mesh, for a cell where a role moves between two node-admins.
pub async fn admin_side_serving_as(ip: std::net::IpAddr, fabric: &FabricId, name: &str, status: rdm_core::digest::MemberStatus, serve: impl FnOnce(rdm_transport::ServerBuilder) -> rdm_transport::ServerBuilder) -> AdminSide {
    admin_side_taking_joins_elected(ip, fabric, name, true, None, serve, status, no_certs(), SideRole::Primary).await
}

/// The node-admin `name` of `primary`'s mesh, taking joins and serving the topology it holds, that
/// says nothing of itself on the mesh channel: no digest, so no node's heartbeat view holds it, and
/// no member projection. A node holds it only where `primary` carries it in its member projection
/// ([`AdminSide::carry_in_projection`]).
pub async fn admin_side_silent_as(ip: std::net::IpAddr, primary: &AdminSide, name: &str) -> AdminSide {
    let key: iroh::PublicKey = primary.seed.0.parse().expect("the primary's endpoint id is an iroh key");
    let seed = iroh::EndpointAddr::new(key).with_ip_addr(primary.seed.1);
    admin_side_taking_joins_elected(ip, &primary.fabric, name, true, None, |b| b, rdm_core::digest::MemberStatus::Ready, no_certs(), SideRole::Silent { seed }).await
}

/// [`admin_side_silent_as`] whose `JoinNode` door is never opened: a node that joins it is answered
/// `NotReady` by name, always, so a join to it is never accepted.
pub async fn admin_side_silent_deaf_as(ip: std::net::IpAddr, primary: &AdminSide, name: &str) -> AdminSide {
    let key: iroh::PublicKey = primary.seed.0.parse().expect("the primary's endpoint id is an iroh key");
    let seed = iroh::EndpointAddr::new(key).with_ip_addr(primary.seed.1);
    admin_side_taking_joins_elected(ip, &primary.fabric, name, false, None, |b| b, rdm_core::digest::MemberStatus::Ready, no_certs(), SideRole::Silent { seed }).await
}

/// [`admin_side`] whose `JoinNode` door is never opened: a launched node's join is answered
/// `NotReady` by name, always.
pub async fn admin_side_deaf_to_joins(ip: std::net::IpAddr, fabric: &FabricId) -> AdminSide {
    admin_side_taking_joins(ip, fabric, "mesh1.admin.1", false, None, |b| b, rdm_core::digest::MemberStatus::Ready, no_certs()).await
}

fn no_certs() -> rdm_admin::certs::CertChoice {
    rdm_admin::certs::CertChoice::NoCerts
}

/// [`admin_side_on_rafka_time`] whose join door issues each admitted birth's member cert with the
/// material `certs` names, on the admin side's rafka-time (the reference, when given).
pub async fn admin_side_issuing(ip: std::net::IpAddr, fabric: &FabricId, reference_ms: Option<u64>, certs: rdm_admin::certs::CertChoice) -> AdminSide {
    admin_side_taking_joins(ip, fabric, "mesh1.admin.1", true, reference_ms, |b| b, rdm_core::digest::MemberStatus::Ready, certs).await
}

async fn admin_side_taking_joins(ip: std::net::IpAddr, fabric: &FabricId, name: &str, takes_joins: bool, reference_ms: Option<u64>, serve_app: impl FnOnce(rdm_transport::ServerBuilder) -> rdm_transport::ServerBuilder, status: rdm_core::digest::MemberStatus, certs: rdm_admin::certs::CertChoice) -> AdminSide {
    admin_side_taking_joins_elected(ip, fabric, name, takes_joins, reference_ms, serve_app, status, certs, SideRole::Primary).await
}

#[allow(clippy::too_many_arguments)]
async fn admin_side_taking_joins_elected(ip: std::net::IpAddr, fabric: &FabricId, name: &str, takes_joins: bool, reference_ms: Option<u64>, serve_app: impl FnOnce(rdm_transport::ServerBuilder) -> rdm_transport::ServerBuilder, status: rdm_core::digest::MemberStatus, certs: rdm_admin::certs::CertChoice, role: SideRole) -> AdminSide {
    // The transport a node-admin binds (rafka-node-admin-core `admin.rs`): a dead path is closed
    // within the topology silence window, so gossip redials instead of holding it.
    let transport = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(Duration::from_secs(1))
        .max_idle_timeout(Some(Duration::from_secs(3).try_into().unwrap()))
        .build();
    let alpns = vec![rdm_transport::ALPN.to_vec(), iroh_gossip::ALPN.to_vec()];
    let admin_ep = rdm_transport::endpoint::bind_exact(SecretKey::generate(), SocketAddr::new(ip, 0), alpns, transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(admin_ep.clone());
    // The stand-in admin is the fabric's Day-0 root: it adopts its own clock once and serves it.
    let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
    match reference_ms {
        Some(ms) => {
            rafka_time.adopt(ms);
        }
        None => {
            rdm_admin::rafka_time::adopt_own_clock(&tracing::Span::current(), &rafka_time, name, rdm_admin::rafka_time::OWN_CLOCK_DAY0_ROOT);
        }
    }
    let seeds = match &role {
        SideRole::Primary => vec![],
        SideRole::Silent { seed } => {
            rdm_transport::topology::learn_addresses(&admin_ep, std::slice::from_ref(seed)).unwrap();
            vec![seed.clone()]
        }
    };
    let topology_view = TopologyView::join(&gossip, &admin_ep, fabric, "mesh1", &MeshId::parse(TEST_MESH_ID).unwrap(), name, Arc::new(rafka_time.clone()), seeds, &[]).await.unwrap();
    // The join, as a node-admin serves it: a launched node reports its digest to the admin that
    // deployed it, which verifies it against the deployment and answers what it hears.
    let nodes_storage: Arc<dyn rdm_admin::storage::NodesStorage> = Arc::new(rdm_admin::storage::MemoryNodesStorage::default());
    let joins = Arc::new(rdm_admin::join::Joins::over(nodes_storage.clone()));
    let rows: Arc<rdm_admin::atlas::Atlas> = Arc::default();
    let (admin_node_id, admin_incarnation) = (NodeId::mint(), IncarnationId::mint());
    let join_slot: rdm_admin::join::JoinSlot = Arc::new(std::sync::OnceLock::new());
    let door = {
        let learner = topology_view.clone();
        Arc::new(rdm_admin::join::JoinDoor { nodes: None, types: Arc::new(|_, _| Box::pin(async { Ok(()) })),
            me: name.parse().unwrap(),
            joins: joins.clone(),
            answer: Arc::new({
                let (rafka_time, served_by) = (rafka_time.clone(), name.to_string());
                move || {
                let (rafka_time, served_by) = (rafka_time.clone(), served_by.clone());
                Box::pin(async move {
                    Ok(rdm_admin::wire::JoinAnswer {
                        served_by,
                        control: rdm_admin::wire::JoinControl { cert_policy: None, provider: rdm_admin::model::ProviderKind::Process, fabric: None, build: None, rafka_time_ms: rafka_time.now_ms(), member_cert: Vec::new(), name: None, config: None },
                        statuses: vec![],
                    })
                })
            }}),
            install: Arc::new(move |d| learner.learn(d.clone(), "join")),
            known: Arc::new(|| Box::pin(async {})),
            // The admin side records the accepted join report's Pending as a node-admin's status
            // authority does, before the join is answered (node-mesh-join.md step 10).
            pending: {
                let digest_view = topology_view.digest_view.clone();
                Arc::new(move |d, _| {
                    digest_view.reported(&d.node.node_id, &d.node.incarnation, rdm_core::MemberStatus::Pending);
                    Box::pin(async {})
                })
            },
            primary: {
                let topology_view = topology_view.clone();
                Arc::new(move || topology_view.mesh_primary().ok().map(|p| p.node.name.to_string()))
            },
            issuer: Arc::new({
                // The admin side holds the testkit fabric's one CA, kept where an admin keeps it.
                let cert_dir = std::env::temp_dir().join(format!("rdm-admin-side-certs-{}", NodeId::mint()));
                rdm_admin::certs::keep_ca(&cert_dir, &crate::test_certs::test_ca()).expect("the admin side keeps the testkit CA");
                certs.issuer(&cert_dir, false, rafka_time.clone()).expect("the admin side's cert choice is made")
            }),
            atlas: Some(rows.clone()),
        })
    };
    if takes_joins {
        let _ = join_slot.set(door);
    }
    let own_digest_slot: Arc<std::sync::OnceLock<rdm_core::digest::MeshDigest>> = Arc::new(std::sync::OnceLock::new());
    // The admin side holds its own mesh at a version its primary put into the mesh, as a running
    // admin does; it serves the topology it holds like every node.
    let admin_publisher = rdm_core::PublisherId { node: name.into(), incarnation: admin_incarnation.clone(), node_id: admin_node_id.clone() };
    let held = rdm_transport::snapshot::Chunk {
        mesh: "mesh1".into(),
        publisher: admin_publisher,
        forwarded_by: Some("topology-read".into()),
        topology_version: 1,
        snapshot_id: 1,
        chunk_index: 0,
        chunk_count: 1,
        digests: vec![],
        in_flight: vec![],
        deleted: vec![],
    };
    // A silent admin side holds only what its primary publishes into the mesh.
    if matches!(role, SideRole::Primary) {
        assert!(matches!(topology_view.take_read_chunk(held, "peer"), rdm_transport::snapshot::Taken::Installed(_)));
    }
    let topology_slot: rdm_admin::topology_read::TopologySlot = Arc::new(std::sync::OnceLock::new());
    // The completion calls a commanded node makes back (`node.connections.deleted`, `node-left`) resolve the
    // open commands of this admin side.
    let commands = Arc::new(rdm_admin::node_commands::CommandView::default());
    let config_updates: Arc<std::sync::Mutex<Vec<(iroh::PublicKey, rdm_core::status::StatusRequest)>>> = Arc::default();
    let serve_commands = {
        let (commands, receiver, digest_view, config_updates) = (commands.clone(), name.to_string(), topology_view.digest_view.clone(), config_updates.clone());
        move |b: rdm_transport::ServerBuilder| {
            b.serve::<rdm_core::status::Status, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |peer: rdm_transport::PeerContext, req: rdm_core::status::StatusRequest| {
                let (commands, receiver, digest_view, config_updates) = (commands.clone(), receiver.clone(), digest_view.clone(), config_updates.clone());
                async move {
                    use rdm_core::status::{NodeState, NotPrimary, StatusReply, StatusRequest as R};
                    // A node's own config update reaches its mesh-primary (Q235): recorded with its sender
                    // and answered as a mesh-primary answers it, the row as written at the next revision.
                    if let R::UpdateNodeConfig { node_id, incarnation, config } = &req {
                        let mut updates = config_updates.lock().unwrap();
                        updates.push((peer.endpoint_id, req.clone()));
                        let reply = StatusReply::ConfigUpdated { node_id: node_id.clone(), incarnation: incarnation.clone(), config: config.clone(), revision: updates.len() as u64 };
                        return Ok(reply);
                    }
                    // A member's report to its mesh's primary sets its status (node-status.md "Status
                    // report": "A node reports to its mesh's primary, which sets its status"), as a
                    // node-admin's status authority applies it into its view: its node.started sets
                    // it ready. A birth the view does not hold yet is not known here: the
                    // member reports again.
                    if let R::DeclareNodeState { node_id, incarnation, state, .. } = &req {
                        let Some(name) = digest_view.get(node_id.as_str()).filter(|(d, _)| d.node.incarnation == *incarnation).map(|(d, _)| d.node.name) else {
                            return Ok(rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown));
                        };
                        use rdm_core::MemberStatus as M;
                        match state {
                            NodeState::Ready => digest_view.started(node_id, incarnation, &name),
                            NodeState::Pending => digest_view.reported(node_id, incarnation, M::Pending),
                            NodeState::ConnectionsDeleting => digest_view.reported(node_id, incarnation, M::ConnectionsDeleting),
                            NodeState::Stopped => digest_view.reported(node_id, incarnation, M::Leaving),
                        };
                        return Ok(rdm_core::reply::applied());
                    }
                    match &req {
                        R::Reserved6 { .. } | R::Reserved7 { .. } => Ok(rdm_admin::node_commands::refuse_reserved(&receiver, "", &req)),
                        R::NodeLeft { .. } => Ok(rdm_admin::node_commands::refuse_node_left(&receiver, "the commanded birth", "", &req)),
                        // The admin side serves declarations only: any other request
                        // has no handler here, so not handled here (R24), decided by dispatch alone.
                        _ => Ok(rdm_admin::node_self::request_not_handled(&receiver, &req)),
                    }
                }
            })
        }
    };
    let rpc_server = serve_app(serve_commands(rdm_admin::topology_read::serve(rdm_admin::join::serve(rdm_transport::ServerBuilder::new(), join_slot), topology_slot.clone())))
        .build(rdm_transport::ServedBirth { node_id: admin_node_id.to_string(), incarnation: admin_incarnation.to_string() })
        .expect("the admin side's catalog is built");
    let router = Router::builder(admin_ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).accept(rdm_transport::ALPN, rpc_server).spawn();
    let addr: SocketAddr = admin_ep.bound_sockets().into_iter().find(|a| a.ip() == ip).unwrap();
    // The admin side publishes its digest as a real node-admin does on joining: a node addresses a
    // connections delete's completion and its start's join to the admin its topology view holds.
    let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let admin_digest = rdm_core::digest::MeshDigest { held_hash: topology_view.held_hash(),
        fabric_id: fabric.clone(),
        node: rdm_core::digest::MeshNode {
            node_id: admin_node_id.clone(),
            name: name.parse().unwrap(),
            endpoint_id: rdm_core::EndpointId(admin_ep.id().to_string()),
            transport_addr: addr,
            incarnation: admin_incarnation.clone(),
            supersedes: None,
            runtime: None,
        },
        status,
        node_state: Default::default(),
        admin_api_base: None,
        emitted_at_rafka_ms: now_ms,
        digest_seq: 1,
        data_dir: None,
        mesh_id: Some(MeshId::parse(TEST_MESH_ID).unwrap()),
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    };
    let primary = matches!(role, SideRole::Primary);
    if primary {
        topology_view.publish(&admin_digest).await.expect("the admin's digest publishes");
    }
    // The launcher is its mesh's primary, as a node-admin holding the role is: it executes its
    // members' lifecycle, sets each member's status from what it reports (its node.started, its ConnectionsDeleting and
    // Leaving), and publishes its
    // member projection on its own mesh channel at each publication and at once on a material change
    // (gossip.md §3.1 `MeshTopology`, the admin's publication). Its members take every status
    // from that projection.
    let digest_cell = Arc::new(std::sync::Mutex::new(admin_digest.clone()));
    let (publications, publisher) = if primary {
        let backbone = Backbone::join(&gossip, &admin_ep, &topology_view, "mesh1", rdm_core::PublisherId { node: name.to_string(), incarnation: admin_incarnation.clone(), node_id: admin_node_id.clone() }, vec![]).await.expect("the launcher joins the backbone");
        backbone.set_mesh_primary(true);
        if name == "mesh1.admin.1" {
            backbone.hold_primary(rdm_core::PrimaryRole::MeshPrimary, rdm_core::Primary { mesh: "mesh1".into(), node_id: admin_node_id.clone(), incarnation: admin_incarnation.clone(), revision: 1 });
        }
        let publications = {
            let (backbone, topology_view) = (backbone.clone(), topology_view.clone());
            AbortOnDrop(tokio::spawn(async move {
                loop {
                    backbone.publish(&topology_view, backbone.own_members(&topology_view)).await;
                    backbone.publication_wake(&topology_view, tokio::time::Instant::now() + rdm_transport::topology::gossip_interval()).await;
                }
            }))
        };
        let cell = digest_cell.clone();
        (Some(publications), Some(AbortOnDrop(topology_view.publish_every(rdm_transport::topology::gossip_interval(), move || cell.lock().unwrap().clone()))))
    } else {
        (None, None)
    };
    let _ = own_digest_slot.set(admin_digest.clone());
    let own_topology_view = topology_view.clone();
    let _ = topology_slot.set(Arc::new(rdm_admin::topology_read::TopologyDoor::new(topology_view.clone(), Arc::new(move || own_topology_view.reported(own_digest_slot.get().expect("the admin digest is set before a read is served"))), rafka_time.clone())));
    let resolver = Arc::new(StaticResolver::new());
    AdminSide {
        observer: LiveMesh { topology_view, client: NodeRpcClient::new(admin_ep.clone(), resolver.clone()), resolver, commands },
        seed: (admin_ep.id().to_string(), addr),
        fabric: fabric.clone(),
        joins,
        nodes_storage,
        launcher: rdm_core::launch::Launcher { name: name.parse().unwrap(), node_id: admin_node_id, incarnation: admin_incarnation },
        config_updates,
        rows,
        digest: digest_cell,
        reap: ReapOnDrop(std::env::temp_dir().join(format!("i143-e2-{}", NodeId::mint()))),
        _publications: publications,
        _publisher: publisher,
        _router: router,
    }
}

/// A task that ends with its holder.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
