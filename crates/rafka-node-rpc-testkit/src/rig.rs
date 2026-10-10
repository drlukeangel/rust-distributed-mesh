//! The launcher half of a functional fabric: an in-process node-admin side (a gossip seed, a Node
//! RPC client, the `JoinNode` and `GetTopology` doors a launched node calls first) and the observer
//! the deployment pipeline waits on. Behind the `rig` feature: test crates share this one rig.
#![allow(missing_docs)]

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId};
use rafka_mesh_transport::membership::Membership;
use rafka_node_admin_core::deployment::pipeline::NodeObserver;
use rafka_node_admin_core::model::Node;
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// Joined = the node's own digest for exactly this birth reached the admin's
/// membership; ready = an Ping answered; drained =
/// this birth's digest says `Draining` with nothing in flight, or `Leaving`;
/// admission closed = no Ping runs any more.
pub struct LiveMesh {
    pub membership: Membership,
    pub client: NodeRpcClient,
    pub resolver: Arc<StaticResolver>,
    /// The commands this admin side sent and awaits a completion call for.
    pub commands: Arc<rafka_node_admin_core::node_commands::CommandBook>,
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
            budget: rafka_node_rpc::Budget::Overall(Duration::from_millis(500)),
            ..CallOptions::default()
        };
        let req = PingRequest::Ping { payload: b"ready?".to_vec() };
        self.client.call::<Ping>(target, &req, &opts).await.0
    }
}

#[async_trait::async_trait]
impl NodeObserver for LiveMesh {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<rafka_node_admin_core::deployment::pipeline::Publication> {
        self.membership
            .book
            .get(node_id.as_str())
            .filter(|(d, _)| &d.node.incarnation == incarnation)
            .map(|(d, _)| rafka_node_admin_core::deployment::pipeline::Publication { runtime: d.node.runtime, data_dir: d.data_dir })
    }

    async fn ready(&self, node: &Node) -> Result<(), String> {
        let target = self.echo_target(node)?;
        {
            match self.echo(&target).await {
                RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Pong { .. }) => {}
                other => return Err(format!("{} answered {}", node.name, other.name())),
            }
        }
        Ok(())
    }

    async fn send_command(&self, node: &Node, cmd: rafka_node_admin_core::node_commands::NodeCommand, ctx: &rafka_node_admin_core::deployment::pipeline::CommandContext) -> rafka_node_admin_core::deployment::pipeline::CommandAdmission {
        let target = match self.echo_target(node) {
            Ok(t) => t,
            Err(reason) => return rafka_node_admin_core::deployment::pipeline::CommandAdmission::NotSent { reason },
        };
        rafka_node_admin_core::node_commands::send_command(&self.client, &self.commands, &target, node, cmd, ctx).await
    }

    async fn await_completion(&self, node: &Node, cmd: rafka_node_admin_core::node_commands::NodeCommand, ctx: &rafka_node_admin_core::deployment::pipeline::CommandContext, within: Duration) -> rafka_node_admin_core::deployment::pipeline::Completion {
        rafka_node_admin_core::node_commands::await_completion(&self.commands, node, cmd, ctx, within).await
    }

    async fn departed(&self, node: &Node) -> bool {
        use rafka_mesh_entity::MemberStatus;
        self.membership
            .book
            .get(node.node_id.as_str())
            .is_some_and(|(d, _)| Some(&d.node.incarnation) == node.incarnation_id.as_ref() && d.status == MemberStatus::Leaving)
    }
}

/// The id of the one mesh these functional fabrics hold (`mesh1`).
pub const TEST_MESH_ID: &str = "meshd0000001";

/// The admin side of a fabric: one endpoint serving gossip on `ip`, used as
/// the nodes' membership seed, and the observer over it.
pub struct AdminSide {
    pub observer: LiveMesh,
    pub seed: (String, SocketAddr),
    /// The births this admin side deployed and awaits a `JoinNode` from.
    pub joins: Arc<rafka_node_admin_core::join::Joins>,
    /// The admin every launch from `template` names as its launcher.
    pub launcher: rafka_mesh_entity::launch::Launcher,
    /// The digest this admin side publishes as itself, on the membership cadence as a node-admin does.
    digest: Arc<std::sync::Mutex<rafka_mesh_entity::digest::MeshDigest>>,
    _router: Router,
}

impl AdminSide {
    /// Say `status` as this admin side's own digest: how a test moves its authority from Pending
    /// to ReadyForTraffic.
    pub async fn publish_status(&self, status: rafka_mesh_entity::digest::MemberStatus) {
        let d = {
            let mut held = self.digest.lock().unwrap();
            held.status = status;
            held.clone()
        };
        self.observer.membership.publish(&d).await.expect("the admin's digest publishes");
    }

    /// Register the birth `launch` describes, as this admin deployed it, before the node starts:
    /// the node's own `JoinNode` is then verified against it. `key` is the node's fabric key.
    pub fn deployed(&self, launch: &rafka_mesh_entity::launch::Launch, key: &SecretKey) {
        let runtime = rafka_mesh_entity::runtime::await_own_record(&launch.data_dir, Duration::from_secs(10)).expect("the birth's runtime record is written");
        let _ = self.joins.expect(rafka_node_admin_core::join::Deployed {
            name: launch.name.clone(),
            node_id: launch.node_id.clone(),
            incarnation: launch.incarnation.clone(),
            supersedes: launch.supersedes.clone(),
            endpoint_id: rafka_mesh_entity::EndpointId(key.public().to_string()),
            runtime,
            data_dir: launch.data_dir.display().to_string(),
        });
    }
}

pub async fn admin_side(ip: std::net::IpAddr, fabric: &FabricId) -> AdminSide {
    admin_side_taking_joins(ip, fabric, true, None, |b| b, rafka_mesh_entity::digest::MemberStatus::ReadyForTraffic).await
}

/// [`admin_side`] whose rafka-time is `reference_ms` (adopted once, as a Day-0 root adopts its own
/// clock) instead of the OS clock: a node it admits adopts that lineage, so a reading that matches
/// it cannot have come from an OS clock.
pub async fn admin_side_on_rafka_time(ip: std::net::IpAddr, fabric: &FabricId, reference_ms: u64) -> AdminSide {
    admin_side_taking_joins(ip, fabric, true, Some(reference_ms), |b| b, rafka_mesh_entity::digest::MemberStatus::ReadyForTraffic).await
}

/// [`admin_side`] that also serves what `serve` adds (an app's ops) and publishes itself as
/// `status` to start with.
pub async fn admin_side_serving(ip: std::net::IpAddr, fabric: &FabricId, status: rafka_mesh_entity::digest::MemberStatus, serve: impl FnOnce(rafka_node_rpc::ServerBuilder) -> rafka_node_rpc::ServerBuilder) -> AdminSide {
    admin_side_taking_joins(ip, fabric, true, None, serve, status).await
}

/// [`admin_side`] whose `JoinNode` door is never opened: a launched node's join is answered
/// `NotReady` by name, always.
pub async fn admin_side_deaf_to_joins(ip: std::net::IpAddr, fabric: &FabricId) -> AdminSide {
    admin_side_taking_joins(ip, fabric, false, None, |b| b, rafka_mesh_entity::digest::MemberStatus::ReadyForTraffic).await
}

async fn admin_side_taking_joins(ip: std::net::IpAddr, fabric: &FabricId, takes_joins: bool, reference_ms: Option<u64>, serve_app: impl FnOnce(rafka_node_rpc::ServerBuilder) -> rafka_node_rpc::ServerBuilder, status: rafka_mesh_entity::digest::MemberStatus) -> AdminSide {
    // The transport a node-admin binds (rafka-node-admin-core `admin.rs`): a dead path is closed
    // within the membership silence window, so gossip redials instead of holding it.
    let transport = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(Duration::from_secs(1))
        .max_idle_timeout(Some(Duration::from_secs(3).try_into().unwrap()))
        .build();
    let alpns = vec![rafka_node_rpc::ALPN.to_vec(), iroh_gossip::ALPN.to_vec()];
    let admin_ep = rafka_node_rpc::endpoint::bind_exact(SecretKey::generate(), SocketAddr::new(ip, 0), alpns, transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(admin_ep.clone());
    // The stand-in admin is the fabric's Day-0 root: it adopts its own clock once and serves it.
    let rafka_time = rafka_mesh_transport::clock::RafkaTime::unadopted();
    match reference_ms {
        Some(ms) => {
            rafka_time.adopt(ms);
        }
        None => {
            rafka_node_admin_core::rafka_time::adopt_own_clock(&tracing::Span::current(), &rafka_time, "mesh1.admin.1", rafka_node_admin_core::rafka_time::OWN_CLOCK_DAY0_ROOT);
        }
    }
    let membership = Membership::join(&gossip, &admin_ep, fabric, "mesh1", &MeshId::parse(TEST_MESH_ID).unwrap(), "mesh1.admin.1", Arc::new(rafka_time.clone()), vec![]).await.unwrap();
    // The join, as a node-admin serves it: a launched node reports its digest to the admin that
    // deployed it, which verifies it against the deployment and answers what it hears.
    let joins = Arc::new(rafka_node_admin_core::join::Joins::default());
    let (admin_node_id, admin_incarnation) = (NodeId::mint(), IncarnationId::mint());
    let join_slot: rafka_node_admin_core::join::JoinSlot = Arc::new(std::sync::OnceLock::new());
    let door = {
        let learner = membership.clone();
        Arc::new(rafka_node_admin_core::join::JoinDoor {
            me: "mesh1.admin.1".parse().unwrap(),
            joins: joins.clone(),
            answer: Arc::new({
                let rafka_time = rafka_time.clone();
                move || {
                let rafka_time = rafka_time.clone();
                Box::pin(async move {
                    Ok(rafka_node_admin_core::wire::JoinAnswer {
                        served_by: "mesh1.admin.1".into(),
                        control: rafka_node_admin_core::wire::JoinControl { provider: rafka_node_admin_core::model::ProviderKind::Process, fabric: None, shutdown: None, build: None, rafka_time_ms: rafka_time.now_ms() },
                        statuses: vec![],
                    })
                })
            }}),
            install: Arc::new(move |d| learner.learn(d.clone(), "join")),
            known: Arc::new(|| Box::pin(async {})),
            primary: Arc::new(|| None),
        })
    };
    if takes_joins {
        let _ = join_slot.set(door);
    }
    let own_digest_slot: Arc<std::sync::OnceLock<rafka_mesh_entity::digest::MeshDigest>> = Arc::new(std::sync::OnceLock::new());
    // The admin side holds its own mesh at a version its primary put into the mesh, as a running
    // admin does; it serves the topology it holds like every node.
    let admin_publisher = rafka_mesh_entity::PublisherId { node: "mesh1.admin.1".into(), incarnation: admin_incarnation.clone() };
    let held = rafka_mesh_transport::snapshot::Chunk {
        mesh: "mesh1".into(),
        publisher: admin_publisher,
        forwarded_by: Some("topology-read".into()),
        topology_version: 1,
        snapshot_id: 1,
        chunk_index: 0,
        chunk_count: 1,
        digests: vec![],
        in_flight: vec![],
        departed: vec![],
    };
    assert!(matches!(membership.take_read_chunk(held, "peer"), rafka_mesh_transport::snapshot::Taken::Installed(_)));
    let topology_slot: rafka_node_admin_core::topology_read::TopologySlot = Arc::new(std::sync::OnceLock::new());
    // The completion calls a commanded node makes back (`node-drained`, `node-left`) resolve the
    // open commands of this admin side.
    let commands = Arc::new(rafka_node_admin_core::node_commands::CommandBook::default());
    let serve_commands = {
        let commands = commands.clone();
        move |b: rafka_node_rpc::ServerBuilder| {
            b.serve::<rafka_node_rpc_contract::status::Status, _, _>(rafka_node_rpc_contract::catalog::OpOwner::Product("rdm".into()), move |_peer: rafka_node_rpc::PeerContext, req: rafka_node_rpc_contract::status::StatusRequest| {
                let commands = commands.clone();
                async move {
                    use rafka_node_rpc_contract::status::StatusRequest as R;
                    let (R::NodeDrained { node_id, incarnation, .. } | R::NodeLeft { node_id, incarnation, .. }) = &req else {
                        return Ok(rafka_node_rpc_contract::status::StatusReply::NotReady { reason: "the admin side serves completion calls only".into() });
                    };
                    let (node_id, incarnation) = (node_id.clone(), incarnation.clone());
                    Ok(rafka_node_admin_core::node_commands::accept_completion(&commands, "mesh1.admin.1", Some((&node_id, Some(&incarnation), "the commanded birth")), &req))
                }
            })
        }
    };
    let rpc_server = serve_app(serve_commands(rafka_node_admin_core::topology_read::serve(rafka_node_admin_core::join::serve(rafka_node_rpc::ServerBuilder::new(), join_slot), topology_slot.clone())))
        .seal(rafka_node_rpc::ServedBirth { node_id: admin_node_id.to_string(), incarnation: admin_incarnation.0.clone() })
        .expect("the admin side's catalog seals");
    let router = Router::builder(admin_ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).accept(rafka_node_rpc::ALPN, rpc_server).spawn();
    let addr: SocketAddr = admin_ep.bound_sockets().into_iter().find(|a| a.ip() == ip).unwrap();
    // A node admits a downward lifecycle operation (a probe, a drain) only from a node-admin it
    // holds in its own membership book, so the admin side publishes its digest as a real
    // node-admin does on joining.
    let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let admin_digest = rafka_mesh_entity::digest::MeshDigest {
        fabric_id: fabric.clone(),
        node: rafka_mesh_entity::digest::MeshNode {
            node_id: admin_node_id.clone(),
            name: "mesh1.admin.1".parse().unwrap(),
            endpoint_id: rafka_mesh_entity::EndpointId(admin_ep.id().to_string()),
            transport_addr: addr,
            incarnation: admin_incarnation.clone(),
            supersedes: None,
            runtime: None,
        },
        status,
        admin_api_base: None,
        emitted_at_rafka_ms: now_ms,
        digest_seq: 1,
        data_dir: None,
        mesh_id: Some(MeshId::parse(TEST_MESH_ID).unwrap()),
        in_flight: None,
        extra: Default::default(),
        load: None,
        gossip: None,
    };
    membership.publish(&admin_digest).await.expect("the admin's digest publishes");
    let digest_cell = Arc::new(std::sync::Mutex::new(admin_digest.clone()));
    let _publisher = {
        let cell = digest_cell.clone();
        membership.publish_every(rafka_mesh_transport::membership::gossip_interval(), move || cell.lock().unwrap().clone())
    };
    let _ = own_digest_slot.set(admin_digest.clone());
    let _ = topology_slot.set(Arc::new(rafka_node_admin_core::topology_read::TopologyDoor::new(membership.clone(), Arc::new(move || own_digest_slot.get().cloned().expect("the admin digest is set before a read is served")), rafka_time.clone())));
    let resolver = Arc::new(StaticResolver::new());
    AdminSide {
        observer: LiveMesh { membership, client: NodeRpcClient::new(admin_ep.clone(), resolver.clone()), resolver, commands },
        seed: (admin_ep.id().to_string(), addr),
        joins,
        launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: admin_node_id, incarnation: admin_incarnation },
        digest: digest_cell,
        _router: router,
    }
}
