//! `node.mesh.join` (node-mesh-join.md), the one primitive that puts a node on its mesh: `JoinNode`
//! to its accepting authority, the handed name and rafka-time taken from the answer, then the kind's
//! channels entered holding them (its mesh, and the backbone for a node-admin), nothing published
//! before. It is run by [node.start](super::node_start) step 2, by the mesh-primary's standalone
//! `JoinMesh`, and by a node-admin entering a mesh; each of them calls this, never a copy of it.

use super::node_start::{reached_back, JoinThrough, JoinTo, JOIN_ATTEMPTS};
use crate::model::{NodeId, PathName};
use crate::node_self::StartFailed;
use crate::wire::JoinAnswer;
use rdm_core::{MemberStatus, MeshDigest};
use rdm_transport::topology::TopologyView;
use rdm_transport::{NodeRpcClient, NodeTarget};
use tracing::Instrument as _;

/// What a node kind plugs into `node.mesh.join`: the digest its report presents and how it enters
/// its channels.
#[async_trait::async_trait]
pub trait MeshJoin: Send {
    /// This kind's refusal; the primitive's typed failure converts into it.
    type Error: From<StartFailed> + Send;

    /// The digest the report presents; the primitive presents it Pending.
    fn join_digest(&self) -> MeshDigest;

    /// The immutable app declaration reported on every join of this birth.
    fn join_types(&self) -> Vec<String> { Vec::new() }

    /// The listeners this birth bound from its launch (`RDM_LISTENERS`), each with the port the
    /// operating system selected, reported on every join of this birth (Q151); none for a kind that
    /// binds none.
    fn join_listeners(&self) -> Vec<(String, std::net::SocketAddr)> { Vec::new() }

    /// Enter the kind's channels holding the name the join handed (`None` with no authority), before
    /// anything is published, emitting `rdm.node.mesh.update.via-joined` per channel. `step` is the
    /// channel step's span: the kind drops it when its channel part ends.
    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&JoinAnswer>) -> Result<TopologyView, Self::Error>;
}

/// One run of the primitive.
pub struct JoinCall<'a> {
    /// The name the birth holds when it reports.
    pub name: String,
    /// The birth's node id.
    pub node_id: &'a NodeId,
    /// The birth's mesh, as the spans name it.
    pub mesh: &'a str,
    /// The birth's MeshId, as the spans name it.
    pub mesh_id: &'a str,
    /// The process's one Node RPC client.
    pub client: &'a NodeRpcClient,
    /// The process's rafka-time, adopted from the answer.
    pub rafka_time: &'a rdm_transport::clock::RafkaTime,
    /// Whom the report goes to.
    pub to: &'a JoinTo,
    /// The authority as the report's span names it.
    pub authority: &'a str,
    /// The operation the join runs for.
    pub operation: &'a str,
    /// The span the report's span is a child of (`None`: the current one).
    pub report_parent: Option<tracing::Span>,
    /// The caller's span for the report (a start's join step): who answered and the name installed are
    /// recorded on it, and it ends before the channel is entered. `Span::none()` for none.
    pub reported: tracing::Span,
    /// The span the channel step (`rdm.mesh.node.update.via-topology-joined`) is a child of: a
    /// start's root (`None`: the current one). The step opens once the report has ended.
    pub channel_parent: Option<tracing::Span>,
}

/// A completed join: the answer, who gave it, the name the birth holds from it, and its topology view.
pub struct MeshJoined {
    /// The authority's answer.
    pub answer: JoinAnswer,
    /// Who gave it, and where the steps after the join reach it.
    pub through: JoinThrough,
    /// The name the birth holds from the answer on.
    pub name: String,
    /// The birth's topology view, on its channel.
    pub topology_view: TopologyView,
}

/// `node.mesh.join` (node-mesh-join.md): `joining` from the report until the channels hold.
pub async fn node_mesh_join<J: MeshJoin + ?Sized>(node: &mut J, call: JoinCall<'_>) -> Result<MeshJoined, J::Error> {
    let JoinCall { mut name, node_id, mesh, mesh_id, client, rafka_time, to, authority, operation, report_parent, reported, channel_parent } = call;
    let mut digest = node.join_digest();
    digest.status = MemberStatus::Pending;
    let joining = match &report_parent {
        Some(parent) => tracing::info_span!(parent: parent, "rdm.node.mesh.update.via-joining", node = %name, node_id = %node_id, mesh = %mesh, mesh_id = %mesh_id, authority = %authority, operation = %operation),
        None => tracing::info_span!("rdm.node.mesh.update.via-joining", node = %name, node_id = %node_id, mesh = %mesh, mesh_id = %mesh_id, authority = %authority, operation = %operation),
    };
    let (answer, through) = join(&name, client, to, &digest, &node.join_types(), &node.join_listeners()).instrument(joining).await?;
    drop(report_parent);
    reported.record("served_by", through.name.to_string().as_str());
    reported.record("endpoint", through.anchor.as_str());
    // The name the answer hands (a join that hands none is refused, `crate::join::call_join_to_types`): the
    // birth holds it from the answer on, and every step from here is named by it.
    if let Some(handed) = &answer.control.name {
        name = handed.to_string();
    }
    if reported.is_none() {
        crate::rafka_time::adopt_join_answer(&tracing::Span::current(), rafka_time, &name, &answer);
    } else {
        crate::rafka_time::adopt_join_answer(&reported, rafka_time, &name, &answer);
    }
    reported.record("installed", name.as_str());
    drop(reported);
    // The channel step opens only once the report's step has ended (node.start's steps never overlap).
    let channel = match &channel_parent {
        Some(parent) => tracing::info_span!(parent: parent, "rdm.mesh.node.update.via-topology-joined", node = %name, mesh_id = tracing::field::Empty, seeds = tracing::field::Empty),
        None => tracing::info_span!("rdm.mesh.node.update.via-topology-joined", node = %name, mesh_id = tracing::field::Empty, seeds = tracing::field::Empty),
    };
    let topology_view = node.enter_channel(channel, Some(&answer)).await?;
    topology_view.learn_statuses(&answer.statuses);
    Ok(MeshJoined { answer, through, name, topology_view })
}

/// `JoinNode` with `digest`, each of its attempts made to whom `to` names then: the launcher, or the
/// admin whose call commanded the join last, reached back over that call. The answer and who gave
/// it: `served_by` names the admin (the launcher's record names it at a first start).
async fn join(node: &str, client: &NodeRpcClient, to: &JoinTo, digest: &MeshDigest, types: &[String], listeners: &[(String, std::net::SocketAddr)]) -> Result<(JoinAnswer, JoinThrough), StartFailed> {
    let next = || {
        let to = match to {
            JoinTo::Launcher { target, endpoint, .. } => Ok((target.clone(), endpoint.fmt_short().to_string())),
            JoinTo::Commander(commander) => Err(*commander.borrow()),
        };
        async move {
            match to {
                Ok(t) => Ok(t),
                Err(key) => reached_back(node, client, &key).await.map(|t| (t, key.fmt_short().to_string())),
            }
        }
    };
    let (answer, target) = match crate::join::call_join_to_types(client, next, digest, JOIN_ATTEMPTS, types, listeners).await {
        Ok(joined) => joined,
        Err(crate::join::JoinFailure::Refused(why)) => return Err(StartFailed { step: "node.join", reason: format!("{node} was refused its join by {}: {why}", whom(to)) }),
        Err(crate::join::JoinFailure::Unreached(why)) => return Err(StartFailed { step: "node.join", reason: format!("{node} could not join through {}: unreached: {why}", whom(to)) }),
    };
    let (endpoint, name) = match (to, &target) {
        (JoinTo::Launcher { endpoint, name, .. }, _) => (*endpoint, name.clone()),
        (JoinTo::Commander(_), NodeTarget::Endpoint { endpoint_id, .. }) => match answer.served_by.parse::<PathName>() {
            Ok(n) => (*endpoint_id, n),
            Err(e) => return Err(StartFailed { step: "node.join", reason: format!("{node}: the join answer from endpoint {endpoint_id} names who answered as {:?}, which is no path.name: {e}", answer.served_by) }),
        },
        (JoinTo::Commander(_), other) => unreachable!("a commander is reached back over its call's endpoint, never {other:?}"),
    };
    Ok((answer, JoinThrough { target, endpoint, name, anchor: endpoint.fmt_short().to_string() }))
}

/// Whom a join goes to, in words.
fn whom(to: &JoinTo) -> String {
    match to {
        JoinTo::Launcher { name, .. } => name.to_string(),
        JoinTo::Commander(commander) => format!("the admin whose call commanded it (endpoint {})", *commander.borrow()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{FabricIdentity, ProviderKind};
    use crate::wire::{answer_to_wire, JoinControl};
    use rdm_core::catalog::OpOwner;
    use rdm_core::join::{Join, JoinReply, JoinRequest};
    use rdm_core::{EndpointId, FabricId, IncarnationId, MeshId, MeshNode};
    use rdm_transport::ServerBuilder;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    struct TestJoinNode {
        ep: iroh::Endpoint,
        digest: MeshDigest,
        fabric_id: FabricId,
        mesh_id: MeshId,
        entered: Arc<Mutex<Vec<Option<PathName>>>>,
    }

    #[async_trait::async_trait]
    impl MeshJoin for TestJoinNode {
        type Error = StartFailed;

        fn join_digest(&self) -> MeshDigest {
            self.digest.clone()
        }

        async fn enter_channel(
            &mut self,
            step: tracing::Span,
            joined: Option<&JoinAnswer>,
        ) -> Result<TopologyView, Self::Error> {
            let handed_name = joined.and_then(|j| j.control.name.clone());
            self.entered.lock().unwrap().push(handed_name.clone());
            let node_name = handed_name
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| self.digest.node.name.to_string());
            tracing::info_span!(
                parent: &step,
                "rdm.node.mesh.update.via-joined",
                node = %node_name,
                mesh = %self.digest.node.name.mesh,
                mesh_id = %self.mesh_id,
                which = "own"
            )
            .in_scope(|| tracing::info!("entered mesh channel"));
            drop(step);
            let clock = Arc::new(rdm_transport::clock::RafkaTime::unadopted());
            clock.adopt(4_200_000);
            let g = iroh_gossip::net::Gossip::builder().spawn(self.ep.clone());
            TopologyView::join(
                &g,
                &self.ep,
                &self.fabric_id,
                &self.digest.node.name.mesh,
                &self.mesh_id,
                &node_name,
                clock,
                Vec::new(),
                &[],
            )
            .await
            .map_err(|e| StartFailed {
                step: "node.channel",
                reason: e.to_string(),
            })
        }
    }

    // @feature: node-mesh-join
    /// Page: docs/architecture/lifecycles/node-mesh-join.md:5
    /// "A primitive (R-MJ1). A mesh is a channel on the one fabric network, and `node.mesh.join` puts a
    /// node on it: the node reports to its primary with `JoinNode` on op `0x1D`, and on acceptance
    /// joins that mesh, `f(FabricId, MeshId)`... Progress is `joining`, completion `node.mesh.joined`."
    /// Page: docs/architecture/lifecycles/node-mesh-join.md:28
    /// "Order: Report accepted first, then the mesh; the handed name, rafka-time and cert installed before anything is published"
    #[test]
    fn node_mesh_join_reports_pending_digest_refuses_mismatch_and_installs_handed_name_and_rafka_time_before_entering_channel() {
        let dir = std::env::temp_dir().join(format!("rdm-node-mesh-join-unit-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-node-mesh-join-unit")
            .expect("evidence capture starts");

        capture.run(2, async {
            let fabric_id = FabricId::mint();
            let mesh_id = MeshId::mint();
            let admin_id = NodeId::mint();
            let admin_incarnation = IncarnationId::mint();
            let admin_name: PathName = "mesh1.admin.1".parse().unwrap();

            // Verify `whom(&JoinTo)` formatting for both launcher and commander targets.
            let sample_key = iroh::SecretKey::generate().public();
            let launcher_to = JoinTo::Launcher {
                target: NodeTarget::ExactNode(admin_id.clone()),
                endpoint: sample_key,
                name: admin_name.clone(),
            };
            assert_eq!(whom(&launcher_to), "mesh1.admin.1");
            let (_tx, rx) = tokio::sync::watch::channel(sample_key);
            let commander_to = JoinTo::Commander(rx);
            assert!(whom(&commander_to).contains(&sample_key.to_string()));

            // Set up an in-process Join (0x1D) server representing the accepting primary.
            let admin_key = iroh::SecretKey::generate();
            let admin_ep = rdm_transport::endpoint::bind(
                admin_key.clone(),
                "127.0.0.1:0".parse().unwrap(),
            )
            .await
            .unwrap();
            let admin_addr = admin_ep
                .bound_sockets()
                .into_iter()
                .find(|a| a.is_ipv4())
                .unwrap();

            let observed_statuses: Arc<Mutex<Vec<MemberStatus>>> = Arc::default();
            let next_reply: Arc<Mutex<Option<JoinReply>>> = Arc::default();

            let statuses_slot = observed_statuses.clone();
            let reply_slot = next_reply.clone();
            let server = ServerBuilder::new()
                .serve::<Join, _, _>(
                    OpOwner::Product("rdm".into()),
                    move |_peer: rdm_transport::PeerContext, req: JoinRequest| {
                        let statuses_slot = statuses_slot.clone();
                        let reply_slot = reply_slot.clone();
                        async move {
                            let JoinRequest::JoinNode { digest, .. } = req;
                            let decoded = MeshDigest::try_from(digest).expect("decode WireDigest");
                            statuses_slot.lock().unwrap().push(decoded.status);
                            Ok(reply_slot
                                .lock()
                                .unwrap()
                                .take()
                                .expect("next JoinReply armed"))
                        }
                    },
                )
                .build(rdm_transport::ServedBirth {
                    node_id: admin_id.to_string(),
                    incarnation: admin_incarnation.to_string(),
                })
                .unwrap();
            let _router = iroh::protocol::Router::builder(admin_ep.clone())
                .accept(rdm_transport::ALPN, server)
                .spawn();

            // Client side (the joining node).
            let node_key = iroh::SecretKey::generate();
            let node_ep = rdm_transport::endpoint::bind(
                node_key.clone(),
                "127.0.0.1:0".parse().unwrap(),
            )
            .await
            .unwrap();
            let node_addr = node_ep
                .bound_sockets()
                .into_iter()
                .find(|a| a.is_ipv4())
                .unwrap();
            let resolver = Arc::new(rdm_transport::LiveNodeResolver::default());
            resolver.apply(
                rdm_transport::ResolvedNode {
                    node_id: admin_id.clone(),
                    name: admin_name.clone(),
                    endpoint_id: admin_key.public(),
                    transport_addr: admin_addr,
                    incarnation: admin_incarnation.clone(),
                },
                None,
            );
            let client = NodeRpcClient::new(node_ep.clone(), resolver).with_caller_system("rdm");

            let node_id = NodeId::mint();
            let node_incarnation = IncarnationId::mint();
            let initial_name: PathName = "mesh1.rpc.1".parse().unwrap();
            let handed_name: PathName = "mesh1.rpc.2".parse().unwrap();

            // Start with status = Ready in the raw digest to prove `node_mesh_join` forces Pending on the wire.
            let digest = MeshDigest {
                held_hash: Default::default(),
                fabric_id: fabric_id.clone(),
                node: MeshNode {
                    node_id: node_id.clone(),
                    name: initial_name.clone(),
                    endpoint_id: EndpointId(node_key.public().to_string()),
                    incarnation: node_incarnation.clone(),
                    supersedes: None,
                    transport_addr: node_addr,
                    runtime: None,
                },
                status: MemberStatus::Ready,
                node_state: Default::default(),
                admin_api_base: None,
                emitted_at_rafka_ms: 0,
                digest_seq: 1,
                mesh_id: Some(mesh_id.clone()),
                in_flight: None,
                load: None,
                gossip: None,
                data_dir: Some("/var/lib/rdm/mesh1.rpc.1".into()),
                is_primary: None,
                is_fabric_primary: None,
            };

            let entered = Arc::new(Mutex::new(Vec::new()));
            let mut joiner = TestJoinNode {
                ep: node_ep,
                digest,
                fabric_id: fabric_id.clone(),
                mesh_id: mesh_id.clone(),
                entered: entered.clone(),
            };
            let to = JoinTo::Launcher {
                target: NodeTarget::ExactNode(admin_id.clone()),
                endpoint: admin_key.public(),
                name: admin_name.clone(),
            };
            let rafka_time = rdm_transport::clock::RafkaTime::unadopted();

            // 1. Refused report (`JoinMismatch`): fails immediately with `StartFailed { step: "node.join", .. }`
            //    and never enters the mesh channel.
            *next_reply.lock().unwrap() = Some(JoinReply::JoinMismatch {
                field: "incarnation".into(),
                deployed: "0000000000000001".into(),
                reported: node_incarnation.to_string(),
            });
            let err = match node_mesh_join(
                &mut joiner,
                JoinCall {
                    name: initial_name.to_string(),
                    node_id: &node_id,
                    mesh: "mesh1",
                    mesh_id: mesh_id.as_str(),
                    client: &client,
                    rafka_time: &rafka_time,
                    to: &to,
                    authority: "mesh1.admin.1",
                    operation: "node.create:mesh1.rpc.1",
                    report_parent: None,
                    reported: tracing::Span::none(),
                    channel_parent: None,
                },
            )
            .await
            {
                Ok(_) => panic!("refused JoinNode must fail node_mesh_join"),
                Err(e) => e,
            };
            assert_eq!(err.step, "node.join");
            assert!(
                err.reason.contains("was refused its join by mesh1.admin.1: incarnation"),
                "unexpected refusal reason: {}",
                err.reason
            );
            assert!(
                entered.lock().unwrap().is_empty(),
                "a refused report must not enter any mesh channel"
            );

            // 2. Accepted report (`Joined`): installs the handed name, adopts rafka-time, and enters the channel.
            let answer = JoinAnswer {
                served_by: admin_name.to_string(),
                control: JoinControl {
                    provider: ProviderKind::Process,
                    fabric: Some(FabricIdentity {
                        fabric_id: fabric_id.clone(),
                        name: "fabric1".into(),
                    }),
                    build: None,
                    rafka_time_ms: 4_200_000,
                    member_cert: Vec::new(),
                    name: Some(handed_name.clone()),
                    config: Some(BTreeMap::from([(
                        "log_level".to_string(),
                        "info".to_string(),
                    )])),
                    cert_policy: None,
                },
                statuses: Vec::new(),
            };
            *next_reply.lock().unwrap() = Some(JoinReply::Joined {
                answer: answer_to_wire(&answer).expect("encode JoinAnswer"),
            });

            let reported_span = tracing::info_span!(
                "rdm.mesh.node.update.via-join",
                node = %initial_name,
                served_by = tracing::field::Empty,
                endpoint = tracing::field::Empty,
                installed = tracing::field::Empty
            );
            let joined = node_mesh_join(
                &mut joiner,
                JoinCall {
                    name: initial_name.to_string(),
                    node_id: &node_id,
                    mesh: "mesh1",
                    mesh_id: mesh_id.as_str(),
                    client: &client,
                    rafka_time: &rafka_time,
                    to: &to,
                    authority: "mesh1.admin.1",
                    operation: "node.create:mesh1.rpc.1",
                    report_parent: None,
                    reported: reported_span,
                    channel_parent: None,
                },
            )
            .await
            .expect("accepted JoinNode must complete node_mesh_join");

            // Both reports presented status = Pending on the wire.
            assert_eq!(
                *observed_statuses.lock().unwrap(),
                vec![MemberStatus::Pending, MemberStatus::Pending]
            );
            assert_eq!(joined.name, handed_name.to_string());
            assert_eq!(joined.through.name, admin_name);
            assert!(
                rafka_time.now_ms() >= 4_200_000,
                "rafka_time must adopt the answer's rafka_time_ms"
            );
            assert_eq!(
                *entered.lock().unwrap(),
                vec![Some(handed_name)],
                "enter_channel must receive the handed name before publishing"
            );
        });

        let spans = capture.spans().expect("captured spans");
        let _ = std::fs::remove_dir_all(&dir);
        for required in [
            "rdm.node.mesh.update.via-joining",
            "rdm.mesh.entry.reject.via-topology-pull-failed",
            "rdm.mesh.entry.update.via-topology-pulled",
            "rdm.mesh.node.update.via-topology-joined",
            "rdm.node.mesh.update.via-joined",
        ] {
            assert!(
                spans.iter().any(|s| s["name"] == required),
                "expected span {required} in node_mesh_join unit cell"
            );
        }
    }
}

