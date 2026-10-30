//! The R-shape consumer's composition root: a role process composed from RDM's public seams and
//! nothing else. No Rafka business crate, no application semantics: a role is a label (a
//! `NodeKind`), the families it serves are the node base's generic proof families, and the
//! testkit's opaque protocols 0x70 (proof store), 0x71 (resolve probe), 0x72 (declare probe)
//! and, for the roles that originate or accept work, 0x73 (originate) are served as the testkit
//! defines them. The R-shape roles allocate no opcode; [`consumer_example`], the compiled example
//! of the consumer guide's own ops, holds two (0x50, 0x51) and runs as its own executable,
//! `rshape-example`.

pub mod consumer_example;
pub mod replica;

use anyhow::{anyhow, Result};
use rdm_core::connections::CarrierPolicy;
use rdm_core::NodeKind;
use rdm_transport::connections::Alpn;
use rdm_node::{launch_for, Role};
use rdm_test::{compose, node, originate, Oracles};
use tracing::Instrument;

/// The carrier policy of this shape: an originated call may travel over a Proxy whose carrier is a
/// gateway (connections.md section 6).
pub const CARRIER_POLICY: CarrierPolicy = CarrierPolicy::Proxy { carrier_kind: NodeKind::Gateway };

/// The shape's one data-plane protocol (connections.md §5), an `Alpn` value handed to the start seam
/// (never an ALPN constant of its own): every role registers it, proxied through a gateway, and
/// answers each stream with `echo <its path.name>: <the bytes it read>`.
pub fn data_alpn() -> Alpn {
    Alpn { name: b"demo/echo".to_vec(), proxy: true, carriers: vec![NodeKind::Gateway] }
}

/// Whether `role` serves the originate door (0x73): the roles work enters through. A compute node
/// originates opaque calls; a gateway accepts them and routes by the supplied RDM mechanisms. A
/// broker answers deterministic proof acknowledgements only.
pub fn serves_originate(role: Role) -> bool {
    matches!(role.name(), "compute" | "gateway")
}

/// Run `role` to completion on the imported substrate: boot as the launch node-admin handed says,
/// serve, wait for the stop signal, drain, leave. `binary` names the process in telemetry and
/// in its exit line.
pub async fn run(role: Role, binary: &str) -> Result<()> {
    let _telemetry = rdm_transport::telemetry::init_evidence_telemetry(binary);
    let launch = launch_for(role)?;
    let boot = tracing::info_span!(
        "rdm.mesh.node.create.via-deployment",
        node = %launch.name,
        node_id = %launch.node_id,
        incarnation_id = %launch.incarnation,
        kind = role.name(),
    );
    if let Ok(tp) = std::env::var("TRACEPARENT") {
        rdm_transport::telemetry::set_parent(&boot, &tp);
    }
    let oracles = Oracles::open(&launch)?;
    let originates = serves_originate(role);
    // The replica (Q296): the broker's records log, opened before the node serves; the gateway
    // forwards to the broker primary its topology names, the view handed over once the node is up.
    let view = std::sync::Arc::new(std::sync::OnceLock::new());
    let replica_node = replica::Node { view: view.clone(), me: launch.node_id.clone(), name: launch.name.clone() };
    let broker = match role.kind {
        NodeKind::Broker => Some(replica::Broker::open(&launch.data_dir.join("replica")).await.map_err(|e| anyhow!("refusing to start: the records log: {e}"))?),
        _ => None,
    };
    let broker_topics = broker.clone();
    // The topics projection (gossip.md §8): the broker announces on the channel, the gateway holds it
    // and pulls the node-admin's at its entry.
    let topics = replica::HeldTopics::default();
    let (types, hooks): (&[&str], rdm_node::node_hooks::NodeHooks) = match role.kind {
        NodeKind::Gateway => (&[replica::TOPIC_KIND], rdm_node::node_hooks::NodeHooks::new(std::sync::Arc::new(replica::Hydrate(topics.clone())))),
        NodeKind::Broker => (&[replica::TOPIC_KIND], rdm_node::node_hooks::NodeHooks::none()),
        _ => (&[], rdm_node::node_hooks::NodeHooks::none()),
    };
    let held = topics.clone();
    let (running, mut acceptor) = node::start_with_seams_alpn_types_and_hooks(&launch, data_alpn(), types, hooks, |b, seams| {
        let client = seams.client.clone();
        let b = oracles.serve(compose(role, &launch.node_id.to_string(), b, client), &launch, seams.resolver.clone(), &seams.name);
        let b = match (role.kind, broker) {
            (NodeKind::Broker, Some(broker)) => replica::register_broker(b, broker, replica_node),
            (NodeKind::Gateway, _) => replica::register_topics(replica::register_gateway(b, seams.client.clone(), replica_node), held),
            _ => b,
        };
        if originates {
            originate::serve_with_policy(b, seams, &launch, CARRIER_POLICY)
        } else {
            b
        }
    })
    .instrument(tracing::Span::none())
    .await
    .map_err(|e| {
        boot.in_scope(|| tracing::error!(error = %e, "node failed to come up"));
        e
    })?;
    let _ = oracles.declare_client.set(running.node_rpc.clone());
    let _ = view.set(running.topology_view.clone());
    match (role.kind, broker_topics) {
        (NodeKind::Broker, Some(broker)) => match running.topology_view.join_channel(replica::TOPIC_KIND).await {
            Ok(channel) => {
                let _ = broker.topics.set(channel);
            }
            Err(e) => tracing::error!(target: "rshape", error = ?e, "the broker could not join the topic channel: it announces no topic"),
        },
        (NodeKind::Gateway, _) => match running.topology_view.join_channel(replica::TOPIC_KIND).await {
            Ok(channel) => {
                tokio::spawn(replica::feed(topics, channel, launch.name.to_string()));
            }
            Err(e) => tracing::error!(target: "rshape", error = ?e, "the gateway could not join the topic channel: it holds only its entry's pull"),
        },
        _ => {}
    }
    // The data plane's answering side: every admitted connection's streams are echoed, named.
    let own = launch.name.to_string();
    tokio::spawn(async move {
        while let Some(conn) = acceptor.accept().await {
            let own = own.clone();
            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    let Ok(bytes) = recv.read_to_end(1 << 20).await else { break };
                    let answer = format!("echo {own}: {}", String::from_utf8_lossy(&bytes));
                    // The connection itself is RDM's record (`rdm.node.connection.serve.via-admitted`,
                    // `rdm.node.connection.create.via-dial`); the role's answer is its own log line, no span of the app's.
                    tracing::info!(target: "rshape", node = %own, peer = %conn.peer().name, kind = ?conn.kind(), bytes = bytes.len(), "a data-plane stream answered");
                    if send.write_all(answer.as_bytes()).await.is_err() || send.finish().is_err() {
                        break;
                    }
                }
            });
        }
    });
    let served = running.server.catalog().entries().count();
    boot.in_scope(|| tracing::info!(served, kind = role.name(), originates, "role process serving on the imported substrate"));
    drop(boot);
    println!("RDM_NODE_READY {}", launch.node_id);
    node::wait_for_signal(binary).await;
    tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.name, incarnation_id = %launch.incarnation).in_scope(|| tracing::info!("stopping"));
    running.stop().await;
    Ok(())
}

/// The process exit for a role binary: its error, named, and status 3 (the node base's code for a
/// node that failed to come up).
pub fn exit_on_error(binary: &str, result: Result<()>) {
    if let Err(e) = result {
        eprintln!("{binary}: {:#}", anyhow!(e));
        std::process::exit(3);
    }
}
