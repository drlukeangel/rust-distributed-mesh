//! `ApplyNodeName` at its subject (proposal node-properties-own-commands.md §2.2 step 6; R12/R13; R-CR1, R-CR1a):
//! the node owns its name and publishes only the name its mesh-primary's row holds. It takes a new
//! name only while it holds no connection: every route it held was closed by
//! `node.connection.delete`, its drop written under the old name, before the name changes. The
//! name it already holds is done whatever it holds.

use rdm_core::connections::{ConnectionKind, NodeConnection};
use rdm_core::{ConnectionId, PathName};
use rdm_core::status::StatusReply;

/// What the subject of an `ApplyNodeName` decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameApply {
    /// The request tried to change the node's immutable path.
    PathImmutable,
    /// The node takes the name: it holds it and publishes it from now on.
    Applied,
    /// The node already holds the name.
    AlreadyApplied,
    /// The node still holds connections: each by the minted id of its open entry, and each route by
    /// its destination, kind and open entry.
    Busy {
        /// The connections it holds, by the minted id of each open entry.
        connections: Vec<ConnectionId>,
        /// The routes it holds, as `<destination> (<kind>, entry <id>)`.
        routes: Vec<String>,
    },
}

impl NameApply {
    /// The decision's token, for spans.
    pub fn name(&self) -> &'static str {
        match self {
            Self::PathImmutable => "rejected-name",
            Self::Applied => "applied",
            Self::AlreadyApplied => "already-applied",
            Self::Busy { .. } => "busy",
        }
    }
}

/// One held route as a refusal names it.
pub fn route_name(route: &NodeConnection) -> String {
    let kind = match route.kind {
        ConnectionKind::Direct => "direct".to_string(),
        ConnectionKind::Proxy => format!("proxy via {}", route.carrier.as_ref().map(|c| c.node_id.to_string()).unwrap_or_default()),
    };
    format!("{} ({kind}, entry {})", route.destination.node_id, route.id)
}

/// Decide the apply of `asked` at a node holding `held` and the connections `routes` (those it has
/// open now, each by its entry's id, its row landed or still owed: Q169).
pub fn decide(held: &PathName, asked: &PathName, routes: &[NodeConnection]) -> NameApply {
    if held.path != asked.path {
        NameApply::PathImmutable
    } else if held == asked {
        NameApply::AlreadyApplied
    } else if !routes.is_empty() {
        NameApply::Busy { connections: routes.iter().map(|r| r.id.clone()).collect(), routes: routes.iter().map(route_name).collect() }
    } else {
        NameApply::Applied
    }
}

/// The reply a node answers for its `decision`: a node holding connections names each by its entry's
/// id (`BusyHoldingConnections`), the routes on the decision's span.
pub fn reply(decision: &NameApply) -> StatusReply {
    match decision {
        NameApply::PathImmutable => rdm_core::reply::status::bounded_rejected_name("a node's path is immutable"),
        NameApply::Applied => rdm_core::reply::applied(),
        NameApply::AlreadyApplied => rdm_core::reply::already_applied(),
        NameApply::Busy { connections, .. } => rdm_core::reply::status::bounded_busy_holding_connections(connections),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_name_apply_never_changes_the_nodes_path() {
        let held = PathName::new("apps.east", "worker").unwrap();
        let other = PathName::new("apps.west", "worker-old").unwrap();
        assert_eq!(decide(&held, &other, &[]), NameApply::PathImmutable);
        assert_eq!(decide(&held, &held.renamed().unwrap(), &[]), NameApply::Applied);
        assert_eq!(held.renamed().unwrap().path, held.path);
        assert_eq!(held.renamed().unwrap().name, "worker-old");
    }

    use rdm_core::connections::{ConnectionEnd, ConnectionState};
    use rdm_core::{ConnectionId, NodeId};

    /// CONTRACT (R-CR1; proposal node-properties-own-commands.md §2.2 step 6: "`Busy { connections }`
    /// naming each connection it still holds"): the held name is done whatever the node holds; another
    /// name is refused naming every connection the node holds by its entry's id, and Applied with none.
    #[test]
    fn a_node_takes_another_name_only_while_it_holds_no_connection() {
        let (new, bare): (PathName, PathName) = (rdm_core::PathName::new("mesh1.rpc", "7").unwrap(), rdm_core::PathName::new("mesh1.rpc", "1").unwrap());
        let end = |path: &str, name: &str, kind| ConnectionEnd { mesh: "mesh1".into(), kind, name: PathName::new(path, name).unwrap(), node_id: NodeId::mint(), incarnation: None };
        let route = NodeConnection { id: ConnectionId::parse("3123456789ab").unwrap(), source: end("mesh1.rpc", "7", rdm_core::NodeKind::RpcNode), destination: end("mesh1.admin", "1", rdm_core::NodeKind::NodeAdmin), kind: ConnectionKind::Direct, alpn: rdm_core::connections::ALPN.to_vec(), state: ConnectionState::Connected, carrier: None, recovery: None, reason: None, logged_at_ms: 1 };
        assert_eq!(decide(&bare, &bare, std::slice::from_ref(&route)), NameApply::AlreadyApplied);
        let busy = decide(&new, &bare, std::slice::from_ref(&route));
        assert_eq!(busy, NameApply::Busy { connections: vec![route.id.clone()], routes: vec![format!("{} (direct, entry 3123456789ab)", route.destination.node_id)] });
        assert_eq!(decide(&new, &bare, &[]), NameApply::Applied);
        assert_eq!(reply(&busy), StatusReply::BusyHoldingConnections { connections: vec![route.id.clone()] });
    }
}
