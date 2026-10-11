//! `ApplyNodeName` at its subject (node-rpc-envelope.md "Meta and config", R12/R13; R-CR1, R-CR1a):
//! the node owns its name and publishes only the name its mesh-primary's row holds. It takes a new
//! name only while it holds no connection: every route it held was closed by
//! `node.connections.delete`, its drop written under the old name, before the name changes. The
//! name it already holds is done whatever it holds.

use rafka_mesh_entity::connections::{ConnectionKind, NodeConnection};
use rafka_mesh_entity::PathName;
use rafka_node_rpc_contract::status::StatusReply;

/// What the subject of an `ApplyNodeName` decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameApply {
    /// The node takes the name: it holds it and publishes it from now on.
    Applied,
    /// The node already holds the name.
    AlreadyApplied,
    /// The node still holds connections: each route by its destination, kind and open entry.
    Busy {
        /// The routes it holds, as `<destination> (<kind>, entry <id>)`.
        routes: Vec<String>,
    },
}

impl NameApply {
    /// The decision's token, for spans.
    pub fn name(&self) -> &'static str {
        match self {
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
        ConnectionKind::Proxy => format!("proxy via {}", route.carrier.as_ref().map(|c| c.name.to_string()).unwrap_or_default()),
    };
    format!("{} ({kind}, entry {})", route.destination.name, route.id)
}

/// Decide the apply of `asked` at a node holding `held` and the connections `routes` (its open
/// entries, every observation landed).
pub fn decide(held: &PathName, asked: &PathName, routes: &[NodeConnection]) -> NameApply {
    if held == asked {
        NameApply::AlreadyApplied
    } else if !routes.is_empty() {
        NameApply::Busy { routes: routes.iter().map(route_name).collect() }
    } else {
        NameApply::Applied
    }
}

/// The reply the node `me` answers for `decision`.
pub fn reply(me: &PathName, asked: &PathName, decision: &NameApply) -> StatusReply {
    match decision {
        NameApply::Applied => StatusReply::Applied,
        NameApply::AlreadyApplied => StatusReply::AlreadyApplied,
        NameApply::Busy { routes } => StatusReply::Busy {
            reason: format!(
                "{me} holds {} connection{}: {}; the name changes to {asked} only once node.connections.delete has closed every one",
                routes.len(),
                if routes.len() == 1 { "" } else { "s" },
                routes.join(", ")
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafka_mesh_entity::connections::{ConnectionEnd, ConnectionState};
    use rafka_mesh_entity::{ConnectionId, NodeId};

    /// CONTRACT (R-CR1): the held name is done whatever the node holds; another name is Busy naming
    /// every route the node holds, and Applied with none.
    #[test]
    fn a_node_takes_another_name_only_while_it_holds_no_connection() {
        let (new, bare): (PathName, PathName) = ("mesh1.rpc.1.new".parse().unwrap(), "mesh1.rpc.1".parse().unwrap());
        let end = |name: &str| ConnectionEnd { name: name.parse().unwrap(), node_id: NodeId::mint(), incarnation: None };
        let route = NodeConnection { id: ConnectionId::parse("3123456789ab").unwrap(), source: end("mesh1.rpc.1.new"), destination: end("mesh1.admin.1"), kind: ConnectionKind::Direct, state: ConnectionState::Connected, carrier: None, recovery: None, reason: None, logged_at_ms: 1 };
        assert_eq!(decide(&bare, &bare, std::slice::from_ref(&route)), NameApply::AlreadyApplied);
        let busy = decide(&new, &bare, std::slice::from_ref(&route));
        assert_eq!(busy, NameApply::Busy { routes: vec!["mesh1.admin.1 (direct, entry 3123456789ab)".into()] });
        assert_eq!(decide(&new, &bare, &[]), NameApply::Applied);
        assert_eq!(
            reply(&new, &bare, &busy),
            StatusReply::Busy { reason: "mesh1.rpc.1.new holds 1 connection: mesh1.admin.1 (direct, entry 3123456789ab); the name changes to mesh1.rpc.1 only once node.connections.delete has closed every one".into() }
        );
    }
}
