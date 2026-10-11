//! `ApplyNodeName` at its subject (node-rpc-envelope.md "Meta and config", R12/R13): the node owns
//! its name and publishes only the name its mesh-primary's row holds. It takes a new name only with
//! no traffic in flight; the name it already holds is done whatever its traffic.

use rafka_mesh_entity::PathName;
use rafka_node_rpc_contract::status::StatusReply;

/// What the subject of an `ApplyNodeName` decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameApply {
    /// The node takes the name: it holds it and publishes it from now on.
    Applied,
    /// The node already holds the name.
    AlreadyApplied,
    /// The node still routes traffic: `in_flight` calls other than the apply itself.
    Busy {
        /// The calls in flight, the apply call not counted.
        in_flight: u64,
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

/// Decide the apply of `asked` at a node holding `held` with `in_flight` calls in flight (the apply
/// call itself not counted).
pub fn decide(held: &PathName, asked: &PathName, in_flight: u64) -> NameApply {
    if held == asked {
        NameApply::AlreadyApplied
    } else if in_flight > 0 {
        NameApply::Busy { in_flight }
    } else {
        NameApply::Applied
    }
}

/// The reply the node `me` answers for `decision`.
pub fn reply(me: &PathName, asked: &PathName, decision: &NameApply) -> StatusReply {
    match decision {
        NameApply::Applied => StatusReply::Applied,
        NameApply::AlreadyApplied => StatusReply::AlreadyApplied,
        NameApply::Busy { in_flight } => StatusReply::Busy {
            reason: format!("{me}: {in_flight} call{} in flight; the name changes to {asked} only with no traffic", if *in_flight == 1 { "" } else { "s" }),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: the held name is done whatever the traffic; another name is Busy while any call other
    /// than the apply is in flight, and Applied with none.
    #[test]
    fn a_node_takes_another_name_only_with_nothing_in_flight() {
        let (new, bare): (PathName, PathName) = ("mesh1.rpc.1.new".parse().unwrap(), "mesh1.rpc.1".parse().unwrap());
        assert_eq!(decide(&bare, &bare, 3), NameApply::AlreadyApplied);
        assert_eq!(decide(&new, &bare, 1), NameApply::Busy { in_flight: 1 });
        assert_eq!(decide(&new, &bare, 0), NameApply::Applied);
        assert_eq!(reply(&new, &bare, &NameApply::Busy { in_flight: 1 }), StatusReply::Busy { reason: "mesh1.rpc.1.new: 1 call in flight; the name changes to mesh1.rpc.1 only with no traffic".into() });
    }
}
