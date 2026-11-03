//! Stable operation identities, independent of a node's current name or resolved allocation.

use crate::{FabricId, IncarnationId, MeshId, NodeId, PathName};
use serde::{Deserialize, Serialize};

/// The subject chosen when an operation is admitted. A create's subject is the node.id the
/// fabric-primary minted for the node when it accepted the Build, as every node operation's is.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationSubject {
    /// An admitted existing node.
    ///
    /// Position 0 on the wire.
    Node(NodeId),
    /// An exact admitted birth, for operations that distinguish incarnations.
    ///
    /// Position 1 on the wire.
    Birth {
        /// The globally unique node ID.
        node_id: NodeId,
        /// The exact process incarnation, compared only for equality.
        incarnation: IncarnationId,
    },
    /// Reserved position: the removed location subject. Every node operation's subject is its node.id
    /// (D58), a create's included. Never sent; kept so every later subject keeps its position.
    ///
    /// Position 2 on the wire.
    Reserved2,
    /// An existing mesh.
    ///
    /// Position 3 on the wire.
    Mesh(MeshId),
    /// An existing fabric.
    ///
    /// Position 4 on the wire.
    Fabric(FabricId),
    /// A non-node operation's explicit name, such as a storage change.
    ///
    /// Position 5 on the wire.
    Named(String),
    /// A primary update that precedes an exact admitted operation. Build and run
    /// remain separate fields on the enclosing protocol and memo records.
    ///
    /// Position 6 on the wire.
    PrimaryUpdate {
        /// The explicitly selected role to update.
        role: crate::PrimaryRole,
        /// The original operation, including its subject and accepted step position.
        preceding: Box<OperationId>,
    },
}

/// The existing operation key carried as fields, never a joined node identifier.
/// Enclosing records retain their existing Build, attempt and execution scope.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationId {
    /// The operation token, for example `node.create`.
    pub action: String,
    /// Its admitted subject; absent for an operation with no subject.
    pub subject: Option<OperationSubject>,
    /// Original accepted recipe position, numbered from one; never the remaining-step index.
    pub recipe_step: Option<u32>,
    /// A standalone command cycle, minted once and retained across transport retries.
    pub occurrence: Option<crate::OperationOccurrenceId>,
}

impl OperationId {
    /// An operation on an explicit subject.
    pub fn new(action: impl Into<String>, subject: OperationSubject) -> Self {
        Self { action: action.into(), subject: Some(subject), recipe_step: None, occurrence: None }
    }

    /// An operation on an already admitted node ID.
    pub fn node(action: impl Into<String>, node_id: &NodeId) -> Self {
        Self::new(action, OperationSubject::Node(node_id.clone()))
    }

    /// A primary update retains the full preceding operation instead of encoding it in text.
    pub fn primary_update(role: crate::PrimaryRole, preceding: &Self) -> Self {
        let action = match role {
            crate::PrimaryRole::FabricPrimary => "node.fabric-primary.update",
            _ => "node.primary.update",
        };
        Self::new(action, OperationSubject::PrimaryUpdate { role, preceding: Box::new(preceding.clone()) })
    }

    /// Bind this operation to an explicitly minted standalone occurrence.
    pub fn in_occurrence(mut self, occurrence: crate::OperationOccurrenceId) -> Self {
        self.occurrence = Some(occurrence);
        self
    }

    /// Preserve the accepted recipe position through allocation, commands and replay.
    pub fn at_step(mut self, step: Option<u32>) -> Self {
        self.recipe_step = step;
        self
    }

    /// Keep the admitted identity for its own primitive. A composed child command has
    /// its own action and exact node ID, while retaining the admitted step position.
    pub fn command(&self, action: &str, node_id: &NodeId) -> Self {
        if self.action == action { return self.clone(); }
        let mut command = Self::node(action, node_id).at_step(self.recipe_step);
        command.occurrence = self.occurrence.clone();
        command
    }

    /// An operation without a subject. This does not parse an encoded reference.
    pub fn unscoped(action: impl Into<String>) -> Self {
        Self { action: action.into(), subject: None, recipe_step: None, occurrence: None }
    }

    /// The node the operation acts on, when it acts on one: its subject's node.id, a primary update's
    /// the node its preceding operation acts on. The `subject` of its `rdm.build.step` event (build.md §5).
    pub fn node_id(&self) -> Option<&NodeId> {
        match self.subject.as_ref()? {
            OperationSubject::Node(id) | OperationSubject::Birth { node_id: id, .. } => Some(id),
            OperationSubject::PrimaryUpdate { preceding, .. } => preceding.node_id(),
            OperationSubject::Reserved2 | OperationSubject::Mesh(_) | OperationSubject::Fabric(_) | OperationSubject::Named(_) => None,
        }
    }
}

/// The operation as the pages write it, `<action>:<subject>` (`node.create:<node_id>`,
/// `mesh.delete:<mesh_id>`, a primary update `node.primary.update:<the operation it precedes>`): the `op`
/// of its `rdm.build.step` event (build.md §5). An operation with no subject is its action alone.
impl std::fmt::Display for OperationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.action)?;
        match &self.subject {
            None | Some(OperationSubject::Reserved2) => Ok(()),
            Some(OperationSubject::Node(id)) | Some(OperationSubject::Birth { node_id: id, .. }) => write!(f, ":{id}"),
            Some(OperationSubject::Mesh(id)) => write!(f, ":{id}"),
            Some(OperationSubject::Fabric(id)) => write!(f, ":{id}"),
            Some(OperationSubject::Named(name)) => write!(f, ":{name}"),
            Some(OperationSubject::PrimaryUpdate { preceding, .. }) => write!(f, ":{preceding}"),
        }
    }
}

// Lexical ordering is only for deterministic collection storage; it assigns no temporal
// or ordinal meaning to an opaque identity.
impl OperationSubject {
    fn sort_fields(&self) -> (u8, &str, &str) {
        match self {
            Self::Node(id) => (0, id.as_str(), ""),
            Self::Birth { node_id, incarnation } => (1, node_id.as_str(), incarnation.as_str()),
            Self::Reserved2 => (2, "", ""),
            Self::Mesh(id) => (3, id.as_str(), ""),
            Self::Fabric(id) => (4, id.as_str(), ""),
            Self::Named(name) => (5, name, ""),
            Self::PrimaryUpdate { .. } => (6, "", ""),
        }
    }
}
impl Ord for OperationSubject {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::PrimaryUpdate { role: a, preceding: x }, Self::PrimaryUpdate { role: b, preceding: y }) => (a, x).cmp(&(b, y)),
            _ => self.sort_fields().cmp(&other.sort_fields()),
        }
    }
}
impl PartialOrd for OperationSubject {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: a primary update retains role, preceding action, subject and recipe position.
    // @feature: node-records
    #[test]
    fn primary_update_retains_structured_predecessor_and_role() {
        let node = NodeId::mint();
        let stop = OperationId::node("node.stop", &node).at_step(Some(3));
        let update = OperationId::primary_update(crate::PrimaryRole::MeshPrimary, &stop);
        let delete = OperationId::node("node.delete", &node).at_step(Some(3));
        let variants = [
            update.clone(),
            OperationId::primary_update(crate::PrimaryRole::FabricPrimary, &stop),
            OperationId::primary_update(crate::PrimaryRole::NodePrimary("node".into()), &stop),
            OperationId::primary_update(crate::PrimaryRole::MeshPrimary, &delete),
            OperationId::primary_update(crate::PrimaryRole::MeshPrimary, &stop.clone().at_step(Some(4))),
            OperationId::primary_update(crate::PrimaryRole::MeshPrimary, &OperationId::node("node.stop", &NodeId::mint()).at_step(Some(3))),
        ];
        assert_eq!(variants.iter().cloned().collect::<std::collections::BTreeSet<_>>().len(), variants.len());
        assert_eq!(variants.iter().cloned().collect::<std::collections::HashSet<_>>().len(), variants.len());
        let encoded = serde_json::to_value(&update).unwrap();
        assert_eq!(encoded["subject"]["PrimaryUpdate"]["preceding"]["action"], "node.stop");
        assert_eq!(serde_json::from_value::<OperationId>(encoded).unwrap(), update);
    }

    /// CONTRACT: resolving a node after admission does not change its operation identity;
    /// a composed child command names the resolved ID and retains the accepted step position.
    // @feature: node-records
    #[test]
    fn command_keeps_admitted_subject_and_scopes_nested_primitive() {
        let location = PathName::new("arbitrary.location", "worker").unwrap();
        let admitted = OperationId::node("node.stop", &crate::fixture_node_id(&location)).at_step(Some(7));
        let allocated = NodeId::mint();
        assert_eq!(admitted.command("node.stop", &allocated), admitted);
        assert_eq!(admitted.command("node.stop", &NodeId::mint()), admitted);
        assert_eq!(
            admitted.command("node.connection.delete", &allocated),
            OperationId::node("node.connection.delete", &allocated).at_step(Some(7)),
        );
        let pinned = OperationId::node("node.stop", &allocated).at_step(Some(8));
        assert_eq!(pinned.command("node.stop", &NodeId::mint()), pinned);
    }
    /// CONTRACT: each standalone cycle is distinct; retries and child commands retain it.
    // @feature: node-records
    #[test]
    fn standalone_occurrence_survives_retries_children_and_nested_wire_objects() {
        let node = NodeId::mint();
        let plain = OperationId::node("node.mesh.join", &node);
        let first = plain.clone().in_occurrence(crate::OperationOccurrenceId::mint());
        let second = plain.clone().in_occurrence(crate::OperationOccurrenceId::mint());
        assert_ne!(first, second);
        assert_eq!(first.command("node.mesh.join", &node), first);
        assert_eq!(first.command("node.start", &node).occurrence, first.occurrence);
        let nested = OperationId::primary_update(crate::PrimaryRole::MeshPrimary, &first);
        let envelope = (vec![plain, first, second, nested], 1234u32, "following field".to_owned());
        let bytes = postcard::to_allocvec(&envelope).unwrap();
        let decoded: (Vec<OperationId>, u32, String) = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, envelope);
    }

}
