//! The name this birth holds now (R12, R-JN1; lifecycles/node-start.md step 2: at any join the row
//! wins). A node's name moves by `node.name.update`: a stopped node takes its row's name from its join
//! answer, a live one from `ApplyNodeName`; either way this holds it, and its topology view is handed it. Every testkit service that
//! names this node in a reply (the testkit store's provenance, the resolve probe's and the originate
//! service's `by`) and the originate service's own route source read it per call, as the connections writer
//! names its rows' source ([`rdm_admin::connections_writer::ConnectionsWriter::follow_held_name`](crate::connections_writer::ConnectionsWriter::follow_held_name)).
//! Before the node has joined it is the launch's name, the name it holds while nothing can have given
//! it another.

use rdm_core::PathName;
use std::sync::{Arc, Mutex};

/// The name this birth holds now: the one its row gave it ([`Self::set`]), else its launch's. The
/// node's own record holds it (gossip-by-kind.md §4.3); its topology view is handed it
/// ([`Self::told`]) and publishes it in every digest.
#[derive(Clone)]
pub struct HeldName {
    path: String,
    /// The name, beside its path.
    name: String,
    /// The name its row gave it (`ApplyNodeName`, or its join answer); `None` while it holds its launch name.
    told: Arc<Mutex<Option<PathName>>>,
}

impl HeldName {
    /// The `(path, name)` lookup index key of path and name.
    fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

impl HeldName {
    /// The name of a birth launched as `birth`: `birth` until [`Self::set`] gives it another.
    pub fn of_birth(birth: PathName) -> Self {
        Self { path: birth.path.clone(), name: birth.name.clone(), told: Arc::default() }
    }

    /// Hold `name` as this birth's name (R12: the node publishes only the name its row holds): every
    /// digest its topology view publishes from now on carries it.
    pub fn set(&self, name: PathName) {
        *self.told.lock().unwrap() = Some(name);
    }

    /// The name this birth holds now.
    pub fn now(&self) -> PathName {
        self.told.lock().unwrap().clone().unwrap_or_else(|| self.location())
    }

    /// The name its row gave it, as the topology view that publishes it is handed it
    /// (`rdm_gossip::topology::TopologyView::new`).
    pub fn told(&self) -> Arc<Mutex<Option<PathName>>> {
        self.told.clone()
    }
}

impl std::fmt::Debug for HeldName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldName").field("birth", &self.location()).field("now", &self.now()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_birth_holds_its_launch_name_until_its_row_gives_it_another() {
        let name = HeldName::of_birth(rdm_core::PathName::new("mesh1.node", "7").unwrap());
        assert_eq!(name.now(), rdm_core::PathName::new("mesh1.node", "7").unwrap());
        name.clone().set(rdm_core::PathName::new("mesh1.node", "1").unwrap());
        assert_eq!(name.now(), rdm_core::PathName::new("mesh1.node", "1").unwrap(), "a clone sets the one name");
        assert_eq!(*name.told().lock().unwrap(), Some(rdm_core::PathName::new("mesh1.node", "1").unwrap()), "the view is handed the same name");
    }
}
