//! The name this birth holds now (R12, R-JN1; lifecycles/node-start.md step 2: at any join the row
//! wins). A node's name moves by `node.name.update`: a stopped node takes its row's name from its join
//! answer, a live one from `ApplyNodeName`; either way its topology view holds it. Every testkit service that
//! names this node in a reply (the testkit store's provenance, the resolve probe's and the originate
//! service's `by`) and the originate service's own route source read it per call, as the connections writer
//! names its rows' source ([`rdm_admin::connections_writer::ConnectionsWriter::follow_held_name`](crate::connections_writer::ConnectionsWriter::follow_held_name)).
//! Before the node has joined it is the launch's name, the name it holds while nothing can have given
//! it another.

use rdm_core::PathName;
use std::sync::{Arc, OnceLock};

type Held = Arc<dyn Fn() -> PathName + Send + Sync>;

/// The name this birth holds now: its topology view's once bound, its launch's before.
#[derive(Clone)]
pub struct HeldName {
    path: String,
    /// The name, beside its path.
    name: String,
    held: Arc<OnceLock<Held>>,
}

impl HeldName {
    /// The `(path, name)` lookup index key of path and name.
    fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

impl HeldName {
    /// The name of a birth launched as `birth`, read from `birth` until [`Self::follow`] binds it.
    pub fn of_birth(birth: PathName) -> Self {
        Self { path: birth.path.clone(), name: birth.name.clone(), held: Arc::new(OnceLock::new()) }
    }

    /// Read the name from `held` (the topology view's held name) from now on. Bound once, when the node
    /// joins; a second binding is refused by name.
    pub fn follow(&self, held: impl Fn() -> PathName + Send + Sync + 'static) -> Result<(), String> {
        self.held.set(Arc::new(held)).map_err(|_| format!("path {:?}, name {:?}: this birth already reads its name from its topology view", self.path, self.name))
    }

    /// The name this birth holds now.
    pub fn now(&self) -> PathName {
        self.held.get().map_or_else(|| self.location(), |held| held())
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
    fn a_birth_holds_its_launch_name_until_its_topology_view_is_followed_then_the_topology_view_s() {
        let name = HeldName::of_birth(rdm_core::PathName::new("mesh1.node", "7").unwrap());
        assert_eq!(name.now(), rdm_core::PathName::new("mesh1.node", "7").unwrap());
        let held = Arc::new(std::sync::Mutex::new(rdm_core::PathName::new("mesh1.node", "1").unwrap()));
        let h = held.clone();
        name.follow(move || h.lock().unwrap().clone()).unwrap();
        assert_eq!(name.clone().now(), rdm_core::PathName::new("mesh1.node", "1").unwrap(), "a clone reads the same binding");
        *held.lock().unwrap() = rdm_core::PathName::new("mesh1.node", "1-old").unwrap();
        assert_eq!(name.now(), rdm_core::PathName::new("mesh1.node", "1-old").unwrap(), "read per call, never cached");
        assert!(name.follow(|| rdm_core::PathName::new("mesh1.node", "9").unwrap()).is_err(), "a second binding is refused by name");
    }
}
