//! The name this birth holds now (R12, R-JN1; lifecycles/node-start.md step 2: at any join the row
//! wins). A node's name moves by `node.name.update`: a stopped node takes its row's name from its join
//! answer, a live one from `ApplyNodeName`; either way its topology view holds it. Every testkit door that
//! names this node in a reply (the proof store's provenance, the resolve probe's and the originate
//! door's `by`) and the originate door's own route source read it per call, as the connections writer
//! names its rows' source ([`rdm_admin::connections_writer::ConnectionsWriter::follow_held_name`]).
//! Before the node has joined it is the launch's name, the name it holds while nothing can have given
//! it another.

use rdm_core::PathName;
use std::sync::{Arc, OnceLock};

type Held = Arc<dyn Fn() -> PathName + Send + Sync>;

/// The name this birth holds now: its topology view's once bound, its launch's before.
#[derive(Clone)]
pub struct HeldName {
    birth: PathName,
    held: Arc<OnceLock<Held>>,
}

impl HeldName {
    /// The name of a birth launched as `birth`, read from `birth` until [`Self::follow`] binds it.
    pub fn of_birth(birth: PathName) -> Self {
        Self { birth, held: Arc::new(OnceLock::new()) }
    }

    /// Read the name from `held` (the topology view's held name) from now on. Bound once, when the node
    /// joins; a second binding is refused by name.
    pub fn follow(&self, held: impl Fn() -> PathName + Send + Sync + 'static) -> Result<(), String> {
        self.held.set(Arc::new(held)).map_err(|_| format!("{}: this birth already reads its name from its topology view", self.birth))
    }

    /// The name this birth holds now.
    pub fn now(&self) -> PathName {
        self.held.get().map_or_else(|| self.birth.clone(), |held| held())
    }
}

impl std::fmt::Debug for HeldName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldName").field("birth", &self.birth).field("now", &self.now()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_birth_holds_its_launch_name_until_its_topology_view_is_followed_then_the_topology_view_s() {
        let name = HeldName::of_birth("mesh1.rpc.7".parse().unwrap());
        assert_eq!(name.now().to_string(), "mesh1.rpc.7");
        let held = Arc::new(std::sync::Mutex::new("mesh1.rpc.1".parse::<PathName>().unwrap()));
        let h = held.clone();
        name.follow(move || h.lock().unwrap().clone()).unwrap();
        assert_eq!(name.clone().now().to_string(), "mesh1.rpc.1", "a clone reads the same binding");
        *held.lock().unwrap() = "mesh1.rpc.1.old".parse().unwrap();
        assert_eq!(name.now().to_string(), "mesh1.rpc.1.old", "read per call, never cached");
        assert!(name.follow(|| "mesh1.rpc.9".parse().unwrap()).is_err(), "a second binding is refused by name");
    }
}
