//! Server admission: execution capacity, not transport capacity
//! (node-rpc.md §17). Node-wide per protocol, plus a per-(protocol, caller
//! fabric id) fairness cap. Admission never queues: a full slot is an
//! immediate typed `Busy`, proving the handler never ran.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub node_wide: usize,
    pub per_caller: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self { node_wide: 1024, per_caller: 256 }
    }
}

#[derive(Debug, Default)]
struct Counts {
    node: HashMap<u8, usize>,
    caller: HashMap<(u8, String), usize>,
}

#[derive(Debug, Default, Clone)]
pub struct Admission {
    limits: HashMap<u8, Limits>,
    counts: Arc<Mutex<Counts>>,
}

/// Held for the whole supervised invocation; released on every exit path.
#[derive(Debug)]
pub struct Permit {
    op: u8,
    caller: String,
    counts: Arc<Mutex<Counts>>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut c = self.counts.lock().unwrap();
        if let Some(n) = c.node.get_mut(&self.op) {
            *n -= 1;
        }
        if let Some(n) = c.caller.get_mut(&(self.op, self.caller.clone())) {
            *n -= 1;
        }
    }
}

impl Admission {
    pub fn set(&mut self, op: u8, limits: Limits) {
        self.limits.insert(op, limits);
    }

    /// `Err(reason)` is the typed `Busy` reason.
    pub fn try_admit(&self, op: u8, caller: &str) -> Result<Permit, String> {
        let l = self.limits.get(&op).copied().unwrap_or_default();
        let mut c = self.counts.lock().unwrap();
        let node = *c.node.get(&op).unwrap_or(&0);
        if node >= l.node_wide {
            return Err(format!("node-wide in-flight cap {} reached", l.node_wide));
        }
        let key = (op, caller.to_string());
        let per = *c.caller.get(&key).unwrap_or(&0);
        if per >= l.per_caller {
            return Err(format!("per-caller in-flight cap {} reached", l.per_caller));
        }
        *c.node.entry(op).or_default() += 1;
        *c.caller.entry(key).or_default() += 1;
        Ok(Permit { op, caller: caller.to_string(), counts: self.counts.clone() })
    }
}
