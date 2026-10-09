//! The chaos kit: the fault backends a scenario, a test or the demo UI applies to a running
//! estate, each answering with a typed outcome.
//!
//! - [`process_faults`]: kill, stop and continue of ONE exact process runtime, read from the
//!   runtime record its birth published; refused by name when the pid is not that runtime.
//! - [`netfault`]: UDP between two sets of loopback ports dropped with `iptables`, until dropped.
//! - [`container_faults`]: Docker primitives against the exact containers of a container estate.
//!
//! The kit depends on no product crate: it reads published records as JSON and asks the OS (or
//! the Docker daemon) itself. Choosing a target, and refusing one a caller must never aim at,
//! is the caller's.
#![deny(missing_docs)]

pub mod container_faults;
pub mod netfault;
pub mod process_faults;
