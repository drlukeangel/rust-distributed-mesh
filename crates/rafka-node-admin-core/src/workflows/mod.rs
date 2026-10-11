//! The lifecycle workflows: each op is one workflow, a sequence of calls to primitives in its
//! lifecycle page's order, which every node kind runs and plugs into.

pub mod node_start;
