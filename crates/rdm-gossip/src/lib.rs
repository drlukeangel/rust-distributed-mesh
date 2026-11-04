//! RDM gossip on iroh-gossip: the channels a node joins (each mesh's topology channel, the
//! node-admin backbone and the application channels), the topology view every node holds, and the
//! frames that fill it: each node's `Digest`, each mesh's full topology and its delta, and status
//! (gossip.md §3, §4, §8).
//!
//! # Where it sits
//!
//! Above `rdm-transport`, whose endpoint, clock, wire codec and chunking it uses, and below
//! `rdm-node` and `rdm-admin`, which join a node's channels with it. Nothing in `rdm-transport`
//! or `rdm-client` depends on it.
//!
//! # Guide
//!
//! [Gossip](../../guide/architecture/gossip.html), in the docs site `scripts/docs-site.sh` builds.
#![deny(missing_docs)]

pub mod envelope;
pub mod gossip;
pub(crate) mod forward_path;
pub(crate) mod discipline;
pub mod topology;
pub(crate) mod receive_order;
pub mod snapshot;
