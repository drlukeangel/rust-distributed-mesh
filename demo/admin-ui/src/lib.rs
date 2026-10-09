//! The demo admin UI: a client of node-admin that draws the mesh truthfully.
//!
//! It holds no lifecycle authority: every topology change is a Build submitted to node-admin
//! ([`control`]); what it shows is node-admin's own answers ([`view`]) and the nodes' own span
//! records ([`timeline`]); a fault goes through the chaos kit's typed backends ([`chaos`]).

pub mod alerts;
pub mod chaos;
pub mod control;
pub mod timeline;
pub mod view;
