//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

mod acceptance_gate;
mod address_lookup;
mod admin_ui_client;
mod connections_parity;
mod dependency_rules;
mod legacy_binaries;
mod lock_ratchets;
mod parity_scanner;
mod public_api_frozen;
mod rshape_definition;
