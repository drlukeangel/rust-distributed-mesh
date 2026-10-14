//! `rshape-node-admin`: the R-shape consumer's node-admin entry point, composed from RDM's public
//! node-admin seam (`rafka_node_admin_core::entry::run`). All configuration is the environment.

#[tokio::main]
async fn main() {
    rafka_node_admin_core::entry::run("rshape-node-admin").await
}
