//! `rshape-node-admin`: the R-shape consumer's node-admin entry point, composed from RDM's public
//! node-admin seam (`rdm_admin::entry::run`). All configuration is the environment.

#[tokio::main]
async fn main() {
    rdm_admin::entry::run("rshape-node-admin").await
}
