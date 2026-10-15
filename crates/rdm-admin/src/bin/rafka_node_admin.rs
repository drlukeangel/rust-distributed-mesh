//! `rafka-node-admin`: a fabric member serving the control API. All
//! configuration is the environment.

#[tokio::main]
async fn main() {
    rdm_admin::entry::run("rafka-node-admin").await
}
