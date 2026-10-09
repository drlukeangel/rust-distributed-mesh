//! `rafka-node-admin`: a fabric member serving the control API. All
//! configuration is the environment.

#[tokio::main]
async fn main() {
    rafka_node_admin_core::entry::run("rafka-node-admin").await
}
