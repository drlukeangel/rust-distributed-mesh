//! A consumer's own node-admin executable: the public node-admin runtime under its own service name.

#[tokio::main]
async fn main() {
    rafka_node_admin_core::entry::run("consumer-node-admin").await
}
