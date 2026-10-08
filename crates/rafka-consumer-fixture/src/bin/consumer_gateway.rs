//! A consumer's own `gateway` executable: the public node base running one role, and nothing else.

#[tokio::main]
async fn main() {
    if let Err(e) = rafka_node_base::run(rafka_node_base::Role::gateway()).await {
        eprintln!("consumer-gateway: {e:#}");
        std::process::exit(3);
    }
}
