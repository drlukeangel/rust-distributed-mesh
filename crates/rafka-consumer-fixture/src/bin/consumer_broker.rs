//! A consumer's own `broker` executable: the public node base running one role, and nothing else.

#[tokio::main]
async fn main() {
    if let Err(e) = rafka_node_base::run(rafka_node_base::Role::broker()).await {
        eprintln!("consumer-broker: {e:#}");
        std::process::exit(3);
    }
}
