//! A consumer's own `compute` executable: the public node base running one role, and nothing else.

#[tokio::main]
async fn main() {
    if let Err(e) = rafka_node_base::run(rafka_node_base::Role::compute()).await {
        eprintln!("consumer-compute: {e:#}");
        std::process::exit(3);
    }
}
