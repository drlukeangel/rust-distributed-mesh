//! `rdm-node`: the one node executable of node-admin's built-in set. Every node_type but
//! `node_admin` runs it; the node_type is the kind of the path.name its launch names, and
//! everything else it is comes from the base and that launch.

#[tokio::main]
async fn main() {
    if let Err(e) = rdm_test::run_proof_launched().await {
        eprintln!("rdm-node: {e:#}");
        std::process::exit(3);
    }
}
