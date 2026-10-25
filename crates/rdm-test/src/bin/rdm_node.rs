//! `rdm-node`: the testkit's one node executable for every node_type but `node_admin` and `rpc_node`
//! (the harness names it per path, `rdm_test::executables`); the node_type is the kind of the
//! path.name its launch names, and
//! everything else it is comes from the base and that launch.

#[tokio::main]
async fn main() {
    if let Err(e) = rdm_test::run_proof_launched().await {
        eprintln!("rdm-node: {e:#}");
        std::process::exit(3);
    }
}
