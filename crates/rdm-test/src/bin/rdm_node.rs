//! `rdm-node`: the testkit's one node executable for every type but `node-admin` and `node`
//! (the harness names it per path, `rdm_test::executables`); the type is the kind of the
//! path.name its launch names, and
//! everything else it is comes from the base and that launch.

#[tokio::main]
async fn main() {
    if let Err(e) = rdm_test::run_test_node_launched().await {
        eprintln!("rdm-node: {e:#}");
        std::process::exit(3);
    }
}
