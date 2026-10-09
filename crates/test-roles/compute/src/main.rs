//! `rafka-compute`: a product role process on the node base (i143.e11.s3). Everything it is comes from
//! the base and the launch node-admin handed it; this file names the role and nothing else.

#[tokio::main]
async fn main() {
    if let Err(e) = rafka_node_base::run(rafka_node_base::Role::compute()).await {
        eprintln!("rafka-compute: {e:#}");
        std::process::exit(3);
    }
}
