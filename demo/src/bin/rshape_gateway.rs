//! `rshape-gateway`: the R-shape consumer's gateway entry point. It names its role and nothing else.

use rafka_node_base::Role;

#[tokio::main]
async fn main() {
    rshape_consumer::exit_on_error("rshape-gateway", rshape_consumer::run(Role::gateway(), "rshape-gateway").await);
}
