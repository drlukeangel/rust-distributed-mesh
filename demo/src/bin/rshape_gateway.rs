//! `rshape-gateway`: the R-shape consumer's gateway entry point. It names its role and nothing else.

use rdm_node::{NodeKind, Role};

#[tokio::main]
async fn main() {
    rshape_consumer::exit_on_error("rshape-gateway", rshape_consumer::run(Role { kind: NodeKind::Gateway }, "rshape-gateway").await);
}
