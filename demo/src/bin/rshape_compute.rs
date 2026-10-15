//! `rshape-compute`: the R-shape consumer's compute entry point. It names its role and nothing else.

use rdm_node::{NodeKind, Role};

#[tokio::main]
async fn main() {
    rshape_consumer::exit_on_error("rshape-compute", rshape_consumer::run(Role { kind: NodeKind::Compute }, "rshape-compute").await);
}
