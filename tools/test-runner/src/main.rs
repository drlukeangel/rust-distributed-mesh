//! `rdm-test-runner`: the test inventory and runs behind `/api/tests/*`.
//! `RDM_TESTS_TREE` names the checkout whose built test executables it runs (default the main
//! checkout), `RDM_TEST_RUNNER_BIND_ADDR` where it listens (default 127.0.0.1:19190),
//! `RDM_TESTS_PARALLEL` the default process bound (8).

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let bind = std::env::var("RDM_TEST_RUNNER_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:19190".into());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    eprintln!("rdm-test-runner listening on {bind}");
    axum::serve(listener, rdm_test_runner::router()).await
}
