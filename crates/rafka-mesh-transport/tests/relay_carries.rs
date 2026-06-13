//! Sprint-16 / PRD-01 — the standing OPEN claim, proven: the relay actually CARRIES
//! cross-mesh traffic when there is NO direct path.
//!
//! Approach (per the relay-fallback-proof memory): iroh's BUILT-IN `test_utils`,
//! cross-platform (Windows, no WSL, no Docker, no external patchbay, NOT the live
//! `IrohMeshTransport` — which has the insecure-CA blocker).
//!
//! Airtight, no timing race: both endpoints call `.clear_ip_transports()`, so the IP
//! transport does not exist and a direct hole-punch is PHYSICALLY IMPOSSIBLE. The
//! relay is the only transport. The client dials a RELAY-ONLY `EndpointAddr`
//! (`.with_relay_url`, no direct addr). A successful bi-stream echo therefore proves
//! the relay carried the bytes — and we additionally assert the selected QUIC path
//! is the relay (`is_relayed`), so it's not merely "connected via relay then
//! upgraded to direct" (there is no direct to upgrade to).

use iroh::endpoint::presets;
use iroh::tls::CaTlsConfig;
use iroh::test_utils::run_relay_server;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};

const TEST_ALPN: &[u8] = b"rafka-relay-carries-v1";

/// True iff the connection's SELECTED path is a relay path. (A relay path merely
/// existing while a direct path is selected would NOT be proof — but here we also
/// cleared IP transports, so no direct path can exist at all.)
fn is_relay_selected(conn: &iroh::endpoint::Connection) -> bool {
    conn.paths()
        .iter()
        .find(|p| p.is_selected())
        .is_some_and(|p| p.is_relay())
}

#[tokio::test]
async fn relay_carries_cross_mesh_write_when_direct_is_impossible() {
    // 1. Stand up a local relay (built-in test_utils; self-signed cert).
    let (relay_map, relay_url, _relay_guard) =
        run_relay_server().await.expect("run_relay_server");

    // 2. Server endpoint — relay-only: RelayMode::Custom + trust the test cert +
    //    clear_ip_transports() so NO direct IP path is even possible.
    let server_secret = SecretKey::generate();
    let server_id = server_secret.public();
    let server_ep = Endpoint::builder(presets::N0)
        .secret_key(server_secret)
        .relay_mode(RelayMode::Custom(relay_map.clone()))
        .ca_tls_config(CaTlsConfig::insecure_skip_verify())
        .clear_ip_transports()
        .alpns(vec![TEST_ALPN.to_vec()])
        .bind()
        .await
        .expect("server bind");
    // Wait until the server is actually reachable via the relay before the client dials.
    server_ep.online().await;

    // Server accept loop: echo one bi-stream payload back.
    let server_accept = {
        let ep = server_ep.clone();
        tokio::spawn(async move {
            let incoming = ep.accept().await.expect("incoming");
            let conn = incoming.await.expect("server conn");
            let (mut send, mut recv) = conn.accept_bi().await.expect("accept_bi");
            let msg = recv.read_to_end(256).await.expect("server read");
            send.write_all(&msg).await.expect("server write");
            send.finish().expect("server finish");
            // Hold the connection open so the client can read the echo + inspect paths.
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), conn.closed()).await;
        })
    };

    // 3. Client endpoint — also relay-only (no IP transport).
    let client_ep = Endpoint::builder(presets::N0)
        .relay_mode(RelayMode::Custom(relay_map))
        .ca_tls_config(CaTlsConfig::insecure_skip_verify())
        .clear_ip_transports()
        .alpns(vec![TEST_ALPN.to_vec()])
        .bind()
        .await
        .expect("client bind");

    // 4. Dial a RELAY-ONLY address (relay URL, NO direct transport addr). The only
    //    transport either side has is the relay, so iroh must carry this over it.
    let dest = EndpointAddr::new(server_id).with_relay_url(relay_url);
    let conn = client_ep
        .connect(dest, TEST_ALPN)
        .await
        .expect("connect via relay-only address");

    // 5. The cross-mesh write: a bi-stream round-trip. Success == the relay carried it
    //    (there is no direct path that could have).
    let (mut send, mut recv) = conn.open_bi().await.expect("open_bi");
    let payload = b"mesh1.gateway -> mesh2.broker (relay-carried)";
    send.write_all(payload).await.expect("client write");
    send.finish().expect("client finish");
    let echoed = recv.read_to_end(256).await.expect("client read echo");
    assert_eq!(echoed, payload, "relay must carry the cross-mesh write intact");

    // 6. And the selected QUIC path IS the relay (belt-and-suspenders: direct was
    //    cleared, so this can only be the relay).
    assert!(
        is_relay_selected(&conn),
        "selected path must be the relay — direct IP transport was cleared, so the \
         relay is the only thing that could have carried the write"
    );

    conn.close(0u32.into(), b"done");
    let _ = server_accept.await;
}
