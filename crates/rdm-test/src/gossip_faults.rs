//! Deterministic incoming-chunk loss, driven beside the public Channel API.
/// Drop one incoming chunk at `index` on the held app channel, then remove the cut.
/// The remaining chunks exercise the ordinary assembly expiry and loss watch.
pub async fn drop_chunk_once(
    membership: &rdm_transport::membership::Membership,
    kind: &str,
    index: usize,
) -> anyhow::Result<()> {
    membership.drop_app_chunk_once(kind, index).await
}
