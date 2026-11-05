//! Deterministic incoming-chunk loss, driven beside the public Channel API.
/// Drop one incoming chunk at `index` on the held app channel, then remove the cut.
/// The remaining chunks exercise the ordinary assembly expiry and loss watch.
pub async fn drop_chunk_once(
    channels: &rdm_gossip::channel::Channels,
    kind: &str,
    index: usize,
) -> anyhow::Result<()> {
    channels.drop_app_chunk_once(kind, index).await
}
