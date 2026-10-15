//! Packing in order into gossip messages that each fit iroh-gossip's frame limit: the one method
//! the Build facts (`rafka-node-admin-core` `fabric_builds`) and the membership snapshots share.

use bytes::Bytes;

/// iroh-gossip's frame limit (`DEFAULT_MAX_MESSAGE_SIZE`): a frame of this many bytes or more is
/// refused at write, which closes the connection, and with it every topic sharing that connection.
pub const GOSSIP_FRAME_LIMIT: usize = 4096;

/// The largest message payload. The frame is the payload plus the message envelope (two enum
/// tags, the 32-byte message id, the payload's length prefix, the delivery scope and round: about
/// 40 bytes); 64 bytes are kept for it.
pub const MAX_MESSAGE_BYTES: usize = GOSSIP_FRAME_LIMIT - 64;

/// `items` packed, in order, into runs whose encoding fits one message. `encode` renders a run
/// and refuses (`Err`) a run that does not fit. Each run is returned with the bytes that were
/// measured to fit, so a caller whose encoding varies (a fresh nonce) sends exactly those. An
/// item that fits no message on its own is left out and its refusal returned.
pub fn pack_in_order<T: Clone, E>(items: Vec<T>, encode: impl Fn(&[T]) -> Result<Bytes, E>) -> (Vec<(Vec<T>, Bytes)>, Vec<E>) {
    let (mut out, mut refused, mut batch) = (Vec::new(), Vec::new(), Vec::new());
    let mut fitted: Option<Bytes> = None;
    for it in items {
        batch.push(it);
        if let Ok(b) = encode(&batch) {
            fitted = Some(b);
            continue;
        }
        let last = batch.pop().expect("just pushed");
        if let Some(b) = fitted.take() {
            out.push((std::mem::take(&mut batch), b));
        }
        batch.clear();
        match encode(std::slice::from_ref(&last)) {
            Ok(b) => {
                batch.push(last);
                fitted = Some(b);
            }
            Err(e) => refused.push(e),
        }
    }
    if let Some(b) = fitted {
        out.push((batch, b));
    }
    (out, refused)
}
