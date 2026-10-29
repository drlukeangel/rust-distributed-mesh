//! Packing in order into gossip messages that each fit iroh-gossip's frame limit: the one method
//! the Build facts (`rafka-node-admin-core` `fabric_builds`) and the topology snapshots share.

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
///
/// A run's encoding only grows with its length, so each run is the longest prefix of what is left
/// that fits: the whole remainder is tried first (most messages hold every item left), then the
/// run is found by doubling and halving, a few encodes a run instead of one per item.
pub fn pack_in_order<T: Clone, E>(items: Vec<T>, encode: impl Fn(&[T]) -> Result<Bytes, E>) -> (Vec<(Vec<T>, Bytes)>, Vec<E>) {
    let (mut out, mut refused) = (Vec::new(), Vec::new());
    let mut start = 0;
    while start < items.len() {
        let rest = &items[start..];
        if let Ok(b) = encode(rest) {
            out.push((rest.to_vec(), b));
            break;
        }
        let mut fit = match encode(&rest[..1]) {
            Ok(b) => (1, b),
            Err(e) => {
                refused.push(e);
                start += 1;
                continue;
            }
        };
        // `over` is a length known not to fit; the remainder's own length is the first.
        let mut over = rest.len();
        let mut n = 2;
        while n < over {
            match encode(&rest[..n]) {
                Ok(b) => {
                    fit = (n, b);
                    n *= 2;
                }
                Err(_) => over = n,
            }
        }
        while over - fit.0 > 1 {
            let mid = fit.0 + (over - fit.0) / 2;
            match encode(&rest[..mid]) {
                Ok(b) => fit = (mid, b),
                Err(_) => over = mid,
            }
        }
        out.push((rest[..fit.0].to_vec(), fit.1));
        start += fit.0;
    }
    (out, refused)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The packing that adds one item at a time, re-encoding the run after each: the runs
    /// `pack_in_order` must return.
    fn one_at_a_time<T: Clone, E>(items: Vec<T>, encode: impl Fn(&[T]) -> Result<Bytes, E>) -> (Vec<(Vec<T>, Bytes)>, Vec<E>) {
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

    /// Item sizes in bytes, a postcard-like length prefix and a limit: the encoding of a run, or
    /// its length when it does not fit.
    fn sized(limit: usize) -> impl Fn(&[usize]) -> Result<Bytes, usize> {
        move |run: &[usize]| {
            let prefix = if run.len() < 128 { 1 } else { 2 };
            let mut bytes = vec![run.len() as u8; prefix];
            for (k, n) in run.iter().enumerate() {
                bytes.extend(std::iter::repeat((k + n) as u8).take(*n));
            }
            if bytes.len() > limit { Err(bytes.len()) } else { Ok(Bytes::from(bytes)) }
        }
    }

    #[test]
    fn every_run_and_its_bytes_are_those_one_item_at_a_time_packing_returns() {
        let mut seed: u64 = 0x5eed;
        let mut next = |m: u64| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (seed >> 33) % m
        };
        for _ in 0..2000 {
            let limit = 16 + next(300) as usize;
            let items: Vec<usize> = (0..next(60)).map(|_| if next(10) == 0 { limit + next(20) as usize } else { 1 + next(limit as u64 / 3 + 1) as usize }).collect();
            let encode = sized(limit);
            assert_eq!(pack_in_order(items.clone(), &encode), one_at_a_time(items.clone(), &encode), "items {items:?} limit {limit}");
        }
    }

    #[test]
    fn a_remainder_that_fits_is_one_encode() {
        let calls = std::cell::Cell::new(0);
        let encode = |run: &[usize]| {
            calls.set(calls.get() + 1);
            sized(4096)(run)
        };
        let (runs, refused) = pack_in_order(vec![10; 40], encode);
        assert_eq!((runs.len(), refused.len(), calls.get()), (1, 0, 1));
    }
}
