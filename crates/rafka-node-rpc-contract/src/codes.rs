//! The reserved Node RPC reset/stop code namespace (ownership amendment §12).

/// A code Node RPC itself emits on a stream reset or stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ResetCode {
    /// No catalog entry serves the tag; proves no protocol dispatch.
    UnservedTag = 421,
    /// The server stops the request body after a typed early refusal;
    /// flow-control hygiene, never result certainty.
    RequestStop = 422,
    /// A supervised unexpected runtime/handler fault after dispatch.
    InternalRpcFailure = 423,
    /// A framing/order/runtime contract violation.
    ProtocolViolation = 424,
    /// The request's fence names another node than the receiver, or a port it
    /// does not serve; proves no protocol dispatch.
    StaleTarget = 425,
    /// The sender reset an unfinished request; proves pre-dispatch `NotSent`.
    FrameNotSent = 499,
}

/// Historical pre-Node-RPC codes: reserved forever, never emitted, never reassigned.
pub const RETIRED_CODES: [u32; 3] = [413, 501, 503];

impl ResetCode {
    pub const ALL: [ResetCode; 6] =
        [Self::UnservedTag, Self::RequestStop, Self::InternalRpcFailure, Self::ProtocolViolation, Self::StaleTarget, Self::FrameNotSent];

    pub fn code(self) -> u32 {
        self as u32
    }

    /// The code's canonical name.
    pub fn name(self) -> &'static str {
        match self {
            Self::UnservedTag => "UNSERVED_TAG",
            Self::RequestStop => "REQUEST_STOP",
            Self::InternalRpcFailure => "INTERNAL_RPC_FAILURE",
            Self::ProtocolViolation => "PROTOCOL_VIOLATION",
            Self::StaleTarget => "STALE_TARGET",
            Self::FrameNotSent => "FRAME_NOT_SENT",
        }
    }

    /// The code a reset/stop carried, if it is one of ours. A retired code is
    /// recognised by number but is not a current `ResetCode`.
    pub fn from_code(code: u64) -> Option<Self> {
        Self::ALL.into_iter().find(|c| u64::from(c.code()) == code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_is_421_to_425_plus_499() {
        let codes: Vec<u32> = ResetCode::ALL.iter().map(|c| c.code()).collect();
        assert_eq!(codes, vec![421, 422, 423, 424, 425, 499]);
        assert_eq!(ResetCode::StaleTarget.name(), "STALE_TARGET", "425 is pinned to STALE_TARGET");
        for c in ResetCode::ALL {
            assert_eq!(ResetCode::from_code(c.code().into()), Some(c));
        }
    }

    #[test]
    fn retired_codes_are_never_current() {
        for r in RETIRED_CODES {
            assert_eq!(ResetCode::from_code(r.into()), None);
            assert!(!ResetCode::ALL.iter().any(|c| c.code() == r));
        }
    }
}
