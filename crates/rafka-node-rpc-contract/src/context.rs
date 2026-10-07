//! Caller identity and W3C context on the request envelope (node-rpc.md §11, §33;
//! i143.e6.s9).
//!
//! The envelope carries `caller_system`, `traceparent`, `tracestate` and `baggage` beside
//! the target fence. They are observability only: nothing here reaches target selection,
//! authority, the fence, lifecycle or a result. A value that is malformed or over its bound
//! is dropped, never refused and never truncated; the call proceeds and the local span
//! records what was dropped and why.

use serde::{Deserialize, Serialize};

/// `caller_system`: at most this many bytes of `[a-z0-9-]`.
pub const MAX_CALLER_SYSTEM_BYTES: usize = 32;
/// `tracestate`: at most this many bytes.
pub const MAX_TRACESTATE_BYTES: usize = 512;
/// `baggage`: at most this many bytes.
pub const MAX_BAGGAGE_BYTES: usize = 8192;

/// Baggage keys a span exposes as attributes; every other key propagates and is not recorded.
pub const BAGGAGE_ALLOWLIST: [&str; 3] = ["test_case", "scenario", "operation"];

/// The observability context one invocation carries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallContext {
    /// The originating system (`rdm`, `rafka`), kept across a carried hop.
    pub caller_system: Option<String>,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
    pub baggage: Option<String>,
}

/// Why a part of the context was dropped, in the words the span records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dropped {
    CallerSystem,
    /// An invalid `traceparent` takes `tracestate` with it.
    Traceparent,
    Tracestate,
    Baggage,
}

impl Dropped {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CallerSystem => "caller_system",
            Self::Traceparent => "traceparent",
            Self::Tracestate => "tracestate",
            Self::Baggage => "baggage",
        }
    }
}

impl CallContext {
    /// A `traceparent` that is well-formed: `00-<32 hex>-<16 hex>-<2 hex>`, trace and parent
    /// ids not all zero.
    pub fn traceparent_is_valid(tp: &str) -> bool {
        let parts: Vec<&str> = tp.split('-').collect();
        let [version, trace, parent, flags] = parts[..] else { return false };
        let hex = |s: &str, n: usize| s.len() == n && s.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase());
        hex(version, 2) && version != "ff" && hex(trace, 32) && hex(parent, 16) && hex(flags, 2) && trace.bytes().any(|b| b != b'0') && parent.bytes().any(|b| b != b'0')
    }

    fn caller_system_is_valid(s: &str) -> bool {
        !s.is_empty() && s.len() <= MAX_CALLER_SYSTEM_BYTES && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    }

    /// `tracestate` list members: `key=value`, comma separated, within the bound.
    fn tracestate_is_valid(s: &str) -> bool {
        s.len() <= MAX_TRACESTATE_BYTES && !s.is_empty() && s.split(',').all(|m| {
            let m = m.trim();
            matches!(m.split_once('='), Some((k, v)) if !k.is_empty() && !v.is_empty() && !k.contains(' '))
        })
    }

    /// Baggage list members: `key=value[;prop...]`, comma separated, within the bound.
    fn baggage_is_valid(s: &str) -> bool {
        s.len() <= MAX_BAGGAGE_BYTES && !s.is_empty() && s.split(',').all(|m| {
            let member = m.trim().split(';').next().unwrap_or_default();
            matches!(member.split_once('='), Some((k, _)) if !k.trim().is_empty() && !k.contains(' '))
        })
    }

    /// The context with every malformed or over-bound part dropped, and the names of what
    /// was dropped. Never refuses, never truncates.
    pub fn sanitized(mut self) -> (CallContext, Vec<Dropped>) {
        let mut dropped = Vec::new();
        if self.caller_system.as_deref().is_some_and(|s| !Self::caller_system_is_valid(s)) {
            self.caller_system = None;
            dropped.push(Dropped::CallerSystem);
        }
        if self.traceparent.as_deref().is_some_and(|s| !Self::traceparent_is_valid(s)) {
            self.traceparent = None;
            self.tracestate = None;
            dropped.push(Dropped::Traceparent);
        } else if self.tracestate.as_deref().is_some_and(|s| !Self::tracestate_is_valid(s)) {
            self.tracestate = None;
            dropped.push(Dropped::Tracestate);
        }
        if self.baggage.as_deref().is_some_and(|s| !Self::baggage_is_valid(s)) {
            self.baggage = None;
            dropped.push(Dropped::Baggage);
        }
        (self, dropped)
    }

    /// Every baggage member as `(key, value)`, properties removed. A context with no baggage
    /// has none.
    pub fn baggage_entries(&self) -> Vec<(String, String)> {
        self.baggage
            .as_deref()
            .map(|b| {
                b.split(',')
                    .filter_map(|m| {
                        let member = m.trim().split(';').next()?;
                        let (k, v) = member.split_once('=')?;
                        Some((k.trim().to_string(), v.trim().to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The baggage entries a span records: the allowlisted keys, in allowlist order.
    pub fn span_baggage(&self) -> Vec<(&'static str, String)> {
        let entries = self.baggage_entries();
        BAGGAGE_ALLOWLIST.iter().filter_map(|k| entries.iter().find(|(e, _)| e == k).map(|(_, v)| (*k, v.clone()))).collect()
    }
}

/// The dropped parts as one span value (`traceparent,baggage`), or empty when nothing was.
pub fn dropped_as_str(d: &[Dropped]) -> String {
    d.iter().map(|x| x.as_str()).collect::<Vec<_>>().join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    const TP: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    fn full() -> CallContext {
        CallContext {
            caller_system: Some("rdm".into()),
            traceparent: Some(TP.into()),
            tracestate: Some("rojo=00f067aa0ba902b7,congo=t61rcWkgMzE".into()),
            baggage: Some("test_case=a,scenario=b;p=1,operation=c,secret=x".into()),
        }
    }

    #[test]
    fn a_well_formed_context_is_kept_whole_and_round_trips() {
        let (c, dropped) = full().sanitized();
        assert_eq!(c, full());
        assert!(dropped.is_empty());
        let back: CallContext = postcard::from_bytes(&postcard::to_allocvec(&c).unwrap()).unwrap();
        assert_eq!(back, c);
        assert_eq!(CallContext::default().sanitized(), (CallContext::default(), vec![]), "no context is valid");
    }

    #[test]
    fn an_invalid_traceparent_drops_the_tracestate_with_it() {
        for bad in ["00-abc-def-01", "", "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "00-00000000000000000000000000000000-b7ad6b7169203331-01", "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01", "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01"] {
            let (c, dropped) = CallContext { traceparent: Some(bad.into()), ..full() }.sanitized();
            assert_eq!(dropped, vec![Dropped::Traceparent], "{bad:?}");
            assert_eq!((c.traceparent, c.tracestate), (None, None), "{bad:?}");
            assert_eq!(c.caller_system.as_deref(), Some("rdm"), "nothing else is touched");
        }
    }

    #[test]
    fn over_bound_or_malformed_parts_are_dropped_one_by_one_never_truncated() {
        let (c, dropped) = CallContext {
            caller_system: Some("Rafka Gateway".into()),
            tracestate: Some("x".repeat(MAX_TRACESTATE_BYTES + 1)),
            baggage: Some(format!("k={}", "v".repeat(MAX_BAGGAGE_BYTES))),
            ..full()
        }
        .sanitized();
        assert_eq!(dropped, vec![Dropped::CallerSystem, Dropped::Tracestate, Dropped::Baggage]);
        assert_eq!((c.caller_system, c.tracestate, c.baggage), (None, None, None));
        assert_eq!(c.traceparent.as_deref(), Some(TP), "a valid traceparent stays when only its tracestate is bad");
        assert_eq!(dropped_as_str(&dropped), "caller_system,tracestate,baggage");
        let (c, dropped) = CallContext { tracestate: Some("no-equals".into()), baggage: Some("=v".into()), ..full() }.sanitized();
        assert_eq!(dropped, vec![Dropped::Tracestate, Dropped::Baggage]);
        assert!(c.tracestate.is_none() && c.baggage.is_none());
        let exact = "k=".to_string() + &"v".repeat(MAX_BAGGAGE_BYTES - 2);
        assert!(CallContext { baggage: Some(exact), ..Default::default() }.sanitized().1.is_empty(), "the bound is inclusive");
    }

    #[test]
    fn baggage_propagates_whole_but_only_allowlisted_keys_reach_a_span() {
        let c = full();
        assert_eq!(c.baggage_entries(), vec![("test_case".to_string(), "a".to_string()), ("scenario".into(), "b".into()), ("operation".into(), "c".into()), ("secret".into(), "x".into())]);
        assert_eq!(c.span_baggage(), vec![("test_case", "a".to_string()), ("scenario", "b".to_string()), ("operation", "c".to_string())]);
        let only_secret = CallContext { baggage: Some("secret=x".into()), ..Default::default() };
        assert!(only_secret.span_baggage().is_empty());
        assert_eq!(only_secret.baggage_entries().len(), 1, "it still propagates");
    }
}
