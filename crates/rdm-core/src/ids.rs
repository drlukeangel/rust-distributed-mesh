//! Identities.
//!
//! Product identities ([`NodeId`], [`MeshId`], [`FabricId`]) are canonical
//! Crockford60: 60 random bits, 12 lowercase Crockford base32 characters
//! (alphabet [`CROCKFORD`]), the same bare form downstream Rafka mints
//! (`rafka_iam_types::identity::encode_crockford_60bit`). The value is random:
//! it carries no time, age, ordinal or topology. Display prefixes (`msh_`,
//! `fab_`, node-kind prefixes) are a presentation concern and never stored.
//!
//! The incarnation, Build and deployment identities are canonical Crockford60 too (R-BI5: one id
//! type, never hex). The transport identity ([`EndpointId`]) is iroh's own key, opaque, compared by
//! equality only, and keeps iroh's representation.

use serde::{Deserialize, Serialize};
use std::fmt;

/// The Crockford base32 alphabet, lowercase: no `i`, `l`, `o`, `u`.
pub const CROCKFORD: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Width of a canonical product id.
pub const CROCKFORD60_LEN: usize = 12;

/// The `id_format` evidence names for a canonical product id.
pub const ID_FORMAT: &str = "crockford60";

/// Encode the low 60 bits of `v` as 12 Crockford characters, the most
/// significant 5-bit group first (so lexical order is numeric order).
pub fn encode_crockford60(v: u64) -> String {
    let v = v & ((1u64 << 60) - 1);
    (0..CROCKFORD60_LEN).map(|i| CROCKFORD[((v >> (5 * (CROCKFORD60_LEN - 1 - i))) & 0x1f) as usize] as char).collect()
}

/// The 60-bit value of a canonical id; `None` for anything else.
pub fn decode_crockford60(s: &str) -> Option<u64> {
    if s.len() != CROCKFORD60_LEN {
        return None;
    }
    s.bytes().try_fold(0u64, |v, b| Some((v << 5) | CROCKFORD.iter().position(|c| *c == b)? as u64))
}

/// A canonical id for a fixture `label`, for tests: the same label always yields the same id.
/// Production ids are minted, never derived.
#[doc(hidden)]
pub fn fixture_crockford60(label: &str) -> String {
    let h = label.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3));
    encode_crockford60(h)
}

/// Mint a canonical id: 60 bits from the OS CSPRNG, nothing else feeds it.
pub(crate) fn mint_crockford60() -> String {
    encode_crockford60(rand::random::<u64>())
}

/// Why a value is not a canonical product id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdError {
    /// Not 12 characters.
    WrongWidth {
        /// The kind of id that was parsed.
        kind: &'static str,
        /// The value that was refused.
        value: String,
        /// The length it has.
        len: usize,
    },
    /// A character outside the lowercase Crockford alphabet.
    NonCanonical {
        /// The kind of id that was parsed.
        kind: &'static str,
        /// The value that was refused.
        value: String,
        /// The byte offset of the character.
        at: usize,
        /// The character outside the alphabet.
        found: char,
    },
}

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongWidth { kind, value, len } => {
                write!(f, "{kind} {value:?} is {len} characters; a canonical {kind} is {CROCKFORD60_LEN} lowercase Crockford characters")
            }
            Self::NonCanonical { kind, value, at, found } => write!(
                f,
                "{kind} {value:?} has {found:?} at {at}, outside the lowercase Crockford alphabet {}",
                std::str::from_utf8(CROCKFORD).unwrap_or_default()
            ),
        }
    }
}

impl std::error::Error for IdError {}

/// Validate `value` as a canonical product id of `kind`, refusing by name.
pub(crate) fn parse_crockford60(kind: &'static str, value: &str) -> Result<(), IdError> {
    if let Some((at, found)) = value.char_indices().find(|(_, c)| !c.is_ascii() || !CROCKFORD.contains(&(*c as u8))) {
        return Err(IdError::NonCanonical { kind, value: value.into(), at, found });
    }
    if value.len() != CROCKFORD60_LEN {
        return Err(IdError::WrongWidth { kind, value: value.into(), len: value.len() });
    }
    Ok(())
}

macro_rules! product_id {
    ($(#[$m:meta])* $name:ident, $kind:literal $(, $ord:ident)*) => {
        $(#[$m])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize $(, $ord)*)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            /// Mint a fresh canonical id.
            pub fn mint() -> Self {
                Self(mint_crockford60())
            }

            /// The canonical id `value`, refused by name when it is not one.
            pub fn parse(value: &str) -> Result<Self, IdError> {
                parse_crockford60($kind, value).map(|()| Self(value.to_string()))
            }

            /// The id as its bare string.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;
            fn try_from(value: String) -> Result<Self, IdError> {
                parse_crockford60($kind, &value).map(|()| Self(value))
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.0
            }
        }

        impl std::str::FromStr for $name {
            type Err = IdError;
            fn from_str(s: &str) -> Result<Self, IdError> {
                Self::parse(s)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

product_id!(
    /// Logical node identity: minted once, kept across restarts, never
    /// reused; a permanent replacement mints a new one. Ordered: lexical order
    /// of two canonical NodeIds is the order of their values (the election
    /// key, e4.s14).
    NodeId, "node id", PartialOrd, Ord
);
product_id!(
    /// A mesh's identity: recovery keeps it, an intentional replacement mints
    /// a new one. Equality only: never an election key, age or precedence.
    MeshId, "mesh id"
);
product_id!(
    /// The logical Fabric's identity, kept for the Fabric's lifetime.
    /// Equality only: never an election key, age or precedence.
    FabricId, "fabric id"
);
product_id!(
    /// One entry of a node's connections log (connections.md §1, node-identity-and-state.md §8: every
    /// event receives its own minted row id). The id of a connection's open entry names that
    /// connection to `node.connection.delete` (R-CR3). Equality only.
    ConnectionId, "connection id"
);

product_id!(
    /// One process birth of a logical node (`RuntimeIncarnationId`). Equality only: never ordered.
    IncarnationId, "incarnation id"
);
product_id!(
    /// A Build's identity (build.md §3): minted once when the Build is accepted, carried by every
    /// attempt, claim and receipt of it. Ordered only so a set of Builds has a stable iteration.
    BuildId, "build id", PartialOrd, Ord
);
product_id!(
    /// A storage change's durable identity (storage.md §2 "Changing it"): minted at the acceptance of
    /// its Build and recorded with it before its first command; every command of the change carries
    /// it, the `commit` record is keyed by it with the Build's id, and whoever resumes the change
    /// resumes it under the same one. Equality only.
    ChangeId, "change id"
);

/// The log and span rendering of a [`BuildId`] (`R-BI5a`): `bid_xxxx-xxxx-xxxx`. Never stored,
/// wired or parsed; [`BuildId`]'s own [`fmt::Display`] stays the bare 12 lowercase Crockford
/// characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildIdLog<'a>(&'a BuildId);

impl BuildId {
    /// Render this Build id as `bid_xxxx-xxxx-xxxx` for tracing span fields and log lines only
    /// (`R-BI5a`).
    pub fn log(&self) -> BuildIdLog<'_> {
        BuildIdLog(self)
    }
}

impl fmt::Display for BuildIdLog<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.0.as_str();
        write!(f, "bid_{}-{}-{}", &s[0..4], &s[4..8], &s[8..12])
    }
}

product_id!(
    /// One runtime a node-admin's provider realised for a node birth, local to that node-admin.
    /// Ordered only so a set of deployments has a stable iteration.
    DeploymentId, "deployment id", PartialOrd, Ord
);

/// The node's authenticated transport identity: its Iroh endpoint id (public key), under iroh's
/// own name, exactly what iroh issues. Never a product identity; never the Fabric's id; never
/// minted here, so it carries no mint and no parse.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EndpointId(pub String);

impl fmt::Display for EndpointId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Values Rafka's `encode_crockford_60bit` produced for these inputs
    /// (`tests/fixtures/crockford60-parity.json` holds the full set).
    #[test]
    fn the_encoding_is_rafkas_most_significant_group_first() {
        assert_eq!(encode_crockford60(0), "000000000000");
        assert_eq!(encode_crockford60(1), "000000000001");
        assert_eq!(encode_crockford60((1 << 60) - 1), "zzzzzzzzzzzz");
        assert_eq!(encode_crockford60(u64::MAX), "zzzzzzzzzzzz", "bits above 60 are masked");
    }

    #[test]
    fn every_product_id_mints_twelve_lowercase_crockford_characters() {
        for _ in 0..2000 {
            let minted = [
                NodeId::mint().to_string(),
                MeshId::mint().to_string(),
                FabricId::mint().to_string(),
                IncarnationId::mint().to_string(),
                BuildId::mint().to_string(),
                DeploymentId::mint().to_string(),
            ];
            for s in minted {
                assert_eq!(s.len(), 12, "{s}");
                assert!(s.bytes().all(|b| CROCKFORD.contains(&b)), "{s}");
                assert!(!s.contains(['i', 'l', 'o', 'u']), "{s}");
            }
        }
    }

    /// R-BI5 (Luke 2026-10-12, "we only have one id type, never a hex"): an incarnation id is minted
    /// as every other id is, 12 lowercase Crockford characters.
    #[test]
    fn an_incarnation_id_mints_twelve_lowercase_crockford_characters() {
        for _ in 0..2000 {
            let s = IncarnationId::mint().to_string();
            assert_eq!(s.len(), 12, "{s}");
            assert!(s.bytes().all(|b| CROCKFORD.contains(&b)), "{s}");
        }
    }

    /// R-BI5: a stored hex incarnation, Build (`bld-<24 hex>`) or deployment id is an old row; reading
    /// it is refused by name, the error naming the kind and the value.
    #[test]
    fn a_stored_hex_id_is_refused_by_name() {
        let hex32 = "0123456789abcdef0123456789abcdef";
        let bld = "bld-0123456789abcdef01234567";
        let e = serde_json::from_str::<IncarnationId>(&format!("\"{hex32}\"")).unwrap_err().to_string();
        assert!(e.contains("incarnation id") && e.contains(hex32), "{e}");
        let e = serde_json::from_str::<DeploymentId>(&format!("\"{hex32}\"")).unwrap_err().to_string();
        assert!(e.contains("deployment id") && e.contains(hex32), "{e}");
        let e = serde_json::from_str::<BuildId>(&format!("\"{bld}\"")).unwrap_err().to_string();
        assert!(e.contains("build id") && e.contains(bld), "{e}");
        assert!(matches!(BuildId::parse(bld), Err(IdError::NonCanonical { kind: "build id", found: 'l', .. })), "{:?}", BuildId::parse(bld));
        assert!(matches!(IncarnationId::parse(hex32), Err(IdError::WrongWidth { kind: "incarnation id", len: 32, .. })), "{:?}", IncarnationId::parse(hex32));
    }

    /// The transport identity is iroh's own key, whatever width iroh issues: it is never parsed as a product id.
    #[test]
    fn an_endpoint_id_is_whatever_iroh_issues() {
        let key = "ab".repeat(32);
        let id: EndpointId = serde_json::from_str(&format!("\"{key}\"")).unwrap();
        assert_eq!(id.to_string(), key);
    }

    #[test]
    fn a_mint_carries_sixty_random_bits() {
        // Every bit position of the 60 takes both values across mints.
        let (mut ones, mut zeros) = (0u64, 0u64);
        for _ in 0..512 {
            let v = decode_crockford60(NodeId::mint().as_str()).unwrap();
            ones |= v;
            zeros |= !v;
        }
        let all = (1u64 << 60) - 1;
        assert_eq!(ones & all, all, "every bit was set at least once");
        assert_eq!(zeros & all, all, "every bit was clear at least once");
        let a: std::collections::HashSet<String> = (0..10_000).map(|_| MeshId::mint().to_string()).collect();
        assert_eq!(a.len(), 10_000, "no repeats");
    }

    #[test]
    fn a_non_canonical_value_is_refused_by_name() {
        assert!(matches!(NodeId::parse("0123456789ab"), Ok(_)));
        assert_eq!(
            NodeId::parse("0123456789a"),
            Err(IdError::WrongWidth { kind: "node id", value: "0123456789a".into(), len: 11 })
        );
        assert!(matches!(MeshId::parse(&"a".repeat(32)), Err(IdError::WrongWidth { kind: "mesh id", len: 32, .. })));
        for (bad, found) in [("0123456789aB", 'B'), ("0123456789ai", 'i'), ("0123456789al", 'l'), ("0123456789ao", 'o'), ("0123456789au", 'u'), ("0123-456789a", '-')] {
            assert!(
                matches!(FabricId::parse(bad), Err(IdError::NonCanonical { kind: "fabric id", found: f, .. }) if f == found),
                "{bad}: {:?}",
                FabricId::parse(bad)
            );
        }
        let e = NodeId::parse("0123456789aB").unwrap_err().to_string();
        assert!(e.contains("node id") && e.contains("'B'"), "{e}");
    }

    #[test]
    fn deserializing_refuses_a_non_canonical_product_id() {
        let ok: NodeId = serde_json::from_str("\"0123456789ab\"").unwrap();
        assert_eq!(serde_json::to_string(&ok).unwrap(), "\"0123456789ab\"", "round trip, bare");
        let hex = format!("\"{}\"", "f".repeat(32));
        assert!(serde_json::from_str::<MeshId>(&hex).unwrap_err().to_string().contains("mesh id"), "a 32-char hex mesh id is refused");
    }

    /// Representation only (e4.s14 owns elections): lexical order of two
    /// canonical NodeIds is the order of their 60-bit values.
    #[test]
    fn lexical_order_of_node_ids_is_numeric_order() {
        let mut ids: Vec<NodeId> = (0..500).map(|_| NodeId::mint()).collect();
        ids.sort();
        let values: Vec<u64> = ids.iter().map(|i| decode_crockford60(i.as_str()).unwrap()).collect();
        assert!(values.windows(2).all(|w| w[0] <= w[1]));
    }
}
