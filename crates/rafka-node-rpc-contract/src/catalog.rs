//! The sealed protocol catalog and op ledger (ownership amendment §10–§11,
//! §14; node-rpc.md §9, §12; PRD §1.21).
//!
//! The ledger is the op-allocation authority: every op that is or was ever
//! used is recorded with its owner, and a retired op stays reserved forever.
//! A process composes RDM core protocols, product protocols and transitional
//! legacy adapters into one [`CatalogBuilder`] and seals it once, before the
//! first accepted stream is dispatched. The sealed catalog is the only
//! dispatch table: an op it does not hold is `421 UNSERVED_OP`, even when the
//! ledger reserves that op for a product family. There is no second switch
//! and no fallthrough to legacy code after seal.

use crate::protocol::NodeProtocol;
use std::collections::BTreeMap;
use std::fmt;

/// Who owns an op allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpOwner {
    /// RDM core (generic substrate).
    Core,
    /// A product family (e.g. Rafka), named.
    Product(String),
    /// RDM's proof testkit: served only by the testkit rpc node, never by a
    /// product binary, and only on an op in [`TESTKIT_OPS`].
    Testkit,
}

/// The tags reserved for RDM's proof testkit, permanently. No core or product
/// family is ever allocated one, and no testkit family lives outside them.
pub const TESTKIT_OPS: std::ops::RangeInclusive<u8> = 0x70..=0x7F;

/// Whether an allocated op is served or reserved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpState {
    /// The op is served.
    Live,
    /// Reserved forever; never served, never reassigned.
    Retired,
}

/// One op-ledger row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    /// The op tag.
    pub op: u8,
    /// The family the op belongs to.
    pub family: String,
    /// The product that owns the op.
    pub owner: OpOwner,
    /// Whether the op is live or retired.
    pub state: OpState,
}

/// RDM's ledger: core allocations plus the product reservations entering
/// migration (ownership amendment §10), so RDM never allocates them.
pub(crate) fn core_ledger() -> Vec<LedgerEntry> {
    let rafka = |op: u8, family: &str| LedgerEntry {
        op,
        family: family.into(),
        owner: OpOwner::Product("rafka".into()),
        state: OpState::Live,
    };
    vec![
        LedgerEntry { op: 0x01, family: "ping".into(), owner: OpOwner::Core, state: OpState::Live },
        rafka(0x10, "legacy-control"),
        LedgerEntry { op: 0x11, family: "echo".into(), owner: OpOwner::Core, state: OpState::Retired },
        rafka(0x12, "data-frame"),
        rafka(0x13, "snapshot"),
        rafka(0x14, "control"),
        rafka(0x15, "credential-resolve"),
        LedgerEntry {
            op: 0x16,
            family: "layout-reprovision".into(),
            owner: OpOwner::Product("rafka".into()),
            state: OpState::Retired,
        },
        rafka(0x17, "forward-write"),
        rafka(0x18, "forward-read"),
        rafka(0x19, "peer-tickle"),
        LedgerEntry { op: 0x1A, family: "forward".into(), owner: OpOwner::Core, state: OpState::Live },
        LedgerEntry { op: 0x1B, family: "status".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
        LedgerEntry { op: 0x1C, family: "build-claim".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
        LedgerEntry { op: 0x1D, family: "join".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
        LedgerEntry { op: 0x1E, family: "topology".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
        LedgerEntry { op: 0x1F, family: "build-facts".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
        LedgerEntry { op: 0x21, family: "fabric-primary-handover".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
        LedgerEntry { op: 0x70, family: "proof-store".into(), owner: OpOwner::Testkit, state: OpState::Live },
        LedgerEntry { op: 0x71, family: "resolve-probe".into(), owner: OpOwner::Testkit, state: OpState::Live },
        LedgerEntry { op: 0x72, family: "declare-probe".into(), owner: OpOwner::Testkit, state: OpState::Live },
        LedgerEntry { op: 0x73, family: "originate".into(), owner: OpOwner::Testkit, state: OpState::Live },
        LedgerEntry { op: 0x74, family: "hydrate-pull".into(), owner: OpOwner::Testkit, state: OpState::Live },
    ]
}

/// Whether a call has one reply frame or a stream of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// One request, one reply frame.
    Unary,
    /// One request, a stream of reply frames.
    ServerStreaming,
}

/// How a cataloged op is executed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryKind {
    /// A canonical Node RPC protocol.
    Canonical,
    /// A product's sealed adapter for a pre-Node-RPC family, carried only
    /// until `migration_unit` removes it. RDM runs no handler of its own.
    Transitional {
        /// The unit that removes the adapter.
        migration_unit: String,
    },
}

/// One served catalog entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    /// The op tag.
    pub op: u8,
    /// The protocol's name.
    pub name: String,
    /// The product that owns the op.
    pub owner: OpOwner,
    /// Whether the entry is canonical or a transitional adapter.
    pub kind: EntryKind,
    /// Whether the op is unary or streaming.
    pub shape: Shape,
    /// The largest request frame the op accepts, in bytes.
    pub max_request_frame_bytes: usize,
    /// The largest reply frame the op sends, in bytes.
    pub max_reply_frame_bytes: usize,
    /// Whether the op may be carried through a peer.
    pub forwardable: bool,
    /// Served while the node drains (lifecycle control), never refused `Draining`.
    pub served_while_draining: bool,
}

impl CatalogEntry {
    /// The entry for a canonical protocol `P`.
    pub fn canonical<P: NodeProtocol>(owner: OpOwner, shape: Shape) -> Self {
        Self {
            op: P::OP,
            name: P::NAME.into(),
            owner,
            kind: EntryKind::Canonical,
            shape,
            max_request_frame_bytes: P::MAX_REQUEST_FRAME_BYTES,
            max_reply_frame_bytes: P::MAX_REPLY_FRAME_BYTES,
            forwardable: P::FORWARDABLE,
            served_while_draining: P::SERVED_WHILE_DRAINING,
        }
    }

    /// A product's transitional legacy adapter.
    pub fn transitional(op: u8, name: &str, product: &str, migration_unit: &str, max_request_frame_bytes: usize) -> Self {
        Self {
            op,
            name: name.into(),
            owner: OpOwner::Product(product.into()),
            kind: EntryKind::Transitional { migration_unit: migration_unit.into() },
            shape: Shape::Unary,
            max_request_frame_bytes,
            max_reply_frame_bytes: max_request_frame_bytes,
            forwardable: false,
            served_while_draining: false,
        }
    }
}

/// Why a catalog refuses to seal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealError {
    /// Two entries serve the same op tag.
    DuplicateTag {
        /// The op tag concerned.
        op: u8,
        /// The first entry's name.
        first: String,
        /// The second entry's name.
        second: String,
    },
    /// The op tag is retired.
    RetiredTag {
        /// The op tag concerned.
        op: u8,
        /// The entry's name.
        name: String,
    },
    /// The op has no ledger row: allocate it in the ledger before serving it.
    UnledgeredTag {
        /// The op tag with no ledger row.
        op: u8,
        /// The protocol's catalog name.
        name: String,
    },
    /// The ledger names a different owner for this op.
    OwnerMismatch {
        /// The op tag.
        op: u8,
        /// The entry's name.
        name: String,
        /// The owner the ledger names.
        ledger: OpOwner,
        /// The owner the entry names.
        entry: OpOwner,
    },
    /// A transitional adapter must be a product family, never core.
    /// A transitional adapter names core as its owner.
    CoreTransitional {
        /// The op tag concerned.
        op: u8,
        /// The entry's name.
        name: String,
    },
    /// A server-streaming protocol may not be forwardable (node-rpc.md §36.3).
    /// A server-streaming protocol is declared forwardable.
    ForwardableStream {
        /// The op tag concerned.
        op: u8,
        /// The entry's name.
        name: String,
    },
    /// Two ledger rows for one op.
    /// The ledger holds two rows for the op.
    DuplicateLedgerRow {
        /// The op tag concerned.
        op: u8,
    },
    /// A protocol declares a zero request or reply frame ceiling.
    ZeroCeiling {
        /// The op tag concerned.
        op: u8,
        /// The entry's name.
        name: String,
    },
    /// A ledger row breaks the testkit range: a testkit family outside
    /// [`TESTKIT_OPS`], or a core/product family inside it.
    TestkitRange {
        /// The op tag.
        op: u8,
        /// The ledger row's family.
        family: String,
        /// The ledger row's owner.
        owner: OpOwner,
    },
    /// Op `0` is reserved as invalid: a zeroed fence is never served.
    /// An entry serves op 0.
    ReservedZero {
        /// The entry's name.
        name: String,
    },
}

impl fmt::Display for SealError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateTag { op, first, second } => write!(f, "op {op:#04x} registered twice ({first}, {second})"),
            Self::RetiredTag { op, name } => write!(f, "op {op:#04x} ({name}) is retired forever"),
            Self::UnledgeredTag { op, name } => write!(f, "op {op:#04x} ({name}) has no op-ledger row"),
            Self::OwnerMismatch { op, name, ledger, entry } => {
                write!(f, "op {op:#04x} ({name}) is owned by {ledger:?} in the ledger, registered by {entry:?}")
            }
            Self::CoreTransitional { op, name } => write!(f, "op {op:#04x} ({name}): core runs no transitional adapter"),
            Self::ForwardableStream { op, name } => write!(f, "op {op:#04x} ({name}): streaming families are not forwardable"),
            Self::DuplicateLedgerRow { op } => write!(f, "the op ledger has two rows for {op:#04x}"),
            Self::ZeroCeiling { op, name } => write!(f, "op {op:#04x} ({name}) declares a zero frame ceiling"),
            Self::TestkitRange { op, family, owner } => write!(
                f,
                "op {op:#04x} ({family}, {owner:?}) breaks the testkit range {:#04x}..={:#04x}: only testkit families live there, and only there",
                TESTKIT_OPS.start(),
                TESTKIT_OPS.end()
            ),
            Self::ReservedZero { name } => write!(f, "op 0 ({name}) is reserved as invalid: a zeroed fence is never served"),
        }
    }
}

/// The unsealed composition: core + product protocols + transitional adapters.
#[derive(Debug, Default)]
pub struct CatalogBuilder {
    ledger: Vec<LedgerEntry>,
    entries: Vec<CatalogEntry>,
}

impl CatalogBuilder {
    /// Start from RDM's core ledger.
    pub fn new() -> Self {
        Self { ledger: core_ledger(), entries: Vec::new() }
    }

    /// Add product ledger rows (new product allocations).
    pub fn ledger(mut self, rows: impl IntoIterator<Item = LedgerEntry>) -> Self {
        self.ledger.extend(rows);
        self
    }

    /// Add `entry` to the served set.
    pub fn serve(mut self, entry: CatalogEntry) -> Self {
        self.entries.push(entry);
        self
    }

    /// Validate and seal. Consumes the builder: nothing can be added after.
    pub fn seal(self) -> Result<SealedCatalog, Vec<SealError>> {
        let mut errors = Vec::new();
        let mut ledger: BTreeMap<u8, &LedgerEntry> = BTreeMap::new();
        for row in &self.ledger {
            if row.op == 0 {
                errors.push(SealError::ReservedZero { name: row.family.clone() });
            }
            if ledger.insert(row.op, row).is_some() {
                errors.push(SealError::DuplicateLedgerRow { op: row.op });
            }
            if TESTKIT_OPS.contains(&row.op) != (row.owner == OpOwner::Testkit) {
                errors.push(SealError::TestkitRange { op: row.op, family: row.family.clone(), owner: row.owner.clone() });
            }
        }
        let mut served: BTreeMap<u8, CatalogEntry> = BTreeMap::new();
        for e in self.entries {
            let named = |e: &CatalogEntry| (e.op, e.name.clone());
            if e.op == 0 {
                errors.push(SealError::ReservedZero { name: e.name.clone() });
                continue;
            }
            if let Some(first) = served.get(&e.op) {
                errors.push(SealError::DuplicateTag { op: e.op, first: first.name.clone(), second: e.name.clone() });
                continue;
            }
            match ledger.get(&e.op) {
                None => {
                    let (op, name) = named(&e);
                    errors.push(SealError::UnledgeredTag { op, name });
                }
                Some(row) if row.state == OpState::Retired => {
                    let (op, name) = named(&e);
                    errors.push(SealError::RetiredTag { op, name });
                }
                Some(row) if row.owner != e.owner => errors.push(SealError::OwnerMismatch {
                    op: e.op,
                    name: e.name.clone(),
                    ledger: row.owner.clone(),
                    entry: e.owner.clone(),
                }),
                Some(_) => {}
            }
            if matches!(e.kind, EntryKind::Transitional { .. }) && e.owner == OpOwner::Core {
                errors.push(SealError::CoreTransitional { op: e.op, name: e.name.clone() });
            }
            if e.forwardable && e.shape == Shape::ServerStreaming {
                errors.push(SealError::ForwardableStream { op: e.op, name: e.name.clone() });
            }
            if e.max_request_frame_bytes == 0 || e.max_reply_frame_bytes == 0 {
                errors.push(SealError::ZeroCeiling { op: e.op, name: e.name.clone() });
            }
            served.insert(e.op, e);
        }
        if errors.is_empty() {
            Ok(SealedCatalog { served })
        } else {
            Err(errors)
        }
    }
}

/// The one sealed dispatch table of a process. Immutable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedCatalog {
    served: BTreeMap<u8, CatalogEntry>,
}

impl SealedCatalog {
    /// The entry serving `op`; `None` means `421 UNSERVED_OP`, whatever the
    /// ledger reserves.
    pub fn lookup(&self, op: u8) -> Option<&CatalogEntry> {
        self.served.get(&op)
    }

    /// The request ceiling for framing (`framing::parse_request_head`).
    pub(crate) fn request_ceiling(&self, op: u8) -> Option<usize> {
        self.lookup(op).map(|e| e.max_request_frame_bytes)
    }

    /// Every served entry.
    pub fn entries(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.served.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ping::Ping;
    use crate::framing::{parse_request_head, RequestHead};

    fn echo() -> CatalogEntry {
        CatalogEntry::canonical::<Ping>(OpOwner::Core, Shape::Unary)
    }

    #[test]
    fn core_echo_seals_and_serves_only_what_it_registered() {
        let c = CatalogBuilder::new().serve(echo()).seal().unwrap();
        assert_eq!(c.lookup(0x01).unwrap().name, "ping");
        assert_eq!(c.request_ceiling(0x01), Some(64 * 1024));
        assert_eq!(c.lookup(0x11), None, "0x11 (echo) is retired");
        for op in [0x10u8, 0x12, 0x17, 0x19, 0x42] {
            assert_eq!(c.lookup(op), None, "ledger-reserved or free, an unregistered op is unserved: {op:#x}");
        }
    }

    #[test]
    fn a_duplicate_tag_refuses_the_seal() {
        let err = CatalogBuilder::new().serve(echo()).serve(echo()).seal().unwrap_err();
        assert_eq!(err, vec![SealError::DuplicateTag { op: 0x01, first: "ping".into(), second: "ping".into() }]);
    }

    #[test]
    fn a_retired_tag_refuses_the_seal() {
        let err = CatalogBuilder::new()
            .serve(CatalogEntry::transitional(0x16, "layout-reprovision", "rafka", "never", 1024))
            .seal()
            .unwrap_err();
        assert_eq!(err, vec![SealError::RetiredTag { op: 0x16, name: "layout-reprovision".into() }]);
    }

    #[test]
    fn an_unledgered_tag_or_a_wrong_owner_refuses_the_seal() {
        let err = CatalogBuilder::new()
            .serve(CatalogEntry::transitional(0x42, "mystery", "rafka", "u9", 1024))
            .serve(CatalogEntry::transitional(0x01, "ping-legacy", "rafka", "u1", 1024))
            .seal()
            .unwrap_err();
        assert_eq!(
            err,
            vec![
                SealError::UnledgeredTag { op: 0x42, name: "mystery".into() },
                SealError::OwnerMismatch {
                    op: 0x01,
                    name: "ping-legacy".into(),
                    ledger: OpOwner::Core,
                    entry: OpOwner::Product("rafka".into())
                },
            ]
        );
        // A product allocates a new op through the ledger first.
        let ok = CatalogBuilder::new()
            .ledger([LedgerEntry { op: 0x42, family: "mystery".into(), owner: OpOwner::Product("rafka".into()), state: OpState::Live }])
            .serve(CatalogEntry::transitional(0x42, "mystery", "rafka", "u9", 1024))
            .seal();
        assert!(ok.is_ok());
    }

    #[test]
    fn transitional_adapters_compose_beside_core_with_no_fallthrough() {
        let c = CatalogBuilder::new()
            .serve(echo())
            .serve(CatalogEntry::transitional(0x12, "data-frame", "rafka", "i142.U6", 8 * 1024 * 1024))
            .seal()
            .unwrap();
        assert!(matches!(c.lookup(0x12).unwrap().kind, EntryKind::Transitional { .. }));
        // 0x17 is reserved for rafka forward-write in the ledger but not registered:
        // the sealed catalog is the only table, so it is unserved — no legacy fallthrough.
        let unserved = crate::framing::encode_request(&crate::framing::RequestHeader::fence(crate::framing::Fence { target_node_id: "n1".into(), op: 0x17 }), &[]);
        assert_eq!(parse_request_head(&unserved, |t| c.request_ceiling(t)), RequestHead::Unserved { op: 0x17 });
        let header = crate::framing::RequestHeader::fence(crate::framing::Fence { target_node_id: "n1".into(), op: 0x12 });
        let head = crate::framing::encode_request(&header, &[0]);
        assert_eq!(
            parse_request_head(&head, |t| c.request_ceiling(t)),
            RequestHead::Ready { op: 0x12, header, payload_len: 1, head_len: head.len() - 1 }
        );
    }

    #[test]
    fn structural_rules_are_named() {
        let mut core_t = CatalogEntry::transitional(0x01, "ping", "rafka", "x", 10);
        core_t.owner = OpOwner::Core;
        let mut stream = echo();
        stream.shape = Shape::ServerStreaming;
        stream.forwardable = true;
        let mut zero = echo();
        zero.op = 0x13;
        zero.owner = OpOwner::Product("rafka".into());
        zero.max_request_frame_bytes = 0;
        let e1 = CatalogBuilder::new().serve(core_t).seal().unwrap_err();
        assert!(e1.contains(&SealError::CoreTransitional { op: 0x01, name: "ping".into() }), "{e1:?}");
        let e2 = CatalogBuilder::new().serve(stream).seal().unwrap_err();
        assert!(e2.contains(&SealError::ForwardableStream { op: 0x01, name: "ping".into() }), "{e2:?}");
        let e3 = CatalogBuilder::new().serve(zero).seal().unwrap_err();
        assert!(e3.contains(&SealError::ZeroCeiling { op: 0x13, name: "ping".into() }), "{e3:?}");
        let e4 = CatalogBuilder::new()
            .ledger([LedgerEntry { op: 0x11, family: "dup".into(), owner: OpOwner::Core, state: OpState::Live }])
            .seal()
            .unwrap_err();
        assert_eq!(e4, vec![SealError::DuplicateLedgerRow { op: 0x11 }]);
    }

    #[test]
    fn op_zero_is_reserved_and_never_sealed() {
        let e = CatalogBuilder::new()
            .ledger([LedgerEntry { op: 0, family: "zeroed".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live }])
            .serve(CatalogEntry::transitional(0, "zeroed", "rdm", "x", 16))
            .seal()
            .unwrap_err();
        assert!(e.contains(&SealError::ReservedZero { name: "zeroed".into() }), "{e:?}");
    }

    #[test]
    fn the_core_ledger_matches_the_ownership_amendment() {
        let l = core_ledger();
        let tags: Vec<u8> = l.iter().map(|r| r.op).collect();
        assert_eq!(tags, vec![0x01, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x21, 0x70, 0x71, 0x72, 0x73, 0x74]);
        assert_eq!(l.iter().find(|r| r.op == 0x11).map(|r| r.state), Some(OpState::Retired), "echo is retired forever; ping took op 1");
        assert!(l.iter().all(|r| r.op != 0), "op 0 is reserved as invalid");
        assert_eq!(l.iter().find(|r| r.op == 0x1B).map(|r| r.owner.clone()), Some(OpOwner::Product("rdm".into())), "status is RDM's control family, not core");
        let core: Vec<u8> = l.iter().filter(|r| r.owner == OpOwner::Core && r.state == OpState::Live).map(|r| r.op).collect();
        assert_eq!(core, vec![0x01, 0x1A], "the live core ops are exactly ping and Forward");
        let testkit: Vec<u8> = l.iter().filter(|r| r.owner == OpOwner::Testkit).map(|r| r.op).collect();
        assert_eq!(testkit, vec![0x70, 0x71, 0x72, 0x73, 0x74], "the testkit tags are the proof store, the resolve probe, the declare probe, the originate door and the hydrate pull");
    }

    #[test]
    fn the_testkit_range_holds_only_testkit_families_and_they_live_nowhere_else() {
        let product_inside = CatalogBuilder::new()
            .ledger([LedgerEntry { op: 0x7F, family: "sneaky".into(), owner: OpOwner::Product("rafka".into()), state: OpState::Live }])
            .seal()
            .unwrap_err();
        assert_eq!(
            product_inside,
            vec![SealError::TestkitRange { op: 0x7F, family: "sneaky".into(), owner: OpOwner::Product("rafka".into()) }]
        );
        let testkit_outside = CatalogBuilder::new()
            .ledger([LedgerEntry { op: 0x42, family: "stray".into(), owner: OpOwner::Testkit, state: OpState::Live }])
            .seal()
            .unwrap_err();
        assert_eq!(testkit_outside, vec![SealError::TestkitRange { op: 0x42, family: "stray".into(), owner: OpOwner::Testkit }]);
        let core_inside = CatalogBuilder::new()
            .ledger([LedgerEntry { op: 0x7F, family: "core-probe".into(), owner: OpOwner::Core, state: OpState::Live }])
            .seal()
            .unwrap_err();
        assert_eq!(core_inside, vec![SealError::TestkitRange { op: 0x7F, family: "core-probe".into(), owner: OpOwner::Core }]);
    }
}
