//! The sealed protocol catalog and tag ledger (ownership amendment §10–§11,
//! §14; node-rpc.md §9, §12; PRD §1.21).
//!
//! The ledger is the tag-allocation authority: every tag that is or was ever
//! used is recorded with its owner, and a retired tag stays reserved forever.
//! A process composes RDM core protocols, product protocols and transitional
//! legacy adapters into one [`CatalogBuilder`] and seals it once, before the
//! first accepted stream is dispatched. The sealed catalog is the only
//! dispatch table: a tag it does not hold is `421 UNSERVED_TAG`, even when the
//! ledger reserves that tag for a product family. There is no second switch
//! and no fallthrough to legacy code after seal.

use crate::protocol::NodeProtocol;
use std::collections::BTreeMap;
use std::fmt;

/// Who owns a tag allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagOwner {
    /// RDM core (generic substrate).
    Core,
    /// A product family (e.g. Rafka), named.
    Product(String),
    /// RDM's proof testkit: served only by the testkit rpc node, never by a
    /// product binary, and only on a tag in [`TESTKIT_TAGS`].
    Testkit,
}

/// The tags reserved for RDM's proof testkit, permanently. No core or product
/// family is ever allocated one, and no testkit family lives outside them.
pub const TESTKIT_TAGS: std::ops::RangeInclusive<u8> = 0x70..=0x7F;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagState {
    Live,
    /// Reserved forever; never served, never reassigned.
    Retired,
}

/// One tag-ledger row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    pub tag: u8,
    pub family: String,
    pub owner: TagOwner,
    pub state: TagState,
}

/// RDM's ledger: core allocations plus the product reservations entering
/// migration (ownership amendment §10), so RDM never allocates them.
pub fn core_ledger() -> Vec<LedgerEntry> {
    let rafka = |tag: u8, family: &str| LedgerEntry {
        tag,
        family: family.into(),
        owner: TagOwner::Product("rafka".into()),
        state: TagState::Live,
    };
    vec![
        rafka(0x10, "legacy-control"),
        LedgerEntry { tag: 0x11, family: "echo".into(), owner: TagOwner::Core, state: TagState::Live },
        rafka(0x12, "data-frame"),
        rafka(0x13, "snapshot"),
        rafka(0x14, "control"),
        rafka(0x15, "credential-resolve"),
        LedgerEntry {
            tag: 0x16,
            family: "layout-reprovision".into(),
            owner: TagOwner::Product("rafka".into()),
            state: TagState::Retired,
        },
        rafka(0x17, "forward-write"),
        rafka(0x18, "forward-read"),
        rafka(0x19, "peer-tickle"),
        LedgerEntry { tag: 0x1A, family: "forward".into(), owner: TagOwner::Core, state: TagState::Live },
        LedgerEntry { tag: 0x70, family: "proof-store".into(), owner: TagOwner::Testkit, state: TagState::Live },
        LedgerEntry { tag: 0x71, family: "resolve-probe".into(), owner: TagOwner::Testkit, state: TagState::Live },
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    Unary,
    ServerStreaming,
}

/// How a cataloged tag is executed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryKind {
    /// A canonical Node RPC protocol.
    Canonical,
    /// A product's sealed adapter for a pre-Node-RPC family, carried only
    /// until `migration_unit` removes it. RDM runs no handler of its own.
    Transitional { migration_unit: String },
}

/// One served catalog entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    pub tag: u8,
    pub name: String,
    pub owner: TagOwner,
    pub kind: EntryKind,
    pub shape: Shape,
    pub max_request_frame_bytes: usize,
    pub max_reply_frame_bytes: usize,
    pub forwardable: bool,
}

impl CatalogEntry {
    /// The entry for a canonical protocol `P`.
    pub fn canonical<P: NodeProtocol>(owner: TagOwner, shape: Shape) -> Self {
        Self {
            tag: P::TAG,
            name: P::NAME.into(),
            owner,
            kind: EntryKind::Canonical,
            shape,
            max_request_frame_bytes: P::MAX_REQUEST_FRAME_BYTES,
            max_reply_frame_bytes: P::MAX_REPLY_FRAME_BYTES,
            forwardable: P::FORWARDABLE,
        }
    }

    /// A product's transitional legacy adapter.
    pub fn transitional(tag: u8, name: &str, product: &str, migration_unit: &str, max_request_frame_bytes: usize) -> Self {
        Self {
            tag,
            name: name.into(),
            owner: TagOwner::Product(product.into()),
            kind: EntryKind::Transitional { migration_unit: migration_unit.into() },
            shape: Shape::Unary,
            max_request_frame_bytes,
            max_reply_frame_bytes: max_request_frame_bytes,
            forwardable: false,
        }
    }
}

/// Why a catalog refuses to seal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealError {
    DuplicateTag { tag: u8, first: String, second: String },
    RetiredTag { tag: u8, name: String },
    /// The tag has no ledger row: allocate it in the ledger before serving it.
    UnledgeredTag { tag: u8, name: String },
    /// The ledger names a different owner for this tag.
    OwnerMismatch { tag: u8, name: String, ledger: TagOwner, entry: TagOwner },
    /// A transitional adapter must be a product family, never core.
    CoreTransitional { tag: u8, name: String },
    /// A server-streaming protocol may not be forwardable (node-rpc.md §36.3).
    ForwardableStream { tag: u8, name: String },
    /// Two ledger rows for one tag.
    DuplicateLedgerRow { tag: u8 },
    ZeroCeiling { tag: u8, name: String },
    /// A ledger row breaks the testkit range: a testkit family outside
    /// [`TESTKIT_TAGS`], or a core/product family inside it.
    TestkitRange { tag: u8, family: String, owner: TagOwner },
}

impl fmt::Display for SealError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateTag { tag, first, second } => write!(f, "tag {tag:#04x} registered twice ({first}, {second})"),
            Self::RetiredTag { tag, name } => write!(f, "tag {tag:#04x} ({name}) is retired forever"),
            Self::UnledgeredTag { tag, name } => write!(f, "tag {tag:#04x} ({name}) has no tag-ledger row"),
            Self::OwnerMismatch { tag, name, ledger, entry } => {
                write!(f, "tag {tag:#04x} ({name}) is owned by {ledger:?} in the ledger, registered by {entry:?}")
            }
            Self::CoreTransitional { tag, name } => write!(f, "tag {tag:#04x} ({name}): core runs no transitional adapter"),
            Self::ForwardableStream { tag, name } => write!(f, "tag {tag:#04x} ({name}): streaming families are not forwardable"),
            Self::DuplicateLedgerRow { tag } => write!(f, "the tag ledger has two rows for {tag:#04x}"),
            Self::ZeroCeiling { tag, name } => write!(f, "tag {tag:#04x} ({name}) declares a zero frame ceiling"),
            Self::TestkitRange { tag, family, owner } => write!(
                f,
                "tag {tag:#04x} ({family}, {owner:?}) breaks the testkit range {:#04x}..={:#04x}: only testkit families live there, and only there",
                TESTKIT_TAGS.start(),
                TESTKIT_TAGS.end()
            ),
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

    pub fn serve(mut self, entry: CatalogEntry) -> Self {
        self.entries.push(entry);
        self
    }

    /// Validate and seal. Consumes the builder: nothing can be added after.
    pub fn seal(self) -> Result<SealedCatalog, Vec<SealError>> {
        let mut errors = Vec::new();
        let mut ledger: BTreeMap<u8, &LedgerEntry> = BTreeMap::new();
        for row in &self.ledger {
            if ledger.insert(row.tag, row).is_some() {
                errors.push(SealError::DuplicateLedgerRow { tag: row.tag });
            }
            if TESTKIT_TAGS.contains(&row.tag) != (row.owner == TagOwner::Testkit) {
                errors.push(SealError::TestkitRange { tag: row.tag, family: row.family.clone(), owner: row.owner.clone() });
            }
        }
        let mut served: BTreeMap<u8, CatalogEntry> = BTreeMap::new();
        for e in self.entries {
            let named = |e: &CatalogEntry| (e.tag, e.name.clone());
            if let Some(first) = served.get(&e.tag) {
                errors.push(SealError::DuplicateTag { tag: e.tag, first: first.name.clone(), second: e.name.clone() });
                continue;
            }
            match ledger.get(&e.tag) {
                None => {
                    let (tag, name) = named(&e);
                    errors.push(SealError::UnledgeredTag { tag, name });
                }
                Some(row) if row.state == TagState::Retired => {
                    let (tag, name) = named(&e);
                    errors.push(SealError::RetiredTag { tag, name });
                }
                Some(row) if row.owner != e.owner => errors.push(SealError::OwnerMismatch {
                    tag: e.tag,
                    name: e.name.clone(),
                    ledger: row.owner.clone(),
                    entry: e.owner.clone(),
                }),
                Some(_) => {}
            }
            if matches!(e.kind, EntryKind::Transitional { .. }) && e.owner == TagOwner::Core {
                errors.push(SealError::CoreTransitional { tag: e.tag, name: e.name.clone() });
            }
            if e.forwardable && e.shape == Shape::ServerStreaming {
                errors.push(SealError::ForwardableStream { tag: e.tag, name: e.name.clone() });
            }
            if e.max_request_frame_bytes == 0 || e.max_reply_frame_bytes == 0 {
                errors.push(SealError::ZeroCeiling { tag: e.tag, name: e.name.clone() });
            }
            served.insert(e.tag, e);
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
    /// The entry serving `tag`; `None` means `421 UNSERVED_TAG`, whatever the
    /// ledger reserves.
    pub fn lookup(&self, tag: u8) -> Option<&CatalogEntry> {
        self.served.get(&tag)
    }

    /// The request ceiling for framing (`framing::parse_request_head`).
    pub fn request_ceiling(&self, tag: u8) -> Option<usize> {
        self.lookup(tag).map(|e| e.max_request_frame_bytes)
    }

    pub fn entries(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.served.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::echo::Echo;
    use crate::framing::{parse_request_head, RequestHead};

    fn echo() -> CatalogEntry {
        CatalogEntry::canonical::<Echo>(TagOwner::Core, Shape::Unary)
    }

    #[test]
    fn core_echo_seals_and_serves_only_what_it_registered() {
        let c = CatalogBuilder::new().serve(echo()).seal().unwrap();
        assert_eq!(c.lookup(0x11).unwrap().name, "echo");
        assert_eq!(c.request_ceiling(0x11), Some(64 * 1024));
        for tag in [0x10u8, 0x12, 0x17, 0x19, 0x42] {
            assert_eq!(c.lookup(tag), None, "ledger-reserved or free, an unregistered tag is unserved: {tag:#x}");
        }
    }

    #[test]
    fn a_duplicate_tag_refuses_the_seal() {
        let err = CatalogBuilder::new().serve(echo()).serve(echo()).seal().unwrap_err();
        assert_eq!(err, vec![SealError::DuplicateTag { tag: 0x11, first: "echo".into(), second: "echo".into() }]);
    }

    #[test]
    fn a_retired_tag_refuses_the_seal() {
        let err = CatalogBuilder::new()
            .serve(CatalogEntry::transitional(0x16, "layout-reprovision", "rafka", "never", 1024))
            .seal()
            .unwrap_err();
        assert_eq!(err, vec![SealError::RetiredTag { tag: 0x16, name: "layout-reprovision".into() }]);
    }

    #[test]
    fn an_unledgered_tag_or_a_wrong_owner_refuses_the_seal() {
        let err = CatalogBuilder::new()
            .serve(CatalogEntry::transitional(0x42, "mystery", "rafka", "u9", 1024))
            .serve(CatalogEntry::transitional(0x11, "echo-legacy", "rafka", "u1", 1024))
            .seal()
            .unwrap_err();
        assert_eq!(
            err,
            vec![
                SealError::UnledgeredTag { tag: 0x42, name: "mystery".into() },
                SealError::OwnerMismatch {
                    tag: 0x11,
                    name: "echo-legacy".into(),
                    ledger: TagOwner::Core,
                    entry: TagOwner::Product("rafka".into())
                },
            ]
        );
        // A product allocates a new tag through the ledger first.
        let ok = CatalogBuilder::new()
            .ledger([LedgerEntry { tag: 0x42, family: "mystery".into(), owner: TagOwner::Product("rafka".into()), state: TagState::Live }])
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
        assert_eq!(parse_request_head(&[0x17, 1, 0], |t| c.request_ceiling(t)), RequestHead::Unserved { tag: 0x17 });
        let target = crate::framing::RequestTarget { node_id: "n1".into(), incarnation: "i1".into(), slot: "rpc".into(), freshness: "f".into() };
        let head = crate::framing::encode_request(0x12, &target, &[0]);
        assert_eq!(
            parse_request_head(&head, |t| c.request_ceiling(t)),
            RequestHead::Ready { tag: 0x12, target, payload_len: 1, head_len: head.len() - 1 }
        );
    }

    #[test]
    fn structural_rules_are_named() {
        let mut core_t = CatalogEntry::transitional(0x11, "echo", "rafka", "x", 10);
        core_t.owner = TagOwner::Core;
        let mut stream = echo();
        stream.shape = Shape::ServerStreaming;
        stream.forwardable = true;
        let mut zero = echo();
        zero.tag = 0x13;
        zero.owner = TagOwner::Product("rafka".into());
        zero.max_request_frame_bytes = 0;
        let e1 = CatalogBuilder::new().serve(core_t).seal().unwrap_err();
        assert!(e1.contains(&SealError::CoreTransitional { tag: 0x11, name: "echo".into() }), "{e1:?}");
        let e2 = CatalogBuilder::new().serve(stream).seal().unwrap_err();
        assert!(e2.contains(&SealError::ForwardableStream { tag: 0x11, name: "echo".into() }), "{e2:?}");
        let e3 = CatalogBuilder::new().serve(zero).seal().unwrap_err();
        assert!(e3.contains(&SealError::ZeroCeiling { tag: 0x13, name: "echo".into() }), "{e3:?}");
        let e4 = CatalogBuilder::new()
            .ledger([LedgerEntry { tag: 0x11, family: "dup".into(), owner: TagOwner::Core, state: TagState::Live }])
            .seal()
            .unwrap_err();
        assert_eq!(e4, vec![SealError::DuplicateLedgerRow { tag: 0x11 }]);
    }

    #[test]
    fn the_core_ledger_matches_the_ownership_amendment() {
        let l = core_ledger();
        let tags: Vec<u8> = l.iter().map(|r| r.tag).collect();
        assert_eq!(tags, vec![0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x70, 0x71]);
        let core: Vec<u8> = l.iter().filter(|r| r.owner == TagOwner::Core).map(|r| r.tag).collect();
        assert_eq!(core, vec![0x11, 0x1A], "the core tags are exactly Echo and Forward");
        let testkit: Vec<u8> = l.iter().filter(|r| r.owner == TagOwner::Testkit).map(|r| r.tag).collect();
        assert_eq!(testkit, vec![0x70, 0x71], "the testkit tags are the proof store and the resolve probe");
        assert_eq!(TESTKIT_TAGS, 0x70..=0x7F, "the testkit range is pinned");
        assert_eq!(l.iter().find(|r| r.tag == 0x16).unwrap().state, TagState::Retired);
    }

    #[test]
    fn the_testkit_range_holds_only_testkit_families_and_they_live_nowhere_else() {
        let product_inside = CatalogBuilder::new()
            .ledger([LedgerEntry { tag: 0x72, family: "sneaky".into(), owner: TagOwner::Product("rafka".into()), state: TagState::Live }])
            .seal()
            .unwrap_err();
        assert_eq!(
            product_inside,
            vec![SealError::TestkitRange { tag: 0x72, family: "sneaky".into(), owner: TagOwner::Product("rafka".into()) }]
        );
        let testkit_outside = CatalogBuilder::new()
            .ledger([LedgerEntry { tag: 0x42, family: "stray".into(), owner: TagOwner::Testkit, state: TagState::Live }])
            .seal()
            .unwrap_err();
        assert_eq!(testkit_outside, vec![SealError::TestkitRange { tag: 0x42, family: "stray".into(), owner: TagOwner::Testkit }]);
        let core_inside = CatalogBuilder::new()
            .ledger([LedgerEntry { tag: 0x7F, family: "core-probe".into(), owner: TagOwner::Core, state: TagState::Live }])
            .seal()
            .unwrap_err();
        assert_eq!(core_inside, vec![SealError::TestkitRange { tag: 0x7F, family: "core-probe".into(), owner: TagOwner::Core }]);
    }
}
