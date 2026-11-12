//! The built protocol catalog and op ledger (ownership amendment §10–§11,
//! §14; node-rpc.md §9, §12; PRD §1.21).
//!
//! The ledger is the registry of RDM's own opcodes, each with its owner, and a
//! reserved op stays reserved forever. RDM never records a product's opcodes:
//! a product serves its families with no row (Q444).
//! A process composes RDM core protocols, product protocols and transitional
//! legacy adapters into one [`CatalogBuilder`] and builds it once, before the
//! first accepted stream is dispatched. The built catalog is the only
//! dispatch table: an op it does not hold is `421 UNSERVED_OP`, whatever the
//! ledger holds for that op. There is no second switch and no fallthrough to
//! legacy code after it is built.

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
    /// RDM's test harness: its families serve only in RDM's own test
    /// processes, never in a product binary.
    Testkit,
}

/// Whether an allocated op is served or reserved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpState {
    /// The op is served.
    Live,
    /// Reserved forever; never served, never reassigned.
    Reserved,
}

/// One op-ledger row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    /// The opcode.
    pub op: u8,
    /// The family the op belongs to.
    pub family: String,
    /// The product that owns the op.
    pub owner: OpOwner,
    /// Whether the op is live or reserved.
    pub state: OpState,
}

/// RDM's ledger: RDM's own opcodes only. A product's opcodes are never rows here: a product
/// serves its families with no row (Q444).
pub(crate) fn core_ledger() -> Vec<LedgerEntry> {
    vec![
        LedgerEntry { op: 0x01, family: "ping".into(), owner: OpOwner::Core, state: OpState::Live },
        LedgerEntry { op: 0x11, family: "echo".into(), owner: OpOwner::Core, state: OpState::Reserved },
        LedgerEntry { op: 0x1A, family: "forward".into(), owner: OpOwner::Core, state: OpState::Live },
        LedgerEntry { op: 0x1B, family: "status".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
        LedgerEntry { op: 0x1C, family: "build-claim".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Reserved },
        LedgerEntry { op: 0x1D, family: "join".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
        LedgerEntry { op: 0x1E, family: "topology".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
        LedgerEntry { op: 0x1F, family: "build-facts".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Reserved },
        LedgerEntry { op: 0x20, family: "build".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
        LedgerEntry { op: 0x21, family: "node-primary-update".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
        LedgerEntry { op: 0x22, family: "node-command".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
        LedgerEntry { op: 0x23, family: "read".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live },
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
    /// A product's built adapter for a pre-Node-RPC family, carried only
    /// until `migration_unit` removes it. RDM runs no handler of its own.
    Transitional {
        /// The unit that removes the adapter.
        migration_unit: String,
    },
}

/// One served catalog entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    /// The opcode.
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
    /// Served while the node is connections-deleting (lifecycle control), never refused `ConnectionsDeleting`.
    pub served_while_connections_deleting: bool,
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
            served_while_connections_deleting: P::SERVED_WHILE_CONNECTIONS_DELETING,
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
            served_while_connections_deleting: false,
        }
    }
}

/// Why a catalog refuses to build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    /// Two entries serve the same opcode.
    DuplicateTag {
        /// The opcode concerned.
        op: u8,
        /// The first entry's name.
        first: String,
        /// The second entry's name.
        second: String,
    },
    /// The opcode is reserved.
    ReservedOpcode {
        /// The opcode concerned.
        op: u8,
        /// The entry's name.
        name: String,
    },
    /// The ledger names a different owner for this op: a product's family on an RDM opcode.
    OwnerMismatch {
        /// The opcode.
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
        /// The opcode concerned.
        op: u8,
        /// The entry's name.
        name: String,
    },
    /// A server-streaming protocol may not be forwardable (node-rpc.md §36.3).
    /// A server-streaming protocol is declared forwardable.
    ForwardableStream {
        /// The opcode concerned.
        op: u8,
        /// The entry's name.
        name: String,
    },
    /// Two ledger rows for one op.
    /// The ledger holds two rows for the op.
    DuplicateLedgerRow {
        /// The opcode concerned.
        op: u8,
    },
    /// A protocol declares a zero request or reply frame ceiling.
    ZeroCeiling {
        /// The opcode concerned.
        op: u8,
        /// The entry's name.
        name: String,
    },
    /// Op `0` is reserved as invalid: a zeroed target is never served.
    /// An entry serves op 0.
    ReservedZero {
        /// The entry's name.
        name: String,
    },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateTag { op, first, second } => write!(f, "op {op:#04x} registered twice ({first}, {second})"),
            Self::ReservedOpcode { op, name } => write!(f, "op {op:#04x} ({name}) is reserved forever"),
            Self::OwnerMismatch { op, name, ledger, entry } => {
                write!(f, "op {op:#04x} ({name}) is owned by {ledger:?} in the ledger, registered by {entry:?}")
            }
            Self::CoreTransitional { op, name } => write!(f, "op {op:#04x} ({name}): core runs no transitional adapter"),
            Self::ForwardableStream { op, name } => write!(f, "op {op:#04x} ({name}): streaming families are not forwardable"),
            Self::DuplicateLedgerRow { op } => write!(f, "the op ledger has two rows for {op:#04x}"),
            Self::ZeroCeiling { op, name } => write!(f, "op {op:#04x} ({name}) declares a zero frame ceiling"),
            Self::ReservedZero { name } => write!(f, "op 0 ({name}) is reserved as invalid: a zeroed target is never served"),
        }
    }
}

/// The composition before it is built: core + product protocols + transitional adapters.
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

    /// Optional: add a product's own reserved rows, so that a code the product has withdrawn
    /// is refused by name. A product's served families need no row.
    pub fn ledger(mut self, rows: impl IntoIterator<Item = LedgerEntry>) -> Self {
        self.ledger.extend(rows);
        self
    }

    /// Add `entry` to the served set.
    pub fn serve(mut self, entry: CatalogEntry) -> Self {
        self.entries.push(entry);
        self
    }

    /// Validate and build. Consumes the builder: nothing can be added after.
    pub fn build(self) -> Result<Catalog, Vec<CatalogError>> {
        let mut errors = Vec::new();
        let mut ledger: BTreeMap<u8, &LedgerEntry> = BTreeMap::new();
        for row in &self.ledger {
            if row.op == 0 {
                errors.push(CatalogError::ReservedZero { name: row.family.clone() });
            }
            if ledger.insert(row.op, row).is_some() {
                errors.push(CatalogError::DuplicateLedgerRow { op: row.op });
            }
        }
        let mut served: BTreeMap<u8, CatalogEntry> = BTreeMap::new();
        for e in self.entries {
            let named = |e: &CatalogEntry| (e.op, e.name.clone());
            if e.op == 0 {
                errors.push(CatalogError::ReservedZero { name: e.name.clone() });
                continue;
            }
            if let Some(first) = served.get(&e.op) {
                errors.push(CatalogError::DuplicateTag { op: e.op, first: first.name.clone(), second: e.name.clone() });
                continue;
            }
            match ledger.get(&e.op) {
                // A product's family: no row, served under its own owner (Q444).
                None => {}
                Some(row) if row.state == OpState::Reserved => {
                    let (op, name) = named(&e);
                    errors.push(CatalogError::ReservedOpcode { op, name });
                }
                Some(row) if row.owner != e.owner => errors.push(CatalogError::OwnerMismatch {
                    op: e.op,
                    name: e.name.clone(),
                    ledger: row.owner.clone(),
                    entry: e.owner.clone(),
                }),
                Some(_) => {}
            }
            if matches!(e.kind, EntryKind::Transitional { .. }) && e.owner == OpOwner::Core {
                errors.push(CatalogError::CoreTransitional { op: e.op, name: e.name.clone() });
            }
            if e.forwardable && e.shape == Shape::ServerStreaming {
                errors.push(CatalogError::ForwardableStream { op: e.op, name: e.name.clone() });
            }
            if e.max_request_frame_bytes == 0 || e.max_reply_frame_bytes == 0 {
                errors.push(CatalogError::ZeroCeiling { op: e.op, name: e.name.clone() });
            }
            served.insert(e.op, e);
        }
        if errors.is_empty() {
            Ok(Catalog { served })
        } else {
            Err(errors)
        }
    }
}

/// The one built dispatch table of a process. Immutable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    served: BTreeMap<u8, CatalogEntry>,
}

impl Catalog {
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
    fn core_echo_builds_and_serves_only_what_it_registered() {
        let c = CatalogBuilder::new().serve(echo()).build().unwrap();
        assert_eq!(c.lookup(0x01).unwrap().name, "ping");
        assert_eq!(c.request_ceiling(0x01), Some(64 * 1024));
        assert_eq!(c.lookup(0x11), None, "0x11 (echo) is reserved");
        for op in [0x10u8, 0x12, 0x17, 0x1B, 0x42] {
            assert_eq!(c.lookup(op), None, "with a ledger row or none, an unregistered op is unserved: {op:#x}");
        }
    }

    /// A product's family on `op`, owned by the product `demo`.
    fn product(op: u8, name: &str) -> CatalogEntry {
        CatalogEntry { op, name: name.into(), owner: OpOwner::Product("demo".into()), ..echo() }
    }

    #[test]
    fn a_product_family_with_no_ledger_row_builds_and_serves() {
        let mut testkit = product(0x70, "testkit-store");
        testkit.owner = OpOwner::Testkit;
        let c = CatalogBuilder::new()
            .serve(echo())
            .serve(product(0x52, "records"))
            .serve(product(0x24, "a-former-rafka-code"))
            .serve(CatalogEntry::transitional(0x12, "data-frame", "rafka", "i142 U6", 1024))
            .serve(testkit)
            .build()
            .unwrap();
        for (op, name, owner) in [
            (0x52, "records", OpOwner::Product("demo".into())),
            (0x24, "a-former-rafka-code", OpOwner::Product("demo".into())),
            (0x12, "data-frame", OpOwner::Product("rafka".into())),
            (0x70, "testkit-store", OpOwner::Testkit),
        ] {
            let held = c.lookup(op).unwrap_or_else(|| panic!("{op:#04x} is served with no ledger row"));
            assert_eq!((held.name.as_str(), &held.owner), (name, &owner), "served under its own owner: {op:#04x}");
        }
        let header = crate::framing::RequestHeader::target(crate::framing::Target { node_id: "n1".into(), op: 0x52 });
        let head = crate::framing::encode_request(&header, &[0]);
        assert_eq!(
            parse_request_head(&head, |t| c.request_ceiling(t)),
            RequestHead::Ready { op: 0x52, header, payload_len: 1, head_len: head.len() - 1 }
        );
    }

    #[test]
    fn a_duplicate_tag_refuses_the_build() {
        let err = CatalogBuilder::new().serve(echo()).serve(echo()).build().unwrap_err();
        assert_eq!(err, vec![CatalogError::DuplicateTag { op: 0x01, first: "ping".into(), second: "ping".into() }]);
        let two_products = CatalogBuilder::new().serve(product(0x52, "records")).serve(product(0x52, "orders")).build().unwrap_err();
        assert_eq!(two_products, vec![CatalogError::DuplicateTag { op: 0x52, first: "records".into(), second: "orders".into() }]);
    }

    #[test]
    fn a_tag_whose_row_is_not_live_refuses_the_build() {
        let rdm_reserved = CatalogBuilder::new().serve(product(0x1C, "build-claim")).build().unwrap_err();
        assert_eq!(rdm_reserved, vec![CatalogError::ReservedOpcode { op: 0x1C, name: "build-claim".into() }]);
        // A product's own reserved row (the optional `ledger` call): a code it withdrew is refused by name.
        let withdrawn = CatalogBuilder::new()
            .ledger([LedgerEntry { op: 0x60, family: "withdrawn".into(), owner: OpOwner::Product("demo".into()), state: OpState::Reserved }])
            .serve(product(0x60, "withdrawn"))
            .build()
            .unwrap_err();
        assert_eq!(withdrawn, vec![CatalogError::ReservedOpcode { op: 0x60, name: "withdrawn".into() }]);
    }

    #[test]
    fn a_product_family_on_an_rdm_opcode_refuses_the_build() {
        let err = CatalogBuilder::new()
            .serve(CatalogEntry::transitional(0x01, "ping-legacy", "rafka", "u1", 1024))
            .serve(product(0x1D, "join"))
            .build()
            .unwrap_err();
        assert_eq!(
            err,
            vec![
                CatalogError::OwnerMismatch {
                    op: 0x01,
                    name: "ping-legacy".into(),
                    ledger: OpOwner::Core,
                    entry: OpOwner::Product("rafka".into())
                },
                CatalogError::OwnerMismatch {
                    op: 0x1D,
                    name: "join".into(),
                    ledger: OpOwner::Product("rdm".into()),
                    entry: OpOwner::Product("demo".into())
                },
            ]
        );
    }

    #[test]
    fn transitional_adapters_compose_beside_core_with_no_fallthrough() {
        let c = CatalogBuilder::new()
            .serve(echo())
            .serve(CatalogEntry::transitional(0x12, "data-frame", "rafka", "i142.U6", 8 * 1024 * 1024))
            .build()
            .unwrap();
        assert!(matches!(c.lookup(0x12).unwrap().kind, EntryKind::Transitional { .. }));
        // 0x17 is not registered: the built catalog is the only table, so it is
        // unserved — no legacy fallthrough.
        let unserved = crate::framing::encode_request(&crate::framing::RequestHeader::target(crate::framing::Target { node_id: "n1".into(), op: 0x17 }), &[]);
        assert_eq!(parse_request_head(&unserved, |t| c.request_ceiling(t)), RequestHead::Unserved { op: 0x17 });
        let header = crate::framing::RequestHeader::target(crate::framing::Target { node_id: "n1".into(), op: 0x12 });
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
        let e1 = CatalogBuilder::new().serve(core_t).build().unwrap_err();
        assert!(e1.contains(&CatalogError::CoreTransitional { op: 0x01, name: "ping".into() }), "{e1:?}");
        let e2 = CatalogBuilder::new().serve(stream).build().unwrap_err();
        assert!(e2.contains(&CatalogError::ForwardableStream { op: 0x01, name: "ping".into() }), "{e2:?}");
        let e3 = CatalogBuilder::new().serve(zero).build().unwrap_err();
        assert!(e3.contains(&CatalogError::ZeroCeiling { op: 0x13, name: "ping".into() }), "{e3:?}");
        let e4 = CatalogBuilder::new()
            .ledger([LedgerEntry { op: 0x11, family: "dup".into(), owner: OpOwner::Core, state: OpState::Live }])
            .build()
            .unwrap_err();
        assert_eq!(e4, vec![CatalogError::DuplicateLedgerRow { op: 0x11 }]);
    }

    #[test]
    fn op_zero_is_reserved_and_never_built() {
        let e = CatalogBuilder::new()
            .ledger([LedgerEntry { op: 0, family: "zeroed".into(), owner: OpOwner::Product("rdm".into()), state: OpState::Live }])
            .serve(CatalogEntry::transitional(0, "zeroed", "rdm", "x", 16))
            .build()
            .unwrap_err();
        assert!(e.contains(&CatalogError::ReservedZero { name: "zeroed".into() }), "{e:?}");
    }

    #[test]
    fn the_core_ledger_holds_rdms_own_opcodes_only() {
        let l = core_ledger();
        let codes: Vec<u8> = l.iter().map(|r| r.op).collect();
        assert_eq!(codes, vec![0x01, 0x11, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20, 0x21, 0x22, 0x23]);
        assert_eq!(l.iter().find(|r| r.op == 0x11).map(|r| r.state), Some(OpState::Reserved), "echo is reserved forever; ping took op 1");
        assert!(l.iter().all(|r| r.op != 0), "op 0 is reserved as invalid");
        assert_eq!(l.iter().find(|r| r.op == 0x1B).map(|r| r.owner.clone()), Some(OpOwner::Product("rdm".into())), "status is RDM's control family, not core");
        let core: Vec<u8> = l.iter().filter(|r| r.owner == OpOwner::Core && r.state == OpState::Live).map(|r| r.op).collect();
        assert_eq!(core, vec![0x01, 0x1A], "the live core ops are exactly ping and Forward");
        assert!(
            l.iter().all(|r| r.owner == OpOwner::Core || r.owner == OpOwner::Product("rdm".into())),
            "every row is RDM's: core or the mesh product, never another product's or the testkit's"
        );
        let reserved: Vec<u8> = l.iter().filter(|r| r.state == OpState::Reserved).map(|r| r.op).collect();
        assert_eq!(reserved, vec![0x11, 0x1C, 0x1F], "echo, build-claim and build-facts are reserved forever");
    }
}
