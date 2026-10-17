# Q40 B — Forward typed causes: exact page replacement for Luke

Ruling: Luke, 2026-10-17, Q40 B. The code and this page replacement land together. Luke approved this exact text on 2026-10-17. `Code`’s standard definitions and the local cause-to-code accessors are additive prerequisites for the ruled cells; the shared ReplyKind view and other protocol classifications remain e5.1’s next landing.

File: `docs/architecture/node-rpc-envelope.md`, Forward section. Replace exactly:

Old:

````text
pub enum ForwardReply {
    Forwarded { inner: Vec<u8> },
    InnerNotSent { reason: String },
    InnerUnserved { op: u8 },
    InnerRejectedStale { target_node_id: NodeId },
    InnerIndeterminate { reason: String },
    NotForwardable { op: u8 },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
    CarrierEdgeLost { reason: String },
    NoInnerBudget { remaining_ms: u64, reserve_ms: u64 },
}
```
````

New:

````text
pub enum ForwardReply {
    Forwarded { inner: Vec<u8> },
    InnerNotSent { reason: String, cause: NotSentCause },
    InnerUnserved { op: u8 },
    InnerRejectedStale { target_node_id: NodeId },
    InnerIndeterminate { reason: String, cause: IndeterminateCause },
    NotForwardable { op: u8 },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
    CarrierEdgeLost { reason: String },
    NoInnerBudget { remaining_ms: u64, reserve_ms: u64 },
}

pub enum IndeterminateCause {
    ReceiverGone,
    ReplyDeadline,
    ReplyLost,
    Reset(u64),
    UnsupportedReplyVariant,
    CorruptReply,
    ProtocolViolation,
}

pub enum NotSentCause {
    Resolve(ResolveFailure),
    Connection,
    StreamBudget,
    Deadline,
    FrameNotSent,
    Carried,
    CarrierEdgeLost,
    CarrierNoBudget {
        remaining_ms: u64,
        reserve_ms: u64,
    },
    NotForwardable {
        op: u8,
    },
    NoActiveRoute,
    AdmissionBlocked,
}

pub enum ResolveFailure { Unknown, Gone, Unavailable }
```

The `cause` field is appended after `reason` in each of these two variants. It is filled from the inner call’s typed outcome, never from its explanation. The origin rebuilds the typed `NotSentReason` or `IndeterminateReason` from `cause` and keeps `reason` as explanation. A proven receiving birth’s exit is `UNAVAILABLE` with detail `receiver-gone`; an owner’s bound lapsing is `DEADLINE_EXCEEDED`, before or after commit; a lost reply or non-fault reset after commit is `UNKNOWN`. `INTERNAL_RPC_FAILURE` is `INTERNAL`. The carrier’s own `CarrierEdgeLost` and failed inner dial are `UNAVAILABLE`; `NotForwardable` is `FAILED_PRECONDITION`. These codes describe the typed result; they change neither dispatch certainty nor delivery obligations. This is the Q40 Forward-only wire change; the 14 reply positions stand, and no legacy decoder is kept.
````
