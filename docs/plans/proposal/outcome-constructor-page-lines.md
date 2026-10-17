# Q43 — e5.2 outcome constructors, runtime spans and failure receipts

Status: approved by Luke (Q43, 2026-10-17); page lines land with e5.2.

All replacements are in `docs/architecture/node-rpc-envelope.md`. The constructor library owns shared outcomes; runtime handlers call the library, enforced by a syntax-based check over every workspace runtime source. The code is selected from typed evidence, never explanation text.

The requested receipt field crosses serialization: Build's terminal and receipt chunks, the Build topic's failed step/attempt facts, and PrimaryUpdate when it carries a failed Build terminal. This is e5.2's receipt-field wire change under R6, alongside the otherwise byte-preserving constructor migration. Existing fields keep their positions; `code: Code` is appended, using the standard numeric value 0–16. The affected seals and hand byte goldens move together; no old wire decoder is retained. `spans.md` and the generated wire reference regenerate.

For an old on-disk JSON failed-state receipt with no code, replay keeps its existing FAILED_PRECONDITION classification (the old typed Failed state); it never guesses the original delivery cause from its reason. New receipts retain the code captured where the step failed. The existing old-journal replay control remains; new wire frames require the code. No new top-level outcome number is introduced.

## Runtime spans

Old:

````text
Every canonical call emits `rdm.node_rpc.request.update.via-call` on the caller and
`rdm.node_rpc.request.serve.via-direct` on the server. The caller records `target`, `op`,
`outcome`, `reason`, `reused`, `elapsed_ms`; the server records `protocol`, `op`, `peer`,
`caller_system`, `context_dropped`, `test_case`, `scenario`, `operation`.
With propagated context, the server shares the caller's TraceId and its ParentSpanId is the
call span's SpanId. Handler spans belong to the serving process. The invocation's own end — a stream's
`rdm.node_rpc.stream.serve.via-terminal` or refusal, a unary fault or panic — is a child of its serve span. Compatibility adapters emit
their own diagnostics as stated below; they do not acquire canonical spans merely by being
reserved in the ledger.

````

New:

````text
Every canonical call emits `rdm.node_rpc.request.update.via-call` on the caller and
`rdm.node_rpc.request.serve.via-direct` on the server. The caller records `target`, `op`,
`outcome`, `reason`, `reused`, `elapsed_ms`, `code`, `detail`; the server records `protocol`,
`op`, `peer`, `caller_system`, `context_dropped`, `test_case`, `scenario`, `operation`,
`code`, `detail`. `code` is the standard gRPC name from §25. `detail` names the typed result
or refusal, or the typed local delivery reason; explanation text stays in `reason` and never
selects a code. Both attributes come from the typed outcome. An owner's bound lapsing is
`DEADLINE_EXCEEDED`, including before commit; resolver Unknown is `NOT_FOUND`, while a proven
receiver-gone is `UNAVAILABLE`; a lost connection after commit is `UNKNOWN`, while HandlerFault
and a real PROTOCOL_VIOLATION are `INTERNAL`.
With propagated context, the server shares the caller's TraceId and its ParentSpanId is the
call span's SpanId. Handler spans belong to the serving process. The invocation's own end — a stream's
`rdm.node_rpc.stream.serve.via-terminal` or refusal, a unary fault or panic — is a child of its serve span.
Stream result frames and the invocation's end record their own `code` and `detail`; workflow step
spans retain the step's code and named detail, and a failed Build receipt carries that code beside
its step and explanation. Compatibility adapters emit their own diagnostics as stated below;
they do not acquire canonical spans merely by being reserved in the ledger.

````

## Build read receipt schema

Old:

````text
pub enum StepResult {
    Complete,
    Failed { reason: String },
}
````

New:

````text
pub enum StepResult {
    Complete,
    Failed { reason: String, code: Code },
}
````

## Build terminal schema

Old:

````text
    Failed { build_id: String, attempt: u32, operation: String, step: String, reason: String },
````

New:

````text
    Failed { build_id: String, attempt: u32, operation: String, step: String, reason: String, code: Code },
````

## PrimaryUpdate carries the Build terminal

Old:

````text
or `Failed { step, reason }`, the Build carrier's terminal (R-BI4), when it ends failed; `Incomplete` is never
used for a step that ended. The commander fails the step on that variant, never on the reason's text, and
claims the attempt at the claim door before it commands.
````

New:

````text
or `Failed { step, reason, code }`, the Build carrier's terminal (R-BI4), when it ends failed; `Incomplete` is never
used for a step that ended. The commander fails the step on that variant, retaining the code and never selecting it from the reason's text, and
claims the attempt at the claim door before it commands.
````

