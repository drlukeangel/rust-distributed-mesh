# node-base — how-to

## Add a new node type

1. New top-level directory: `<newrole>/`
2. `<newrole>/Cargo.toml`:
   ```toml
   [package]
   name = "rafka-<newrole>"
   [[bin]]
   name = "rafka-<newrole>"
   path = "src/main.rs"
   [dependencies]
   rafka-node-base = { path = "../crates/rafka-node-base" }
   tokio = { workspace = true }
   anyhow = { workspace = true }
   ```
3. `<newrole>/src/main.rs`:
   ```rust
   use anyhow::Result;
   use rafka_node_base::{NodeRuntime, Role};
   
   #[tokio::main]
   async fn main() -> Result<()> {
       NodeRuntime::new("<newrole>")
           .with_role(Role::<NewRole>)  // add a new variant to Role enum first
           .run()
           .await
   }
   ```
4. Add `"<newrole>"` to workspace `Cargo.toml` members.
5. Update CLAUDE.md Principle #10 node_type enum to include the new role.
6. Add D-NNN locking the new node type's purpose.

## Build all node binaries against the shared base

```bash
CARGO_TARGET_DIR=E:/cargo-target-v2 cargo build -p rafka-gateway -p rafka-broker -p rafka-compute -p rafka-registry
```

Any change to node-base recompiles all dependents — that's the entire point.

## Verify thin-shell discipline

```bash
wc -l gateway/src/main.rs broker/src/main.rs compute/src/main.rs registry/src/main.rs
```

Each should be in the ~10-line range UNTIL the binary grows role-specific application logic (sprint-09+ for gateway). If they're ballooning back to 500+ lines, substrate code leaked back in — refactor it into node-base.
