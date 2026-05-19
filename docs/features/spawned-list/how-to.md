# spawned-list — how-to

## List UI-spawned subprocesses

```bash
curl -s http://localhost:19090/api/nodes/spawned
# → {"spawned":["broker-a1b2c3d4","compute-7e8f9a0b","gateway-XXXX","registry-YYYY"]}
```

## Spawn → list → kill round-trip

```bash
rfa mesh node add --type broker
# → spawned: broker-XXXX

curl -s http://localhost:19090/api/nodes/spawned
# → {"spawned":["broker-XXXX", ...prior]}

rfa mesh node remove broker-XXXX

curl -s http://localhost:19090/api/nodes/spawned
# → {"spawned":[...without broker-XXXX]}
```

## In chaos primitive code

```rust
async fn pick_random_spawned(ctx: &ChaosContext) -> Result<String, ChaosError> {
    let body: Value = ctx.http.get(&format!("{}/api/nodes/spawned", ctx.topology_ui_url))
        .send().await?.json().await?;
    let names: Vec<String> = body["spawned"].as_array().map(|a| ...).unwrap_or_default();
    let mut rng = ctx.rng.lock().await;
    Ok(names.choose(&mut *rng).cloned()...)
}
```

(Already implemented in `crates/rafka-chaos/src/primitives.rs::pick_random_spawned`.)
