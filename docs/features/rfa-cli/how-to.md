# rfa-cli — how-to

## Build

```bash
CARGO_TARGET_DIR=E:/cargo-target-v2 cargo build -p rfa
# binary: E:/cargo-target-v2/debug/rfa.exe
```

## Read-only commands

```bash
rfa mesh node list
# NODE                 TYPE
# ------------------------------
# broker               broker
# compute              compute
# gateway              gateway
# registry             registry

rfa mesh node describe gateway
# node_id: <hex>
# SPAN                                      OFFSET ms     DUR ms
# node.ready                                    0.000      0.641
# boot.endpoint_created                         0.110      0.069
# ...

rfa mesh topology show --format dot | dot -Tsvg -o topology.svg
# pipes graphviz to a rendered SVG

rfa mesh status
# table with per-node peer_count + last-heartbeat age
```

## Mutations

```bash
rfa mesh node add --type broker
# spawned:  broker-a1b2c3d4
# pid:      12345

rfa mesh node remove broker-a1b2c3d4
# killed:  broker-a1b2c3d4
# reason:  graceful

rfa mesh wait-converged --target 4 --timeout 30s
# converged: 4/4 nodes (3 polls)
```

## Chaos

```bash
rfa mesh chaos kill
# random target
# primitive: kill_node
# target:    broker-XXX
# detection: Passed { waited_ms: 0 }

rfa mesh chaos kill --target broker-a1b2c3d4
# explicit target

rfa mesh chaos restart
# kill + immediate re-spawn

rfa mesh chaos soak --duration 5m --interval 30s --seed 42
# soak start: duration=5m interval=30s seed=42
# soak end: events=10 passed=10 failed_timeout=0 failed_assertion=0
# report: E:/tmp/rafka-chaos-soak-42.json
```

## Override target API URL

```bash
rfa --api-url http://other-host:19090 mesh node list
```

## Output as JSON for scripting

```bash
rfa mesh node list --format json
```
