# subprocess-control — how-to

## Spawn a node from the UI

Open http://localhost:19090. Click `[+ Spawn broker]` (or any node type button). The spawned node appears in the dropdown within ~5s once its boot trace lands in Jaeger.

## Spawn a node from the CLI

```bash
rfa mesh node add --type broker
# → spawned:  broker-a1b2c3d4
#   pid:      12345
```

## Kill via the UI

In the dropdown, pick a UI-spawned node (prefixed in the list). Click `[Kill <name>]`. Subprocess terminates within 10s.

## Kill via the CLI

```bash
rfa mesh node remove broker-a1b2c3d4
# → killed:  broker-a1b2c3d4
#   reason:  graceful
```

## List ONLY UI-spawned subprocesses

```bash
curl -s http://localhost:19090/api/nodes/spawned
# → {"spawned":["broker-a1b2c3d4", "compute-7e8f9a0b"]}
```

(Future: `rfa mesh node ls --spawned` will wrap this — currently access via curl.)

## Verify subprocess actually died

```powershell
Get-Process -Id <pid> -ErrorAction SilentlyContinue
# returns nothing if killed; the rfa kill response always confirms via spawn-list polling first
```

## Verify spawn data dir cleaned up

```powershell
Test-Path "E:/tmp/rafka-ui-nodes/<node_name>"
# → False after kill
```
