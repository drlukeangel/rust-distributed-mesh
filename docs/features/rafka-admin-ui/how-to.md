# rafka-v2-mesh-ui — How-to

## Start the UI

> Default port is 19106. 19105 has a known zombie-socket issue on this
> machine; if `Error: Only one usage of each socket address` appears, pick
> the next port up.

```powershell
$env:RAFKA_TOPOLOGY_UI_BIND_ADDR = "127.0.0.1:19106"
$env:JAEGER_QUERY_URL            = "http://localhost:16686"
$env:RAFKA_UI_STATIC_DIR         = "E:\dev\rafka-V2-new-mesh\topology-ui\web\dist"
$env:CARGO_TARGET_DIR            = "E:\cargo-target-v2"

Start-Process -FilePath "E:\cargo-target-v2\debug\rafka-topology-ui.exe" `
  -WindowStyle Hidden -PassThru `
  -RedirectStandardOutput "E:\tmp\topo-react-out.log" `
  -RedirectStandardError  "E:\tmp\topo-react-err.log"
```

Open <http://localhost:19106>. Hard-refresh (Ctrl+Shift+R) if the asset URLs
look stale.

## Bootstrap the demo

Click **bootstrap 2-mesh** in the top bar (or `curl -XPOST
http://localhost:19106/api/bootstrap`). Eighteen children spawn:
- 8 in mesh-a (2 of each type)
- 8 in mesh-b (2 of each type)
- 2 bridges

Topology tab will show two distinct mesh circles with bridges floating in
between within ~3 s.

**Bootstrap is ADDITIVE.** Calling it twice yields 36 nodes, not 18.
If you want a fresh slate, restart the UI process (kills its spawned_meta)
or kill the existing pool via the Heartbeat tab buttons.

## Add / remove nodes manually

1. **Pick a mesh** in the dropdown (mesh-a, mesh-b, or `+ new mesh…` for an
   arbitrary one).
2. **Click + gateway / + broker / etc.** Spawns a single node in that mesh.
3. **To remove**: switch to the Heartbeat tab → click the red `kill` button
   on the card for the node.

CLI equivalents (either body shape works):
```
# Preferred (first-class field):
curl -XPOST -H 'Content-Type: application/json' \
  -d '{"node_type":"broker","mesh_id":"mesh-a"}' \
  http://localhost:19106/api/nodes/spawn

# Equivalent (env-nested form):
curl -XPOST -H 'Content-Type: application/json' \
  -d '{"node_type":"broker","extra_env":{"RAFKA_MESH_ID":"mesh-a"}}' \
  http://localhost:19106/api/nodes/spawn

# mesh_id must match ^[a-z0-9][a-z0-9-]{0,63}$ — slashes, spaces, unicode
# return 400. Invalid node_type also returns 400.

curl -XDELETE http://localhost:19106/api/nodes/broker-abc12345
```

## Start / stop continuous chaos

Top bar:
- **start chaos** → POST /api/chaos/start (cadence 30 s; kills + respawns
  one random non-bridge node)
- **stop chaos** → POST /api/chaos/stop (current iteration finishes; no more
  are scheduled)

Chaos tab shows live state: running flag, cadence, total events, time since
last event.

## Run a test

UI:
- Tests tab → find the test card → click **run**
- Status pill turns blue (`running`) → green (`passed`) or red (`failed`)
- Click **run all** in the header to run every test sequentially (~6 min
  total)

CLI:
```
E:\cargo-target-v2\debug\rfa.exe mesh test list
E:\cargo-target-v2\debug\rfa.exe mesh test run backpressure-stream-flood --seed 42
E:\cargo-target-v2\debug\rfa.exe mesh test all --seed 42
```

Reports live under `E:\tmp\rafka-tests\<name>-<seed>.json`. The UI polls this
directory every 3 s; CLI-driven runs appear automatically.

## Read the boot trace for a specific node

1. Boot Waterfall tab
2. Pick the node from the dropdown — entries are prefixed `<mesh> : <name>`
3. Bars render proportional to span duration; tooltip shows ms.

If the dropdown is populated but the chart is empty, the message **waiting on
Jaeger ingestion for <name>…** is shown. Spans typically land within 5 s of
spawn; longer = check Jaeger is up.

## Look at recent events

Timeline tab shows the merged stream of:
- Local events (instant): `node.spawn`, `node.killed`, `chaos.kill`,
  `chaos.respawn`, `test.start`, `test.end`
- Jaeger events (5-30 s lag): `node.ready`, `peer.connected`,
  `peer.disconnected`

Each row has timestamp, kind pill, node_name (mesh), and optional detail.

## Read alerts

Alerts tab shows non-passing chaos primitive detections from the last 10 min.
Empty = system healthy.

## CLI cheat-sheet

```
rfa.exe mesh test list
rfa.exe mesh test run <name> --seed <n>
rfa.exe mesh test all --seed <n>
rfa.exe --api-url http://127.0.0.1:19106 mesh test ...
```

`--api-url` defaults to `http://127.0.0.1:19106`. Tests that need a live UI
(everything in the `chaos` kind except `backpressure-stream-flood`) will
target it.
