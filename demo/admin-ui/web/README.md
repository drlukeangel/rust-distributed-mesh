# `rdm-admin-ui` web interface

`demo/admin-ui/web` is the React and TypeScript browser interface served by the `rdm-admin-ui`
binary (`demo/admin-ui`).

RDM's own crates expose only [Node RPC](../../../docs/architecture/node-rpc.md); no RDM crate serves
HTTP. `rdm-admin-ui` is the demo application's HTTP control client: it attaches to a `node-admin`
over `rdm-client` (`RDM_NODE_ADMIN_BIRTH`) and serves this single-page app alongside `/api/*` routes
for:

- reading the fabric, meshes, nodes, connections, and Builds (`/api/overview`, `/api/topology`,
  `/api/nodes`, `/api/builds`, `/api/timeline`),
- submitting node, mesh, and fabric lifecycle operations and Builds (`/api/nodes/:name/stop`,
  `/api/nodes/:name/start`, `/api/nodes/:name/replace`, `/api/nodes/:name/delete`, `/api/nodes`,
  `/api/meshes`, `/api/fabric/bootstrap`), and
- driving process and network fault injection on the demo estate (`/api/chaos/*`).

## Building the static bundle

From the repository root:

```bash
npm --prefix demo/admin-ui/web ci
npm --prefix demo/admin-ui/web run build
```

This compiles the TypeScript sources and writes the production bundle to `demo/admin-ui/web/dist`.

## Running with the demo estate

`scripts/demo-estate.sh up` builds (if needed) and starts `rdm-admin-ui` attached to the Day-0
`node-admin`, pointing `RDM_UI_STATIC_DIR` at `demo/admin-ui/web/dist`, and prints the bound URL
(also written to `target/demo-estate-current/ui.url`):

```bash
RDM_DEMO_BIN=$PWD/demo/target/debug scripts/demo-estate.sh up
```

## Running `rdm-admin-ui` standalone

To attach `rdm-admin-ui` to any running `node-admin` whose stdout printed `RDM_NODE_BIRTH=rdm://...`:

```bash
RDM_NODE_ADMIN_BIRTH="rdm://<birth-token>" \
RDM_UI_STATIC_DIR="$PWD/demo/admin-ui/web/dist" \
RDM_ADMIN_UI_BIND_ADDR="127.0.0.1:8080" \
cargo run --locked --manifest-path demo/Cargo.toml -p rdm-admin-ui --bin rdm-admin-ui
```

For local frontend development with Vite hot reload:

```bash
npm --prefix demo/admin-ui/web run dev
```
