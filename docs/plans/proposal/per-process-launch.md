## R-D3: what a node-admin hands each process it launches

Proposal for i143 R-D3 (`docs/plans/i143-prd.md:2938`, assigned to the lead 2026-10-24). **Ruled
2026-10-24; being built.** Q150 (R-D3.1) and Q151 (a product kind's listeners) are ruled A by the
architect under delegation, A1 concurring, and Luke approved the one page sentence; R-D3.2 and R-D3.3 are
A, no code, accepted as read from the pages. The rulings are in `docs/plans/rulings/INDEX.md` (Q150,
Q151). The decided contract is §0; §1-§8 are the analysis the rulings were made on, read at RDM
`7811eb4a3` and rafka-v2 `f52217511f`, kept as the record. Revised 2026-10-24 on the architect's
adversarial review of `708afd101` and Architect 1's recommendations on it: §0 gains the value's life and
its file beside the data dir, the diagnostics scrub, the container boundary, the listener address check,
the refusal of a duplicate declaration, and the env table rows; §7 gains D7. RDM `file:line` is bare; rafka-v2
`file:line` is prefixed `rafka-v2`.

RDM carries and enforces the contract. Rafka's variable names (`RAFKA_SYSTEM_KEY`,
`RAFKA_SIGNING_KEY_HEX`, `RAFKA_ALLOW_INSECURE_DEV_KEY`) and the kinds that receive them stay Rafka's.

---

### 0. Decided: what is built

#### 0.1 Declared secrets (Q150: A with three amendments)

- **The declaration.** Rafka's wiring declares each per-process variable by name with the node kinds it
  is set on, registered at boot like the hook registry: never config, never a row. Rafka declares
  `RAFKA_SYSTEM_KEY` on compute, gateway and broker, and `RAFKA_SIGNING_KEY_HEX` on gateway.
- **The value.** Read once from the bootstrap admin's own environment at its start, as the fabric root is.
- **Custody.** Every node-admin RDM launches is written every declared value beside the root, in one
  launch variable (`RDM_DECLARED_SECRETS`: a JSON document, `{"v":1,"declared":[{"name","kinds","value"}]}`,
  so a value holding a separator, `=`, a newline or nothing at all round-trips; a document that does not
  parse, names an unknown version or repeats a name is refused by name without echoing any of it), so
  whichever admin runs a create can launch the named kinds. A node-admin's process carries the app's variable itself only when
  `node_admin` is a named kind. The landing adds the variable's row to CLAUDE.md's env table, with the
  rows for `RDM_TRANSPORT_ADDR` and `RDM_LISTENERS` the table lacks (D1).
- **Fixed for the fabric's life.** The value is captured once, at the bootstrap. A replace and a roll
  launch from the launching admin's custody copy, so the value never changes while the fabric runs;
  changing it is a new fabric, and a live rotation is a separate proposal, not R-D3's. Every admin keeps
  the declared values beside its data dir as it keeps the root (`ca-secret`, `certs.rs:37`), read back
  on a start from disk without a launcher; that file and the launch environment are the two places a
  value lives, and the approved §2.3 sentence names records and projections, neither of which they are.
- **A duplicate declaration is refused at registration**, naming the variable and never its value: two
  declarations of one name are not merged and registration order chooses nothing.
- **Use.** The value is set in the process environment of the named kinds only.
- **Redaction.** Held as `SecretHex` (its debug form names its length, never its value) until the spawn;
  never written to a row, a Build record, a receipt, the launch record or a span. RDM's own diagnostics
  are scrubbed before they leave the provider: a spawn error and `failure_detail` (the child's stdout
  and stderr tail, `pipeline.rs:1809-1811`, `container.rs:727-736`) have every declared value and RDM's
  four secrets replaced before they reach a receipt or a span. The guarantee is RDM's diagnostics; what
  an application logs of its own environment is the product's.
- **The container boundary, stated.** The container provider passes `spec.env` on the `docker run` argv
  as `-e K=V` (`container.rs:585-587`), so the daemon's container configuration and, briefly, the host's
  process table hold every value, RDM's four secrets included today. The landing passes each secret as
  `-e K` with the value in the docker client's own environment, which keeps it off the command-line
  arguments (the client's environment still holds it, as any launcher's does); the daemon's configuration
  still holds a container's environment, and that is the accepted boundary: it is the daemon's record,
  not RDM's, the same for the root since day 0.
- **Amendment 1.** A declared name is removed from the inherited environment before the spec's env is
  applied, exactly as `RDM_*` is (`process.rs:388-397`). The spawn comment at `process.rs:388-391`,
  which says the child inherits nothing while it inherits the host environment, is corrected to what
  the code does.
- **Amendment 2.** The order has no host tier: RDM's own launch variables, the declared ones and RDM's
  fixed pass-through list, which by amendment 3 never share a name, so their order decides nothing; the
  implementation keeps §3's "a later write of the same name wins" as its fact. The host environment
  reaches a child only for other names (`PATH`, `HOME`), never for a declared or an RDM name.
- **Amendment 3.** Refused by name at the admin's start, nothing bootstrapped: a declared name that RDM
  sets or passes through (`RDM_*`, `TRACEPARENT`, `TRACESTATE`, the pass-through list).
- **The page sentence (Luke, 2026-10-24), landing with the code.** deployment-provider.md §2.3, old:
  "the operational context needed to manage it: data_dir, the resolved executable, the launch's
  environment". New: "the operational context needed to manage it: data_dir, the resolved executable,
  the launch's environment with its declared secrets excepted (a declared secret is written to the
  admins that may launch its kinds and to those processes, never to a record or a projection)".
- **Cells, RED first, each under both providers** (i50: both spawn modes). A declared secret reaches
  only the named kinds and no other process, admin included, read from each child's environment; a
  declared secret set in the bootstrap admin's environment is absent from a kind not named; a declared
  name colliding with an RDM or a pass-through name, and a name declared twice, are refused at start
  with nothing bootstrapped, the refusal naming the variable; a create run by a launched admin, not the
  bootstrap, sets the declared value; an admin started again from its data dir without a launcher
  launches the named kinds with the value; a value holding a separator, `=` and a newline, and an empty
  value, round-trip through the custody document unchanged; a child that fails after printing a sentinel
  value leaves no trace of it in the step's failure, its receipt or its span. The secrecy assertions,
  stated so they can hold under the contract: a kind not named lacks the application variable in its
  environment; an admin's environment and its data-dir file hold the custody document, which is
  permitted; no RDM-generated diagnostic, receipt, span, row or Build record carries the value, checked
  against the run's span JSONL, the admins' logs and the store root; the one application log the failing
  child wrote its sentinel into is excluded from that check by name.

#### 0.2 A product kind's listeners (Q151: A with A1's requirements)

- **The sequence.** The product names each kind's listeners in the same wiring declaration (Rafka's
  gateway: `kafka`, `rest`, `proxy`). `spec_for` returns those names. The pipeline assigns each the
  birth's address with port 0, as the transport gets today; `RDM_LISTENERS` carries the assigned
  addresses, port 0 included, before the bind. The node binds them and keeps every successful bind open
  through its join; it reports the OS-selected ports at join; the accepting primary reads them back
  (`Bound::of`); they are published on the node row's `listeners` (R-D2).
- **Requirements.** Listener names are unique within a kind; a name RDM owns is refused at the admin's
  start (none exists today, D7; the rule stands for any RDM adds). A join report is refused by name as a
  whole, installing nothing (node-create.md step 4), unless it carries exactly the declared names, each
  on the assigned address exactly, with a port that is not 0, as `Bound::of` already requires of the
  transport (`pipeline.rs:1092-1100`): a listener missing, duplicated, undeclared, on another address,
  on an unspecified address or on port 0 refuses the report; nothing is dropped. Nothing probes and releases a port: a
  successful bind stays open through the join (node-create.md: the executor never picks, reserves or
  predicts a port).
- **No page text changes.** The sentences on assigned endpoints, port 0, the provider honouring and
  never inventing a port, and the report at join cover the transport and the listeners alike. The
  landing corrects CLAUDE.md's env table row from `RDM_ENDPOINTS` (`slot=addr=freshness`) to
  `RDM_LISTENERS` (`name=addr`): §7 D1, a table-versus-code difference, not a page.
- **Cells, RED first.** A gateway's three listeners bound, reported and published on its row; two
  gateways launched at once with no conflict; a join report missing one, duplicating one, naming an
  undeclared one, carrying one on another address, on an unspecified address and on port 0, each refused
  by name with nothing installed; a container fabric's listeners on the container's own address. This is
  the demo-surface plan's A1 cell.

#### 0.3 Settled without code

- **R-D3.2, the endpoint assignment's lifetime: A.** Today's lifetime (§5) is the contract; its one gap
  with evidence, listeners, is §0.2.
- **R-D3.3, the no-slot refusal: A.** No RDM code: Rafka's front door names the provider from
  `FabricView.provider`; RDM's typed calls cannot carry a field the type lacks; the one setting §0.1 adds
  is refused at the admin's start, naming the setting and the fabric's provider.
- **The "restart env override"** (§2): a restart keeps the process; a changed non-secret setting is a
  `node.config.update`; a changed executable is a replace or a roll; a changed secret is a new fabric
  (§0.1, fixed for the fabric's life).
- **Non-secret per-node settings** (§1): the row's `config`, read by the role at its entry; Rafka moves
  `RAFKA_GATEWAY_CONFIG`, `RAFKA_KAFKA_CLIENT_HOST`, `RAFKA_STATEFUL` and the rest there.

---

### 1. Settled: cited, not asked

**1.1 Endpoints: node-admin assigns the address, the node binds port 0 and reports at its join.**
This is the no-code part of R-D3: it is what RDM does today.

- [page] `lifecycles/node-create.md:17`: "Port | Bind port 0; advertise the actual OS-assigned transport_addr".
- [page] `lifecycles/node-create.md:37`: "The executor never picks, reserves or predicts a transport port."
- [page] `lifecycles/node-mesh-join.md:38-39`: "The node holds its bound endpoint; port 0 was bound at launch,
  or the stopped process still holds its port. Nothing predicts or reserves a port."
- [page] `deployment-provider.md:76`: `bind_ip`: "The host address a runtime binds on, port 0; the node
  reports its port at its join."
- [page] `deployment-provider.md:185`: "`ResolvedNodeLaunch` carries the already-resolved logical identity,
  FabricId and MeshId, executable, arguments, environment, data directory, assigned endpoints, resource
  limits and deployment id. The provider does not decide topology, elections, restart versus replacement,
  advertised ports, desired counts, readiness or logical identity."
- [page] `deployment-provider.md:234`: "node-admin assigns every container's address (`--ip`) so a node's
  bind address is known before it runs."
- [code] `crates/rdm-admin/src/deployment/endpoint.rs:3-5`: "A node binds port 0: the operating system
  assigns every port, and the node reports the address it really bound in its `JoinNode` digest ...
  Node-admin picks no port".
- [code] `deployment/pipeline.rs:1598-1608`: the bind address is `provider.bind_ip(node, deployment_id)`
  with port 0; listeners get the same address.
- [code] The launch carries it as `RDM_TRANSPORT_ADDR` and `RDM_LISTENERS` (`crates/rdm-core/src/launch.rs:24-27`,
  written at `:300`, `:304`).
- Naming: the brief and CLAUDE.md's env table cite `RDM_ENDPOINTS` (`slot=addr=freshness`). No such
  variable exists in code or on `deployment-provider.md`; this proposal uses the code's two names (D1, §7).

**1.2 The KMS master key: through the launch environment only, to compute, gateway and broker.**

- [page] rafka-v2 `docs/plans/i50-secrets-rebuild.md:107-114` (Q3, ruled 2026-10-24, arm (b)): "the key
  reaches exactly compute, gateway and broker, explicitly, in both spawn modes, through the launch
  environment only (under RDM the deployment provider's spec env, the per-process env slot named to 143),
  never through config, tags or any durable row."
- [page] rafka-v2 `docs/plans/prompts/designs/i50-2045-secrets-resolve-in-compute.md:33-39`: "node-admin's
  subprocess spawn stops passing the master key's variables to processes that do not use them, and its
  container spawn passes them to the three that do ... Cells: each of the three gets it in both modes; a
  gateway-free process (node-admin's own children that do not use it) does not."
- RDM's own precedent for a secret that travels only in a launch environment: [page]
  `fabric-certs.md:128-138` ("written into its launch environment beside its mesh's issuing material
  (`RDM_CA_SECRET`, the way `RDM_ISSUING_KEY` is) ... The root is never in a row, a frame or a digest.");
  [code] `launch.rs:39-57` (the four variables, each "A secret: it travels only in a launch's environment"),
  `launch.rs:79-87` (`SecretHex`: "its debug form names its length, never its value").

**1.3 Non-secret per-node settings: the node row's `config`.**

- [page] `lifecycles/node-config-update.md:5-7`: "`node.config.update` replaces a node's application
  config, the whole map. It is a single call, not a Build. Config is a property bag the application stores
  on the node; RDM never reads it".
- [page] `node-config-update.md:15-17`: "A live node is sent `ApplyNodeConfig`; it applies the map and
  carries it in its next digest. ... A stopped node is sent nothing: the command completes after the
  durable row write, and its next start receives the config in the join answer."
- [page] `docs/plans/proposal/i143-demo-surface-plan.md:19`, row (a): "per-node settings into the child:
  `extra_env` allow-list, CPU/RAM budgets, `cpu_set`, transport pin | the row's `config`
  (`JoinControl::config`, node-config-update.md) ... **A2** a role reads its `config` from the join answer
  and a `node.config.update` reaches it live."
- [code] `crates/rdm-admin/src/storage.rs:651-652`: `NodeRow.config: BTreeMap<String, String>`.

**1.4 A setting with no slot is refused `no-rdm-slot` at the front door.**

- [page] rafka-v2 `docs/plans/prompts/designs/i141-b3-binary-roll-ruling-packet.md:54-57` (§4): "The front
  door (B1, `node-admin/src/front_door.rs`): ... `shape_bins` and `extra_env` have no RDM slot and are
  refused `no-rdm-slot`; `node.restart` takes only `{node}`".
- [code] rafka-v2 `node-admin/src/front_door.rs:12-13`: "a request field RDM's op has no slot for is
  refused by name (`no-rdm-slot`, 400) before anything is sent, never dropped"; `:71-78` lists the fields,
  among them spawn's `extra_env`, `cpu_budget`, `ram_budget`, `transport_pin`, `mesh_endpoints`, `cpu_set`,
  `bin_path`, `expected_sha` and "every restart body field (`extra_env`, `wipe_data`, `bin_key`, ...)";
  `:179-181` is the refusal. The front door is not yet served (`:4-5`: "Unwired: nothing serves this
  router until the cutover (B3)").

---

### 2. The "restart env override": resolved, restart not redefined

The pages:

- [page] `lifecycles/states.md:134-137`: "node.start is the only transition out of stopped. It retains
  node.id, incarnation, endpoint key, bound port and bin and completes all start gates before
  ready-for-traffic. node.restart composes that stop/start; it is not an exit and a new birth."
- [page] `lifecycles/states.md:176-179`: "A new binary requires a replacement node with a new NodeId and
  incarnation; node.restart retains the same process and binary."
- [page] `lifecycles/node-restart.md:7`: "It runs on the stopped process, which never exits."
  `:13-14`: "Kept | Same node.id, incarnation, immutable path, name, endpoint key, bound port, bin and data
  dir"; "Runtime | Live process stops and rejoins; no new process". `:18`: "New executable | Requires a new
  node through Rafka's replace recipe".
- [page] `lifecycles/node-replace.md:18`: "Immutable exe | New bin/budgets require this new node, never a
  restart of the stopped one".
- [page] `deployment-provider.md:117`: "A restart keeps the NodeId, the IncarnationId and the
  RuntimeFact: the process never exits."

So:

- A changed **non-secret** setting is `node.config.update` (§1.3). A live node applies it at once through
  `ApplyNodeConfig`; a stopped node receives it in its next start's join answer. No restart is needed.
- A changed **executable** needs a new process, so a new birth: Rafka's replace recipe
  (node-replace.md), or a roll for an executable set (`docs/plans/proposal/roll.md`, landed). The new
  birth's launch carries the fabric's captured values. (Superseded, 2026-10-24: this paragraph first put
  a changed secret here too; §0.1 fixes a declared value for the fabric's life, so a changed secret is a
  new fabric.)
- [inferred] A restart has no moment at which it could read a new environment: the process never exits,
  and an environment is fixed when a process is launched. The "override" is impossible by the pages, not
  refused by a rule of R-D3's.
- [code] Rafka's front door already refuses a restart body field (rafka-v2 `front_door.rs:74`) and
  `node.restart` takes only `{node}` (B3 packet `:57`). Nothing to build for this item.

---

### 3. How the launch environment is assembled today

In order; a later write of the same name wins:

1. [code] `AdminConfig.passthrough` (`admin/records.rs:62-63`, filled `:139-142`): a fixed RDM list,
   `RDM_EVIDENCE_DIR`, `RUST_LOG`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `RDM_CONTAINER_SUBNET_POOL`,
   `RDM_STALENESS_MS`, `RDM_GOSSIP_INTERVAL_MS`, `RDM_BACKBONE_INTERVAL_MS`, the stopped-node TTL and
   `RDM_DATA_DIR_GONE_FLOORS`, each when set on this admin. It becomes `LaunchTemplate.env`
   (`admin/boot.rs:1289`; field `deployment/pipeline.rs:876-877`).
2. [code] For a node-admin only, `admin_env` (`admin/runner.rs:323-324`): `RDM_BIN_DIR`
   (`boot.rs:1277-1278`). Since the roll, no binding variable is forwarded.
3. [code] `RDM_MESH_ID` (`runner.rs:331-333`).
4. [code] `launch.to_env()` (`pipeline.rs:1698-1699`; `launch.rs:290-322`): the launch contract, with,
   for a node-admin's launch, the four secrets `RDM_ISSUING_KEY`, `RDM_ISSUING_CERT`, `RDM_CA_SECRET`,
   `RDM_FABRIC_CA_KEY` (`launch.rs:311-316`) as plain strings: the `SecretHex` wrapper ends here.
5. [code] `RDM_INHERITED_DATA_DIR` (`pipeline.rs:1700`), then `TRACEPARENT` (`:1701-1703`).
6. [code] The result is `ResolvedNodeLaunch.env` (`pipeline.rs:1697-1714`, the `spec_for` closure;
   struct `deployment/provider.rs:100-123`). `args` is always empty (`pipeline.rs:1708`). (Not the
   `spec_for(kind)` of `endpoint.rs:36`, which names a kind's listeners.)

The two providers then differ:

- [code] **Process** (`deployment/process.rs:388-397`): the child starts from the launching admin's whole
  host environment, removes every `RDM_*`, `TRACEPARENT` and `TRACESTATE`, then applies `spec.env`. The
  contract cell is `crates/rdm-admin/tests/process_launch_environment.rs:11-13, 54-58` ("what the process
  needs from the host (`PATH`) is kept").
- [code] **Container** (`deployment/container.rs:585-587`): only `spec.env`, each as `-e K=V` on the
  `docker run` argv, plus `HOME` for a node-admin (`:567`). The file's header says "the launch environment
  is identical to the process provider's" (`container.rs:10-13`); for variables outside `spec.env` it is
  not (D5, §7).

Where an application value can enter `spec.env` today: [code] nowhere. The passthrough list is fixed
names (`records.rs:139`); `Wiring` has no environment field (`wiring.rs:167-216`); `RecipeStep::Create`
carries `node` and `executable` only (`crates/rdm-core/src/build.rs:174-185`). The one seam is
`Wiring.provider`, a decorator of the provider (`wiring.rs:201-202`), which the file describes as the
place for "a decorator that delays, refuses or records" (`wiring.rs:6-9`).

---

### 4. What can carry an environment value today

- [code] No serialized struct has an environment field. The only `env` maps in the crates' sources are
  `LaunchTemplate.env` (`pipeline.rs:877`; `#[derive(Debug, Clone)]` at `:860`) and
  `ResolvedNodeLaunch.env` (`provider.rs:111`; `#[derive(Debug, Clone, PartialEq, Eq)]` at `:100`),
  neither `Serialize`; and `AdminConfig.passthrough` (`records.rs:30, 63`), the fixed list.
- [code] Which of them holds a plain secret: only `ResolvedNodeLaunch.env`. The four RDM secrets enter it
  through `launch.to_env()` (`pipeline.rs:1699`). `LaunchTemplate` carries them as `issuing`
  (`IssuingMaterial`, its fields `SecretHex`, `launch.rs:92-104`), not in its `env`; `passthrough` holds
  none. So `ResolvedNodeLaunch`'s derived `Debug` would print them: a single `?spec` added to a span
  would. This is latent, not present: no site formats it today (next bullet).
- [code] **Spans and logs**: a grep of every `tracing` macro in `crates/*/src` finds none that formats a
  `ResolvedNodeLaunch`, a `LaunchTemplate`, an `AdminConfig` or an env map. The one place a launched node
  prints its environment is `rdm-transport/src/transport_diag.rs:84-102`, which names eight non-secret
  variables one by one.
- [code] **Rows**: `NodeRow` (`storage.rs:643-709`), `BirthFacts` (`:465-514`), `NodeRecord`
  (`:567-598`), `RuntimeRow` (`:614-626`), `Allocation` (`:723-734`): no environment field.
- [code] **Receipts**: `RuntimeEvidence` (`pipeline.rs:514-525`): deployment id, provider, two
  fingerprints, locator kind, data dir.
- [code] **Build records**: the accepted change and recipe steps carry no environment
  (`rdm-core/src/build.rs:63`, `:170-215`); the roll's `Roll` carries a binding set (`:160`).
- [code] **Launch records on disk**: the process provider's `deployment.json` holds deployment id, pid and
  start (`process.rs:418`).
- [code] A launch whose runtime exits before it reports puts `provider.failure_detail` into the step's
  failure (`pipeline.rs:1809-1811`, and in the later waits `:1884-1885`, `:1911-1912`): the process
  provider's tail of the runtime's own stdout and stderr, the container provider's `docker logs --tail 20`
  (`container.rs:727-736`). That failure reaches the receipt and the span.
- [code] The container provider puts every `spec.env` value on the `docker run` argv (`container.rs:585-587`).
  Its error text carries only the subcommand and docker's stderr (`container.rs:79-91`), not the argv.

So the four places the brief names (a row, a Build record, a receipt, a span) carry no environment value
today; the redaction point R-D3.1 asks for has nothing to undo, only something to keep true.

---

### 5. The endpoint assignment's lifetime today

Reserved versus advertised, as the code has them:

- **Assigned**: the address only, never a port. [code] Process: loopback, the provider trait's default
  (`provider.rs:438-445`). Container: one address per birth's `deployment_id` on the fabric network,
  chosen from the addresses neither this provider nor the daemon lists as taken (`container.rs:694-725`),
  held in memory "until that birth's container is removed" (`container.rs:55-60`).
- **Reserved**: nothing. [page] node-create.md:37, node-mesh-join.md:39 (§1.1). [code] port 0
  (`pipeline.rs:1607`).
- **Advertised**: what the birth reports at its join. [code] `WaitForBind` takes it from the validated
  `JoinNode` report (`pipeline.rs:1801-1820`; `Bound::of` `:1092-1100`, which refuses an unspecified IP
  or port 0), publishes it (`:1828-1829`, `:1835-1839`), and the mesh's row writer puts it on that
  birth's facts (`admin/rows_writer.rs:109-111`; `BirthFacts.transport_addr` `storage.rs:498-500`).
- **Listeners**: [code] every kind's `KindSpec` names none (`endpoint.rs:28-43`; product kinds: "its own
  listeners are the product's to assign through its node-admin binding", `:38-39`), and `Bound::of`
  returns none for every kind (`pipeline.rs:1098-1099`, commented for a node-admin only). So
  `RDM_LISTENERS` is always empty today. Rafka assigns its own app ports ([page] demo plan `:18`, A1:
  "Rafka's REST and Kafka ports are assigned by node-admin today, `assign_gateway_listen_ports`").

By case:

| case | assigned address | advertised address |
|---|---|---|
| failed launch, same operation re-run | [code] a re-run continues the same birth and deployment id from the claim row (`pipeline.rs:1472-1489`, `storage.rs:720-734`); the container provider returns the address it already gave that deployment (`container.rs:703-705`, `:708-710`) | none was written: publishing runs after `WaitForBind` (`pipeline.rs:1801-1839`) |
| failed launch, decided afresh | [code] a new deployment id gets its own address; the failed deployment's entry stays in the provider's memory until that container is removed or the admin restarts (`container.rs:476-481`) | none |
| exit | [code] the container's address is dropped when its container is removed (`container.rs:476-481`, called from `terminate`, `:602-657`, at `:611, 620, 625, 641, 656`); recovery and delete begin with `TerminateRuntime` ([page] `deployment-provider.md:270`) | [code] `TerminateRuntime` waits, up to 3 s, until `/proc` shows the exited pid holds none of the birth's advertised sockets (`pipeline.rs:2426-2442`, `endpoint.rs:71-85`); the facts keep the address as that birth's until mesh-left clears it (`storage.rs:522-531`, `mesh_delete.rs:879`) or the record keeps only its two newest births (`storage.rs:556-558`) |
| restart | [page] unchanged: "bound port" kept (node-restart.md:13) | unchanged: same birth, same facts |
| new birth at the same `path.name` (recovery, replace, roll) | [page] "binds port 0, with a new node.id, incarnation and port" (`node-recovery.md:94`); [code] a container birth gets an address of its own (`container.rs:694-698`) | a new `BirthFacts` entry of its own |
| admin restart | [code] the container provider's map starts empty (`container.rs:415`); the daemon's network listing is what is taken (`:706`, `:713`) | rows are read back; nothing depends on the restarted admin's memory |
| container | as above: one address per birth, ports the operating system's (`container.rs:5-8`, `:694-695`); a listener on another address is refused by name (`:508-510`) | the container's own socket table is read (`container.rs:7-8`) |

---

### 6. The open questions

#### R-D3.1: the allow-list's owner and precedence, and the redaction point

**Ruled: A with three amendments (Q150). The decided contract is §0.1.**

##### 1. Spec
i50 Q3 arm (b) (§1.2): the key reaches exactly compute, gateway and broker, in both providers, through
the provider's per-process spec env only; never config, tags or a durable row. The PRD row (`:2938`):
"the spawn env allow-list and its redaction"; "Secrets never in a row, a Build record, a receipt or a
span." Owner: the node-admin that executes the create, the owning mesh's executor ([page] `build.md:80`:
"The owning Mesh executor realizes ordinary members and sibling node-admins"). Sequence: the app names
its variables and kinds → RDM writes each into the launches of those kinds only → nothing persists.

##### 2. Issue
No red: an application cannot hand a variable to the kinds it chooses under either provider; found by
reading, not by a failure.

##### 3. Flame
None: a design question, no trace.

##### 4. Observations (RDM `7811eb4a3`, rafka-v2 `f52217511f`)
- [code] No application value enters `spec.env` (§3, last paragraph).
- [code] Process provider: every non-`RDM_*` variable of the launching admin reaches every child
  (`process.rs:388-397`).
- [code] Container provider: only `spec.env` reaches a container (`container.rs:585-587`).
- [code] Rafka's front door refuses `extra_env` today (rafka-v2 `front_door.rs:71`).
- [page] Rafka today builds its children's environment in its own spawn and passes the key to all of
  them (rafka-v2 i50 brief `:33-36`: "node-admin's subprocess spawn stops passing the master key's
  variables to processes that do not use them").
- [code] Redaction today is at the type (`SecretHex`, `launch.rs:79-87`), lost when the value enters the
  spec's string map (`launch.rs:311-316`, `pipeline.rs:1699`); no span, row, receipt or Build record
  carries an env value (§4).
- [code] Collision handling today: a later write wins (§3), and the process provider removes the
  launcher's `RDM_*`, `TRACEPARENT`, `TRACESTATE` before applying the spec (`process.rs:392-396`).
- [page] RDM's custody shape for a value an admin must hand on: every launched node-admin is written the
  root in its launch and keeps it, so any admin can act with it (`fabric-certs.md:128-137`).

###### Inferred
- [inferred] With the key on a node-admin's own environment, the process provider hands it to every kind,
  rpc nodes and node-admins included, and each launched admin hands it on the same way; the container
  provider hands it to none. Neither meets "exactly compute, gateway and broker, in both spawn modes".
- [inferred] Every node-admin must hold the value: the owning mesh's executor launches the three kinds
  (`build.md:80`), and any admin of a mesh can become its mesh-primary. So the admins hold it as custody,
  the way they hold the root, and i50's "exactly three" counts the kinds the variable is set on for use.
  Rafka confirms that reading; its node-admin holds the key today.

##### 5. Delta
i50 Q3 says the key is carried by the provider's per-process spec env to three kinds only; RDM's code has
no path for an application value into `spec.env`, so the process provider hands every inherited variable
to every kind and the container provider hands none.

##### 6. Arms
Every arm assumes RDM's own launch contract (`RDM_*`, `TRACEPARENT`, `TRACESTATE`) is written by RDM
alone and never overridden.

- **A (recommended): the app declares, RDM enforces, the type keeps the value out of every span.** The
  app's executable declares in its `Wiring` each variable by name with the node kinds it is set on; the
  value is read once from the bootstrap admin's own environment at its start, the way it reads
  `RDM_CA_SECRET`. Two rules, kept apart:
  - *Custody (RDM's rule).* Every node-admin RDM launches is written every declared value in its launch,
    beside its issuing material, as `fabric-certs.md:128-137` does for the root, so whichever admin
    executes a create can launch the named kinds. This is not optional: without it a mesh-primary that
    is not the bootstrap admin cannot launch them.
  - *Use (Rafka's list).* A declared variable is set in the process environment only of the kinds the
    app names (for the KMS key: compute, gateway, broker).
  Owner: the app names; RDM writes, carries and refuses. Precedence: RDM's launch contract, then the
  declared variables, then the passthrough list, then (process provider only) the inherited host
  environment. Refused by name at the admin's start, before any provider opens (as the bindings are,
  `records.rs:124-138`): a declared name that is `RDM_*`, `TRACEPARENT` or `TRACESTATE`, and a kind the
  fabric does not run. The process provider removes every declared name from the inherited environment
  of every launch and sets it only where the two rules say, so a kind not named never sees it in either
  provider. Redaction point: from the read to the provider's spawn the value is held in a type whose debug
  form names its length, never its value (`SecretHex`, `launch.rs:79-87`); `ResolvedNodeLaunch` carries
  declared values and RDM's own four secrets beside `env` in that type, which closes the latent `Debug`
  path of §4. Nothing is written to a row, a Build record, a receipt or the launch record. Changes:
  `Wiring` (one field), `ResolvedNodeLaunch` (one field), the launch of a node-admin (the custody
  write), both providers' env application, the admin's start refusal.
- **B: widen the passthrough list.** A variable set on the admin names further host variables to pass;
  they reach every kind. Changes the config list only; fails "exactly three kinds"; no redaction point.
- **C: values on the Build.** The Create step or the mesh's desired topology carries the variables.
  Puts raw values in the Build record, against the brief's rule and i50's "never ... any durable row";
  a names-only variant still needs the values from somewhere else.
- **D: the app's provider decorator.** Rafka wraps the provider (`Wiring.provider`, `wiring.rs:201-202`)
  and adds its variables in its own spawn. No RDM code; RDM refuses no collision and keeps no redaction
  point; the process provider's inheritance (`process.rs:392-397`) still reaches every kind, and a
  launched admin's custody copy is Rafka's to carry.
- **X: something else.**

Not asked, named: the container provider puts every `spec.env` value, RDM's own four secrets included,
on the `docker run` argv (`container.rs:585-587`), where the host's process table and the container's
configuration show it [inferred from Docker's behaviour]. It is none of the four places the brief names,
and RDM's secrets already take that path; arm A does not change it.

#### R-D3.2: the endpoint assignment's lifetime

**Ruled: A, no code. Its listener gap is ruled separately as Q151, §0.2.**

##### 1. Spec
[page] node-create.md:17 and :37, node-mesh-join.md:38-39, deployment-provider.md:76, :185, :234 (§1.1).
Owner: the executing node-admin assigns the address; the node binds port 0 and reports; the mesh's row
writer records the report. Sequence: `bind_ip` → launch with port 0 → `JoinNode` report → `WaitForBind`
→ published on the birth's facts → released at proven exit.

##### 2. Issue
No red: the PRD row owes a port assignment "non-conflicting, released on termination, bound ports on the
node view"; the question is whether today's lifetime already is that.

##### 3. Flame
None: a design question, no trace.

##### 4. Observations (RDM `7811eb4a3`)
- [code] Non-conflicting: the operating system assigns every port (`endpoint.rs:3-5`, `pipeline.rs:1607`);
  a container address is chosen from those not taken (`container.rs:706-724`).
- [code] Released on termination: `TerminateRuntime` proves the exited pid holds none of the advertised
  sockets before the step ends (`pipeline.rs:2426-2442`).
- [code] Bound ports on the node view: the reported address is on the birth's facts
  (`rows_writer.rs:109-111`, `storage.rs:498-503`).
- [code] Every case of the brief: the §5 table.
- [code] Listeners: every `KindSpec` is empty and `Bound::of` drops reported listeners
  (`endpoint.rs:28-43`, `pipeline.rs:1098-1099`); `BirthFacts.listeners` exists and is never filled
  from a report (`storage.rs:501-503`).
- [page] Demo plan A1 (`i143-demo-surface-plan.md:18`): "One role binds one slot, the row carries it, a
  restart keeps it": the listener slot is that plan's row.

###### Inferred
- [inferred] The only gap with evidence is listeners, and its evidence is Rafka's: its app ports are
  assigned by its own node-admin today (demo plan A1), and its front door lists `kafka_port`,
  `rest_port` as fields RDM does not hold (rafka-v2 `front_door.rs:53`).
- [inferred] A failed launch decided afresh leaves one container address in the provider's memory until
  the admin restarts; no telemetry shows a network running out.

##### 5. Delta
Spec says a launch carries assigned endpoints and nothing reserves a port; code assigns an address per
birth, reserves no port, advertises what the birth reports, and carries no listener.

##### 6. Arms
Every arm assumes the transport port is never picked or reserved (node-create.md:37).

- **A (recommended): today's lifetime is the contract; no code in R-D3.** Assigned is the address only,
  per birth (`deployment_id`); reserved is nothing; advertised is the birth's report, kept on its facts
  for the birth's life (a restart keeps it), released by proven exit and taken off at mesh-left. A failed
  launch re-run keeps its birth's address; an admin restart reads the daemon and the rows, not memory. The
  listener slot rides this same lifetime when demo plan A1 fills it, as A1's own row. Changes nothing:
  reuses `pipeline.rs:1598-1608, 1801-1839, 2426-2442`, `container.rs:694-725`.
- **B: complete the listener slot inside R-D3.** A kind declares listener names; each binds the assigned
  address at port 0, is reported at the join, kept by `Bound::of` and written to `BirthFacts.listeners`,
  released with the transport. Changes `KindSpec` and `Bound::of`; the lifetime is A's.
- **C: an assignment kept across births.** The address and ports are kept per `path.name` across
  recovery, replace and roll. Changes the pages: node-create.md:37, node-mesh-join.md:39 and
  node-recovery.md:94 ("a new node.id, incarnation and port").
- **X: something else.**

#### R-D3.3: the no-slot refusal names the setting and the provider before anything launches

**Ruled: A, no RDM code (§0.3).**

##### 1. Spec
The brief (PRD `:2938`): "a setting with no slot refused `no-rdm-slot` at the front door (i141 B3 packet
§4)"; open: "the no-slot refusal naming setting and provider before anything launches". [page]
`deployment-provider.md:24`: "A Build request does not choose its provider." Owner: the front door that
decodes the request, before any RDM call.

##### 2. Issue
No red: the refusal exists at Rafka's front door and names the route, the fields and the RDM op, not the
provider.

##### 3. Flame
None: a design question, no trace.

##### 4. Observations (RDM `7811eb4a3`, rafka-v2 `f52217511f`)
- [code] rafka-v2 `front_door.rs:179-181`: `"{route}: {fields} carr{ies,y} a value RDM's {op} has no
  slot for; nothing was sent"`: before any call, no provider named.
- [code] The provider is readable at the front door: `FabricView.provider` (`rdm-core/src/views.rs:236-237`,
  re-exported `rdm-client/src/lib.rs:23`), and the build route already calls `fabric.get` first
  (rafka-v2 `front_door.rs:36`).
- [code] RDM's typed calls are postcard (`rdm-core/src/protocol.rs:35-49`): a field the type lacks cannot
  be sent, so an unslotted setting can exist only where JSON is decoded, at an app's door.
- [code] RDM has no provider-dependent setting: `ResolvedNodeLaunch` has no resource limits, cpu set or
  pin (`provider.rs:100-123`), and `RecipeStep::Create` carries `node` and `executable` (`build.rs:174-185`).
- [code] `build_names_a_provider` (`provider.rs:81-86`) has no caller outside its own test (`:634-636`).
- [code] rafka-v2 `node-admin/src/lib.rs:1265-1302`: Rafka hands `cpu_budget`, `ram_budget`,
  `transport_pin` (a CPU pin mode, `:1304-1311`), `mesh_endpoints` (a count, `:1316-1319`) and `cpu_set`
  to a child as argv flags at its spawn.

###### Inferred
- [inferred] Under arm A of R-D3.1 the only new application setting RDM accepts (a declared variable)
  is refused, when it is, at the admin's start, before any provider opens; that refusal can name the
  fabric's provider, which the admin holds.
- [inferred] Rafka's argv settings are read by its process at start, before its join answer carries the
  row's `config`; §1.3 settles that they go through `config`, so Rafka's process reads them from the join
  answer or applies them once it has it. Named, not asked.

##### 5. Delta
Spec (the brief) says the no-slot refusal names the setting and the provider before anything launches;
code's refusal, at Rafka's front door, names the setting and the RDM op, not the provider, and RDM has no
provider-dependent setting to refuse.

##### 6. Arms
Every arm assumes an unslotted setting is refused by name, never dropped.

- **A (recommended): the refusal stays at the app's door; no RDM code.** Rafka's `no_slot` adds the
  provider from `fabric.get`'s `FabricView.provider` (one line, Rafka's). RDM's typed calls cannot carry
  an unslotted setting; the one application setting R-D3.1 A adds is refused at the admin's start, naming
  the setting and the provider, before any provider opens (built with R-D3.1 A). Changes nothing in RDM
  for this question: reuses `views.rs:236-237`.
- **B: an RDM refusal at the Build door.** A typed setting the fabric's provider cannot apply is refused
  at acceptance, `no-rdm-slot { setting, provider }`, nothing accepted. Needs a provider-dependent field
  first; none exists.
- **C: refusal at the provider's spawn.** `DeployRuntime` refuses. By then the name claim and identity
  are written (`pipeline.rs:1637-1648`), so the refusal leaves a claim to withdraw.
- **X: something else.**

---

### 7. Page-vs-code differences found

- **D1** CLAUDE.md env table, `RDM_ENDPOINTS` row ("Comma-separated `slot=addr=freshness` the node must
  bind exactly; a provider never invents a port"), and `i143-demo-surface-plan.md:18` (`RDM_ENDPOINTS`):
  no such variable. Code: `RDM_TRANSPORT_ADDR` (an address with port 0) and `RDM_LISTENERS`
  (`name=addr`), `launch.rs:24-27`. CLAUDE.md has no row for either.
- **D2** `deployment-provider.md:158-164`: the birth's current runtime metadata is "data_dir, the resolved
  executable, the launch's environment", and "Required runtime-management metadata converges to every
  node-admin as current birth projection state. It never stays launcher-private." Code projects no
  environment (§4: no row, receipt or record field). The page line as written would put
  `RDM_CA_SECRET` in projection state, against `fabric-certs.md:138` ("The root is never in a row, a
  frame or a digest") and i50 Q3. R-D3.1 arm A cannot be built against this line as written; its text is
  Luke's to change.
- **D3** `deployment-provider.md:185`: `ResolvedNodeLaunch` carries "assigned endpoints, resource limits".
  Code (`provider.rs:100-123`): a transport address with port 0, listeners always empty, no resource
  limits.
- **D4** `deployment-provider.md:249-250`: the create steps include `ClaimName` and `AllocateEndpoints`.
  Code `CreateStep::ORDER` (`pipeline.rs:121-135`) has neither: the claim is inside `PrepareStorage`
  (`:78-80`, `:1643`), and the address is decided outside any step (`:1598-1608`), so no receipt
  records it.
- **D5** `container.rs:10-13` (code comment): "the launch environment is identical to the process
  provider's". It is not for variables outside `spec.env`: the process child inherits the launcher's
  non-`RDM_*` environment (`process.rs:388-397`), a container gets none (`container.rs:585-587`).
  `deployment-provider.md:225` says of the process provider only "The launcher's own `RDM_*`,
  `TRACEPARENT` and `TRACESTATE` never reach the child"; the page is silent on the rest.
- **D6** `deployment-provider.md:24` ("A Build request does not choose its provider") and
  `provider.rs:7` ("a Build never names a provider"): the refusal function `build_names_a_provider`
  (`provider.rs:81-86`) has no caller outside its test. The typed wire cannot carry a provider field, so
  nothing reaches a Build today and the page holds; the function is unused. Named cleanup with R-D3's
  code: remove it.
- **D7** CLAUDE.md env table, `RDM_NODE_ADMIN_API_BIND` ("The control API's HTTP bind ... A launched
  admin binds the `control` slot node-admin assigned") and `RDM_NODE_ADMIN_API_BASE` ("the URL node-admin
  prints"); code: `rdm-admin` depends on no HTTP server, `api_base` is `rdm://<node id>`
  (`admin/boot.rs:650`), and `records.rs:120` (`api_bind`) and `boot.rs:499` (`addr_of("control")`,
  always none since `NODE_ADMIN` names no listener, `endpoint.rs:33`) are inputs nothing serves. So RDM
  owns no listener name today, which §0.2's refusal of "a name RDM owns" states; the two table rows and
  the two dead reads are cleanup with R-D3's code, like D6; `boot.rs:82` keeps printing the node's
  `rdm://` base under a name that no longer says HTTP, or the name goes with the rows.

---

### 8. Inferred claims, together

1. A restart has no moment at which it could read a new environment (§2).
2. With the key on a node-admin's environment, the process provider hands it to every kind and onward
   through launched admins; the container provider to none (R-D3.1).
3. Every node-admin must hold the value as custody, since any admin can become the executor that launches
   the three kinds; i50's "exactly three" is read as the kinds it is set on for use, and Rafka confirms
   that reading (R-D3.1).
4. Docker shows `-e` values in the host's process table and the container's configuration (R-D3.1).
5. A failed launch decided afresh leaves one container address in the provider's memory until the admin
   restarts; no telemetry shows a network running out (§5, R-D3.2).
6. The only endpoint gap with evidence is listeners, and the evidence is Rafka's (R-D3.2).
7. A declared-variable refusal at the admin's start can name the provider (R-D3.3).
8. Rafka's argv settings are read before the join answer that carries `config` (R-D3.3).
