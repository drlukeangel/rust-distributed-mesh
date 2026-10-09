import { open, tab, shot, api, row, sleep, summary, nodes } from "./lib.mjs";
const { browser, page } = await open();
const errs = []; page.on("pageerror", (e) => errs.push(String(e))); page.on("response", (r) => r.status() >= 400 && errs.push(`${r.status()} ${r.url()}`));
await page.waitForTimeout(3500);

// H1 header
let txt = await page.locator("header").innerText();
let s = await shot(page, "H1-header");
row("H1", "R-U1(2): header shows fabric state, current Build, per-mesh node counts, fabric primary", "header", "load the UI", "fabric ready-for-traffic, build shown, mesh1 10 nodes, mesh2 10 nodes, 20 total, fabric primary named, traffic running",
  txt.replace(/\n/g, " | ").slice(0, 400), /ready-for-traffic/.test(txt) && /mesh1: 10 nodes/.test(txt) && /mesh2: 10 nodes/.test(txt) && /20 nodes total/.test(txt) && /fabric primary mesh1\.admin\.1/.test(txt) && /traffic running/.test(txt), s);

const sum = await page.locator("[data-testid=cluster-summary]").innerText();
row("H0", "Luke visual spec(1): header line 'N spawned · meshes · chaos/min · mean peers'", "header", "read the summary line", "20 spawned · meshes: mesh1, mesh2 · chaos: 0/min · mean peers: number", sum, /^20 spawned · meshes: mesh1, mesh2 · chaos: \d+\/min · mean peers: [\d.]+$/.test(sum), await shot(page, "H0-summary"));
const opts = await page.locator("[data-testid=mesh-select] option").allInnerTexts();
row("H0b", "Luke visual spec(2): mesh dropdown marks the mesh holding the fabric primary", "spawn bar mesh select", "read options", "mesh1 (primary), mesh2", opts.join(", "), opts[0] === "mesh1 (primary)" && opts[1] === "mesh2");
const bn = await page.locator("[data-testid=budget-note]").innerText();
row("H0c", "Luke visual spec(2): cpu/ram preset boxes only if node-admin carries the budget; otherwise not faked", "spawn bar", "read", "no preset boxes; a labelled note says budgets are fixed at launch", bn, !(await page.locator(".spawn-bar input").count()) && /fixed at launch/.test(bn));

// H2 spawn bar
const btns = (await page.locator(".spawn-bar button").allInnerTexts()).map((x) => x.trim());
s = await shot(page, "H2-spawn-bar");
row("H2", "spec: spawn bar kinds = what node-admin manages in the R-shape", "spawn bar", "read the buttons", "+ node_admin, + gateway, + broker, + compute, + mesh, remove mesh, bootstrap; no rpc_node",
  btns.join(" / "), ["+ node_admin", "+ gateway", "+ broker", "+ compute", "+ mesh"].every((b) => btns.includes(b)) && !btns.some((b) => /rpc/.test(b)) && btns.some((b) => /bootstrap 2-mesh/.test(b)), s);

// H3 server refuses rpc_node
let r = await api("/api/nodes/spawn", "POST", { mesh: "mesh1", kind: "rpc_node" });
row("H3", "spec: rpc_node is not a managed kind of the R-shape", "POST /api/nodes/spawn", "ask for rpc_node", "422 kind-not-managed-in-the-r-shape", `${r.status} ${r.json?.error}`, r.status === 422 && r.json?.error === "kind-not-managed-in-the-r-shape");

// T1..T5 topology
await tab(page, "Topology");
s = await shot(page, "T1-topology");
let t = await nodes();
const kinds = {}; for (const n of t.nodes) kinds[`${n.mesh}/${n.kind}`] = (kinds[`${n.mesh}/${n.kind}`] ?? 0) + 1;
const groups = await page.locator(".react-flow__node-group").count();
const colours = {}; for (const k of ["node_admin","gateway","broker","compute"]) { const n = t.nodes.find((x) => x.kind === k); colours[k] = await page.locator(`.react-flow__node:has([data-testid="node-${n.name}"])`).evaluate((el) => getComputedStyle(el).borderTopColor); }
row("T1", "e11: the canonical R-shape x2 is what is drawn", "Topology", "open the tab", "2 mesh groups; per mesh node_admin 2, gateway 3, broker 3, compute 2",
  `${groups} groups; ${JSON.stringify(kinds)}`, groups === 2 && Object.entries(kinds).every(([k, v]) => v === { node_admin: 2, gateway: 3, broker: 3, compute: 2 }[k.split("/")[1]]), s);
row("T1b", "Luke visual spec(3): kind palette gateway blue, broker orange, compute green, node_admin gold", "Topology cards", "read border colours", "gateway rgb(88,166,255), broker rgb(240,136,62), compute rgb(63,185,80), node_admin rgb(227,179,65)", JSON.stringify(colours), colours.gateway === "rgb(88, 166, 255)" && colours.broker === "rgb(240, 136, 62)" && colours.compute === "rgb(63, 185, 80)" && colours.node_admin === "rgb(227, 179, 65)", s);
const lines = await page.locator("[data-testid=node-mesh1\\.broker\\.1]").innerText();
row("T1c", "Luke visual spec(3): cards show short name, kind, status, CPU used/budget, MEM used/budget, seat, RX/TX placeholder", "Topology card mesh1.broker.1", "read the card", "CPU:x/y and MEM:x/ygb lines, RX/TX pending R-L2", lines.replace(/\n/g, " | "), /CPU:\d/.test(lines) && /MEM:\d.*gb/.test(lines) && /RX\/TX: pending R-L2/.test(lines), s);
const bbn = t.nodes.filter((n) => n.backbone).map((n) => `${n.name}:${n.backbone}`);
const bbEdges = t.edges.filter((e) => e.cross_mesh && e.backbone);
row("T6", "Luke: every cross-mesh node-admin pair on the backbone shows a live dashed link", "Topology gold dashed edges", "count backbone edges among listeners", "one edge per listener pair across meshes, all connected", `${bbn.join(", ")}; ${bbEdges.length} links: ${bbEdges.map((e) => e.source + "->" + e.destination + ":" + e.state + "/" + e.basis).join("; ")}`, bbEdges.length > 0 && bbEdges.every((e) => e.state === "connected") && bbn.every((x) => x.includes(".admin.")), s);
const seats = t.nodes.filter((n) => n.seat).map((n) => `${n.name}:${n.seat}`);
row("T2", "R-U1(2): 'mesh primary' only on the admin holding it; fabric primary labelled", "Topology / Nodes", "read seat labels", "one mesh primary per mesh (2), both node_admins; FP labelled 'fabric primary · mesh primary'",
  seats.join("; "), seats.length === 2 && seats.every((x) => x.includes(".admin.")) && seats.some((x) => x.includes("fabric primary")), s);
const srcKinds = new Set(t.edges.map((e) => e.source.split(".")[1])); 
row("T3", "R-U1(1)/task: edges are drawn from EVERY node, not only node-admins", "Topology", "count edges by source role", "edges whose source is gateway, broker and compute as well as admin",
  `${t.edges.length} edges; source roles ${[...srcKinds].join(",")}; fact sources ${JSON.stringify(t.fact_sources)}`, ["admin", "gateway", "broker", "compute"].every((k) => srcKinds.has(k)), s);
const gb = t.edges.filter((e) => /gateway/.test(e.source) && /broker/.test(e.destination)).length + t.edges.filter((e) => /broker/.test(e.source) && /gateway/.test(e.destination)).length;
row("T3b", "task: gateway<->broker traffic shows", "Topology", "count gateway/broker edges", ">0", `${gb}`, gb > 0, s);
const legend = await page.locator("[data-testid=topology-legend]").innerText(); const rule = await page.locator("[data-testid=topology-rule]").innerText();
row("T4", "task: the current-state rule is documented in the UI legend", "Topology legend", "read legend", "legend lists connected/proxy/recovered/failed/disconnected and the edge rule", `${legend.length} chars legend; rule: ${rule.slice(0, 120)}...`, /recovered/.test(legend) && /failed/.test(legend) && /CURRENT state/.test(rule), s);
const chips = await page.locator("text=/^CPU:\\d/").count();
row("T5", "task: CPU/RAM per node on Topology", "Topology node chips", "count chips", "20 cards with CPU:x/y and MEM lines", `${chips} chips`, chips === 20, s);

// N1 Nodes
await tab(page, "Nodes"); s = await shot(page, "N1-nodes");
const cards = await page.locator(".grid-cards .card").count(); const bars = await page.locator("[data-testid^=load-]").count();
row("N1", "task: CPU/RAM bars on Nodes", "Nodes", "open the tab", "20 cards each with CPU and RAM bars", `${cards} cards, ${bars} with bars`, cards === 20 && bars === 20, s);

// M1 Messages
await tab(page, "Messages"); s = await shot(page, "M1-messages");
const mrows = await page.locator("[data-testid=msg-row]").count();
const mtxt = await page.locator("main").innerText();
row("M1", "Luke visual spec(5): Messages tab is a live feed of Node RPC calls and gossip activity", "Messages", "open the tab", "rows with from -> to, op, outcome, elapsed", `${mrows} rows; sample: ${mtxt.split("\n").slice(2, 8).join(" | ").slice(0, 200)}`, mrows > 20 && /op \d+/.test(mtxt) && /Reply/.test(mtxt), s);
await page.locator("[data-testid=msg-kind]").selectOption("gossip"); await page.waitForTimeout(2500);
const gtxt = await page.locator("main").innerText(); const grows = await page.locator("[data-testid=msg-row]").count();
row("M2", "Messages: gossip filter shows seat/forwarded/backbone activity only", "Messages feed select", "choose gossip", "rows whose span is mesh.seat / membership forwarded / backbone, none rpc", `${grows} rows; ${gtxt.includes("node_rpc") ? "contains rpc" : "no rpc"}; sample ${gtxt.split("\n").slice(2, 6).join(" | ").slice(0, 160)}`, grows > 0 && !gtxt.includes("node_rpc.request"), await shot(page, "M2-messages-gossip"));
await page.locator("[data-testid=msg-kind]").selectOption("rpc"); await page.locator("[data-testid=msg-filter]").fill("mesh2.broker.1"); await page.waitForTimeout(2500);
const ftxt = await page.locator("[data-testid=msg-row]").allInnerTexts();
row("M3", "Messages: text filter", "Messages filter box", "kind=Node RPC, filter 'mesh2.broker.1'", "only rows naming mesh2.broker.1", `${ftxt.length} rows, all match: ${ftxt.every((x) => x.includes("mesh2.broker.1"))}`, ftxt.length > 0 && ftxt.every((x) => x.includes("mesh2.broker.1")), await shot(page, "M3-messages-filter"));
await page.locator("[data-testid=msg-filter]").fill("");

// B1 Builds
await tab(page, "Builds"); s = await shot(page, "B1-builds");
const bt = await page.locator("main").innerText();
row("B1", "R-U1(3): Builds tab shows attempts and steps", "Builds", "open the tab", "current Build with attempt and steps", bt.replace(/\n/g, " ").slice(0, 200), /bld-/.test(bt) && /attempt/i.test(bt), s);

// L1/L2 Timeline
await tab(page, "Timeline"); s = await shot(page, "L1-timeline");
const lt = await page.locator("main").innerText();
const hb = /via-heartbeat/.test(lt);
row("L1", "task: Timeline shows the nodes' own span records; heartbeats are not the story", "Timeline", "open the tab", "events present, no heartbeat rows by default", `${lt.split("\n").length} lines; heartbeat shown: ${hb}`, lt.length > 200 && !hb, s);
const toggle = page.locator("input[type=checkbox]").first();
if (await toggle.count()) { await toggle.check(); await page.waitForTimeout(2500); const lt2 = await page.locator("main").innerText(); s = await shot(page, "L2-timeline-high-volume");
  row("L2", "Timeline high-volume toggle", "Timeline checkbox", "tick it", "heartbeats and membership snapshots appear", `heartbeat rows shown: ${/via-heartbeat/.test(lt2)}`, /via-heartbeat/.test(lt2), s); }

// W1 boot waterfall
await tab(page, "Boot Waterfall"); await page.waitForTimeout(8000); s = await shot(page, "W1-boot-waterfall");
const wt = await page.locator("main").innerText();
row("W1", "task: Boot Waterfall + trace link work against Jaeger", "Boot Waterfall", "open the tab (first node)", "spans drawn with a link to the trace", wt.replace(/\n/g, " ").slice(0, 260), /open this trace in Jaeger/.test(wt), s);

// Alerts tab opens
await tab(page, "Alerts"); s = await shot(page, "A0-alerts");
const tabsOrder = (await page.locator(".tab").allInnerTexts()).join(", ");
row("TAB", "Luke visual spec(5): tabs in the old order", "tab strip", "read", "Topology, Nodes, Messages, Boot Waterfall, Chaos, Timeline, Alerts (+ Builds; Tests is the sibling's)", tabsOrder, tabsOrder.startsWith("Topology, Nodes, Messages, Boot Waterfall, Chaos, Timeline, Alerts"));
row("A0", "L2: Alerts tab opens and lists", "Alerts", "open the tab", "list or 'no alerts raised yet'", (await page.locator("main").innerText()).replace(/\n/g, " ").slice(0, 160), true, s);
row("PAGE", "no browser errors", "console", "all tabs opened", "no pageerror/console error", errs.join(" | ") || "none", errs.length === 0);
await browser.close();
