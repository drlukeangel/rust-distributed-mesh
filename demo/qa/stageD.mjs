import { open, tab, shot, api, row, sleep, summary, nodes, until } from "./lib.mjs";
const { browser, page } = await open();
page.setDefaultTimeout(150000);
await page.waitForTimeout(3000);
const card = (name) => page.locator(".grid-cards .card").filter({ has: page.getByText(name, { exact: true }) }).first();
const allReady = async (n = 20) => until("all ready", async () => { const s = await summary(); return s.nodes === n && s.by["ready-for-traffic"] === n ? s : null; }, 300000, 2500);
const lastOutcome = async () => (await page.locator("[data-testid=fault-outcome]").first().innerText().catch(() => "")).replace(/\s+/g, " ");
const traffic = async () => (await api("/api/traffic")).json;
const cuts = async () => (await api("/api/chaos")).json.cuts;
const bbLinks = async () => (await nodes()).edges.filter((e) => e.cross_mesh && e.backbone);

// ---- C3 cut a node, heal
const N = "mesh1.gateway.3";
await tab(page, "Chaos");
await card(N).locator("button", { hasText: "cut network" }).click(); await page.waitForTimeout(1500);
const oc = await lastOutcome();
const nonReady = await until("node unheard", async () => { const s = await summary(); return s.by["ready-for-traffic"] < 20 ? s : null; }, 90000, 1500);
await sleep(4000);
let sm = await summary(); let shotA = await shot(page, "C3a-cut-node");
const red = sm.t.edges.filter((e) => (e.source === N || e.destination === N)).reduce((a, e) => ((a[e.state] = (a[e.state] ?? 0) + 1), a), {});
const cutsNow = await cuts();
row("C3a", "R-U1(5): network cut of a node", "Chaos 'cut network'", `cut ${N}`, "outcome applied with the member and its ports; the node goes unheard; its edges are not green", `${oc.slice(0, 200)}; status counts ${JSON.stringify(sm.by)}; edges at ${N}: ${JSON.stringify(red)}; active cuts ${JSON.stringify(cutsNow.map((c) => [c.id, c.members, c.ports]))}`, /applied/.test(oc) && !!nonReady && !red.connected, shotA);
await tab(page, "Topology"); await shot(page, "C3a-cut-node-topology");
await tab(page, "Chaos");
await page.locator("button", { hasText: "heal" }).first().click(); await page.waitForTimeout(1500);
const oh = await lastOutcome();
sm = await allReady(); await sleep(5000); sm = await summary();
const shotB = await shot(page, "C3b-heal-node");
await tab(page, "Topology"); await shot(page, "C3b-heal-node-topology");
row("C3b", "heal: the estate recovers and the UI shows it (no stale red)", "Chaos 'heal'", "heal the node cut", "20 ready; no failed edges; the cut's `failed` facts shown as recovered, not red", `${oh.slice(0, 100)}; ${JSON.stringify(sm.by)}; edges ${JSON.stringify(sm.edges)}`, sm.by["ready-for-traffic"] === 20 && !(sm.edges.failed > 0) && !(sm.edges.unheard > 0), shotB);

// ---- C4 cut a peer mesh, heal
await tab(page, "Chaos");
const tb0 = await traffic();
const bb0 = await bbLinks();
await page.locator(".card", { hasText: "Peer meshes" }).locator("button", { hasText: "cut mesh from the rest" }).first().click(); await page.waitForTimeout(2000);
const om = await lastOutcome();
await until("mesh unheard", async () => { const s = await summary(); return s.by["ready-for-traffic"] <= 12 ? s : null; }, 120000, 2000);
await sleep(5000);
sm = await summary(); const tb1 = await traffic();
await tab(page, "Chaos"); const shotC = await shot(page, "C4a-cut-mesh2");
await tab(page, "Topology"); await page.waitForTimeout(2500); const shotC2 = await shot(page, "C4a-cut-mesh2-topology");
const bb1 = await bbLinks();
const cutMeshNow = (await cuts())[0];
row("C4a", "R-U1(5): network cut of a peer mesh; Luke: after a mesh2 cut the backbone link turns red", "Chaos 'cut mesh from the rest'", "cut mesh2", "applied; mesh2 members unheard; every backbone link failed; traffic to mesh2 gets typed non-Reply outcomes", `${om.slice(0, 160)}; statuses ${JSON.stringify(sm.by)}; backbone links before ${JSON.stringify(bb0.map((e) => e.state))} during ${JSON.stringify(bb1.map((e) => e.state))}; cut members ${cutMeshNow?.members.length}; traffic outcomes during ${JSON.stringify(Object.fromEntries(Object.entries(tb1.by_outcome).map(([k, v]) => [k, v - (tb0.by_outcome[k] ?? 0)])))}`, /applied/.test(om) && bb1.length > 0 && bb1.every((e) => e.state === "failed") && cutMeshNow?.members.length === 10, shotC2);

// ---- C5 birth during the cut
await tab(page, "Topology");
await page.locator("[data-testid=mesh-select]").selectOption("mesh2");
await page.locator(".spawn-bar button", { hasText: "+ broker" }).click(); await page.waitForTimeout(1500);
const spawnMsg = await page.locator("[data-testid=spawn-msg]").innerText().catch(() => "");
const joined = await until("newborn in cut", async () => { const c = (await cuts())[0]; return c && c.members.length === 11 ? c : null; }, 120000, 1500);
const alertsTxt = JSON.stringify((await api("/api/alerts")).json.alerts.slice(0, 6));
const shotD = await shot(page, "C5a-birth-during-cut");
const newborn = joined ? joined.members.find((m) => m === "mesh2.broker.4") : null;
row("C5a", "coordinator defect: a mesh cut holds for members born while it stands", "Chaos cut + spawn bar '+ broker' on mesh2", "cut mesh2, then + broker on mesh2", "the cut's members grow to include the newborn (mesh2.broker.4) and its ports are in the rule set", `${spawnMsg}; cut members ${joined ? joined.members.length : "not grown"} newborn=${newborn}; ports ${joined ? JSON.stringify(joined.ports) : ""}; chaos alert present: ${/born while it stands/.test(alertsTxt)}`, !!newborn && /born while it stands/.test(alertsTxt), shotD);
// the rule: newborn's UDP ports are in the iptables chain
const { execSync } = await import("node:child_process");
let chain = ""; try { chain = execSync("sudo -n iptables -S | grep -c RAFKA-PART || true", { encoding: "utf8" }).trim(); } catch (e) { chain = String(e).slice(0, 80); }
const newbornPorts = joined ? joined.ports.length : 0;
await sleep(20000);
const pendingNew = (await nodes()).nodes.find((n) => n.name === "mesh2.broker.4");
const bld = (await api("/api/builds")).json;
row("C5b", "birth during a cut does not converge until healed (cut holds), proving the cut covers the newborn", "Topology / Builds", "wait 20s with the cut in force", "mesh2.broker.4 is not ready-for-traffic while cut", `status ${pendingNew?.status ?? "absent"}; iptables chain rule lines ${chain}`, !pendingNew || pendingNew.status !== "ready-for-traffic", await shot(page, "C5b-newborn-held"));

// heal
await tab(page, "Chaos");
await page.locator("button", { hasText: "heal" }).first().click(); await page.waitForTimeout(1500);
const oh2 = await lastOutcome();
sm = await allReady(21); await sleep(8000); sm = await summary();
const bb2 = await bbLinks();
await tab(page, "Topology"); await page.waitForTimeout(2500); const shotE = await shot(page, "C4b-healed-topology");
row("C4b", "heal of a mesh cut: every node ready, backbone links live again (connected, or recovered and still drawn as the gold backbone link), the birth held by the cut converges", "Chaos 'heal'", "heal the mesh2 cut", "21 ready (the newborn converged); backbone links connected; no failed edges", `${oh2.slice(0, 80)}; ${JSON.stringify(sm.by)}; backbone ${JSON.stringify(bb2.map((e) => e.state))}; edges ${JSON.stringify(sm.edges)}`, sm.by["ready-for-traffic"] === 21 && bb2.length > 0 && bb2.every((e) => e.state === "connected" || e.state === "recovered") && !(sm.edges.failed > 0) && !(sm.edges.unheard > 0), shotE);
const tb2 = await traffic();
row("C4c", "traffic resumes after the heal", "traffic line", "compare Reply counts", "Reply keeps rising after the heal", `Reply ${tb1.by_outcome.Reply ?? 0} -> ${tb2.by_outcome.Reply ?? 0}`, (tb2.by_outcome.Reply ?? 0) > (tb1.by_outcome.Reply ?? 0));

// ---- C6 refusals
let r = await api("/api/chaos/fault", "POST", { node: "mesh1.admin.1", action: "stop" });
row("C6a", "fault aimed at the fabric primary is refused 403 by name", "POST /api/chaos/fault", "stop mesh1.admin.1", "403 fabric-primary-is-never-a-target", `${r.status} ${r.json?.detail?.refusal}`, r.status === 403 && r.json?.detail?.refusal === "fabric-primary-is-never-a-target");
r = await api("/api/chaos/fault", "POST", { node: "mesh1.admin.1", action: "kill" });
row("C6b", "kill of the fabric primary refused", "POST /api/chaos/fault", "kill mesh1.admin.1", "403", `${r.status}`, r.status === 403);
r = await api("/api/chaos/cut", "POST", { scope: "mesh", target: "mesh1" });
row("C6c", "cut of the mesh holding the fabric primary refused", "POST /api/chaos/cut", "cut mesh1", "403 by name", `${r.status} ${r.json?.detail?.refusal}`, r.status === 403 && r.json?.detail?.refusal === "fabric-primary-is-never-a-target");
r = await api("/api/chaos/cut", "POST", { scope: "node", target: "mesh1.admin.1" });
row("C6d", "cut of the fabric primary node refused", "POST /api/chaos/cut", "cut node mesh1.admin.1", "403", `${r.status}`, r.status === 403);
r = await api("/api/chaos/fault", "POST", { node: "mesh9.broker.1", action: "stop" });
row("C6e", "unknown node refused by name", "POST /api/chaos/fault", "stop mesh9.broker.1", "200 record refused unknown-node", `${r.status} ${r.json?.outcome} ${r.json?.detail?.refusal}`, r.json?.outcome === "refused" && r.json?.detail?.refusal === "unknown-node");
r = await api("/api/chaos/fault", "POST", { node: "mesh1.broker.1", action: "explode" });
row("C6f", "unknown action refused by name", "POST /api/chaos/fault", "explode", "refused unknown-action", `${r.json?.detail?.refusal}`, r.json?.detail?.refusal === "unknown-action");
r = await api("/api/chaos/heal", "POST", { id: 9999 });
row("C6g", "heal of no cut refused by name", "POST /api/chaos/heal", "heal 9999", "refused no-such-cut", `${r.json?.detail?.refusal}`, r.json?.detail?.refusal === "no-such-cut");
await tab(page, "Chaos"); await shot(page, "C6-outcomes");
await browser.close();
