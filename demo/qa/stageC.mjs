import { open, tab, shot, api, row, sleep, summary, nodes, until } from "./lib.mjs";
const { browser, page } = await open();
page.setDefaultTimeout(120000);
await page.waitForTimeout(3000);
const card = (name) => page.locator(".grid-cards .card").filter({ has: page.getByText(name, { exact: true }) }).first();
const st = async (name) => (await nodes()).nodes.find((n) => n.name === name)?.status;
const allReady = async (n = 20) => until("all ready", async () => { const s = await summary(); return s.nodes === n && s.by["ready-for-traffic"] === n ? s : null; }, 240000, 2500);
const lastOutcome = async () => (await page.locator("[data-testid=fault-outcome]").first().innerText().catch(() => "")).replace(/\s+/g, " ");
const traffic = async () => (await api("/api/traffic")).json;

await tab(page, "Chaos");
let s = await shot(page, "C0-chaos-tab");
const fpName = (await nodes()).nodes.find((n) => n.is_fabric_primary).name;
const nbtn = await page.locator(".grid-cards .card button").count();
const fpText = await card(fpName).innerText();
const meshRows = await page.locator("text=/never cut|cut mesh from the rest/").allInnerTexts();
row("C0", "R-U1(5): Chaos tab per node stop/continue/kill/cut; the fabric primary is never a target", "Chaos", "open the tab", "19 cards with 4 buttons each, the FP card says never a fault target; mesh1 (holds FP) never cut, mesh2 cuttable",
  `${nbtn} buttons; FP ${fpName}: "${fpText.replace(/\n/g, " ")}"; mesh rows: ${meshRows.join("; ")}`, nbtn === 19 * 4 && /never a fault target/.test(fpText) && meshRows.some((x) => /never cut/.test(x)) && meshRows.some((x) => /cut mesh from the rest/.test(x)), s);

// C1 stop / continue a broker
const T = "mesh1.broker.2";
const t0 = await traffic();
await card(T).locator("button", { hasText: "stop" }).click();
await page.waitForTimeout(1500);
const o1 = await lastOutcome();
const pend = await until("pending", async () => (await st(T)) !== "ready-for-traffic" ? await st(T) : null, 90000, 1500);
s = await shot(page, "C1-stop-broker-pending");
const sumDuring = await summary();
const edgesAtT = sumDuring.t.edges.filter((e) => e.source === T || e.destination === T).map((e) => e.state);
row("C1a", "R-U1(5): stop = SIGSTOP of the exact runtime, typed outcome; node-admin marks it", "Chaos 'stop'", `stop ${T}`, "outcome applied with state_after T; node leaves ready-for-traffic", `${o1.slice(0, 220)} -> status ${pend}; edges at ${T}: ${JSON.stringify(edgesAtT)}`, /applied/.test(o1) && /"state_after":"T"/.test(o1) && !!pend, s);
await tab(page, "Chaos");
await card(T).locator("button", { hasText: "continue" }).click(); await page.waitForTimeout(1500);
const o2 = await lastOutcome();
const back = await until("ready again", async () => (await st(T)) === "ready-for-traffic", 240000, 2000);
const sumAfter = await allReady();
s = await shot(page, "C1b-continue-recovered");
row("C1b", "R-U1(5): continue releases the hold; the same birth is back ready", "Chaos 'continue'", `continue ${T}`, "outcome applied; 20 nodes ready; edges no red", `${o2.slice(0, 160)} -> ${back ? "ready" : "NOT ready"}; ${sumAfter ? JSON.stringify(sumAfter.edges) : "estate not settled"}`, /applied/.test(o2) && !!back && !!sumAfter && !(sumAfter.edges.failed > 0), s);

// C2 kill a compute
const K = "mesh2.compute.2";
const idBefore = (await nodes()).nodes.find((n) => n.name === K).incarnation_id;
await tab(page, "Chaos");
await card(K).locator("button", { hasText: "kill" }).click(); await page.waitForTimeout(1500);
const o3 = await lastOutcome();
s = await shot(page, "C2a-kill-compute");
const after = await until("recovered or dead", async () => { const n = (await nodes()).nodes.find((x) => x.name === K); return n && n.status === "ready-for-traffic" && n.incarnation_id !== idBefore ? n : null; }, 150000, 3000);
const stK = await st(K);
row("C2", "R-U1(5): kill = SIGKILL of the exact runtime, typed outcome; recovery observed", "Chaos 'kill'", `kill ${K}`, "outcome applied exited; the estate's own drift recovery or a Nodes-tab restart brings a new birth ready", `${o3.slice(0, 200)} -> after 150s: status ${stK}${after ? " (reborn by node-admin, new incarnation)" : ""}`, /applied/.test(o3) && /"exited":true/.test(o3), s);
if (!after) {
  await tab(page, "Nodes");
  await card(K).locator("button", { hasText: "restart" }).click();
  const re = await until("restarted", async () => { const n = (await nodes()).nodes.find((x) => x.name === K); return n && n.status === "ready-for-traffic" && n.incarnation_id !== idBefore ? n : null; }, 240000, 3000);
  const sm = await allReady();
  row("C2b", "recovery of a killed node: restart through the rectifier", "Nodes 'restart'", `restart ${K}`, "new incarnation, 20 ready", re && sm ? "reborn, 20 ready" : "NOT recovered", !!re && !!sm, await shot(page, "C2b-restart-after-kill"));
}
await browser.close();
