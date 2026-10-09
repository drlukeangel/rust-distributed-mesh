import { open, tab, shot, api, row, sleep, summary, nodes, until } from "./lib.mjs";
const { browser, page } = await open();
page.setDefaultTimeout(150000);
await page.waitForTimeout(3000);
const settled = (n) => until(`settled ${n}`, async () => { const s = await summary(); return s.nodes === n && s.by["ready-for-traffic"] === n ? s : null; }, 420000, 3000);
const msg = async () => { await page.waitForTimeout(900); return (await page.locator("[data-testid=spawn-msg]").innerText().catch(() => "")); };
const setMesh = async (m) => { await page.locator("[data-testid=mesh-select]").selectOption(m); };

for (const kind of ["node_admin", "gateway", "broker", "compute"]) {
  await tab(page, "Topology");
  await setMesh("mesh2");
  let m = "";
  if ((await summary()).nodes === 20 || true) { await page.locator(".spawn-bar button", { hasText: `+ ${kind}` }).click(); m = await msg(); }
  const shotA = await shot(page, `S-add-${kind}-clicked`);
  const s = await settled(21);
  const shotB = await shot(page, `S-add-${kind}-settled`);
  row(`S-${kind}`, `spawn bar: + ${kind} adds one ${kind} to the mesh`, "spawn bar '+ " + kind + "'", `mesh=mesh2, click + ${kind}`, "Build accepted (message), then 21 nodes all ready-for-traffic", `${m} -> ${s ? JSON.stringify(s.by) + " nodes " + s.nodes : "NOT settled in 7 min"}`, !!s && /build bld-/.test(m), shotB);
  // remove it through the Nodes tab
  const t = await nodes();
  const mine = t.nodes.filter((n) => n.mesh === "mesh2" && n.kind === kind).map((n) => n.name).sort();
  const victim = mine[mine.length - 1];
  await tab(page, "Nodes");
  await page.locator(".grid-cards .card").filter({ has: page.getByText(victim, { exact: true }) }).first().locator("button", { hasText: "remove" }).click();
  await page.waitForTimeout(800);
  const rm = await page.locator("main .card.mono").first().innerText().catch(() => "");
  const s2 = await settled(20);
  const shotC = await shot(page, `S-remove-${kind}-settled`);
  row(`R-${kind}`, `Nodes tab: remove submits a RemoveNode Build for ${victim}`, "Nodes card 'remove'", `click remove on ${victim}`, "Build accepted, node gone, 20 nodes ready", `${rm} -> ${s2 ? "20 ready; has victim: " + (await nodes()).nodes.some((n) => n.name === victim) : "NOT settled"}`, !!s2 && /build bld-/.test(rm), shotC);
}

// bootstrap
await tab(page, "Topology");
await page.locator(".spawn-bar button", { hasText: "bootstrap 2-mesh" }).click();
const bm = await msg(); const sb = await settled(20); const shotBoot = await shot(page, "S-bootstrap");
row("S-bootstrap", "spawn bar: bootstrap reconciles to the canonical shape x2", "'bootstrap R-shape x2'", "click while canonical", "Build accepted, shape unchanged (20 ready)", `${bm} -> ${sb ? "20 ready" : "NOT settled"}`, !!sb && /build bld-/.test(bm), shotBoot);

// restart a broker
const before = (await nodes()).nodes.find((n) => n.name === "mesh1.broker.2");
await tab(page, "Nodes");
await page.locator(".grid-cards .card").filter({ has: page.getByText("mesh1.broker.2", { exact: true }) }).first().locator("button", { hasText: "restart" }).click();
await page.waitForTimeout(800);
const rmsg = await page.locator("main .card.mono").first().innerText().catch(() => "");
const after = await until("restarted", async () => { const n = (await nodes()).nodes.find((x) => x.name === "mesh1.broker.2"); return n && n.status === "ready-for-traffic" && n.incarnation_id !== before.incarnation_id ? n : null; }, 240000, 2000);
const shotR = await shot(page, "N2-restart");
row("N2", "Nodes tab: restart = RestartNode of the same node, new incarnation", "Nodes card 'restart'", "restart mesh1.broker.2", "Build accepted; same node_id, new incarnation, ready", `${rmsg} -> ${after ? `node_id same: ${after.node_id === before.node_id}; incarnation ${before.incarnation_id.slice(0, 8)} -> ${after.incarnation_id.slice(0, 8)}` : "NOT reborn"}`, !!after && after.node_id === before.node_id, shotR);

// + mesh / remove mesh
await tab(page, "Topology");
await page.locator(".spawn-bar button", { hasText: "+ mesh" }).click();
const mm = await msg();
const s3 = await settled(30); const shotM = await shot(page, "S-mesh3-created");
const kinds = {}; for (const n of (await nodes()).nodes.filter((n) => n.mesh === "mesh3")) kinds[n.kind] = (kinds[n.kind] ?? 0) + 1;
row("S-mesh", "spawn bar: + mesh creates a mesh in the canonical per-mesh shape", "'+ mesh'", "mesh=mesh3, click + mesh", "Build accepted; mesh3 holds node_admin 2, gateway 3, broker 3, compute 2; 30 ready", `${mm} -> ${s3 ? "30 ready" : "NOT settled"}; mesh3 ${JSON.stringify(kinds)}`, !!s3 && kinds.node_admin === 2 && kinds.gateway === 3 && kinds.broker === 3 && kinds.compute === 2, shotM);
await page.waitForTimeout(3500); await setMesh("mesh3");
await page.locator(".spawn-bar button", { hasText: "remove mesh" }).click();
const rmm = await msg(); const s4 = await settled(20); const shotRM = await shot(page, "S-mesh3-removed");
row("S-rmmesh", "spawn bar: remove mesh retires the whole mesh", "'remove mesh'", "mesh=mesh3, click remove mesh", "Build accepted; mesh3 gone; 20 ready", `${rmm} -> ${s4 ? "20 ready" : "NOT settled"}`, !!s4 && /build bld-/.test(rmm), shotRM);
await browser.close();
