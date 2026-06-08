// Playwright proof harness for the CERT phase (trust boundary).
// Usage:  node cert-shot.js
// Env:    ADMIN_URL (default http://localhost:19090)
//
// Proves, through the live admin-ui, that an UNCERTIFIED node is rejected:
//   1. Capture the certified mesh (N nodes) on the Topology tab.
//   2. Spawn a node with RAFKA_NO_CERT=true via the API.
//   3. Wait past PENDING_GRACE_MS (15s) so a real joiner would have appeared.
//   4. Re-capture: the uncertified node MUST be absent from the react-flow graph.
//
// react-flow renders each node as div.react-flow__node[data-id="<node.id>"], so we
// assert on the live DOM, not just a pixel diff.

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const BASE = process.env.ADMIN_URL || 'http://localhost:19090';
const OUT = path.join(__dirname, 'screenshots', 'cert');
fs.mkdirSync(OUT, { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function gotoTopology(page) {
  const tab = page.locator('div.tab', { hasText: 'Topology' }).first();
  if ((await tab.count()) > 0) { await tab.click(); await sleep(3000); }
}
async function nodeIds(page) {
  return page.$$eval('.react-flow__node', (els) =>
    els.map((e) => e.getAttribute('data-id')).filter(Boolean));
}

(async () => {
  console.log(`[cert-shot] url=${BASE} out=${OUT}`);
  const browser = await chromium.launch();
  const ctx = await browser.newContext({ viewport: { width: 1680, height: 1050 } });
  const page = await ctx.newPage();
  page.on('console', (m) => console.log(`  [page] ${m.type()}: ${m.text()}`));

  await page.goto(BASE, { waitUntil: 'networkidle', timeout: 60000 });
  await sleep(2000);
  await gotoTopology(page);

  const certified = await nodeIds(page);
  await page.screenshot({ path: path.join(OUT, 'cert-1-certified-mesh.png'), fullPage: true });
  console.log(`  [1] certified mesh: ${certified.length} nodes -> ${certified.join(', ')}`);

  // Spawn an UNCERTIFIED node directly through the admin API (same origin).
  const spawn = await page.evaluate(async (base) => {
    const r = await fetch(base + '/api/nodes/spawn', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ node_type: 'broker', mesh_id: 'certmesh', extra_env: { RAFKA_NO_CERT: 'true' } }),
    });
    return r.json();
  }, BASE);
  const noCertId = spawn.node_name;
  console.log(`  [2] spawned UNCERTIFIED node: ${noCertId} (pid ${spawn.pid})`);

  // Within the grace window it shows transiently as pending — capture that, then
  // wait it out so suppression is unambiguous (a real node would have gone live).
  await sleep(4000);
  await gotoTopology(page);
  await page.screenshot({ path: path.join(OUT, 'cert-2-nocert-pending.png'), fullPage: true });

  console.log('  [3] waiting 17s for PENDING_GRACE_MS to elapse...');
  await sleep(17000);
  await gotoTopology(page);
  const after = await nodeIds(page);
  await page.screenshot({ path: path.join(OUT, 'cert-3-nocert-rejected.png'), fullPage: true });
  console.log(`  [3] after grace: ${after.length} nodes -> ${after.join(', ')}`);

  const present = after.includes(noCertId);
  const certifiedStillThere = certified.every((id) => after.includes(id));
  console.log('');
  console.log(`  uncertified node (${noCertId}) in graph after grace : ${present ? 'YES' : 'NO'}`);
  console.log(`  all ${certified.length} certified nodes still present       : ${certifiedStillThere ? 'YES' : 'NO'}`);
  const pass = !present && certifiedStillThere && certified.length >= 5;
  console.log(`  RESULT: ${pass ? 'PASS — trust boundary rejects uncertified node' : 'FAIL'}`);

  await browser.close();
  process.exit(pass ? 0 : 1);
})().catch((e) => { console.error('[cert-shot] fatal:', e); process.exit(2); });
