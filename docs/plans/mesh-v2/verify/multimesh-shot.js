// Playwright proof harness for the MULTI-MESH phase.
// Usage:  node multimesh-shot.js
// Env:    ADMIN_A (default http://localhost:19090)  ADMIN_B (default http://localhost:19091)
//
// Proves, through the live admin consoles, that TWO separately-launched node-admins
// (sharing one root CA) each render BOTH meshes on the Topology tab:
//   - own mesh  : full per-node detail (gossip)
//   - other mesh: from the backbone directory (only the admin bridges it now)
// Asserts both mesh group labels (mesh1 + mesh2) are present in each console.

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const A = process.env.ADMIN_A || 'http://localhost:19090';
const B = process.env.ADMIN_B || 'http://localhost:19091';
const OUT = path.join(__dirname, 'screenshots', 'multimesh');
fs.mkdirSync(OUT, { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function shoot(page, url, label) {
  await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });
  await sleep(2000);
  const tab = page.locator('div.tab', { hasText: 'Topology' }).first();
  if ((await tab.count()) > 0) { await tab.click(); await sleep(3500); }
  // The topology renders one react-flow group node per mesh, plus a label node
  // whose text is the mesh id. Collect all node text to assert both meshes show.
  const allText = await page.$$eval('.react-flow__node', (els) => els.map((e) => e.textContent || ''));
  const blob = allText.join(' | ');
  const hasMesh1 = /mesh1/.test(blob);
  const hasMesh2 = /mesh2/.test(blob);
  await page.screenshot({ path: path.join(OUT, `${label}.png`), fullPage: true });
  console.log(`  [${label}] mesh1 visible=${hasMesh1}  mesh2 visible=${hasMesh2}`);
  return hasMesh1 && hasMesh2;
}

(async () => {
  console.log(`[multimesh-shot] A=${A} B=${B} out=${OUT}`);
  const browser = await chromium.launch();
  const ctx = await browser.newContext({ viewport: { width: 1680, height: 1050 } });
  const page = await ctx.newPage();
  const aOk = await shoot(page, A, 'mm-1-admin-a-mesh1-console');
  const bOk = await shoot(page, B, 'mm-2-admin-b-mesh2-console');
  await browser.close();
  const pass = aOk && bOk;
  console.log('');
  console.log(`  admin-a console shows BOTH meshes: ${aOk ? 'YES' : 'NO'}`);
  console.log(`  admin-b console shows BOTH meshes: ${bOk ? 'YES' : 'NO'}`);
  console.log(`  RESULT: ${pass ? 'PASS — two admins, shared root, each renders both meshes' : 'FAIL'}`);
  process.exit(pass ? 0 : 1);
})().catch((e) => { console.error('[multimesh-shot] fatal:', e); process.exit(2); });
