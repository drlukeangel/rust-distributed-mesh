// Playwright proof harness for the REPEATER phase.
// Usage:  node repeater-shot.js
// Env:    ADMIN_A (default http://localhost:19090)  ADMIN_B (default http://localhost:19091)
//
// With the repeater running, each console should render the OTHER mesh as FULL
// cert-verified live members (intra-mesh edges) rather than the "backbone" directory
// box. Asserts both meshes are present AND neither mesh group is tagged "backbone"
// in the foreign console — i.e. the repeater has upgraded directory -> full membership.

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const A = process.env.ADMIN_A || 'http://localhost:19090';
const B = process.env.ADMIN_B || 'http://localhost:19091';
const OUT = path.join(__dirname, 'screenshots', 'repeater');
fs.mkdirSync(OUT, { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function shoot(page, url, label) {
  await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });
  await sleep(2000);
  const tab = page.locator('div.tab', { hasText: 'Topology' }).first();
  if ((await tab.count()) > 0) { await tab.click(); await sleep(3500); }
  const allText = await page.$$eval('.react-flow__node', (els) => els.map((e) => e.textContent || ''));
  const blob = allText.join(' | ');
  const hasMesh1 = /mesh1/.test(blob);
  const hasMesh2 = /mesh2/.test(blob);
  const hasBackboneTag = /backbone/i.test(blob);
  await page.screenshot({ path: path.join(OUT, `${label}.png`), fullPage: true });
  console.log(`  [${label}] mesh1=${hasMesh1} mesh2=${hasMesh2} backbone-tag-present=${hasBackboneTag}`);
  return { hasMesh1, hasMesh2, hasBackboneTag };
}

(async () => {
  console.log(`[repeater-shot] A=${A} B=${B} out=${OUT}`);
  const browser = await chromium.launch();
  const ctx = await browser.newContext({ viewport: { width: 1680, height: 1050 } });
  const page = await ctx.newPage();
  const a = await shoot(page, A, 'rep-1-admin-a-mesh1-bridged');
  const b = await shoot(page, B, 'rep-2-admin-b-mesh2-bridged');
  await browser.close();
  // Both consoles show both meshes; with the repeater bridging full membership, the
  // foreign mesh is NOT a backbone directory box anymore.
  const pass = a.hasMesh1 && a.hasMesh2 && b.hasMesh1 && b.hasMesh2 && !a.hasBackboneTag && !b.hasBackboneTag;
  console.log('');
  console.log(`  both consoles show both meshes      : ${a.hasMesh1 && a.hasMesh2 && b.hasMesh1 && b.hasMesh2 ? 'YES' : 'NO'}`);
  console.log(`  foreign mesh upgraded off "backbone": ${!a.hasBackboneTag && !b.hasBackboneTag ? 'YES' : 'NO'}`);
  console.log(`  RESULT: ${pass ? 'PASS — repeater bridges full cert-verified membership across meshes' : 'FAIL'}`);
  process.exit(pass ? 0 : 1);
})().catch((e) => { console.error('[repeater-shot] fatal:', e); process.exit(2); });
