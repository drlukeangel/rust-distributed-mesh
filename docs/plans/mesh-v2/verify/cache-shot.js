// Playwright verify + screenshot for the rafka admin-ui Caches + Channels tabs.
// Usage:  node cache-shot.js [out-dir]
// Env:    ADMIN_URL (default http://localhost:19090)
//
// This is BOTH the deliverable proof (full-page PNGs of each tab) AND a contract
// check: it asserts the exact data-testid elements the UI builder MUST emit, so a
// green run means the tabs render the real cache/channel data, not just that the
// page loaded. Exit code is non-zero if any required element is missing.
//
// ── UI element contract (the builder must produce these) ──────────────────────
//  Tab bar:   <div class="tab">Caches</div>, <div class="tab">Channels</div>
//  Caches:    [data-testid="cache-matrix"]
//             [data-testid="cache-row-<name>"]          one per cache (9)
//             [data-testid="cache-type-<name>"]         badge: shared|leader|key|key-gossip
//             [data-testid="cache-channel-<name>"]      badge: main|<dedicated>
//             [data-testid="cache-cell-<name>-<nodetype>"] with data-held="true|false"
//  Channels:  [data-testid="channels-grid"]
//             [data-testid="channel-col-<name>"]        one per channel (main, backbone, + dedicated)
//             [data-testid="channel-events-<name>"]     the stream list for a channel
//             [data-testid="channel-event"]             one per event row

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const OUT = process.argv[2] || path.join(__dirname, 'screenshots', 'caches');
const BASE = process.env.ADMIN_URL || 'http://localhost:19090';
fs.mkdirSync(OUT, { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let failures = 0;
const need = async (page, sel, label) => {
  const n = await page.locator(sel).count();
  const ok = n > 0;
  if (!ok) failures++;
  console.log(`  [${ok ? 'ok ' : 'MISS'}] ${label.padEnd(42)} ${sel}  (${n})`);
  return ok;
};

(async () => {
  console.log(`[cache-shot] url=${BASE} out=${OUT}`);
  const browser = await chromium.launch();
  const ctx = await browser.newContext({ viewport: { width: 1680, height: 1050 } });
  const page = await ctx.newPage();
  page.on('console', (m) => console.log(`    [page ${m.type()}] ${m.text()}`));
  await page.goto(BASE, { waitUntil: 'networkidle', timeout: 60000 });
  await sleep(2500);

  // ── Caches tab ──────────────────────────────────────────────────────────────
  console.log('\n[Caches tab]');
  const cachesTab = page.locator('div.tab', { hasText: 'Caches' }).first();
  if ((await cachesTab.count()) === 0) { console.log('  [MISS] no "Caches" tab'); failures++; }
  else {
    await cachesTab.click();
    await sleep(3500);
    await need(page, '[data-testid="cache-matrix"]', 'matrix present');
    await need(page, '[data-testid="cache-row-topology-key-gossip"]', 'topology row (rides main)');
    await need(page, '[data-testid="cache-row-shared-1"]', 'shared-1 row');
    // the multi-node-type shared cache must be HELD by BOTH gateway and compute:
    await need(page, '[data-testid="cache-cell-shared-1-gateway"][data-held="true"]', 'shared-1 held @ gateway');
    await need(page, '[data-testid="cache-cell-shared-1-compute"][data-held="true"]', 'shared-1 held @ compute');
    // ...and NOT held by broker (a non-owner — proves the matrix is real, not all-filled):
    await need(page, '[data-testid="cache-cell-shared-1-broker"][data-held="false"]', 'shared-1 NOT held @ broker');
    await need(page, '[data-testid="cache-type-leader-1"]', 'leader-1 type badge');
    await page.screenshot({ path: path.join(OUT, 'caches.png'), fullPage: true });
    console.log(`  [png] ${path.join(OUT, 'caches.png')}`);
  }

  // ── Channels tab ────────────────────────────────────────────────────────────
  console.log('\n[Channels tab]');
  const chTab = page.locator('div.tab', { hasText: 'Channels' }).first();
  if ((await chTab.count()) === 0) { console.log('  [MISS] no "Channels" tab'); failures++; }
  else {
    await chTab.click();
    await sleep(3500);
    await need(page, '[data-testid="channels-grid"]', 'channels grid present');
    await need(page, '[data-testid="channel-col-main"]', 'main channel column (real)');
    await need(page, '[data-testid="channel-col-shared-1"]', 'shared-1 channel column');
    await need(page, '[data-testid="channel-event"]', 'at least one live event row');
    await page.screenshot({ path: path.join(OUT, 'channels.png'), fullPage: true });
    console.log(`  [png] ${path.join(OUT, 'channels.png')}`);
  }

  await browser.close();
  console.log(`\n[cache-shot] done — ${failures} missing element(s).`);
  process.exit(failures > 0 ? 1 : 0);
})().catch((e) => { console.error('[cache-shot] fatal:', e); process.exit(1); });
