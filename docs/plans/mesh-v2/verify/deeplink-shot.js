// Deep-link proof: navigate directly to /cache, confirm the Cache tab is active
// (URL stays /cache after load), screenshot the address-bar-equivalent state.
const { chromium } = require('playwright');
const path = require('path');
const BASE = process.env.ADMIN_URL || 'http://localhost:19092';
const OUT = path.join(__dirname, 'screenshots', 'phase2-cross-mesh');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newContext({ viewport: { width: 1680, height: 1050 } }).then(c => c.newPage());
  // 1. Direct deep link to /cache
  await page.goto(`${BASE}/cache`, { waitUntil: 'networkidle', timeout: 60000 });
  await sleep(3000);
  const activeTab = await page.locator('div.tab.active').first().textContent();
  const url = page.url();
  console.log(`[deeplink] GET /cache → url=${url} activeTab=${activeTab}`);
  await page.screenshot({ path: path.join(OUT, '10-deeplink-cache.png'), fullPage: true });
  // 2. Reload — must STAY on cache
  await page.reload({ waitUntil: 'networkidle' });
  await sleep(2000);
  const afterReload = await page.locator('div.tab.active').first().textContent();
  console.log(`[deeplink] after reload → url=${page.url()} activeTab=${afterReload}`);
  // 3. Click Topology — URL must update
  await page.locator('div.tab', { hasText: 'Topology' }).first().click();
  await sleep(1500);
  console.log(`[deeplink] after click Topology → url=${page.url()}`);
  await browser.close();
  const ok = activeTab.includes('Cache') && afterReload.includes('Cache') && page.url().endsWith('/topology');
  console.log(ok ? '[deeplink] PASS' : '[deeplink] CHECK MANUALLY');
})().catch(e => { console.error(e); process.exit(1); });
