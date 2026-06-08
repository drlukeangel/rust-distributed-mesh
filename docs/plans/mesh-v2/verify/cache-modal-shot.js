const { chromium } = require('playwright');
const path = require('path');
const BASE = process.env.ADMIN_URL || 'http://127.0.0.1:19090';
const OUT = path.join(__dirname, 'screenshots', 'caches');
const sleep = ms => new Promise(r => setTimeout(r, ms));
(async () => {
  const browser = await chromium.launch();
  const page = await (await browser.newContext({ viewport: { width: 1680, height: 1050 } })).newPage();
  await page.goto(BASE, { waitUntil: 'networkidle', timeout: 60000 });
  await sleep(2500);
  await page.locator('div.tab', { hasText: 'Caches' }).first().click();
  await sleep(2500);
  // click the shared-1 cache name (the multi-node-type one, has entries)
  await page.locator('[data-testid="cache-row-shared-1"] button', { hasText: 'shared-1' }).first().click();
  await sleep(1500);
  const modal = page.locator('[data-testid="cache-modal-shared-1"]');
  const present = await modal.count();
  const text = (await modal.textContent()) || '';
  const hasRows = /row-[abc]/.test(text);            // shared-1 keys are row-a/b/c
  const hasEntries = /\d+ entries/.test(text);
  console.log('modal present:', present, '| shared keys (row-a/b/c) visible:', hasRows, '| entry count shown:', hasEntries);
  await page.screenshot({ path: path.join(OUT, 'cache-modal.png'), fullPage: false });
  console.log('saved cache-modal.png');
  await browser.close();
  process.exit(present > 0 && hasRows ? 0 : 1);
})().catch(e => { console.error(e); process.exit(1); });
