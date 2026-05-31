// Playwright screenshot harness for the rafka admin-ui.
// Usage:  node screenshot.js <phase-label>
// Env:    ADMIN_URL (default http://localhost:19090)
//
// Drives each tab in the live admin-ui and saves a full-page PNG per tab into
// screenshots/<phase-label>/. This is the durable per-phase proof the PRD requires:
// the admin-ui is the proof, and the proof is captured, not just watched.

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const PHASE = process.argv[2] || 'phase0-baseline';
const BASE = process.env.ADMIN_URL || 'http://localhost:19090';
const OUT = path.join(__dirname, 'screenshots', PHASE);
fs.mkdirSync(OUT, { recursive: true });

// The admin-ui is a React/react-flow SPA (web/dist). Tabs are <div class="tab">
// clicked by their visible text. Order matches the rendered tab bar.
const TABS = [
  ['Topology', '1-topology'],
  ['Nodes', '2-nodes'],
  ['Messages', '3-messages'],
  ['Boot Waterfall', '4-boot-waterfall'],
  ['Timeline', '5-timeline'],
  ['Alerts', '6-alerts'],
  ['Chaos', '7-chaos'],
  ['Tests', '8-tests'],
  ['Cache', '9-cache'],
];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  console.log(`[screenshot] phase=${PHASE} url=${BASE} out=${OUT}`);
  const browser = await chromium.launch();
  const ctx = await browser.newContext({ viewport: { width: 1680, height: 1050 } });
  const page = await ctx.newPage();
  page.on('console', (m) => console.log(`  [page console] ${m.type()}: ${m.text()}`));

  await page.goto(BASE, { waitUntil: 'networkidle', timeout: 60000 });
  await sleep(2500); // let the initial fetches settle

  const results = [];
  for (const [tabText, label] of TABS) {
    try {
      const tab = page.locator('div.tab', { hasText: tabText }).first();
      if ((await tab.count()) === 0) {
        console.log(`  [skip] no tab for "${tabText}"`);
        continue;
      }
      await tab.click();
      await sleep(3500); // let the panel's fetch + render complete
      const file = path.join(OUT, `${label}.png`);
      await page.screenshot({ path: file, fullPage: true });
      console.log(`  [ok] ${label} -> ${file}`);
      results.push(label);
    } catch (e) {
      console.log(`  [err] ${label}: ${e.message}`);
    }
  }

  await browser.close();
  console.log(`[screenshot] captured ${results.length} tabs: ${results.join(', ')}`);
})().catch((e) => {
  console.error('[screenshot] fatal:', e);
  process.exit(1);
});
