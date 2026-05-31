// Capture the raw Jaeger UI (localhost:16686) as telemetry proof for a sprint.
// Usage:  node jaeger-shot.js <out-dir> <service> [operation]
// Env:    JAEGER_URL (default http://localhost:16686)
//
// Navigates Jaeger's search deep-link for the given service (+ optional operation),
// waits for the trace list to render, and saves search-results + first-trace PNGs.

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const OUT = process.argv[2] || path.join(__dirname, 'screenshots', 'jaeger');
const SERVICE = process.argv[3] || 'broker';
const OPERATION = process.argv[4] || '';
const BASE = process.env.JAEGER_URL || 'http://localhost:16686';
fs.mkdirSync(OUT, { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  let url = `${BASE}/search?service=${encodeURIComponent(SERVICE)}&lookback=1h&limit=20`;
  if (OPERATION) url += `&operation=${encodeURIComponent(OPERATION)}`;
  console.log(`[jaeger] ${url} -> ${OUT}`);
  const browser = await chromium.launch();
  const ctx = await browser.newContext({ viewport: { width: 1680, height: 1050 } });
  const page = await ctx.newPage();
  await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });
  await sleep(4000); // let the search fire + results render

  const results = path.join(OUT, 'jaeger-search.png');
  await page.screenshot({ path: results, fullPage: true });
  console.log(`  [ok] search results -> ${results}`);

  // Open the first trace (top result) for the span-waterfall detail.
  try {
    const firstTrace = page.locator('a:has-text("rafka.mesh")').first();
    if ((await firstTrace.count()) > 0) {
      await firstTrace.click();
      await sleep(4000);
      const trace = path.join(OUT, 'jaeger-trace.png');
      await page.screenshot({ path: trace, fullPage: true });
      console.log(`  [ok] trace detail -> ${trace}`);
    } else {
      console.log('  [warn] no trace links found in results');
    }
  } catch (e) {
    console.log(`  [warn] trace detail: ${e.message}`);
  }
  await browser.close();
})().catch((e) => { console.error('[jaeger] fatal:', e); process.exit(1); });
