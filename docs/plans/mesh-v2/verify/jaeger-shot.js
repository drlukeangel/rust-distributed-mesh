// Jaeger UI capture for sprint-13 proof.
// Usage: TRACE_ID=<id> node jaeger-shot.js <out-subdir>
// Captures: System Architecture (DAG) dependency graph, a produce trace waterfall,
// and the service-filtered search page. Saves PNGs into screenshots/<out-subdir>/.
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const JAEGER = process.env.JAEGER_URL || 'http://localhost:16686';
const TRACE_ID = process.env.TRACE_ID || '';
const SUB = process.argv[2] || 'sprint13-telemetry';
const OUT = path.join(__dirname, 'screenshots', SUB);
fs.mkdirSync(OUT, { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const browser = await chromium.launch();
  const page = await (await browser.newContext({ viewport: { width: 1680, height: 1050 } })).newPage();
  page.on('console', (m) => console.log(`  [page] ${m.type()}: ${m.text()}`.slice(0, 200)));

  // 1. System Architecture — DAG dependency graph.
  await page.goto(`${JAEGER}/dependencies`, { waitUntil: 'networkidle', timeout: 60000 });
  await sleep(2500);
  // Jaeger defaults to "Force Directed Graph"; click the "DAG" tab if present for a clean layout.
  try {
    const dag = page.locator('text=DAG').first();
    if (await dag.count()) { await dag.click(); await sleep(2500); }
  } catch (e) { console.log('  [dag] no DAG toggle: ' + e.message); }
  await page.screenshot({ path: path.join(OUT, 'jaeger-1-system-architecture.png'), fullPage: true });
  console.log('  [ok] system-architecture');

  // 2. Produce trace waterfall.
  if (TRACE_ID) {
    await page.goto(`${JAEGER}/trace/${TRACE_ID}`, { waitUntil: 'networkidle', timeout: 60000 });
    await sleep(3500);
    await page.screenshot({ path: path.join(OUT, 'jaeger-2-produce-trace.png'), fullPage: true });
    console.log('  [ok] produce-trace ' + TRACE_ID);
  }

  // 3. Service search page (left-rail shows distinct per-mesh services).
  await page.goto(`${JAEGER}/search?service=mesh2.broker&lookback=1h`, { waitUntil: 'networkidle', timeout: 60000 });
  await sleep(3000);
  await page.screenshot({ path: path.join(OUT, 'jaeger-3-service-search.png'), fullPage: true });
  console.log('  [ok] service-search');

  await browser.close();
})().catch((e) => { console.error('fatal', e); process.exit(1); });
