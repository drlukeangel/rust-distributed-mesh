// One-off DOM probe: dump the rendered tab/nav structure of the live admin-ui.
const { chromium } = require('playwright');
const BASE = process.env.ADMIN_URL || 'http://localhost:19090';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto(BASE, { waitUntil: 'networkidle', timeout: 60000 });
  await sleep(3000);
  const info = await page.evaluate(() => {
    const out = { title: document.title, buttons: [], roleTabs: [], navLinks: [] };
    document.querySelectorAll('button').forEach((b) => {
      out.buttons.push({ text: (b.textContent || '').trim().slice(0, 40), cls: b.className, attrs: Array.from(b.attributes).map((a) => `${a.name}=${a.value}`).join(' ') });
    });
    document.querySelectorAll('[role="tab"]').forEach((b) => out.roleTabs.push({ text: (b.textContent || '').trim().slice(0, 40), cls: b.className }));
    document.querySelectorAll('nav a, .tab, [class*="tab"]').forEach((b) => out.navLinks.push({ tag: b.tagName, text: (b.textContent || '').trim().slice(0, 40), cls: b.className }));
    return out;
  });
  console.log(JSON.stringify(info, null, 2));
  await browser.close();
})().catch((e) => { console.error(e); process.exit(1); });
