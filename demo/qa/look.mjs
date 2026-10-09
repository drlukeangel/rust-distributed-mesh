import { open, tab, shot } from "./lib.mjs";
const { browser, page } = await open();
await page.waitForTimeout(3000);
for (const t of ["Topology","Nodes","Messages"]) { await tab(page, t); console.log(await shot(page, "look-" + t.replace(" ", "-"))); }
await browser.close();
