import { open, tab, shot } from "./lib.mjs";
const { browser, page } = await open();
await tab(page, "Nodes");
const c = page.locator(".grid-cards .card", { hasText: /mesh2\.admin\.3\b/ });
console.log(await c.count());
console.log((await page.locator(".grid-cards .card").allInnerTexts()).map(x=>x.split("\n")[0]).join(" "));
await browser.close();
