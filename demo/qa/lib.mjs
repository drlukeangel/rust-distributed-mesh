// Shared QA helpers: headless Chromium against the live UI, one screenshot per checklist row.
import { createRequire } from "node:module";
const require = createRequire("/tmp/claude-1000/-home-admin-rafka-v2/cdaf5d3c-9da9-474f-8c23-804393ba8652/scratchpad/pw.YoeH/package.json");
export const { chromium } = require("playwright");
export const BASE = process.env.UI ?? "http://127.0.0.1:19090";
export const SHOTS = new URL("./shots/", import.meta.url).pathname;
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export async function open() {
  const browser = await chromium.launch({ executablePath: process.env.CHROME ?? '/usr/bin/google-chrome', args: ['--no-sandbox'] });
  const page = await browser.newPage({ viewport: { width: 1700, height: 1000 } });
  await page.goto(BASE);
  return { browser, page };
}
export const tab = async (page, name) => { await page.locator(".tab", { hasText: new RegExp(`^${name}$`) }).click(); await sleep(2500); };
export const shot = async (page, name) => { const p = `${SHOTS}${name}.png`; await page.screenshot({ path: p }); return p; };
export const api = async (path, method = "GET", body) => {
  const r = await fetch(BASE + path, { method, headers: { "Content-Type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
  const text = await r.text();
  let json = null; try { json = JSON.parse(text); } catch {}
  return { status: r.status, json, text };
};
import fs from "node:fs";
export const RESULTS = new URL("./results.jsonl", import.meta.url).pathname;
export function row(id, requirement, where, action, expected, observed, pass, screenshot = "") {
  const r = { id, requirement, where, action, expected, observed, pass, screenshot };
  fs.appendFileSync(RESULTS, JSON.stringify(r) + "\n");
  console.log(`${pass ? "PASS" : "FAIL"} ${id}: ${observed}`);
  return r;
}
export const nodes = async () => (await api("/api/topology")).json;
export async function until(what, fn, ms = 120000, every = 1500) {
  const t0 = Date.now();
  for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await sleep(every); }
}
export const summary = async () => {
  const t = await nodes();
  const by = {};
  for (const n of t.nodes) by[n.status] = (by[n.status] ?? 0) + 1;
  const es = {};
  for (const e of t.edges) es[e.state] = (es[e.state] ?? 0) + 1;
  return { nodes: t.nodes.length, by, edges: es, t };
};
