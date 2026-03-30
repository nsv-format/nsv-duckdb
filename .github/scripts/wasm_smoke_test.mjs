#!/usr/bin/env node
//
// WASM smoke test for the nsv DuckDB extension.
// Runs in a real browser via Playwright because duckdb-wasm's extension
// loading (INSTALL/LOAD) requires browser APIs (fetch, WebAssembly).
//
// Usage: node wasm_smoke_test.mjs <artifact-dir>
//

import { createRequire } from "node:module";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "node:http";

const require = createRequire(import.meta.url);

// ── Locate extension file ──────────────────────────────────────────

const artifactDir = process.argv[2];
if (!artifactDir) {
  console.error("Usage: node wasm_smoke_test.mjs <artifact-dir>");
  process.exit(1);
}

const files = readdirSync(artifactDir);
const extFile = files.find((f) => f.startsWith("nsv"));
if (!extFile) {
  console.error("No extension file found in", artifactDir, "— contents:", files);
  process.exit(1);
}

const extBuf = readFileSync(join(artifactDir, extFile));
console.log(`Extension: ${extFile} (${extBuf.length} bytes)`);

// ── Resolve duckdb-wasm dist path ──────────────────────────────────

const dist = dirname(
  require.resolve("@duckdb/duckdb-wasm/dist/duckdb-eh.wasm"),
);

// ── HTTP server ────────────────────────────────────────────────────

let baseUrl; // set after listen

const server = createServer((req, res) => {
  const url = req.url;

  if (url === "/") {
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(buildTestPage(baseUrl));
    return;
  }

  if (url.startsWith("/dist/")) {
    const filePath = join(dist, url.slice(6));
    try {
      const data = readFileSync(filePath);
      const ct = url.endsWith(".wasm")
        ? "application/wasm"
        : url.endsWith(".js") || url.endsWith(".mjs")
          ? "application/javascript"
          : "application/octet-stream";
      res.writeHead(200, { "Content-Type": ct });
      res.end(data);
    } catch {
      res.writeHead(404);
      res.end();
    }
    return;
  }

  if (url.includes("nsv")) {
    res.writeHead(200, {
      "Content-Type": "application/octet-stream",
      "Content-Length": extBuf.length,
    });
    res.end(extBuf);
    return;
  }

  res.writeHead(404);
  res.end();
});

await new Promise((r) => server.listen(0, "127.0.0.1", r));
baseUrl = `http://127.0.0.1:${server.address().port}`;
console.log(`Server: ${baseUrl}`);

// ── Run browser via Playwright ─────────────────────────────────────

const { chromium } = await import("playwright");
const browser = await chromium.launch();
const page = await browser.newPage();

page.on("console", (msg) => console.log(`  [browser] ${msg.text()}`));
page.on("pageerror", (err) => console.error(`  [browser error] ${err.message}`));

console.log("Navigating to test page...");
await page.goto(baseUrl, { timeout: 60000, waitUntil: "domcontentloaded" });

try {
  await page.waitForFunction(
    () => {
      const t = document.getElementById("log")?.textContent || "";
      return t.includes("SMOKE_TEST_PASSED") || t.includes("SMOKE_TEST_FAILED");
    },
    { timeout: 120000 },
  );
} catch {
  console.error("Test timed out after 120s");
}

const logContent = await page.textContent("#log");
console.log("\n--- Test output ---");
console.log(logContent);

await browser.close();
server.close();

if (logContent.includes("SMOKE_TEST_PASSED")) {
  console.log("\nWASM smoke test PASSED");
  process.exit(0);
} else {
  console.error("\nWASM smoke test FAILED");
  process.exit(1);
}

// ── Test page generator ────────────────────────────────────────────

function buildTestPage(origin) {
  return `<!DOCTYPE html>
<html>
<head><title>NSV WASM Smoke Test</title></head>
<body>
<pre id="log"></pre>
<script type="module">
const log = document.getElementById("log");
function print(msg) { log.textContent += msg + "\\n"; console.log(msg); }

try {
  const duckdb = await import("/dist/duckdb-browser.mjs");
  print("duckdb-wasm imported");

  const bundle = await duckdb.selectBundle({
    eh: {
      mainModule: "/dist/duckdb-eh.wasm",
      mainWorker: "/dist/duckdb-browser-eh.worker.js",
    },
  });

  const worker = new Worker(bundle.mainWorker);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule);
  print("DuckDB instantiated");

  await db.open({ allowUnsignedExtensions: true });
  const conn = await db.connect();

  await conn.query("SET autoinstall_known_extensions=false");
  await conn.query("SET autoload_known_extensions=false");
  await conn.query("SET custom_extension_repository='${origin}'");
  await conn.query("INSTALL nsv");
  await conn.query("LOAD nsv");
  print("LOAD nsv OK");

  const nsvData = new TextEncoder().encode("greet\\nn\\n\\nhello\\n42\\n\\n");
  await db.registerFileBuffer("test.nsv", nsvData);
  const r1 = await conn.query("SELECT * FROM read_nsv('test.nsv')");
  print("read_nsv: " + JSON.stringify(r1.toArray()));

  await conn.query("COPY (SELECT 'world' AS greet, 7 AS n) TO 'out.nsv' (FORMAT nsv)");
  const r2 = await conn.query("SELECT * FROM read_nsv('out.nsv')");
  print("COPY TO: " + JSON.stringify(r2.toArray()));

  await conn.close();
  await db.terminate();
  worker.terminate();
  print("SMOKE_TEST_PASSED");
} catch (e) {
  print("SMOKE_TEST_FAILED: " + e.message);
  print(e.stack || "");
}
</script>
</body>
</html>`;
}
