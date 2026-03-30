#!/usr/bin/env node
//
// WASM smoke test for the nsv DuckDB extension.
// Loads the extension into duckdb-wasm via a local HTTP server,
// then verifies that read_nsv and COPY TO nsv work.
//
// Usage: node wasm_smoke_test.mjs <artifact-dir>
//

import * as duckdb from "@duckdb/duckdb-wasm";
import { createRequire } from "node:module";
import { Worker } from "node:worker_threads";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, resolve, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "node:http";

const require = createRequire(import.meta.url);
const __dirname = dirname(fileURLToPath(import.meta.url));

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

// ── HTTP server for extension loading ──────────────────────────────
// DuckDB INSTALL fetches from {repo}/v{version}/{platform}/{name}.duckdb_extension.wasm
// We serve the file for any request that mentions "nsv".

const server = createServer((req, res) => {
  if (req.url.includes("nsv")) {
    res.writeHead(200, {
      "Content-Type": "application/octet-stream",
      "Content-Length": extBuf.length,
    });
    res.end(extBuf);
  } else {
    res.writeHead(404);
    res.end();
  }
});

await new Promise((r) => server.listen(0, "127.0.0.1", r));
const port = server.address().port;

// ── Initialise duckdb-wasm ─────────────────────────────────────────
// duckdb-wasm's worker uses globalThis.onmessage/postMessage (Web Worker API).
// Node.js worker_threads uses parentPort. The bridge file patches both.

const dist = dirname(
  require.resolve("@duckdb/duckdb-wasm/dist/duckdb-eh.wasm"),
);
const bridgePath = resolve(__dirname, "duckdb-worker-bridge.cjs");

const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
const worker = new Worker(bridgePath);
worker.addEventListener = (type, fn) =>
  worker.on(type, type === "message" ? (data) => fn({ data }) : fn);
worker.removeEventListener = (type, fn) => worker.off(type, fn);
const db = new duckdb.AsyncDuckDB(logger, worker);
await db.instantiate(resolve(dist, "duckdb-eh.wasm"));
await db.open({ allowUnsignedExtensions: true });

const conn = await db.connect();

// ── Load extension ─────────────────────────────────────────────────

await conn.query(
  `SET custom_extension_repository = 'http://127.0.0.1:${port}'`,
);
await conn.query("INSTALL nsv");
await conn.query("LOAD nsv");
console.log("Extension loaded");

// ── Smoke test: read_nsv ───────────────────────────────────────────

const nsv = "greet\nn\n\nhello\n42\n\n";
await db.registerFileBuffer("test.nsv", new TextEncoder().encode(nsv));

const r1 = await conn.query("SELECT * FROM read_nsv('test.nsv')");
const rows1 = r1.toArray();
console.log("read_nsv:", JSON.stringify(rows1));
assert(rows1.length === 1, `read_nsv: expected 1 row, got ${rows1.length}`);

// ── Smoke test: COPY TO nsv ───────────────────────────────────────

await conn.query(
  "COPY (SELECT 'world' AS greet, 7 AS n) TO 'out.nsv' (FORMAT nsv)",
);
const r2 = await conn.query("SELECT * FROM read_nsv('out.nsv')");
const rows2 = r2.toArray();
console.log("COPY TO round-trip:", JSON.stringify(rows2));
assert(rows2.length === 1, `COPY TO: expected 1 row, got ${rows2.length}`);

// ── Cleanup ────────────────────────────────────────────────────────

await conn.close();
await db.terminate();
worker.terminate();
server.close();

console.log("\nWASM smoke test PASSED");

function assert(cond, msg) {
  if (!cond) {
    console.error("ASSERTION FAILED:", msg);
    process.exit(1);
  }
}
