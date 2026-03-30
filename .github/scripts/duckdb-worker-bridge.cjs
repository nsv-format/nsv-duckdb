// Bridge between Node.js worker_threads and duckdb-wasm's Web Worker protocol.
// duckdb-wasm's worker uses globalThis.onmessage / globalThis.postMessage.
// Node.js worker_threads uses parentPort.on('message') / parentPort.postMessage.
const { parentPort } = require("worker_threads");

globalThis.postMessage = (data, transfer) =>
  parentPort.postMessage(data, transfer);

parentPort.on("message", (data) => {
  if (globalThis.onmessage) globalThis.onmessage({ data });
});

require("@duckdb/duckdb-wasm/dist/duckdb-node-eh.worker.cjs");
