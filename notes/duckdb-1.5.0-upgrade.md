# DuckDB 1.5.0 Upgrade Notes

## Starting state
- nsv-duckdb pinned to DuckDB v1.4.2, extension-ci-tools at e6882cf (v1.4.2 era)
- CI workflow references v1.4.2 in 7 places (workflow refs, version params, artifact patterns)
- Extension code: ~410 lines C++, ~370 lines Rust FFI
- Rust FFI layer is DuckDB-independent, should need zero changes

## What was done
1. Bumped `duckdb/` submodule → v1.5.0 tag (commit 3a3967aa81)
2. Bumped `extension-ci-tools/` → v1.5.0 branch
3. Updated `.github/workflows/MainDistributionPipeline.yml` — all 7 `v1.4.2` → `v1.5.0`
4. Built with `make clean && make` — **zero code changes needed**, compiled cleanly
5. Tests: `make test` — all 71 assertions pass

## Key finding
**Zero C++ or Rust code changes were required.** The extension's API surface
(`ExtensionLoader`, `TableFunction`, `CopyFunction`, `Value::TryCastAs`,
`FileSystem`, `DataChunk`) was fully compatible between v1.4.2 and v1.5.0.

## Files changed (in nsv-duckdb)
- `duckdb/` — submodule pointer advanced to v1.5.0
- `extension-ci-tools/` — submodule pointer advanced to v1.5.0
- `.github/workflows/MainDistributionPipeline.yml` — version refs bumped

## Stable C++ API consideration
DuckDB now offers a stable C++ API wrapper (github.com/duckdb/duckdb-cpp-api)
that would eliminate per-release recompilation. However:
- It may not yet support CopyFunction or projection pushdown
- Current extension compiled cleanly with zero changes, so the pain point isn't acute
- Worth revisiting if future upgrades break the internal API

## For future Claudes
- The upgrade from v1.4.2 → v1.5.0 was trivial: submodule bumps + CI version strings only
- Build takes ~30 minutes from clean (single-threaded DuckDB compilation)
- If a future upgrade breaks, check `Value::TryCastAs` and `CopyFunction` signatures first — those are the riskiest API surfaces in this extension
- The Rust FFI layer (`rust-ffi/`) is completely DuckDB-independent and will never need changes for a DuckDB version bump
- The `extension-ci-tools` repo maintains branches per DuckDB version; always use the matching branch
