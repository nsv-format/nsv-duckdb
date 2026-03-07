# Release CI Audit

Audited `.github/workflows/MainDistributionPipeline.yml` against `duckdb/extension-ci-tools@v1.4.2`.

## Artifact Naming: MATCH

The build job (`_extension_distribution.yml`) names artifacts as:
```
{extension_name}-{duckdb_version}-extension-{duckdb_arch}{artifact_postfix}
```

With `extension_name=nsv`, `duckdb_version=v1.4.2`, and no postfix, this produces:
```
nsv-v1.4.2-extension-linux_amd64
nsv-v1.4.2-extension-linux_arm64
nsv-v1.4.2-extension-linux_amd64_musl
nsv-v1.4.2-extension-osx_amd64
nsv-v1.4.2-extension-osx_arm64
nsv-v1.4.2-extension-windows_amd64
nsv-v1.4.2-extension-windows_arm64
nsv-v1.4.2-extension-windows_amd64_mingw
nsv-v1.4.2-extension-wasm_mvp
nsv-v1.4.2-extension-wasm_eh
nsv-v1.4.2-extension-wasm_threads
```

The release job download pattern `nsv-v1.4.2-extension-*` matches all of these. **OK.**

## Release Rename Logic: OK

```bash
arch=$(basename "$dir" | sed 's/nsv-v1.4.2-extension-//')
cp "$file" "release/nsv-${arch}.${ext}"
```

This correctly strips the prefix and produces e.g. `nsv-linux_amd64.duckdb_extension`. **OK.**

## Potential Issues

1. **`softprops/action-gh-release@v1`** — v1 is deprecated in favor of v2. Not blocking but should be updated. v1 uses the Node 16 runtime which GitHub is phasing out.

2. **Rust cross-compilation for all platforms** — The build uses `extra_toolchains: rust`. Some platforms (windows_arm64, wasm variants) may fail to cross-compile Rust. No evidence these have been tested. The first actual release will reveal any platform-specific build failures.

3. **No `exclude_archs` or `build_duckdb_shell`** — The workflow doesn't filter any platforms. If the Rust static library can't cross-compile for wasm or windows_arm64, those artifact uploads will fail silently (they just won't exist), and the release job will skip them (the `for file in "$dir"*` loop handles missing gracefully).

4. **Never triggered** — No tags exist in the repo. The release job has never run. First release should be a `-rc` tag to test the pipeline.

## Recommendation

1. Push a `v0.1.0-rc1` tag to test the full release pipeline
2. Update `softprops/action-gh-release` from v1 to v2
3. After first release, verify which platforms actually produced artifacts
