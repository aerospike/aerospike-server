# CI configuration data

## `build-matrix.json`

JSON array of objects `{ "host", "distro", "container" }` passed to `build-sign-deploy.yaml` as `strategy.matrix.include` (via `fromJson`).

**Removing** a distro, changing a runner label, or bumping a container image pin needs only this file. Hotfix branches can merge or cherry-pick such changes without touching workflow logic.

**Adding** a new distro unconditionally requires `.github/bin/install_deps.bash` (add an `install_deps_<distro>` function and a `case` arm; the build fails with `Unsupported distro` otherwise). Depending on the distro's package family and toolchain, it may also require:
- `.github/bin/build_edition.bash` — only if the distro needs a new toolchain arm (e.g. a new EL gcc-toolset version; same-family distros like a new Ubuntu release are covered by the existing `ubuntu*|debian*` glob).
- `.github/workflows/build-sign-deploy.yaml` — only if the distro needs new prereqs or build-time workarounds not covered by the existing glob patterns in the `Install prereqs` step.

Invalid JSON fails fast in the `setup` job, which runs `jq empty` on this file before the build matrix fans out (not later at the downstream `fromJson`). Still, use your editor or any JSON checker before pushing if you want an even earlier signal.
