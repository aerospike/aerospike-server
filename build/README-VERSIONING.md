# Release version strings (`build/version`, `build/gen_version`)

## Semver release tags (current)

- **`build/version`** is the single source of truth for all version and
  build-number output. It **always** anchors on the bare `x.y.z.w` tag via
  `--match '[0-9]*.[0-9]*.[0-9]*.[0-9]*'` (never the `v…` ship tag); if no bare
  tag is reachable it falls back to the short SHA, never `v…`. Three modes:

  | Invocation | master / hotfix/* | dev branches |
  |---|---|---|
  | `bash build/version` *(default)* | `w.x.y.z-n` | full `git describe` with SHA |
  | `bash build/version -v` | `w.x.y.z` (bare) | full `git describe` with SHA |
  | `bash build/version -r` | build number `n` | `1` |

  Where `n` is the count of first-parent commits since the bare `x.y.z.w`
  next-release marker tag (see below).

- **`build/gen_version`** emits the compiled-in build-id (`aerospike_build_id`).
  It derives the version string by calling `bash build/version -v` (bare, which
  gives `w.x.y.z` on release lines) — the single source of truth — rather than
  reimplementing the `git describe` logic, and adds only the build SHA, telemetry,
  and the C-header fields. The running server therefore reports the bare `w.x.y.z`
  marketing version throughout the dev cycle; the build number `-n` is *not*
  compiled in. The JFrog build-id published by the deploy workflow uses the full
  `w.x.y.z-n` instead (see Callers) so each commit maps to a unique, non-overwritten
  JFrog coordinate.

### Callers

The packaging Makefiles and deploy workflows call `build/version` with explicit
flags to get exactly the string they need:

- `pkg/deb/Makefile`, `pkg/rpm/Makefile`: `build/version -v` for `REV`,
  `build/version -r` for `BUILD_NUMBER`.
- `pkg/src/Makefile`: `build/version -v` for the source-archive name.
- `.github/workflows/tag-release.yaml`: `bash build/version -v` to derive the
  bare release tag.
- `.github/workflows/sign-build-deploy.yaml`: `bash build/version -v` for the
  `release` / `version` output used in artifact and bundle naming.
- `.github/workflows/build-sign-deploy.yaml`: `bash build/version` (default,
  full `w.x.y.z-n`) for the `setup.VERSION` output, which becomes the JFrog
  `jf-build-id`. The full build number keeps each commit's build-id unique so a
  later run for a different commit does not overwrite an existing JFrog build.

### `build_number` and the `tag-release` "both" sequence

1. **Release** (same commit): Git tag **`vX.Y.Z.W`** (with leading `v`).
2. **Empty commit** then Git tag **`X.Y.Z.(W+1)`** — **bare**, no `v` — marks
   the **start of the next** release line; `build/version -r` counts commits
   **only** from this tag (`X.Y.Z.(W+1)..HEAD` on first-parent).
3. The `v…` release tag is **ignored** for the build number while a newer bare
   tag exists on the path: `git describe … --match '[0-9]*.[0-9]*.[0-9]*.[0-9]*'`
   only matches bare tags, so the pretag on `HEAD` is chosen.

If only a **`v…`** release tag exists (e.g. `release-only`, or before the bare
pretag is pushed), the script's second `describe` (`--match 'v[0-9]*…'`) anchors
on that tag until the bare next-release tag appears.

## Migration from `x.y.z.w-start` tags

Historically some lines used annotated tags matching `*.*.*.*-start` as the
"line open" marker. That convention is **replaced** by normal release tags for
version resolution:

- After you cut a **numeric release tag** on a line, ensure `bash build/version -v`
  on `master` / `hotfix/*` returns the expected `x.y.z.w`.
- Feature branches resolve via `--match '[0-9]*.[0-9]*.[0-9]*.[0-9]*'` (and
  `--long`), keeping the `-<n>-g<sha>` suffix; there is no unrestricted-describe
  fallback (which could pick the `v…` ship tag), so a missing bare tag degrades
  to the short SHA.
- If a repo still has only `-start` tags and no semver release tag yet,
  `build/version -r` may return `1` until the first matching release tag
  exists — plan the first tag accordingly.

## Operational references

- Tagging across three repos: `.github/workflows/tag-release.yaml` (maintainer
  notes at top of file).
