# Release version strings (`build/version`, `build/gen_version`, `build/build_number`)

## Semver release tags (current)

- **`build/version`** prints a **bare** four-part line `x.y.z.w` (no `v`) for packaging and CI inputs. That is independent of **Git** tag names from `tag-release`: the workflow creates a **`v`‑prefixed** annotated tag for the release and a **bare** tag for the start of the next release (see below).
- **`build/gen_version`** must stay **aligned** with `build/version` for non-`/work` builds (same `git describe` strategy). On `master` / `hotfix/*`, both strip the `git describe --long` suffix with `sed 's/-[0-9]*-g[0-9a-f]*$//'` (same as CI workflows) so hyphenated tags (e.g. `8.0.0.1-rc1`) are not truncated.
- **`build/build_number`** counts first-parent commits since the **bare** `x.y.z.w` **next-release** tag when one exists on the branch (that is the tag on the empty commit after a `both` run). It does **not** use the `v…` release tag for the count once the bare tag exists. If there is no bare tag yet, it falls back to the nearest `vx.y.z.w` tag (e.g. `release-only`).

### `build_number` and the `tag-release` “both” sequence

1. **Release** (same commit): Git tag **`vX.Y.Z.W`** (with leading `v`).
2. **Empty commit** then Git tag **`X.Y.Z.(W+1)`** — **bare**, no `v` — marks the **start of the next** release line; `build/build_number` counts commits **only** from this tag (`X.Y.Z.(W+1)..HEAD` on first-parent).
3. The `v…` release tag is **ignored** for `build_number` while a newer bare tag exists on the path: `git describe … --match '[0-9]*.[0-9]*.[0-9]*.[0-9]*'` only matches bare tags, so the pretag on `HEAD` is chosen.

If only a **`v…`** release tag exists (e.g. `release-only`, or before the bare pretag is pushed), the script’s second `describe` (`--match 'v[0-9]*…'`) anchors on that tag until the bare next-release tag appears.

## Migration from `x.y.z.w-start` tags

Historically some lines used annotated tags matching `*.*.*.*-start` as the “line open” marker. That convention is **replaced** by normal release tags for version resolution:

- After you cut a **numeric release tag** on a line, ensure `bash build/version` on `master` / `hotfix/*` returns the expected `x.y.z.w`.
- Feature branches still resolve via `--match '[0-9]*.[0-9]*.[0-9]*.[0-9]*'` (and `--long`) so an exact tag does not collapse to a bare four-part string without the `*-*-*` guard path.
- If a repo still has only `-start` tags and no semver release tag yet, `build/build_number` may fall back to `1` until the first matching release tag exists—plan the first tag accordingly.

## Operational references

- Tagging across three repos: `.github/workflows/tag-release.yaml` (maintainer notes at top of file).
