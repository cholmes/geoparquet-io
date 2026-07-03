# Contributing

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

## Setup

```bash
git clone https://github.com/geoparquet/geoparquet-io.git
cd geoparquet-io
uv sync --all-extras
uv run pre-commit install
```

## Tests

```bash
uv run pytest                                          # Full suite
uv run pytest tests/test_yourfile.py -v               # Single file
uv run pytest -m "not slow and not network"           # Fast tests only
```

CI runs three test tiers:

- **Fast tests** (`not slow and not network`) — run on every PR across the full
  OS/Python matrix and **block merging**.
- **Slow tests** (`slow and not network`) — run after merge to main and nightly;
  opt in on a PR by adding the `run-slow-tests` label.
- **Network tests** (`network`) — hit live third-party services (ArcGIS, WFS,
  Carto, ...). They are **non-blocking**: failures open/update a tracking issue
  instead of failing CI, because a remote server's behavior is not something a
  PR author can fix. They run serially with per-test timeouts, retries, and
  process isolation (`--forked`).

Coverage gates (both enforced in CI):

- Global floor: 67% total coverage.
- **Changed lines: 90%** — `diff-cover` checks that the lines your PR touches
  are covered *by fast tests*. New code ships with tests that run on every PR,
  not only in the post-merge slow suite.

<!-- BEGIN GENERATED: test-markers -->
### Test Markers

| Marker | Description |
|--------|-------------|
| `@pytest.mark.slow` | marks tests as slow (deselect with '-m "not slow"') |
| `@pytest.mark.network` | marks tests requiring network access (deselect with '-m "not network"') |
| `@pytest.mark.integration` | marks end-to-end integration tests |
<!-- END GENERATED: test-markers -->

## Code Quality

`.pre-commit-config.yaml` is the **single source of truth** for every quality
rule. CI runs the exact same hooks (`pre-commit run --all-files`, plus the
pre-push stage), so a check can never pass locally and fail in CI for a
different rule set — and skipping local hook install just moves the same
failure to CI.

```bash
uv run pre-commit run --all-files                        # commit-stage hooks
uv run pre-commit run --all-files --hook-stage pre-push  # deptry, vulture,
                                                         # xenon, mypy,
                                                         # import-linter, menard
```

Two ratchet/ignore files gate quality over time:

- `.pip-audit-ignores` — self-expiring CVE ignore list shared by the CI
  security job and the daily security audit. Each entry needs an expiry date
  and a reason; expired entries automatically re-fail CI.
- `.mutation-baseline` — minimum mutation kill rate enforced by the nightly
  mutation-testing run. Ratchet it up as test quality improves.

Branch protection is code too: `scripts/apply_branch_protection.sh` declares
the required status checks and review rules as GitHub rulesets (run once by an
admin after changes).

## Documentation

```bash
uv run mkdocs serve
```

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): message`

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

## Architecture

New CLI commands need corresponding Python API:

1. Core logic in `geoparquet_io/core/<feature>.py`
2. CLI wrapper in `geoparquet_io/cli/main.py`
3. Python API in `geoparquet_io/api/table.py` and `api/ops.py`

See `CLAUDE.md` for full architecture details.

### External extractors

Extractors that page through a remote service (`wfs`, `arcgis`, `carto`, `bigquery`) keep
re-discovering the same edge cases. Reuse the shared schema helpers in `core/common.py`
(`_compute_unified_schema`, `_cast_table_to_schema`, `_promote_numeric_type`) rather than
hand-rolling schema reconciliation — the `forbid-bespoke-schema-reconciliation` pre-commit hook
enforces this. Known edge cases to handle:

- **Pagination**: detect server `startIndex`/page-size limits; supply a stable sort
  (`sortBy`/`orderByFields`) for layers without a primary key.
- **Schema across pages**: never trust the first page's inferred schema. Where a source has an
  authoritative schema (ArcGIS layer metadata), cast each page to it via `_cast_table_to_schema`.
  Where it does not (WFS), unify with `_compute_unified_schema` (handles int/float/decimal mixes,
  uint64 overflow).
- **Empty/null responses**: handle empty result sets and null properties without crashing.
- **CRS/SR**: normalize WKID/SR; resolve native WKT → EPSG; unset CRS for unresolvable codes; honor
  `--output-crs` by reprojecting when the server returns a different CRS.
- **Network**: retry transient errors; surface upstream stderr.

## Releasing

(Maintainers only)

Uses commitizen + automated CI workflow:

```bash
# 1. Create bump PR (updates version + changelog)
uv run cz bump --changelog
git push origin HEAD

# 2. Open PR, merge to main
# 3. CI automatically: creates tag, publishes to PyPI, creates GitHub Release
```

**Recovery** (if release fails after merge):
```bash
git push origin :refs/tags/vX.Y.Z  # Delete orphan tag
gh workflow run release.yml --ref main  # Retry
```

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
