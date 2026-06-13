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

Coverage minimum: 67% (enforced in CI).

<!-- BEGIN GENERATED: test-markers -->
### Test Markers

| Marker | Description |
|--------|-------------|
| `@pytest.mark.slow` | marks tests as slow (deselect with '-m "not slow"') |
| `@pytest.mark.network` | marks tests requiring network access (deselect with '-m "not network"') |
| `@pytest.mark.integration` | marks end-to-end integration tests |
<!-- END GENERATED: test-markers -->

## Code Quality

All handled by pre-commit:

```bash
uv run pre-commit run --all-files
```

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
