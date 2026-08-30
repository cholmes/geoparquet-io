# Claude Code Instructions for geoparquet-io

## Project Overview

geoparquet-io (`gpio`) is a Python CLI for GeoParquet I/O. Entry point: `geoparquet_io/cli/main.py`

---

## Package Management

**uv only.** See `pyproject.toml` for dependencies.
```bash
uv sync --all-extras        # Install
uv run pytest               # Run commands
uv tool install geoparquet-io  # Global install
```

---

## Before Writing Code

1. Search for existing patterns (`grep -r "pattern"`)
2. Check `core/common.py` and `cli/decorators.py` first
3. Review tests for the area you're modifying

---

## Test-Driven Development (MANDATORY)

**WRITE TESTS FIRST.** Unless user says "skip tests":
1. Write failing test → 2. Implement → 3. Verify pass → 4. Add edge cases

---

## Architecture

```
geoparquet_io/
├── cli/main.py        # CLI commands (thin wrappers)
├── cli/decorators.py  # Reusable Click options
├── core/              # Business logic (52 modules)
│   └── common.py      # Shared utilities - CHECK FIRST
└── api/               # Python API (table.py, ops.py)
```

**Enforced rules** (see `.pre-commit-config.yaml`):
- `no-click-echo`: Use logger in `core/`, not `click.echo()`
- `duckdb-antipatterns`: Blocks `.fetch_arrow_table()`, `.to_arrow_table()`, `TRY_CAST.*GEOMETRY`
- `import-linter`: Core cannot import Click; API cannot import CLI
- `check-api-for-cli`: Reminds to add Python API for new CLI commands

<!-- freshness: last-verified: 2026-04-03, maps-to: geoparquet_io/cli/main.py -->
<!-- BEGIN GENERATED: cli-commands -->
### CLI Command Groups

| Command Group | Subcommands | Description |
|---------------|-------------|-------------|
| `gpio add` | a5, admin-divisions, bbox, bbox-metadata, geometry-metrics, h3, kdtree, quadkey, s2 | Commands for enhancing GeoParquet files in various ways |
| `gpio benchmark` | compare, explain, report, suite | Benchmark GeoParquet performance |
| `gpio check` | all, bbox, compression, optimization, row-group, spatial, spec, stac | Check GeoParquet files for best practices |
| `gpio convert` | csv, flatgeobuf, geojson, geopackage, geoparquet, reproject, shapefile | Convert between formats and coordinate systems |
| `gpio extract` | arcgis, bigquery, carto, geoparquet, wfs | Extract data from files and services to GeoParquet |
| `gpio inspect` | head, layers, meta, stats, summary, tail | Inspect GeoParquet files and show metadata, previews, or statistics |
| `gpio partition` | a5, admin, h3, kdtree, quadkey, s2, string | Commands for partitioning GeoParquet files |
| `gpio pmtiles` | create, pyramid | PMTiles generation commands |
| `gpio process` | aggregate, overview | Transform or reduce GeoParquet data (aggregate, overview, |
| `gpio publish` | stac, upload | Commands for publishing GeoParquet data (STAC metadata, cloud uploads) |
| `gpio skills` |  | List and access LLM skills for gpio |
| `gpio sort` | column, hilbert, quadkey, str | Commands for sorting GeoParquet files |
<!-- END GENERATED: cli-commands -->

> **S2 is unavailable in this release.** The tables above are generated from the
> Click command tree, so `s2` still appears under `gpio add` and `gpio partition`,
> but both subcommands stop with an explanation instead of running: they need the
> `geography` DuckDB community extension, which is published only up to DuckDB
> 1.5.1 while gpio requires `duckdb>=1.5.2` ([#737](https://github.com/geoparquet/geoparquet-io/issues/737)).
> The same applies to `ops.add_s2`, `Table.add_s2` and `Table.partition_by_s2`.
> Use `gpio add a5` / `gpio partition a5` instead — a hierarchical, globally-uniform
> cell index over the whole sphere. Do not suggest pinning `duckdb==1.5.1`: it is
> below the dependency floor, so `uv pip check` fails and the next `uv sync` reverts
> it. S2 returns with no gpio change once the extension is republished upstream.

<!-- BEGIN GENERATED: core-modules -->
### Core Modules

| Module | Purpose | Lines |
|--------|---------|-------|
| `common.py` |  | 4086 |
| `validate.py` | GeoParquet file validation against specification r... | 2854 |
| `inspect_utils.py` | Utilities for inspecting GeoParquet files. | 1608 |
| `convert.py` |  | 1395 |
| `duckdb_metadata.py` | DuckDB-based Parquet metadata extraction. | 1322 |
| `arcgis.py` | ArcGIS Feature Service to GeoParquet conversion. | 1226 |
| `extract.py` | Extract columns and rows from GeoParquet files. | 1225 |
| `metadata_utils.py` | Utilities for extracting and formatting GeoParquet... | 1197 |
| `wfs.py` | WFS (Web Feature Service) to GeoParquet conversion... | 1193 |
| `extract_bigquery.py` |  | 1044 |
| `partition_common.py` |  | 908 |
| `admin_datasets.py` |  | 735 |
| `partition_admin_hierarchical.py` |  | 698 |
| `upload.py` | Upload GeoParquet files to cloud object storage. | 675 |
| ... | *39 more modules* | |
<!-- END GENERATED: core-modules -->

<!-- freshness: last-verified: 2026-03-20, maps-to: geoparquet_io/core/common.py, geoparquet_io/cli/decorators.py -->
### Key Patterns

1. **CLI/Core Separation**: CLI commands are thin wrappers; business logic in `core/`
2. **Common Utilities**: Always check `core/common.py` before writing new utilities
3. **Shared Decorators**: Use existing decorators from `cli/decorators.py`
4. **Error Handling**: Use `ClickException` for user-facing errors

### Critical Rules

- **Never use `click.echo()` in `core/` modules** - Use logging helpers instead
- **Every CLI command needs a Python API** - Add to `api/table.py` (methods) and `api/ops.py` (functions)
- **All documentation needs CLI + Python examples** - Use tabbed format

---

<!-- freshness: last-verified: 2026-03-20, maps-to: geoparquet_io/core/common.py -->
## Key Imports

```python
from geoparquet_io.core.common import get_duckdb_connection, needs_httpfs
from geoparquet_io.core.logging_config import success, warn, error, info, debug
from pathlib import Path  # Prefer over os.path
```

### DuckDB 1.5 Patterns

**Enforced by `duckdb-antipatterns` pre-commit hook.** Violations fail the build.

| Old (crashes) | Correct |
|---------------|---------|
| `.fetch_arrow_table()` | `.arrow().read_all()` |
| `.to_arrow_table()` | `.arrow().read_all()` |
| `TRY_CAST(x AS GEOMETRY)` | `TRY(ST_GeomFromText(x))` |
| `f'"{col}"'` / `col.replace('"', '""')` | `quote_identifier(col)` |
| `WHERE path = '{value}'` | `_escape_sql_string(value)` |

Never hand-roll SQL escaping. `quote_identifier()` is for **identifiers**
(column/table names — doubles embedded `"`); `_escape_sql_string()` is for SQL
**string literals** (doubles embedded `'`). Both live in `core/duckdb_utils.py`
and take a RAW value — escaping is not idempotent, so escape exactly once.
Column names arrive from a file's own `geo.primary_column` and from
`--column`/`--bbox-name`, so this is an injection surface, not a style nit.

Additional patterns (not yet enforced):
- `ST_Transform(..., always_xy := true)` → `SET geometry_always_xy = true` at session level
- `apply_crs_to_parquet()` removed → use `_wrap_query_with_crs()`

---

<!-- freshness: last-verified: 2026-04-03, maps-to: pyproject.toml -->
## Testing

Config in `pyproject.toml [tool.pytest.ini_options]`.

```bash
uv run pytest -n auto -m "not slow and not network and not meta"  # Fast tests (no coverage)
uv run pytest -m meta                                             # Repo tooling checks
uv run pytest --cov=geoparquet_io --cov-report=term-missing --cov-fail-under=0  # opt into coverage
```

`--cov-fail-under=0` is needed on partial runs: `[tool.coverage.report].fail_under`
re-arms the 67% floor whenever you opt into `--cov`, and a subset never clears it.

Local runs are uninstrumented: `addopts` carries no `--cov`, so a single-file run
is fast and a partial run can't fail a whole-suite gate. The 67% floor and the 90%
diff-cover gate on changed lines are enforced in CI (the ubuntu/3.11 job in
`.github/workflows/tests.yml`), which passes the coverage flags explicitly.

The `meta` lane (codespell, commitizen, doc-sync, mutmut, mypy,
validate-claude-md, security tool checks) is excluded from the fast suite and
runs in the slow/nightly job instead. Pre-commit covers most of it locally, but
not all: commitizen is a `commit-msg`-stage hook and mutmut has no hook, so
`uv run pytest -m meta` is the only local way to check those two.

<!-- BEGIN GENERATED: test-markers -->
### Test Markers

| Marker | Description |
|--------|-------------|
| `@pytest.mark.slow` | marks tests as slow (deselect with '-m "not slow"') |
| `@pytest.mark.network` | marks tests requiring network access (deselect with '-m "not network"') |
| `@pytest.mark.integration` | marks end-to-end integration tests; runs in the fast suite unless also marked slow/network (see tests/e2e/test_integration_lane.py) |
| `@pytest.mark.corpus` | tests against the official geoparquet-testing corpus (requires git submodule) |
| `@pytest.mark.meta` | repo tooling checks, excluded from the fast suite |
| `@pytest.mark.docs_example` | a fenced example block executed out of docs/guide/*.md |
<!-- END GENERATED: test-markers -->

---

## Code Quality

**All handled by pre-commit.** See `.pre-commit-config.yaml` for full list.

| Stage | Hooks |
|-------|-------|
| commit | ruff, codespell, no-click-echo, duckdb-antipatterns, doc-sync, menard-check |
| pre-push | xenon (complexity), import-linter, deptry, vulture |

Complexity guidance: guard clauses, dictionary dispatch, max 30-40 lines/function.

---

## Git Workflow

**Commits**: Enforced by commitizen hook. Format: `type(scope): message`
**PRs**: Update `docs/guide/` and `docs/api/python-api.md` if API changed.

---

## New Feature Checklist

1. [ ] Core logic in `core/<feature>.py`
2. [ ] CLI wrapper in `cli/main.py`
3. [ ] Python API in `api/table.py` and `api/ops.py`
4. [ ] Tests in `tests/`
5. [ ] Docs in `docs/guide/`

---

## Claude Hooks

**Permissions**: See `.claude/settings.local.json`
**Global hooks**: See `~/.claude/CLAUDE.md` (approve-variants.py, rtk-rewrite.sh)

Dangerous patterns (command substitution `$(...)`, backticks) always rejected.

---

## Debugging

```bash
gpio inspect summary file.parquet --verbose
gpio inspect meta file.parquet --json
gpio extract input.parquet output.parquet --dry-run --show-sql
```
