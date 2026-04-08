# Claude Code Instructions for geoparquet-io

This file contains project-specific instructions for Claude Code when working in this repository.

## Project Overview

geoparquet-io (gpio) is a Python CLI tool for fast I/O and transformation of GeoParquet files. It uses Click for CLI, PyArrow and DuckDB for data processing, and follows modern Python packaging standards.

**Entry point**: `gpio` command defined in `geoparquet_io/cli/main.py`

---

## Documentation Structure

### context/ Directory
Contains reference documentation for AI developers:

- **context/shared/documentation/** - Durable docs (geometry handling, platform issues)

Other subdirectories (`plans/`, `reports/`, `research/`) exist for future use but are currently empty.

---

## Before Writing Code: Research First

**Always research before implementing.** Before any code changes:

1. **Understand the request** - Ask clarifying questions if ambiguous
2. **Search for patterns** - Check if similar functionality exists (`grep -r "pattern"`)
3. **Check utilities** - Review `core/common.py` and `cli/decorators.py` first
4. **Identify affected files** - Map out what needs to change
5. **Review existing tests** - Look at tests for the area you're modifying
6. **Plan documentation** - Identify docs needing updates

**Key questions:** Does this exist partially? What utilities can I reuse? How do similar features handle errors? What's the test coverage expectation?

---

## Test-Driven Development (MANDATORY)

**YOU MUST USE TDD. NO EXCEPTIONS.** Unless the user explicitly says "skip tests":

1. **WRITE TESTS FIRST** - Before ANY implementation code
2. **RUN TESTS** - Verify they fail with `uv run pytest`
3. **IMPLEMENT** - Minimal code to pass tests
4. **RUN TESTS AGAIN** - Verify they pass
5. **ADD EDGE CASES** - Test error conditions

**VIOLATING TDD IS UNACCEPTABLE.** Every feature needs tests FIRST.

---

## Architecture & Key Files

```
geoparquet_io/
├── cli/
│   ├── main.py          # All CLI commands (~5400 lines)
│   ├── decorators.py    # Reusable Click options - CHECK FIRST
│   └── fix_helpers.py   # Check --fix helpers
├── core/                # 52 specialized modules
│   ├── common.py        # Shared utilities (~4000 lines) - CHECK FIRST
│   ├── add_*.py         # Add column implementations
│   ├── partition_*.py   # Partitioning implementations
│   ├── check_*.py       # Validation implementations
│   └── logging_config.py
└── api/
    ├── table.py         # Table class with all operations
    ├── ops.py           # Functional API
    ├── check.py         # Validation API
    ├── pipeline.py      # Pipeline operations
    └── stac.py          # STAC metadata API
```

<!-- freshness: last-verified: 2026-03-31, maps-to: geoparquet_io/cli/main.py -->
<!-- BEGIN GENERATED: cli-commands -->
### CLI Command Groups

| Command Group | Subcommands | Description |
|---------------|-------------|-------------|
| `gpio add` | a5, admin-divisions, bbox, bbox-metadata, h3, kdtree, quadkey, s2 | Commands for enhancing GeoParquet files in various ways |
| `gpio benchmark` | compare, report, suite | Benchmark GeoParquet performance |
| `gpio check` | all, bbox, compression, row-group, spatial, spec, stac | Check GeoParquet files for best practices |
| `gpio convert` | csv, flatgeobuf, geojson, geopackage, geoparquet, reproject, shapefile | Convert between formats and coordinate systems |
| `gpio extract` | arcgis, bigquery, geoparquet, wfs | Extract data from files and services to GeoParquet |
| `gpio inspect` | head, meta, stats, summary, tail | Inspect GeoParquet files and show metadata, previews, or statistics |
| `gpio partition` | a5, admin, h3, kdtree, quadkey, s2, string | Commands for partitioning GeoParquet files |
| `gpio publish` | stac, upload | Commands for publishing GeoParquet data (STAC metadata, cloud uploads) |
| `gpio sort` | column, hilbert, quadkey | Commands for sorting GeoParquet files |
<!-- END GENERATED: cli-commands -->

<!-- BEGIN GENERATED: core-modules -->
### Core Modules

| Module | Purpose | Lines |
|--------|---------|-------|
| `common.py` |  | 4036 |
| `validate.py` | GeoParquet file validation against specification r... | 2854 |
| `inspect_utils.py` | Utilities for inspecting GeoParquet files. | 1548 |
| `convert.py` |  | 1369 |
| `duckdb_metadata.py` | DuckDB-based Parquet metadata extraction. | 1277 |
| `arcgis.py` | ArcGIS Feature Service to GeoParquet conversion. | 1226 |
| `extract.py` | Extract columns and rows from GeoParquet files. | 1225 |
| `wfs.py` | WFS (Web Feature Service) to GeoParquet conversion... | 1193 |
| `metadata_utils.py` | Utilities for extracting and formatting GeoParquet... | 1077 |
| `extract_bigquery.py` |  | 1044 |
| `partition_common.py` |  | 908 |
| `admin_datasets.py` |  | 735 |
| `benchmark.py` | Benchmark utilities for comparing GeoParquet conve... | 701 |
| `partition_admin_hierarchical.py` |  | 698 |
| `upload.py` | Upload GeoParquet files to cloud object storage. | 675 |
| ... | *37 more modules* | |
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
## Dependencies Quick Reference

```python
# DuckDB with extensions
from geoparquet_io.core.common import get_duckdb_connection, needs_httpfs
con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(file_path))

# Logging (not click.echo!)
from geoparquet_io.core.logging_config import success, warn, error, info, debug, progress

# Remote files
from geoparquet_io.core.common import is_remote_url, remote_write_context, setup_aws_profile_if_needed
```

---

<!-- freshness: last-verified: 2026-03-30, maps-to: pyproject.toml -->
## Testing with uv

```bash
# Fast tests only (recommended for development)
uv run pytest -n auto -m "not slow and not network"

# Specific test
uv run pytest tests/test_extract.py::TestParseBbox::test_valid_bbox -v

# With coverage
uv run pytest --cov=geoparquet_io --cov-report=term-missing
```

<!-- BEGIN GENERATED: test-markers -->
### Test Markers

| Marker | Description |
|--------|-------------|
| `@pytest.mark.slow` | marks tests as slow (deselect with '-m "not slow"') |
| `@pytest.mark.network` | marks tests requiring network access (deselect with '-m "not network"') |
| `@pytest.mark.integration` | marks end-to-end integration tests |
<!-- END GENERATED: test-markers -->

- **Coverage requirement**: 75% minimum (enforced), 80% for new code

---

## Git Workflow

### Commits
- **One line, imperative mood**: "Add feature" not "Added feature"
- Start with verb: Add, Fix, Update, Remove, Refactor
- No emoji, no period, no Claude footer

### Pull Requests
- Update relevant guide in `docs/guide/`
- Update `docs/api/python-api.md` if API changed
- Include both CLI and Python examples
- Follow PR template

---

## Code Quality

```bash
# Before committing (all handled by pre-commit)
pre-commit run --all-files

# Or manually
uv run ruff check --fix .
uv run ruff format .
uv run xenon --max-absolute=A geoparquet_io/  # Aim for A grade
```

**Complexity reduction:**
- Extract helper functions
- Use early returns (guard clauses)
- Dictionary dispatch over long if-elif
- Max 30-40 lines per function

---

## Quick Checklist for New Features

1. [ ] Core logic in `core/<feature>.py` with `*_table()` function
2. [ ] CLI wrapper in `cli/main.py` using decorators
3. [ ] Python API in `api/table.py` and `api/ops.py`
4. [ ] Tests in `tests/test_<feature>.py` and `tests/test_api.py`
5. [ ] Documentation in `docs/guide/<feature>.md` with CLI/Python tabs
6. [ ] Complexity grade A (`xenon --max-absolute=A`)
7. [ ] Coverage >80% for new code

---

## Debugging

```bash
# Inspect file structure
gpio inspect file.parquet --verbose

# Check metadata (note: 'meta' is a subcommand of 'inspect')
gpio inspect meta file.parquet --json

# Dry-run with SQL
gpio extract input.parquet output.parquet --dry-run --show-sql
```

For Windows: Always close DuckDB connections explicitly, use UUID in temp filenames.

---

## Claude Hooks & Permissions

### Automatic Command Approvals
The project uses smart command auto-approval patterns. Commands are automatically approved when they follow safe patterns with common wrappers.

**Safe wrapper patterns** (automatically stripped and approved):
- `uv run <command>` - Package manager execution
- `timeout <seconds> <command>` - Time-limited execution
- `.venv/bin/<command>` - Virtual environment commands
- `nice <command>` - Priority adjustment
- Environment variables: `ENV_VAR=value <command>`

**Safe core commands** (auto-approved after wrapper stripping):
- **Testing**: `pytest`, `pre-commit`, `ruff`, `xenon`
- **Git**: All git operations including `add`, `commit`, `push`
- **GitHub**: `gh pr`, `gh issue`, `gh api`
- **Build tools**: `make`, `cargo`, `npm`, `yarn`, `pip`, `uv`
- **Read-only**: `ls`, `cat`, `grep`, `find`, `head`, `tail`
- **Project CLI**: `gpio` (all subcommands)

**Example auto-approvals**:
```bash
uv run pytest -n auto                    # ✅ Auto-approved
timeout 60 uv run pytest tests/          # ✅ Auto-approved
.venv/bin/gpio convert input.parquet     # ✅ Auto-approved
SKIP=xenon pre-commit run --all-files    # ✅ Auto-approved
```

Commands with dangerous patterns (command substitution `$(...)`, backticks) are always rejected for safety.

### Custom Permission Overrides
For commands not covered by patterns, add to `.claude/settings.local.json`:
```json
{
  "permissions": {
    "allow": [
      "Bash(custom-command:*)",
      "WebFetch(domain:example.com)"
    ]
  }
}
```

### PreToolUse Hooks
The project includes command modification hooks in `.claude/settings.local.json`:

```json
"hooks": {
  "PreToolUse": [
    {
      "matcher": "Bash",
      "hooks": [{
        "type": "command",
        "command": "python .claude/hooks/ensure-uv-run.py"
      }]
    }
  ]
}
```

**ensure-uv-run.py**: Automatically prefixes Python commands with `uv run`:
- `pytest` → `uv run pytest`
- `ruff check` → `uv run ruff check`
- `gpio` → `uv run gpio`

This ensures commands always use the correct virtual environment without manual intervention.

### Session Hooks
- **pre-session-hook.md**: Instructions Claude reads at session start
- Enforces documentation checks, context loading, etc.

This maintains consistency across conversations and prevents reinventing already-solved problems.

## MCP Distill Tools (Token Optimization)

| Action | Tool |
|--------|------|
| Read code for exploration | `mcp__distill__smart_file_read filePath="file.py"` |
| Extract function/class | `mcp__distill__smart_file_read filePath="file.py" target={"type":"function","name":"fn"}` |
| Compress build/test output | `mcp__distill__auto_optimize content="<large output>"` |
| Multi-step operations | `mcp__distill__code_execute code="return ctx.files.glob('**/*.py')"` |
| Before editing files | Use native `Read` tool (Edit requires Read first) |

**code_execute SDK** (`ctx`): `ctx.files.{read,glob}`, `ctx.compress.{auto,logs}`, `ctx.code.{skeleton,extract}`, `ctx.search.{grep,symbols}`
