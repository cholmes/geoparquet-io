# gpio process aggregate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `gpio process aggregate` command that reduces a large GeoParquet (single file, glob, or partition) into a small GeoParquet/Parquet of spatial buckets (a5 grid cells or admin regions) carrying per-bucket statistics, for low-zoom visualization.

**Architecture:** A new `process` CLI group with an `aggregate` subgroup and `a5`/`admin` subcommands, mirroring `gpio partition`. A shared `core/process/aggregate/common.py` parses the metric/breakdown spec and builds the `GROUP BY` SQL; `by_a5.py` and `by_admin.py` assemble scheme-specific key + geometry SQL around it and write output via the existing `write_geoparquet_table`. Each scheme also gets a Python API (Table method + ops function).

**Tech Stack:** Python 3, Click, DuckDB 1.5 (+ `spatial` and community `a5` extensions), PyArrow, uv, pytest.

## Global Constraints

- Package manager: **uv only**. Run tools via `uv run` (e.g. `uv run pytest`).
- **Never use `click.echo()` in `core/`** — use `from geoparquet_io.core.logging_config import success, warn, error, info, debug`.
- **Core may not import Click; API may not import CLI** (enforced by import-linter). Raise domain exceptions (`InvalidParameterError` from `core/exceptions.py`) in core; translate to `ClickException`/`UsageError` in `cli/main.py`.
- DuckDB 1.5 patterns: use `.arrow().read_all()` (never `.fetch_arrow_table()`/`.to_arrow_table()`); never `TRY_CAST(x AS GEOMETRY)`. Set `SET geometry_always_xy = true;` at session level rather than `always_xy :=`.
- **TDD**: write the failing test first, watch it fail, implement minimal code, watch it pass, commit.
- Commit messages: commitizen format `type(scope): message`, ending with:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
- Fast test invocation: `uv run pytest -n auto -m "not slow and not network"`. Tests needing the `a5` community extension or admin data download are marked `@pytest.mark.slow` and/or `@pytest.mark.network`.
- Coverage minimum 67% (enforced) — keep tests meaningful.
- Work happens on branch `feature/process-aggregate` (already created).

---

## File Structure

**Create:**
- `geoparquet_io/core/process/__init__.py` — new package marker.
- `geoparquet_io/core/process/aggregate/__init__.py` — package marker.
- `geoparquet_io/core/process/aggregate/common.py` — metric/breakdown spec parsing, SQL builders, column-name sanitization + collision handling, shared constants.
- `geoparquet_io/core/process/aggregate/by_a5.py` — a5 aggregation (file + table entry points).
- `geoparquet_io/core/process/aggregate/by_admin.py` — admin aggregation (file entry point).
- `tests/test_process_aggregate_common.py` — unit tests for common.py.
- `tests/test_process_aggregate_a5.py` — a5 end-to-end tests.
- `tests/test_process_aggregate_admin.py` — admin end-to-end tests.
- `docs/guide/process-aggregate.md` — user guide.

**Modify:**
- `geoparquet_io/cli/main.py` — add `process` group, `aggregate` subgroup, `a5`/`admin` commands.
- `geoparquet_io/api/table.py` — add `Table.aggregate_a5(...)` and `Table.aggregate_admin(...)`.
- `geoparquet_io/api/ops.py` — add `aggregate_a5(...)` and `aggregate_admin(...)`.
- `docs/api/python-api.md` — document new API.
- `CHANGELOG.md` (root) — add entry (doc-sync copies to `docs/CHANGELOG.md`).

**Reference (do not modify, read for patterns):**
- `geoparquet_io/core/add/a5.py` — a5 key SQL (`a5_lonlat_to_cell`), BLOB→geometry handling, table pattern.
- `geoparquet_io/core/partition/admin_hierarchical.py` — admin spatial-join pattern, `_setup_admin_dataset`, `dataset.prepare_data_source(con)`.
- `geoparquet_io/core/partition/auto_resolution.py` — `calculate_auto_resolution(...)`.
- `geoparquet_io/core/common.py:1690` — `write_geoparquet_table(...)`.
- `geoparquet_io/cli/main.py:5172` — `partition_a5` command (option/flow template).

---

## Task 1: `process` group + `aggregate` subgroup skeleton

**Files:**
- Modify: `geoparquet_io/cli/main.py`
- Test: `tests/test_process_aggregate_a5.py` (create with the CLI-tree test)

**Interfaces:**
- Produces: a `process` Click group registered on `cli`, and an `aggregate` Click group registered on `process`. Subcommands `a5`/`admin` are added in later tasks.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_process_aggregate_a5.py
from click.testing import CliRunner

from geoparquet_io.cli.main import cli


def test_process_aggregate_group_exists():
    runner = CliRunner()
    result = runner.invoke(cli, ["process", "aggregate", "--help"])
    assert result.exit_code == 0
    assert "aggregate" in result.output.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_process_aggregate_a5.py::test_process_aggregate_group_exists -v`
Expected: FAIL — `process` is not a command.

- [ ] **Step 3: Add the groups in `cli/main.py`**

Find an existing `@cli.group()` block (e.g. the `partition` group near line 4593) and add, following the same style:

```python
@cli.group()
@click.pass_context
def process(ctx):
    """Transform or reduce GeoParquet data (aggregate, ...)."""
    pass


@process.group(name="aggregate")
@click.pass_context
def process_aggregate(ctx):
    """Aggregate features into spatial buckets with per-bucket statistics.

    Reduces large datasets into a small file of grid cells or admin regions,
    each carrying a count and optional metric/breakdown columns, for low-zoom
    visualization. Subcommands choose the bucketing scheme.
    """
    pass
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_process_aggregate_a5.py::test_process_aggregate_group_exists -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add geoparquet_io/cli/main.py tests/test_process_aggregate_a5.py
git commit -m "feat(process): add process aggregate command group skeleton

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Metric spec parsing (`parse_metrics`, `build_metric_select`)

**Files:**
- Create: `geoparquet_io/core/process/__init__.py` (empty)
- Create: `geoparquet_io/core/process/aggregate/__init__.py` (empty)
- Create: `geoparquet_io/core/process/aggregate/common.py`
- Test: `tests/test_process_aggregate_common.py`

**Interfaces:**
- Produces:
  - `VALID_METRIC_FUNCS: set[str]` = `{"sum", "avg", "min", "max"}`
  - `VALID_OUT_GEOMETRY: set[str]` = `{"polygon", "centroid", "both", "none"}`
  - `@dataclass MetricSpec(func: str, column: str, output_name: str)`
  - `parse_metrics(metric_str: str | None) -> list[MetricSpec]`
  - `build_metric_select(metrics: list[MetricSpec]) -> str` — returns the comma-joined aggregate expressions (no leading comma), or `""` if empty.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_process_aggregate_common.py
import pytest

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate.common import (
    MetricSpec,
    build_metric_select,
    parse_metrics,
)


def test_parse_metrics_empty():
    assert parse_metrics(None) == []
    assert parse_metrics("") == []


def test_parse_metrics_func_and_bare():
    specs = parse_metrics("sum:area_ha, avg:yield, population")
    assert specs == [
        MetricSpec("sum", "area_ha", "sum_area_ha"),
        MetricSpec("avg", "yield", "avg_yield"),
        MetricSpec("sum", "population", "sum_population"),  # bare -> sum
    ]


def test_parse_metrics_rejects_unknown_func():
    with pytest.raises(InvalidParameterError):
        parse_metrics("median:area_ha")


def test_parse_metrics_rejects_missing_column():
    with pytest.raises(InvalidParameterError):
        parse_metrics("sum:")


def test_build_metric_select():
    specs = parse_metrics("sum:area_ha, avg:yield")
    sql = build_metric_select(specs)
    assert sql == 'SUM("area_ha") AS "sum_area_ha", AVG("yield") AS "avg_yield"'
    assert build_metric_select([]) == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_process_aggregate_common.py -v`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Create the package files and implement**

Create empty `geoparquet_io/core/process/__init__.py` and `geoparquet_io/core/process/aggregate/__init__.py`.

Create `geoparquet_io/core/process/aggregate/common.py`:

```python
#!/usr/bin/env python3
"""Shared aggregation spec parsing and SQL builders for `gpio process aggregate`."""

from __future__ import annotations

from dataclasses import dataclass

from geoparquet_io.core.exceptions import InvalidParameterError

VALID_METRIC_FUNCS = {"sum", "avg", "min", "max"}
VALID_OUT_GEOMETRY = {"polygon", "centroid", "both", "none"}


@dataclass(frozen=True)
class MetricSpec:
    """A single numeric rollup: ``func`` over ``column`` -> ``output_name``."""

    func: str
    column: str
    output_name: str


def parse_metrics(metric_str: str | None) -> list[MetricSpec]:
    """Parse a --metric string into MetricSpec entries.

    Accepts comma-separated ``func:column`` pairs. A bare ``column`` with no
    ``func:`` prefix defaults to ``sum`` (a total is the common viz intent).
    """
    if not metric_str:
        return []
    specs: list[MetricSpec] = []
    for raw in metric_str.split(","):
        entry = raw.strip()
        if not entry:
            continue
        if ":" in entry:
            func, _, column = entry.partition(":")
            func = func.strip().lower()
            column = column.strip()
        else:
            func = "sum"
            column = entry
        if func not in VALID_METRIC_FUNCS:
            raise InvalidParameterError(
                f"Unknown metric function '{func}'. "
                f"Valid functions: {', '.join(sorted(VALID_METRIC_FUNCS))}"
            )
        if not column:
            raise InvalidParameterError(f"Metric '{entry}' is missing a column name")
        specs.append(MetricSpec(func=func, column=column, output_name=f"{func}_{column}"))
    return specs


def build_metric_select(metrics: list[MetricSpec]) -> str:
    """Build the comma-joined aggregate expressions for the SELECT (no leading comma)."""
    return ", ".join(
        f'{m.func.upper()}("{m.column}") AS "{m.output_name}"' for m in metrics
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_process_aggregate_common.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add geoparquet_io/core/process tests/test_process_aggregate_common.py
git commit -m "feat(process): add metric spec parsing for aggregate

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Breakdown column naming + SQL literal escaping

**Files:**
- Modify: `geoparquet_io/core/process/aggregate/common.py`
- Test: `tests/test_process_aggregate_common.py`

**Interfaces:**
- Produces:
  - `sanitize_value_for_column(value: object) -> str` — lowercased, non-alphanumerics → `_`, trimmed; empty/None → `"null"`/`"value"`.
  - `build_breakdown_column_names(values: list, reserved: set[str] | None = None) -> list[tuple[object, str]]` — maps each raw value to a unique `count_<sanitized>` column name, disambiguating collisions with `_2`, `_3`, …; never collides with `reserved` (used to reserve `"count_other"`).
  - `sql_literal(value: object) -> str` — single-quoted, `'`-escaped string literal, or bare number for int/float.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_process_aggregate_common.py
from geoparquet_io.core.process.aggregate.common import (
    build_breakdown_column_names,
    sanitize_value_for_column,
    sql_literal,
)


def test_sanitize_value_for_column():
    assert sanitize_value_for_column("Wheat") == "wheat"
    assert sanitize_value_for_column("row crop / cereal") == "row_crop_cereal"
    assert sanitize_value_for_column("2021") == "2021"
    assert sanitize_value_for_column(None) == "null"
    assert sanitize_value_for_column("!!!") == "value"


def test_build_breakdown_column_names_disambiguates_collisions():
    # "a/b" and "a.b" both sanitize to "a_b" -> must not merge
    mapping = build_breakdown_column_names(["a/b", "a.b"])
    names = [n for _, n in mapping]
    assert names == ["count_a_b", "count_a_b_2"]


def test_build_breakdown_column_names_respects_reserved():
    mapping = build_breakdown_column_names(["other"], reserved={"count_other"})
    assert mapping == [("other", "count_other_2")]


def test_sql_literal():
    assert sql_literal("wheat") == "'wheat'"
    assert sql_literal("O'Brien") == "'O''Brien'"
    assert sql_literal(2021) == "2021"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_process_aggregate_common.py -k "sanitize or breakdown_column or sql_literal" -v`
Expected: FAIL — names not defined.

- [ ] **Step 3: Implement in `common.py`**

Add to `geoparquet_io/core/process/aggregate/common.py`:

```python
import re

_UNSAFE_CHARS = re.compile(r"[^0-9a-zA-Z]+")


def sanitize_value_for_column(value: object) -> str:
    """Turn a data value into a safe column-name fragment."""
    if value is None:
        return "null"
    cleaned = _UNSAFE_CHARS.sub("_", str(value).strip().lower()).strip("_")
    return cleaned or "value"


def build_breakdown_column_names(
    values: list, reserved: set[str] | None = None
) -> list[tuple[object, str]]:
    """Map each raw value to a unique ``count_<sanitized>`` column name.

    Collisions (distinct values that sanitize to the same fragment, or that hit a
    reserved name) are disambiguated with a numeric suffix so two categories are
    never silently merged into one column.
    """
    used = set(reserved or set())
    mapping: list[tuple[object, str]] = []
    for value in values:
        base = f"count_{sanitize_value_for_column(value)}"
        name = base
        suffix = 2
        while name in used:
            name = f"{base}_{suffix}"
            suffix += 1
        used.add(name)
        mapping.append((value, name))
    return mapping


def sql_literal(value: object) -> str:
    """Render a Python value as a safe DuckDB SQL literal."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_process_aggregate_common.py -v`
Expected: PASS (all tests in file)

- [ ] **Step 5: Commit**

```bash
git add geoparquet_io/core/process/aggregate/common.py tests/test_process_aggregate_common.py
git commit -m "feat(process): add breakdown column naming and sql literal helpers

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Breakdown value resolution + breakdown SELECT builder

**Files:**
- Modify: `geoparquet_io/core/process/aggregate/common.py`
- Test: `tests/test_process_aggregate_common.py`

**Interfaces:**
- Consumes: `build_breakdown_column_names`, `sql_literal` (Task 3).
- Produces:
  - `resolve_breakdown_values(con, source_sql: str, column: str, limit: int) -> tuple[list, bool]` — returns `(top_values, has_other)`. `top_values` are the up-to-`limit` most frequent values (desc by count, then value); `has_other` is True when more distinct values exist than the limit.
  - `build_breakdown_select(column: str, value_colmap: list[tuple[object, str]], has_other: bool) -> str` — comma-joined `COUNT(*) FILTER (...)` expressions producing `count_<value>` columns plus `count_other` when `has_other`.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_process_aggregate_common.py
import duckdb

from geoparquet_io.core.process.aggregate.common import (
    build_breakdown_select,
    resolve_breakdown_values,
)


def _crop_con():
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE features AS
        SELECT * FROM (VALUES
            ('wheat'), ('wheat'), ('wheat'),
            ('corn'), ('corn'),
            ('rice'), ('barley'), (NULL)
        ) AS t(crop)
        """
    )
    return con


def test_resolve_breakdown_values_top_n_and_other():
    con = _crop_con()
    top, has_other = resolve_breakdown_values(con, "SELECT * FROM features", "crop", limit=2)
    assert top == ["wheat", "corn"]  # most frequent first
    assert has_other is True


def test_resolve_breakdown_values_no_other():
    con = _crop_con()
    top, has_other = resolve_breakdown_values(con, "SELECT * FROM features", "crop", limit=10)
    assert has_other is False


def test_build_breakdown_select_counts_and_other():
    con = _crop_con()
    from geoparquet_io.core.process.aggregate.common import build_breakdown_column_names

    colmap = build_breakdown_column_names(["wheat", "corn"], reserved={"count_other"})
    select = build_breakdown_select("crop", colmap, has_other=True)
    row = con.execute(f"SELECT {select} FROM features").fetchone()
    # wheat=3, corn=2, other(rice+barley+null)=3
    assert row == (3, 2, 3)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_process_aggregate_common.py -k "breakdown_values or breakdown_select" -v`
Expected: FAIL — names not defined.

- [ ] **Step 3: Implement in `common.py`**

```python
def resolve_breakdown_values(con, source_sql: str, column: str, limit: int) -> tuple[list, bool]:
    """Find the top-N most frequent values of ``column`` in the source.

    Returns (top_values, has_other). NULL is treated as its own value here; it is
    rolled into ``count_other`` by build_breakdown_select unless it makes the cut.
    """
    rows = con.execute(
        f'SELECT "{column}" AS v, COUNT(*) AS n '
        f"FROM ({source_sql}) GROUP BY 1 ORDER BY n DESC, v"
    ).fetchall()
    top = [r[0] for r in rows[:limit]]
    has_other = len(rows) > limit
    return top, has_other


def build_breakdown_select(
    column: str, value_colmap: list[tuple[object, str]], has_other: bool
) -> str:
    """Build COUNT(*) FILTER expressions for each kept value, plus count_other."""
    parts: list[str] = []
    for value, colname in value_colmap:
        if value is None:
            cond = f'"{column}" IS NULL'
        else:
            cond = f'"{column}" = {sql_literal(value)}'
        parts.append(f'COUNT(*) FILTER (WHERE {cond}) AS "{colname}"')

    if has_other:
        kept_non_null = [v for v, _ in value_colmap if v is not None]
        kept_null = any(v is None for v, _ in value_colmap)
        kept_conds: list[str] = []
        if kept_non_null:
            in_list = ", ".join(sql_literal(v) for v in kept_non_null)
            kept_conds.append(f'"{column}" IN ({in_list})')
        if kept_null:
            kept_conds.append(f'"{column}" IS NULL')
        kept_clause = " OR ".join(kept_conds) if kept_conds else "FALSE"
        parts.append(
            f'COUNT(*) FILTER (WHERE NOT ({kept_clause})) AS "count_other"'
        )
    return ", ".join(parts)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_process_aggregate_common.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add geoparquet_io/core/process/aggregate/common.py tests/test_process_aggregate_common.py
git commit -m "feat(process): add breakdown value resolution and select builder

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: a5 aggregation core engine (`aggregate_by_a5`)

**Files:**
- Create: `geoparquet_io/core/process/aggregate/by_a5.py`
- Test: `tests/test_process_aggregate_a5.py`

**Interfaces:**
- Consumes: everything in `common.py` (Tasks 2–4); `write_geoparquet_table` (`core/common.py`); `find_primary_geometry_column` (`core/geometry_detection.py`); `safe_file_url` (`core/file_utils.py`); `get_duckdb_connection` (`core/duckdb_utils.py`).
- Produces:
  ```python
  def aggregate_by_a5(
      input_parquet: str,
      output_parquet: str,
      resolution: int,
      metric: str | None = None,
      breakdown: str | None = None,
      breakdown_limit: int = 20,
      out_geometry: str = "polygon",
      a5_column_name: str = "a5_cell",
      compression: str = "ZSTD",
      compression_level: int | None = None,
      geoparquet_version: str | None = None,
      verbose: bool = False,
      show_sql: bool = False,
  ) -> None
  ```
  Writes the aggregated output. (Auto-resolution is added in Task 6; here `resolution` is required and explicit.)

**Output column order:** `a5_cell` (UBIGINT), `count` (BIGINT), metric columns, breakdown columns, then `geometry` (and `centroid` when `out_geometry="both"`). For `out_geometry="centroid"`, the single geometry column is the point. For `out_geometry="none"`, no geometry/centroid columns.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_process_aggregate_a5.py
import duckdb
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5


def _write_points_geoparquet(path, rows):
    """rows: list of (lon, lat, crop, area). Writes a tiny GeoParquet of points."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(
        f"({lon}, {lat}, '{crop}', {area})" for lon, lat, crop, area in rows
    )
    con.execute(
        f"""
        COPY (
            SELECT ST_AsWKB(ST_Point(lon, lat)) AS geometry, crop, area
            FROM (VALUES {values}) AS t(lon, lat, crop, area)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


@pytest.mark.slow
def test_aggregate_a5_count_metric_breakdown(tmp_path):
    src = tmp_path / "fields.parquet"
    out = tmp_path / "agg.parquet"
    # Two clusters far apart so they land in different res-5 cells.
    _write_points_geoparquet(
        src,
        [
            (10.00, 50.00, "wheat", 4.0),
            (10.001, 50.001, "wheat", 6.0),
            (10.002, 50.002, "corn", 2.0),
            (-120.0, 40.0, "wheat", 1.0),
        ],
    )
    aggregate_by_a5(
        str(src),
        str(out),
        resolution=5,
        metric="sum:area",
        breakdown="crop",
        out_geometry="polygon",
    )
    table = pq.read_table(out)
    cols = table.column_names
    assert "a5_cell" in cols
    assert "count" in cols
    assert "sum_area" in cols
    assert "count_wheat" in cols and "count_corn" in cols
    assert "geometry" in cols
    # Two output cells (two clusters)
    assert table.num_rows == 2
    # The 3-feature cell totals area 12 and has 2 wheat + 1 corn
    df = table.to_pandas().sort_values("count", ascending=False).reset_index(drop=True)
    assert int(df.loc[0, "count"]) == 3
    assert float(df.loc[0, "sum_area"]) == 12.0
    assert int(df.loc[0, "count_wheat"]) == 2
    assert int(df.loc[0, "count_corn"]) == 1


@pytest.mark.slow
def test_aggregate_a5_out_geometry_none_is_plain_table(tmp_path):
    src = tmp_path / "fields.parquet"
    out = tmp_path / "agg_none.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0)])
    aggregate_by_a5(str(src), str(out), resolution=5, out_geometry="none")
    table = pq.read_table(out)
    assert "a5_cell" in table.column_names
    assert "count" in table.column_names
    assert "geometry" not in table.column_names
    assert b"geo" not in (table.schema.metadata or {})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_process_aggregate_a5.py -k "a5_count or none_is_plain" -v`
Expected: FAIL — module/function does not exist.

- [ ] **Step 3: Implement `by_a5.py`**

Create `geoparquet_io/core/process/aggregate/by_a5.py`:

```python
#!/usr/bin/env python3
"""A5-cell aggregation for `gpio process aggregate a5`."""

from __future__ import annotations

import pyarrow.parquet as pq

from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import safe_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import configure_verbose, debug, success
from geoparquet_io.core.process.aggregate.common import (
    VALID_OUT_GEOMETRY,
    build_breakdown_column_names,
    build_breakdown_select,
    build_metric_select,
    parse_metrics,
    resolve_breakdown_values,
)

# DOUBLE[2][] boundary array -> closed WKB polygon
_POLY_WKB = (
    "ST_AsWKB(ST_MakePolygon(ST_MakeLine("
    "list_transform(list_append({pts}, {pts}[1]), p -> ST_Point(p[1], p[2]))"
    ")))"
)


def _read_source_sql(input_url: str, geom_col: str) -> str:
    """Source relation exposing the original columns plus a parsed __geom geometry."""
    return (
        f'SELECT *, ST_GeomFromWKB("{geom_col}") AS __geom '
        f"FROM read_parquet('{input_url}', hive_partitioning=false, union_by_name=true)"
    )


def aggregate_by_a5(
    input_parquet: str,
    output_parquet: str,
    resolution: int,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    a5_column_name: str = "a5_cell",
    compression: str = "ZSTD",
    compression_level: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    show_sql: bool = False,
) -> None:
    configure_verbose(verbose)
    if out_geometry not in VALID_OUT_GEOMETRY:
        raise InvalidParameterError(
            f"Invalid --out-geometry '{out_geometry}'. "
            f"Valid: {', '.join(sorted(VALID_OUT_GEOMETRY))}"
        )
    if not 0 <= resolution <= 30:
        raise InvalidParameterError(f"A5 resolution must be 0-30, got {resolution}")

    metrics = parse_metrics(metric)
    input_url = safe_file_url(input_parquet, verbose)
    geom_col = find_primary_geometry_column(input_parquet, verbose) or "geometry"

    con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
    try:
        con.execute("INSTALL a5 FROM community")
        con.execute("LOAD a5")
        con.execute("SET geometry_always_xy = true")

        source_sql = _read_source_sql(input_url, geom_col)

        # Keyed relation: every feature tagged with its a5 cell id.
        keyed_sql = (
            f"SELECT *, a5_lonlat_to_cell("
            f"ST_X(ST_Centroid(__geom)), ST_Y(ST_Centroid(__geom)), {resolution}"
            f") AS __key FROM ({source_sql})"
        )

        # Breakdown columns (resolved against the keyed source so the column exists).
        breakdown_select = ""
        if breakdown:
            top_values, has_other = resolve_breakdown_values(
                con, keyed_sql, breakdown, breakdown_limit
            )
            colmap = build_breakdown_column_names(top_values, reserved={"count_other"})
            breakdown_select = build_breakdown_select(breakdown, colmap, has_other)

        # Aggregate SELECT list.
        agg_parts = [f'__key AS "{a5_column_name}"', "COUNT(*) AS count"]
        metric_select = build_metric_select(metrics)
        if metric_select:
            agg_parts.append(metric_select)
        if breakdown_select:
            agg_parts.append(breakdown_select)
        agg_sql = (
            f"SELECT {', '.join(agg_parts)} "
            f"FROM ({keyed_sql}) GROUP BY __key"
        )

        final_sql = _wrap_with_geometry(agg_sql, a5_column_name, out_geometry)
        if show_sql or verbose:
            debug(final_sql)

        result = con.execute(final_sql).arrow().read_all()
    finally:
        con.close()

    if out_geometry == "none":
        pq.write_table(result, output_parquet, compression=compression)
    else:
        write_geoparquet_table(
            result,
            output_parquet,
            geometry_column="geometry",
            compression=compression,
            compression_level=compression_level,
            geoparquet_version=geoparquet_version,
            verbose=verbose,
        )
    success(f"Aggregated to {result.num_rows} a5 cells -> {output_parquet}")


def _wrap_with_geometry(agg_sql: str, a5_column_name: str, out_geometry: str) -> str:
    """Add geometry/centroid columns derived from the a5 cell id."""
    if out_geometry == "none":
        return agg_sql

    poly = _POLY_WKB.format(pts="__pts")
    centroid = "ST_AsWKB(ST_Point(__ll[1], __ll[2]))"

    if out_geometry == "polygon":
        geom_cols = f"{poly} AS geometry"
    elif out_geometry == "centroid":
        geom_cols = f"{centroid} AS geometry"
    else:  # both
        geom_cols = f"{poly} AS geometry, {centroid} AS centroid"

    return (
        f"SELECT a.* EXCLUDE (__pts, __ll), {geom_cols} "
        f"FROM (SELECT *, a5_cell_to_boundary(\"{a5_column_name}\") AS __pts, "
        f"a5_cell_to_lonlat(\"{a5_column_name}\") AS __ll FROM ({agg_sql})) a"
    )
```

> Note: `a5_cell_to_boundary` returns `DOUBLE[2][]` (array of `[lon, lat]`), so the
> polygon is built by closing the ring (`list_append(pts, pts[1])`) and feeding
> `ST_Point` into `ST_MakeLine`/`ST_MakePolygon`. Verified working SQL.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_process_aggregate_a5.py -k "a5_count or none_is_plain" -v`
Expected: PASS

- [ ] **Step 5: Run the common+a5 suite and commit**

Run: `uv run pytest tests/test_process_aggregate_a5.py tests/test_process_aggregate_common.py -v`
Expected: PASS

```bash
git add geoparquet_io/core/process/aggregate/by_a5.py tests/test_process_aggregate_a5.py
git commit -m "feat(process): add a5 aggregation core engine

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: a5 auto-resolution

**Files:**
- Modify: `geoparquet_io/core/process/aggregate/by_a5.py`
- Test: `tests/test_process_aggregate_a5.py`

**Interfaces:**
- Consumes: `calculate_auto_resolution` from `core/partition/auto_resolution.py` (supports `spatial_index_type="a5"`).
- Produces: new params on `aggregate_by_a5`: `auto: bool = False`, `target_per_cell: int = 10000`, `max_cells: int = 500000`, and `resolution: int | None` now defaults to `None`. When `auto` is True, resolution is computed; passing both `resolution` and `auto` raises `InvalidParameterError`; passing neither raises `InvalidParameterError`.

**Heuristic:** auto targets ~`target_per_cell` features aggregated into each cell (so #cells ≈ total_rows / target_per_cell), capped at `max_cells`, by calling
`calculate_auto_resolution(input_parquet, "a5", target_rows_per_partition=target_per_cell, max_partitions=max_cells, verbose=verbose)`.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_process_aggregate_a5.py
def test_aggregate_a5_requires_resolution_or_auto(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0)])
    with pytest.raises(Exception):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"))  # neither given


def test_aggregate_a5_rejects_resolution_and_auto(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0)])
    with pytest.raises(Exception):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, auto=True)


@pytest.mark.slow
def test_aggregate_a5_auto_runs(tmp_path):
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0), (10.001, 50.001, "corn", 2.0)])
    aggregate_by_a5(str(src), str(out), auto=True, target_per_cell=1)
    assert out.exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_process_aggregate_a5.py -k "requires_resolution or rejects_resolution or auto_runs" -v`
Expected: FAIL — `auto`/`target_per_cell` not accepted, or no validation.

- [ ] **Step 3: Implement**

In `by_a5.py`, change the signature so `resolution: int | None = None` and add `auto: bool = False`, `target_per_cell: int = 10000`, `max_cells: int = 500000`. At the top of the function body (after `configure_verbose`), resolve the resolution:

```python
    from geoparquet_io.core.partition.auto_resolution import calculate_auto_resolution

    if auto and resolution is not None:
        raise InvalidParameterError("Pass either --resolution or --auto, not both")
    if not auto and resolution is None:
        raise InvalidParameterError("A5 aggregation requires --resolution or --auto")
    if auto:
        resolution = calculate_auto_resolution(
            input_parquet,
            "a5",
            target_rows_per_partition=target_per_cell,
            max_partitions=max_cells,
            verbose=verbose,
        )
        if verbose:
            debug(f"Auto-selected a5 resolution {resolution}")
```

Keep the existing `0 <= resolution <= 30` check after this block.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_process_aggregate_a5.py -k "requires_resolution or rejects_resolution or auto_runs" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add geoparquet_io/core/process/aggregate/by_a5.py tests/test_process_aggregate_a5.py
git commit -m "feat(process): add a5 auto-resolution to aggregate

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: `gpio process aggregate a5` CLI command

**Files:**
- Modify: `geoparquet_io/cli/main.py`
- Test: `tests/test_process_aggregate_a5.py`

**Interfaces:**
- Consumes: `aggregate_by_a5` (Tasks 5–6).
- Produces: `process_aggregate.command(name="a5")` wired to call `aggregate_by_a5`.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_process_aggregate_a5.py
from click.testing import CliRunner

from geoparquet_io.cli.main import cli


@pytest.mark.slow
def test_cli_process_aggregate_a5(tmp_path):
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0), (10.001, 50.001, "corn", 2.0)])
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["process", "aggregate", "a5", str(src), str(out),
         "--resolution", "5", "--metric", "sum:area", "--breakdown", "crop"],
    )
    assert result.exit_code == 0, result.output
    assert out.exists()


def test_cli_process_aggregate_a5_bad_metric(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0)])
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["process", "aggregate", "a5", str(src), str(tmp_path / "o.parquet"),
         "--resolution", "5", "--metric", "median:area"],
    )
    assert result.exit_code != 0
    assert "median" in result.output.lower() or "metric" in result.output.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_process_aggregate_a5.py -k "cli_process_aggregate_a5" -v`
Expected: FAIL — no such command.

- [ ] **Step 3: Add the command in `cli/main.py`**

After the `process_aggregate` group (Task 1), add. Use `SingleFileCommand` is NOT needed here (input may be a glob); use a plain command. Import `aggregate_by_a5` at the top of `main.py` with the other core imports:

```python
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5 as aggregate_by_a5_impl
```

Command:

```python
@process_aggregate.command(name="a5")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option("--resolution", type=click.IntRange(0, 30), default=None,
              help="A5 resolution (0-30). Required unless --auto.")
@click.option("--auto", is_flag=True, help="Auto-select resolution from data size.")
@click.option("--target-per-cell", type=int, default=10000,
              help="Target features per cell when using --auto (default: 10000).")
@click.option("--max-cells", type=int, default=500000,
              help="Maximum output cells when using --auto (default: 500000).")
@click.option("--metric", default=None,
              help='Numeric rollups, e.g. "sum:area_ha,avg:yield". Bare column = sum.')
@click.option("--breakdown", default=None,
              help="Categorical column to pivot count by (one count_<value> column each).")
@click.option("--breakdown-limit", type=int, default=20,
              help="Max breakdown values before remainder rolls into count_other (default: 20).")
@click.option("--out-geometry",
              type=click.Choice(["polygon", "centroid", "both", "none"]),
              default="polygon", help="Output geometry per cell (default: polygon).")
@compression_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def process_aggregate_a5(ctx, input_parquet, output_parquet, resolution, auto,
                         target_per_cell, max_cells, metric, breakdown, breakdown_limit,
                         out_geometry, compression, compression_level, verbose,
                         geoparquet_version, show_sql):
    """Aggregate features into A5 grid cells.

    Examples:

        gpio process aggregate a5 fields.parquet cells.parquet --resolution 8
        gpio process aggregate a5 fields.parquet cells.parquet --auto \\
            --metric "sum:area_ha" --breakdown crop_type
        gpio process aggregate a5 fields.parquet cells.csv-like.parquet \\
            --resolution 8 --out-geometry none
    """
    with _activate_s3(ctx):
        try:
            aggregate_by_a5_impl(
                input_parquet, output_parquet,
                resolution=resolution, auto=auto,
                target_per_cell=target_per_cell, max_cells=max_cells,
                metric=metric, breakdown=breakdown, breakdown_limit=breakdown_limit,
                out_geometry=out_geometry,
                compression=compression.upper(), compression_level=compression_level,
                geoparquet_version=geoparquet_version, verbose=verbose, show_sql=show_sql,
            )
        except (InvalidParameterError, ValueError) as exc:
            raise click.ClickException(str(exc)) from exc
```

> Verify `compression_options`, `verbose_option`, `geoparquet_version_option`,
> `show_sql_option`, `_activate_s3`, and `InvalidParameterError` are already
> imported in `main.py` (they are used by `partition_a5`); reuse those imports. If
> `InvalidParameterError` is not imported, add
> `from geoparquet_io.core.exceptions import InvalidParameterError`.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_process_aggregate_a5.py -k "cli_process_aggregate_a5" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add geoparquet_io/cli/main.py tests/test_process_aggregate_a5.py
git commit -m "feat(process): add gpio process aggregate a5 CLI command

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: a5 Python API (`Table.aggregate_a5`, `ops.aggregate_a5`)

**Files:**
- Modify: `geoparquet_io/core/process/aggregate/by_a5.py` (add table-centric function)
- Modify: `geoparquet_io/api/ops.py`
- Modify: `geoparquet_io/api/table.py`
- Test: `tests/test_process_aggregate_a5.py`

**Interfaces:**
- Produces:
  - `by_a5.aggregate_a5_table(table: pa.Table, resolution: int, metric=None, breakdown=None, breakdown_limit=20, out_geometry="polygon", a5_column_name="a5_cell", geometry_column=None) -> pa.Table`
  - `ops.aggregate_a5(table, **kwargs) -> pa.Table`
  - `Table.aggregate_a5(self, resolution, metric=None, breakdown=None, breakdown_limit=20, out_geometry="polygon") -> Table`

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_process_aggregate_a5.py
import pyarrow as pa


def _points_table():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    return con.execute(
        """
        SELECT ST_AsWKB(ST_Point(lon, lat)) AS geometry, crop, area FROM (VALUES
            (10.0, 50.0, 'wheat', 4.0),
            (10.001, 50.001, 'corn', 2.0)
        ) AS t(lon, lat, crop, area)
        """
    ).arrow().read_all()


@pytest.mark.slow
def test_table_aggregate_a5_api():
    from geoparquet_io.api.table import Table

    result = Table(_points_table()).aggregate_a5(resolution=5, metric="sum:area")
    assert "a5_cell" in result.column_names
    assert "count" in result.column_names
    assert "sum_area" in result.column_names
    assert "geometry" in result.column_names
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_process_aggregate_a5.py -k "table_aggregate_a5_api" -v`
Expected: FAIL — method missing.

- [ ] **Step 3: Implement**

In `by_a5.py`, add a table-centric function that registers the Arrow table and reuses the same SQL assembly. Refactor the SQL-building portion of `aggregate_by_a5` into a private helper so both share it:

```python
def aggregate_a5_table(
    table,
    resolution: int,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    a5_column_name: str = "a5_cell",
    geometry_column: str | None = None,
):
    """Aggregate an in-memory Arrow table by a5 cell. Returns a new Arrow table."""
    import pyarrow as pa  # noqa: F401

    if out_geometry not in VALID_OUT_GEOMETRY:
        raise InvalidParameterError(
            f"Invalid out_geometry '{out_geometry}'. Valid: {', '.join(sorted(VALID_OUT_GEOMETRY))}"
        )
    if not 0 <= resolution <= 30:
        raise InvalidParameterError(f"A5 resolution must be 0-30, got {resolution}")

    geom_col = geometry_column or "geometry"
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.execute("INSTALL a5 FROM community")
        con.execute("LOAD a5")
        con.execute("SET geometry_always_xy = true")
        con.register("__agg_input", table)
        source_sql = (
            f'SELECT * EXCLUDE ("{geom_col}"), '
            f'ST_GeomFromWKB("{geom_col}") AS __geom FROM __agg_input'
        )
        final_sql = _build_a5_query(
            con, source_sql, resolution, metric, breakdown, breakdown_limit,
            out_geometry, a5_column_name,
        )
        return con.execute(final_sql).arrow().read_all()
    finally:
        con.close()
```

Extract `_build_a5_query(con, source_sql, resolution, metric, breakdown, breakdown_limit, out_geometry, a5_column_name) -> str` containing the keyed-SQL / breakdown-resolution / agg-SQL / `_wrap_with_geometry` logic currently inline in `aggregate_by_a5`, and call it from `aggregate_by_a5` too (passing its `source_sql`). This keeps both paths DRY.

In `api/ops.py`, add:

```python
def aggregate_a5(
    table,
    resolution: int,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
):
    """Aggregate an Arrow table into a5 cells. Returns a new Arrow table."""
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_a5_table

    return aggregate_a5_table(
        table, resolution=resolution, metric=metric, breakdown=breakdown,
        breakdown_limit=breakdown_limit, out_geometry=out_geometry,
    )
```

In `api/table.py` (inside `class Table`), add:

```python
    def aggregate_a5(
        self,
        resolution: int,
        metric: str | None = None,
        breakdown: str | None = None,
        breakdown_limit: int = 20,
        out_geometry: str = "polygon",
    ) -> "Table":
        """Aggregate features into A5 grid cells with per-cell statistics."""
        from geoparquet_io.core.process.aggregate.by_a5 import aggregate_a5_table

        result = aggregate_a5_table(
            self._table, resolution=resolution, metric=metric, breakdown=breakdown,
            breakdown_limit=breakdown_limit, out_geometry=out_geometry,
            geometry_column=self._geometry_column,
        )
        return Table(result, "geometry" if out_geometry != "none" else None)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_process_aggregate_a5.py -k "table_aggregate_a5_api" -v`
Then full a5 + common suite: `uv run pytest tests/test_process_aggregate_a5.py tests/test_process_aggregate_common.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add geoparquet_io/core/process/aggregate/by_a5.py geoparquet_io/api/ops.py geoparquet_io/api/table.py tests/test_process_aggregate_a5.py
git commit -m "feat(process): add a5 aggregate Python API

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 9: admin aggregation core engine (`aggregate_by_admin`)

**Files:**
- Create: `geoparquet_io/core/process/aggregate/by_admin.py`
- Test: `tests/test_process_aggregate_admin.py`

**Interfaces:**
- Consumes: `common.py` builders; `write_geoparquet_table`; admin setup from `core/partition/admin_hierarchical.py` (`_setup_admin_dataset`) and `core/admin_datasets.py` (`AdminDatasetFactory`, `dataset.prepare_data_source(con)`, `dataset.get_geometry_column()`, `dataset.get_level_column_mapping()`).
- Produces:
  ```python
  def aggregate_by_admin(
      input_parquet: str,
      output_parquet: str,
      level: str = "country",
      metric: str | None = None,
      breakdown: str | None = None,
      breakdown_limit: int = 20,
      out_geometry: str = "polygon",
      dataset: str = "overture",
      compression: str = "ZSTD",
      compression_level: int | None = None,
      geoparquet_version: str | None = None,
      verbose: bool = False,
      show_sql: bool = False,
  ) -> None
  ```

**Behavior:** spatial-join each feature centroid into admin regions; group by region code; output columns `admin_code`, `admin_name`, `count`, metrics, breakdowns, geometry. Features outside all regions form one `unassigned` bucket (`admin_code='unassigned'`, `admin_name=NULL`, geometry `NULL`); log how many features were unassigned.

> **Implementation note:** mirror the spatial-join mechanics in
> `admin_hierarchical.py` (`_setup_admin_dataset`, `dataset.prepare_data_source`,
> `_build_admin_table_reference`, the `ST_Intersects` join and bbox prefilter). The
> key difference: this command groups by the region code and takes
> `ANY_VALUE(<admin geom>)` as the bucket geometry (the geom is constant per region),
> rather than splitting into partition files. Resolve the region **code** and
> **name** columns from `dataset.get_level_column_mapping()` for the requested
> `level` (country -> 2-char ISO code). Confirm exact column names against the
> dataset class while implementing.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_process_aggregate_admin.py
import duckdb
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin


def _write_points_geoparquet(path, rows):
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, '{cls}')" for lon, lat, cls in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_AsWKB(ST_Point(lon, lat)) AS geometry, cls
            FROM (VALUES {values}) AS t(lon, lat, cls)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_admin_country_with_unassigned(tmp_path):
    src = tmp_path / "pts.parquet"
    out = tmp_path / "by_country.parquet"
    # Two points in France, one in the middle of the ocean (unassigned).
    _write_points_geoparquet(
        src,
        [(2.35, 48.85, "a"), (4.85, 45.75, "b"), (-30.0, 0.0, "c")],
    )
    aggregate_by_admin(str(src), str(out), level="country", out_geometry="polygon")
    table = pq.read_table(out)
    cols = table.column_names
    assert "admin_code" in cols and "admin_name" in cols and "count" in cols
    codes = set(table.column("admin_code").to_pylist())
    assert "unassigned" in codes
    df = table.to_pandas()
    assert int(df.loc[df["admin_code"] == "unassigned", "count"].iloc[0]) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_process_aggregate_admin.py -v`
Expected: FAIL — module/function does not exist.

- [ ] **Step 3: Implement `by_admin.py`**

Create `geoparquet_io/core/process/aggregate/by_admin.py`. Build it incrementally, leaning on the referenced helpers. Concrete skeleton:

```python
#!/usr/bin/env python3
"""Admin-region aggregation for `gpio process aggregate admin`."""

from __future__ import annotations

import pyarrow.parquet as pq

from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import safe_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import configure_verbose, debug, info, success
from geoparquet_io.core.partition.admin_hierarchical import (
    _build_admin_table_reference,
    _setup_admin_dataset,
)
from geoparquet_io.core.process.aggregate.common import (
    VALID_OUT_GEOMETRY,
    build_breakdown_column_names,
    build_breakdown_select,
    build_metric_select,
    parse_metrics,
    resolve_breakdown_values,
)


def aggregate_by_admin(
    input_parquet: str,
    output_parquet: str,
    level: str = "country",
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    dataset: str = "overture",
    compression: str = "ZSTD",
    compression_level: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    show_sql: bool = False,
) -> None:
    configure_verbose(verbose)
    if out_geometry not in VALID_OUT_GEOMETRY:
        raise InvalidParameterError(
            f"Invalid out_geometry '{out_geometry}'. Valid: {', '.join(sorted(VALID_OUT_GEOMETRY))}"
        )
    metrics = parse_metrics(metric)

    admin_dataset, _boundary_columns = _setup_admin_dataset(dataset, verbose, [level])
    admin_dataset.validate_levels([level])
    code_col = admin_dataset.get_level_column_mapping()[level]
    name_col = code_col  # refine if dataset exposes a separate name column for the level
    admin_geom_col = admin_dataset.get_geometry_column()

    input_url = safe_file_url(input_parquet, verbose)
    geom_col = find_primary_geometry_column(input_parquet, verbose) or "geometry"

    con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
    try:
        con.execute("SET geometry_always_xy = true")
        admin_source = admin_dataset.prepare_data_source(con)
        admin_ref = _build_admin_table_reference(admin_dataset, admin_source)

        # Join: feature centroid within admin region.
        joined_sql = f"""
            SELECT s.*,
                   b."{code_col}" AS __admin_code,
                   b."{name_col}" AS __admin_name,
                   b."{admin_geom_col}" AS __admin_geom
            FROM (
                SELECT *, ST_Centroid(ST_GeomFromWKB("{geom_col}")) AS __cen
                FROM read_parquet('{input_url}', hive_partitioning=false, union_by_name=true)
            ) s
            LEFT JOIN {admin_ref} b
              ON ST_Intersects(ST_GeomFromWKB(b."{admin_geom_col}"), s.__cen)
        """

        breakdown_select = ""
        if breakdown:
            top_values, has_other = resolve_breakdown_values(
                con, joined_sql, breakdown, breakdown_limit
            )
            colmap = build_breakdown_column_names(top_values, reserved={"count_other"})
            breakdown_select = build_breakdown_select(breakdown, colmap, has_other)

        agg_parts = [
            "COALESCE(__admin_code, 'unassigned') AS admin_code",
            "ANY_VALUE(__admin_name) AS admin_name",
            "ANY_VALUE(__admin_geom) AS __admin_geom",
            "COUNT(*) AS count",
        ]
        metric_select = build_metric_select(metrics)
        if metric_select:
            agg_parts.append(metric_select)
        if breakdown_select:
            agg_parts.append(breakdown_select)
        agg_sql = (
            f"SELECT {', '.join(agg_parts)} FROM ({joined_sql}) "
            f"GROUP BY COALESCE(__admin_code, 'unassigned')"
        )

        final_sql = _wrap_admin_geometry(agg_sql, out_geometry)
        if show_sql or verbose:
            debug(final_sql)
        result = con.execute(final_sql).arrow().read_all()

        unassigned = con.execute(
            f"SELECT COUNT(*) FROM ({joined_sql}) WHERE __admin_code IS NULL"
        ).fetchone()[0]
        if unassigned:
            info(f"{unassigned} features fell outside all admin regions (-> 'unassigned')")
    finally:
        con.close()

    if out_geometry == "none":
        pq.write_table(result, output_parquet, compression=compression)
    else:
        write_geoparquet_table(
            result, output_parquet, geometry_column="geometry",
            compression=compression, compression_level=compression_level,
            geoparquet_version=geoparquet_version, verbose=verbose,
        )
    success(f"Aggregated to {result.num_rows} admin regions -> {output_parquet}")


def _wrap_admin_geometry(agg_sql: str, out_geometry: str) -> str:
    """Add geometry/centroid columns from the per-region admin geometry (WKB)."""
    if out_geometry == "none":
        return f"SELECT a.* EXCLUDE (__admin_geom) FROM ({agg_sql}) a"

    geom = "__admin_geom"  # already WKB
    centroid = f"ST_AsWKB(ST_Centroid(ST_GeomFromWKB({geom})))"
    if out_geometry == "polygon":
        geom_cols = f"{geom} AS geometry"
    elif out_geometry == "centroid":
        geom_cols = f"{centroid} AS geometry"
    else:  # both
        geom_cols = f"{geom} AS geometry, {centroid} AS centroid"
    return f"SELECT a.* EXCLUDE (__admin_geom), {geom_cols} FROM ({agg_sql}) a"
```

> While implementing, verify with `gpio inspect meta` / the dataset class:
> (a) the exact `code_col`/`name_col` for the chosen level (country level should map
> to a 2-char ISO code; if a distinct human-readable name column exists, use it for
> `name_col`); (b) whether `b."{admin_geom_col}"` is already a GEOMETRY (then drop
> the `ST_GeomFromWKB` wrappers in the join/centroid, matching how
> `admin_hierarchical.py` calls `ST_Intersects` on the raw column). Adjust the two
> `ST_GeomFromWKB(b...)` sites to match the dataset's actual geometry storage.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_process_aggregate_admin.py -v`
Expected: PASS (downloads admin cache on first run; `network`+`slow` marked)

- [ ] **Step 5: Commit**

```bash
git add geoparquet_io/core/process/aggregate/by_admin.py tests/test_process_aggregate_admin.py
git commit -m "feat(process): add admin aggregation core engine

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 10: `gpio process aggregate admin` CLI command

**Files:**
- Modify: `geoparquet_io/cli/main.py`
- Test: `tests/test_process_aggregate_admin.py`

**Interfaces:**
- Consumes: `aggregate_by_admin` (Task 9).
- Produces: `process_aggregate.command(name="admin")`.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_process_aggregate_admin.py
from click.testing import CliRunner

from geoparquet_io.cli.main import cli


def test_cli_process_aggregate_admin_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["process", "aggregate", "admin", "--help"])
    assert result.exit_code == 0
    assert "--level" in result.output
    assert "--out-geometry" in result.output


@pytest.mark.slow
@pytest.mark.network
def test_cli_process_aggregate_admin_runs(tmp_path):
    src = tmp_path / "pts.parquet"
    out = tmp_path / "o.parquet"
    _write_points_geoparquet(src, [(2.35, 48.85, "a"), (4.85, 45.75, "b")])
    runner = CliRunner()
    result = runner.invoke(
        cli, ["process", "aggregate", "admin", str(src), str(out), "--level", "country"]
    )
    assert result.exit_code == 0, result.output
    assert out.exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_process_aggregate_admin.py -k "cli_process_aggregate_admin" -v`
Expected: FAIL — no such command.

- [ ] **Step 3: Add the command in `cli/main.py`**

Import at top: `from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin as aggregate_by_admin_impl`. Then:

```python
@process_aggregate.command(name="admin")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option("--level", type=click.Choice(["country", "region"]), default="country",
              help="Administrative level to aggregate to (default: country).")
@click.option("--metric", default=None,
              help='Numeric rollups, e.g. "sum:area_ha,avg:yield". Bare column = sum.')
@click.option("--breakdown", default=None,
              help="Categorical column to pivot count by.")
@click.option("--breakdown-limit", type=int, default=20,
              help="Max breakdown values before remainder rolls into count_other (default: 20).")
@click.option("--out-geometry",
              type=click.Choice(["polygon", "centroid", "both", "none"]),
              default="polygon", help="Output geometry per region (default: polygon).")
@compression_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def process_aggregate_admin(ctx, input_parquet, output_parquet, level, metric,
                            breakdown, breakdown_limit, out_geometry, compression,
                            compression_level, verbose, geoparquet_version, show_sql):
    """Aggregate features into administrative regions.

    Examples:

        gpio process aggregate admin fields.parquet by_country.parquet --level country
        gpio process aggregate admin fields.parquet by_region.parquet \\
            --level region --metric "sum:area_ha" --breakdown crop_type
    """
    with _activate_s3(ctx):
        try:
            aggregate_by_admin_impl(
                input_parquet, output_parquet, level=level,
                metric=metric, breakdown=breakdown, breakdown_limit=breakdown_limit,
                out_geometry=out_geometry,
                compression=compression.upper(), compression_level=compression_level,
                geoparquet_version=geoparquet_version, verbose=verbose, show_sql=show_sql,
            )
        except (InvalidParameterError, ValueError) as exc:
            raise click.ClickException(str(exc)) from exc
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_process_aggregate_admin.py -k "cli_process_aggregate_admin" -v`
Expected: PASS (the `_help` test runs offline; the `_runs` test needs network)

- [ ] **Step 5: Commit**

```bash
git add geoparquet_io/cli/main.py tests/test_process_aggregate_admin.py
git commit -m "feat(process): add gpio process aggregate admin CLI command

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 11: admin Python API (`Table.aggregate_admin`, `ops.aggregate_admin`)

**Files:**
- Modify: `geoparquet_io/api/ops.py`
- Modify: `geoparquet_io/api/table.py`
- Test: `tests/test_process_aggregate_admin.py`

**Interfaces:**
- Consumes: `aggregate_by_admin` (Task 9) and the existing temp-file round-trip pattern used by `ops.add_admin_divisions` (see `api/ops.py:116` and `_file_round_trip`/`_table_to_temp_parquet_and_convert` at `api/ops.py:588`).
- Produces:
  - `ops.aggregate_admin(table, level="country", metric=None, breakdown=None, breakdown_limit=20, out_geometry="polygon") -> pa.Table`
  - `Table.aggregate_admin(self, level="country", metric=None, breakdown=None, breakdown_limit=20, out_geometry="polygon") -> Table`

  Because admin aggregation needs external admin data and is file-oriented, the API
  writes the input table to a temp GeoParquet, calls `aggregate_by_admin`, and reads
  the result back — exactly the round-trip pattern `ops.add_admin_divisions` already
  uses. Reuse that existing helper rather than writing a new one.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_process_aggregate_admin.py
@pytest.mark.slow
@pytest.mark.network
def test_table_aggregate_admin_api(tmp_path):
    import duckdb

    from geoparquet_io.api.table import Table

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = con.execute(
        "SELECT ST_AsWKB(ST_Point(2.35, 48.85)) AS geometry, 'a' AS cls"
    ).arrow().read_all()
    result = Table(tbl).aggregate_admin(level="country")
    assert "admin_code" in result.column_names
    assert "count" in result.column_names
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_process_aggregate_admin.py -k "table_aggregate_admin_api" -v`
Expected: FAIL — method missing.

- [ ] **Step 3: Implement**

In `api/ops.py`, look at how `add_admin_divisions` (line ~116) round-trips a table through temp parquet via the helper near line 588. Mirror it:

```python
def aggregate_admin(
    table,
    level: str = "country",
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
):
    """Aggregate an Arrow table into admin regions. Returns a new Arrow table."""
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    def _run(in_path, out_path):
        aggregate_by_admin(
            in_path, out_path, level=level, metric=metric, breakdown=breakdown,
            breakdown_limit=breakdown_limit, out_geometry=out_geometry,
        )

    return _file_round_trip(table, _run)  # use the same helper add_admin_divisions uses
```

> If the existing helper has a different name/signature than `_file_round_trip`,
> use whichever helper `add_admin_divisions` calls; match its exact call shape.
> If no reusable helper exists, write the temp-file round trip inline:
> `tempfile.mkdtemp()` → `write_geoparquet_table(table, in_path)` →
> `aggregate_by_admin(in_path, out_path, ...)` → `pq.read_table(out_path)` →
> clean up the temp dir.

In `api/table.py`:

```python
    def aggregate_admin(
        self,
        level: str = "country",
        metric: str | None = None,
        breakdown: str | None = None,
        breakdown_limit: int = 20,
        out_geometry: str = "polygon",
    ) -> "Table":
        """Aggregate features into administrative regions with per-region statistics."""
        from geoparquet_io.api.ops import aggregate_admin

        result = aggregate_admin(
            self._table, level=level, metric=metric, breakdown=breakdown,
            breakdown_limit=breakdown_limit, out_geometry=out_geometry,
        )
        return Table(result, "geometry" if out_geometry != "none" else None)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_process_aggregate_admin.py -k "table_aggregate_admin_api" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add geoparquet_io/api/ops.py geoparquet_io/api/table.py tests/test_process_aggregate_admin.py
git commit -m "feat(process): add admin aggregate Python API

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 12: Docs + changelog + full-suite verification

**Files:**
- Create: `docs/guide/process-aggregate.md`
- Modify: `docs/api/python-api.md`
- Modify: `CHANGELOG.md` (root)

**Interfaces:**
- Consumes: nothing new; documents Tasks 1–11.

- [ ] **Step 1: Write the guide**

Create `docs/guide/process-aggregate.md` with the tabbed CLI + Python format used by
other guide pages (open an existing page like `docs/guide/` partition docs for the
exact tab directive syntax). Cover:

- What aggregation is and the visualization use case (low-zoom rollup of huge data).
- `gpio process aggregate a5` with `--resolution`/`--auto`, `--metric`,
  `--breakdown`/`--breakdown-limit`, `--out-geometry polygon|centroid|both|none`.
- `gpio process aggregate admin` with `--level country|region`.
- The always-present bucket id (`a5_cell` / `admin_code`) and how `--out-geometry none`
  produces a plain Parquet table to join geometry onto later.
- Python API examples: `Table(...).aggregate_a5(resolution=8, metric="sum:area_ha", breakdown="crop_type")`
  and `Table(...).aggregate_admin(level="country")`.

Each example must show both a CLI tab and a Python tab.

- [ ] **Step 2: Update the Python API reference**

In `docs/api/python-api.md`, add entries for `Table.aggregate_a5`, `Table.aggregate_admin`,
`ops.aggregate_a5`, `ops.aggregate_admin` following the existing method-doc format on
that page (signature + one runnable example each).

- [ ] **Step 3: Update the changelog**

Add to the top "Unreleased"/latest section of root `CHANGELOG.md` (NOT
`docs/CHANGELOG.md`, which is generated):

```markdown
### Added
- `gpio process aggregate a5` and `gpio process aggregate admin`: aggregate large
  GeoParquet datasets into A5 grid cells or admin regions with per-bucket `count`,
  `--metric` rollups (sum/avg/min/max), and `--breakdown` category pivots, for
  low-zoom visualization. Output geometry is selectable
  (`--out-geometry polygon|centroid|both|none`); every row carries the bucket id
  (`a5_cell`/`admin_code`) so `none` output can be re-joined to geometry. Python
  API: `Table.aggregate_a5`, `Table.aggregate_admin`.
```

- [ ] **Step 4: Run pre-commit + full fast suite**

Run:
```bash
uv run pytest -n auto -m "not slow and not network"
uv run pre-commit run --all-files
```
Expected: tests PASS; pre-commit hooks PASS (notably `no-click-echo`, `duckdb-antipatterns`, `check-api-for-cli`, `doc-sync`, import-linter). Fix any reported issues (e.g. doc-sync may rewrite `docs/CHANGELOG.md` from root — re-stage).

- [ ] **Step 5: Commit**

```bash
git add docs/guide/process-aggregate.md docs/api/python-api.md CHANGELOG.md docs/CHANGELOG.md
git commit -m "docs(process): document gpio process aggregate

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Final Verification

- [ ] Run the full slow/network suite for the feature:
  `uv run pytest tests/test_process_aggregate_common.py tests/test_process_aggregate_a5.py tests/test_process_aggregate_admin.py -v`
  (admin tests need network for the first admin-cache download).
- [ ] `uv run pre-commit run --all-files` clean.
- [ ] `gpio process aggregate a5 --help` and `gpio process aggregate admin --help` render.
- [ ] Manual smoke test on a real file:
  `gpio process aggregate a5 <some.parquet> /tmp/cells.parquet --auto --metric "sum:area" --breakdown <cat_col>`
  then `gpio inspect summary /tmp/cells.parquet` shows the expected columns and a
  valid GeoParquet.

---

## Self-Review Notes (author)

- **Spec coverage:** command group (T1); count+metric+breakdown (T2–T4, applied in
  T5/T9); top-N cap + count_other (T4); collision-safe column names (T3);
  out-geometry polygon/centroid/both/none (T5/T9); always-present bucket id
  a5_cell/admin_code (T5/T9); a5 resolution + auto (T5/T6); admin level + unassigned
  bucket (T9); glob/dir input (read_parquet with union_by_name in T5/T9); valid
  GeoParquet vs plain Parquet output (T5/T9); Python API for both (T8/T11); docs +
  changelog (T12); TDD throughout. h3, PMTiles packaging, per-category cross-tabs
  remain deferred per spec.
- **Type consistency:** `aggregate_by_a5` / `aggregate_a5_table` / `aggregate_by_admin`
  signatures and the `_build_a5_query`, `_wrap_with_geometry`, `_wrap_admin_geometry`,
  and common.py helper names are used consistently across tasks.
- **Known confirm-on-implement points (flagged inline, not placeholders):** the admin
  dataset's exact code/name/geometry column names and whether its geometry is stored
  as GEOMETRY vs WKB — both resolved by mirroring `admin_hierarchical.py`, which is
  working code in the repo.
