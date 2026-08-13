#!/usr/bin/env python3
"""Shared aggregation spec parsing and SQL builders for `gpio process aggregate`."""

from __future__ import annotations

import re
from dataclasses import dataclass

from geoparquet_io.core.duckdb_utils import quote_identifier
from geoparquet_io.core.exceptions import InvalidParameterError

VALID_METRIC_FUNCS = {"sum", "avg", "min", "max"}
VALID_OUT_GEOMETRY = {"polygon", "centroid", "both", "none"}


def aggregate_source_relation(input_url: str) -> str:
    """``read_parquet`` expression for the input scan of an aggregation.

    Hive partitioning is left to DuckDB's auto-detection (the default) rather
    than forced off, so ``--where "year = 2025"`` can filter on a partition
    column of a hive-style glob/directory -- the documented use case, which the
    forced ``hive_partitioning=false`` broke with a Binder error while the
    ``--auto`` row-count path (a bare ``FROM 'url'``, auto-detecting) accepted
    it (gpio #612).

    Partition columns cannot leak into the output: every aggregation projects a
    fixed column list (bucket id, count, metrics, breakdown pivots, geometry),
    so a passthrough column added by the scan is dropped by the GROUP BY.
    """
    return f"read_parquet('{input_url}', union_by_name=true)"


def geometry_to_geom_expr(con, relation: str, geom_col: str) -> str:
    """Return a SQL expression yielding a GEOMETRY for ``geom_col`` in ``relation``.

    DuckDB 1.5 reads a GeoParquet geometry column as a ``GEOMETRY`` type, so it can
    be used directly. In-memory Arrow tables and plain WKB-blob Parquet expose the
    column as ``BLOB``, which must be decoded with ``ST_GeomFromWKB``. This inspects
    the actual column type so callers get a GEOMETRY either way.

    ``relation`` must be usable in a FROM clause (e.g. ``read_parquet('...')`` or a
    registered relation name). ``con`` must have the spatial extension loaded.
    """
    qcol = quote_identifier(geom_col)
    rows = con.execute(f"DESCRIBE SELECT {qcol} FROM {relation}").fetchall()
    col_type = (rows[0][1] if rows else "").upper()
    if "GEOMETRY" in col_type:
        return qcol
    # BLOB/BINARY (and anything unrecognized) is treated as WKB. Wrap in TRY so a
    # single malformed value becomes NULL (-> unassigned bucket) instead of
    # aborting the whole aggregation.
    return f"TRY(ST_GeomFromWKB({qcol}))"


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
                "metric",
                f"Unknown metric function '{func}'. "
                f"Valid functions: {', '.join(sorted(VALID_METRIC_FUNCS))}",
            )
        if not column:
            raise InvalidParameterError("metric", f"Metric '{entry}' is missing a column name")
        specs.append(MetricSpec(func=func, column=column, output_name=f"{func}_{column}"))
    return specs


def build_metric_select(metrics: list[MetricSpec]) -> str:
    """Build the comma-joined aggregate expressions for the SELECT (no leading comma)."""
    return ", ".join(
        f"{m.func.upper()}({quote_identifier(m.column)}) AS {quote_identifier(m.output_name)}"
        for m in metrics
    )


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


def resolve_breakdown_values(con, source_sql: str, column: str, limit: int) -> tuple[list, bool]:
    """Find the top-N most frequent values of ``column`` in the source.

    Returns (top_values, has_other). NULL is treated as its own value here; it is
    rolled into ``count_other`` by build_breakdown_select unless it makes the cut.
    """
    rows = con.execute(
        f"SELECT {quote_identifier(column)} AS v, COUNT(*) AS n"
        f" FROM ({source_sql}) GROUP BY 1 ORDER BY n DESC, v"
    ).fetchall()
    top = [r[0] for r in rows[:limit]]
    has_other = len(rows) > limit
    return top, has_other


def build_breakdown_select(
    column: str, value_colmap: list[tuple[object, str]], has_other: bool
) -> str:
    """Build COUNT(*) FILTER expressions for each kept value, plus count_other."""
    qcol = quote_identifier(column)
    parts: list[str] = []
    for value, colname in value_colmap:
        if value is None:
            cond = f"{qcol} IS NULL"
        else:
            cond = f"{qcol} = {sql_literal(value)}"
        parts.append(f'COUNT(*) FILTER (WHERE {cond}) AS "{colname}"')

    if has_other:
        kept_non_null = [v for v, _ in value_colmap if v is not None]
        kept_null = any(v is None for v, _ in value_colmap)

        if kept_null:
            # NULL is explicitly kept, so count_other = NOT(kept values including NULL)
            kept_conds: list[str] = []
            if kept_non_null:
                in_list = ", ".join(sql_literal(v) for v in kept_non_null)
                kept_conds.append(f"{qcol} IN ({in_list})")
            kept_conds.append(f"{qcol} IS NULL")
            kept_clause = " OR ".join(kept_conds)
            parts.append(f'COUNT(*) FILTER (WHERE NOT ({kept_clause})) AS "count_other"')
        else:
            # NULL is not explicitly kept, so include it in count_other
            # Need to use NOT IN with explicit NULL handling since NULL NOT IN (...) = NULL
            if kept_non_null:
                in_list = ", ".join(sql_literal(v) for v in kept_non_null)
                other_clause = f"{qcol} NOT IN ({in_list}) OR {qcol} IS NULL"
            else:
                # No non-null values kept (shouldn't happen, but handle gracefully)
                other_clause = "TRUE"
            parts.append(f'COUNT(*) FILTER (WHERE {other_clause}) AS "count_other"')
    return ", ".join(parts)
