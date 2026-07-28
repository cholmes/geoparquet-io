#!/usr/bin/env python3
"""Detect the shape of a `gpio process aggregate` output for overview rollups.

Given an aggregate file (or registered relation), works out which bucketing
scheme produced it (a5 / h3 / admin), the base level of the cell ids, how each
column rolls up to a coarser level, and what output geometry to regenerate.
"""

from __future__ import annotations

import gc
import re
from contextlib import contextmanager
from dataclasses import dataclass

from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import safe_file_url
from geoparquet_io.core.logging_config import warn
from geoparquet_io.core.process.aggregate.by_a5 import A5_SCHEME
from geoparquet_io.core.process.aggregate.by_h3 import H3_SCHEME
from geoparquet_io.core.process.aggregate.common import geometry_to_geom_expr

VALID_SCHEMES = ("a5", "h3", "admin")
# Default cell column per scheme, single-sourced from the aggregate engine's
# GridScheme descriptors so the two stay in lockstep.
DEFAULT_CELL_COLUMNS = {
    "a5": A5_SCHEME.default_column,
    "h3": H3_SCHEME.default_column,
    "admin": "admin_code",
}

_INTEGER_TYPES = {
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
}
# H3 string cell ids are 15 hex chars (e.g. "85283473fffffff").
_H3_STRING_RE = re.compile(r"^[0-9a-fA-F]{15}$")

_GRID_EXTENSIONS = {"a5": "a5", "h3": "h3"}
_RESOLUTION_FUNCS = {"a5": "a5_get_resolution", "h3": "h3_get_resolution"}


@dataclass(frozen=True)
class RollupColumn:
    """How one attribute column rolls up to a parent cell."""

    name: str
    func: str  # "sum" | "avg" | "min" | "max"
    cast_to_bigint: bool = False  # SUM(BIGINT) widens to HUGEINT; cast back


@dataclass(frozen=True)
class AggregateInfo:
    """Everything overview building needs to know about an aggregate input."""

    scheme: str  # "a5" | "h3" | "admin"
    cell_column: str
    base_level: int | str  # grid resolution, or "region" for admin
    rollup_columns: tuple[RollupColumn, ...]
    out_geometry: str  # "polygon" | "centroid" | "both" | "none"
    dropped_columns: tuple[str, ...] = ()

    @property
    def num_attributes(self) -> int:
        """Numeric attribute columns per cell (count + rollups), for sizing."""
        return 1 + len(self.rollup_columns)


def ensure_grid_extension(con, scheme: str) -> None:
    """Install/load the community extension backing a grid scheme."""
    extension = _GRID_EXTENSIONS[scheme]
    con.execute(f"INSTALL {extension} FROM community")
    con.execute(f"LOAD {extension}")


def _describe_columns(con, relation: str) -> list[tuple[str, str]]:
    return [
        (row[0], row[1].upper())
        for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    ]


def _detect_scheme(
    con,
    relation: str,
    columns: dict[str, str],
    cell_column: str | None,
    scheme: str | None = None,
):
    """Return (cell_column, scheme)."""
    if scheme is not None:
        if scheme not in VALID_SCHEMES:
            raise InvalidParameterError(
                "scheme", f"invalid scheme '{scheme}'. Valid: {', '.join(VALID_SCHEMES)}"
            )
        cell_col = cell_column or DEFAULT_CELL_COLUMNS[scheme]
        if cell_col not in columns:
            raise InvalidParameterError(
                "cell_column", f"column '{cell_col}' not found in the input"
            )
        return cell_col, scheme
    if cell_column is not None:
        if cell_column not in columns:
            raise InvalidParameterError(
                "cell_column", f"column '{cell_column}' not found in the input"
            )
        return cell_column, _scheme_for_column(con, relation, cell_column, columns[cell_column])
    if "a5_cell" in columns and columns["a5_cell"] in _INTEGER_TYPES:
        return "a5_cell", "a5"
    if "h3_cell" in columns and columns["h3_cell"] == "VARCHAR":
        return "h3_cell", "h3"
    if "admin_code" in columns:
        return "admin_code", "admin"
    raise InvalidParameterError(
        "input",
        "no aggregate cell column found (expected a5_cell, h3_cell, or admin_code). "
        "Is this a `gpio process aggregate` output? Pass --cell-column to override.",
    )


def _scheme_for_column(con, relation: str, name: str, dtype: str) -> str:
    """Infer the scheme for an explicitly named cell column."""
    if dtype in _INTEGER_TYPES:
        if "a5" in name.lower():
            return "a5"
        # a5 ids and packed H3 ids are both integers; guessing a5 here would
        # silently roll H3 data up the wrong hierarchy.
        raise InvalidParameterError(
            "cell_column",
            f"cannot infer a grid scheme for integer column '{name}' "
            "(a5 and packed H3 ids are both stored as integers). "
            "Pass --scheme a5 or --scheme h3 to disambiguate.",
        )
    qcol = quote_identifier(name)
    row = con.execute(f"SELECT {qcol} FROM {relation} WHERE {qcol} IS NOT NULL LIMIT 1").fetchone()
    if row and isinstance(row[0], str) and _H3_STRING_RE.match(row[0]):
        return "h3"
    return "admin"


def _classify_columns(columns: list[tuple[str, str]], cell_column: str, scheme: str):
    """Split attribute columns into rollup roles; return (rollups, dropped)."""
    special = {cell_column, "count", "geometry", "centroid"}
    if scheme == "admin":
        special.add("admin_name")
    prefix_to_func = {"sum_": "sum", "count_": "sum", "min_": "min", "max_": "max", "avg_": "avg"}

    rollups: list[RollupColumn] = []
    dropped: list[str] = []
    for name, dtype in columns:
        if name in special:
            continue
        func = next((f for p, f in prefix_to_func.items() if name.startswith(p)), None)
        if func is None:
            dropped.append(name)
            continue
        rollups.append(
            RollupColumn(name, func, cast_to_bigint=(func == "sum" and dtype in _INTEGER_TYPES))
        )
    return tuple(rollups), tuple(dropped)


def _infer_out_geometry(con, relation: str, columns: dict[str, str]) -> str:
    if "geometry" not in columns:
        return "none"
    if "centroid" in columns:
        return "both"
    geom_expr = geometry_to_geom_expr(con, relation, "geometry")
    row = con.execute(
        f"SELECT ST_GeometryType({geom_expr}) FROM {relation} WHERE geometry IS NOT NULL LIMIT 1"
    ).fetchone()
    if row and str(row[0]).upper() == "POINT":
        return "centroid"
    return "polygon"


def _detect_grid_base_level(con, relation: str, scheme: str, cell_column: str) -> int:
    ensure_grid_extension(con, scheme)
    qcol = quote_identifier(cell_column)
    res_func = _RESOLUTION_FUNCS[scheme]
    rows = con.execute(
        f"SELECT DISTINCT {res_func}({qcol}) FROM {relation} WHERE {qcol} IS NOT NULL"
    ).fetchall()
    resolutions = sorted(row[0] for row in rows)
    if not resolutions:
        raise InvalidParameterError("input", f"no non-NULL {cell_column} values to roll up")
    if len(resolutions) > 1:
        raise InvalidParameterError(
            "input",
            f"mixed {scheme} resolutions in {cell_column}: {resolutions}. "
            "Overviews require a single base resolution.",
        )
    return int(resolutions[0])


def _detect_admin_base_level(con, relation: str, cell_column: str) -> str:
    qcol = quote_identifier(cell_column)
    row = con.execute(
        f"SELECT COUNT(*) FILTER (WHERE {qcol} LIKE '%-%') FROM {relation} "
        f"WHERE {qcol} IS NOT NULL AND {qcol} != 'unassigned'"
    ).fetchone()
    if not row or row[0] == 0:
        raise InvalidParameterError(
            "input",
            "admin aggregate is already at country level (no region codes like "
            "'US-CA' found); there is no coarser admin level to roll up to.",
        )
    return "region"


def detect_aggregate_info(
    con, relation: str, cell_column: str | None = None, scheme: str | None = None
) -> AggregateInfo:
    """Detect scheme, base level, column roles, and output geometry.

    ``relation`` must be usable in a FROM clause (``read_parquet('...')`` or a
    registered table name). ``con`` needs the spatial extension loaded; grid
    community extensions are installed on demand for base-level probing.
    ``scheme`` (a5/h3/admin) skips scheme inference for ambiguous cell columns
    (e.g. H3 ids stored as integers).
    """
    ordered = _describe_columns(con, relation)
    columns = dict(ordered)

    cell_col, scheme = _detect_scheme(con, relation, columns, cell_column, scheme)
    if "count" not in columns:
        raise InvalidParameterError(
            "input",
            "aggregate input must have a count column; re-run gpio process aggregate",
        )
    rollups, dropped = _classify_columns(ordered, cell_col, scheme)
    if dropped:
        warn(f"Ignoring columns that cannot be rolled up: {', '.join(dropped)}")

    if scheme == "admin":
        base_level: int | str = _detect_admin_base_level(con, relation, cell_col)
    else:
        base_level = _detect_grid_base_level(con, relation, scheme, cell_col)

    out_geometry = _infer_out_geometry(con, relation, columns)
    return AggregateInfo(
        scheme=scheme,
        cell_column=cell_col,
        base_level=base_level,
        rollup_columns=rollups,
        out_geometry=out_geometry,
        dropped_columns=dropped,
    )


def detect_aggregate_file(
    input_parquet: str, cell_column: str | None = None, scheme: str | None = None
) -> AggregateInfo:
    """File-path convenience wrapper around :func:`detect_aggregate_info`."""
    with aggregate_connection(input_parquet) as (con, relation):
        return detect_aggregate_info(con, relation, cell_column, scheme)


@contextmanager
def aggregate_connection(input_parquet: str, verbose: bool = False):
    """Yield ``(con, relation)`` for reading an aggregate file.

    Shared connection boilerplate for every consumer of an aggregate file
    (overview building, pyramid planning, file detection): spatial + httpfs
    connection, lon/lat axis order, and a ``read_parquet`` relation string.
    """
    url = safe_file_url(input_parquet, verbose)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
    try:
        con.execute("SET geometry_always_xy = true")
        yield con, f"read_parquet('{url}', hive_partitioning=false, union_by_name=true)"
    finally:
        con.close()
        # Release GDAL/spatial native handles before the next spatial
        # connection opens; leaked native state can segfault sibling
        # xdist tests.
        gc.collect()
