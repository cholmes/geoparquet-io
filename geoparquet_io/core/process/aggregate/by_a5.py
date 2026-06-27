#!/usr/bin/env python3
"""A5-cell aggregation for `gpio process aggregate a5`."""

from __future__ import annotations

import pyarrow.parquet as pq

from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import safe_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import configure_verbose, debug, info, success
from geoparquet_io.core.process.aggregate.common import (
    VALID_OUT_GEOMETRY,
    build_breakdown_column_names,
    build_breakdown_select,
    build_metric_select,
    geometry_to_geom_expr,
    parse_metrics,
    resolve_breakdown_values,
)

# DOUBLE[2][] boundary array -> closed WKB polygon
_POLY_WKB = (
    "ST_AsWKB(ST_MakePolygon(ST_MakeLine("
    "list_transform(list_append({pts}, {pts}[1]), p -> ST_Point(p[1], p[2]))"
    ")))"
)


def _read_source_sql(con, input_url: str, geom_col: str) -> str:
    """Source relation exposing the original columns plus a GEOMETRY ``__geom``.

    Detects whether the input geometry column is read as GEOMETRY (real GeoParquet)
    or BLOB (plain WKB) so it works on both.
    """
    read_rel = f"read_parquet('{input_url}', hive_partitioning=false, union_by_name=true)"
    geom_expr = geometry_to_geom_expr(con, read_rel, geom_col)
    return f"SELECT *, {geom_expr} AS __geom FROM {read_rel}"


def _build_a5_query(
    con,
    source_sql: str,
    resolution: int,
    metric: str | None,
    breakdown: str | None,
    breakdown_limit: int,
    out_geometry: str,
    a5_column_name: str,
) -> str:
    """Build the full a5 aggregation SQL from a source SQL relation exposing __geom."""
    metrics = parse_metrics(metric)

    # Keyed relation: every feature tagged with its a5 cell id.
    keyed_sql = (
        f"SELECT *, a5_lonlat_to_cell("
        f"ST_X(ST_Centroid(__geom)), ST_Y(ST_Centroid(__geom)), {resolution}"
        f") AS __key FROM ({source_sql})"
    )

    # Breakdown columns (resolved against the keyed source so the column exists).
    # Materialize the keyed relation once when a breakdown is requested so that
    # resolve_breakdown_values and the aggregation both read from the same temp
    # table rather than re-running the full key-assignment expression twice.
    breakdown_select = ""
    if breakdown:
        con.execute(f"CREATE TEMP TABLE __agg_keyed AS {keyed_sql}")
        keyed_ref = "SELECT * FROM __agg_keyed"
        top_values, has_other = resolve_breakdown_values(con, keyed_ref, breakdown, breakdown_limit)
        colmap = build_breakdown_column_names(top_values, reserved={"count_other"})
        breakdown_select = build_breakdown_select(breakdown, colmap, has_other)
    else:
        keyed_ref = keyed_sql

    # Aggregate SELECT list.
    agg_parts = [f'__key AS "{a5_column_name}"', "COUNT(*) AS count"]
    metric_select = build_metric_select(metrics)
    if metric_select:
        agg_parts.append(metric_select)
    if breakdown_select:
        agg_parts.append(breakdown_select)
    agg_sql = f"SELECT {', '.join(agg_parts)} FROM ({keyed_ref}) GROUP BY __key"

    return _wrap_with_geometry(agg_sql, a5_column_name, out_geometry)


def aggregate_by_a5(
    input_parquet: str,
    output_parquet: str,
    resolution: int | None = None,
    auto: bool = False,
    target_per_cell: int = 10000,
    max_cells: int = 500000,
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
    from geoparquet_io.core.partition.auto_resolution import calculate_auto_resolution

    if auto and resolution is not None:
        raise InvalidParameterError(
            "resolution",
            "Pass either --resolution or --auto, not both",
        )
    if not auto and resolution is None:
        raise InvalidParameterError(
            "resolution",
            "A5 aggregation requires --resolution or --auto",
        )
    if out_geometry not in VALID_OUT_GEOMETRY:
        raise InvalidParameterError(
            "out_geometry",
            f"Invalid value '{out_geometry}'. Valid: {', '.join(sorted(VALID_OUT_GEOMETRY))}",
        )
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
    if not 0 <= resolution <= 30:
        raise InvalidParameterError(
            "resolution",
            f"A5 resolution must be 0-30, got {resolution}",
        )

    input_url = safe_file_url(input_parquet, verbose)
    geom_col = find_primary_geometry_column(input_parquet, verbose) or "geometry"

    con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
    try:
        con.execute("INSTALL a5 FROM community")
        con.execute("LOAD a5")
        con.execute("SET geometry_always_xy = true")

        source_sql = _read_source_sql(con, input_url, geom_col)
        final_sql = _build_a5_query(
            con,
            source_sql,
            resolution,
            metric,
            breakdown,
            breakdown_limit,
            out_geometry,
            a5_column_name,
        )
        if show_sql or verbose:
            debug(final_sql)

        result = con.execute(final_sql).arrow().read_all()
    finally:
        con.close()

    # Report features that had no assignable cell (NULL/empty geometry).
    a5_ids = result.column(a5_column_name).to_pylist()
    if None in a5_ids:
        unassigned = result.column("count")[a5_ids.index(None)].as_py()
        info(f"{unassigned} features had no assignable a5 cell (NULL/empty geometry)")

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
    if out_geometry not in VALID_OUT_GEOMETRY:
        raise InvalidParameterError(
            "out_geometry",
            f"Invalid value '{out_geometry}'. Valid: {', '.join(sorted(VALID_OUT_GEOMETRY))}",
        )
    if not 0 <= resolution <= 30:
        raise InvalidParameterError(
            "resolution",
            f"A5 resolution must be 0-30, got {resolution}",
        )

    geom_col = geometry_column or "geometry"
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.execute("INSTALL a5 FROM community")
        con.execute("LOAD a5")
        con.execute("SET geometry_always_xy = true")
        con.register("__agg_input", table)
        geom_expr = geometry_to_geom_expr(con, "__agg_input", geom_col)
        source_sql = (
            f"SELECT * EXCLUDE ({quote_identifier(geom_col)}), "
            f"{geom_expr} AS __geom FROM __agg_input"
        )
        final_sql = _build_a5_query(
            con,
            source_sql,
            resolution,
            metric,
            breakdown,
            breakdown_limit,
            out_geometry,
            a5_column_name,
        )
        return con.execute(final_sql).arrow().read_all()
    finally:
        con.close()


def _wrap_with_geometry(agg_sql: str, a5_column_name: str, out_geometry: str) -> str:
    """Add geometry/centroid columns derived from the a5 cell id.

    Rows whose cell id is NULL (features with empty/NULL geometry that could not be
    assigned a cell) get NULL geometry. The NULL guard is at the output CASE so
    DuckDB short-circuits and never feeds a degenerate boundary to ST_MakePolygon.
    """
    if out_geometry == "none":
        return agg_sql

    qcol = quote_identifier(a5_column_name)
    poly = f"CASE WHEN {qcol} IS NULL THEN NULL ELSE {_POLY_WKB.format(pts='__pts')} END"
    centroid = f"CASE WHEN {qcol} IS NULL THEN NULL ELSE ST_AsWKB(ST_Point(__ll[1], __ll[2])) END"

    if out_geometry == "polygon":
        geom_cols = f"{poly} AS geometry"
    elif out_geometry == "centroid":
        geom_cols = f"{centroid} AS geometry"
    else:  # both
        geom_cols = f"{poly} AS geometry, {centroid} AS centroid"

    return (
        f"SELECT a.* EXCLUDE (__pts, __ll), {geom_cols} "
        f"FROM (SELECT *, a5_cell_to_boundary({qcol}) AS __pts, "
        f"a5_cell_to_lonlat({qcol}) AS __ll FROM ({agg_sql})) a"
    )
