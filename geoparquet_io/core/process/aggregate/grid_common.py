#!/usr/bin/env python3
"""Shared grid-cell aggregation engine for `gpio process aggregate <grid>`.

A :class:`GridScheme` captures the few SQL fragments and parameters that differ
between discrete global grid systems (a5, h3, ...). Everything else -- reading the
source, geometry-type detection, the GROUP BY + metric + breakdown assembly, the
NULL-cell guard, unassigned logging, and writing -- is shared here so each scheme
module stays a thin descriptor.
"""

from __future__ import annotations

import gc
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.crs_utils import (
    crs_transform_sql_expr,
    extract_crs_from_parquet,
    extract_crs_from_table,
)
from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    quote_identifier,
    validate_where_clause,
    where_sql_fragment,
)
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import safe_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import configure_verbose, debug, info, success
from geoparquet_io.core.process.aggregate.common import (
    VALID_OUT_GEOMETRY,
    aggregate_source_relation,
    build_breakdown_column_names,
    build_breakdown_select,
    build_metric_select,
    geometry_to_geom_expr,
    resolve_breakdown_values,
    resolve_metric_column_types,
    validate_metric_nodata,
)


@dataclass(frozen=True)
class GridScheme:
    """Per-grid SQL fragments and parameters for the shared aggregation engine.

    Templates use ``str.format`` placeholders:

    - ``key_template``: ``{pt}`` (a POINT GEOMETRY expression), ``{res}`` -> cell id
    - ``boundary_template``: ``{cell}`` -> per-row boundary intermediate
    - ``latlng_template``: ``{cell}`` -> per-row centroid intermediate
    - ``poly_wkb_template``: ``{bnd}`` (boundary intermediate alias) -> WKB polygon
    - ``centroid_wkb_template``: ``{ll}`` (centroid intermediate alias) -> WKB point

    ``name`` doubles as the ``calculate_auto_resolution`` index type and the noun
    used in log messages.
    """

    name: str
    extension: str
    min_resolution: int
    max_resolution: int
    default_column: str
    key_template: str
    boundary_template: str
    latlng_template: str
    poly_wkb_template: str
    centroid_wkb_template: str


# Internal column aliases used while building the aggregation. Any input column
# with one of these names is dropped from the SELECT * passthrough so a generated
# column can never be shadowed by a same-named user column. ("__geom" is kept
# reserved for inputs that carry a stale column from earlier gpio versions.)
_RESERVED_INTERNAL = ("__geom", "__pt", "__key", "__bnd", "__ll")

# --bucket-point mode keywords; any other value names an existing point column.
BUCKET_POINT_GEOMETRY = "geometry"
BUCKET_POINT_BBOX = "bbox"


def _exclude_reserved(con, relation: str, extra: tuple[str, ...] = ()) -> str:
    """Return an `` EXCLUDE (...)`` clause dropping input columns that would collide
    with the internal aliases (or names in ``extra``); empty string if none clash."""
    cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()}
    drop: list[str] = []
    for name in (*extra, *_RESERVED_INTERNAL):
        if name in cols and name not in drop:
            drop.append(name)
    return f" EXCLUDE ({', '.join(quote_identifier(c) for c in drop)})" if drop else ""


def _validate_bucket_point_args(bucket_point: str, bbox_column: str | None) -> None:
    """Reject option combinations that would silently do the wrong thing."""
    if bbox_column and bucket_point != BUCKET_POINT_BBOX:
        raise InvalidParameterError("bucket-point", "--bbox-column requires --bucket-point bbox")


def bucket_point_expr(
    con, relation: str, geom_col: str, source_crs, bucket_point: str, bbox_column: str | None
) -> tuple[str, tuple[str, ...]]:
    """Build the keying-point expression for a source relation.

    Returns ``(pt_expr, exclude_columns)``. ``pt_expr`` yields a lon/lat POINT
    (reprojected from a non-CRS84 ``source_crs``, #525). In ``bbox`` and
    point-column modes the main geometry column is excluded from the passthrough
    SELECT so Parquet projection pushdown never reads its column chunks (#567).
    """
    if bucket_point == BUCKET_POINT_GEOMETRY:
        geom_expr = crs_transform_sql_expr(
            geometry_to_geom_expr(con, relation, geom_col), source_crs
        )
        return f"ST_Centroid({geom_expr})", ()
    if bucket_point == BUCKET_POINT_BBOX:
        qbox = quote_identifier(bbox_column)
        pt = f"ST_Point(({qbox}.xmin + {qbox}.xmax) / 2.0, ({qbox}.ymin + {qbox}.ymax) / 2.0)"
        # The bbox covering column is stored in the file's CRS, same as geometry.
        return crs_transform_sql_expr(pt, source_crs), (geom_col,)
    # Any other value names an existing (point) geometry column. ST_Centroid is a
    # no-op for points and keeps non-point columns keyable rather than erroring.
    point_expr = crs_transform_sql_expr(
        geometry_to_geom_expr(con, relation, bucket_point), source_crs
    )
    return f"ST_Centroid({point_expr})", (geom_col,)


def read_grid_source_sql(
    con,
    input_url: str,
    geom_col: str,
    source_crs=None,
    where: str | None = None,
    bucket_point: str = BUCKET_POINT_GEOMETRY,
    bbox_column: str | None = None,
) -> str:
    """Source relation exposing the original columns plus a keying POINT ``__pt``.

    Detects whether the input geometry column is read as GEOMETRY (real GeoParquet)
    or BLOB (plain WKB) so it works on both. Grid keying expects lon/lat, so a
    non-CRS84 ``source_crs`` is reprojected to OGC:CRS84 before keying (#525); a
    CRS-less / already-CRS84 input is left untouched.

    ``where`` is applied to this source scan, so keying, metrics, and breakdowns
    all see only the filtered rows (#568). The caller validates the clause. Hive
    partition columns are visible to it (#612); see
    :func:`aggregate_source_relation`.

    ``bucket_point`` selects where ``__pt`` comes from: the geometry centroid
    (default), the center of a bbox covering column, or an existing point column
    (#567) -- the latter two skip reading the geometry column entirely.
    """
    read_rel = aggregate_source_relation(input_url)
    pt_expr, exclude = bucket_point_expr(
        con, read_rel, geom_col, source_crs, bucket_point, bbox_column
    )
    return (
        f"SELECT *{_exclude_reserved(con, read_rel, exclude)}, {pt_expr} AS __pt "
        f"FROM {read_rel}{where_sql_fragment(where)}"
    )


def build_grid_query(
    con,
    scheme: GridScheme,
    source_sql: str,
    resolution: int,
    cell_column: str,
    metric: str | None,
    breakdown: str | None,
    breakdown_limit: int,
    out_geometry: str,
    metric_nodata: str | None = None,
) -> str:
    """Build the full grid aggregation SQL from a source relation exposing ``__pt``."""
    metrics, nodata_values = validate_metric_nodata(metric, metric_nodata)
    # Resolve metric column types so sentinel literals match the column's actual
    # precision (REAL vs DOUBLE, #613) and non-numeric columns fail up-front.
    column_types = resolve_metric_column_types(con, source_sql, metrics) if nodata_values else None

    key_expr = scheme.key_template.format(pt="__pt", res=resolution)
    keyed_sql = f"SELECT *, {key_expr} AS __key FROM ({source_sql})"

    # Materialize the keyed relation once when a breakdown is requested so that
    # resolve_breakdown_values and the aggregation both read from the same temp
    # table rather than re-running the key-assignment expression twice.
    breakdown_select = ""
    if breakdown:
        con.execute(f"CREATE TEMP TABLE __agg_keyed AS {keyed_sql}")
        keyed_ref = "SELECT * FROM __agg_keyed"
        top_values, has_other = resolve_breakdown_values(con, keyed_ref, breakdown, breakdown_limit)
        colmap = build_breakdown_column_names(top_values, reserved={"count_other"})
        breakdown_select = build_breakdown_select(breakdown, colmap, has_other)
    else:
        keyed_ref = keyed_sql

    agg_parts = [f"__key AS {quote_identifier(cell_column)}", "COUNT(*) AS count"]
    metric_select = build_metric_select(
        metrics, nodata_values=nodata_values, column_types=column_types
    )
    if metric_select:
        agg_parts.append(metric_select)
    if breakdown_select:
        agg_parts.append(breakdown_select)
    agg_sql = f"SELECT {', '.join(agg_parts)} FROM ({keyed_ref}) GROUP BY __key"

    return wrap_grid_geometry(agg_sql, scheme, cell_column, out_geometry)


def wrap_grid_geometry(
    agg_sql: str, scheme: GridScheme, cell_column: str, out_geometry: str
) -> str:
    """Add geometry/centroid columns derived from the grid cell id.

    Rows whose cell id is NULL (features with empty/NULL geometry that could not be
    assigned a cell) get NULL geometry. The boundary/centroid intermediates are
    NULL-guarded (so a scheme's cell function is never called on a NULL cell), and
    the output is guarded again so DuckDB short-circuits the geometry constructor
    for NULL-cell rows.
    """
    if out_geometry == "none":
        return agg_sql

    qcol = quote_identifier(cell_column)
    poly_expr = scheme.poly_wkb_template.format(bnd="__bnd")
    centroid_expr = scheme.centroid_wkb_template.format(ll="__ll")
    poly = f"CASE WHEN {qcol} IS NULL THEN NULL ELSE {poly_expr} END"
    centroid = f"CASE WHEN {qcol} IS NULL THEN NULL ELSE {centroid_expr} END"

    if out_geometry == "polygon":
        geom_cols = f"{poly} AS geometry"
    elif out_geometry == "centroid":
        geom_cols = f"{centroid} AS geometry"
    else:  # both
        geom_cols = f"{poly} AS geometry, {centroid} AS centroid"

    boundary = scheme.boundary_template.format(cell=qcol)
    latlng = scheme.latlng_template.format(cell=qcol)
    return (
        f"SELECT a.* EXCLUDE (__bnd, __ll), {geom_cols} "
        f"FROM (SELECT *, "
        f"CASE WHEN {qcol} IS NULL THEN NULL ELSE {boundary} END AS __bnd, "
        f"CASE WHEN {qcol} IS NULL THEN NULL ELSE {latlng} END AS __ll "
        f"FROM ({agg_sql})) a"
    )


def _resolve_resolution(
    scheme,
    input_parquet,
    resolution,
    auto,
    target_per_cell,
    max_cells,
    verbose,
    where: str | None = None,
):
    """Resolve the explicit or auto resolution and validate against scheme bounds.

    ``where`` is forwarded to the auto-resolution sizing so --auto picks the grid
    from the *filtered* row count, not the raw file size (#568).
    """
    from geoparquet_io.core.partition import auto_resolution as _auto_resolution

    if auto and resolution is not None:
        raise InvalidParameterError("resolution", "Pass either --resolution or --auto, not both")
    if not auto and resolution is None:
        raise InvalidParameterError(
            "resolution", f"{scheme.name.upper()} aggregation requires --resolution or --auto"
        )
    if auto:
        resolution = _auto_resolution.calculate_auto_resolution(
            input_parquet,
            scheme.name,
            target_rows_per_partition=target_per_cell,
            max_partitions=max_cells,
            verbose=verbose,
            where=where,
        )
        if verbose:
            debug(f"Auto-selected {scheme.name} resolution {resolution}")
    if not scheme.min_resolution <= resolution <= scheme.max_resolution:
        raise InvalidParameterError(
            "resolution",
            f"{scheme.name.upper()} resolution must be "
            f"{scheme.min_resolution}-{scheme.max_resolution}, got {resolution}",
        )
    return resolution


def _resolve_bbox_column_for_file(
    input_parquet: str, bbox_column: str | None, verbose: bool
) -> str:
    """Return the bbox covering column to key from, auto-detecting when not given."""
    from geoparquet_io.core.common import check_bbox_structure

    if bbox_column:
        return bbox_column
    detected = check_bbox_structure(input_parquet, verbose).get("bbox_column_name")
    if not detected:
        raise InvalidParameterError(
            "bucket-point",
            "--bucket-point bbox requires a bbox covering column, but none was "
            "detected. Pass --bbox-column NAME or use --bucket-point geometry.",
        )
    return detected


def _resolve_bbox_column_for_table(table, bbox_column: str | None) -> str:
    """Table-path variant of bbox column resolution (Arrow schema detection)."""
    from geoparquet_io.core.common import _detect_bbox_column_from_table

    if bbox_column:
        return bbox_column
    detected = _detect_bbox_column_from_table(table)
    if not detected:
        raise InvalidParameterError(
            "bucket-point",
            "bucket_point='bbox' requires a bbox covering column, but none was "
            "detected. Pass bbox_column or use bucket_point='geometry'.",
        )
    return detected


def _validate_out_geometry(out_geometry: str) -> None:
    if out_geometry not in VALID_OUT_GEOMETRY:
        raise InvalidParameterError(
            "out_geometry",
            f"Invalid value '{out_geometry}'. Valid: {', '.join(sorted(VALID_OUT_GEOMETRY))}",
        )


def aggregate_grid_file(
    scheme: GridScheme,
    input_parquet: str,
    output_parquet: str,
    *,
    resolution: int | None = None,
    auto: bool = False,
    target_per_cell: int = 10000,
    max_cells: int = 500000,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    cell_column: str | None = None,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    show_sql: bool = False,
    where: str | None = None,
    metric_nodata: str | None = None,
    bucket_point: str = BUCKET_POINT_GEOMETRY,
    bbox_column: str | None = None,
) -> None:
    """Aggregate a GeoParquet file into grid cells. Writes the output file."""
    configure_verbose(verbose)
    cell_column = cell_column or scheme.default_column
    _validate_out_geometry(out_geometry)
    if where:
        validate_where_clause(where)
    # Validate metric/nodata pairing before any expensive setup (--auto scanning,
    # CRS reads, connection + community-extension install).
    validate_metric_nodata(metric, metric_nodata)
    _validate_bucket_point_args(bucket_point, bbox_column)
    if bucket_point == BUCKET_POINT_BBOX:
        bbox_column = _resolve_bbox_column_for_file(input_parquet, bbox_column, verbose)
    resolution = _resolve_resolution(
        scheme, input_parquet, resolution, auto, target_per_cell, max_cells, verbose, where=where
    )

    input_url = safe_file_url(input_parquet, verbose)
    geom_col = find_primary_geometry_column(input_parquet, verbose) or "geometry"
    source_crs = extract_crs_from_parquet(input_parquet, verbose)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
    try:
        con.execute(f"INSTALL {scheme.extension} FROM community")
        con.execute(f"LOAD {scheme.extension}")
        con.execute("SET geometry_always_xy = true")

        source_sql = read_grid_source_sql(
            con,
            input_url,
            geom_col,
            source_crs,
            where=where,
            bucket_point=bucket_point,
            bbox_column=bbox_column,
        )
        final_sql = build_grid_query(
            con,
            scheme,
            source_sql,
            resolution,
            cell_column,
            metric,
            breakdown,
            breakdown_limit,
            out_geometry,
            metric_nodata=metric_nodata,
        )
        if show_sql or verbose:
            debug(final_sql)
        result = con.execute(final_sql).arrow().read_all()
    finally:
        con.close()
        # Release GDAL/spatial native handles before the next spatial connection
        # opens; leaked native state can segfault sibling xdist tests.
        gc.collect()

    # Report features that had no assignable cell (NULL/empty geometry).
    ids = result.column(cell_column).to_pylist()
    if None in ids:
        unassigned = result.column("count")[ids.index(None)].as_py()
        info(f"{unassigned} features had no assignable {scheme.name} cell (NULL/empty geometry)")

    if out_geometry == "none":
        if compression_level is not None:
            pq.write_table(
                result,
                output_parquet,
                compression=compression,
                compression_level=compression_level,
            )
        else:
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
    success(f"Aggregated to {result.num_rows} {scheme.name} cells -> {output_parquet}")


def aggregate_grid_table(
    scheme: GridScheme,
    table,
    *,
    resolution: int,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    cell_column: str | None = None,
    geometry_column: str | None = None,
    where: str | None = None,
    metric_nodata: str | None = None,
    bucket_point: str = BUCKET_POINT_GEOMETRY,
    bbox_column: str | None = None,
) -> pa.Table:
    """Aggregate an in-memory Arrow table into grid cells. Returns a new Arrow table."""
    cell_column = cell_column or scheme.default_column
    _validate_out_geometry(out_geometry)
    if where:
        validate_where_clause(where)
    # Validate metric/nodata pairing before connection setup and extension install.
    validate_metric_nodata(metric, metric_nodata)
    _validate_bucket_point_args(bucket_point, bbox_column)
    if bucket_point == BUCKET_POINT_BBOX:
        bbox_column = _resolve_bbox_column_for_table(table, bbox_column)
    if not scheme.min_resolution <= resolution <= scheme.max_resolution:
        raise InvalidParameterError(
            "resolution",
            f"{scheme.name.upper()} resolution must be "
            f"{scheme.min_resolution}-{scheme.max_resolution}, got {resolution}",
        )

    geom_col = geometry_column or "geometry"
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.execute(f"INSTALL {scheme.extension} FROM community")
        con.execute(f"LOAD {scheme.extension}")
        con.execute("SET geometry_always_xy = true")
        con.register("__agg_input", table)
        source_crs = extract_crs_from_table(table, geom_col)
        pt_expr, exclude = bucket_point_expr(
            con, "__agg_input", geom_col, source_crs, bucket_point, bbox_column
        )
        source_sql = (
            f"SELECT *{_exclude_reserved(con, '__agg_input', (geom_col, *exclude))}, "
            f"{pt_expr} AS __pt FROM __agg_input{where_sql_fragment(where)}"
        )
        final_sql = build_grid_query(
            con,
            scheme,
            source_sql,
            resolution,
            cell_column,
            metric,
            breakdown,
            breakdown_limit,
            out_geometry,
            metric_nodata=metric_nodata,
        )
        return con.execute(final_sql).arrow().read_all()
    finally:
        con.close()
        # Release GDAL/spatial native handles before the next spatial connection
        # opens; leaked native state can segfault sibling xdist tests.
        gc.collect()
