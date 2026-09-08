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

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.crs_utils import (
    crs_transform_sql_expr,
    extract_crs_from_parquet,
    extract_crs_from_table,
    is_geographic_crs,
)
from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    get_duckdb_connection,
    load_community_extension,
    quote_identifier,
    sql_path,
    validate_where_clause,
    where_sql_fragment,
)
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import configure_verbose, debug, info, success, warn
from geoparquet_io.core.process.aggregate.common import (
    VALID_OUT_GEOMETRY,
    aggregate_source_relation,
    build_breakdown_column_names,
    build_breakdown_select,
    build_metric_select,
    geometry_to_geom_expr,
    resolve_breakdown_values,
    resolve_metric_column_types,
    validate_agg_columns,
    validate_metric_nodata,
)
from geoparquet_io.core.remote import needs_httpfs


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


# Cell rings that straddle the antimeridian must stay contiguous. Vertices are
# unwrapped relative to the first, so a ring may legitimately run past +/-180,
# which is what a5_cell_to_boundary already emits (for example -180.2).
#
# Without this, h3_cell_to_boundary_wkt wraps every vertex into [-180, 180] and
# the ring tears: a cell near Fiji comes back spanning 359 degrees of longitude
# and a renderer draws it as a band across the whole map.
#
# `{bnd}` is an open ring as DOUBLE[2][]; the ring is closed here.
UNWRAPPED_POLY_WKB = (
    "ST_AsWKB(ST_MakePolygon(ST_MakeLine(list_transform("
    "list_append({bnd}, {bnd}[1]), p -> ST_Point("
    "CASE WHEN p[1] - {bnd}[1][1] > 180 THEN p[1] - 360 "
    "WHEN p[1] - {bnd}[1][1] < -180 THEN p[1] + 360 "
    "ELSE p[1] END, p[2])))))"
)

# Internal column aliases used while building the aggregation. Any input column
# with one of these names is dropped from the SELECT * passthrough so a generated
# column can never be shadowed by a same-named user column. ("__geom" is kept
# reserved for inputs that carry a stale column from earlier gpio versions.)
_RESERVED_INTERNAL = ("__geom", "__pt", "__key", "__bnd", "__ll")

# --bucket-point mode keywords; any other value names an existing point column.
BUCKET_POINT_GEOMETRY = "geometry"
BUCKET_POINT_BBOX = "bbox"

# Struct fields a bbox covering column must expose.
_BBOX_STRUCT_FIELDS = frozenset({"xmin", "ymin", "xmax", "ymax"})


def _relation_columns(con, relation: str) -> set[str]:
    """Column names exposed by ``relation``."""
    return {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()}


def build_exclude_clause(
    con: duckdb.DuckDBPyConnection, relation: str, columns: tuple[str, ...]
) -> str:
    """Return an `` EXCLUDE (...)`` clause dropping the ``columns`` that actually
    exist in ``relation``; empty string when none do.

    Checking existence keeps the clause safe for inputs that lack a column —
    e.g. attribute+bbox-only files with no geometry column at all (#567).
    """
    cols = _relation_columns(con, relation)
    drop: list[str] = []
    for name in columns:
        if name in cols and name not in drop:
            drop.append(name)
    return f" EXCLUDE ({', '.join(quote_identifier(c) for c in drop)})" if drop else ""


def _exclude_reserved(con, relation: str, extra: tuple[str, ...] = ()) -> str:
    """Return an `` EXCLUDE (...)`` clause dropping input columns that would collide
    with the internal aliases (or names in ``extra``); empty string if none clash."""
    return build_exclude_clause(con, relation, (*extra, *_RESERVED_INTERNAL))


def _validate_bucket_point_args(bucket_point: str, bbox_column: str | None) -> None:
    """Reject option combinations that would silently do the wrong thing."""
    if not bucket_point:
        raise InvalidParameterError(
            "bucket-point",
            "bucket point must be 'geometry', 'bbox', or the name of an existing "
            "point column; got an empty string",
        )
    if bbox_column and bucket_point != BUCKET_POINT_BBOX:
        raise InvalidParameterError(
            "bucket-point", "a bbox column only applies when the bucket point is 'bbox'"
        )


def _validate_bbox_struct_column(con, relation: str, bbox_column: str) -> None:
    """Ensure ``bbox_column`` exists in ``relation`` and is a bbox covering struct."""
    if bbox_column not in _relation_columns(con, relation):
        raise InvalidParameterError(
            "bbox-column", f"bbox column '{bbox_column}' not found in the input"
        )
    qbox = quote_identifier(bbox_column)
    try:
        fields = {
            row[0] for row in con.execute(f"DESCRIBE SELECT {qbox}.* FROM {relation}").fetchall()
        }
    except duckdb.Error:  # not a struct -- .* expansion does not bind
        fields = set()
    missing = _BBOX_STRUCT_FIELDS - fields
    if missing:
        raise InvalidParameterError(
            "bbox-column",
            f"column '{bbox_column}' is not a bbox covering struct: expected "
            f"xmin/ymin/xmax/ymax fields, missing {'/'.join(sorted(missing))}",
        )


def _validate_point_column(con, relation: str, bucket_point: str) -> None:
    """Ensure a point-column ``bucket_point`` names an existing column."""
    if bucket_point in _relation_columns(con, relation):
        return
    hint = ""
    lowered = bucket_point.lower()
    if lowered in (BUCKET_POINT_BBOX, BUCKET_POINT_GEOMETRY):
        hint = f" (mode keywords are lowercase — did you mean '{lowered}'?)"
    raise InvalidParameterError(
        "bucket-point",
        f"point column '{bucket_point}' not found in the input{hint}; the bucket "
        "point must be 'geometry', 'bbox', or the name of an existing point column",
    )


def _bbox_center_lon_sql(qbox: str, source_crs) -> str:
    """Longitude of the bbox center, wraparound-aware for the antimeridian.

    GeoJSON/GeoParquet coverings encode an antimeridian crossing as
    ``xmin > xmax`` (Fiji: xmin=179.9, xmax=-179.9); the naive midpoint would
    land near lon 0. For those rows take the +360-shifted midpoint and wrap
    values > 180 back into (-180, 180]. Only geographic CRSs get this
    treatment: in a projected CRS ``xmin > xmax`` cannot encode a dateline
    crossing, so the plain midpoint is always correct there.
    """
    plain = f"({qbox}.xmin + {qbox}.xmax) / 2.0"
    if not is_geographic_crs(source_crs):
        return plain
    shifted = f"(({qbox}.xmin + {qbox}.xmax + 360.0) / 2.0)"
    wrapped = f"CASE WHEN {shifted} > 180.0 THEN {shifted} - 360.0 ELSE {shifted} END"
    return f"CASE WHEN {qbox}.xmin > {qbox}.xmax THEN {wrapped} ELSE {plain} END"


def bucket_point_expr(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    geom_col: str,
    source_crs: dict | str | None,
    bucket_point: str,
    bbox_column: str | None,
) -> tuple[str, tuple[str, ...]]:
    """Build the keying-point expression for a source relation.

    Returns ``(pt_expr, exclude_columns)``. ``pt_expr`` yields a lon/lat POINT
    (reprojected from a non-CRS84 ``source_crs``, #525). In ``bbox`` and
    point-column modes the main geometry column is excluded from the passthrough
    SELECT so Parquet projection pushdown never reads its column chunks (#567).
    The bbox/point column is validated against ``relation`` so a typo fails with
    a clear error instead of a late binder error.
    """
    _validate_bucket_point_args(bucket_point, bbox_column)
    if bucket_point == BUCKET_POINT_GEOMETRY:
        geom_expr = crs_transform_sql_expr(
            geometry_to_geom_expr(con, relation, geom_col), source_crs
        )
        return f"ST_Centroid({geom_expr})", ()
    if bucket_point == BUCKET_POINT_BBOX:
        if not bbox_column:
            raise InvalidParameterError(
                "bbox-column",
                "bucket point 'bbox' requires a bbox column name (none given or detected)",
            )
        _validate_bbox_struct_column(con, relation, bbox_column)
        qbox = quote_identifier(bbox_column)
        lon = _bbox_center_lon_sql(qbox, source_crs)
        pt = f"ST_Point({lon}, ({qbox}.ymin + {qbox}.ymax) / 2.0)"
        # The bbox covering column is stored in the file's CRS, same as geometry.
        return crs_transform_sql_expr(pt, source_crs), (geom_col,)
    # Any other value names an existing (point) geometry column. ST_Centroid is a
    # no-op for points and keeps non-point columns keyable rather than erroring.
    _validate_point_column(con, relation, bucket_point)
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
    if metrics or breakdown:
        # Fail with a clear message (not a DuckDB binder error) when a requested
        # metric/breakdown column doesn't exist -- especially `--metric count`,
        # which is a no-op request since count is always emitted. Runs before the
        # type resolution below so a missing column reports as missing, not as a
        # non-numeric metric.
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ({source_sql})").fetchall()}
        validate_agg_columns(cols, metrics, breakdown)
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
    """Return the bbox covering column to key from, auto-detecting when not given.

    Detection consults the file's GeoParquet ``covering.bbox`` metadata first,
    falling back to naming conventions (see ``check_bbox_structure``).
    """
    from geoparquet_io.core.common import check_bbox_structure

    if bbox_column:
        return bbox_column
    detected = check_bbox_structure(input_parquet, verbose).get("bbox_column_name")
    if not detected:
        raise InvalidParameterError(
            "bucket-point",
            "bucket_point='bbox' requires a bbox covering column, but none was "
            "detected. Pass bbox_column or use bucket_point='geometry'.",
        )
    return detected


def _validate_bbox_column_in_table(table, bbox_column: str) -> None:
    """Ensure an explicit table-path ``bbox_column`` exists and is a bbox struct."""
    import pyarrow as pa

    try:
        field = table.schema.field(bbox_column)
    except KeyError:
        raise InvalidParameterError(
            "bbox-column", f"bbox column '{bbox_column}' not found in the table"
        ) from None
    if not pa.types.is_struct(field.type) or not _BBOX_STRUCT_FIELDS.issubset(
        {f.name for f in field.type}
    ):
        raise InvalidParameterError(
            "bbox-column",
            f"column '{bbox_column}' is not a bbox covering struct with xmin/ymin/xmax/ymax fields",
        )


def _resolve_bbox_column_for_table(table, bbox_column: str | None) -> str:
    """Table-path variant of bbox column resolution (Arrow schema detection)."""
    from geoparquet_io.core.common import _detect_bbox_column_from_table

    if bbox_column:
        _validate_bbox_column_in_table(table, bbox_column)
        return bbox_column
    detected = _detect_bbox_column_from_table(table)
    if not detected:
        raise InvalidParameterError(
            "bucket-point",
            "bucket_point='bbox' requires a bbox covering column, but none was "
            "detected. Pass bbox_column or use bucket_point='geometry'.",
        )
    return detected


def _warn_files_missing_column(con, input_path: str, column: str) -> None:
    """Warn when a glob input has files that lack the keying ``column``.

    With ``union_by_name=true`` those files' rows get NULL for the column, so
    all of their features silently land in the unassigned bucket. Detection
    (and up-front validation) only sees the merged schema, hence this check.

    ``input_path`` is a RAW path: this function does its own escaping (#718).
    """
    if not any(ch in input_path for ch in "*?["):
        return
    col_lit = _escape_sql_string(column)
    try:
        total, with_col = con.execute(
            f"SELECT count(DISTINCT file_name), "
            f"count(DISTINCT file_name) FILTER (WHERE name = '{col_lit}') "
            f"FROM parquet_schema({sql_path(input_path)})"
        ).fetchone()
    except duckdb.Error:  # pragma: no cover - best-effort diagnostics only
        return
    if total and with_col < total:
        warn(
            f"{total - with_col} of {total} input files lack column '{column}'; "
            f"their rows have no keying value and will be counted as unassigned"
        )


def _validate_keying_columns_for_file(
    input_parquet: str, bucket_point: str, bbox_column: str | None, verbose: bool
) -> None:
    """Validate the bbox/point keying column against the file schema up front.

    Runs before any expensive work (the --auto probe, grid extension install,
    admin dataset setup) so a typo'd or wrongly-shaped column fails immediately
    with a clear error rather than a late binder error. Also warns when a glob
    input is heterogeneous (some files lack the keying column).
    """
    if bucket_point == BUCKET_POINT_GEOMETRY:
        return
    input_url = resolve_file_url(input_parquet, verbose=False)
    relation = f"read_parquet({sql_path(input_url)}, hive_partitioning=false, union_by_name=true)"
    con = get_duckdb_connection(load_spatial=False, load_httpfs=needs_httpfs(input_parquet))
    try:
        if bucket_point == BUCKET_POINT_BBOX and bbox_column:
            _validate_bbox_struct_column(con, relation, bbox_column)
            _warn_files_missing_column(con, input_parquet, bbox_column)
        elif bucket_point != BUCKET_POINT_BBOX:
            _validate_point_column(con, relation, bucket_point)
            _warn_files_missing_column(con, input_parquet, bucket_point)
    finally:
        con.close()


def _unassigned_reason(bucket_point: str, bbox_column: str | None) -> str:
    """Describe why rows had no keying point, per bucket-point mode.

    When keying came from a bbox or point column, the geometry itself may be
    perfectly intact -- do not blame it.
    """
    if bucket_point == BUCKET_POINT_BBOX:
        return f"NULL '{bbox_column}' bbox value"
    if bucket_point != BUCKET_POINT_GEOMETRY:
        return f"NULL/empty '{bucket_point}' point"
    return "NULL/empty geometry"


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
    _validate_keying_columns_for_file(input_parquet, bucket_point, bbox_column, verbose)
    resolution = _resolve_resolution(
        scheme, input_parquet, resolution, auto, target_per_cell, max_cells, verbose, where=where
    )

    input_url = resolve_file_url(input_parquet, verbose)
    geom_col = find_primary_geometry_column(input_parquet, verbose) or "geometry"
    source_crs = extract_crs_from_parquet(input_parquet, verbose)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
    try:
        load_community_extension(con, scheme.extension, feature=f"{scheme.name} aggregation")
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

    # Report features that had no assignable cell (no keying point).
    ids = result.column(cell_column).to_pylist()
    if None in ids:
        unassigned = result.column("count")[ids.index(None)].as_py()
        info(
            f"{unassigned} features had no assignable {scheme.name} cell "
            f"({_unassigned_reason(bucket_point, bbox_column)})"
        )

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
        load_community_extension(con, scheme.extension, feature=f"{scheme.name} aggregation")
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
