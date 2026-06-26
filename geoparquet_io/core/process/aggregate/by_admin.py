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


def _get_admin_ref(dataset, con, level: str) -> str:
    """Return an SQL table-reference for the admin dataset at the given level.

    For datasets with per-level caches (Overture), uses the pre-filtered
    level-specific cache file directly.  For others, falls back to the
    generic prepare_data_source + read_parquet options path.
    """
    if dataset.supports_per_level_sources():
        path = dataset.get_source_for_level(level)
        return f"read_parquet('{path}')"
    admin_source = dataset.prepare_data_source(con)
    return _build_admin_table_reference(dataset, admin_source)


def _build_joined_sql(
    input_url: str,
    geom_col: str,
    admin_ref: str,
    code_col: str,
    name_col: str,
    admin_geom_col: str,
    admin_bbox_col: str | None = None,
) -> str:
    """Build the spatial-join SQL tagging each input feature with its admin region.

    The admin geometry column is GEOMETRY type in all supported datasets, so it
    is used directly in ST_Intersects.  The input geometry column is WKB binary
    (as written by DuckDB COPY / ST_AsWKB), so ST_GeomFromWKB is applied before
    computing the centroid.  The admin geometry is stored as WKB (ST_AsWKB) so
    that _wrap_admin_geometry can treat __admin_geom uniformly as WKB binary.

    When admin_bbox_col is provided, a cheap bbox prefilter is added to the ON
    clause so ST_Intersects is only evaluated for admin polygons whose bounding
    box contains the feature centroid point.
    """
    if admin_bbox_col:
        bbox_filter = (
            f"b.{admin_bbox_col}.xmin <= ST_X(s.__cen) AND "
            f"b.{admin_bbox_col}.xmax >= ST_X(s.__cen) AND "
            f"b.{admin_bbox_col}.ymin <= ST_Y(s.__cen) AND "
            f"b.{admin_bbox_col}.ymax >= ST_Y(s.__cen) AND "
        )
    else:
        bbox_filter = ""
    return f"""
        SELECT s.*,
               b."{code_col}" AS __admin_code,
               b."{name_col}" AS __admin_name,
               ST_AsWKB(b."{admin_geom_col}") AS __admin_geom
        FROM (
            SELECT *, ST_Centroid(ST_GeomFromWKB("{geom_col}")) AS __cen
            FROM read_parquet('{input_url}', hive_partitioning=false, union_by_name=true)
        ) s
        LEFT JOIN {admin_ref} b
          ON {bbox_filter}ST_Intersects(b."{admin_geom_col}", s.__cen)
    """


def _build_agg_sql(joined_sql: str, metrics, breakdown_select: str) -> str:
    """Build the GROUP BY aggregation on top of the spatial join."""
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
    return (
        f"SELECT {', '.join(agg_parts)} FROM ({joined_sql}) "
        f"GROUP BY COALESCE(__admin_code, 'unassigned')"
    )


def _wrap_admin_geometry(agg_sql: str, out_geometry: str) -> str:
    """Add geometry/centroid columns derived from the per-region admin polygon (WKB)."""
    if out_geometry == "none":
        return f"SELECT a.* EXCLUDE (__admin_geom) FROM ({agg_sql}) a"

    geom_wkb = "a.__admin_geom"  # WKB binary stored in __admin_geom
    centroid = f"ST_AsWKB(ST_Centroid(ST_GeomFromWKB({geom_wkb})))"
    if out_geometry == "polygon":
        geom_cols = f"{geom_wkb} AS geometry"
    elif out_geometry == "centroid":
        geom_cols = f"{centroid} AS geometry"
    else:  # both
        geom_cols = f"{geom_wkb} AS geometry, {centroid} AS centroid"
    return f"SELECT a.* EXCLUDE (__admin_geom), {geom_cols} FROM ({agg_sql}) a"


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
    """Aggregate input features by administrative region.

    Spatially joins each input feature's centroid against admin boundary polygons
    and rolls up per-region statistics (count + optional metrics and breakdowns).
    Features whose centroid falls outside every region form one ``unassigned``
    bucket (``admin_code='unassigned'``, null admin_name, null geometry).

    Args:
        input_parquet: Path to the input GeoParquet file.
        output_parquet: Path for the output aggregated GeoParquet file.
        level: Administrative level to aggregate at (e.g. ``country``, ``region``).
        metric: Comma-separated ``func:column`` metric specs (e.g. ``sum:area``).
        breakdown: Column name for pivot-style breakdown counts.
        breakdown_limit: Maximum number of breakdown values to pivot (default 20).
        out_geometry: One of ``polygon``, ``centroid``, ``both``, ``none``.
        dataset: Admin boundary dataset to use (default ``overture``).
        compression: Parquet compression codec (default ``ZSTD``).
        compression_level: Optional compression level.
        geoparquet_version: GeoParquet spec version to write.
        verbose: Enable verbose debug logging.
        show_sql: Log the final SQL query.
    """
    configure_verbose(verbose)
    if out_geometry not in VALID_OUT_GEOMETRY:
        raise InvalidParameterError(
            "out_geometry",
            f"Invalid value '{out_geometry}'. Valid: {', '.join(sorted(VALID_OUT_GEOMETRY))}",
        )
    metrics = parse_metrics(metric)

    admin_dataset, _boundary_columns = _setup_admin_dataset(dataset, verbose, [level])
    mapping = admin_dataset.get_level_column_mapping()
    code_col = mapping[level]
    # Overture per-level caches store only the code column (e.g. ISO-2 country code
    # like "FR"), not a separate human-readable name.  admin_name will therefore
    # equal the code value.  For richer datasets that expose a name column, override
    # name_col here when get_level_column_mapping returns distinct name keys.
    name_col = code_col
    admin_geom_col = admin_dataset.get_geometry_column()
    admin_bbox_col = admin_dataset.get_bbox_column()

    input_url = safe_file_url(input_parquet, verbose)
    geom_col = find_primary_geometry_column(input_parquet, verbose) or "geometry"

    con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
    try:
        con.execute("SET geometry_always_xy = true")
        admin_dataset.configure_s3(con)

        admin_ref = _get_admin_ref(admin_dataset, con, level)

        joined_sql = _build_joined_sql(
            input_url, geom_col, admin_ref, code_col, name_col, admin_geom_col, admin_bbox_col
        )

        breakdown_select = ""
        if breakdown:
            top_values, has_other = resolve_breakdown_values(
                con, joined_sql, breakdown, breakdown_limit
            )
            colmap = build_breakdown_column_names(top_values, reserved={"count_other"})
            breakdown_select = build_breakdown_select(breakdown, colmap, has_other)

        agg_sql = _build_agg_sql(joined_sql, metrics, breakdown_select)
        final_sql = _wrap_admin_geometry(agg_sql, out_geometry)

        if show_sql or verbose:
            debug(final_sql)
        result = con.execute(final_sql).arrow().read_all()

        # Derive unassigned count from the already-materialized result table to
        # avoid re-executing the (potentially expensive) spatial join a second time.
        codes = result.column("admin_code").to_pylist()
        if "unassigned" in codes:
            unassigned_count = result.column("count")[codes.index("unassigned")].as_py()
        else:
            unassigned_count = 0
        if unassigned_count:
            info(f"{unassigned_count} feature(s) fell outside all admin regions (-> 'unassigned')")
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
    success(f"Aggregated to {result.num_rows} admin regions -> {output_parquet}")
