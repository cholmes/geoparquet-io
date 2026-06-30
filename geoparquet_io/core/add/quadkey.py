#!/usr/bin/env python3

from __future__ import annotations

import mercantile
import pyarrow as pa

from geoparquet_io.core.common import (
    get_bbox_advice,
    get_parquet_metadata,
    write_parquet_with_metadata,
)
from geoparquet_io.core.constants import DEFAULT_QUADKEY_COLUMN_NAME, DEFAULT_QUADKEY_RESOLUTION
from geoparquet_io.core.crs_utils import (
    crs_transform_sql_expr,
    extract_crs_from_parquet,
    extract_crs_from_table,
)
from geoparquet_io.core.duckdb_metadata import get_column_names
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.exceptions import GeoParquetError, InvalidParameterError
from geoparquet_io.core.file_utils import handle_output_overwrite, safe_file_url
from geoparquet_io.core.geometry_detection import (
    STANDARD_GEOMETRY_NAMES,
    find_primary_geometry_column,
)
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.remote import (
    _sanitize_url_for_logging,
    is_remote_url,
    needs_httpfs,
    setup_aws_profile_if_needed,
    validate_profile_for_urls,
)
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import (
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)


def _lat_lon_to_quadkey(lat: float, lon: float, level: int) -> str:
    """Convert latitude and longitude to a quadkey string using mercantile."""
    tile = mercantile.tile(lon, lat, level)
    return mercantile.quadkey(tile)


def add_quadkey_table(
    table: pa.Table,
    quadkey_column_name: str = DEFAULT_QUADKEY_COLUMN_NAME,
    resolution: int = DEFAULT_QUADKEY_RESOLUTION,
    use_centroid: bool = False,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add a quadkey column to an Arrow Table.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        quadkey_column_name: Name for the quadkey column (default: 'quadkey')
        resolution: Quadkey zoom level (0-23). Default: 13
        use_centroid: Force using geometry centroid even if bbox exists
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with quadkey column added

    Raises:
        ValueError: If resolution is not an integer between 0 and 23
    """
    # Validate resolution before any DuckDB operations
    resolution = int(resolution)
    if resolution < 0 or resolution > 23:
        raise ValueError(f"resolution must be between 0 and 23 inclusive, got {resolution}")

    # Find geometry column
    geom_col = geometry_column or find_geometry_column_from_table(table)
    if not geom_col:
        geom_col = "geometry"

    # Quadkeys require lon/lat degrees. A projected input is reprojected to
    # OGC:CRS84 before keying (issue #525); the stored bbox column is in the
    # source CRS, so it cannot be used as the keying location when reprojecting.
    source_crs = extract_crs_from_table(table, geom_col)

    # Check if bbox column exists
    use_bbox = False
    bbox_col = None
    if not use_centroid and source_crs is None:
        for name in ["bbox", "bounds", "bounding_box"]:
            if name in table.column_names:
                use_bbox = True
                bbox_col = name
                break

    # Register table and execute query using context manager for safe cleanup
    with get_duckdb_connection(load_spatial=True, load_httpfs=False) as con:
        # Register Python UDF
        con.create_function(
            "lat_lon_to_quadkey",
            _lat_lon_to_quadkey,
            ["DOUBLE", "DOUBLE", "INTEGER"],
            "VARCHAR",
        )

        con.register("__input_table", table)

        # Check if geometry column is BLOB (needs conversion)
        columns_info = con.execute("DESCRIBE __input_table").fetchall()
        geom_is_blob = any(col[0] == geom_col and "BLOB" in col[1].upper() for col in columns_info)

        if geom_is_blob and geom_col in table.column_names:
            # Quote column names to handle special characters (colons, spaces, etc.)
            other_cols = [f'"{c}"' for c in table.column_names if c != geom_col]
            col_defs = other_cols + [f'ST_GeomFromWKB("{geom_col}") AS "{geom_col}"']
            view_query = (
                f"CREATE VIEW __input_view AS SELECT {', '.join(col_defs)} FROM __input_table"
            )
            con.execute(view_query)
            source_ref = "__input_view"
        else:
            source_ref = "__input_table"

        # Build lat/lon expressions
        if use_bbox and bbox_col:
            lat_expr = f'(("{bbox_col}".ymin + "{bbox_col}".ymax) / 2.0)'
            lon_expr = f'(("{bbox_col}".xmin + "{bbox_col}".xmax) / 2.0)'
        else:
            centroid = crs_transform_sql_expr(f'ST_Centroid("{geom_col}")', source_crs)
            lat_expr = f"ST_Y({centroid})"
            lon_expr = f"ST_X({centroid})"

        # Get non-geometry columns
        other_cols = [f'"{c}"' for c in table.column_names if c != geom_col]
        select_cols = ", ".join(other_cols) if other_cols else ""

        # Build SELECT with geometry converted back to WKB
        if select_cols:
            query = f"""
                SELECT {select_cols},
                       ST_AsWKB("{geom_col}") AS "{geom_col}",
                       lat_lon_to_quadkey({lat_expr}, {lon_expr}, {resolution}) AS "{quadkey_column_name}"
                FROM {source_ref}
            """
        else:
            query = f"""
                SELECT ST_AsWKB("{geom_col}") AS "{geom_col}",
                       lat_lon_to_quadkey({lat_expr}, {lon_expr}, {resolution}) AS "{quadkey_column_name}"
                FROM {source_ref}
            """
        result = con.execute(query).arrow().read_all()

        # Preserve metadata
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)

        return result


def add_quadkey_column(
    input_parquet: str,
    output_parquet: str | None = None,
    quadkey_column_name: str = DEFAULT_QUADKEY_COLUMN_NAME,
    resolution: int = DEFAULT_QUADKEY_RESOLUTION,
    use_centroid: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
) -> None:
    """
    Add a quadkey column to a GeoParquet file.

    Computes quadkey tile IDs based on geometry location. By default, uses the
    bbox column midpoint if available, otherwise falls back to geometry centroid.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Args:
        input_parquet: Path to the input parquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        quadkey_column_name: Name for the quadkey column (default: 'quadkey')
        resolution: Quadkey zoom level (0-23). Default: 13
        use_centroid: Force using geometry centroid even if bbox exists
        dry_run: Whether to print SQL commands without executing them
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
    """
    # Check for streaming mode (stdin input or stdout output)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming and not dry_run:
        _add_quadkey_streaming(
            input_parquet,
            output_parquet,
            quadkey_column_name,
            resolution,
            use_centroid,
            verbose,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
        )
        return

    # File-based mode
    _add_quadkey_file_based(
        input_parquet,
        output_parquet,
        quadkey_column_name,
        resolution,
        use_centroid,
        dry_run,
        verbose,
        compression,
        compression_level,
        row_group_size_mb,
        row_group_rows,
        profile,
        geoparquet_version,
        overwrite,
    )


def _add_quadkey_streaming(
    input_path: str,
    output_path: str | None,
    quadkey_column_name: str,
    resolution: int,
    use_centroid: bool,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
) -> None:
    """Handle streaming input/output for add_quadkey."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    # Validate resolution
    if not 0 <= resolution <= 23:
        raise InvalidParameterError("resolution", f"must be between 0 and 23, got {resolution}")

    # Detect a projected source CRS so the centroid is reprojected to OGC:CRS84
    # before keying. stdin carries no readable CRS, so it is treated as CRS84.
    source_crs = None if is_stdin(input_path) else extract_crs_from_parquet(input_path, verbose)

    with open_input(input_path, verbose=verbose) as (source, metadata, is_stream, con):
        # Register Python UDF for quadkey generation
        con.create_function(
            "lat_lon_to_quadkey",
            _lat_lon_to_quadkey,
            ["DOUBLE", "DOUBLE", "INTEGER"],
            "VARCHAR",
        )

        # Get column names from query result (works with both table names and read_parquet)
        sample = con.execute(f"SELECT * FROM {source} LIMIT 0").description
        col_names = [col[0] for col in sample]

        # Find geometry column
        geom_col = None
        for name in STANDARD_GEOMETRY_NAMES:
            if name in col_names:
                geom_col = name
                break
        if not geom_col:
            geom_col = "geometry"

        # Check for bbox column. The stored bbox is in the source CRS, so it
        # cannot be used as the keying location when reprojecting.
        bbox_col = None
        if not use_centroid and source_crs is None:
            for name in ["bbox", "bounds", "bounding_box"]:
                if name in col_names:
                    bbox_col = name
                    break

        # Build lat/lon expressions
        if bbox_col:
            lat_expr = f'(("{bbox_col}".ymin + "{bbox_col}".ymax) / 2.0)'
            lon_expr = f'(("{bbox_col}".xmin + "{bbox_col}".xmax) / 2.0)'
        else:
            centroid = crs_transform_sql_expr(f'ST_Centroid("{geom_col}")', source_crs)
            lat_expr = f"ST_Y({centroid})"
            lon_expr = f"ST_X({centroid})"

        query = f"""
            SELECT *,
                   lat_lon_to_quadkey({lat_expr}, {lon_expr}, {resolution}) AS "{quadkey_column_name}"
            FROM {source}
        """

        if verbose:
            debug(f"Streaming quadkey query: {query}")

        # Write output
        write_output(
            con,
            query,
            output_path,
            original_metadata=metadata,
            geometry_column=geom_col,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
        )

        if not should_stream_output(output_path):
            success(
                f"Successfully added quadkey column '{quadkey_column_name}' "
                f"(zoom level {resolution}) to: {output_path}"
            )


def _add_quadkey_file_based(
    input_parquet: str,
    output_parquet: str | None,
    quadkey_column_name: str,
    resolution: int,
    use_centroid: bool,
    dry_run: bool,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
    overwrite: bool = False,
) -> None:
    """Handle file-based add_quadkey operation."""
    configure_verbose(verbose)

    # Check if output file exists and handle overwrite (fixes issue #278)
    handle_output_overwrite(output_parquet, overwrite)

    # Validate resolution
    if not 0 <= resolution <= 23:
        raise InvalidParameterError("resolution", f"must be between 0 and 23, got {resolution}")

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_parquet, output_parquet)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_parquet, output_parquet)

    # Get safe URL for input file
    input_url = safe_file_url(input_parquet, verbose)

    # Get geometry column
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Quadkeys require lon/lat degrees. A projected input is reprojected to
    # OGC:CRS84 before keying (issue #525); the stored bbox column is in the
    # source CRS, so it cannot be used as the keying location when reprojecting.
    source_crs = extract_crs_from_parquet(input_parquet, verbose)

    # Check if column already exists (skip in dry-run)
    if not dry_run:
        column_names = get_column_names(input_url)
        if quadkey_column_name in column_names:
            raise GeoParquetError(
                f"Column '{quadkey_column_name}' already exists in the file. "
                f"Please choose a different name."
            )

    # Determine whether to use bbox or centroid
    use_bbox = False
    bbox_col = None
    if not use_centroid and source_crs is None:
        bbox_advice = get_bbox_advice(input_parquet, "bounds_calculation", verbose)
        if bbox_advice["has_bbox_column"]:
            use_bbox = True
            bbox_col = bbox_advice["bbox_column_name"]
            if verbose:
                debug(f"Using bbox column '{bbox_col}' for quadkey calculation")
        elif bbox_advice["needs_warning"]:
            warn(bbox_advice["message"] + " - using geometry centroid for quadkey calculation")
            for suggestion in bbox_advice["suggestions"]:
                info(f"Tip: {suggestion}")
    elif source_crs is not None and verbose:
        debug("Projected CRS detected — reprojecting centroid to OGC:CRS84 for quadkey keying")

    # Dry-run mode header
    if dry_run:
        warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
        display_input = (
            _sanitize_url_for_logging(input_url) if is_remote_url(input_url) else input_url
        )
        display_output = (
            _sanitize_url_for_logging(output_parquet)
            if is_remote_url(output_parquet)
            else output_parquet
        )
        info(f"-- Input file: {display_input}")
        info(f"-- Output file: {display_output}")
        info(f"-- Geometry column: {geom_col}")
        info(f"-- New column: {quadkey_column_name}")
        info(f"-- Resolution (zoom level): {resolution}")
        method = "bbox midpoint" if use_bbox else "geometry centroid"
        info(f"-- Calculation method: {method}")
        return

    # Get metadata before processing
    metadata, _ = get_parquet_metadata(input_parquet, verbose)

    if verbose:
        debug(f"Adding quadkey column '{quadkey_column_name}' at resolution {resolution}...")

    # Create DuckDB connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))

    try:
        # Register Python UDF for quadkey generation
        con.create_function(
            "lat_lon_to_quadkey",
            _lat_lon_to_quadkey,
            ["DOUBLE", "DOUBLE", "INTEGER"],
            "VARCHAR",
        )

        # Build the SQL expression based on calculation method
        if use_bbox:
            lat_expr = f"(({bbox_col}.ymin + {bbox_col}.ymax) / 2.0)"
            lon_expr = f"(({bbox_col}.xmin + {bbox_col}.xmax) / 2.0)"
        else:
            centroid = crs_transform_sql_expr(f"ST_Centroid({geom_col})", source_crs)
            lat_expr = f"ST_Y({centroid})"
            lon_expr = f"ST_X({centroid})"

        # Build SELECT query with new column
        query = f"""
            SELECT *,
                   lat_lon_to_quadkey({lat_expr}, {lon_expr}, {resolution}) AS {quadkey_column_name}
            FROM '{input_url}'
        """

        if verbose:
            debug(f"Query: {query}")

        if not dry_run:
            progress(f"Adding quadkey column '{quadkey_column_name}' (zoom level {resolution})...")

        # Prepare quadkey metadata for GeoParquet spec
        quadkey_metadata = {
            "covering": {"quadkey": {"column": quadkey_column_name, "resolution": resolution}}
        }

        # Write output with metadata
        write_parquet_with_metadata(
            con,
            query,
            output_parquet,
            original_metadata=metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
            custom_metadata=quadkey_metadata,
        )

        success(
            f"Successfully added quadkey column '{quadkey_column_name}' "
            f"(zoom level {resolution}) to: {output_parquet}"
        )

    finally:
        con.close()


if __name__ == "__main__":
    add_quadkey_column()
