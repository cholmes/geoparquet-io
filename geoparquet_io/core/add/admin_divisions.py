#!/usr/bin/env python3

"""
Add admin division columns from multiple datasets.

This module extends the add_country_codes functionality to support
multiple admin datasets with hierarchical level support.
"""

from geoparquet_io.core.add.spatial_join import build_bbox_condition
from geoparquet_io.core.admin_datasets import AdminDatasetFactory
from geoparquet_io.core.common import (
    check_bbox_structure,
    get_bbox_advice,
    get_parquet_metadata,
    write_parquet_with_metadata,
)
from geoparquet_io.core.file_utils import handle_output_overwrite, safe_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, info, progress, success, warn
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.remote import _sanitize_url_for_logging, is_remote_url


def _build_admin_subquery(
    dataset,
    levels,
    boundary_columns,
    admin_table_ref,
    admin_geom_col,
    admin_bbox_col,
    admin_where_clauses,
):
    """Build admin data subquery with filters."""
    admin_where_clause = ""
    if admin_where_clauses:
        admin_where_clause = "WHERE " + " AND ".join(admin_where_clauses)

    # Build column list for subquery - handle struct access
    subquery_cols = []
    for i, col in enumerate(boundary_columns):
        if "[" in col or "(" in col:
            subquery_cols.append(f"{col} as _col_{i}")
        else:
            subquery_cols.append(f'"{col}"')
    subquery_cols_str = ", ".join(subquery_cols)

    return f"""(
        SELECT {admin_geom_col}, {admin_bbox_col if admin_bbox_col else admin_geom_col}, {subquery_cols_str}
        FROM {admin_table_ref}
        {admin_where_clause}
    )"""


def _build_admin_select_clause(dataset, levels, partition_columns, prefix=None):
    """Build SELECT clause for admin columns with transformations."""
    admin_select_parts = []
    for i, (level, col) in enumerate(zip(levels, partition_columns, strict=True)):
        output_col_name = dataset.get_output_column_name(level, prefix=prefix)
        col_transform = dataset.get_column_transform(level)

        if col_transform:
            admin_select_parts.append(f'{col_transform} as "{output_col_name}"')
        elif "[" in col or "(" in col:
            admin_select_parts.append(f'b._col_{i} as "{output_col_name}"')
        else:
            admin_select_parts.append(f'b."{col}" as "{output_col_name}"')

    return ", ".join(admin_select_parts)


def _build_spatial_join_query(
    input_url,
    admin_subquery,
    admin_select_clause,
    input_bbox_col,
    admin_bbox_col,
    input_geom_col,
    admin_geom_col,
    input_has_native_geo=False,
    deduplicate=True,
):
    """Build spatial join query with optional bbox optimization and deduplication."""
    bbox_condition = build_bbox_condition(
        input_geom_col=input_geom_col,
        other_bbox_col=admin_bbox_col,
        input_bbox_col=input_bbox_col,
        input_has_native_geo=input_has_native_geo,
    )

    if bbox_condition:
        join_clause = (
            f"ON {bbox_condition}\n"
            f"        AND ST_Intersects(\n"
            f"            b.{admin_geom_col},\n"
            f"            a.{input_geom_col}\n"
            f"        )"
        )
    else:
        join_clause = f"ON ST_Intersects(b.{admin_geom_col}, a.{input_geom_col})"

    if deduplicate:
        return f"""
    WITH _gpio_input AS (
        SELECT *, ROW_NUMBER() OVER () AS _gpio_row_id
        FROM '{input_url}'
    )
    SELECT * EXCLUDE (_gpio_row_id) FROM (
        SELECT
            a.*,
            {admin_select_clause}
        FROM _gpio_input a
        LEFT JOIN {admin_subquery} b
        {join_clause}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY a._gpio_row_id
            ORDER BY ST_Area(ST_Intersection(b.{admin_geom_col}, a.{input_geom_col})) DESC NULLS LAST
        ) = 1
    )
"""

    return f"""
    SELECT
        a.*,
        {admin_select_clause}
    FROM '{input_url}' a
    LEFT JOIN {admin_subquery} b
    {join_clause}
"""


def _add_extent_filter(con, input_url, input_bbox_col, input_geom_col, admin_bbox_col, verbose):
    """Add bbox extent filter to admin where clauses."""
    if not admin_bbox_col:
        return None

    if input_bbox_col:
        extent_query = f"""
            SELECT
                MIN({input_bbox_col}.xmin) as xmin,
                MAX({input_bbox_col}.xmax) as xmax,
                MIN({input_bbox_col}.ymin) as ymin,
                MAX({input_bbox_col}.ymax) as ymax
            FROM '{input_url}'
        """
    else:
        extent_query = f"""
            SELECT
                MIN(ST_XMin("{input_geom_col}")) as xmin,
                MAX(ST_XMax("{input_geom_col}")) as xmax,
                MIN(ST_YMin("{input_geom_col}")) as ymin,
                MAX(ST_YMax("{input_geom_col}")) as ymax
            FROM '{input_url}'
        """

    extent = con.execute(extent_query).fetchone()
    if extent and all(v is not None for v in extent):
        xmin, xmax, ymin, ymax = extent
        extent_filter = f"""
            ({admin_bbox_col}.xmin <= {xmax} AND
             {admin_bbox_col}.xmax >= {xmin} AND
             {admin_bbox_col}.ymin <= {ymax} AND
             {admin_bbox_col}.ymax >= {ymin})
        """
        if verbose:
            debug(
                f"Filtering admin boundaries to input extent: ({xmin:.2f}, {ymin:.2f}, {xmax:.2f}, {ymax:.2f})"
            )
        return extent_filter
    return None


def _handle_bbox_optimization(input_parquet, input_bbox_info, add_bbox_flag, verbose):
    """Handle bbox optimization if needed."""
    # Skip for native geometry files - they use native stats instead of bbox pre-filtering
    if input_bbox_info.get("status") == "native":
        return input_bbox_info

    if input_bbox_info["status"] != "optimal":
        warn(
            "\nWarning: Input file could benefit from bbox optimization:\n"
            + input_bbox_info["message"]
        )
        if add_bbox_flag and not input_bbox_info["has_bbox_column"]:
            progress("Adding bbox column to input file...")
            from geoparquet_io.core.common import add_bbox

            add_bbox(input_parquet, "bbox", verbose)
            success("✓ Added bbox column and metadata to input file")
            return check_bbox_structure(input_parquet, verbose)
    return input_bbox_info


def _print_dry_run_header(
    input_url,
    admin_source,
    output_parquet,
    input_geom_col,
    admin_geom_col,
    input_bbox_col,
    admin_bbox_col,
):
    """Print dry-run mode header."""
    warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
    display_input = _sanitize_url_for_logging(input_url) if is_remote_url(input_url) else input_url
    display_admin = (
        _sanitize_url_for_logging(admin_source) if is_remote_url(admin_source) else admin_source
    )
    display_output = (
        _sanitize_url_for_logging(output_parquet)
        if is_remote_url(output_parquet)
        else output_parquet
    )
    info(f"-- Input file: {display_input}")
    info(f"-- Admin dataset: {display_admin}")
    info(f"-- Output file: {display_output}")
    info(f"-- Geometry columns: {input_geom_col} (input), {admin_geom_col} (admin)")
    info(
        f"-- Bbox columns: {input_bbox_col or 'none'} (input), {admin_bbox_col or 'none'} (admin)\n"
    )


def _get_result_stats(con, output_parquet, dataset, levels, verbose, prefix=None):
    """Get statistics about the results."""
    output_col_names = [dataset.get_output_column_name(level, prefix=prefix) for level in levels]
    admin_cols_check = " OR ".join([f'"{col}" IS NOT NULL' for col in output_col_names])

    stats_query = f"""
    SELECT
        COUNT(*) as total_features,
        COUNT(CASE WHEN {admin_cols_check} THEN 1 END) as features_with_admin
    FROM '{output_parquet}';
    """

    stats = con.execute(stats_query).fetchone()
    total_features = stats[0]
    features_with_admin = stats[1]

    unique_counts = []
    for level, output_col in zip(levels, output_col_names, strict=True):
        count_query = f"""
        SELECT COUNT(DISTINCT "{output_col}") as unique_count
        FROM '{output_parquet}'
        WHERE "{output_col}" IS NOT NULL;
        """
        result = con.execute(count_query).fetchone()
        unique_counts.append((level, result[0]))

    return total_features, features_with_admin, unique_counts


def _setup_dataset_and_columns(
    input_parquet, dataset_name, dataset_source, levels, verbose, no_cache=False
):
    """Setup dataset and get column information."""
    from geoparquet_io.core.admin_datasets import get_or_cache_dataset

    dataset = AdminDatasetFactory.create(dataset_name, dataset_source, verbose)

    if verbose:
        debug(f"\nUsing admin dataset: {dataset.get_dataset_name()}")
        debug(f"Adding admin levels: {', '.join(levels)}")

    dataset.validate_levels(levels)
    partition_columns = dataset.get_partition_columns(levels)

    input_url = safe_file_url(input_parquet, verbose)

    # Use caching for remote admin datasets (unless no_cache is specified)
    admin_source = get_or_cache_dataset(dataset, no_cache=no_cache, verbose=verbose)

    if verbose:
        debug(f"Data source: {admin_source}")

    input_geom_col = find_primary_geometry_column(input_parquet, verbose)
    admin_geom_col = dataset.get_geometry_column()

    # Check if we should skip bbox pre-filtering (for native geometry files)
    input_bbox_advice = get_bbox_advice(input_parquet, "spatial_filtering", verbose)
    input_has_native_geo = input_bbox_advice.get("has_native_geometry", False)
    if input_bbox_advice["skip_bbox_prefilter"]:
        if verbose:
            debug("Input has native geometry - will use geometry bounds for bbox pre-filter")
        input_bbox_info = {"status": "native", "bbox_column_name": None, "has_bbox_column": False}
        input_bbox_col = None
    else:
        input_bbox_info = check_bbox_structure(input_parquet, verbose)
        input_bbox_col = input_bbox_info["bbox_column_name"]

    admin_bbox_col = dataset.get_bbox_column()

    return (
        dataset,
        partition_columns,
        input_url,
        admin_source,
        input_geom_col,
        admin_geom_col,
        input_bbox_info,
        input_bbox_col,
        admin_bbox_col,
        input_has_native_geo,
    )


def _setup_duckdb_connection():
    """Create and configure DuckDB connection."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    return get_duckdb_connection(load_spatial=True, load_httpfs=True)


def _build_admin_where_clauses_list(
    con,
    dataset,
    levels,
    input_url,
    input_bbox_col,
    input_geom_col,
    admin_bbox_col,
    verbose,
    dry_run,
):
    """Build WHERE clauses for admin boundaries."""
    admin_where_clauses = []
    subtype_filter = dataset.get_subtype_filter(levels)
    if subtype_filter:
        admin_where_clauses.append(subtype_filter)
        if verbose and not dry_run:
            debug(f"Filtering admin boundaries: {subtype_filter}")

    extent_filter = _add_extent_filter(
        con, input_url, input_bbox_col, input_geom_col, admin_bbox_col, verbose and not dry_run
    )
    if extent_filter:
        admin_where_clauses.append(extent_filter)

    return admin_where_clauses


def _build_query_components(
    con,
    dataset,
    levels,
    partition_columns,
    input_url,
    admin_source,
    admin_geom_col,
    admin_bbox_col,
    input_geom_col,
    input_bbox_col,
    verbose,
    dry_run,
    prefix=None,
    input_has_native_geo=False,
    deduplicate=True,
):
    """Build all query components."""
    # Use provided admin_source (may be cached local path or remote URL)
    # Quote the path for SQL safety
    read_options = dataset.get_read_parquet_options()
    admin_table_ref = (
        f"read_parquet('{admin_source}', {', '.join([f'{k}={v}' for k, v in read_options.items()])})"
        if read_options
        else f"'{admin_source}'"
    )

    admin_where_clauses = _build_admin_where_clauses_list(
        con,
        dataset,
        levels,
        input_url,
        input_bbox_col,
        input_geom_col,
        admin_bbox_col,
        verbose,
        dry_run,
    )

    admin_select_clause = _build_admin_select_clause(
        dataset, levels, partition_columns, prefix=prefix
    )
    admin_subquery = _build_admin_subquery(
        dataset,
        levels,
        partition_columns,
        admin_table_ref,
        admin_geom_col,
        admin_bbox_col,
        admin_where_clauses,
    )

    if input_bbox_col and admin_bbox_col and verbose and not dry_run:
        debug("Using bbox columns for initial filtering...")
    elif input_has_native_geo and admin_bbox_col and not dry_run:
        progress("Using native geometry bounds for bbox pre-filtering...")
    elif not (input_bbox_col and admin_bbox_col) and not dry_run:
        progress("No bbox columns available, using full geometry intersection...")

    query = _build_spatial_join_query(
        input_url,
        admin_subquery,
        admin_select_clause,
        input_bbox_col,
        admin_bbox_col,
        input_geom_col,
        admin_geom_col,
        input_has_native_geo=input_has_native_geo,
        deduplicate=deduplicate,
    )

    return query, admin_source


def _handle_dry_run_mode(
    dry_run,
    input_url,
    admin_source,
    output_parquet,
    input_geom_col,
    admin_geom_col,
    input_bbox_col,
    admin_bbox_col,
    query,
    compression,
    compression_level,
):
    """Handle dry-run mode output."""
    if not dry_run:
        return False

    _print_dry_run_header(
        input_url,
        admin_source,
        output_parquet,
        input_geom_col,
        admin_geom_col,
        input_bbox_col,
        admin_bbox_col,
    )

    info("-- Main spatial join query")
    if input_bbox_col and admin_bbox_col:
        info("-- Using bbox columns for optimized spatial join")
    else:
        info("-- Using full geometry intersection (no bbox optimization)")

    if compression in ["GZIP", "ZSTD", "BROTLI"]:
        compression_str = f"{compression}:{compression_level}"
    else:
        compression_str = compression

    duckdb_compression = compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"
    display_query = f"""COPY ({query.strip()})
TO '{output_parquet}'
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""
    progress(display_query)

    info(f"\n-- Note: Using {compression_str} compression")
    info("-- Original metadata would also be preserved in the output file")
    return True


def add_admin_divisions_multi(
    input_parquet: str,
    output_parquet: str,
    dataset_name: str,
    levels: list[str],
    dataset_source: str | None = None,
    add_bbox_flag: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    prefix: str | None = None,
    no_cache: bool = False,
    vecorel: bool = False,
    all_matches: bool = False,
):
    """
    Add admin division columns from a multi-level admin dataset.

    Args:
        input_parquet: Input GeoParquet file (local or remote URL)
        output_parquet: Output GeoParquet file (local or remote URL)
        dataset_name: Name of admin dataset ("current", "gaul", "overture")
        levels: List of hierarchical levels to add as columns
        dataset_source: Optional custom path/URL to admin dataset
        add_bbox_flag: Automatically add bbox column if missing
        dry_run: Show SQL without executing
        verbose: Enable verbose output
        compression: Compression type
        compression_level: Compression level
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        prefix: Optional column name prefix (default: dataset name, use "admin" for admin: format)
        no_cache: Skip local cache and use remote dataset directly
        all_matches: Keep all matching boundaries per feature (default: deduplicate by largest overlap)
    """
    # When vecorel mode is active, override prefix to use Vecorel column names
    effective_prefix = "vecorel" if vecorel else prefix

    # Check if output file exists and handle overwrite (fixes issue #278)
    handle_output_overwrite(output_parquet, overwrite, input_parquet)

    # Check for partition input (not supported)
    require_single_file(input_parquet, "add admin-divisions")

    # Setup dataset and columns
    (
        dataset,
        partition_columns,
        input_url,
        admin_source,
        input_geom_col,
        admin_geom_col,
        input_bbox_info,
        input_bbox_col,
        admin_bbox_col,
        input_has_native_geo,
    ) = _setup_dataset_and_columns(
        input_parquet, dataset_name, dataset_source, levels, verbose, no_cache=no_cache
    )

    # Get metadata before processing (skip in dry-run)
    metadata = None
    if not dry_run:
        metadata, _ = get_parquet_metadata(input_parquet, verbose)
        input_bbox_info = _handle_bbox_optimization(
            input_parquet, input_bbox_info, add_bbox_flag, verbose
        )
        input_bbox_col = input_bbox_info["bbox_column_name"]

        if verbose:
            debug(f"Using geometry columns: {input_geom_col} (input), {admin_geom_col} (admin)")

    # Create DuckDB connection with ambient S3 config from dataset
    from geoparquet_io.core.duckdb_utils import s3_config_scope

    with s3_config_scope(dataset.get_s3_config()):
        con = _setup_duckdb_connection()
        try:
            # Get total input count (skip in dry-run)
            if not dry_run:
                total_count = con.execute(f"SELECT COUNT(*) FROM '{input_url}'").fetchone()[0]
                progress(f"Processing {total_count:,} input features...")

            # Build query components (use cached admin_source from _setup_dataset_and_columns)
            query, admin_source = _build_query_components(
                con,
                dataset,
                levels,
                partition_columns,
                input_url,
                admin_source,
                admin_geom_col,
                admin_bbox_col,
                input_geom_col,
                input_bbox_col,
                verbose,
                dry_run,
                prefix=effective_prefix,
                input_has_native_geo=input_has_native_geo,
                deduplicate=not all_matches,
            )

            # Handle dry-run mode
            if _handle_dry_run_mode(
                dry_run,
                input_url,
                admin_source,
                output_parquet,
                input_geom_col,
                admin_geom_col,
                input_bbox_col,
                admin_bbox_col,
                query,
                compression,
                compression_level,
            ):
                return

            # Execute the query using the common write method
            if verbose:
                debug("Performing spatial join with admin boundaries...")

            # Build Vecorel metadata if requested
            extra_kv = None
            if vecorel:
                from geoparquet_io.core.constants import (
                    VECOREL_ADMIN_SCHEMA,
                    build_collection_metadata,
                )

                extra_kv = build_collection_metadata([VECOREL_ADMIN_SCHEMA], metadata)

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
                extra_kv_metadata=extra_kv,
            )

            # Get statistics about the results
            total_features, features_with_admin, unique_counts = _get_result_stats(
                con, output_parquet, dataset, levels, verbose, prefix=effective_prefix
            )
        finally:
            con.close()

    # Ensure Vecorel-required id column exists
    if vecorel:
        from geoparquet_io.core.constants import ensure_vecorel_columns

        ensure_vecorel_columns(output_parquet, verbose)

    progress("\nResults:")
    progress(
        f"- Added admin division data to {features_with_admin:,} of {total_features:,} features"
    )
    for level, count in unique_counts:
        progress(f"- Found {count:,} unique {level} values")

    success(f"\nSuccessfully wrote output to: {output_parquet}")
