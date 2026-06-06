#!/usr/bin/env python3

"""
Add admin division columns from multiple datasets.

This module extends the add_country_codes functionality to support
multiple admin datasets with hierarchical level support.
"""

from dataclasses import dataclass

from geoparquet_io.core.admin_datasets import AdminDatasetFactory
from geoparquet_io.core.common import (
    check_bbox_structure,
    get_parquet_metadata,
    resolve_input_bbox_info,
    write_parquet_with_metadata,
)
from geoparquet_io.core.duckdb_utils import build_spatial_join_condition, quote_identifier
from geoparquet_io.core.file_utils import handle_output_overwrite, safe_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, info, progress, success, warn
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.remote import _sanitize_url_for_logging, is_remote_url

_TEMP_TABLE_PREFIX = "_gpio_"


@dataclass
class _WriteConfig:
    compression: str = "ZSTD"
    compression_level: int | None = None
    row_group_size_mb: float | None = None
    row_group_rows: int | None = None
    profile: str | None = None
    geoparquet_version: str | None = None


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

    q_geom = quote_identifier(admin_geom_col)
    q_bbox = quote_identifier(admin_bbox_col) if admin_bbox_col else q_geom
    # _gpio_admin_rid gives each admin row a stable id for deterministic
    # tiebreaking in deduplication (base-table rowid isn't exposed on a subquery).
    return f"""(
        SELECT {q_geom}, {q_bbox}, {subquery_cols_str},
               ROW_NUMBER() OVER () AS _gpio_admin_rid
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


def _format_input_ref(input_url):
    """Format input reference for SQL — quote file paths, leave table names bare."""
    if input_url.startswith(_TEMP_TABLE_PREFIX):
        return input_url
    return f"'{input_url}'"


def _build_spatial_join_query(
    input_url,
    admin_subquery,
    admin_select_clause,
    input_geom_col,
    admin_geom_col,
    input_bbox_col=None,
    admin_bbox_col=None,
    deduplicate=False,
):
    """Build spatial join query with optional deduplication.

    When both sides expose a bbox column, a cheap bbox-overlap pre-filter is
    ANDed before the expensive ST_Intersects check (see PR #460).
    """
    input_ref = _format_input_ref(input_url)
    q_input_geom = quote_identifier(input_geom_col)
    q_admin_geom = quote_identifier(admin_geom_col)
    join_condition = build_spatial_join_condition(
        input_geom_col,
        admin_geom_col,
        input_bbox_col=input_bbox_col,
        target_bbox_col=admin_bbox_col,
    )
    join_clause = f"ON {join_condition}"

    if deduplicate:
        return f"""
    WITH _gpio_input AS (
        SELECT *, ROW_NUMBER() OVER () AS _gpio_row_id,
               ST_Centroid({q_input_geom}) AS _gpio_centroid
        FROM {input_ref}
    )
    SELECT * EXCLUDE (_gpio_row_id, _gpio_centroid) FROM (
        SELECT
            a.*,
            {admin_select_clause}
        FROM _gpio_input a
        LEFT JOIN {admin_subquery} b
        {join_clause}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY a._gpio_row_id
            ORDER BY ST_Contains(b.{q_admin_geom}, a._gpio_centroid) DESC NULLS LAST,
                     b._gpio_admin_rid
        ) = 1
    )
"""

    return f"""
    SELECT
        a.*,
        {admin_select_clause}
    FROM {input_ref} a
    LEFT JOIN {admin_subquery} b
    {join_clause}
"""


def _add_extent_filter(con, input_url, input_bbox_col, input_geom_col, admin_bbox_col, verbose):
    """Add bbox extent filter to admin where clauses."""
    if not admin_bbox_col:
        return None

    q_admin_bbox = quote_identifier(admin_bbox_col)
    input_ref = _format_input_ref(input_url)
    if input_bbox_col:
        q_input_bbox = quote_identifier(input_bbox_col)
        extent_query = f"""
            SELECT
                MIN({q_input_bbox}.xmin) as xmin,
                MAX({q_input_bbox}.xmax) as xmax,
                MIN({q_input_bbox}.ymin) as ymin,
                MAX({q_input_bbox}.ymax) as ymax
            FROM {input_ref}
        """
    else:
        q_input_geom = quote_identifier(input_geom_col)
        extent_query = f"""
            SELECT
                MIN(ST_XMin({q_input_geom})) as xmin,
                MAX(ST_XMax({q_input_geom})) as xmax,
                MIN(ST_YMin({q_input_geom})) as ymin,
                MAX(ST_YMax({q_input_geom})) as ymax
            FROM {input_ref}
        """

    extent = con.execute(extent_query).fetchone()
    if extent and all(v is not None for v in extent):
        xmin, xmax, ymin, ymax = extent
        extent_filter = f"""
            ({q_admin_bbox}.xmin <= {xmax} AND
             {q_admin_bbox}.xmax >= {xmin} AND
             {q_admin_bbox}.ymin <= {ymax} AND
             {q_admin_bbox}.ymax >= {ymin})
        """
        if verbose:
            debug(
                f"Filtering admin boundaries to input extent: ({xmin:.2f}, {ymin:.2f}, {xmax:.2f}, {ymax:.2f})"
            )
        return extent_filter
    return None


def _handle_bbox_optimization(input_parquet, input_bbox_info, add_bbox_flag, verbose):
    """Handle bbox column setup for spatial pre-filtering of admin boundaries.

    Bbox columns serve two pre-filters: narrowing admin boundaries to the input
    extent (WHERE clause) and the cheap per-row bbox-overlap test ANDed before
    ST_Intersects in the join ON clause (see build_spatial_join_condition).
    """
    if input_bbox_info.get("status") == "native":
        return input_bbox_info

    if input_bbox_info["status"] != "optimal":
        warn(
            "\nWarning: Input file could benefit from a bbox column for extent filtering:\n"
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


def _get_result_stats(con, output_parquet, dataset, levels, verbose):
    """Get statistics about the results."""
    output_col_names = [dataset.get_output_column_name(level) for level in levels]
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

    # Per-level datasets resolve sources per-level in add_admin_divisions_multi
    if dataset.supports_per_level_sources():
        admin_source = None
    else:
        admin_source = get_or_cache_dataset(dataset, no_cache=no_cache, verbose=verbose)

    if verbose and admin_source:
        debug(f"Data source: {admin_source}")

    input_geom_col = find_primary_geometry_column(input_parquet, verbose)
    admin_geom_col = dataset.get_geometry_column()

    input_bbox_info, input_bbox_col = resolve_input_bbox_info(input_parquet, verbose)

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
    deduplicate=False,
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

    query = _build_spatial_join_query(
        input_url,
        admin_subquery,
        admin_select_clause,
        input_geom_col,
        admin_geom_col,
        input_bbox_col=input_bbox_col,
        admin_bbox_col=admin_bbox_col,
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

    if input_bbox_col and admin_bbox_col:
        info("-- Main spatial join query (bbox-overlap pre-filter before ST_Intersects)")
    else:
        info("-- Main spatial join query (ST_Intersects via DuckDB SPATIAL_JOIN operator)")

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


def _execute_per_level_joins(
    con,
    dataset,
    levels,
    partition_columns,
    input_url,
    admin_geom_col,
    admin_bbox_col,
    input_geom_col,
    input_bbox_col,
    output_parquet,
    metadata,
    dry_run,
    verbose,
    write_config,
    prefix,
    no_cache,
):
    """Run separate spatial joins per admin level for per-level-source datasets.

    Each level joins against its own cache file, chaining results through
    DuckDB temp tables so overlapping country/region polygons don't cause
    row multiplication.
    """
    current_source = input_url

    for i, (level, col) in enumerate(zip(levels, partition_columns, strict=True)):
        level_admin_source = dataset.get_source_for_level(level, no_cache=no_cache)
        is_last = i == len(levels) - 1

        level_query, _ = _build_query_components(
            con,
            dataset,
            [level],
            [col],
            current_source,
            level_admin_source,
            admin_geom_col,
            admin_bbox_col,
            input_geom_col,
            input_bbox_col,
            verbose,
            dry_run,
            prefix=prefix,
            deduplicate=True,
        )

        if dry_run:
            output_col = dataset.get_output_column_name(level, prefix)
            progress(f"\n-- Step {i + 1}: Add {output_col} from {level} boundaries")
            progress(f"-- Source: {level_admin_source}")
            progress(level_query)
            continue

        if verbose:
            debug(f"Spatial join: adding {level} from {level_admin_source}...")

        if is_last:
            write_parquet_with_metadata(
                con,
                level_query,
                output_parquet,
                original_metadata=metadata,
                compression=write_config.compression,
                compression_level=write_config.compression_level,
                row_group_size_mb=write_config.row_group_size_mb,
                row_group_rows=write_config.row_group_rows,
                verbose=verbose,
                profile=write_config.profile,
                geoparquet_version=write_config.geoparquet_version,
            )
        else:
            temp_table = f"{_TEMP_TABLE_PREFIX}admin_step_{i}"
            con.execute(f"CREATE OR REPLACE TEMP TABLE {temp_table} AS {level_query}")
            current_source = temp_table

    if dry_run:
        return None, None, None

    total_features, features_with_admin, unique_counts = _get_result_stats(
        con, output_parquet, dataset, levels, verbose
    )
    return total_features, features_with_admin, unique_counts


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
    """
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

    write_config = _WriteConfig(
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        profile=profile,
        geoparquet_version=geoparquet_version,
    )

    # Create DuckDB connection with ambient S3 config from dataset
    from geoparquet_io.core.duckdb_utils import s3_config_scope

    with s3_config_scope(dataset.get_s3_config()):
        con = _setup_duckdb_connection()
        try:
            # Get total input count (skip in dry-run)
            if not dry_run:
                total_count = con.execute(f"SELECT COUNT(*) FROM '{input_url}'").fetchone()[0]
                progress(f"Processing {total_count:,} input features...")

            if dataset.supports_per_level_sources():
                total_features, features_with_admin, unique_counts = _execute_per_level_joins(
                    con,
                    dataset,
                    levels,
                    partition_columns,
                    input_url,
                    admin_geom_col,
                    admin_bbox_col,
                    input_geom_col,
                    input_bbox_col,
                    output_parquet,
                    metadata,
                    dry_run,
                    verbose,
                    write_config,
                    prefix,
                    no_cache,
                )
                if dry_run:
                    return
            else:
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
                    prefix=prefix,
                )

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

                if verbose:
                    debug("Performing spatial join with admin boundaries...")

                write_parquet_with_metadata(
                    con,
                    query,
                    output_parquet,
                    original_metadata=metadata,
                    compression=write_config.compression,
                    compression_level=write_config.compression_level,
                    row_group_size_mb=write_config.row_group_size_mb,
                    row_group_rows=write_config.row_group_rows,
                    verbose=verbose,
                    profile=write_config.profile,
                    geoparquet_version=write_config.geoparquet_version,
                )

                total_features, features_with_admin, unique_counts = _get_result_stats(
                    con, output_parquet, dataset, levels, verbose
                )
        finally:
            con.close()

    progress("\nResults:")
    progress(
        f"- Added admin division data to {features_with_admin:,} of {total_features:,} features"
    )
    for level, count in unique_counts:
        progress(f"- Found {count:,} unique {level} values")

    success(f"\nSuccessfully wrote output to: {output_parquet}")
