#!/usr/bin/env python3

"""
Hierarchical admin partition functionality.

This module provides partitioning by administrative boundaries through a two-step process:
1. Spatial join with remote admin boundaries dataset to add admin columns
2. Partition the enriched data by those admin columns
"""

from __future__ import annotations

import os
import shutil

import duckdb

from geoparquet_io.core.admin_datasets import AdminDatasetFactory
from geoparquet_io.core.common import (
    check_bbox_structure,
    get_parquet_metadata,
)
from geoparquet_io.core.crs_utils import crs_transform_sql_expr, extract_crs_from_parquet
from geoparquet_io.core.duckdb_utils import quote_identifier
from geoparquet_io.core.exceptions import PartitionError
from geoparquet_io.core.file_utils import safe_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, progress, success, warn
from geoparquet_io.core.partition.common import sanitize_filename
from geoparquet_io.core.partition.staging import (
    PartitionWriteOptions,
    check_output_collision,
    create_staging_dir,
    finalize_partition_file,
    iter_staging_partitions,
    make_partition_aliases,
    run_partitioned_copy,
)
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file


def _reprojected_input_geom_sql(input_geom_col, source_crs, alias="a"):
    """Return the input geometry expression reprojected to the admin CRS.

    Admin boundaries are OGC:CRS84, so a non-CRS84 input must be reprojected
    before ``ST_Intersects`` (issue #525) — DuckDB 1.5 otherwise refuses the
    join with a CRS-mismatch error. Returns ``None`` when no transform is needed
    (input already CRS84 / CRS-less). ``alias`` is the SQL table alias prefix;
    pass ``None`` for a bare column reference (e.g. an aggregate extent query).
    """
    base = (
        f"{alias}.{quote_identifier(input_geom_col)}" if alias else quote_identifier(input_geom_col)
    )
    transformed = crs_transform_sql_expr(base, source_crs)
    return transformed if transformed != base else None


def _build_enrichment_query(
    input_url,
    admin_table_ref,
    admin_where_clause,
    admin_select_clause,
    admin_geom_col,
    admin_bbox_col,
    boundary_columns,
    input_geom_col,
    input_bbox_col,
    enriched_table,
    input_is_table_ref=False,
    source_crs=None,
):
    """Build enrichment query for spatial join.

    ``input_is_table_ref`` marks ``input_url`` as a DuckDB temp-table name
    (per-level chaining) rather than a file path, so it is referenced without
    surrounding quotes.

    ``source_crs`` (when a non-default CRS) reprojects the input geometry to the
    admin CRS (OGC:CRS84) before ``ST_Intersects`` (issue #525). The stored input
    bbox is then in the source CRS and cannot be compared against the admin bbox,
    so the bbox pre-filter is skipped.
    """
    input_geom_sql = _reprojected_input_geom_sql(input_geom_col, source_crs)
    input_geom_ref = input_geom_sql or f'a."{input_geom_col}"'
    use_bbox_prefilter = input_bbox_col and admin_bbox_col and input_geom_sql is None
    # Build column list for subquery - handle struct access
    subquery_cols = []
    for i, col in enumerate(boundary_columns):
        if "[" in col or "(" in col:
            subquery_cols.append(f"{col} as _col_{i}")
        else:
            subquery_cols.append(f'"{col}"')
    subquery_cols_str = ", ".join(subquery_cols)

    input_ref = input_url if input_is_table_ref else f"'{input_url}'"

    if use_bbox_prefilter:
        bbox_filter = f"""
            (a.{input_bbox_col}.xmin <= b.{admin_bbox_col}.xmax AND
             a.{input_bbox_col}.xmax >= b.{admin_bbox_col}.xmin AND
             a.{input_bbox_col}.ymin <= b.{admin_bbox_col}.ymax AND
             a.{input_bbox_col}.ymax >= b.{admin_bbox_col}.ymin)
        """

        return f"""
            CREATE TEMP TABLE {enriched_table} AS
            SELECT
                a.*,
                {admin_select_clause}
            FROM {input_ref} a
            LEFT JOIN (
                SELECT {admin_geom_col}, {admin_bbox_col}, {subquery_cols_str}
                FROM {admin_table_ref}
                {admin_where_clause}
            ) b
            ON {bbox_filter}
                AND ST_Intersects(b.{admin_geom_col}, {input_geom_ref})
        """
    else:
        return f"""
            CREATE TEMP TABLE {enriched_table} AS
            SELECT
                a.*,
                {admin_select_clause}
            FROM {input_ref} a
            LEFT JOIN (
                SELECT {admin_geom_col}, {subquery_cols_str}
                FROM {admin_table_ref}
                {admin_where_clause}
            ) b
            ON ST_Intersects(b.{admin_geom_col}, {input_geom_ref})
        """


def _compute_input_extent(con, input_url, input_bbox_col, input_geom_col, source_crs=None):
    """Compute the input's (xmin, xmax, ymin, ymax) extent in one scan.

    The extent does not change across admin levels, so callers compute it once
    and reuse it for every level's WHERE clause (issue #480) rather than
    re-scanning the input per level — important when there is no bbox column and
    the fallback decodes every geometry.

    When the input is reprojected to the admin CRS (``source_crs`` non-default),
    the extent is computed from the *reprojected* geometry so the resulting
    bounds are comparable to the admin (CRS84) bbox; the stored input bbox column
    is in the source CRS and is therefore ignored (issue #525).
    """
    input_geom_sql = _reprojected_input_geom_sql(input_geom_col, source_crs, alias=None)
    if input_bbox_col and input_geom_sql is None:
        extent_query = f"""
            SELECT
                MIN({input_bbox_col}.xmin) as xmin,
                MAX({input_bbox_col}.xmax) as xmax,
                MIN({input_bbox_col}.ymin) as ymin,
                MAX({input_bbox_col}.ymax) as ymax
            FROM '{input_url}'
        """
    else:
        geom_expr = input_geom_sql or quote_identifier(input_geom_col)
        extent_query = f"""
            SELECT
                MIN(ST_XMin({geom_expr})) as xmin,
                MAX(ST_XMax({geom_expr})) as xmax,
                MIN(ST_YMin({geom_expr})) as ymin,
                MAX(ST_YMax({geom_expr})) as ymax
            FROM '{input_url}'
        """
    extent = con.execute(extent_query).fetchone()
    if extent and all(v is not None for v in extent):
        return extent
    return None


def _build_admin_where_clause(dataset, levels, admin_bbox_col, extent, verbose):
    """Build WHERE clause for admin boundaries with filters.

    ``extent`` is the precomputed input extent (from ``_compute_input_extent``)
    or None; it is reused across levels instead of being recomputed each call.
    """
    admin_where_clauses = []

    # Add subtype filter if applicable
    subtype_filter = dataset.get_subtype_filter(levels)
    if subtype_filter:
        admin_where_clauses.append(subtype_filter)
        if verbose:
            debug(f"  → Filtering admin boundaries: {subtype_filter}")

    # Add bbox extent filter
    if admin_bbox_col and extent:
        xmin, xmax, ymin, ymax = extent
        extent_filter = f"""
            ({admin_bbox_col}.xmin <= {xmax} AND
             {admin_bbox_col}.xmax >= {xmin} AND
             {admin_bbox_col}.ymin <= {ymax} AND
             {admin_bbox_col}.ymax >= {ymin})
        """
        admin_where_clauses.append(extent_filter)
        if verbose:
            debug(
                f"  → Filtering admin boundaries to input extent: ({xmin:.2f}, {ymin:.2f}, {xmax:.2f}, {ymax:.2f})"
            )

    return "WHERE " + " AND ".join(admin_where_clauses) if admin_where_clauses else ""


def _setup_admin_dataset(dataset_name, verbose, levels):
    """Setup and validate admin dataset."""
    dataset = AdminDatasetFactory.create(dataset_name, source_path=None, verbose=verbose)

    if verbose:
        debug(f"\nUsing admin dataset: {dataset.get_dataset_name()}")
        debug(f"Remote source: {dataset.get_source()}")
        debug(f"Hierarchical levels: {' → '.join(levels)}")

    dataset.validate_levels(levels)
    boundary_columns = dataset.get_partition_columns(levels)

    if verbose:
        debug(f"Boundary dataset columns: {', '.join(boundary_columns)}")

    return dataset, boundary_columns


def _get_input_file_info(input_parquet, verbose):
    """Get input file info (URL, geometry column, bbox column, source CRS).

    ``source_crs`` is the detected non-default input CRS (or ``None`` for
    CRS84/CRS-less): admin boundaries are OGC:CRS84, so a projected input is
    reprojected before the spatial join (issue #525).
    """
    input_url = safe_file_url(input_parquet, verbose)
    input_geom_col = find_primary_geometry_column(input_parquet, verbose)
    input_bbox_info = check_bbox_structure(input_parquet, verbose)
    input_bbox_col = input_bbox_info["bbox_column_name"]
    source_crs = extract_crs_from_parquet(input_parquet, verbose)
    if source_crs is not None and verbose:
        debug("  → Projected input CRS detected — reprojecting to OGC:CRS84 for admin join")

    return input_url, input_geom_col, input_bbox_col, source_crs


def _setup_admin_join_connection(dataset, get_duckdb_connection):
    """Create the DuckDB connection for the enrichment join.

    Per-level datasets (Overture) can join multi-million-feature inputs and need
    the same memory discipline as ``gpio add admin-divisions`` to spill rather
    than OOM (see add_divisions todo 013): a temp directory for spill,
    single-threaded execution (parallel spatial-join operators each grab memory
    and cannot coordinate spilling, so they OOM even with a temp dir), and no
    insertion-order buffering. Other datasets keep the simple default connection.
    """
    if dataset.supports_per_level_sources():
        from geoparquet_io.core.admin_datasets import get_cache_dir

        temp_dir = get_cache_dir()
        temp_dir.mkdir(parents=True, exist_ok=True)
        con = get_duckdb_connection(
            load_spatial=True, load_httpfs=True, temp_directory=str(temp_dir), threads=1
        )
        con.execute("SET preserve_insertion_order = false")
        return con
    return get_duckdb_connection(load_spatial=True, load_httpfs=True)


def _setup_duckdb_extensions(con):
    """Load required DuckDB extensions."""
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    con.execute("SET geometry_always_xy = true;")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")


def _build_admin_select_for_partitioning(levels, boundary_columns, dataset=None, vecorel=False):
    """Build admin SELECT clause for partitioning.

    In normal mode the admin columns get temporary internal names
    (``_admin_<level>``) used only to drive the partition split. In Vecorel
    mode they are named with Vecorel-compliant column names (e.g.
    ``admin:country_code``) and any dataset column transform (such as stripping
    the country prefix from Overture region codes) is applied, so the columns
    can be written into the output partitions.
    """
    admin_select_parts = []
    output_column_names = []
    for i, (level, col) in enumerate(zip(levels, boundary_columns, strict=True)):
        if vecorel:
            output_col = dataset.get_output_column_name(level, prefix="vecorel")
            col_transform = dataset.get_column_transform(level)
        else:
            output_col = f"_admin_{level}"  # Temporary internal name
            col_transform = None
        output_column_names.append(output_col)

        # Handle struct field access vs simple column names
        if col_transform:
            expr = col_transform
        elif "[" in col or "(" in col:
            expr = f"b._col_{i}"
        else:
            expr = f'b."{col}"'

        admin_select_parts.append(f'{expr} as "{output_col}"')

    return ", ".join(admin_select_parts), output_column_names


def _build_admin_table_reference(dataset, admin_source):
    """Build admin table reference with read options if needed."""
    read_options = dataset.get_read_parquet_options()
    if read_options:
        options_str = ", ".join([f"{k}={v}" for k, v in read_options.items()])
        return f"read_parquet({admin_source}, {options_str})"
    return admin_source


def _perform_enrichment_join(
    con,
    enriched_table,
    input_url,
    admin_table_ref,
    admin_where_clause,
    admin_select_clause,
    admin_geom_col,
    admin_bbox_col,
    boundary_columns,
    input_geom_col,
    input_bbox_col,
    source_crs=None,
):
    """Perform spatial join enrichment."""
    enrichment_query = _build_enrichment_query(
        input_url,
        admin_table_ref,
        admin_where_clause,
        admin_select_clause,
        admin_geom_col,
        admin_bbox_col,
        boundary_columns,
        input_geom_col,
        input_bbox_col,
        enriched_table,
        source_crs=source_crs,
    )
    con.execute(enrichment_query)


def _perform_per_level_enrichment_join(
    con,
    dataset,
    levels,
    boundary_columns,
    enriched_table,
    input_url,
    admin_geom_col,
    admin_bbox_col,
    input_geom_col,
    input_bbox_col,
    vecorel,
    verbose,
    source_crs=None,
):
    """Enrich by chaining one LEFT JOIN per level against its own land cache.

    Datasets that ``supports_per_level_sources()`` (Overture) split each admin
    level into a separate, land-only cache file (see
    ``OvertureAdminDataset._build_level_cache_query``). Joining each level
    against its own non-overlapping cache — rather than one combined join over
    the raw remote dataset, which still contains the maritime (EEZ) polygons
    that double-match every land feature — is what keeps the output row count
    ≈ the input and bounds memory. Each level reads the previous level's temp
    table so a feature carries all admin columns forward; the final join
    produces ``enriched_table``.

    Returns the list of output admin column names.
    """
    current_source = input_url
    current_is_table_ref = False
    output_column_names = []
    intermediate_tables = []

    # The data extent does not change across levels, so compute it once and reuse
    # it for every level's WHERE clause rather than re-scanning per level (#480).
    extent = _compute_input_extent(con, input_url, input_bbox_col, input_geom_col, source_crs)

    for i, (level, col) in enumerate(zip(levels, boundary_columns, strict=True)):
        level_source = dataset.get_source_for_level(level)
        admin_table_ref = _build_admin_table_reference(dataset, f"'{level_source}'")
        select_clause, level_outputs = _build_admin_select_for_partitioning(
            [level], [col], dataset=dataset, vecorel=vecorel
        )
        output_column_names.extend(level_outputs)

        admin_where_clause = _build_admin_where_clause(
            dataset, [level], admin_bbox_col, extent, verbose
        )

        is_last = i == len(levels) - 1
        target = enriched_table if is_last else f"_admin_step_{i}"
        if verbose:
            debug(f"  → Level {i + 1}/{len(levels)}: joining {level} from {level_source}")

        con.execute(
            _build_enrichment_query(
                current_source,
                admin_table_ref,
                admin_where_clause,
                select_clause,
                admin_geom_col,
                admin_bbox_col,
                [col],
                input_geom_col,
                input_bbox_col,
                target,
                input_is_table_ref=current_is_table_ref,
                source_crs=source_crs,
            )
        )
        if not is_last:
            intermediate_tables.append(target)
        current_source = target
        current_is_table_ref = True

    for table in intermediate_tables:
        con.execute(f"DROP TABLE IF EXISTS {table}")

    return output_column_names


def _verify_enrichment_results(con, enriched_table, output_column_names):
    """Verify enrichment results and return stats.

    Also warns when features match a coarser level but are NULL at a finer level:
    they pass the "matched" (any-level) check but are excluded from every
    partition by the all-levels-NOT-NULL filter, so they would otherwise vanish
    silently (#480).
    """
    any_clause = " OR ".join([f'"{col}" IS NOT NULL' for col in output_column_names])
    all_clause = " AND ".join([f'"{col}" IS NOT NULL' for col in output_column_names])
    stats_query = f"""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN {any_clause} THEN 1 END) as with_admin,
            COUNT(CASE WHEN {all_clause} THEN 1 END) as with_all_admin
        FROM {enriched_table}
    """
    total_count, with_admin_count, with_all_count = con.execute(stats_query).fetchone()

    success(f"  ✓ Matched {with_admin_count:,} of {total_count:,} features to admin boundaries")

    if with_admin_count == 0:
        raise PartitionError(
            "No features matched to admin boundaries. Check that input data and boundaries "
            "are in compatible CRS and overlap geographically."
        )

    dropped = with_admin_count - with_all_count
    if dropped > 0:
        warn(
            f"  ⚠️  {dropped:,} feature(s) matched a coarser admin level but are missing a finer "
            "level; they will not appear in any partition (incomplete admin hierarchy)."
        )

    return total_count, with_admin_count


def _get_original_columns(con, input_url):
    """Get original column names from input file."""
    original_columns_query = f"SELECT * FROM '{input_url}' LIMIT 0"
    original_schema = con.execute(original_columns_query)
    return [desc[0] for desc in original_schema.description]


def _build_admin_staging_select(
    enriched_table, output_column_names, original_cols, vecorel, aliases
):
    """Build the SELECT for the single admin partitioned COPY.

    Keeps the original columns (plus the admin columns in Vecorel mode, so each
    partition is Vecorel-compliant) and adds one aliased partition key per level
    (``aliases``, guaranteed not to collide with a real column). PARTITION_BY
    drops those aliases from the written files, so the admin columns are dropped
    from non-Vecorel output exactly as before.
    """
    keep_cols = list(original_cols)
    if vecorel:
        keep_cols += list(output_column_names)
    keep_sql = ", ".join(quote_identifier(col) for col in keep_cols)

    alias_sql = ", ".join(
        f"{quote_identifier(col)} AS {alias}"
        for col, alias in zip(output_column_names, aliases, strict=True)
    )
    where_sql = " AND ".join(f"{quote_identifier(col)} IS NOT NULL" for col in output_column_names)
    return f"SELECT {keep_sql}, {alias_sql} FROM {enriched_table} WHERE {where_sql}"


def _finalize_admin_partition(
    con,
    values,
    partition_dir,
    levels,
    output_folder,
    hive,
    filename_prefix,
    overwrite,
    metadata,
    verbose,
    vecorel,
    write_options,
    seen_outputs,
):
    """Rewrite one staging partition into its final nested location."""
    folder_parts = [
        f"{level}={sanitize_filename(str(value))}" if hive else sanitize_filename(str(value))
        for level, value in zip(levels, values, strict=True)
    ]
    partition_folder = os.path.join(output_folder, *folder_parts)

    safe_last = sanitize_filename(str(values[-1]))
    filename = (
        f"{filename_prefix}_{safe_last}.parquet" if filename_prefix else f"{safe_last}.parquet"
    )
    output_file = os.path.join(partition_folder, filename)
    check_output_collision(seen_outputs, output_file, tuple(values))

    os.makedirs(partition_folder, exist_ok=True)
    if verbose and not (os.path.exists(output_file) and not overwrite):
        debug(f"  → Creating: {'/'.join(folder_parts)}")

    created = finalize_partition_file(
        con, partition_dir, output_file, metadata, overwrite, verbose, write_options
    )

    # Ensure Vecorel schema compliance (id column + non-nullable columns) — DuckDB
    # cannot write non-nullable Parquet columns, so this rewrites in place.
    if created and vecorel:
        from geoparquet_io.core.constants import ensure_vecorel_columns

        ensure_vecorel_columns(output_file, verbose)

    return created


def _create_all_partitions(
    con,
    enriched_table,
    output_column_names,
    levels,
    output_folder,
    hive,
    filename_prefix,
    overwrite,
    metadata,
    verbose,
    profile,
    original_cols,
    geoparquet_version=None,
    compression="ZSTD",
    compression_level=15,
    row_group_size_mb=None,
    row_group_rows=None,
    memory_limit=None,
    vecorel=False,
    extra_kv=None,
):
    """Create all partition files in a single pass.

    Routes the enriched rows into a staging dir with one DuckDB
    ``COPY ... PARTITION_BY`` (no per-combination re-scan, issue #478), then
    rewrites each (small) staging partition into its final nested file with the
    correct per-partition metadata.
    """
    # Aliases must not collide with kept columns (originals + admin columns).
    aliases = make_partition_aliases(
        len(output_column_names), list(original_cols) + list(output_column_names)
    )
    write_options = PartitionWriteOptions(
        geoparquet_version=geoparquet_version,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        memory_limit=memory_limit,
        profile=profile,
        extra_kv_metadata=extra_kv,
    )

    staging_dir = create_staging_dir(output_folder)
    partition_count = 0
    seen_outputs: dict[str, tuple] = {}
    try:
        select_sql = _build_admin_staging_select(
            enriched_table, output_column_names, original_cols, vecorel, aliases
        )
        run_partitioned_copy(con, select_sql, aliases, staging_dir, verbose, memory_limit)

        for values, partition_dir in iter_staging_partitions(staging_dir):
            if _finalize_admin_partition(
                con,
                values,
                partition_dir,
                levels,
                output_folder,
                hive,
                filename_prefix,
                overwrite,
                metadata,
                verbose,
                vecorel,
                write_options,
                seen_outputs,
            ):
                partition_count += 1
            # Incremental cleanup caps peak staging disk at ~one partition.
            shutil.rmtree(partition_dir, ignore_errors=True)
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)

    return partition_count


def partition_by_admin_hierarchical(
    input_parquet: str,
    output_folder: str | None,
    dataset_name: str,
    levels: list[str],
    hive: bool = False,
    overwrite: bool = False,
    preview: bool = False,
    preview_limit: int = 15,
    verbose: bool = False,
    force: bool = False,
    skip_analysis: bool = False,
    filename_prefix: str | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    compression: str = "ZSTD",
    compression_level: int = 15,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    memory_limit: str | None = None,
    vecorel: bool = False,
) -> int:
    """
    Partition a GeoParquet file by administrative boundaries.

    Supports Arrow IPC streaming for input:
    - Input "-" reads from stdin (output is always a directory)

    This performs a two-step operation:
    1. Spatial join with remote admin boundaries to add admin columns
    2. Partition the enriched data by those admin columns

    Args:
        input_parquet: Input GeoParquet file (local, remote URL, or "-" for stdin)
        output_folder: Output directory for partitioned files
        dataset_name: Name of admin dataset ("gaul", "overture")
        levels: List of hierarchical levels to partition by
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing partition files
        preview: Preview partitions without creating files
        preview_limit: Number of partitions to show in preview
        verbose: Enable verbose output
        force: Force partitioning even if analysis detects issues
        skip_analysis: Skip partition strategy analysis
        filename_prefix: Prefix for output filenames
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write
        compression: Compression codec (default: ZSTD)
        compression_level: Compression level (default: 15)
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
        memory_limit: DuckDB memory limit for write operations (e.g., "2GB")
        vecorel: Output Vecorel-compliant admin columns (admin:country_code,
            admin:subdivision_code) in each partition along with Vecorel
            collection metadata. Expects the Overture dataset with
            country,region levels.

    Returns:
        Number of partitions created
    """
    # Handle stdin input
    stdin_temp_file = None
    actual_input = input_parquet

    if is_stdin(input_parquet):
        stdin_temp_file = read_stdin_to_temp_file(verbose)
        actual_input = stdin_temp_file

    try:
        # Setup dataset and get input file info
        dataset, boundary_columns = _setup_admin_dataset(dataset_name, verbose, levels)
        input_url, input_geom_col, input_bbox_col, source_crs = _get_input_file_info(
            actual_input, verbose
        )

        # Get admin dataset info
        admin_geom_col = dataset.get_geometry_column()
        admin_bbox_col = dataset.get_bbox_column()

        # Use ambient S3 config from dataset, applied via get_duckdb_connection
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection, s3_config_scope

        with s3_config_scope(dataset.get_s3_config()):
            con = _setup_admin_join_connection(dataset, get_duckdb_connection)

            # STEP 1: Spatial join to create enriched data with admin columns
            progress("\n📍 Step 1/2: Performing spatial join with admin boundaries...")

            enriched_table = "_enriched_with_admin"

            if input_bbox_col and admin_bbox_col and verbose:
                debug("  → Using bbox columns for optimized spatial join")
            elif not (input_bbox_col and admin_bbox_col) and verbose:
                debug("  → Using full geometry intersection (no bbox optimization)")

            if dataset.supports_per_level_sources():
                # Per-level land caches: chain one join per level so maritime
                # (EEZ) polygons don't double-match and memory stays bounded.
                output_column_names = _perform_per_level_enrichment_join(
                    con,
                    dataset,
                    levels,
                    boundary_columns,
                    enriched_table,
                    input_url,
                    admin_geom_col,
                    admin_bbox_col,
                    input_geom_col,
                    input_bbox_col,
                    vecorel,
                    verbose,
                    source_crs=source_crs,
                )
            else:
                admin_source = dataset.prepare_data_source(con)
                admin_select_clause, output_column_names = _build_admin_select_for_partitioning(
                    levels, boundary_columns, dataset=dataset, vecorel=vecorel
                )
                admin_table_ref = _build_admin_table_reference(dataset, admin_source)
                extent = _compute_input_extent(
                    con, input_url, input_bbox_col, input_geom_col, source_crs
                )
                admin_where_clause = _build_admin_where_clause(
                    dataset, levels, admin_bbox_col, extent, verbose
                )
                _perform_enrichment_join(
                    con,
                    enriched_table,
                    input_url,
                    admin_table_ref,
                    admin_where_clause,
                    admin_select_clause,
                    admin_geom_col,
                    admin_bbox_col,
                    boundary_columns,
                    input_geom_col,
                    input_bbox_col,
                    source_crs=source_crs,
                )

            # Verify enrichment results
            _verify_enrichment_results(con, enriched_table, output_column_names)

            # STEP 2: Partition the enriched data
            progress(f"\n📁 Step 2/2: Partitioning by {' → '.join(levels)}...")

            # Preview mode
            if preview:
                _preview_hierarchical_partitions(
                    con,
                    enriched_table,
                    output_column_names,
                    levels,
                    preview_limit,
                    verbose,
                )
                return 0

            # Get metadata from input for preservation
            metadata, _ = get_parquet_metadata(actual_input, verbose)

            # Build Vecorel collection metadata if requested
            extra_kv = None
            if vecorel:
                from geoparquet_io.core.constants import (
                    VECOREL_ADMIN_SCHEMA,
                    build_collection_metadata,
                )

                extra_kv = build_collection_metadata([VECOREL_ADMIN_SCHEMA], metadata)

            # Create output directory
            os.makedirs(output_folder, exist_ok=True)

            # Get original columns (exclude temporary admin columns)
            original_cols = _get_original_columns(con, input_url)

            # Create each partition
            partition_count = _create_all_partitions(
                con,
                enriched_table,
                output_column_names,
                levels,
                output_folder,
                hive,
                filename_prefix,
                overwrite,
                metadata,
                verbose,
                profile,
                original_cols,
                geoparquet_version,
                compression,
                compression_level,
                row_group_size_mb,
                row_group_rows,
                memory_limit,
                vecorel,
                extra_kv,
            )

        success(f"\n✓ Created {partition_count} partition(s) in {output_folder}")

        return partition_count
    finally:
        # Clean up stdin temp file
        if stdin_temp_file and os.path.exists(stdin_temp_file):
            os.remove(stdin_temp_file)


def _get_preview_partitions(con, table_name, partition_columns, level_names):
    """Query partition statistics for preview."""
    group_by_cols = ", ".join([f'"{col}"' for col in partition_columns])
    select_cols = ", ".join(
        [f'"{col}" as {name}' for col, name in zip(partition_columns, level_names, strict=True)]
    )

    query = f"""
        SELECT
            {select_cols},
            COUNT(*) as record_count
        FROM {table_name}
        WHERE {" AND ".join([f'"{col}" IS NOT NULL' for col in partition_columns])}
        GROUP BY {group_by_cols}
        ORDER BY record_count DESC
    """

    result = con.execute(query)
    return result.fetchall()


def _display_preview_header(level_names, all_partitions, total_records, limit):
    """Display preview header and stats."""
    progress(f"\n📊 Partition Preview ({' → '.join(level_names)}):")
    progress(f"  Total partitions: {len(all_partitions)}")
    progress(f"  Total records: {total_records:,}")
    progress(f"\n  Top {min(limit, len(all_partitions))} partitions by size:")

    header_parts = [f"{name:<25}" for name in level_names]
    header_parts.append(f"{'Records':>15}")
    header_parts.append(f"{'%':>8}")
    header = "  ".join(header_parts)
    progress(f"\n  {header}")
    progress(f"  {'-' * len(header)}")
    return header


def _display_preview_rows(all_partitions, limit, total_records):
    """Display preview rows."""
    for i, row in enumerate(all_partitions):
        if i >= limit:
            break

        values = row[:-1]
        count = row[-1]
        percentage = (count / total_records) * 100

        row_parts = [f"{str(val):<25}" for val in values]
        row_parts.append(f"{count:>15,}")
        row_parts.append(f"{percentage:>7.1f}%")
        progress(f"  {'  '.join(row_parts)}")


def _display_preview_summary(all_partitions, limit, total_records, header):
    """Display preview summary if more exist."""
    if len(all_partitions) > limit:
        remaining = len(all_partitions) - limit
        remaining_records = sum(row[-1] for row in all_partitions[limit:])
        remaining_pct = (remaining_records / total_records) * 100
        progress(f"  {'-' * len(header)}")
        progress(
            f"  ... and {remaining} more partition(s) with {remaining_records:,} records ({remaining_pct:.1f}%)"
        )
        progress("\n  Use --preview-limit to show more partitions")


def _preview_hierarchical_partitions(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    partition_columns: list[str],
    level_names: list[str],
    limit: int,
    verbose: bool,
) -> None:
    """Preview hierarchical partitions without creating files."""
    all_partitions = _get_preview_partitions(con, table_name, partition_columns, level_names)

    if len(all_partitions) == 0:
        warn("\n⚠️  No partitions would be created (no features with admin boundaries)")
        return

    total_records = sum(row[-1] for row in all_partitions)
    header = _display_preview_header(level_names, all_partitions, total_records, limit)
    _display_preview_rows(all_partitions, limit, total_records)
    _display_preview_summary(all_partitions, limit, total_records, header)
