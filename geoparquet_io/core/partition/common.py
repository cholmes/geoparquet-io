#!/usr/bin/env python3

import os
import re
import shutil
from urllib.parse import unquote

from geoparquet_io.core.common import (
    get_parquet_metadata,
    write_parquet_with_metadata,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.exceptions import PartitionError
from geoparquet_io.core.file_utils import safe_file_url
from geoparquet_io.core.logging_config import debug, error, info, progress, warn
from geoparquet_io.core.remote import (
    needs_httpfs,
    remote_write_context,
    setup_aws_profile_if_needed,
    show_remote_read_message,
    upload_if_remote,
)


class PartitionAnalysisError(PartitionError):
    """Raised when partition analysis detects a problematic strategy.

    Inherits from PartitionError to participate in the unified exception hierarchy,
    enabling consistent CLI error handling via handle_core_exception().
    """

    pass


def sanitize_filename(value: str) -> str:
    """
    Sanitize a string value for use in a filename.

    Replaces special characters with underscores while preserving alphanumeric and common safe chars.

    Args:
        value: String value to sanitize

    Returns:
        Sanitized string safe for filenames
    """
    # Replace problematic characters with underscores
    # Keep alphanumeric, dash, underscore, and period
    sanitized = re.sub(r"[^a-zA-Z0-9._-]", "_", value)
    # Remove leading/trailing underscores and dots
    sanitized = sanitized.strip("_.")
    # Collapse multiple underscores
    sanitized = re.sub(r"_+", "_", sanitized)
    return sanitized


def calculate_partition_stats(output_folder: str, num_partitions: int) -> tuple[float, float]:
    """Calculate total and average size of created partitions.

    Args:
        output_folder: Path to the folder containing partition files.
        num_partitions: Number of partitions created.

    Returns:
        Tuple of (total_size_mb, avg_size_mb).
    """
    total_size_bytes = sum(
        os.path.getsize(os.path.join(root, f))
        for root, _, files in os.walk(output_folder)
        for f in files
        if f.endswith(".parquet")
    )
    total_size_mb = total_size_bytes / (1024 * 1024)
    avg_size_mb = total_size_mb / num_partitions if num_partitions > 0 else 0
    return total_size_mb, avg_size_mb


def _calculate_size_estimates(file_size_bytes, total_rows, min_rows, max_rows, avg_rows):
    """Calculate partition size estimates based on file size and row distribution."""
    if file_size_bytes > 0 and total_rows > 0:
        bytes_per_row = file_size_bytes / total_rows
        return (
            int(min_rows * bytes_per_row),
            int(max_rows * bytes_per_row),
            int(avg_rows * bytes_per_row),
        )
    else:
        # Fallback if we can't get file size - assume 1KB per row
        return min_rows * 1000, max_rows * 1000, avg_rows * 1000


def _check_partition_errors(
    partition_count,
    avg_rows,
    avg_size_mb,
    imbalance_ratio,
    median_rows,
    max_rows,
    max_partitions,
    min_rows_per_partition,
    min_partition_size_mb,
    max_imbalance_ratio,
):
    """Check for blocking partition strategy errors."""
    errors = []

    if partition_count > max_partitions:
        errors.append(
            f"Pathological partitioning: {partition_count:,} partitions exceeds maximum of {max_partitions:,}. "
            f"This will create too many small files with poor performance."
        )

    if avg_rows < min_rows_per_partition:
        errors.append(
            f"Tiny partitions: Average of {avg_rows:,} rows per partition is below minimum of {min_rows_per_partition:,}. "
            f"This will result in poor I/O performance and excessive file overhead."
        )

    if avg_size_mb < min_partition_size_mb:
        errors.append(
            f"Tiny partition files: Average size of {avg_size_mb:.3f} MB is below minimum of {min_partition_size_mb:.3f} MB. "
            f"These tiny files will have poor I/O performance."
        )

    if imbalance_ratio > max_imbalance_ratio:
        errors.append(
            f"Extreme imbalance: Largest partition ({max_rows:,} rows) is {imbalance_ratio:.1f}x the median ({median_rows:,} rows). "
            f"This indicates a data quality issue or wrong partition key choice."
        )

    return errors


def _check_partition_warnings(
    partition_count,
    avg_rows,
    imbalance_ratio,
    largest_partition_pct,
    warn_partition_count,
    warn_min_rows,
    warn_imbalance_ratio,
    max_imbalance_ratio,
    min_rows_per_partition,
):
    """Check for partition strategy warnings."""
    warnings = []

    if warn_imbalance_ratio <= imbalance_ratio < max_imbalance_ratio:
        warnings.append(
            f"Moderate imbalance: Largest partition is {imbalance_ratio:.1f}x the median. "
            f"This might be expected (e.g., some regions are larger) but worth noting."
        )

    if warn_partition_count <= partition_count:
        warnings.append(
            f"Many partitions: Creating {partition_count:,} partitions. Not broken but getting unwieldy."
        )

    if min_rows_per_partition <= avg_rows < warn_min_rows:
        warnings.append(
            f"Small partitions: Average of {avg_rows:,} rows per partition is below recommended {warn_min_rows:,}. "
            f"Suboptimal but might be intentional for your use case."
        )

    if largest_partition_pct > 50:
        warnings.append(
            f"Single partition dominates: One partition contains {largest_partition_pct:.1f}% of all data. "
            f"This might defeat the purpose of partitioning."
        )

    return warnings


def analyze_partition_strategy(
    input_parquet: str,
    column_name: str,
    column_prefix_length: int | None = None,
    max_partitions: int = 10000,
    min_rows_per_partition: int = 100,
    min_partition_size_mb: float = 0.001,  # 1KB
    max_imbalance_ratio: float = 1000.0,
    warn_imbalance_ratio: float = 100.0,
    warn_partition_count: int = 1000,
    warn_min_rows: int = 10000,
    verbose: bool = False,
) -> dict:
    """
    Analyze a partition strategy before execution to detect potential issues.

    Calculates comprehensive statistics about how data would be partitioned and
    checks for pathological cases like too many partitions, tiny partitions,
    extreme imbalance, etc.

    Args:
        input_parquet: Input file path
        column_name: Column to partition by
        column_prefix_length: If set, use first N characters of column value
        max_partitions: Maximum allowed partitions (raises error if exceeded)
        min_rows_per_partition: Minimum rows per partition (raises error if avg below this)
        min_partition_size_mb: Minimum partition size in MB (raises error if avg below this)
        max_imbalance_ratio: Maximum ratio of largest to median partition (raises error if exceeded)
        warn_imbalance_ratio: Warn if imbalance ratio exceeds this
        warn_partition_count: Warn if partition count exceeds this
        warn_min_rows: Warn if average rows per partition below this
        verbose: Print detailed analysis

    Returns:
        Dictionary with analysis results including:
        - partition_count: Number of partitions
        - total_rows: Total number of rows
        - row_stats: Dict with min, max, avg, median, stddev
        - size_stats: Dict with estimated sizes
        - imbalance_ratio: Ratio of max to median partition size
        - warnings: List of warning messages
        - errors: List of error messages (blocking)
        - largest_partition_pct: Percentage of data in largest partition

    Raises:
        PartitionAnalysisError: If blocking issues are detected
    """
    # Show remote read message
    show_remote_read_message(input_parquet, verbose)

    input_url = safe_file_url(input_parquet, verbose)

    # Create DuckDB connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))

    # Build the column expression for partitioning
    if column_prefix_length is not None:
        column_expr = f'LEFT("{column_name}", {column_prefix_length})'
    else:
        column_expr = f'"{column_name}"'

    if verbose:
        debug("\n📊 Analyzing partition strategy...")

    # Calculate comprehensive statistics in one query
    stats_query = f"""
        WITH partition_stats AS (
            SELECT
                {column_expr} as partition_value,
                COUNT(*) as row_count
            FROM '{input_url}'
            WHERE "{column_name}" IS NOT NULL
            GROUP BY partition_value
        )
        SELECT
            COUNT(*) as partition_count,
            SUM(row_count) as total_rows,
            MIN(row_count) as min_rows,
            MAX(row_count) as max_rows,
            AVG(row_count)::BIGINT as avg_rows,
            MEDIAN(row_count)::BIGINT as median_rows
        FROM partition_stats
    """

    result = con.execute(stats_query).fetchone()

    # Get approximate file size for estimation
    try:
        import os

        file_size_bytes = os.path.getsize(input_parquet)
    except Exception:
        file_size_bytes = 0

    con.close()

    # Unpack results
    partition_count, total_rows, min_rows, max_rows, avg_rows, median_rows = result

    # Estimate size based on file size and row distribution
    min_size_bytes, max_size_bytes, avg_size_bytes = _calculate_size_estimates(
        file_size_bytes, total_rows, min_rows, max_rows, avg_rows
    )

    # Calculate derived metrics
    imbalance_ratio = max_rows / median_rows if median_rows > 0 else 0
    largest_partition_pct = (max_rows / total_rows * 100) if total_rows > 0 else 0
    avg_size_mb = avg_size_bytes / (1024 * 1024)
    max_size_mb = max_size_bytes / (1024 * 1024)

    # Check for errors and warnings
    errors = _check_partition_errors(
        partition_count,
        avg_rows,
        avg_size_mb,
        imbalance_ratio,
        median_rows,
        max_rows,
        max_partitions,
        min_rows_per_partition,
        min_partition_size_mb,
        max_imbalance_ratio,
    )

    warnings = _check_partition_warnings(
        partition_count,
        avg_rows,
        imbalance_ratio,
        largest_partition_pct,
        warn_partition_count,
        warn_min_rows,
        warn_imbalance_ratio,
        max_imbalance_ratio,
        min_rows_per_partition,
    )

    # Generate recommendations
    recommendations = _generate_recommendations(
        partition_count=partition_count,
        total_rows=total_rows,
        avg_rows=avg_rows,
        max_rows=max_rows,
        avg_size_mb=avg_size_mb,
        imbalance_ratio=imbalance_ratio,
        column_prefix_length=column_prefix_length,
    )

    # Build result dictionary
    analysis = {
        "partition_count": partition_count,
        "total_rows": total_rows,
        "row_stats": {
            "min": min_rows,
            "max": max_rows,
            "avg": avg_rows,
        },
        "size_stats": {
            "min_mb": min_size_bytes / (1024 * 1024),
            "max_mb": max_size_mb,
            "avg_mb": avg_size_mb,
        },
        "imbalance_ratio": imbalance_ratio,
        "largest_partition_pct": largest_partition_pct,
        "warnings": warnings,
        "errors": errors,
        "recommendations": recommendations,
    }

    # Display analysis if verbose
    if verbose or warnings or errors or recommendations:
        _display_partition_analysis(analysis, column_name, column_prefix_length)

    # Raise error if blocking conditions found
    if errors:
        error_msg = "\n\n".join([f"❌ {err}" for err in errors])
        error_msg += "\n\nUse --force to override this check if you're certain about this partitioning strategy."
        raise PartitionAnalysisError(error_msg)

    return analysis


def _generate_recommendations(
    partition_count: int,
    total_rows: int,
    avg_rows: int,
    max_rows: int,
    avg_size_mb: float,
    imbalance_ratio: float,
    column_prefix_length: int | None = None,
) -> list:
    """
    Generate actionable recommendations based on partition analysis.

    Args:
        partition_count: Number of partitions
        total_rows: Total number of rows
        avg_rows: Average rows per partition
        max_rows: Maximum rows in a partition
        avg_size_mb: Average partition size in MB
        imbalance_ratio: Ratio of max to median partition
        column_prefix_length: If partitioning by prefix (e.g., H3 resolution)

    Returns:
        List of recommendation strings
    """
    recommendations = []

    # Very small dataset - not worth partitioning
    if total_rows < 1000:
        recommendations.append(
            f"Dataset is very small ({total_rows:,} rows). "
            "Partitioning is not recommended - use as a single file for better performance."
        )
        return recommendations

    # Too many tiny files - suggest not partitioning or using fewer partitions
    if partition_count > 1000 and avg_rows < 10000:
        recommendations.append(
            "Consider NOT partitioning this dataset - you'll create many small files with poor performance. "
            "Alternative: Use the data as a single file with spatial indexing (bbox column)."
        )

    # Large partitions - suggest more granular partitioning
    if column_prefix_length is not None and avg_rows > 1000000:
        recommendations.append(
            f"Partitions are very large ({avg_rows:,} avg rows). "
            f"Consider using more granular partitioning (e.g., higher resolution/iterations) "
            f"for smaller, more manageable partitions."
        )

    # Very large max partition - suggest hierarchical partitioning
    if max_rows > 10000000 and imbalance_ratio > 5:
        recommendations.append(
            "Largest partition is very large and imbalanced. "
            "Consider hierarchical partitioning: partition by a coarser key first (e.g., country/region), "
            "then by spatial partitioning within each partition."
        )

    # Moderate number of reasonable-sized partitions - good strategy
    if 10 <= partition_count <= 500 and 10000 <= avg_rows <= 5000000:
        recommendations.append(
            "This looks like a reasonable partitioning strategy. "
            f"You'll create {partition_count} partitions with ~{avg_rows:,} rows each."
        )

    # Too few partitions - might not be worth it
    if partition_count < 5 and total_rows > 1000000:
        recommendations.append(
            f"Only {partition_count} partitions for {total_rows:,} rows. "
            "Consider using more granular partitioning (e.g., higher resolution/iterations or prefix partitioning)."
        )

    # Suggest hierarchical for specific cases
    if partition_count > 5000:
        recommendations.append(
            f"Too many partitions ({partition_count:,}). Consider hierarchical approach: "
            f"first partition by a coarser key (e.g., region/country), then by finer spatial partitioning within each."
        )

    return recommendations


def _display_partition_analysis(
    analysis: dict, column_name: str, column_prefix_length: int | None
) -> None:
    """Display partition analysis results in a formatted way."""
    partition_desc = f"'{column_name}'"
    if column_prefix_length is not None:
        partition_desc = f"first {column_prefix_length} character(s) of {partition_desc}"

    progress(f"\nAnalyzing partition strategy for {partition_desc}...")
    progress(f"  Partitions: {analysis['partition_count']:,}")
    progress(f"  Total rows: {analysis['total_rows']:,}")

    # Row distribution
    stats = analysis["row_stats"]
    progress(
        f"  Rows per partition: min={stats['min']:,}, max={stats['max']:,}, avg={stats['avg']:,}"
    )

    # Size estimates
    size = analysis["size_stats"]
    if size["avg_mb"] >= 0.01:
        progress(
            f"  Estimated size per partition: min={size['min_mb']:.2f}MB, max={size['max_mb']:.2f}MB, avg={size['avg_mb']:.2f}MB"
        )

    # Balance metrics
    progress(
        f"  Imbalance: {analysis['imbalance_ratio']:.1f}x (largest/median), {analysis['largest_partition_pct']:.1f}% in largest"
    )

    # Recommendations
    if analysis.get("recommendations"):
        progress("\nRecommendations:")
        for rec in analysis["recommendations"]:
            info(f"  💡 {rec}")

    # Warnings
    if analysis["warnings"]:
        progress("\nWarnings:")
        for warning in analysis["warnings"]:
            warn(f"  {warning}")

    # Errors
    if analysis["errors"]:
        progress("\nErrors:")
        for error_msg in analysis["errors"]:
            error(f"  {error_msg}")


def preview_partition(
    input_parquet: str,
    column_name: str,
    column_prefix_length: int | None = None,
    limit: int = 15,
    verbose: bool = False,
) -> dict:
    """
    Preview the partitions that would be created without actually creating them.

    Args:
        input_parquet: Input file path
        column_name: Column to partition by
        column_prefix_length: If set, use first N characters of column value
        limit: Maximum number of partitions to display (default: 15)
        verbose: Print detailed output

    Returns:
        Dictionary with partition statistics
    """
    # Show remote read message
    show_remote_read_message(input_parquet, verbose)

    input_url = safe_file_url(input_parquet, verbose)

    # Create DuckDB connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))

    # Build the column expression for partitioning
    if column_prefix_length is not None:
        column_expr = f'LEFT("{column_name}", {column_prefix_length})'
        partition_description = f"first {column_prefix_length} character(s) of '{column_name}'"
    else:
        column_expr = f'"{column_name}"'
        partition_description = f"'{column_name}'"

    # Get partition counts
    query = f"""
        SELECT
            {column_expr} as partition_value,
            COUNT(*) as record_count
        FROM '{input_url}'
        WHERE "{column_name}" IS NOT NULL
        GROUP BY partition_value
        ORDER BY record_count DESC
    """

    result = con.execute(query)
    all_partitions = result.fetchall()

    con.close()

    if len(all_partitions) == 0:
        raise PartitionError(f"No non-NULL values found in column '{column_name}'")

    # Calculate total records
    total_records = sum(row[1] for row in all_partitions)

    # Display preview
    progress(f"\nPartition Preview for {partition_description}:")
    progress(f"Total partitions: {len(all_partitions)}")
    progress(f"Total records: {total_records:,}")
    progress("\nPartitions (sorted by record count):")
    progress(f"{'Partition Value':<30} {'Records':>15} {'Percentage':>12}")
    progress("-" * 60)

    # Show up to 'limit' partitions
    for i, (partition_value, count) in enumerate(all_partitions):
        if i >= limit:
            break
        percentage = (count / total_records) * 100
        progress(f"{str(partition_value):<30} {count:>15,} {percentage:>11.2f}%")

    # Show summary if there are more partitions
    if len(all_partitions) > limit:
        remaining_count = len(all_partitions) - limit
        remaining_records = sum(row[1] for row in all_partitions[limit:])
        remaining_pct = (remaining_records / total_records) * 100
        progress("-" * 60)
        progress(
            f"... and {remaining_count} more partition(s) with {remaining_records:,} records ({remaining_pct:.2f}%)"
        )
        progress("\nUse --preview-limit to show more partitions")

    return {
        "total_partitions": len(all_partitions),
        "total_records": total_records,
        "partitions": all_partitions,
    }


def _run_partition_analysis(
    input_parquet, column_name, column_prefix_length, skip_analysis, force, verbose
):
    """Run partition analysis if enabled."""
    if skip_analysis:
        return

    try:
        analyze_partition_strategy(
            input_parquet=input_parquet,
            column_name=column_name,
            column_prefix_length=column_prefix_length,
            verbose=verbose,
        )
    except PartitionAnalysisError as e:
        if not force:
            raise PartitionError(str(e)) from e
        if verbose:
            warn("\n⚠️  Forcing partition creation despite analysis warnings/errors...")


def _build_column_expression(column_name, column_prefix_length):
    """Build column expression for partitioning."""
    if column_prefix_length is not None:
        column_expr = f'LEFT("{column_name}", {column_prefix_length})'
        partition_description = f"first {column_prefix_length} characters of {column_name}"
    else:
        column_expr = f'"{column_name}"'
        partition_description = column_name
    return column_expr, partition_description


# Internal alias used to drive the single-pass PARTITION_BY split. DuckDB drops
# the PARTITION_BY column from the written files, so using a dedicated alias lets
# us keep or drop the *original* column independently (see _build_staging_select).
_PARTITION_ALIAS = "__gpio_part"


def _build_staging_select(input_url, column_name, column_prefix_length, keep_partition_column):
    """Build the SELECT for the single partitioned COPY.

    Adds an aliased partition key (``__gpio_part``) that PARTITION_BY drops from
    the output files, so the original column is kept or excluded independently
    via ``keep_partition_column``. Only non-NULL partition values are written,
    matching the previous per-value behaviour.
    """
    if column_prefix_length is not None:
        pexpr = f'LEFT("{column_name}", {column_prefix_length})'
    else:
        pexpr = f'"{column_name}"'

    projection = "*" if keep_partition_column else f'* EXCLUDE ("{column_name}")'

    return (
        f"SELECT {projection}, {pexpr} AS {_PARTITION_ALIAS} "
        f"FROM '{input_url}' "
        f'WHERE "{column_name}" IS NOT NULL'
    )


def _run_partitioned_copy(con, select_sql, partition_cols, staging_dir, verbose, memory_limit=None):
    """Run a single DuckDB ``COPY ... PARTITION_BY`` into ``staging_dir``.

    This reads the input exactly once and routes every row to its partition
    writer (issue #478). Staging files use native geometry + SNAPPY (fast,
    transient); the final files are rewritten with the requested settings and
    correct per-partition metadata by the caller.
    """
    from geoparquet_io.core.write_strategies.duckdb_kv import get_default_memory_limit

    con.execute("SET threads = 1")  # one file per partition + bounded memory
    con.execute("SET preserve_insertion_order = false")
    effective_limit = memory_limit or get_default_memory_limit()
    con.execute(f"SET memory_limit = '{effective_limit}'")

    part_cols = ", ".join(partition_cols)
    escaped_dir = staging_dir.replace("'", "''")
    copy_sql = (
        f"COPY ({select_sql}) TO '{escaped_dir}' "
        f"(FORMAT PARQUET, PARTITION_BY ({part_cols}), "
        f"COMPRESSION SNAPPY, GEOPARQUET_VERSION 'NONE', OVERWRITE_OR_IGNORE)"
    )
    if verbose:
        debug("Single-pass partition COPY (one scan of input):")
        debug(copy_sql)
    con.execute(copy_sql)


def _iter_staging_partitions(staging_dir):
    """Yield ``(values, partition_dir)`` for each leaf partition in staging.

    ``values`` is the list of decoded partition values (one per PARTITION_BY
    level), recovered from DuckDB's Hive-encoded ``alias=value`` directory
    names. Supports nested (multi-level) partitioning.
    """

    def _walk(current_dir, prefix):
        entries = sorted(
            d
            for d in os.listdir(current_dir)
            if "=" in d and os.path.isdir(os.path.join(current_dir, d))
        )
        for entry in entries:
            _, _, enc_value = entry.partition("=")
            value = unquote(enc_value)
            child = os.path.join(current_dir, entry)
            sub = [
                d for d in os.listdir(child) if "=" in d and os.path.isdir(os.path.join(child, d))
            ]
            if sub:
                yield from _walk(child, [*prefix, value])
            else:
                yield [*prefix, value], child

    yield from _walk(staging_dir, [])


def _finalize_partition_file(
    con, partition_dir, output_filename, metadata, overwrite, verbose, **write_kwargs
):
    """Rewrite one staging partition into its final file with correct metadata.

    Reads the (small) staging partition via a glob so a partition that DuckDB
    split across files still collapses into one output file. Recomputes the
    tight per-partition bbox by stripping the inherited bbox.
    """
    if os.path.exists(output_filename) and not overwrite:
        if verbose:
            debug(f"Output file {output_filename} already exists, skipping...")
        return False

    staging_glob = os.path.join(partition_dir, "*.parquet").replace("'", "''")
    query = f"SELECT * FROM read_parquet('{staging_glob}')"
    write_parquet_with_metadata(
        con,
        query,
        output_filename,
        original_metadata=_strip_bbox_from_metadata(metadata),
        verbose=False,
        **write_kwargs,
    )
    if verbose:
        debug(f"Wrote {output_filename}")
    return True


def _determine_output_path(
    actual_output, partition_value, column_name, column_prefix_length, hive, filename_prefix
):
    """Determine output path for partition."""
    safe_value = sanitize_filename(str(partition_value))
    filename = (
        f"{filename_prefix}_{safe_value}.parquet" if filename_prefix else f"{safe_value}.parquet"
    )

    if hive:
        if column_prefix_length is not None:
            folder_name = f"{column_name}_prefix={safe_value}"
        else:
            folder_name = f"{column_name}={safe_value}"
        write_folder = os.path.join(actual_output, folder_name)
        os.makedirs(write_folder, exist_ok=True)
        output_filename = os.path.join(write_folder, filename)
    else:
        write_folder = actual_output
        output_filename = os.path.join(write_folder, filename)

    return output_filename


def _strip_bbox_from_metadata(metadata: dict | None) -> dict | None:
    """
    Strip bbox from metadata so it will be recomputed for each partition.

    When writing partitions, we must NOT use the original file's bbox since each
    partition contains only a subset of the data with different bounds.

    Args:
        metadata: Original metadata dict (may contain 'geo' or b'geo' key)

    Returns:
        Copy of metadata with bbox stripped from geo columns, or None
    """
    if not metadata:
        return None

    import copy
    import json

    metadata_copy = copy.deepcopy(metadata)

    # Handle both string and bytes keys
    for geo_key in ("geo", b"geo"):
        if geo_key in metadata_copy:
            geo_data = metadata_copy[geo_key]

            # Parse if needed
            if isinstance(geo_data, bytes):
                geo_dict = json.loads(geo_data.decode("utf-8"))
            elif isinstance(geo_data, str):
                geo_dict = json.loads(geo_data)
            else:
                geo_dict = copy.deepcopy(geo_data)

            # Strip bbox from each geometry column
            if "columns" in geo_dict:
                for _col_name, col_meta in geo_dict["columns"].items():
                    if "bbox" in col_meta:
                        del col_meta["bbox"]

            # Re-encode back to original format
            if isinstance(geo_data, bytes):
                metadata_copy[geo_key] = json.dumps(geo_dict).encode("utf-8")
            elif isinstance(geo_data, str):
                metadata_copy[geo_key] = json.dumps(geo_dict)
            else:
                metadata_copy[geo_key] = geo_dict

    return metadata_copy


def partition_by_column(
    input_parquet: str,
    output_folder: str,
    column_name: str,
    column_prefix_length: int | None = None,
    hive: bool = False,
    overwrite: bool = False,
    verbose: bool = False,
    keep_partition_column: bool = True,
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
) -> int:
    """
    Common function to partition a GeoParquet file by column values.

    Supports both local and remote outputs (S3, GCS, Azure). Remote outputs
    are written to a temporary local directory, then uploaded.

    Args:
        input_parquet: Input file path
        output_folder: Output directory (local path or remote URL)
        column_name: Column to partition by
        column_prefix_length: If set, use first N characters of column value
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing files
        verbose: Print detailed output
        keep_partition_column: Whether to keep the partition column in output files (default: True)
        force: Force partitioning even if analysis detects issues
        skip_analysis: Skip partition strategy analysis (for performance)
        filename_prefix: Optional prefix for partition filenames (e.g., 'fields' → fields_USA.parquet)
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        compression: Compression codec (default: ZSTD)
        compression_level: Compression level (default: 15)
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
        memory_limit: DuckDB memory limit for write operations (e.g., "2GB")

    Returns:
        Number of partitions created
    """
    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_parquet, output_folder)

    # Show remote read message
    show_remote_read_message(input_parquet, verbose)

    input_url = safe_file_url(input_parquet, verbose)

    # Run partition analysis unless skipped
    _run_partition_analysis(
        input_parquet, column_name, column_prefix_length, skip_analysis, force, verbose
    )

    with remote_write_context(output_folder, is_directory=True, verbose=verbose) as (
        actual_output,
        is_remote,
    ):
        # Get metadata before processing
        metadata, _ = get_parquet_metadata(input_parquet, verbose)

        # Create output directory
        os.makedirs(actual_output, exist_ok=True)
        if verbose:
            debug(f"Created output directory: {actual_output}")

        # Create DuckDB connection with httpfs if needed
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))

        _, partition_description = _build_column_expression(column_name, column_prefix_length)
        if verbose:
            debug(f"Partitioning by {partition_description} in a single pass...")

        # Single pass: COPY ... PARTITION_BY routes every row in one input scan
        # into a staging dir, then each (small) staging partition is rewritten
        # into its final file with correct per-partition metadata (issue #478).
        staging_dir = os.path.join(actual_output, ".gpio_partition_staging")
        shutil.rmtree(staging_dir, ignore_errors=True)
        partition_count = 0
        try:
            select_sql = _build_staging_select(
                input_url, column_name, column_prefix_length, keep_partition_column
            )
            _run_partitioned_copy(
                con, select_sql, [_PARTITION_ALIAS], staging_dir, verbose, memory_limit
            )

            if not os.path.isdir(staging_dir) or not any("=" in d for d in os.listdir(staging_dir)):
                raise PartitionError(f"No non-NULL values found in column '{column_name}'")

            for values, partition_dir in _iter_staging_partitions(staging_dir):
                output_filename = _determine_output_path(
                    actual_output,
                    values[0],
                    column_name,
                    column_prefix_length,
                    hive,
                    filename_prefix,
                )
                if _finalize_partition_file(
                    con,
                    partition_dir,
                    output_filename,
                    metadata,
                    overwrite,
                    verbose,
                    geoparquet_version=geoparquet_version,
                    compression=compression,
                    compression_level=compression_level,
                    row_group_size_mb=row_group_size_mb,
                    row_group_rows=row_group_rows,
                    memory_limit=memory_limit,
                ):
                    partition_count += 1
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)

        con.close()

        # Upload to remote if needed
        if is_remote:
            if verbose:
                debug(f"\nUploading {partition_count} partition files to {output_folder}...")

            upload_if_remote(
                actual_output, output_folder, profile=profile, is_directory=True, verbose=verbose
            )

        return partition_count
