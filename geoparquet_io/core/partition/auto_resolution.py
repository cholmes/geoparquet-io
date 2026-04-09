#!/usr/bin/env python3

"""
Auto-resolution calculation for spatial partitioning.

This module provides utilities to automatically calculate optimal spatial index
resolutions based on target partition sizes and constraints.
"""

from __future__ import annotations

import math

from geoparquet_io.core.common import get_duckdb_connection, needs_httpfs, safe_file_url
from geoparquet_io.core.logging_config import debug, info, warn


def _get_total_row_count(
    input_parquet: str, verbose: bool = False, profile: str | None = None
) -> int:
    """
    Get total row count from parquet file.

    Args:
        input_parquet: Input file path
        verbose: Print debug messages
        profile: AWS profile name for S3 authentication (optional)

    Returns:
        Total number of rows
    """
    from geoparquet_io.core.common import setup_aws_profile_if_needed

    input_url = safe_file_url(input_parquet, verbose)

    # Create connection inside try block to ensure cleanup on any error
    con = None
    try:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))

        # Setup S3 authentication if profile specified
        if profile:
            setup_aws_profile_if_needed(con, profile)

        query = f"SELECT COUNT(*) FROM '{input_url}'"
        result = con.execute(query).fetchone()
        return result[0] if result else 0
    finally:
        if con is not None:
            con.close()


def _calculate_h3_resolution(
    total_rows: int,
    target_rows_per_partition: int,
    max_partitions: int = 10000,
    min_resolution: int = 0,
    max_resolution: int = 15,
    verbose: bool = False,
) -> int:
    """
    Calculate optimal H3 resolution for target partition size.

    H3 indexing system:
    - Resolution 0: ~122 cells globally (average cell area ~4.3M km²)
    - Each resolution level subdivides cells by ~7x (hexagonal subdivision)
    - Resolution 15: ~569 trillion cells globally (average cell area ~0.9 m²)

    Strategy:
    - Calculate target partition count from total_rows / target_rows
    - Use geometric progression to estimate resolution
    - Clamp to reasonable bounds

    Args:
        total_rows: Total number of rows in dataset
        target_rows_per_partition: Desired rows per partition
        max_partitions: Maximum allowed partitions
        min_resolution: Minimum H3 resolution (default 0)
        max_resolution: Maximum H3 resolution (default 15)
        verbose: Print debug messages

    Returns:
        Optimal H3 resolution (0-15)
    """
    if total_rows == 0:
        return min_resolution

    # Calculate target partition count
    target_partitions = total_rows / target_rows_per_partition

    # Respect max_partitions constraint
    if target_partitions > max_partitions:
        if verbose:
            warn(
                f"Target partition count ({target_partitions:.0f}) exceeds max ({max_partitions}), "
                f"adjusting target to {max_partitions}"
            )
        target_partitions = max_partitions

    # H3 has approximately 122 base cells at resolution 0
    # Each resolution level multiplies by ~7 (hexagonal subdivision)
    # Formula: cells(res) ≈ 122 × 7^res
    #
    # Solving for res: target_partitions = 122 × 7^res
    # res = log(target_partitions / 122) / log(7)

    if target_partitions < 122:
        # Very few partitions needed, use resolution 0
        estimated_resolution = 0
    else:
        estimated_resolution = math.log(target_partitions / 122) / math.log(7)
        estimated_resolution = round(estimated_resolution)

    # Clamp to valid range
    resolution = max(min_resolution, min(estimated_resolution, max_resolution))

    if verbose:
        estimated_partitions = 122 * (7**resolution)
        if estimated_partitions > 0:
            estimated_rows_per_partition = total_rows / estimated_partitions
            info(
                f"H3 auto-resolution: {resolution} "
                f"(~{estimated_partitions:.0f} partitions, ~{estimated_rows_per_partition:.0f} rows/partition)"
            )
        else:
            info(f"H3 auto-resolution: {resolution}")

    return resolution


def _calculate_quadkey_resolution(
    total_rows: int,
    target_rows_per_partition: int,
    max_partitions: int = 10000,
    min_resolution: int = 0,
    max_resolution: int = 23,
    verbose: bool = False,
) -> int:
    """
    Calculate optimal quadkey zoom level for target partition size.

    Quadkey (Bing Maps) indexing system:
    - Zoom level 0: 1 tile covering entire world
    - Each zoom level quadruples the number of tiles (2x2 subdivision)
    - Zoom level 23: ~70 trillion tiles globally

    Strategy:
    - Calculate target partition count from total_rows / target_rows
    - Use geometric progression to estimate zoom level
    - Clamp to reasonable bounds

    Args:
        total_rows: Total number of rows in dataset
        target_rows_per_partition: Desired rows per partition
        max_partitions: Maximum allowed partitions
        min_resolution: Minimum quadkey zoom level (default 0)
        max_resolution: Maximum quadkey zoom level (default 23)
        verbose: Print debug messages

    Returns:
        Optimal quadkey zoom level (0-23)
    """
    if total_rows == 0:
        return min_resolution

    # Calculate target partition count
    target_partitions = total_rows / target_rows_per_partition

    # Respect max_partitions constraint
    if target_partitions > max_partitions:
        if verbose:
            warn(
                f"Target partition count ({target_partitions:.0f}) exceeds max ({max_partitions}), "
                f"adjusting target to {max_partitions}"
            )
        target_partitions = max_partitions

    # Quadkey has 4^zoom tiles at each zoom level
    # Formula: tiles(zoom) = 4^zoom
    #
    # Solving for zoom: target_partitions = 4^zoom
    # zoom = log(target_partitions) / log(4) = log2(target_partitions) / 2

    if target_partitions <= 1:
        estimated_resolution = 0
    else:
        estimated_resolution = math.log2(target_partitions) / 2
        estimated_resolution = round(estimated_resolution)

    # Clamp to valid range
    resolution = max(min_resolution, min(estimated_resolution, max_resolution))

    if verbose:
        estimated_partitions = 4**resolution
        if estimated_partitions > 0:
            estimated_rows_per_partition = total_rows / estimated_partitions
            info(
                f"Quadkey auto-resolution: {resolution} "
                f"(~{estimated_partitions:.0f} partitions, ~{estimated_rows_per_partition:.0f} rows/partition)"
            )
        else:
            info(f"Quadkey auto-resolution: {resolution}")

    return resolution


def _calculate_a5_resolution(
    total_rows: int,
    target_rows_per_partition: int,
    max_partitions: int = 10000,
    min_resolution: int = 0,
    max_resolution: int = 30,
    verbose: bool = False,
    index_name: str = "A5",
) -> int:
    """
    Calculate optimal A5 (or S2) resolution for target partition size.

    A5/S2 indexing system:
    - Resolution 0: 6 base cells (one per cube face)
    - Each resolution level quadruples the number of cells (2x2 subdivision)
    - Resolution 30: maximum resolution (~1cm cells)

    Strategy:
    - Calculate target partition count from total_rows / target_rows
    - Use geometric progression to estimate resolution
    - Clamp to reasonable bounds

    Args:
        total_rows: Total number of rows in dataset
        target_rows_per_partition: Desired rows per partition
        max_partitions: Maximum allowed partitions
        min_resolution: Minimum A5/S2 resolution (default 0)
        max_resolution: Maximum A5/S2 resolution (default 30)
        verbose: Print debug messages
        index_name: Name of index for logging (default 'A5')

    Returns:
        Optimal A5/S2 resolution (0-30)
    """
    if total_rows == 0:
        return min_resolution

    # Calculate target partition count
    target_partitions = total_rows / target_rows_per_partition

    # Respect max_partitions constraint
    if target_partitions > max_partitions:
        if verbose:
            warn(
                f"Target partition count ({target_partitions:.0f}) exceeds max ({max_partitions}), "
                f"adjusting target to {max_partitions}"
            )
        target_partitions = max_partitions

    # A5/S2 has 6 base cells at resolution 0
    # Each resolution level multiplies by 4 (quadtree subdivision)
    # Formula: cells(res) = 6 × 4^res
    #
    # Solving for res: target_partitions = 6 × 4^res
    # res = log(target_partitions / 6) / log(4) = log2(target_partitions / 6) / 2

    if target_partitions < 6:
        # Very few partitions needed, use resolution 0
        estimated_resolution = 0
    else:
        estimated_resolution = math.log2(target_partitions / 6) / 2
        estimated_resolution = round(estimated_resolution)

    # Clamp to valid range
    resolution = max(min_resolution, min(estimated_resolution, max_resolution))

    if verbose:
        estimated_partitions = 6 * (4**resolution)
        if estimated_partitions > 0:
            estimated_rows_per_partition = total_rows / estimated_partitions
            info(
                f"{index_name} auto-resolution: {resolution} "
                f"(~{estimated_partitions:.0f} partitions, ~{estimated_rows_per_partition:.0f} rows/partition)"
            )
        else:
            info(f"{index_name} auto-resolution: {resolution}")

    return resolution


def calculate_auto_resolution(
    input_parquet: str,
    spatial_index_type: str,
    target_rows_per_partition: int,
    max_partitions: int = 10000,
    min_resolution: int | None = None,
    max_resolution: int | None = None,
    verbose: bool = False,
    profile: str | None = None,
) -> int:
    """
    Calculate optimal spatial index resolution for target partition size.

    This is the main entry point for auto-resolution calculation. It determines
    total row count, then calls the appropriate calculator for the spatial index type.

    Args:
        input_parquet: Input file path
        spatial_index_type: Type of spatial index ('h3', 'quadkey', 'a5', 's2')
        target_rows_per_partition: Desired rows per partition
        max_partitions: Maximum allowed partitions (default 10000)
        min_resolution: Minimum resolution (None = use index default)
        max_resolution: Maximum resolution (None = use index default)
        verbose: Print debug messages
        profile: AWS profile name for S3 authentication (optional)

    Returns:
        Optimal resolution for the specified spatial index

    Raises:
        ValueError: If spatial_index_type is not supported or parameters are invalid

    Examples:
        >>> # Calculate H3 resolution for ~100K rows per partition
        >>> resolution = calculate_auto_resolution(
        ...     'input.parquet',
        ...     'h3',
        ...     target_rows_per_partition=100000
        ... )
        >>> print(f"Use H3 resolution {resolution}")

        >>> # Calculate quadkey zoom level with custom bounds
        >>> zoom = calculate_auto_resolution(
        ...     'input.parquet',
        ...     'quadkey',
        ...     target_rows_per_partition=50000,
        ...     min_resolution=5,
        ...     max_resolution=12
        ... )
    """
    # Validate parameters
    if target_rows_per_partition <= 0:
        raise ValueError(
            f"target_rows_per_partition must be a positive integer, got {target_rows_per_partition}"
        )

    if max_partitions <= 0:
        raise ValueError(f"max_partitions must be a positive integer, got {max_partitions}")

    if verbose:
        debug(f"Calculating auto-resolution for {spatial_index_type}...")

    # Get total row count
    total_rows = _get_total_row_count(input_parquet, verbose, profile)

    if verbose:
        debug(f"Total rows: {total_rows:,}")

    if total_rows == 0:
        raise ValueError("Input file has no rows")

    # Calculate resolution based on spatial index type
    if spatial_index_type == "h3":
        # H3 resolution range: 0-15
        default_min = 0
        default_max = 15
        calc_func = _calculate_h3_resolution
        index_name = None  # Use default in function

    elif spatial_index_type == "quadkey":
        # Quadkey zoom level range: 0-23
        default_min = 0
        default_max = 23
        calc_func = _calculate_quadkey_resolution
        index_name = None  # Use default in function

    elif spatial_index_type == "a5":
        # A5 resolution range: 0-30
        default_min = 0
        default_max = 30
        calc_func = _calculate_a5_resolution
        index_name = "A5"

    elif spatial_index_type == "s2":
        # S2 level range: 0-30 (same as A5)
        default_min = 0
        default_max = 30
        calc_func = _calculate_a5_resolution
        index_name = "S2"

    else:
        raise ValueError(
            f"Unsupported spatial index type: {spatial_index_type}. Supported types: h3, quadkey, a5, s2"
        )

    # Use defaults if not specified
    if min_resolution is None:
        min_resolution = default_min
    if max_resolution is None:
        max_resolution = default_max

    # Calculate optimal resolution
    kwargs = {
        "total_rows": total_rows,
        "target_rows_per_partition": target_rows_per_partition,
        "max_partitions": max_partitions,
        "min_resolution": min_resolution,
        "max_resolution": max_resolution,
        "verbose": verbose,
    }
    # For A5/S2, pass index_name parameter
    if index_name is not None:
        kwargs["index_name"] = index_name

    return calc_func(**kwargs)
