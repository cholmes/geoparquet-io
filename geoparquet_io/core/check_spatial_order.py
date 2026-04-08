#!/usr/bin/env python3


import random as _random

from geoparquet_io.core.common import (
    find_primary_geometry_column,
    get_duckdb_connection,
    needs_httpfs,
    safe_file_url,
)
from geoparquet_io.core.logging_config import debug, progress


def _bboxes_overlap(bbox1: dict, bbox2: dict) -> bool:
    """Check if two bounding boxes overlap.

    Two bounding boxes overlap if they share any interior area.
    Boxes that only touch at edges or corners are not considered overlapping.

    Args:
        bbox1: First bbox dict with xmin, ymin, xmax, ymax
        bbox2: Second bbox dict with xmin, ymin, xmax, ymax

    Returns:
        True if bboxes overlap, False otherwise
    """
    # Boxes overlap if they overlap in BOTH X and Y dimensions
    # X overlap: bbox1.xmax > bbox2.xmin AND bbox2.xmax > bbox1.xmin
    # Y overlap: bbox1.ymax > bbox2.ymin AND bbox2.ymax > bbox1.ymin
    x_overlap = bbox1["xmax"] > bbox2["xmin"] and bbox2["xmax"] > bbox1["xmin"]
    y_overlap = bbox1["ymax"] > bbox2["ymin"] and bbox2["ymax"] > bbox1["ymin"]
    return x_overlap and y_overlap


def _calculate_consecutive_avg(con, safe_url, geometry_column, row_limit, verbose):
    """Calculate average distance between consecutive features."""
    query = f"""
    WITH numbered AS (
        SELECT ROW_NUMBER() OVER () as id, {geometry_column} as geom
        FROM '{safe_url}' {row_limit}
    )
    SELECT AVG(ST_Distance(a.geom, b.geom)) as avg_dist
    FROM numbered a JOIN numbered b ON b.id = a.id + 1;
    """
    if verbose:
        progress("Calculating average distance between consecutive features...")
    result = con.execute(query).fetchone()
    avg = result[0] if result else None
    if verbose:
        debug(f"Average distance between consecutive features: {avg}")
    return avg


def _calculate_random_avg(con, safe_url, geometry_column, row_limit, random_sample_size, verbose):
    """Calculate average distance between random pairs of features."""
    query = f"""
    WITH sample AS (SELECT {geometry_column} as geom FROM '{safe_url}' {row_limit}),
    random_pairs AS (
        SELECT a.geom as geom1, b.geom as geom2
        FROM (SELECT geom FROM sample ORDER BY random() LIMIT {random_sample_size}) a,
             (SELECT geom FROM sample ORDER BY random() LIMIT {random_sample_size}) b
        WHERE a.geom != b.geom
    )
    SELECT AVG(ST_Distance(geom1, geom2)) as avg_dist FROM random_pairs;
    """
    if verbose:
        progress(f"Calculating average distance between {random_sample_size} random pairs...")
    result = con.execute(query).fetchone()
    avg = result[0] if result else None
    if verbose:
        debug(f"Average distance between random features: {avg}")
    return avg


def _build_results_dict(ratio, consecutive_avg, random_avg):
    """Build structured results dictionary for sampling method."""
    passed = ratio is not None and ratio < 0.5
    issues = []
    recommendations = []
    if ratio is not None and ratio >= 0.5:
        issues.append(f"Poor spatial ordering (ratio: {ratio:.2f})")
        recommendations.append("Apply Hilbert spatial ordering for better query performance")
    return {
        "passed": passed,
        "ratio": ratio,
        "consecutive_avg": consecutive_avg,
        "random_avg": random_avg,
        "method": "sampling",
        "issues": issues,
        "recommendations": recommendations,
        "fix_available": not passed,
    }


def _get_row_limit_clause(con, safe_url, limit_rows, verbose):
    """Determine row limit clause based on total rows."""
    total_rows = con.execute(f"SELECT COUNT(*) FROM '{safe_url}'").fetchone()[0]
    if verbose:
        debug(f"Total rows in file: {total_rows:,}")

    if total_rows > limit_rows:
        if verbose:
            debug(f"Limiting analysis to first {limit_rows:,} rows")
        return f"LIMIT {limit_rows}"
    return ""


def _print_standalone_results(ratio, consecutive_avg, random_avg):
    """Print results when running as standalone command (not from check_all)."""
    progress("\nResults:")
    debug(f"Average distance between consecutive features: {consecutive_avg}")
    debug(f"Average distance between random features: {random_avg}")
    progress(f"Ratio (consecutive / random): {ratio}")

    if ratio is not None and ratio < 0.5:
        progress("=> Data seems strongly spatially clustered.")
    elif ratio is not None:
        progress("=> Data might not be strongly clustered (or is partially clustered).")


def _print_bbox_stats_results(ratio, overlap_count, total_pairs):
    """Print bbox-stats results when running as standalone command."""
    progress("\nResults:")
    debug(f"Row group pairs analyzed: {total_pairs}")
    debug(f"Overlapping pairs: {overlap_count}")
    progress(f"Overlap ratio: {ratio:.2f}")

    if ratio < 0.3:
        progress("=> Data appears well spatially ordered (low row group overlap).")
    else:
        progress("=> Data may benefit from spatial ordering (high row group overlap).")


def check_spatial_order_bbox_stats(parquet_file, verbose=False, return_results=False, quiet=False):
    """Check spatial ordering using row group bbox statistics.

    This method is faster than sampling because it only reads row group metadata
    instead of actual geometry data. It checks if consecutive row groups have
    overlapping bounding boxes, which indicates poor spatial ordering.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict
        quiet: If True, suppress all output

    Returns:
        ratio (float) if return_results=False, or dict if return_results=True
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_per_row_group_bbox_stats,
        has_bbox_column,
    )

    safe_url = safe_file_url(parquet_file, verbose)

    # Check for bbox column
    has_bbox, bbox_col_name = has_bbox_column(safe_url)
    if not has_bbox or not bbox_col_name:
        raise ValueError(
            f"File {parquet_file} does not have a bbox column. "
            "Use the sampling-based method instead."
        )

    if verbose:
        debug(f"Using bbox column: {bbox_col_name}")

    # Get bbox stats per row group
    row_group_bboxes = get_per_row_group_bbox_stats(safe_url, bbox_col_name)

    if verbose:
        debug(f"Analyzing {len(row_group_bboxes)} row groups")

    # Detect if this looks like Hilbert ordering with large row groups
    # (row groups with ~100k rows each AND high spatial overlap is expected)
    likely_hilbert_with_large_groups = False
    if len(row_group_bboxes) >= 5:  # Needs multiple row groups to be meaningful
        # Check average rows per group from parquet metadata
        from geoparquet_io.core.common import get_duckdb_connection, needs_httpfs

        con = get_duckdb_connection(load_spatial=False, load_httpfs=needs_httpfs(parquet_file))
        try:
            result = con.execute(f"""
                SELECT num_rows::DOUBLE / num_row_groups as avg_rows
                FROM parquet_file_metadata('{safe_url}')
            """).fetchone()
            avg_rows = result[0] if result else 0
            # Require BOTH correct row count AND high spatial overlap
            # (Hilbert curves with large row groups inherently have high bbox overlap)
            if 80000 <= avg_rows <= 120000:
                # Calculate overlap ratio to confirm spatial characteristic
                prelim_overlap_count = 0
                for i in range(len(row_group_bboxes) - 1):
                    if _bboxes_overlap(row_group_bboxes[i], row_group_bboxes[i + 1]):
                        prelim_overlap_count += 1
                prelim_ratio = prelim_overlap_count / (len(row_group_bboxes) - 1)

                # High overlap (>70%) + correct row count = likely Hilbert
                if prelim_ratio > 0.7:
                    likely_hilbert_with_large_groups = True
                    if verbose:
                        debug(
                            f"Detected likely Hilbert ordering (avg {avg_rows:.0f} rows/group, {prelim_ratio:.0%} overlap)"
                        )
        finally:
            con.close()

    # Handle edge cases
    if len(row_group_bboxes) <= 1:
        # Can't meaningfully check ordering with 0 or 1 row groups
        # (need at least 2 groups to compare consecutive pairs).
        # Return ratio=0.0 (perfect ordering) since there's no evidence of poor ordering.
        if verbose:
            debug("Only one or zero row groups - assuming well ordered")
        ratio = 0.0
        overlap_count = 0
        total_pairs = 0
    else:
        # Count overlaps in consecutive row group pairs
        overlap_count = 0
        for i in range(len(row_group_bboxes) - 1):
            bbox1 = row_group_bboxes[i]
            bbox2 = row_group_bboxes[i + 1]
            if _bboxes_overlap(bbox1, bbox2):
                overlap_count += 1
                if verbose:
                    debug(f"Row groups {bbox1['row_group_id']} and {bbox2['row_group_id']} overlap")

        total_pairs = len(row_group_bboxes) - 1
        ratio = overlap_count / total_pairs if total_pairs > 0 else 0.0

        if verbose:
            debug(f"Overlapping pairs: {overlap_count}/{total_pairs}")

    # Pass if < 30% overlap, OR if Hilbert-ordered with large groups (expected behavior)
    passed = ratio < 0.3 or likely_hilbert_with_large_groups

    # Build results dict
    issues = []
    recommendations = []
    if not passed:
        issues.append(f"Poor spatial ordering (overlap ratio: {ratio:.2f})")
        recommendations.append("Apply Hilbert spatial ordering for better query performance")

    # Print standalone results if not quiet and not return_results
    if not quiet and not return_results and not verbose:
        _print_bbox_stats_results(ratio, overlap_count, total_pairs)

    if return_results:
        return {
            "passed": passed,
            "ratio": ratio,
            "overlap_count": overlap_count,
            "total_pairs": total_pairs,
            "method": "bbox_stats",
            "issues": issues,
            "recommendations": recommendations,
            # Don't offer fix if already Hilbert-ordered with large groups
            "fix_available": not passed and not likely_hilbert_with_large_groups,
        }

    return ratio


def check_spatial_order(
    parquet_file, random_sample_size, limit_rows, verbose, return_results=False, quiet=False
):
    """Check if a GeoParquet file is spatially ordered.

    Automatically detects if the file has a bbox column (GeoParquet 2.0+) and uses
    the faster bbox-stats method. Falls back to sampling method for older files.

    Args:
        parquet_file: Path to parquet file
        random_sample_size: Number of rows in each random sample (sampling method only)
        limit_rows: Max number of rows to analyze (sampling method only)
        verbose: Print additional information
        return_results: If True, return structured results dict
        quiet: If True, suppress all output (for multi-file batch mode)

    Returns:
        ratio (float) if return_results=False, or dict if return_results=True
    """
    from geoparquet_io.core.duckdb_metadata import has_bbox_column
    from geoparquet_io.core.logging_config import warn

    safe_url = safe_file_url(parquet_file, verbose)

    # Try bbox-stats method first (faster)
    has_bbox, bbox_col_name = has_bbox_column(safe_url)
    if has_bbox and bbox_col_name:
        if verbose:
            debug(f"Using bbox-stats method (bbox column: {bbox_col_name})")
        try:
            return check_spatial_order_bbox_stats(
                parquet_file, verbose=verbose, return_results=return_results, quiet=quiet
            )
        except (ValueError, KeyError, IndexError) as e:
            # ValueError: Invalid bbox column structure
            # KeyError: Missing expected bbox fields (xmin, ymin, xmax, ymax)
            # IndexError: Empty or malformed row group stats
            if verbose:
                warn(f"Bbox-stats method failed: {e}, falling back to sampling")
            # Fall through to sampling method

    # Fall back to sampling method
    if verbose or not quiet:
        warn(
            "Bbox column not found - using slower sampling method. "
            "For faster checks, add bbox column with 'gpio add bbox'."
        )

    geometry_column = find_primary_geometry_column(parquet_file, verbose)
    if verbose:
        debug(f"Using geometry column: {geometry_column}")
        debug("Using sampling method")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))
    try:
        row_limit = _get_row_limit_clause(con, safe_url, limit_rows, verbose)

        consecutive_avg = _calculate_consecutive_avg(
            con, safe_url, geometry_column, row_limit, verbose
        )
        random_avg = _calculate_random_avg(
            con, safe_url, geometry_column, row_limit, random_sample_size, verbose
        )

        ratio = consecutive_avg / random_avg if consecutive_avg and random_avg else None

        if not verbose and not quiet:
            _print_standalone_results(ratio, consecutive_avg, random_avg)

        if return_results:
            return _build_results_dict(ratio, consecutive_avg, random_avg)

        return ratio
    finally:
        con.close()


def _compute_data_extent(row_group_bboxes: list[dict]) -> dict:
    """Compute the total spatial extent across all row group bboxes.

    Args:
        row_group_bboxes: List of dicts with xmin, ymin, xmax, ymax keys.

    Returns:
        Dict with xmin, ymin, xmax, ymax for the full extent.

    Raises:
        ValueError: If row_group_bboxes is empty.
    """
    if not row_group_bboxes:
        raise ValueError("No row group bboxes provided")
    return {
        "xmin": min(b["xmin"] for b in row_group_bboxes),
        "ymin": min(b["ymin"] for b in row_group_bboxes),
        "xmax": max(b["xmax"] for b in row_group_bboxes),
        "ymax": max(b["ymax"] for b in row_group_bboxes),
    }


def _generate_sample_query_bboxes(
    extent: dict,
    num_samples: int = 10,
    query_fraction: float = 0.1,
    seed: int | None = None,
) -> list[dict]:
    """Generate random sample query bboxes within the data extent.

    Each sample covers approximately ``query_fraction`` of the extent in each
    dimension (so the area fraction is roughly query_fraction^2).

    Args:
        extent: Dict with xmin, ymin, xmax, ymax for the full data extent.
        num_samples: Number of sample bboxes to generate.
        query_fraction: Fraction of each dimension the query should span.
        seed: Optional random seed for reproducibility.

    Returns:
        List of bbox dicts with xmin, ymin, xmax, ymax.
    """
    rng = _random.Random(seed)  # nosec B311 - not used for security
    x_range = extent["xmax"] - extent["xmin"]
    y_range = extent["ymax"] - extent["ymin"]
    query_width = x_range * query_fraction
    query_height = y_range * query_fraction

    samples = []
    for _ in range(num_samples):
        x_start = rng.uniform(extent["xmin"], extent["xmax"] - query_width)  # nosec B311
        y_start = rng.uniform(extent["ymin"], extent["ymax"] - query_height)  # nosec B311
        samples.append(
            {
                "xmin": x_start,
                "ymin": y_start,
                "xmax": x_start + query_width,
                "ymax": y_start + query_height,
            }
        )
    return samples


def _compute_skip_rate_for_query(query_bbox: dict, row_group_bboxes: list[dict]) -> float:
    """Compute the fraction of row groups that can be skipped for a query bbox.

    A row group can be skipped if its bbox does not overlap with the query bbox.

    Args:
        query_bbox: The query bbox dict with xmin, ymin, xmax, ymax.
        row_group_bboxes: List of row group bbox dicts.

    Returns:
        Float between 0.0 and 1.0 representing the fraction skippable.
    """
    if not row_group_bboxes:
        return 0.0
    skipped = sum(1 for rg in row_group_bboxes if not _bboxes_overlap(query_bbox, rg))
    return skipped / len(row_group_bboxes)


def _compute_avg_bbox_area_ratio(row_group_bboxes: list[dict], extent: dict) -> float:
    """Compute average ratio of row group bbox area to total extent area.

    Lower values mean tighter row group bboxes (better spatial locality).

    Args:
        row_group_bboxes: List of row group bbox dicts.
        extent: The total data extent dict.

    Returns:
        Average area ratio (0.0 to 1.0). Returns 0.0 if extent area is zero.
    """
    extent_area = (extent["xmax"] - extent["xmin"]) * (extent["ymax"] - extent["ymin"])
    if extent_area <= 0:
        return 0.0
    ratios = []
    for rg in row_group_bboxes:
        rg_area = (rg["xmax"] - rg["xmin"]) * (rg["ymax"] - rg["ymin"])
        ratios.append(rg_area / extent_area)
    return sum(ratios) / len(ratios) if ratios else 0.0


def check_spatial_pushdown_readiness(
    parquet_file: str,
    verbose: bool = False,
    num_samples: int = 20,
    query_fraction: float = 0.1,
    seed: int = 42,
) -> dict:
    """Check how well a file supports spatial filter pushdown.

    Evaluates whether the file has geo_bbox metadata per row group, measures
    spatial locality, and estimates what percentage of row groups a typical
    regional query could skip.

    Args:
        parquet_file: Path to the GeoParquet file.
        verbose: If True, log detailed progress.
        num_samples: Number of random sample queries to evaluate.
        query_fraction: Fraction of each dimension each sample query spans.
        seed: Random seed for reproducible sample queries.

    Returns:
        Dict with keys:
            has_geo_bbox (bool): Whether file has per-RG geo_bbox stats.
            num_row_groups (int): Number of row groups in the file.
            estimated_skip_rate (float): Average fraction of RGs skippable.
            avg_bbox_area_ratio (float): Average RG bbox area / total extent area.
            passed (bool): True if skip rate >= 0.5 (good pushdown readiness).
            issues (list[str]): Problems found.
            recommendations (list[str]): Suggestions for improvement.
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_per_row_group_bbox_stats,
        has_bbox_column,
    )

    safe_url = safe_file_url(parquet_file, verbose)

    has_bbox, bbox_col_name = has_bbox_column(safe_url)

    issues: list[str] = []
    recommendations: list[str] = []

    if not has_bbox or not bbox_col_name:
        if verbose:
            debug("No geo_bbox column found, pushdown not possible")
        issues.append(
            "File has no geo_bbox column. "
            "Spatial filter pushdown requires per-row-group bbox stats (GeoParquet 2.0+)."
        )
        recommendations.append("Add bbox column with 'gpio add bbox' and upgrade to GeoParquet 2.0")
        return {
            "has_geo_bbox": False,
            "num_row_groups": 0,
            "estimated_skip_rate": 0.0,
            "avg_bbox_area_ratio": 0.0,
            "passed": False,
            "issues": issues,
            "recommendations": recommendations,
        }

    if verbose:
        debug(f"Using bbox column: {bbox_col_name}")

    row_group_bboxes = get_per_row_group_bbox_stats(safe_url, bbox_col_name)
    num_rgs = len(row_group_bboxes)

    if verbose:
        debug(f"Found {num_rgs} row groups with bbox stats")

    if num_rgs <= 1:
        if verbose:
            debug("Only 0 or 1 row groups, skip rate is trivially 0.0")
        return {
            "has_geo_bbox": True,
            "num_row_groups": num_rgs,
            "estimated_skip_rate": 0.0,
            "avg_bbox_area_ratio": 0.0,
            "passed": False,  # Can't skip any row groups with only 0-1 row groups
            "issues": ["Single row group provides no pushdown benefit"],
            "recommendations": ["Consider using smaller row groups for spatial queries"],
        }

    extent = _compute_data_extent(row_group_bboxes)
    avg_area_ratio = _compute_avg_bbox_area_ratio(row_group_bboxes, extent)

    if verbose:
        debug(f"Data extent: {extent}")
        debug(f"Average bbox area ratio: {avg_area_ratio:.4f}")

    sample_bboxes = _generate_sample_query_bboxes(
        extent, num_samples=num_samples, query_fraction=query_fraction, seed=seed
    )
    skip_rates = [_compute_skip_rate_for_query(s, row_group_bboxes) for s in sample_bboxes]
    avg_skip_rate = sum(skip_rates) / len(skip_rates)

    if verbose:
        debug(f"Estimated average skip rate: {avg_skip_rate:.2%}")

    passed = avg_skip_rate >= 0.5

    if not passed:
        issues.append(
            f"Low spatial filter pushdown efficiency (estimated skip rate: {avg_skip_rate:.0%})"
        )
        recommendations.append(
            "Apply Hilbert spatial ordering with 'gpio sort hilbert' to improve pushdown"
        )

    if avg_area_ratio > 0.5:
        issues.append(
            f"Row group bboxes are large relative to data extent (avg ratio: {avg_area_ratio:.2f})"
        )
        recommendations.append(
            "Spatially sorting and re-partitioning may produce tighter row group bboxes"
        )

    return {
        "has_geo_bbox": True,
        "num_row_groups": num_rgs,
        "estimated_skip_rate": avg_skip_rate,
        "avg_bbox_area_ratio": avg_area_ratio,
        "passed": passed,
        "issues": issues,
        "recommendations": recommendations,
    }
