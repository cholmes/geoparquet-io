"""Geometry column detection utilities for GeoParquet files.

This module provides functions to detect geometry columns in Parquet files,
using GeoParquet metadata or schema-based detection with standard column names.

Example:
    >>> from geoparquet_io.core.geometry_detection import find_primary_geometry_column
    >>> geom_col = find_primary_geometry_column("data.parquet")
    >>> print(geom_col)
    'geometry'
"""

import json

from geoparquet_io.core.logging_config import debug

# Standard geometry column names for fallback detection
STANDARD_GEOMETRY_NAMES = ["geometry", "geom", "wkb_geometry", "shape", "the_geom"]


def find_primary_geometry_column(parquet_file: str, verbose: bool = False) -> str:
    """
    Find the primary geometry column from GeoParquet metadata.

    Looks up the geometry column name from GeoParquet metadata. Falls back
    to detect_parquet_geometry_column() (which checks standard geometry column
    names in the schema) if no metadata is present or if the primary column
    is not specified. Final fallback is 'geometry'.

    Args:
        parquet_file: Path to the parquet file (local or remote URL)
        verbose: Print verbose output

    Returns:
        str: Name of the primary geometry column (defaults to 'geometry')
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata
    from geoparquet_io.core.file_utils import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)
    geo_meta = get_geo_metadata(safe_url)

    if verbose and geo_meta:
        debug(f"\nGeo metadata: {json.dumps(geo_meta, indent=2)}")

    if geo_meta:
        if isinstance(geo_meta, dict):
            primary = geo_meta.get("primary_column")
            if primary:
                return primary
        elif isinstance(geo_meta, list):
            for col in geo_meta:
                if isinstance(col, dict) and col.get("primary", False):
                    name = col.get("name")
                    if name:
                        return name

    # No geo metadata or no primary_column specified - use schema-based detection
    detected = detect_parquet_geometry_column(parquet_file, verbose=verbose)
    return detected if detected else "geometry"


def detect_parquet_geometry_column(parquet_file: str, verbose: bool = False) -> str | None:
    """
    Detect the geometry column in a Parquet file.

    Checks GeoParquet metadata first (primary_column), then falls back to
    matching column names against standard geometry column names.

    Args:
        parquet_file: Path to the parquet file (local or remote URL)
        verbose: Print verbose output

    Returns:
        str or None: Name of the geometry column, or None if not found
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata
    from geoparquet_io.core.file_utils import safe_file_url
    from geoparquet_io.core.remote import needs_httpfs

    # Import get_duckdb_connection locally to avoid circular import
    # (common.py imports from geometry_detection, and geometry_detection needs connection)

    # Normalize path for consistent handling of URLs and local files
    safe_url = safe_file_url(parquet_file, verbose=False)

    # 1. Check GeoParquet metadata first
    geo_meta = get_geo_metadata(safe_url)
    if geo_meta and isinstance(geo_meta, dict):
        primary = geo_meta.get("primary_column")
        if primary:
            if verbose:
                debug(f"Detected geometry column from metadata: {primary}")
            return primary

    # 2. Fall back to name-based detection from schema using DuckDB
    # (DuckDB handles local files, S3, GCS, and HTTP URLs natively)
    # Import here to avoid circular dependency
    from geoparquet_io.core.common import get_duckdb_connection

    con = None
    try:
        con = get_duckdb_connection(load_httpfs=needs_httpfs(safe_url))
        result = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{safe_url}')").fetchall()
        column_names = [row[0] for row in result]
        for std_name in STANDARD_GEOMETRY_NAMES:
            for col in column_names:
                if col.lower() == std_name.lower():
                    if verbose:
                        debug(f"Detected geometry column from schema: {col}")
                    return col
    except OSError as e:
        if verbose:
            debug(f"Failed to read schema: {e}")
    finally:
        if con:
            con.close()

    if verbose:
        debug("No geometry column found in parquet file")
    return None


# Public API - what gets exported with `from geometry_detection import *`
__all__ = [
    "STANDARD_GEOMETRY_NAMES",
    "find_primary_geometry_column",
    "detect_parquet_geometry_column",
]
