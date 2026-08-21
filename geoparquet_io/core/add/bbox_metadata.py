#!/usr/bin/env python3
"""Add bbox covering metadata to GeoParquet files.

This module adds bbox covering metadata to existing GeoParquet files,
enabling spatial filtering optimizations in readers that support it.

Uses DuckDB COPY TO with KV_METADATA to preserve file properties including
bloom filters, native GEOMETRY logical type, and existing key-value metadata
(fixes #433).

Note: This operation only supports local files. Remote URLs (S3, GCS, Azure)
are not supported for in-place metadata modification.
"""

import json
import os

import duckdb

from geoparquet_io.core.check_parquet_structure import get_compression_info, get_row_group_stats
from geoparquet_io.core.common import (
    check_bbox_structure,
    get_duckdb_connection,
    get_parquet_metadata,
)
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.file_utils import safe_file_url
from geoparquet_io.core.geo_metadata import parse_geo_metadata
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, error, success


def _is_remote_url(path: str) -> bool:
    """Check if the path is a remote URL (S3, GCS, Azure, HTTP)."""
    remote_prefixes = ("s3://", "gs://", "az://", "azure://", "http://", "https://")
    return path.lower().startswith(remote_prefixes)


def _detect_native_geometry(
    conn: duckdb.DuckDBPyConnection, parquet_file: str, geometry_column: str
) -> bool:
    """Detect if the file uses native GEOMETRY logical type (GeoParquet 2.0).

    Args:
        conn: DuckDB connection (reused to avoid repeated INSTALL/LOAD)
        parquet_file: Path to the parquet file
        geometry_column: Name of the geometry column to check

    Returns:
        True if the file has native GEOMETRY type, False otherwise
    """
    result = conn.execute(f"""
        SELECT logical_type
        FROM parquet_schema('{parquet_file}')
        WHERE name = '{geometry_column}'
    """).fetchone()

    if result and result[0]:
        logical_type = str(result[0])
        return "Geometry" in logical_type or "Geography" in logical_type
    return False


def _get_existing_kv_metadata(conn: duckdb.DuckDBPyConnection, parquet_file: str) -> dict:
    """Read existing key-value metadata from a parquet file.

    Args:
        conn: DuckDB connection
        parquet_file: Path to the parquet file

    Returns:
        Dict mapping metadata keys to values (both as strings)
    """
    result = conn.execute(
        f"SELECT key, value FROM parquet_kv_metadata('{parquet_file}')"
    ).fetchall()

    metadata = {}
    for key, value in result:
        # Keys and values come as bytes, decode them
        key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        value_str = value.decode("utf-8") if isinstance(value, bytes) else str(value)
        metadata[key_str] = value_str

    return metadata


def _build_kv_metadata_clause(existing_metadata: dict, new_geo_meta: dict) -> str:
    """Build the KV_METADATA clause preserving existing metadata.

    DuckDB's KV_METADATA replaces all metadata, so we must include all existing
    keys we want to preserve, plus the updated geo key.

    Args:
        existing_metadata: Dict of existing key-value pairs
        new_geo_meta: The new geo metadata dict to set

    Returns:
        KV_METADATA clause string for DuckDB COPY
    """
    kv_parts = []

    # Preserve all existing metadata except 'geo' (which we're updating)
    for key, value in existing_metadata.items():
        if key == "geo":
            continue  # Skip - we'll add the updated geo below
        # Escape single quotes for DuckDB string literals
        escaped_value = value.replace("'", "''")
        kv_parts.append(f"{key}: '{escaped_value}'")

    # Add the new geo metadata
    geo_json = json.dumps(new_geo_meta)
    escaped_geo = geo_json.replace("'", "''")
    kv_parts.append(f"geo: '{escaped_geo}'")

    return f"KV_METADATA {{{', '.join(kv_parts)}}}"


def add_bbox_metadata(
    parquet_file: str,
    verbose: bool = False,
) -> None:
    """Add bbox covering metadata to a GeoParquet file.

    Updates the GeoParquet metadata to include bbox covering information,
    which enables spatial filtering optimizations in readers that support it.

    This operation preserves all file properties including:
    - Bloom filters on all columns
    - Native GEOMETRY logical type (GeoParquet 2.0)
    - Compression settings
    - Row group structure
    - All existing key-value metadata (pandas, ARROW:schema, custom, etc.)

    Note: Only local files are supported. Remote URLs will raise an error.

    Args:
        parquet_file: Path to the parquet file (will be modified in place)
        verbose: Print verbose output

    Raises:
        GeoParquetError: If the file is remote or the operation fails
    """
    # Reject remote URLs - in-place modification requires local filesystem
    if _is_remote_url(parquet_file):
        raise GeoParquetError(
            f"Remote URLs are not supported for in-place metadata modification: {parquet_file}\n"
            "Download the file locally, modify it, then upload."
        )

    safe_url = safe_file_url(parquet_file, verbose)

    # Check current bbox structure
    bbox_info = check_bbox_structure(safe_url, verbose)

    if bbox_info["has_bbox_metadata"]:
        success(
            f"Bbox covering metadata already exists for column '{bbox_info['bbox_column_name']}'"
        )
        return

    if not bbox_info["has_bbox_column"]:
        error("No valid bbox column found in the file. Please add a bbox column first.")
        return

    # Get existing metadata
    metadata, _ = get_parquet_metadata(safe_url)
    geo_meta = parse_geo_metadata(metadata, False)

    if not geo_meta:
        geo_meta = {"version": "1.1.0", "primary_column": "geometry", "columns": {}}

    # Find primary geometry column
    primary_col = find_primary_geometry_column(safe_url, verbose)

    # Update or create the columns section
    if "columns" not in geo_meta:
        geo_meta["columns"] = {}

    if primary_col not in geo_meta["columns"]:
        geo_meta["columns"][primary_col] = {}

    # Add bbox covering metadata
    geo_meta["columns"][primary_col]["covering"] = {
        "bbox": {
            "xmin": [bbox_info["bbox_column_name"], "xmin"],
            "ymin": [bbox_info["bbox_column_name"], "ymin"],
            "xmax": [bbox_info["bbox_column_name"], "xmax"],
            "ymax": [bbox_info["bbox_column_name"], "ymax"],
        }
    }

    if verbose:
        debug("\nUpdated geo metadata:")
        debug(json.dumps(geo_meta, indent=2))

    # Get original file properties
    row_group_stats = get_row_group_stats(parquet_file)
    compression_info = get_compression_info(parquet_file, primary_col)
    row_group_size = int(row_group_stats["avg_rows_per_group"])
    compression = compression_info[primary_col]

    # Create a temporary file for the rewrite
    temp_file = parquet_file + ".tmp"
    try:
        # Use DuckDB COPY TO with KV_METADATA to preserve file properties
        # This preserves bloom filters and native GEOMETRY logical type (fixes #433)
        conn = get_duckdb_connection()

        # Detect if file uses native GEOMETRY type (GeoParquet 2.0)
        # Pass the actual geometry column name - it might not be 'geometry'
        has_native_geometry = _detect_native_geometry(conn, safe_url, primary_col)

        # Read existing KV metadata to preserve non-geo keys
        existing_kv = _get_existing_kv_metadata(conn, safe_url)

        if verbose:
            debug("\nPreserving file properties:")
            debug(f"Row group size: {row_group_size:,} rows")
            debug(f"Compression: {compression}")
            debug(f"Native GEOMETRY type: {has_native_geometry}")
            debug(f"Existing metadata keys: {list(existing_kv.keys())}")

        # Build KV_METADATA clause preserving all existing metadata
        kv_metadata_clause = _build_kv_metadata_clause(existing_kv, geo_meta)

        # Build COPY options
        # Use GEOPARQUET_VERSION 'V2' if native geometry, 'NONE' otherwise
        # (NONE means "don't touch the geometry column, just copy as-is with custom metadata")
        geoparquet_version = "V2" if has_native_geometry else "NONE"

        copy_options = [
            "FORMAT PARQUET",
            f"COMPRESSION {compression}",
            f"GEOPARQUET_VERSION '{geoparquet_version}'",
            kv_metadata_clause,
            f"ROW_GROUP_SIZE {row_group_size}",
        ]

        copy_sql = f"""
            COPY (SELECT * FROM '{safe_url}')
            TO '{temp_file}'
            ({", ".join(copy_options)})
        """

        if verbose:
            debug(f"\nDuckDB COPY SQL:\n{copy_sql}")

        conn.execute(copy_sql)
        conn.close()

        # Replace original file atomically
        os.replace(temp_file, parquet_file)

        success(f"Added bbox covering metadata for column '{bbox_info['bbox_column_name']}'")

    except Exception as e:
        # Clean up temporary file if something goes wrong
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise GeoParquetError(f"Failed to update metadata: {str(e)}") from e
