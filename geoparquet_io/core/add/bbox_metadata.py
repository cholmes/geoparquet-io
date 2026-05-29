#!/usr/bin/env python3
"""Add bbox covering metadata to GeoParquet files.

This module adds bbox covering metadata to existing GeoParquet files,
enabling spatial filtering optimizations in readers that support it.

Uses DuckDB COPY TO with KV_METADATA to preserve file properties including
bloom filters and native GEOMETRY logical type (fixes #433).
"""

import json
import os

import duckdb

from geoparquet_io.core.check_parquet_structure import get_compression_info, get_row_group_stats
from geoparquet_io.core.common import check_bbox_structure, get_parquet_metadata
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.file_utils import safe_file_url
from geoparquet_io.core.geo_metadata import parse_geo_metadata
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, error, success


def _detect_native_geometry(parquet_file: str) -> bool:
    """Detect if the file uses native GEOMETRY logical type (GeoParquet 2.0).

    Args:
        parquet_file: Path to the parquet file

    Returns:
        True if the file has native GEOMETRY type, False otherwise
    """
    conn = duckdb.connect()
    try:
        result = conn.execute(f"""
            SELECT logical_type
            FROM parquet_schema('{parquet_file}')
            WHERE name = 'geometry'
        """).fetchone()

        if result and result[0]:
            logical_type = str(result[0])
            return "Geometry" in logical_type or "Geography" in logical_type
        return False
    finally:
        conn.close()


def _escape_json_for_duckdb(json_str: str) -> str:
    """Escape JSON string for use in DuckDB KV_METADATA option.

    Single quotes must be doubled for DuckDB string literals.
    """
    return json_str.replace("'", "''")


def add_bbox_metadata(
    parquet_file: str,
    verbose: bool = False,
    write_strategy: str = "duckdb-kv",
) -> None:
    """Add bbox covering metadata to a GeoParquet file.

    Updates the GeoParquet metadata to include bbox covering information,
    which enables spatial filtering optimizations in readers that support it.

    This operation preserves all file properties including bloom filters,
    native GEOMETRY logical type, compression, and row group structure.

    Args:
        parquet_file: Path to the parquet file (will be modified in place)
        verbose: Print verbose output
        write_strategy: Write strategy (currently only 'duckdb-kv' supported for
            this operation to preserve file properties)
    """
    safe_url = safe_file_url(parquet_file, verbose)

    # Check current bbox structure
    bbox_info = check_bbox_structure(parquet_file, verbose)

    if bbox_info["has_bbox_metadata"]:
        success(
            f"✓ Bbox covering metadata already exists for column '{bbox_info['bbox_column_name']}'"
        )
        return

    if not bbox_info["has_bbox_column"]:
        error("❌ No valid bbox column found in the file. Please add a bbox column first.")
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

    # Detect if file uses native GEOMETRY type (GeoParquet 2.0)
    has_native_geometry = _detect_native_geometry(parquet_file)

    if verbose:
        debug("\nPreserving file properties:")
        debug(f"Row group size: {row_group_size:,} rows")
        debug(f"Compression: {compression}")
        debug(f"Native GEOMETRY type: {has_native_geometry}")

    # Create a temporary file for the rewrite
    temp_file = parquet_file + ".tmp"
    try:
        # Use DuckDB COPY TO with KV_METADATA to preserve file properties
        # This preserves bloom filters and native GEOMETRY logical type (fixes #433)
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Escape the geo metadata JSON for DuckDB
        geo_meta_escaped = _escape_json_for_duckdb(json.dumps(geo_meta))

        # Build COPY options
        # Use GEOPARQUET_VERSION 'V2' if native geometry, 'NONE' otherwise
        # (NONE means "don't touch the geometry column, just copy as-is with custom metadata")
        geoparquet_version = "V2" if has_native_geometry else "NONE"

        copy_options = [
            "FORMAT PARQUET",
            f"COMPRESSION {compression}",
            f"GEOPARQUET_VERSION '{geoparquet_version}'",
            f"KV_METADATA {{geo: '{geo_meta_escaped}'}}",
            f"ROW_GROUP_SIZE {row_group_size}",
        ]

        copy_sql = f"""
            COPY (SELECT * FROM '{parquet_file}')
            TO '{temp_file}'
            ({", ".join(copy_options)})
        """

        if verbose:
            debug(f"\nDuckDB COPY SQL:\n{copy_sql}")

        conn.execute(copy_sql)
        conn.close()

        # Replace original file atomically
        os.replace(temp_file, parquet_file)

        success(f"✓ Added bbox covering metadata for column '{bbox_info['bbox_column_name']}'")

    except Exception as e:
        # Clean up temporary file if something goes wrong
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise GeoParquetError(f"Failed to update metadata: {str(e)}") from e
