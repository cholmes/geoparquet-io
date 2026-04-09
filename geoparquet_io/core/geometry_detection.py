"""
Geometry column detection for GeoParquet files.

This module provides functions to detect geometry columns in Parquet files
by examining GeoParquet metadata and column names.
"""

from geoparquet_io.core.logging_config import debug

STANDARD_GEOMETRY_NAMES = ["geometry", "geom", "wkb_geometry", "shape", "the_geom"]


def detect_parquet_geometry_column(parquet_file: str, verbose: bool = False) -> str | None:
    """
    Detect geometry column from a Parquet file.

    Args:
        parquet_file: Path to the Parquet file (local or remote).
        verbose: If True, print debug information.

    Returns:
        The name of the detected geometry column, or None if not found.
    """
    from geoparquet_io.core.common import get_duckdb_connection
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata
    from geoparquet_io.core.remote import needs_httpfs

    try:
        geo_meta = get_geo_metadata(parquet_file)
        if geo_meta and "primary_column" in geo_meta:
            primary_col = geo_meta["primary_column"]
            if verbose:
                debug(f"Found primary geometry column from metadata: {primary_col}")
            return primary_col

        con = get_duckdb_connection()
        if needs_httpfs(parquet_file):
            con.execute("INSTALL httpfs; LOAD httpfs;")

        result = con.execute(
            f"SELECT column_name FROM parquet_schema('{parquet_file}') "
            "WHERE column_name IS NOT NULL"
        ).fetchall()
        columns = [row[0].lower() for row in result if row[0]]

        for std_name in STANDARD_GEOMETRY_NAMES:
            if std_name in columns:
                if verbose:
                    debug(f"Found geometry column by standard name: {std_name}")
                return std_name

        if verbose:
            debug("No geometry column detected")
        return None

    except Exception as e:
        if verbose:
            debug(f"Error detecting geometry column: {e}")
        return None


def find_primary_geometry_column(parquet_file: str, verbose: bool = False) -> str | None:
    """Find the primary geometry column from GeoParquet metadata."""
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata

    try:
        geo_meta = get_geo_metadata(parquet_file)
        if geo_meta and "primary_column" in geo_meta:
            return geo_meta["primary_column"]
        return None
    except Exception as e:
        if verbose:
            debug(f"Error finding primary geometry column: {e}")
        return None


def _detect_geometry_from_query(
    con, query: str, original_metadata: dict | None = None, verbose: bool = False
) -> str | None:
    """Detect geometry column from a SQL query result."""
    try:
        describe_result = con.execute(f"DESCRIBE ({query})").fetchall()
        columns = [row[0].lower() for row in describe_result if row[0]]

        if original_metadata and "primary_column" in original_metadata:
            primary = original_metadata["primary_column"].lower()
            if primary in columns:
                if verbose:
                    debug(f"Found original primary geometry column in query: {primary}")
                return original_metadata["primary_column"]

        for std_name in STANDARD_GEOMETRY_NAMES:
            if std_name in columns:
                if verbose:
                    debug(f"Found geometry column by standard name in query: {std_name}")
                return std_name

        for row in describe_result:
            col_name, col_type = row[0], row[1] if len(row) > 1 else ""
            if col_type and "GEOMETRY" in str(col_type).upper():
                if verbose:
                    debug(f"Found geometry column by type in query: {col_name}")
                return col_name

        if verbose:
            debug("No geometry column detected in query result")
        return None

    except Exception as e:
        if verbose:
            debug(f"Error detecting geometry from query: {e}")
        return None
