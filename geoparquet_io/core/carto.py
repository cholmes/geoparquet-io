"""Carto SQL API to GeoParquet conversion.

Extracts data from Carto SQL API endpoints using DuckDB's ST_Read
for efficient GeoJSON parsing.
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import quote, urlparse

import pyarrow as pa

from geoparquet_io.core.common import (
    GeoParquetError,
    InvalidParameterError,
    get_duckdb_connection,
    write_geoparquet_table,
)
from geoparquet_io.core.crs_utils import parse_crs_string_to_projjson
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)


class CartoError(GeoParquetError):
    """Carto-specific error."""

    pass


def _validate_carto_url(url: str) -> str:
    """Validate and normalize Carto SQL API URL.

    Args:
        url: Carto SQL API URL

    Returns:
        Normalized URL ending with /api/v2/sql

    Raises:
        InvalidParameterError: If URL is not a valid Carto SQL API endpoint
    """
    url = url.rstrip("/")
    parsed = urlparse(url)

    if not parsed.scheme:
        raise InvalidParameterError(
            "url",
            f"Invalid URL: {url}. Must include scheme (https://)",
        )

    # Accept URLs ending with /api/v2/sql or /api/v1/sql
    if url.endswith("/api/v2/sql") or url.endswith("/api/v1/sql"):
        return url

    # Try to construct the URL if user gave base domain
    if not parsed.path or parsed.path == "/":
        # User gave just the domain, append standard path
        return f"{url}/api/v2/sql"

    raise InvalidParameterError(
        "url",
        f"Invalid Carto SQL API URL: {url}. "
        "Expected format: https://account.carto.com/api/v2/sql or https://account.carto.com",
    )


def _build_carto_query(
    table_name: str,
    columns: list[str] | None = None,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
) -> str:
    """Build SQL query for Carto API.

    Args:
        table_name: Name of the table to query
        columns: Columns to select (None = all)
        where: SQL WHERE clause
        bbox: Bounding box filter (minx, miny, maxx, maxy)
        limit: Maximum rows to return

    Returns:
        SQL query string
    """
    # Column selection
    if columns:
        # Always include the_geom for geometry
        if "the_geom" not in columns:
            columns = [*columns, "the_geom"]
        col_str = ", ".join(columns)
    else:
        col_str = "*"

    sql = f"SELECT {col_str} FROM {table_name}"

    # Build WHERE clause
    conditions = []
    if where:
        conditions.append(f"({where})")
    if bbox:
        minx, miny, maxx, maxy = bbox
        # Use ST_Intersects with ST_MakeEnvelope for spatial filter
        conditions.append(
            f"ST_Intersects(the_geom, ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 4326))"
        )

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    if limit:
        sql += f" LIMIT {limit}"

    return sql


def _get_row_count(
    url: str,
    table_name: str,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
) -> int:
    """Get row count from Carto table with optional filters.

    Args:
        url: Carto SQL API URL
        table_name: Table name
        where: Optional WHERE clause
        bbox: Optional bounding box filter

    Returns:
        Number of rows matching the filter
    """
    # Build count query with same filters
    sql = f"SELECT COUNT(*) as count FROM {table_name}"

    conditions = []
    if where:
        conditions.append(f"({where})")
    if bbox:
        minx, miny, maxx, maxy = bbox
        conditions.append(
            f"ST_Intersects(the_geom, ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 4326))"
        )

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    full_url = f"{url}?q={quote(sql)}"

    conn = get_duckdb_connection()
    result = conn.execute(f"SELECT rows[1].count FROM read_json_auto('{full_url}')").fetchone()

    return int(result[0]) if result else 0


def carto_to_table(
    url: str,
    table_name: str,
    *,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    verbose: bool = False,
) -> pa.Table:
    """Extract data from Carto SQL API to PyArrow Table.

    Uses DuckDB's ST_Read to efficiently parse GeoJSON from Carto's
    SQL API endpoint.

    Args:
        url: Carto SQL API URL (e.g., https://phl.carto.com/api/v2/sql)
        table_name: Name of the table to query
        where: SQL WHERE clause for filtering
        bbox: Bounding box filter as (minx, miny, maxx, maxy) in WGS84
        limit: Maximum number of rows to return
        include_cols: Comma-separated column names to include
        exclude_cols: Comma-separated column names to exclude (applied after fetch)
        verbose: Enable verbose output

    Returns:
        PyArrow Table with WKB geometry column named 'geometry'

    Raises:
        CartoError: If the Carto API request fails
        InvalidParameterError: If URL is invalid
    """
    configure_verbose(verbose)

    # Validate URL
    url = _validate_carto_url(url)
    debug(f"Carto URL: {url}")

    # Parse column lists
    include_list = [c.strip() for c in include_cols.split(",")] if include_cols else None
    exclude_set = {c.strip() for c in exclude_cols.split(",")} if exclude_cols else set()

    # Get row count for progress
    try:
        total_count = _get_row_count(url, table_name, where, bbox)
        info(f"Table: {table_name}")
        info(f"Total rows matching filter: {total_count:,}")
    except Exception as e:
        debug(f"Could not get row count: {e}")
        total_count = None

    if total_count == 0:
        warn("No rows match the specified filters")
        # Return empty table with geometry column
        return pa.table({"geometry": pa.array([], type=pa.binary())})

    # Build query
    sql = _build_carto_query(
        table_name=table_name,
        columns=include_list,
        where=where,
        bbox=bbox,
        limit=limit,
    )
    debug(f"SQL: {sql}")

    # Construct full URL with GeoJSON format
    full_url = f"{url}?q={quote(sql)}&format=GeoJSON"
    debug(f"Request URL: {full_url[:100]}...")

    # Use DuckDB ST_Read to fetch and parse GeoJSON
    progress("Fetching data from Carto...")
    conn = get_duckdb_connection()
    conn.execute("SET allow_asterisks_in_http_paths = true")

    try:
        table = conn.execute(f'SELECT * FROM ST_Read("{full_url}")').arrow().read_all()
    except Exception as e:
        error_msg = str(e)
        if "404" in error_msg or "Not Found" in error_msg:
            raise CartoError(
                f"Table '{table_name}' not found. Check the table name and ensure "
                f"it is publicly accessible."
            ) from e
        if "timeout" in error_msg.lower():
            raise CartoError(
                "Request timed out. The table may be too large. "
                "Try using --limit or --where to reduce the result set."
            ) from e
        raise CartoError(f"Failed to fetch data from Carto: {e}") from e

    if table.num_rows == 0:
        warn("Query returned no rows")
        return pa.table({"geometry": pa.array([], type=pa.binary())})

    debug(f"Received {table.num_rows:,} rows")

    # Rename 'geom' to 'geometry' for consistency
    # DuckDB ST_Read uses 'geom' by default
    col_names = table.column_names
    if "geom" in col_names:
        idx = col_names.index("geom")
        col_names[idx] = "geometry"
        table = table.rename_columns(col_names)

    # Remove OGC_FID if present (added by ST_Read)
    if "OGC_FID" in table.column_names:
        cols_to_keep = [c for c in table.column_names if c != "OGC_FID"]
        table = table.select(cols_to_keep)

    # Apply column exclusions
    if exclude_set:
        cols_to_keep = [c for c in table.column_names if c not in exclude_set]
        if cols_to_keep:
            table = table.select(cols_to_keep)
            debug(f"Excluded columns: {exclude_set}")

    # Add CRS metadata (Carto uses WGS84)
    crs = parse_crs_string_to_projjson("OGC:CRS84")
    if crs:
        geo_metadata = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "crs": crs,
                    "geometry_types": [],  # Mixed types possible
                }
            },
        }

        existing_metadata = table.schema.metadata or {}
        new_metadata = {**existing_metadata, b"geo": json.dumps(geo_metadata).encode("utf-8")}
        table = table.replace_schema_metadata(new_metadata)

    success(f"Extracted {table.num_rows:,} features")
    return table


def convert_carto_to_geoparquet(
    url: str,
    table_name: str,
    output_file: str,
    *,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    skip_hilbert: bool = False,
    skip_bbox: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    verbose: bool = False,
) -> None:
    """Extract Carto table and save as optimized GeoParquet.

    Args:
        url: Carto SQL API URL
        table_name: Name of the table to query
        output_file: Output GeoParquet file path
        where: SQL WHERE clause for filtering
        bbox: Bounding box filter as (minx, miny, maxx, maxy)
        limit: Maximum rows to extract
        include_cols: Comma-separated columns to include
        exclude_cols: Comma-separated columns to exclude
        skip_hilbert: Skip Hilbert curve sorting
        skip_bbox: Skip adding bbox column
        compression: Compression algorithm
        compression_level: Compression level
        row_group_size_mb: Row group size in MB
        row_group_rows: Row group size in rows
        geoparquet_version: GeoParquet version
        overwrite: Overwrite existing file
        verbose: Enable verbose output
    """
    configure_verbose(verbose)

    # Check output file
    output_path = Path(output_file)
    if output_path.exists() and not overwrite:
        raise CartoError(f"Output file exists: {output_file}\nUse --overwrite to replace it.")

    # Fetch data
    table = carto_to_table(
        url=url,
        table_name=table_name,
        where=where,
        bbox=bbox,
        limit=limit,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        verbose=verbose,
    )

    # Apply Hilbert ordering (unless skipped)
    if not skip_hilbert and table.num_rows > 0:
        progress("Applying Hilbert curve ordering...")
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        table = hilbert_order_table(table, geometry_column="geometry")
        debug("Hilbert sort complete")

    # Add bbox column (unless skipped)
    if not skip_bbox and table.num_rows > 0:
        progress("Adding bbox column...")
        from geoparquet_io.core.add.bbox import add_bbox_table

        table = add_bbox_table(table, geometry_column="geometry")
        debug("Bbox column added")

    # Write output
    progress(f"Writing to {output_file}...")
    write_geoparquet_table(
        table,
        output_file,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version,
    )

    success(f"Wrote {table.num_rows:,} features to {output_file}")
