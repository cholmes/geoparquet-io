"""Carto SQL API to GeoParquet conversion.

Extracts data from Carto SQL API endpoints using DuckDB's ST_Read
for efficient GeoJSON parsing.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib.parse import quote, urlparse

import pyarrow as pa

from geoparquet_io.core.common import (
    InvalidParameterError,
    get_duckdb_connection,
    write_geoparquet_table,
)
from geoparquet_io.core.crs_utils import parse_crs_string_to_projjson
from geoparquet_io.core.duckdb_utils import quote_identifier
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)

# Default timeout and retry settings
DEFAULT_TIMEOUT = 120  # seconds
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 2.0  # seconds

# Environment variable for API key
CARTO_API_KEY_ENV = "CARTO_API_KEY"


class CartoError(Exception):
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


def _validate_table_name(table_name: str) -> str:
    """Validate table name to prevent SQL injection.

    Args:
        table_name: Table name to validate

    Returns:
        Validated table name

    Raises:
        InvalidParameterError: If table name contains dangerous characters
    """
    # Basic validation - table names should be alphanumeric with underscores
    # Allow schema-qualified names (schema.table)
    import re

    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$", table_name):
        raise InvalidParameterError(
            "table_name",
            f"Invalid table name: {table_name}. "
            "Table names must be alphanumeric with underscores, optionally schema-qualified.",
        )
    return table_name


def _build_carto_query(
    table_name: str,
    columns: list[str] | None = None,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
) -> str:
    """Build SQL query for Carto API.

    Args:
        table_name: Name of the table to query (will be quoted for safety)
        columns: Columns to select (None = all)
        where: SQL WHERE clause (user-provided, passed through)
        bbox: Bounding box filter (minx, miny, maxx, maxy)
        limit: Maximum rows to return

    Returns:
        SQL query string

    Note:
        The table_name is quoted using PostgreSQL identifier quoting.
        The where clause is user-provided and passed through - Carto's
        server-side validation handles SQL injection for WHERE clauses.
    """
    # Validate and quote table name to prevent SQL injection
    _validate_table_name(table_name)
    quoted_table = quote_identifier(table_name)

    # Column selection - quote each column name
    if columns:
        # Always include the_geom for geometry
        if "the_geom" not in columns:
            columns = [*columns, "the_geom"]
        col_str = ", ".join(quote_identifier(c) for c in columns)
    else:
        col_str = "*"

    sql = f"SELECT {col_str} FROM {quoted_table}"

    # Build WHERE clause
    conditions = []
    if where:
        # WHERE clause is user-provided - Carto validates on server side
        conditions.append(f"({where})")
    if bbox:
        minx, miny, maxx, maxy = bbox
        # Use ST_Intersects with ST_MakeEnvelope for spatial filter
        # the_geom is quoted for safety
        conditions.append(
            f"ST_Intersects({quote_identifier('the_geom')}, "
            f"ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 4326))"
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
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> int:
    """Get row count from Carto table with optional filters.

    Args:
        url: Carto SQL API URL
        table_name: Table name
        where: Optional WHERE clause
        bbox: Optional bounding box filter
        api_key: Optional API key for authenticated requests
        timeout: Request timeout in seconds

    Returns:
        Number of rows matching the filter
    """
    # Validate and quote table name
    _validate_table_name(table_name)
    quoted_table = quote_identifier(table_name)

    # Build count query with same filters
    sql = f"SELECT COUNT(*) as count FROM {quoted_table}"

    conditions = []
    if where:
        conditions.append(f"({where})")
    if bbox:
        minx, miny, maxx, maxy = bbox
        conditions.append(
            f"ST_Intersects({quote_identifier('the_geom')}, "
            f"ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 4326))"
        )

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    full_url = f"{url}?q={quote(sql)}"
    if api_key:
        full_url += f"&api_key={quote(api_key)}"

    conn = get_duckdb_connection()
    conn.execute(f"SET http_timeout = {int(timeout * 1000)}")  # DuckDB uses milliseconds
    result = conn.execute(f"SELECT rows[1].count FROM read_json_auto('{full_url}')").fetchone()

    return int(result[0]) if result else 0


def _create_empty_geoparquet_table(geoparquet_version: str | None = None) -> pa.Table:
    """Create an empty table with proper GeoParquet metadata.

    Args:
        geoparquet_version: GeoParquet version string (default: "1.1.0")

    Returns:
        Empty PyArrow table with valid GeoParquet metadata
    """
    version = geoparquet_version or "1.1.0"
    crs = parse_crs_string_to_projjson("OGC:CRS84")

    geo_metadata = {
        "version": version,
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "crs": crs,
                "geometry_types": [],
            }
        },
    }

    table = pa.table({"geometry": pa.array([], type=pa.binary())})
    new_metadata = {b"geo": json.dumps(geo_metadata).encode("utf-8")}
    return table.replace_schema_metadata(new_metadata)


def _fetch_with_retry(
    url: str,
    table_name: str,
    sql: str,
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
) -> pa.Table:
    """Fetch data from Carto with retry logic for transient failures.

    Args:
        url: Carto SQL API URL
        table_name: Table name (for error messages)
        sql: SQL query to execute
        api_key: Optional API key
        timeout: Request timeout in seconds
        max_retries: Number of retry attempts
        retry_delay: Base delay between retries (exponential backoff)

    Returns:
        PyArrow Table from DuckDB ST_Read

    Raises:
        CartoError: On fatal errors or exhausted retries
    """
    # Construct full URL with GeoJSON format
    full_url = f"{url}?q={quote(sql)}&format=GeoJSON"
    if api_key:
        full_url += f"&api_key={quote(api_key)}"

    debug(f"Request URL: {full_url[:100]}...")

    last_exception: Exception | None = None

    for attempt in range(max_retries):
        try:
            conn = get_duckdb_connection()
            conn.execute("SET allow_asterisks_in_http_paths = true")
            conn.execute(f"SET http_timeout = {int(timeout * 1000)}")  # milliseconds

            table = conn.execute(f'SELECT * FROM ST_Read("{full_url}")').arrow().read_all()
            return table

        except Exception as e:
            last_exception = e
            error_msg = str(e).lower()

            # Non-retryable errors - fail immediately
            if "404" in error_msg or "not found" in error_msg:
                raise CartoError(
                    f"Table '{table_name}' not found. Check the table name and ensure "
                    f"it is publicly accessible."
                ) from e

            if "401" in error_msg or "unauthorized" in error_msg:
                raise CartoError(
                    f"Unauthorized access to table '{table_name}'. "
                    "Set CARTO_API_KEY environment variable or check permissions."
                ) from e

            if "403" in error_msg or "forbidden" in error_msg:
                raise CartoError(
                    f"Access forbidden to table '{table_name}'. Check permissions."
                ) from e

            # Retryable errors
            if attempt < max_retries - 1:
                delay = retry_delay * (2**attempt)  # Exponential backoff
                if "timeout" in error_msg:
                    warn(
                        f"Request timed out (attempt {attempt + 1}/{max_retries}), retrying in {delay:.1f}s..."
                    )
                elif "connection" in error_msg or "network" in error_msg:
                    warn(
                        f"Connection error (attempt {attempt + 1}/{max_retries}), retrying in {delay:.1f}s..."
                    )
                else:
                    warn(
                        f"Request failed (attempt {attempt + 1}/{max_retries}): {e}, retrying in {delay:.1f}s..."
                    )
                time.sleep(delay)
            else:
                # Final attempt failed
                if "timeout" in error_msg:
                    raise CartoError(
                        f"Request timed out after {max_retries} attempts. "
                        "The table may be too large. Try using --limit or --where to reduce the result set, "
                        "or increase --timeout."
                    ) from e

    # All retries exhausted
    raise CartoError(
        f"Failed to fetch data from Carto after {max_retries} attempts: {last_exception}"
    ) from last_exception


def carto_to_table(
    url: str,
    table_name: str,
    *,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    geoparquet_version: str | None = None,
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
        api_key: API key for authenticated requests (or set CARTO_API_KEY env var)
        timeout: Request timeout in seconds (default: 120)
        max_retries: Number of retry attempts for transient failures (default: 3)
        geoparquet_version: GeoParquet version for metadata (default: "1.1.0")
        verbose: Enable verbose output

    Returns:
        PyArrow Table with WKB geometry column named 'geometry'

    Raises:
        CartoError: If the Carto API request fails
        InvalidParameterError: If URL or table name is invalid
    """
    configure_verbose(verbose)

    # Validate URL
    url = _validate_carto_url(url)
    debug(f"Carto URL: {url}")

    # Get API key from parameter or environment
    effective_api_key = api_key or os.environ.get(CARTO_API_KEY_ENV)
    if effective_api_key:
        debug("Using API key for authentication")

    # Parse column lists
    include_list = [c.strip() for c in include_cols.split(",")] if include_cols else None
    exclude_set = {c.strip() for c in exclude_cols.split(",")} if exclude_cols else set()

    # Get row count for progress
    try:
        total_count = _get_row_count(url, table_name, where, bbox, effective_api_key, timeout)
        info(f"Table: {table_name}")
        info(f"Total rows matching filter: {total_count:,}")
    except Exception as e:
        debug(f"Could not get row count: {e}")
        total_count = None

    if total_count == 0:
        warn("No rows match the specified filters")
        return _create_empty_geoparquet_table(geoparquet_version)

    # Build query
    sql = _build_carto_query(
        table_name=table_name,
        columns=include_list,
        where=where,
        bbox=bbox,
        limit=limit,
    )
    debug(f"SQL: {sql}")

    # Fetch data with retry logic
    progress("Fetching data from Carto...")
    table = _fetch_with_retry(
        url=url,
        table_name=table_name,
        sql=sql,
        api_key=effective_api_key,
        timeout=timeout,
        max_retries=max_retries,
    )

    if table.num_rows == 0:
        warn("Query returned no rows")
        return _create_empty_geoparquet_table(geoparquet_version)

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
    version = geoparquet_version or "1.1.0"
    crs = parse_crs_string_to_projjson("OGC:CRS84")
    if crs:
        geo_metadata = {
            "version": version,
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
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
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
        api_key: API key for authenticated requests (or set CARTO_API_KEY env var)
        timeout: Request timeout in seconds (default: 120)
        max_retries: Number of retry attempts (default: 3)
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
        api_key=api_key,
        timeout=timeout,
        max_retries=max_retries,
        geoparquet_version=geoparquet_version,
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
