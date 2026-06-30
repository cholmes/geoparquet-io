"""
DuckDB connection management utilities.

This module provides functions for creating and managing DuckDB connections
with appropriate extensions loaded for GeoParquet operations.
"""

import threading
from contextlib import contextmanager
from contextvars import ContextVar

import duckdb

# Per-bucket cache for S3 buckets that require authentication
# Buckets not in this set are accessed without credentials (works for public buckets)
_s3_cache_lock = threading.Lock()
_s3_buckets_needing_auth: set[str] = set()

# Ambient S3 config — set once at CLI/API boundary via s3_config_scope(),
# automatically picked up by get_duckdb_connection().
# Uses ContextVar for proper isolation across concurrent async/threaded scopes.
_active_s3_config: ContextVar[dict | None] = ContextVar("_active_s3_config", default=None)


@contextmanager
def s3_config_scope(s3_config: dict):
    """Set ambient S3 config for all get_duckdb_connection() calls within this scope.

    Thread-safe and async-safe via contextvars. Nested scopes merge configs,
    with inner scopes taking precedence. Config is automatically restored on exit.
    """
    current = _active_s3_config.get() or {}
    merged = {**current, **s3_config}
    token = _active_s3_config.set(merged)
    try:
        yield
    finally:
        _active_s3_config.reset(token)


def _extract_bucket_name(path: str) -> str:
    """Extract bucket name from S3 URL."""
    # s3://bucket-name/path -> bucket-name
    path_without_protocol = path.split("://", 1)[1]
    return path_without_protocol.split("/")[0]


def _needs_s3_auth(exception: Exception) -> bool:
    """Detect if exception indicates S3 bucket requires authentication."""
    error_str = str(exception).lower()
    # 403 without credentials means we need to authenticate
    auth_indicators = ["403", "forbidden", "access denied", "unauthorized"]
    return any(ind in error_str for ind in auth_indicators)


def _add_bucket_needing_auth(bucket: str) -> None:
    """Thread-safe add to S3 auth cache."""
    with _s3_cache_lock:
        _s3_buckets_needing_auth.add(bucket)


def _bucket_needs_auth(bucket: str) -> bool:
    """Thread-safe check if bucket requires authentication."""
    with _s3_cache_lock:
        return bucket in _s3_buckets_needing_auth


def _clear_s3_cache() -> None:
    """Clear S3 access cache (useful for testing)."""
    with _s3_cache_lock:
        _s3_buckets_needing_auth.clear()


def _escape_sql_string(value: str) -> str:
    """
    Escape single quotes for SQL string literals.

    SQL standard escaping: single quotes are doubled ('→'').
    This prevents SQL injection when interpolating user-provided
    strings (like file paths) into SQL queries.

    Args:
        value: The string value to escape

    Returns:
        String with single quotes escaped for safe SQL interpolation
    """
    return value.replace("'", "''")


def quote_identifier(name: str) -> str:
    """
    Quote a SQL identifier for safe use in DuckDB queries.

    Escapes embedded double quotes by doubling them, then wraps in double quotes.
    This handles column/table names with spaces, special characters, reserved words,
    or uppercase letters that need to be preserved.

    Args:
        name: The identifier (column name, table name, etc.) to quote

    Returns:
        A safely quoted identifier string
    """
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def build_spatial_join_condition(
    input_geom_col: str,
    target_geom_col: str,
    input_bbox_col: str | None = None,
    target_bbox_col: str | None = None,
    input_alias: str = "a",
    target_alias: str = "b",
) -> str:
    """Build the ON-clause condition for a spatial join between two tables.

    The precise predicate is always ``ST_Intersects(target_geom, input_geom)``.
    When *both* sides expose a bbox covering column, a cheap bounding-box
    overlap test is ANDed in front of it: DuckDB evaluates the four numeric
    comparisons first and only runs the expensive geometry intersection on the
    surviving candidate pairs. Because bbox overlap is a necessary condition for
    geometry intersection, the result is identical to ST_Intersects alone -- it
    is purely a performance pre-filter.

    Dropping this pre-filter (as #457 did) makes the Overture remote datasets
    run a full ST_Intersects against every admin geometry, which effectively
    hangs. See PR #460.

    All identifiers are passed through :func:`quote_identifier`, so column names
    sourced from untrusted file metadata cannot break out of the predicate.

    Args:
        input_geom_col: Geometry column on the input (left) table.
        target_geom_col: Geometry column on the target (right) table.
        input_bbox_col: Optional bbox covering column on the input table.
        target_bbox_col: Optional bbox covering column on the target table.
        input_alias: SQL alias for the input table (default "a").
        target_alias: SQL alias for the target table (default "b").

    Returns:
        A SQL boolean expression for use directly after ``ON``.
    """
    qi_geom = quote_identifier(input_geom_col)
    qt_geom = quote_identifier(target_geom_col)
    intersects = f"ST_Intersects({target_alias}.{qt_geom}, {input_alias}.{qi_geom})"

    # A one-sided bbox cannot form an overlap test, so fall back to the
    # precise predicate alone.
    if not (input_bbox_col and target_bbox_col):
        return intersects

    qi_bbox = quote_identifier(input_bbox_col)
    qt_bbox = quote_identifier(target_bbox_col)
    return (
        "(\n"
        "        -- Fast bbox-overlap pre-filter (cheap; eliminates most candidate pairs)\n"
        f"        {input_alias}.{qi_bbox}.xmin <= {target_alias}.{qt_bbox}.xmax AND\n"
        f"        {input_alias}.{qi_bbox}.xmax >= {target_alias}.{qt_bbox}.xmin AND\n"
        f"        {input_alias}.{qi_bbox}.ymin <= {target_alias}.{qt_bbox}.ymax AND\n"
        f"        {input_alias}.{qi_bbox}.ymax >= {target_alias}.{qt_bbox}.ymin\n"
        "    )\n"
        "    -- Precise check only runs on the bbox matches\n"
        f"    AND {intersects}"
    )


def get_duckdb_connection(
    load_spatial=True,
    load_httpfs=None,
    use_s3_auth=False,
    threads=None,
    s3_endpoint=None,
    s3_region=None,
    s3_use_ssl=None,
    temp_directory=None,
    memory_limit=None,
):
    """
    Create a DuckDB connection with necessary extensions loaded.

    By default, S3 access uses no credentials, which works for public buckets.
    DuckDB automatically handles region detection for public S3 buckets.

    When use_s3_auth=True, loads the aws extension and configures credential
    discovery for private S3 buckets.

    Args:
        load_spatial: Whether to load spatial extension (default: True)
        load_httpfs: Whether to load httpfs extension for S3/Azure/GCS.
                    If None (default), auto-detects based on usage.
        use_s3_auth: Whether to configure AWS credential chain for S3 (default: False).
                    Only needed for private buckets.
        threads: Number of threads for DuckDB to use (default: None = all cores).
                Limiting threads is useful for parallel test execution to prevent
                CPU saturation when multiple pytest workers create connections.
        temp_directory: Directory for DuckDB to spill intermediate results to disk.
                    Bounds peak memory on large spatial joins (e.g. admin-divisions
                    against a 400k-feature input) so they don't OOM.
        memory_limit: DuckDB memory limit (e.g. "8GB"). When set, DuckDB spills to
                    temp_directory once this is exceeded rather than crashing.

    Returns:
        duckdb.DuckDBPyConnection: Configured connection with extensions loaded
    """
    config = {}
    if threads is not None:
        config["threads"] = threads
    con = duckdb.connect(config=config) if config else duckdb.connect()

    # Enable large buffer size for Arrow export to handle datasets with >2GB of
    # string/binary data (e.g., large WKB geometry columns). Without this,
    # DuckDB fails with "Arrow Appender: The maximum total string size for
    # regular string buffers is 2147483647" errors.
    con.execute("SET arrow_large_buffer_size = true;")

    # Spill-to-disk: bound peak memory on large joins/aggregations.
    if temp_directory is not None:
        safe_temp_dir = _escape_sql_string(str(temp_directory))
        con.execute(f"SET temp_directory = '{safe_temp_dir}';")
    if memory_limit is not None:
        safe_memory_limit = _escape_sql_string(str(memory_limit))
        con.execute(f"SET memory_limit = '{safe_memory_limit}';")

    # Always load spatial extension by default (core use case)
    if load_spatial:
        try:
            con.execute("INSTALL spatial;")
        except Exception:
            # Ignore race conditions during parallel extension installation
            # See: https://github.com/duckdb/duckdb/issues/12589
            pass
        con.execute("LOAD spatial;")
        # DuckDB 1.5+: ensure lon/lat = x/y axis order globally.
        # Replaces per-call always_xy := true in ST_Transform.
        con.execute("SET geometry_always_xy = true;")

    # Load httpfs for cloud storage support
    if load_httpfs:
        try:
            con.execute("INSTALL httpfs;")
        except Exception:
            # Ignore race conditions during parallel extension installation
            pass
        con.execute("LOAD httpfs;")

        # Only configure AWS credentials if explicitly requested (for private buckets)
        # Public buckets work without any secret - DuckDB handles them automatically
        if use_s3_auth:
            try:
                con.execute("INSTALL aws;")
            except Exception:
                # Ignore race conditions during parallel extension installation
                pass
            con.execute("LOAD aws;")
            con.execute("""
                CREATE OR REPLACE SECRET (
                    TYPE s3,
                    PROVIDER credential_chain,
                    VALIDATION 'none'
                );
            """)

    # Apply S3 config: explicit kwargs override ambient config
    ambient = _active_s3_config.get() or {}
    eff_endpoint = s3_endpoint if s3_endpoint is not None else ambient.get("s3_endpoint")
    eff_region = s3_region if s3_region is not None else ambient.get("s3_region")
    eff_use_ssl = s3_use_ssl if s3_use_ssl is not None else ambient.get("s3_use_ssl")

    if eff_endpoint:
        safe_endpoint = _escape_sql_string(eff_endpoint)
        con.execute(f"SET s3_endpoint='{safe_endpoint}';")
        con.execute("SET s3_url_style='path';")
        ssl_value = "true" if eff_use_ssl is not False else "false"
        con.execute(f"SET s3_use_ssl={ssl_value};")

    if eff_region:
        safe_region = _escape_sql_string(eff_region)
        con.execute(f"SET s3_region='{safe_region}';")

    return con


def load_community_extension(con, name: str) -> None:
    """INSTALL and LOAD a DuckDB community extension with a clear error message.

    Community extensions (e.g. ``geography`` for S2 support) are built per
    DuckDB release. When a newer DuckDB version ships before the extension has
    been rebuilt for it, ``INSTALL ... FROM community`` fails with an opaque
    HTTP 404. Translate that into an actionable :class:`ExtensionUnavailableError`
    so users understand the extension simply isn't published for their DuckDB
    version yet.

    Args:
        con: An open DuckDB connection.
        name: Community extension name (e.g. "geography", "h3").

    Raises:
        ExtensionUnavailableError: If the extension cannot be installed or loaded.
    """
    from geoparquet_io.core.exceptions import ExtensionUnavailableError

    try:
        con.execute(f"INSTALL {name} FROM community;")
        con.execute(f"LOAD {name};")
    except duckdb.Error as e:
        raise ExtensionUnavailableError(name, duckdb.__version__, str(e)) from e


def get_duckdb_connection_for_s3(
    path: str,
    load_spatial: bool = True,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool | None = None,
) -> "duckdb.DuckDBPyConnection":
    """
    Get DuckDB connection configured for S3 access.

    For S3 paths, uses no credentials by default (works for public buckets).
    If a bucket is known to require auth (from previous attempts), uses
    credential chain. Results are cached per bucket.

    Args:
        path: S3 path to access (used to determine bucket and access mode)
        load_spatial: Whether to load spatial extension (default: True)
        s3_endpoint: Custom S3 endpoint (e.g., 'minio.local:9000')
        s3_region: S3 region for custom endpoints
        s3_use_ssl: Whether to use SSL for the S3 endpoint

    Returns:
        duckdb.DuckDBPyConnection: Configured connection with appropriate S3 access
    """
    from geoparquet_io.core.remote import needs_httpfs

    s3_kwargs = {"s3_endpoint": s3_endpoint, "s3_region": s3_region, "s3_use_ssl": s3_use_ssl}

    # Non-S3 paths: use standard connection
    if not path.startswith(("s3://", "s3a://")):
        return get_duckdb_connection(
            load_spatial=load_spatial, load_httpfs=needs_httpfs(path), **s3_kwargs
        )

    bucket = _extract_bucket_name(path)

    # If we know this bucket needs auth, use credential chain
    if _bucket_needs_auth(bucket):
        return get_duckdb_connection(
            load_spatial=load_spatial, load_httpfs=True, use_s3_auth=True, **s3_kwargs
        )

    # Try without credentials first (works for public buckets)
    con = get_duckdb_connection(
        load_spatial=load_spatial, load_httpfs=True, use_s3_auth=False, **s3_kwargs
    )
    try:
        # Lightweight test query - DuckDB handles glob patterns natively
        # Escape single quotes in path to prevent SQL injection
        safe_path = _escape_sql_string(path)
        con.execute(f"SELECT 1 FROM read_parquet('{safe_path}') LIMIT 1").fetchone()
        return con
    except Exception as e:
        con.close()
        if _needs_s3_auth(e):
            # This bucket requires authentication - cache and retry
            _add_bucket_needing_auth(bucket)
            return get_duckdb_connection(
                load_spatial=load_spatial, load_httpfs=True, use_s3_auth=True, **s3_kwargs
            )
        raise


class _DuckDBSchemaWrapper:
    """Wrapper to provide PyArrow-like interface for DuckDB schema info."""

    def __init__(self, schema_info):
        self._columns = [c for c in schema_info if c.get("name") and "." not in c.get("name", "")]

    def __len__(self):
        return len(self._columns)

    def field(self, i):
        return _DuckDBFieldWrapper(self._columns[i])


class _DuckDBFieldWrapper:
    """Wrapper to provide PyArrow-like interface for a DuckDB column."""

    def __init__(self, col_info):
        self.name = col_info.get("name", "")


def _get_query_columns(con, query: str) -> list[str]:
    """
    Get column names from a SQL query without executing it fully.

    Args:
        con: DuckDB connection
        query: SQL query to analyze

    Returns:
        List of column names in the query result
    """
    result = con.execute(f"DESCRIBE ({query})").fetchall()
    return [row[0] for row in result]


def _get_query_column_type(con, query: str, column_name: str) -> str | None:
    """Return the DuckDB type string for a named column in a query, or None."""
    try:
        rows = con.execute(f"DESCRIBE ({query})").fetchall()
        for row in rows:
            if row[0] == column_name:
                return row[1]
    except Exception:
        pass
    return None


# Maps GeoArrow encoding name → number of flatten() calls needed to reach list[struct].
# bracket_depth in DuckDB type string = flatten_depth + 1 for non-point types.
_GEOARROW_FLATTEN_DEPTH = {
    "point": -1,  # plain STRUCT(x, y), no list wrapper
    "linestring": 0,
    "multipoint": 0,
    "polygon": 1,
    "multilinestring": 1,
    "multipolygon": 2,
}


def _geoarrow_coord_exprs(quoted_geom: str, encoding: str) -> tuple:
    """Return (xmin, ymin, xmax, ymax, centroid_x, centroid_y) SQL expressions
    for a GeoArrow native geometry column, for per-row use in a SELECT list.

    Uses list_transform + flatten to extract coordinates, then list_min/max/avg.
    Unknown encodings fall back to depth 0 (linestring-style, no flatten).
    """
    depth = _GEOARROW_FLATTEN_DEPTH.get(encoding.lower(), 0)

    if depth == -1:
        x = f"{quoted_geom}.x"
        y = f"{quoted_geom}.y"
        return x, y, x, y, x, y

    flat = quoted_geom
    for _ in range(depth):
        flat = f"flatten({flat})"

    x_arr = f"list_transform({flat}, p -> p.x)"
    y_arr = f"list_transform({flat}, p -> p.y)"
    x_min = f"list_min({x_arr})"
    x_max = f"list_max({x_arr})"
    y_min = f"list_min({y_arr})"
    y_max = f"list_max({y_arr})"
    return (
        x_min,
        y_min,
        x_max,
        y_max,
        f"({x_min} + {x_max}) / 2.0",
        f"({y_min} + {y_max}) / 2.0",
    )


def _wrap_query_with_wkb_conversion(query: str, geometry_column: str, con=None) -> str:
    """
    Wrap a query to convert geometry column to WKB format.

    Args:
        query: Original SQL query
        geometry_column: Name of the geometry column
        con: Optional DuckDB connection for column discovery

    Returns:
        Modified query with geometry converted to WKB
    """
    quoted_geom = quote_identifier(geometry_column)

    if con:
        try:
            columns = _get_query_columns(con, query)
            other_cols = [
                quote_identifier(c) for c in columns if c.lower() != geometry_column.lower()
            ]
            if other_cols:
                cols_str = ", ".join(other_cols)
                return f"SELECT {cols_str}, ST_AsWKB({quoted_geom}) AS {quoted_geom} FROM ({query})"
        except Exception:
            pass

    # Fallback: select all and convert geometry
    return (
        f"SELECT * EXCLUDE({quoted_geom}), ST_AsWKB({quoted_geom}) AS {quoted_geom} FROM ({query})"
    )


def _wrap_query_with_blob_conversion(query: str, geometry_column: str, con=None) -> str:
    """
    Wrap query to convert geometry column to plain binary BLOB.

    Unlike _wrap_query_with_wkb_conversion which produces WKB that DuckDB still
    recognizes as spatial, this casts to BLOB to produce truly plain binary data.
    Used for GeoParquet v1.x where we need plain binary WKB without geoarrow
    extension types in the Parquet schema.

    Args:
        query: Original SQL SELECT query
        geometry_column: Name of the geometry column to convert
        con: Optional DuckDB connection to verify column exists

    Returns:
        str: Wrapped query with BLOB conversion, or original query if column doesn't exist
    """
    # If connection provided, check if geometry column exists in query output
    if con is not None:
        try:
            rows = con.execute(f"DESCRIBE ({query})").fetchall()
            col_info = {row[0]: row[1] for row in rows}
            if geometry_column not in col_info:
                return query
            if "STRUCT" in col_info.get(geometry_column, ""):
                return query
        except Exception:
            pass

    # Quote column name to handle special characters
    quoted_geom = geometry_column.replace('"', '""')

    # Cast to BLOB to produce plain binary without geoarrow extension type
    # Use SELECT * REPLACE to preserve column order
    return f"""
        WITH __arrow_source AS ({query})
        SELECT * REPLACE (ST_AsWKB("{quoted_geom}")::BLOB AS "{quoted_geom}")
        FROM __arrow_source
    """
