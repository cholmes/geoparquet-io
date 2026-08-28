"""
DuckDB connection management utilities.

This module provides functions for creating and managing DuckDB connections
with appropriate extensions loaded for GeoParquet operations.
"""

import re
import threading
from contextlib import contextmanager
from contextvars import ContextVar

import duckdb

from geoparquet_io.core.logging_config import warn

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

    The input must be a **bare** identifier, exactly as it appears in the file's
    schema (e.g. from ``find_primary_geometry_column`` or ``table.column_names``).
    The function is deliberately not idempotent: quoting an already-quoted name
    yields an identifier that literally contains double quotes, so never pass it
    something already quoted.

    For a SQL *string literal* (e.g. ``WHERE path_in_schema = '...'``) use
    :func:`_escape_sql_string` instead; the two escapes are not interchangeable.

    Args:
        name: The identifier (column name, table name, etc.) to quote

    Returns:
        A safely quoted identifier string

    Raises:
        ValueError: If the name is empty or contains a NUL byte. Both are legal
            Parquet field names but have no quoted SQL spelling: ``""`` is a
            zero-length delimited identifier and a NUL truncates the identifier
            inside DuckDB's parser, so emitting either produces unparsable SQL
            rather than an actionable error.
    """
    if not name:
        raise ValueError(
            "cannot quote an empty SQL identifier: DuckDB rejects the "
            'zero-length delimited identifier ""'
        )
    if "\x00" in name:
        raise ValueError(
            "cannot quote a SQL identifier containing a NUL byte: DuckDB's "
            "parser truncates the identifier there"
        )
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


# SQL keywords that could be dangerous in a user-supplied WHERE clause.
# These could modify data or database structure.
#
# REPLACE is deliberately absent: ``REPLACE(str, from, to)`` is a standard
# scalar function in DuckDB, BigQuery Standard SQL and Carto/Postgres alike, so
# blocking it rejects ordinary filters such as ``REPLACE(zip,'-','')='19104'``.
# A function call cannot modify data; the statement gate below is what actually
# stops DML. TRUNCATE is kept by the opposite half of the same argument: none of
# those three backends spells its scalar truncation ``TRUNCATE`` (they all use
# ``trunc``/``TRUNC``), so the word only ever shows up as the DDL statement, and
# keeping it costs no legitimate clause.
DANGEROUS_SQL_KEYWORDS = [
    "DROP",
    "DELETE",
    "INSERT",
    "UPDATE",
    "CREATE",
    "ALTER",
    "TRUNCATE",
    "EXEC",
    "EXECUTE",
    "MERGE",
    "GRANT",
    "REVOKE",
]


def where_condition_fragment(where_clause: str) -> str:
    """Return ``(<clause>\\n)`` -- the clause as one AND-able boolean condition.

    A newline is placed before the closing paren so a trailing ``--`` comment in
    the clause cannot swallow the paren -- or whatever the caller appends next
    (another AND-ed condition, ``LIMIT``, ``USING SAMPLE``, a JOIN, ...).
    Callers are responsible for validating the clause with
    :func:`validate_where_clause` first.
    """
    return f"({where_clause}\n)"


def where_sql_fragment(where_clause: str | None) -> str:
    """Return a `` WHERE (...)`` fragment for ``where_clause`` (empty if None)."""
    if not where_clause:
        return ""
    return f" WHERE {where_condition_fragment(where_clause)}"


def _dangerous_keywords_in(where_clause: str) -> list[str]:
    """Return the blocklisted keywords that appear as real SQL words.

    Keywords inside string literals (``name = 'Grant County'``), inside quoted
    identifiers, or inside comments are *data*, not statements, and must not be
    flagged. DuckDB's own lexer is used to find the word tokens, which handles
    every literal form the SQL dialect has (``'...'``, ``E'...'``, ``$$...$$``)
    plus ``--`` and ``/* */`` comments. If the lexer is unavailable, fall back to
    a conservative whole-word regex over the raw clause.
    """
    try:
        tokens = duckdb.tokenize(where_clause)
        word_types = {duckdb.token_type.keyword, duckdb.token_type.identifier}
        words = set()
        for position, token_type in tokens:
            if token_type not in word_types:
                continue
            match = re.match(r"\w+", where_clause[position:])
            if match:
                words.add(match.group(0).upper())
        return [kw for kw in DANGEROUS_SQL_KEYWORDS if kw in words]
    except Exception:  # pragma: no cover - lexer is part of the duckdb API
        upper_clause = where_clause.upper()
        return [kw for kw in DANGEROUS_SQL_KEYWORDS if re.search(rf"\b{kw}\b", upper_clause)]


def validate_where_clause(where_clause: str) -> None:
    """
    Validate that a user-supplied WHERE clause is a single filter expression.

    Two checks run:

    1. A blocklist of keywords that could modify data or database structure,
       matched against real SQL word tokens only (see
       :func:`_dangerous_keywords_in`).
    2. A parser-based statement gate. The clause is composed into the same
       ``WHERE (<clause>\\n)`` shape the callers emit and handed to DuckDB's
       parser; anything that parses as more than one statement is rejected.
       DuckDB's ``execute()`` runs multi-statement strings, so a clause that
       smuggles in a second statement could ``COPY`` data out or ``ATTACH`` a
       database (gpio #612). Counting parsed statements -- rather than scanning
       the text for ``;`` -- is what makes the gate hold: dollar quoting
       (``$$'$$``), block comments, line comments and ``E'\\''`` escapes all hide
       a ``;`` from a hand-rolled quote-state walker but not from the parser.

    Note: this is a safety net for *trusted* input, not a security boundary. It
    stops a clause from becoming extra statements; it cannot stop abuse that
    stays inside one expression (e.g. reading a local file through a scalar
    function). Untrusted input needs parameterized queries or a real allowlist.

    Args:
        where_clause: The WHERE clause string to validate

    Raises:
        ValidationError: If dangerous SQL keywords are found, if the clause does
            not parse, or if it composes into more than one statement.
    """
    from geoparquet_io.core.exceptions import ValidationError

    found_keywords = _dangerous_keywords_in(where_clause)
    if found_keywords:
        raise ValidationError(
            f"WHERE clause contains potentially dangerous SQL keywords: {', '.join(found_keywords)}. "
            "Only SELECT-style filtering expressions are allowed in --where. "
            "Run data-modifying statements against the source system directly."
        )

    probe_query = f"SELECT 1 WHERE {where_condition_fragment(where_clause)}"
    try:
        statements = duckdb.extract_statements(probe_query)
    except Exception as e:
        raise ValidationError(
            f"WHERE clause is not a single filtering expression: it could not be parsed "
            f"as SQL. {e}. Note that --where is parsed with DuckDB's SQL dialect even "
            "when the query runs on another backend."
        ) from e

    if len(statements) > 1:
        raise ValidationError(
            f"WHERE clause is not a single filtering expression: it resolves to "
            f"{len(statements)} SQL statements, separated by a ';' (possibly hidden in a "
            "comment or a quoted string). Run additional statements against the source "
            "system directly."
        )


def build_spatial_join_condition(
    input_geom_col: str,
    target_geom_col: str,
    input_bbox_col: str | None = None,
    target_bbox_col: str | None = None,
    input_alias: str = "a",
    target_alias: str = "b",
    input_geom_sql: str | None = None,
) -> str:
    """Build the ON-clause condition for a spatial join between two tables.

    The precise predicate is always ``ST_Intersects(target_geom, input_geom)``.
    When *both* sides expose a bbox covering column, a cheap bounding-box
    overlap test is ANDed in front of it: DuckDB evaluates the four numeric
    comparisons first and only runs the expensive geometry intersection on the
    surviving candidate pairs. Because bbox overlap is a necessary condition for
    geometry intersection, the result is identical to ST_Intersects alone -- the
    pre-filter changes the query plan and its memory use, never the output.

    Why the pre-filter is kept -- it is a *memory-safety* mechanism, not merely a
    speed-up:

    - A *bare* ``ST_Intersects(...)`` ON clause is the only shape DuckDB's
      ``SPATIAL_JOIN`` operator matches. ``SPATIAL_JOIN`` builds an in-memory,
      non-spilling R-tree; on large real-polygon inputs (e.g. remote Overture
      admin boundaries at >=1M features) that index exhausts memory and OOMs --
      the same "effectively hangs" failure first seen when the pre-filter was
      dropped in #457 (see PR #460).
    - ANDing the bbox-overlap test in front drops the plan to a streaming
      ``BLOCKWISE_NL_JOIN`` driving the LEFT JOIN, which runs in bounded memory
      and completes on those inputs.

    Small, local benchmarks (#538) showed the bare ``SPATIAL_JOIN`` finishing
    faster and suggested it was the fast path to prefer. A >=1M-feature
    validation (#545) showed that does not hold at scale, where the bare
    predicate OOMs. The pre-filter is therefore retained on the explicit-bbox
    (1.x) path for bounded memory; do NOT restore a bare predicate there on the
    strength of small-scale timings. The earlier "~27-73x faster / SPATIAL_JOIN
    is the fast path" rationale is withdrawn.

    Two paths still emit a bare predicate because no usable bbox pair exists: a
    reprojected non-CRS84 input whose stored bbox is in the source CRS (#525),
    and native-geometry inputs (they carry no 1.x bbox covering). Those paths
    share the same ``SPATIAL_JOIN`` OOM risk at scale; whether they need their
    own memory-bounded strategy -- and whether deriving a bbox from the geometry
    (once #462's direction) helps or hurts -- is tracked in #550, not decided
    here.

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
    qt_geom = quote_identifier(target_geom_col)

    # ``input_geom_sql`` is a pre-built input-geometry expression (e.g. wrapped in
    # ST_Transform to reproject a non-CRS84 input, #525). When supplied, the
    # stored input bbox is in the *source* CRS and so cannot be used for the
    # overlap pre-filter — fall back to the precise predicate alone.
    if input_geom_sql is not None:
        intersects = f"ST_Intersects({target_alias}.{qt_geom}, {input_geom_sql})"
        return intersects

    qi_geom = quote_identifier(input_geom_col)
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


# Strategy labels returned by spatial_join_strategy(), kept in sync with the
# predicate decision in build_spatial_join_condition().
SPATIAL_JOIN_NATIVE = "native"
SPATIAL_JOIN_BBOX_PREFILTER = "bbox_prefilter"
SPATIAL_JOIN_NO_BBOX = "no_bbox"


def spatial_join_strategy(
    has_native_geometry: bool,
    input_bbox_col: str | None,
    target_bbox_col: str | None,
    input_geom_rewritten: bool = False,
) -> str:
    """Classify which spatial-join strategy :func:`build_spatial_join_condition` uses.

    Lets callers print a status message that matches the predicate that actually
    runs (issue #538), instead of misreporting the native-geometry fast path as a
    degraded "no bbox" fallback.

    Args:
        has_native_geometry: True when the input exposes a native geometry type
            (GeoParquet 2.x / geo-typed Parquet) rather than a 1.x bbox covering.
        input_bbox_col: Input-side bbox covering column, or ``None``.
        target_bbox_col: Target-side bbox covering column, or ``None``.
        input_geom_rewritten: True when the input geometry is rewritten in the ON
            clause (e.g. an ``ST_Transform`` reprojecting a non-CRS84 input, issue
            #525). :func:`build_spatial_join_condition` then drops the bbox
            pre-filter because the stored (source-CRS) input bbox is incomparable
            to the target bbox, so the emitted predicate is a bare ``ST_Intersects``
            even though ``input_bbox_col`` is populated. The classifier must mirror
            that or it misreports a reprojected 1.x-with-bbox input as a
            bbox-prefilter join (issue #538).

    Returns:
        - ``SPATIAL_JOIN_NATIVE``: native-geometry input. The ON clause is a bare
          ``ST_Intersects``, which DuckDB's ``SPATIAL_JOIN`` operator recognizes
          and accelerates. This is the fast path, not a fallback.
        - ``SPATIAL_JOIN_BBOX_PREFILTER``: 1.x input with a bbox covering on both
          sides; the cheap bbox-overlap test is ANDed in front of ``ST_Intersects``.
        - ``SPATIAL_JOIN_NO_BBOX``: no bbox pre-filter is applied — either a 1.x
          input without a bbox column, or a reprojected input whose stored bbox is
          incomparable. The precise check runs against every candidate.
    """
    if has_native_geometry:
        return SPATIAL_JOIN_NATIVE
    if input_geom_rewritten:
        return SPATIAL_JOIN_NO_BBOX
    if input_bbox_col and target_bbox_col:
        return SPATIAL_JOIN_BBOX_PREFILTER
    return SPATIAL_JOIN_NO_BBOX


def _install_and_load_extension(con, name: str) -> None:
    """INSTALL (best-effort) then LOAD a DuckDB extension.

    An INSTALL failure alone is tolerated silently: parallel installs race
    with each other (https://github.com/duckdb/duckdb/issues/12589), and a
    cached copy of the extension may load fine even when the extension
    directory is not writable. But if the LOAD then also fails, the install
    error is what explains it (issue #574), so surface it as a warning
    before propagating the LOAD error instead of hiding it behind an opaque
    'Extension "..." not found'.
    """
    install_error = None
    try:
        con.execute(f"INSTALL {name};")
    except Exception as e:
        install_error = e
    try:
        con.execute(f"LOAD {name};")
    except Exception:
        if install_error is not None:
            warn(f"could not install the DuckDB '{name}' extension: {install_error}")
        raise


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
        _install_and_load_extension(con, "spatial")
        # DuckDB 1.5+: ensure lon/lat = x/y axis order globally.
        # Replaces per-call always_xy := true in ST_Transform.
        con.execute("SET geometry_always_xy = true;")

    # Load httpfs for cloud storage support
    if load_httpfs:
        _install_and_load_extension(con, "httpfs")

        # Only configure AWS credentials if explicitly requested (for private buckets)
        # Public buckets work without any secret - DuckDB handles them automatically
        if use_s3_auth:
            _install_and_load_extension(con, "aws")
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

    # Cast to BLOB to produce plain binary without geoarrow extension type
    # Use SELECT * REPLACE to preserve column order
    return f"""
        WITH __arrow_source AS ({query})
        SELECT * REPLACE (ST_AsWKB({quote_identifier(geometry_column)})::BLOB AS {quote_identifier(geometry_column)})
        FROM __arrow_source
    """
