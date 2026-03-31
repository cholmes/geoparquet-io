#!/usr/bin/env python3
"""
BigQuery extraction to GeoParquet.

Uses DuckDB BigQuery extension to read from BigQuery tables,
converting GEOGRAPHY columns to GeoParquet geometry with spherical edges.
"""

from __future__ import annotations

import os

import duckdb
import pyarrow as pa

from geoparquet_io.core.common import (
    get_duckdb_connection,
    handle_output_overwrite,
    write_geoparquet_table,
)
from geoparquet_io.core.extract import parse_bbox
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    progress,
    success,
    warn,
)

# Regex patterns for GCP resource validation
# Project IDs: 6-30 chars, lowercase letters, digits, hyphens, must start with letter
_PROJECT_ID_PATTERN = r"^[a-z][a-z0-9\-]{5,29}$"
# Table ID parts: alphanumeric with underscores and hyphens
_TABLE_PART_PATTERN = r"^[a-zA-Z0-9_\-]+$"


def _validate_project_id(project: str) -> str:
    """Validate GCP project ID to prevent SQL injection.

    Args:
        project: Project ID to validate

    Returns:
        The validated project ID

    Raises:
        ValueError: If project ID doesn't match GCP naming rules
    """
    import re

    if not re.match(_PROJECT_ID_PATTERN, project):
        raise ValueError(
            f"Invalid GCP project ID: '{project}'. "
            "Project IDs must be 6-30 characters, start with a lowercase letter, "
            "and contain only lowercase letters, digits, and hyphens."
        )
    return project


def _validate_table_part(part: str, part_name: str) -> str:
    """Validate a single part of a BigQuery table ID.

    Args:
        part: Part to validate (project, dataset, or table name)
        part_name: Name of the part for error messages

    Returns:
        The validated part

    Raises:
        ValueError: If part contains invalid characters
    """
    import re

    if not re.match(_TABLE_PART_PATTERN, part):
        raise ValueError(
            f"Invalid BigQuery {part_name}: '{part}'. "
            "Must contain only alphanumeric characters, underscores, and hyphens."
        )
    return part


def _normalize_table_id(table_id: str, project: str | None = None) -> str:
    """Normalize and validate BigQuery table ID.

    Supports both 2-part (dataset.table) and 3-part (project.dataset.table) formats.
    When project is provided, it overrides any project in the table_id.

    Args:
        table_id: BigQuery table ID (dataset.table or project.dataset.table)
        project: Optional project ID to use (overrides table_id project)

    Returns:
        Fully qualified table ID (project.dataset.table)

    Raises:
        ValueError: If table_id format is invalid or project is missing when needed
    """
    parts = table_id.split(".")

    if len(parts) == 3:
        # project.dataset.table format
        table_project, dataset, table = parts
        _validate_table_part(table_project, "project")
        _validate_table_part(dataset, "dataset")
        _validate_table_part(table, "table")

        # Project override takes precedence
        if project:
            _validate_project_id(project)
            return f"{project}.{dataset}.{table}"
        return table_id

    elif len(parts) == 2:
        # dataset.table format - requires project parameter
        dataset, table = parts
        _validate_table_part(dataset, "dataset")
        _validate_table_part(table, "table")

        if not project:
            raise ValueError(
                f"Table ID '{table_id}' uses dataset.table format but no project was specified. "
                "Either use project.dataset.table format or provide --project."
            )
        _validate_project_id(project)
        return f"{project}.{dataset}.{table}"

    else:
        raise ValueError(
            f"Invalid BigQuery table ID: '{table_id}'. "
            "Expected format: dataset.table or project.dataset.table"
        )


class BigQueryConnection:
    """Context manager for DuckDB connection with BigQuery extension.

    Handles proper cleanup of environment variables and connection resources.
    Safely restores state even if setup fails partway through.
    """

    def __init__(
        self,
        project: str | None = None,
        credentials_file: str | None = None,
        **kwargs,
    ):
        if "geography_as_geometry" in kwargs:
            import warnings

            warnings.warn(
                "geography_as_geometry is deprecated and ignored in DuckDB 1.5+ — "
                "GEOGRAPHY maps to GEOMETRY automatically",
                DeprecationWarning,
                stacklevel=2,
            )

        self.project = project
        self.credentials_file = credentials_file
        self._original_creds: str | None = None
        self._creds_was_set: bool = False
        self._creds_modified: bool = False
        self._con: duckdb.DuckDBPyConnection | None = None

    def _restore_credentials(self) -> None:
        """Restore original GOOGLE_APPLICATION_CREDENTIALS state."""
        if not self._creds_modified:
            return
        if self._creds_was_set:
            if self._original_creds is not None:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._original_creds
        else:
            os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        self._creds_modified = False

    def _cleanup(self) -> None:
        """Clean up connection and credentials."""
        if self._con:
            try:
                self._con.close()
            except Exception:
                pass  # Ignore close errors during cleanup
            self._con = None
        self._restore_credentials()

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        # Save original credentials state before any modifications
        self._original_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        self._creds_was_set = "GOOGLE_APPLICATION_CREDENTIALS" in os.environ

        try:
            self._con = _setup_bigquery_connection()

            # Configure authentication via environment variable if credentials file provided
            if self.credentials_file:
                expanded_path = os.path.expanduser(self.credentials_file)
                if not os.path.exists(expanded_path):
                    raise FileNotFoundError(f"Credentials file not found: {expanded_path}")
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = expanded_path
                self._creds_modified = True

            return self._con

        except Exception:
            # Clean up any partial state on failure
            self._cleanup()
            raise

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self._cleanup()
        return False  # Don't suppress exceptions


def _setup_bigquery_connection() -> duckdb.DuckDBPyConnection:
    """Create and configure a DuckDB connection with BigQuery extension.

    Sets up spatial (via get_duckdb_connection), then layers BigQuery on top
    with ZSTD Arrow compression for efficient network transfer.

    DuckDB 1.5+: GEOGRAPHY maps to GEOMETRY automatically (core type),
    so bq_geography_as_geometry is no longer needed.

    Returns:
        Configured DuckDB connection with spatial + BigQuery extensions
    """
    # Use get_duckdb_connection for consistent setup (spatial + geometry_always_xy)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

    try:
        # Layer BigQuery extension on top
        # CRITICAL: spatial must be loaded BEFORE bigquery for geography conversion
        try:
            con.execute("FORCE INSTALL bigquery FROM community;")
        except Exception:
            # Ignore race conditions during parallel extension installation
            pass
        con.execute("LOAD bigquery;")

        # Reduce BigQuery Storage API network transfer with ZSTD compression
        con.execute("SET bq_arrow_compression = 'ZSTD';")

        # Note: bq_geography_as_geometry is NOT set — deprecated in DuckDB 1.5.
        # GEOGRAPHY columns map to GEOMETRY automatically (core type).

        return con
    except Exception:
        con.close()
        raise


def get_bigquery_connection(
    project: str | None = None,
    credentials_file: str | None = None,
    **kwargs,
) -> duckdb.DuckDBPyConnection:
    """
    Create DuckDB connection with BigQuery extension loaded.

    NOTE: This function mutates GOOGLE_APPLICATION_CREDENTIALS environment variable.
    For proper cleanup, use BigQueryConnection context manager instead.

    Args:
        project: Default GCP project ID (optional, uses gcloud default if not set)
        credentials_file: Path to service account JSON file (optional)

    Returns:
        Configured DuckDB connection with BigQuery extension
    """
    if "geography_as_geometry" in kwargs:
        import warnings

        warnings.warn(
            "geography_as_geometry is deprecated and ignored in DuckDB 1.5+ — "
            "GEOGRAPHY maps to GEOMETRY automatically",
            DeprecationWarning,
            stacklevel=2,
        )

    con = _setup_bigquery_connection()

    try:
        # Configure authentication via environment variable if credentials file provided
        if credentials_file:
            # Expand user paths like ~/
            credentials_file = os.path.expanduser(credentials_file)
            if not os.path.exists(credentials_file):
                raise FileNotFoundError(f"Credentials file not found: {credentials_file}")
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_file

        # Note: project ID is specified in the fully-qualified table name
        # (project.dataset.table) passed to bigquery_scan(), not as a SET parameter

        return con
    except Exception:
        con.close()
        raise


def _get_table_row_count(
    con: duckdb.DuckDBPyConnection,
    table_id: str,
) -> int | None:
    """
    Get approximate row count from BigQuery table metadata.

    Uses __TABLES__ metadata which is fast and doesn't scan the table.
    Returns None if metadata lookup fails.

    Args:
        con: DuckDB connection with BigQuery extension loaded
        table_id: Fully qualified BigQuery table ID (project.dataset.table)

    Returns:
        Row count or None if lookup fails
    """
    try:
        # Parse table_id to get project.dataset.table
        parts = table_id.split(".")
        if len(parts) == 3:
            project, dataset, table = parts
        elif len(parts) == 2:
            # Use default project from connection
            dataset, table = parts
            project = None
        else:
            return None

        # Build metadata query using __TABLES__
        if project:
            metadata_table = f"`{project}.{dataset}.__TABLES__`"
            query_project = project
        else:
            metadata_table = f"`{dataset}.__TABLES__`"
            query_project = ""

        query = f"""
        SELECT * FROM bigquery_query(
            '{query_project}',
            'SELECT row_count FROM {metadata_table} WHERE table_id = "{table}"'
        )
        """
        result = con.execute(query).fetchone()
        return result[0] if result else None
    except Exception:
        return None


def _detect_geometry_column(table: pa.Table) -> str | None:
    """
    Detect geometry column from table schema.

    Args:
        table: PyArrow Table to check

    Returns:
        Name of detected geometry column, or None
    """
    # Look for known geometry column names (case insensitive)
    common_names = ["geometry", "geom", "the_geom", "shape", "geo", "geography"]
    lower_names = {name.lower(): name for name in table.column_names}

    for name in common_names:
        if name in lower_names:
            return lower_names[name]

    # Fallback: look for GEOMETRY type columns by checking for binary/blob types
    # that might contain WKB data
    for field in table.schema:
        field_name_lower = field.name.lower()
        if "geom" in field_name_lower or "geo" in field_name_lower:
            return field.name

    return None


def extract_bigquery_table(
    table: pa.Table,
    limit: int | None = None,
    columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
) -> pa.Table:
    """
    Apply column selection and row limits to an in-memory PyArrow Table.

    This function processes tables that have already been loaded from BigQuery.
    For filtering with WHERE clauses or bbox, use extract_bigquery() which
    pushes filters to BigQuery for better performance.

    Args:
        table: Input PyArrow Table (already loaded from BigQuery)
        limit: Maximum rows to return (0 returns empty table)
        columns: Columns to include (None = all)
        exclude_columns: Columns to exclude

    Returns:
        Filtered PyArrow Table

    Raises:
        ValueError: If limit is negative
    """
    # Validate limit
    if limit is not None and limit < 0:
        raise ValueError(f"limit must be non-negative, got {limit}")

    result = table

    # Apply column selection
    if columns:
        # Ensure geometry column is included
        geom_col = _detect_geometry_column(result)
        if geom_col and geom_col not in columns:
            columns = list(columns) + [geom_col]
        available = [c for c in columns if c in result.column_names]
        result = result.select(available)

    # Apply column exclusion
    if exclude_columns:
        keep_cols = [c for c in result.column_names if c not in exclude_columns]
        result = result.select(keep_cols)

    # Apply limit (limit=0 returns empty table)
    if limit is not None and result.num_rows > limit:
        result = result.slice(0, limit)

    return result


def _detect_geometry_column_from_schema(
    con: duckdb.DuckDBPyConnection,
    table_id: str,
    geography_column: str | None = None,
    table_source: str = "bigquery",
) -> str | None:
    """
    Detect native GEOMETRY-typed column from table schema.

    Only detects columns with DuckDB GEOMETRY type (e.g., BigQuery GEOGRAPHY
    columns that are automatically mapped). Does NOT detect VARCHAR columns
    containing WKT/GeoJSON by name - users must specify those explicitly
    with --geography-column.

    Args:
        con: DuckDB connection
        table_id: Table identifier
        geography_column: If provided and matches a native GEOMETRY column,
            returns it. Otherwise returns None (caller handles VARCHAR columns).
        table_source: "bigquery" for bigquery_scan, "local" for local tables

    Returns:
        Name of detected GEOMETRY column, or None
    """
    if table_source == "bigquery":
        schema_query = f"DESCRIBE SELECT * FROM bigquery_scan('{table_id}') LIMIT 0"
    else:
        schema_query = f"DESCRIBE SELECT * FROM {table_id} LIMIT 0"
    schema_result = con.execute(schema_query).fetchall()

    geometry_cols = []
    all_cols = []
    for row in schema_result:
        col_name = row[0]
        col_type = str(row[1]).upper()
        all_cols.append(col_name)
        if "GEOMETRY" in col_type:
            geometry_cols.append(col_name)

    # If explicit column provided and it's a native GEOMETRY column, use it
    if geography_column:
        lower_map = {c.lower(): c for c in geometry_cols}
        if geography_column.lower() in lower_map:
            return lower_map[geography_column.lower()]
        # Check column exists (for better error messages later)
        all_lower = {c.lower() for c in all_cols}
        if geography_column.lower() not in all_lower:
            raise ValueError(
                f"Column '{geography_column}' not found in table '{table_id}'. "
                f"Available columns: {all_cols}."
            )
        # Column exists but isn't GEOMETRY type - return None so caller
        # can handle it as VARCHAR with WKT/GeoJSON parsing
        return None

    # Return first native GEOMETRY column found
    if geometry_cols:
        return geometry_cols[0]

    return None


def _get_column_type(
    con: duckdb.DuckDBPyConnection,
    table_id: str,
    column_name: str,
) -> str:
    """
    Get the DuckDB type of a column in a BigQuery table.

    Args:
        con: DuckDB connection
        table_id: BigQuery table ID
        column_name: Column name to check

    Returns:
        Uppercase type string (e.g. "GEOMETRY", "VARCHAR")
    """
    schema_query = f"DESCRIBE SELECT * FROM bigquery_scan('{table_id}') LIMIT 0"
    schema_result = con.execute(schema_query).fetchall()
    for row in schema_result:
        if row[0].lower() == column_name.lower():
            return str(row[1]).upper()
    return "VARCHAR"


def _build_geometry_select_expr(
    column_name: str,
    column_type: str,
    geometry_format: str = "wkt",
) -> str:
    """
    Build the SELECT expression for a geometry column based on its DuckDB type.

    Handles three scenarios:
    - Native GEOMETRY type: ST_AsWKB("col")
    - VARCHAR with WKT: ST_AsWKB(ST_GeomFromText("col"))
    - VARCHAR with GeoJSON: ST_AsWKB(ST_GeomFromGeoJSON("col"))

    Args:
        column_name: Name of the geometry column
        column_type: DuckDB type of the column (e.g. "GEOMETRY", "VARCHAR")
        geometry_format: Format of geometry in VARCHAR columns ("wkt" or "geojson")

    Returns:
        SQL expression string for the SELECT clause
    """
    if "GEOMETRY" in column_type:
        return f'ST_AsWKB("{column_name}") AS "{column_name}"'
    elif geometry_format == "geojson":
        return f'ST_AsWKB(ST_GeomFromGeoJSON("{column_name}")) AS "{column_name}"'
    else:
        # Default: WKT
        return f'ST_AsWKB(ST_GeomFromText("{column_name}")) AS "{column_name}"'


def _build_select_with_wkb(
    columns: list[str] | None,
    geometry_column: str | None,
    con: duckdb.DuckDBPyConnection,
    table_id: str,
    geometry_format: str = "wkt",
) -> tuple[str, list[str]]:
    """
    Build SELECT clause with ST_AsWKB for geometry columns.

    DuckDB's GEOMETRY type uses an internal binary format when exported to Arrow,
    not standard WKB. We must use ST_AsWKB() to convert to proper WKB for GeoParquet.

    Handles native GEOMETRY columns, VARCHAR columns containing WKT, and
    VARCHAR columns containing GeoJSON.

    Args:
        columns: List of columns to select (None = all)
        geometry_column: Name of geometry column (already detected)
        con: DuckDB connection
        table_id: BigQuery table ID
        geometry_format: Format of geometry in VARCHAR columns ("wkt" or "geojson")

    Returns:
        Tuple of (SELECT clause string, list of actual column names)
    """
    # Get all column names if selecting all
    if columns is None:
        schema_query = f"DESCRIBE SELECT * FROM bigquery_scan('{table_id}') LIMIT 0"
        schema_result = con.execute(schema_query).fetchall()
        columns = [row[0] for row in schema_result]

    # Detect geometry column type if we have one
    geom_col_type = ""
    if geometry_column:
        geom_col_type = _get_column_type(con, table_id, geometry_column)

    # Build SELECT with appropriate geometry conversion
    select_parts = []
    for col in columns:
        if geometry_column and col.lower() == geometry_column.lower():
            select_parts.append(_build_geometry_select_expr(col, geom_col_type, geometry_format))
        else:
            select_parts.append(f'"{col}"')

    return ", ".join(select_parts), columns


def _handle_dry_run(
    validated_table_id: str,
    include_list: list[str] | None,
    bbox: str | None,
    bbox_mode: str,
    bbox_threshold: int,
    where: str | None,
    limit: int | None,
) -> None:
    """Handle dry_run mode by printing the SQL query without executing."""
    if include_list:
        select_cols = ", ".join(f'"{c}"' for c in include_list)
    else:
        select_cols = "*"

    query = _build_dry_run_query(validated_table_id, select_cols, bbox, bbox_mode, bbox_threshold)

    # Add DuckDB-side conditions
    if where:
        if "WHERE" in query:
            query += f" AND ({where})"
        else:
            query += f" WHERE ({where})"
    if limit is not None:
        query += f" LIMIT {limit}"

    progress(f"SQL: {query}")
    progress("(Actual query will use ST_AsWKB for geometry columns)")


def _build_dry_run_query(
    table_id: str,
    select_cols: str,
    bbox: str | None,
    bbox_mode: str,
    bbox_threshold: int,
) -> str:
    """Build a dry run query string for display."""
    if not bbox:
        return f"SELECT {select_cols} FROM bigquery_scan('{table_id}')"

    # Show bbox mode info
    if bbox_mode == "auto":
        progress(f"Bbox mode: auto (threshold: {bbox_threshold:,} rows)")
        progress("(Will check table size to determine server vs local filtering)")
    else:
        progress(f"Bbox mode: {bbox_mode}")

    xmin, ymin, xmax, ymax = parse_bbox(bbox)
    wkt = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"

    if bbox_mode == "local":
        query = f"SELECT {select_cols} FROM bigquery_scan('{table_id}')"
        query += f" WHERE ST_Intersects(<geometry_column>, ST_GeomFromText('{wkt}'))"
    else:
        # Server or auto mode - show server-side as example
        bq_filter = f"ST_INTERSECTS(<geometry_column>, ST_GEOGFROMTEXT(''{wkt}''))"
        query = f"SELECT {select_cols} FROM bigquery_scan('{table_id}', filter='{bq_filter}')"

    return query


def _build_column_list(
    con: duckdb.DuckDBPyConnection,
    table_id: str,
    include_list: list[str] | None,
    exclude_list: list[str] | None,
    geom_col: str | None,
) -> list[str] | None:
    """
    Build the column list for SELECT based on include/exclude lists.

    Returns:
        List of columns to select, or None for all columns
    """
    if include_list is not None:
        # Ensure geometry column is included unless explicitly excluded
        cols_to_select = list(include_list)
        if geom_col and geom_col not in cols_to_select:
            if exclude_list is None or geom_col not in exclude_list:
                cols_to_select.append(geom_col)
        return cols_to_select

    if exclude_list is not None:
        # Push down exclusions: get all columns, then remove excluded ones
        schema_query = f"DESCRIBE SELECT * FROM bigquery_scan('{table_id}') LIMIT 0"
        schema_result = con.execute(schema_query).fetchall()
        all_schema_cols = [row[0] for row in schema_result]
        return [c for c in all_schema_cols if c not in exclude_list]

    return None  # All columns


def _determine_bbox_strategy(
    con: duckdb.DuckDBPyConnection,
    table_id: str,
    bbox_mode: str,
    bbox_threshold: int,
) -> bool:
    """
    Determine whether to use server-side bbox filtering.

    Returns:
        True if server-side filtering should be used, False for local filtering
    """
    if bbox_mode == "server":
        debug("Using server-side bbox filter (forced by --bbox-mode server)")
        return True
    if bbox_mode == "local":
        debug("Using local bbox filter (forced by --bbox-mode local)")
        return False

    # Auto mode - decide based on row count
    row_count = _get_table_row_count(con, table_id)
    if row_count is not None:
        use_server = row_count >= bbox_threshold
        debug(f"Table has {row_count:,} rows, threshold is {bbox_threshold:,}")
        if use_server:
            debug("Using server-side bbox filter (table exceeds threshold)")
        else:
            debug("Using local bbox filter (table below threshold)")
        return use_server

    # Fallback to local if we can't get row count
    debug("Could not determine row count, defaulting to local filter")
    return False


def _build_bbox_filters(
    bbox: str,
    geom_col: str,
    use_server_side: bool,
) -> tuple[list[str], list[str]]:
    """
    Build bbox filter strings for server-side and local filtering.

    Returns:
        Tuple of (bq_filters list, local_conditions list)
    """
    xmin, ymin, xmax, ymax = parse_bbox(bbox)
    wkt = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"

    bq_filters = []
    local_conditions = []

    if use_server_side:
        bbox_filter = f"ST_INTERSECTS({geom_col}, ST_GEOGFROMTEXT(''{wkt}''))"
        bq_filters.append(bbox_filter)
        debug(f"BigQuery filter: {bbox_filter}")
    else:
        bbox_filter = f"ST_Intersects(\"{geom_col}\", ST_GeomFromText('{wkt}'))"
        local_conditions.append(bbox_filter)
        debug(f"DuckDB filter: {bbox_filter}")

    return bq_filters, local_conditions


def extract_bigquery(
    table_id: str,
    output_parquet: str | None = None,
    *,
    project: str | None = None,
    credentials_file: str | None = None,
    where: str | None = None,
    bbox: str | None = None,
    bbox_mode: str = "auto",
    bbox_threshold: int = 500000,
    limit: int | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    geography_column: str | None = None,
    geometry_format: str = "wkt",
    dry_run: bool = False,
    show_sql: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
) -> pa.Table | None:
    """
    Extract data from BigQuery table to GeoParquet or plain Parquet.

    Uses DuckDB's BigQuery extension with the Storage Read API for
    efficient Arrow-based scanning with filter pushdown.

    If a GEOGRAPHY column is detected (native GEOMETRY type), it is
    automatically converted. If --geography-column is specified, the
    named column is parsed as WKT or GeoJSON geometry.

    If no geometry column is found, the table is written as plain Parquet
    without GeoParquet metadata.

    Args:
        table_id: BigQuery table ID. Supports both formats:
            - project.dataset.table (fully qualified)
            - dataset.table (requires --project parameter)
        output_parquet: Output GeoParquet file path (None = return table only)
        project: GCP project ID. Required for dataset.table format.
            Overrides project in table_id if both are specified.
        credentials_file: Path to service account JSON file
        where: SQL WHERE clause for filtering (BigQuery SQL syntax)
        bbox: Bounding box for spatial filter as "minx,miny,maxx,maxy"
        bbox_mode: Filtering mode - "auto" (default), "server", or "local"
        bbox_threshold: Row count threshold for auto mode (default: 500000)
        limit: Maximum rows to extract
        include_cols: Comma-separated columns to include
        exclude_cols: Comma-separated columns to exclude
        geography_column: Name of column containing geometry data.
            Auto-detected for native GEOGRAPHY columns. Use this to
            specify a VARCHAR column containing WKT or GeoJSON geometry.
        geometry_format: Format of geometry in VARCHAR columns ("wkt" or "geojson")
        dry_run: Show SQL without executing
        show_sql: Print SQL being executed
        verbose: Enable verbose output
        compression: Output compression type
        compression_level: Compression level
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact rows per row group
        geoparquet_version: GeoParquet version to write

    Returns:
        PyArrow Table if output_parquet is None, otherwise None

    Raises:
        ValueError: If table_id format is invalid, project is missing when needed,
            or project ID doesn't match GCP naming rules
    """
    configure_verbose(verbose)

    # Normalize table_id early - validates format and applies project override
    # This ensures validated_table_id is always project.dataset.table format
    validated_table_id = _normalize_table_id(table_id, project)

    # Extract project from normalized table_id for connection setup
    normalized_project = validated_table_id.split(".")[0]

    # Parse column lists
    include_list = [c.strip() for c in include_cols.split(",")] if include_cols else None
    exclude_list = [c.strip() for c in exclude_cols.split(",")] if exclude_cols else None

    # Check if output file exists and handle overwrite (fixes issue #278)
    if output_parquet and not dry_run:
        handle_output_overwrite(output_parquet, overwrite)

    # Handle dry_run without connecting to BigQuery
    if dry_run:
        _handle_dry_run(
            validated_table_id, include_list, bbox, bbox_mode, bbox_threshold, where, limit
        )
        return None

    # Execute the actual BigQuery extraction
    return _execute_bigquery_extraction(
        validated_table_id=validated_table_id,
        project=normalized_project,
        credentials_file=credentials_file,
        geography_column=geography_column,
        geometry_format=geometry_format,
        include_list=include_list,
        exclude_list=exclude_list,
        bbox=bbox,
        bbox_mode=bbox_mode,
        bbox_threshold=bbox_threshold,
        where=where,
        limit=limit,
        show_sql=show_sql,
        output_parquet=output_parquet,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
    )


def _execute_bigquery_extraction(
    *,
    validated_table_id: str,
    project: str | None,
    credentials_file: str | None,
    geography_column: str | None,
    geometry_format: str = "wkt",
    include_list: list[str] | None,
    exclude_list: list[str] | None,
    bbox: str | None,
    bbox_mode: str,
    bbox_threshold: int,
    where: str | None,
    limit: int | None,
    show_sql: bool,
    output_parquet: str | None,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    geoparquet_version: str | None,
    verbose: bool,
) -> pa.Table | None:
    """Execute the BigQuery extraction with the given parameters."""
    debug("Connecting to BigQuery...")
    with BigQueryConnection(
        project=project,
        credentials_file=credentials_file,
    ) as con:
        # Detect geometry column from schema (native GEOMETRY type only)
        geom_col = _detect_geometry_column_from_schema(con, validated_table_id, geography_column)
        if geom_col:
            debug(f"Detected geometry column: {geom_col}")
        else:
            if geography_column:
                # User explicitly specified a column that isn't native GEOMETRY -
                # still use it, parsing as WKT or GeoJSON
                geom_col = geography_column
                debug(
                    f"Using '{geom_col}' as geometry column (parsing as {geometry_format.upper()})"
                )
            else:
                warn(
                    "No geometry column detected. Output will be plain Parquet "
                    "without GeoParquet metadata. Use --geography-column to "
                    "specify a column containing WKT or GeoJSON geometry."
                )

        # Build column list and SELECT clause
        cols_to_select = _build_column_list(
            con, validated_table_id, include_list, exclude_list, geom_col
        )
        select_cols, _ = _build_select_with_wkb(
            cols_to_select, geom_col, con, validated_table_id, geometry_format
        )

        # Build query with bbox and where filters
        query = _build_bigquery_query(
            con=con,
            validated_table_id=validated_table_id,
            select_cols=select_cols,
            bbox=bbox,
            bbox_mode=bbox_mode,
            bbox_threshold=bbox_threshold,
            geom_col=geom_col,
            where=where,
            limit=limit,
        )

        if show_sql:
            progress(f"SQL: {query}")

        # Execute query
        debug(f"Executing BigQuery query: {query}")
        progress("Querying BigQuery...")
        result = con.execute(query).arrow().read_all()
        row_count = result.num_rows
        progress(f"Retrieved {row_count:,} rows from BigQuery")

        # Determine if geometry column is in the final result
        final_geom_col = geom_col if geom_col and geom_col in result.column_names else None

        # Write output if path provided
        if output_parquet:
            if final_geom_col:
                write_geoparquet_table(
                    result,
                    output_parquet,
                    geometry_column=final_geom_col,
                    compression=compression,
                    compression_level=compression_level,
                    row_group_size_mb=row_group_size_mb,
                    row_group_rows=row_group_rows,
                    geoparquet_version=geoparquet_version,
                    verbose=verbose,
                    edges="spherical",
                )
            else:
                # No geometry - write plain Parquet
                import pyarrow.parquet as pq

                pq.write_table(result, output_parquet, compression="ZSTD")

            success(f"Extracted {row_count:,} rows to {output_parquet}")
            return None

        return result


def _build_bigquery_query(
    *,
    con: duckdb.DuckDBPyConnection,
    validated_table_id: str,
    select_cols: str,
    bbox: str | None,
    bbox_mode: str,
    bbox_threshold: int,
    geom_col: str | None,
    where: str | None,
    limit: int | None,
) -> str:
    """Build the BigQuery query with filters applied."""
    bq_filters: list[str] = []
    local_conditions: list[str] = []

    # Handle bbox filtering
    if bbox and geom_col:
        use_server_side = _determine_bbox_strategy(
            con, validated_table_id, bbox_mode, bbox_threshold
        )
        bq_filters, local_conditions = _build_bbox_filters(bbox, geom_col, use_server_side)
    elif bbox and not geom_col:
        warn("--bbox specified but no geometry column detected; ignoring spatial filter")

    # Build base query
    if bq_filters:
        filter_str = " AND ".join(bq_filters)
        query = (
            f"SELECT {select_cols} FROM bigquery_scan('{validated_table_id}', "
            f"filter='{filter_str}')"
        )
    else:
        query = f"SELECT {select_cols} FROM bigquery_scan('{validated_table_id}')"

    # Add WHERE clause
    conditions = local_conditions.copy()
    if where:
        conditions.append(f"({where})")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    # Add LIMIT
    if limit is not None:
        query += f" LIMIT {limit}"

    return query
