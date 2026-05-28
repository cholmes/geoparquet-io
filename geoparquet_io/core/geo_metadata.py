"""
GeoParquet metadata handling functions.

This module provides functions for parsing, creating, and applying GeoParquet
metadata to Arrow tables and Parquet files. It handles both GeoParquet 1.x and
2.0 formats, including bbox covering metadata and native geometry types.

Usage in core modules:
    from geoparquet_io.core.geo_metadata import (
        parse_geo_metadata,
        create_geo_metadata,
        check_bbox_structure,
        get_bbox_advice,
    )

Note: This module uses lazy imports for functions from other core modules
to avoid circular dependencies.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import duckdb

from geoparquet_io.core.logging_config import debug, warn

if TYPE_CHECKING:
    import pyarrow as pa

# =============================================================================
# GeoParquet Version Configuration
# =============================================================================

GEOPARQUET_VERSIONS = {
    "1.0": {"duckdb_param": "V1", "metadata_version": "1.0.0", "rewrite_metadata": True},
    "1.1": {"duckdb_param": "V1", "metadata_version": "1.1.0", "rewrite_metadata": True},
    "2.0": {"duckdb_param": "V2", "metadata_version": "2.0.0", "rewrite_metadata": False},
    "parquet-geo-only": {
        "duckdb_param": "NONE",
        "metadata_version": None,
        "rewrite_metadata": False,
    },
}

DEFAULT_GEOPARQUET_VERSION = "1.1"

# =============================================================================
# Geometry Type Mappings
# =============================================================================

# WKB geometry type codes to GeoParquet base names (2D types)
_GEOMETRY_TYPE_CODES = {
    0: "Unknown",
    1: "Point",
    2: "LineString",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
}

# Dimensional suffixes based on WKB type code modifier
_DIMENSION_SUFFIXES = {
    0: "",  # 2D (no suffix)
    1: " Z",  # Z dimension (codes 1001-1007)
    2: " M",  # M dimension (codes 2001-2007)
    3: " ZM",  # ZM dimensions (codes 3001-3007)
}


# =============================================================================
# Metadata Parsing Functions
# =============================================================================


def parse_geo_metadata(metadata: dict | None, verbose: bool = False) -> dict | None:
    """
    Parse GeoParquet metadata from Parquet file metadata.

    Extracts and decodes the 'geo' key from Parquet metadata bytes.

    Args:
        metadata: Parquet file metadata dict with bytes keys
        verbose: Print verbose output

    Returns:
        Parsed geo metadata dict, or None if not present or invalid
    """
    if not metadata or b"geo" not in metadata:
        return None

    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        if verbose:
            debug("\nParsed geo metadata:")
            debug(json.dumps(geo_meta, indent=2))
        return geo_meta
    except json.JSONDecodeError:
        if verbose:
            warn("Failed to parse geo metadata as JSON")
        return None


def _parse_existing_geo_metadata(original_metadata: dict | None) -> dict | None:
    """
    Parse existing geo metadata from original parquet metadata.

    Args:
        original_metadata: Original parquet file metadata dict

    Returns:
        Parsed geo metadata dict, or None if not present
    """
    if not original_metadata or b"geo" not in original_metadata:
        return None
    try:
        return json.loads(original_metadata[b"geo"].decode("utf-8"))
    except json.JSONDecodeError:
        return None


# =============================================================================
# Metadata Initialization and Building
# =============================================================================


def _initialize_geo_metadata(geo_meta: dict | None, geom_col: str, version: str = "1.1.0") -> dict:
    """
    Initialize or upgrade geo metadata structure.

    Creates a minimal valid GeoParquet metadata structure if none exists,
    or ensures existing metadata has the required structure.

    Args:
        geo_meta: Existing geo metadata dict or None
        geom_col: Name of the geometry column
        version: GeoParquet version string (e.g., "1.0.0", "1.1.0", "2.0.0")

    Returns:
        Initialized geo metadata structure
    """
    if not geo_meta:
        return {"version": version, "primary_column": geom_col, "columns": {geom_col: {}}}

    # Set the specified version
    geo_meta["version"] = version
    if "columns" not in geo_meta:
        geo_meta["columns"] = {}
    if geom_col not in geo_meta["columns"]:
        geo_meta["columns"][geom_col] = {}

    return geo_meta


def _add_bbox_covering(
    geo_meta: dict, geom_col: str, bbox_info: dict | None, verbose: bool
) -> None:
    """
    Add bbox covering metadata to geometry column.

    Updates geo_meta in place with bbox covering information that points
    to the bbox struct column fields.

    Args:
        geo_meta: Geo metadata dict to update
        geom_col: Name of the geometry column
        bbox_info: Result from check_bbox_structure, or None
        verbose: Print verbose output
    """
    if not bbox_info or not bbox_info.get("has_bbox_column"):
        return

    if "covering" not in geo_meta["columns"][geom_col]:
        geo_meta["columns"][geom_col]["covering"] = {}

    geo_meta["columns"][geom_col]["covering"]["bbox"] = {
        "xmin": [bbox_info["bbox_column_name"], "xmin"],
        "ymin": [bbox_info["bbox_column_name"], "ymin"],
        "xmax": [bbox_info["bbox_column_name"], "xmax"],
        "ymax": [bbox_info["bbox_column_name"], "ymax"],
    }
    if verbose:
        debug(f"Added bbox covering metadata for column '{bbox_info['bbox_column_name']}'")


def _add_custom_covering(
    geo_meta: dict, geom_col: str, custom_metadata: dict | None, verbose: bool
) -> None:
    """
    Add custom covering metadata (e.g., H3, S2).

    Updates geo_meta in place with custom covering information for spatial
    indices like H3 or S2.

    Args:
        geo_meta: Geo metadata dict to update
        geom_col: Name of the geometry column
        custom_metadata: Dict with custom metadata including 'covering' key
        verbose: Print verbose output
    """
    if not custom_metadata or "covering" not in custom_metadata:
        return

    if "covering" not in geo_meta["columns"][geom_col]:
        geo_meta["columns"][geom_col]["covering"] = {}

    geo_meta["columns"][geom_col]["covering"].update(custom_metadata["covering"])
    if verbose:
        for key in custom_metadata["covering"]:
            debug(f"Added {key} covering metadata")


def create_geo_metadata(
    original_metadata: dict | None,
    geom_col: str,
    bbox_info: dict | None,
    custom_metadata: dict | None = None,
    verbose: bool = False,
    version: str = "1.1.0",
    edges: str | None = None,
) -> dict:
    """
    Create or update GeoParquet metadata with spatial index covering information.

    Builds a complete GeoParquet metadata structure from existing metadata
    and new covering information.

    Args:
        original_metadata: Original parquet metadata dict
        geom_col: Name of the geometry column
        bbox_info: Result from check_bbox_structure
        custom_metadata: Optional dict with custom metadata (e.g., H3 info)
        verbose: Whether to print verbose output
        version: GeoParquet version string (e.g., "1.0.0", "1.1.0", "2.0.0")
        edges: Edge interpretation, "spherical" or "planar" (default None = planar).
               Use "spherical" for data from BigQuery or other S2-based sources.

    Returns:
        Updated geo metadata dict
    """
    geo_meta = _parse_existing_geo_metadata(original_metadata)
    geo_meta = _initialize_geo_metadata(geo_meta, geom_col, version=version)

    # Add encoding if not present (required by GeoParquet spec)
    if "encoding" not in geo_meta["columns"][geom_col]:
        geo_meta["columns"][geom_col]["encoding"] = "WKB"

    # Add edges if specified (for spherical geometry from BigQuery, etc.)
    if edges:
        geo_meta["columns"][geom_col]["edges"] = edges
        # When spherical, orientation should be counterclockwise per GeoParquet spec
        if edges == "spherical":
            geo_meta["columns"][geom_col]["orientation"] = "counterclockwise"

    # Add bbox covering if needed
    _add_bbox_covering(geo_meta, geom_col, bbox_info, verbose)

    # Add custom covering if needed
    _add_custom_covering(geo_meta, geom_col, custom_metadata, verbose)

    # Add any top-level custom metadata
    if custom_metadata:
        for key, value in custom_metadata.items():
            if key != "covering":
                geo_meta[key] = value

    return geo_meta


# =============================================================================
# SQL-based Metadata Computation
# =============================================================================


def _get_query_columns(con, query: str) -> list[str]:
    """
    Get column names from a query without executing it fully.

    Uses LIMIT 0 to get schema information efficiently.

    Args:
        con: DuckDB connection
        query: SQL SELECT query

    Returns:
        Column names from the query result
    """
    describe_query = f"SELECT * FROM ({query}) AS __subq LIMIT 0"
    result = con.execute(describe_query)
    return [col[0] for col in result.description]


def _get_query_column_type(con, query: str, column_name: str) -> str | None:
    """Return the DuckDB type string for a named column in a query, or None."""
    try:
        rows = con.execute(f"DESCRIBE SELECT * FROM ({query}) LIMIT 0").fetchall()
        for row in rows:
            if row[0] == column_name:
                return row[1]
    except Exception:
        pass
    return None


def compute_bbox_via_sql(
    con,
    query: str,
    geometry_column: str,
) -> list[float] | None:
    """
    Compute bounding box from query using DuckDB spatial functions.

    Uses ST_XMin/YMin/XMax/YMax aggregate functions to compute the
    overall bounding box of all geometries.

    Args:
        con: DuckDB connection with spatial extension loaded
        query: SQL query containing geometry column
        geometry_column: Name of geometry column

    Returns:
        [xmin, ymin, xmax, ymax] or None if query returns no rows
        or geometry column not in query
    """
    # Check if geometry column exists in query result
    try:
        columns = _get_query_columns(con, query)
        if geometry_column not in columns:
            return None
    except (duckdb.Error, RuntimeError, ValueError, AttributeError):
        # If we can't determine schema, return None rather than failing
        return None

    # Escape column name for SQL (double any embedded quotes)
    escaped_col = geometry_column.replace('"', '""')
    quoted_geom = f'"{escaped_col}"'

    # GeoArrow native types (STRUCT(x DOUBLE, y DOUBLE)[N]) cannot be passed to
    # ST_XMin directly. Detect at runtime and use UNNEST to extract coordinates.
    col_type = _get_query_column_type(con, query, geometry_column) or ""
    if "STRUCT" in col_type:
        from geoparquet_io.core.convert import _geoarrow_coord_exprs

        # Infer encoding from nesting depth in the type string
        depth = col_type.count("[]")
        encoding_by_depth = {0: "point", 1: "linestring", 2: "polygon", 3: "multipolygon"}
        encoding = encoding_by_depth.get(depth, "linestring")
        xmin_e, ymin_e, xmax_e, ymax_e, _, _ = _geoarrow_coord_exprs(quoted_geom, encoding)
        bbox_query = f"""
            SELECT
                MIN({xmin_e}) as xmin,
                MIN({ymin_e}) as ymin,
                MAX({xmax_e}) as xmax,
                MAX({ymax_e}) as ymax
            FROM ({query})
        """
    else:
        bbox_query = f"""
            SELECT
                MIN(ST_XMin("{escaped_col}")) as xmin,
                MIN(ST_YMin("{escaped_col}")) as ymin,
                MAX(ST_XMax("{escaped_col}")) as xmax,
                MAX(ST_YMax("{escaped_col}")) as ymax
            FROM ({query})
        """
    result = con.execute(bbox_query).fetchone()

    if result and all(v is not None for v in result):
        return list(result)
    return None


def compute_geometry_types_via_sql(
    con,
    query: str,
    geometry_column: str,
) -> list[str]:
    """
    Compute distinct geometry types from query using DuckDB.

    Uses ST_GeometryType to get distinct types and normalizes them
    to GeoParquet format (e.g., "POINT" -> "Point").

    Args:
        con: DuckDB connection with spatial extension loaded
        query: SQL query containing geometry column
        geometry_column: Name of geometry column

    Returns:
        List of geometry type names (e.g., ["Point", "Polygon"])
        or empty list if column not in query
    """
    # Check if geometry column exists in query result
    try:
        columns = _get_query_columns(con, query)
        if geometry_column not in columns:
            return []
    except (duckdb.Error, RuntimeError, ValueError, AttributeError):
        # If we can't determine schema, return empty list rather than failing
        return []

    # GeoArrow native types (STRUCT(x DOUBLE, y DOUBLE)[N]) can't use ST_GeometryType.
    # For native encodings, geometry_types is already known from the encoding name;
    # returning [] here is valid — GeoParquet allows omitting geometry_types.
    col_type = _get_query_column_type(con, query, geometry_column) or ""
    if "STRUCT" in col_type:
        return []

    # Escape column name for SQL (double any embedded quotes)
    escaped_col = geometry_column.replace('"', '""')
    types_query = f"""
        SELECT DISTINCT ST_GeometryType("{escaped_col}") as geom_type
        FROM ({query})
        WHERE "{escaped_col}" IS NOT NULL
    """
    results = con.execute(types_query).fetchall()

    # DuckDB returns types like "POINT", "POLYGON" - convert to GeoParquet format
    type_map = {
        "POINT": "Point",
        "LINESTRING": "LineString",
        "POLYGON": "Polygon",
        "MULTIPOINT": "MultiPoint",
        "MULTILINESTRING": "MultiLineString",
        "MULTIPOLYGON": "MultiPolygon",
        "GEOMETRYCOLLECTION": "GeometryCollection",
    }

    types = []
    for (geom_type,) in results:
        if geom_type:
            normalized = type_map.get(geom_type.upper(), geom_type)
            types.append(normalized)

    return sorted(set(types))


# =============================================================================
# Version and Type Detection
# =============================================================================


def _detect_version_from_table(table: pa.Table, verbose: bool = False) -> str | None:
    """
    Detect GeoParquet version from table's schema metadata.

    Checks the table's schema metadata for existing geo metadata and extracts
    the version. Also checks for native geoarrow extension types which indicate
    v2.0 or parquet-geo-only format.

    Args:
        table: PyArrow Table to check
        verbose: Whether to print verbose output

    Returns:
        Version string (e.g., "1.1", "2.0", "parquet-geo-only") or None
    """
    # Lazy import to avoid circular dependency
    from geoparquet_io.core.streaming import is_geoarrow_type

    # Check for native geoarrow extension types (indicates v2.0 or parquet-geo-only)
    has_native_geo = False
    for field in table.schema:
        if is_geoarrow_type(field.type):
            has_native_geo = True
            break

    # Check schema metadata for geo version
    metadata = table.schema.metadata
    if not metadata:
        if has_native_geo:
            # Native geo types but no metadata suggests parquet-geo-only
            if verbose:
                debug("Detected parquet-geo-only format from native geo types")
            return "parquet-geo-only"
        return None

    if b"geo" not in metadata:
        if has_native_geo:
            # Native geo types but no geo metadata = parquet-geo-only
            if verbose:
                debug("Detected parquet-geo-only format (native types, no geo metadata)")
            return "parquet-geo-only"
        return None

    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        if isinstance(geo_meta, dict):
            version = geo_meta.get("version")
            if version:
                parts = version.split(".")
                if len(parts) >= 2:
                    major = parts[0]
                    if major == "2":
                        if verbose:
                            debug("Detected GeoParquet version 2.0 from table metadata")
                        return "2.0"
                    # Upgrade all 1.x versions to 1.1 (backwards compatible)
                    if major == "1":
                        if verbose:
                            debug("Detected GeoParquet version 1.x from table metadata")
                        return "1.1"
        return None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _detect_bbox_column_from_table(table: pa.Table, verbose: bool = False) -> str | None:
    """
    Detect bbox struct column from Arrow table schema.

    Looks for columns with conventional names (bbox, bounds, extent) that have
    the required struct fields (xmin, ymin, xmax, ymax).

    Args:
        table: PyArrow Table to check
        verbose: Whether to print verbose output

    Returns:
        Name of bbox column if found, None otherwise
    """
    import pyarrow as pa

    conventional_suffixes = ["bbox", "bounds", "extent"]
    required_fields = {"xmin", "ymin", "xmax", "ymax"}

    for field in table.schema:
        name = field.name
        field_type = field.type

        # Check if column name ends with conventional suffixes
        is_bbox_name = any(name.endswith(suffix) for suffix in conventional_suffixes)
        if not is_bbox_name:
            continue

        # Check if it's a struct with the required fields
        if pa.types.is_struct(field_type):
            struct_field_names = {f.name for f in field_type}
            if required_fields.issubset(struct_field_names):
                if verbose:
                    debug(f"Found bbox column in table: {name}")
                return name

    return None


# =============================================================================
# Geometry Type Helpers
# =============================================================================


def _get_geometry_type_name(code: int) -> str:
    """
    Convert WKB geometry type code to GeoParquet geometry type name.

    Handles 2D types (0-7) and Z/M/ZM variants (1001-1007, 2001-2007, 3001-3007).

    Args:
        code: WKB geometry type code

    Returns:
        GeoParquet geometry type name (e.g., "Point", "Point Z", "Polygon ZM")
    """
    # Extract base type (0-7) and dimensional modifier (0, 1, 2, or 3)
    base_type = code % 1000
    dimension = code // 1000

    base_name = _GEOMETRY_TYPE_CODES.get(base_type, "Unknown")
    if base_name == "Unknown":
        return "Unknown"

    suffix = _DIMENSION_SUFFIXES.get(dimension, "")
    return base_name + suffix


def _is_geoarrow_extension_type(arrow_type) -> bool:
    """
    Check if an Arrow type is a geoarrow extension type.

    Args:
        arrow_type: PyArrow type to check

    Returns:
        True if the type is a geoarrow extension type
    """
    if hasattr(arrow_type, "extension_name"):
        return arrow_type.extension_name.startswith("geoarrow")
    return False


# =============================================================================
# Geometry Data Computation
# =============================================================================


def _compute_geometry_types(table: pa.Table, geometry_column: str, verbose: bool) -> list[str]:
    """
    Compute geometry types from a geometry column using geoarrow.

    Analyzes the actual geometry data to determine the set of geometry types
    present in the column.

    Args:
        table: PyArrow Table containing the geometry column
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output

    Returns:
        List of GeoParquet geometry type names (e.g., ["Point", "Polygon"])
    """
    import geoarrow.pyarrow as ga
    import pyarrow.compute as pc

    # Skip for empty tables (geoarrow crashes on empty arrays)
    if table.num_rows == 0:
        return []

    try:
        geom_col = table.column(geometry_column)

        # Filter out NULL values to avoid geoarrow errors on invalid geometries
        # This handles cases where BigQuery returns NULL or empty geometries
        non_null_mask = pc.is_valid(geom_col)
        if pc.any(non_null_mask).as_py():
            geom_col = pc.filter(geom_col, non_null_mask)
        else:
            # All values are NULL
            return []

        # Skip if no valid geometries remain after filtering
        if len(geom_col) == 0:
            return []

        wkb_arr = ga.as_wkb(geom_col)
        types_struct = ga.unique_geometry_types(wkb_arr)

        # Extract geometry type codes from struct array
        type_codes = types_struct.field("geometry_type").to_pylist()

        # Map codes to GeoParquet standard names (avoid duplicates)
        type_names = []
        for code in type_codes:
            name = _get_geometry_type_name(code)
            if name not in type_names:
                type_names.append(name)

        if verbose:
            debug(f"Computed geometry_types from data: {type_names}")
        return type_names

    except Exception as e:
        # Catch all exceptions including geoarrow C++ errors
        # (e.g., "Expected valid geometry type code but found 0")
        if verbose:
            debug(f"Could not compute geometry_types: {e}")
        # Return empty list as fallback (allowed by spec - means any type)
        return []


def _compute_bbox_from_data(
    table: pa.Table, geometry_column: str, verbose: bool
) -> list[float] | None:
    """
    Compute bounding box from geometry column data.

    Uses geoarrow to compute the overall bounding box of all geometries
    in the column.

    Args:
        table: PyArrow Table containing the geometry column
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output

    Returns:
        [xmin, ymin, xmax, ymax] or None if computation fails
    """
    import geoarrow.pyarrow as ga
    import pyarrow.compute as pc

    # Skip for empty tables
    if table.num_rows == 0:
        return None

    try:
        geom_col = table.column(geometry_column)

        # Filter out NULL values to avoid geoarrow errors on invalid geometries
        non_null_mask = pc.is_valid(geom_col)
        if pc.any(non_null_mask).as_py():
            geom_col = pc.filter(geom_col, non_null_mask)
        else:
            # All values are NULL
            return None

        # Skip if no valid geometries remain after filtering
        if len(geom_col) == 0:
            return None

        wkb_arr = ga.as_wkb(geom_col)
        box_arr = ga.box(wkb_arr)

        # Combine chunks and get storage (underlying struct array)
        combined = box_arr.combine_chunks()
        storage = combined.storage

        # Extract struct fields and compute min/max
        xmin = pc.min(pc.struct_field(storage, "xmin")).as_py()
        ymin = pc.min(pc.struct_field(storage, "ymin")).as_py()
        xmax = pc.max(pc.struct_field(storage, "xmax")).as_py()
        ymax = pc.max(pc.struct_field(storage, "ymax")).as_py()

        if all(v is not None for v in [xmin, ymin, xmax, ymax]):
            if verbose:
                debug(f"Computed bbox from data: [{xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f}]")
            return [xmin, ymin, xmax, ymax]

    except Exception as e:
        # Catch all exceptions including geoarrow C++ errors
        if verbose:
            debug(f"Could not compute bbox: {e}")

    return None


# =============================================================================
# Metadata Application
# =============================================================================


def _assemble_and_apply_geo_metadata(
    table: pa.Table,
    geometry_column: str,
    geo_meta: dict,
    input_crs: dict | None,
    metadata_version: str,
    verbose: bool,
) -> pa.Table:
    """
    Assemble final geo metadata and apply it to the table.

    Adds CRS to geo metadata if provided and applies the complete
    metadata to the table schema.

    Args:
        table: PyArrow Table to modify
        geometry_column: Name of the geometry column
        geo_meta: Geo metadata dict to finalize
        input_crs: PROJJSON dict with CRS (optional)
        metadata_version: GeoParquet metadata version string
        verbose: Whether to print verbose output

    Returns:
        Table with geo metadata applied
    """
    # Lazy import to avoid circular dependency
    from geoparquet_io.core.crs_utils import _format_crs_display, is_default_crs

    # Add CRS to geo metadata if provided (for v1.x and v2.0)
    if input_crs and not is_default_crs(input_crs):
        if geometry_column not in geo_meta.get("columns", {}):
            geo_meta["columns"][geometry_column] = {}
        geo_meta["columns"][geometry_column]["crs"] = input_crs
        if verbose:
            debug(f"Added CRS to geo metadata: {_format_crs_display(input_crs)}")

    # Apply metadata to table
    existing_metadata = dict(table.schema.metadata) if table.schema.metadata else {}
    new_metadata = {}

    # Copy non-geo metadata from existing
    for k, v in existing_metadata.items():
        key_str = k.decode("utf-8") if isinstance(k, bytes) else k
        if not key_str.startswith("geo"):
            new_metadata[k] = v

    # Add geo metadata
    new_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    table = table.replace_schema_metadata(new_metadata)

    if verbose:
        debug(f"Applied geo metadata with version {metadata_version}")

    return table


def _apply_geoparquet_metadata(
    table: pa.Table,
    geometry_column: str,
    geoparquet_version: str | None,
    original_metadata: dict | None = None,
    input_crs: dict | None = None,
    custom_metadata: dict | None = None,
    verbose: bool = False,
    edges: str | None = None,
    geometry_info: dict | None = None,
) -> pa.Table:
    """
    Apply GeoParquet metadata to an Arrow Table based on version.

    Handles different GeoParquet versions:
    - v1.x: Apply geo metadata to schema, CRS via geoarrow type
    - v2.0: Apply CRS to schema type AND geo metadata
    - parquet-geo-only: Apply CRS to schema type only, no geo metadata

    When geoparquet_version is None, the function will detect the version from
    the table's existing schema metadata, preserving v2.0 or parquet-geo-only
    formats when present.

    Args:
        table: PyArrow Table to modify
        geometry_column: Name of the geometry column
        geoparquet_version: GeoParquet version (1.0, 1.1, 2.0, parquet-geo-only),
            or None to auto-detect from existing table metadata
        original_metadata: Original metadata to preserve
        input_crs: PROJJSON dict with CRS
        custom_metadata: Custom metadata (e.g., H3 covering info)
        verbose: Whether to print verbose output
        edges: Edge interpretation, "spherical" or "planar" (default None = planar).
               Use "spherical" for data from BigQuery or other S2-based sources.
        geometry_info: Dict containing multi-geometry column info with keys:
            - "primary": primary geometry column name
            - "secondary": list of secondary geometry column names
            - "metadata": dict mapping column names to their metadata

    Returns:
        Table with GeoParquet metadata applied
    """
    # Lazy import to avoid circular dependency
    from geoparquet_io.core.common import _process_geometry_column_for_version

    # Auto-detect version from table schema metadata if not specified
    effective_version = geoparquet_version
    if effective_version is None:
        effective_version = _detect_version_from_table(table, verbose)

    version_config = GEOPARQUET_VERSIONS.get(
        effective_version, GEOPARQUET_VERSIONS[DEFAULT_GEOPARQUET_VERSION]
    )
    metadata_version = version_config["metadata_version"]
    should_add_geo_metadata = effective_version != "parquet-geo-only"

    if verbose:
        debug(f"Applying GeoParquet metadata for version: {effective_version or 'default (1.1)'}")

    # Check if geometry column exists in table
    if geometry_column not in table.column_names:
        if verbose:
            debug(f"Geometry column '{geometry_column}' not found in table, skipping metadata")
        return table

    # Step 1: Handle geometry column based on version
    table = _process_geometry_column_for_version(
        table, geometry_column, effective_version, input_crs, verbose
    )

    # Step 2: Build and apply geo metadata (unless parquet-geo-only)
    if not should_add_geo_metadata:
        return table

    # Detect bbox column from table schema
    bbox_column = _detect_bbox_column_from_table(table, verbose)
    bbox_info = {
        "has_bbox_column": bbox_column is not None,
        "bbox_column_name": bbox_column,
    }

    # Create geo metadata using existing helper
    geo_meta = create_geo_metadata(
        original_metadata,
        geometry_column,
        bbox_info,
        custom_metadata,
        verbose,
        version=metadata_version,
        edges=edges,
    )

    # Ensure geometry_types is set (required by GeoParquet spec)
    col_meta = geo_meta.get("columns", {}).get(geometry_column, {})
    if "geometry_types" not in col_meta:
        col_meta["geometry_types"] = _compute_geometry_types(table, geometry_column, verbose)
        geo_meta["columns"][geometry_column] = col_meta

    # Compute file-level bbox from geometry data
    computed_bbox = _compute_bbox_from_data(table, geometry_column, verbose)
    if computed_bbox:
        col_meta["bbox"] = computed_bbox
        geo_meta["columns"][geometry_column] = col_meta

    # Handle secondary geometry columns from geometry_info
    if geometry_info:
        secondary_columns = geometry_info.get("secondary", [])
        column_metadata = geometry_info.get("metadata", {})

        for sec_col in secondary_columns:
            if sec_col not in geo_meta["columns"]:
                geo_meta["columns"][sec_col] = {}

            sec_meta = geo_meta["columns"][sec_col]

            # Copy metadata from input for secondary columns
            if sec_col in column_metadata:
                input_sec_meta = column_metadata[sec_col]
                for key, value in input_sec_meta.items():
                    # Preserve input metadata (crs, encoding, geometry_types, etc.)
                    if key not in sec_meta:
                        sec_meta[key] = value

            # Ensure encoding is set (required by spec)
            if "encoding" not in sec_meta:
                sec_meta["encoding"] = "WKB"

    # Assemble and apply final metadata
    return _assemble_and_apply_geo_metadata(
        table, geometry_column, geo_meta, input_crs, metadata_version, verbose
    )


# =============================================================================
# Bbox Structure Checking
# =============================================================================


def _find_bbox_column_in_schema(schema_info: list[dict], verbose: bool) -> str | None:
    """
    Find bbox column in schema by conventional names or structure.

    Args:
        schema_info: List of column dicts from get_schema_info()
        verbose: Whether to print verbose output

    Note:
        DuckDB's parquet_schema() returns nested struct fields without parent prefix.
        For a struct column 'bbox' with fields xmin/ymin/xmax/ymax:
        - bbox appears with num_children=4
        - Child fields appear as 'xmin', 'ymin', 'xmax', 'ymax' (not 'bbox.xmin')

    Returns:
        Name of bbox column if found, None otherwise
    """
    # Check for columns ending with these suffixes (e.g., geometry_bbox, bbox)
    conventional_suffixes = ["bbox", "bounds", "extent"]
    required_fields = {"xmin", "ymin", "xmax", "ymax"}

    for i, col in enumerate(schema_info):
        name = col.get("name", "")
        num_children = col.get("num_children", 0)

        if not name:
            continue

        # Check if column name ends with conventional suffixes and has struct children
        is_bbox_name = any(name.endswith(suffix) for suffix in conventional_suffixes)
        if is_bbox_name and num_children >= 4:
            # Get the next num_children entries as the struct's child fields
            child_names = set()
            for j in range(1, num_children + 1):
                if i + j < len(schema_info):
                    child_name = schema_info[i + j].get("name", "")
                    child_names.add(child_name)

            # Check if all required fields are present
            if required_fields.issubset(child_names):
                if verbose:
                    debug(f"Found bbox column: {name} with children: {child_names}")
                return name

    return None


def _check_bbox_metadata_covering(
    geo_meta: dict | None, has_bbox_column: bool, verbose: bool
) -> bool:
    """
    Check if geo metadata contains proper bbox covering.

    Args:
        geo_meta: Parsed geo metadata dict (from get_geo_metadata())
        has_bbox_column: Whether a bbox column was found in schema
        verbose: Whether to print verbose output

    Returns:
        True if bbox covering metadata is properly configured
    """
    if not (geo_meta and has_bbox_column):
        return False

    if verbose:
        debug("\nParsed geo metadata:")
        debug(json.dumps(geo_meta, indent=2))

    if isinstance(geo_meta, dict) and "columns" in geo_meta:
        columns = geo_meta["columns"]
        for _col_name, col_info in columns.items():
            if isinstance(col_info, dict) and col_info.get("covering", {}).get("bbox"):
                bbox_refs = col_info["covering"]["bbox"]
                # Check if the bbox covering has the required structure
                if (
                    isinstance(bbox_refs, dict)
                    and all(key in bbox_refs for key in ["xmin", "ymin", "xmax", "ymax"])
                    and all(isinstance(ref, list) and len(ref) == 2 for ref in bbox_refs.values())
                ):
                    referenced_bbox_column = bbox_refs["xmin"][0]
                    if verbose:
                        debug(
                            f"Found bbox covering in metadata referencing column: "
                            f"{referenced_bbox_column}"
                        )
                    return True

    return False


def _determine_bbox_status(
    has_bbox_column: bool, bbox_column_name: str | None, has_bbox_metadata: bool
) -> tuple[str, str]:
    """
    Determine bbox status and message.

    Args:
        has_bbox_column: Whether a bbox column was found
        bbox_column_name: Name of the bbox column
        has_bbox_metadata: Whether bbox covering metadata exists

    Returns:
        Tuple of (status, message) where status is "optimal", "suboptimal", or "poor"
    """
    if has_bbox_column and has_bbox_metadata:
        return (
            "optimal",
            f"Found bbox column '{bbox_column_name}' with proper metadata covering",
        )
    elif has_bbox_column:
        return (
            "suboptimal",
            f"Found bbox column '{bbox_column_name}' but no bbox covering metadata "
            "(recommended for better performance)",
        )
    else:
        return "poor", "No valid bbox column found"


def check_bbox_structure(parquet_file: str, verbose: bool = False) -> dict:
    """
    Check bbox structure and metadata coverage in a GeoParquet file.

    Analyzes a file to determine if it has a bbox column and whether
    the GeoParquet metadata properly references it.

    Args:
        parquet_file: Path to the parquet file (local or remote URL)
        verbose: Whether to print verbose output

    Returns:
        dict with:
            - has_bbox_column (bool): Whether a valid bbox struct column exists
            - bbox_column_name (str): Name of the bbox column if found
            - has_bbox_metadata (bool): Whether bbox covering is in metadata
            - status (str): "optimal", "suboptimal", or "poor"
            - message (str): Human readable description
    """
    # Lazy imports to avoid circular dependency
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata, get_schema_info
    from geoparquet_io.core.file_utils import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)

    # Get schema info using DuckDB
    schema_info = get_schema_info(safe_url)

    if verbose:
        debug("\nSchema fields:")
        for col in schema_info:
            name = col.get("name", "")
            col_type = col.get("type", "")
            if name:  # Skip empty names
                debug(f"  {name}: {col_type}")

    # Find the bbox column in the schema
    bbox_column_name = _find_bbox_column_in_schema(schema_info, verbose)
    has_bbox_column = bbox_column_name is not None

    # Get geo metadata and check for bbox covering
    geo_meta = get_geo_metadata(safe_url)
    has_bbox_metadata = _check_bbox_metadata_covering(geo_meta, has_bbox_column, verbose)

    # Determine status and message
    status, message = _determine_bbox_status(has_bbox_column, bbox_column_name, has_bbox_metadata)

    if verbose:
        debug("\nFinal results:")
        debug(f"  has_bbox_column: {has_bbox_column}")
        debug(f"  bbox_column_name: {bbox_column_name}")
        debug(f"  has_bbox_metadata: {has_bbox_metadata}")
        debug(f"  status: {status}")
        debug(f"  message: {message}")

    return {
        "has_bbox_column": has_bbox_column,
        "bbox_column_name": bbox_column_name if has_bbox_column else None,
        "has_bbox_metadata": has_bbox_metadata,
        "status": status,
        "message": message,
    }


def get_bbox_advice(
    parquet_file: str,
    operation: str,
    verbose: bool = False,
) -> dict:
    """
    Get version-aware bbox optimization advice.

    Provides context-aware recommendations based on file type and operation:
    - For GeoParquet 2.0/parquet-geo with spatial_filtering: No bbox needed
    - For GeoParquet 2.0/parquet-geo with bounds_calculation: bbox recommended
    - For GeoParquet 1.x without bbox: Suggest adding bbox OR upgrading to 2.0

    Args:
        parquet_file: Path to the parquet file
        operation: One of:
            - "spatial_filtering": For ST_Intersects, spatial joins, etc.
            - "bounds_calculation": For centroid, extent, quadkey, etc.
            - "check": For validation/inspection
        verbose: Whether to print verbose output

    Returns:
        dict with:
            - needs_warning: bool - Whether to show a warning to the user
            - skip_bbox_prefilter: bool - Whether to skip bbox pre-filtering
            - has_native_geometry: bool - Whether file uses native Parquet geo
            - message: str - User-facing message (if needs_warning)
            - suggestions: list[str] - Suggested actions for the user
    """
    # Lazy import to avoid circular dependency
    from geoparquet_io.core.common import detect_geoparquet_file_type

    file_info = detect_geoparquet_file_type(parquet_file, verbose)
    bbox_info = check_bbox_structure(parquet_file, verbose)

    has_native_geo = file_info["file_type"] in ("geoparquet_v2", "parquet_geo_only")
    has_bbox = bbox_info["has_bbox_column"]

    # Only skip bbox pre-filtering for spatial_filtering operations with native geometry.
    # For bounds_calculation, bbox column provides pre-computed values that are faster.
    skip_bbox = has_native_geo and operation == "spatial_filtering"

    result = {
        "needs_warning": False,
        "skip_bbox_prefilter": skip_bbox,
        "has_native_geometry": has_native_geo,
        "has_bbox_column": has_bbox,
        "bbox_column_name": bbox_info.get("bbox_column_name"),
        "message": "",
        "suggestions": [],
    }

    if operation == "spatial_filtering":
        if has_native_geo:
            # Native geometry stats are used automatically - no warning needed
            if verbose:
                debug("Using native Parquet geometry statistics for spatial filtering")
        elif not has_bbox:
            # 1.x without bbox - warn and suggest options
            result["needs_warning"] = True
            result["message"] = "No bbox column found"
            result["suggestions"] = [
                "Add a bbox column: gpio add bbox <file>",
                "Or upgrade to GeoParquet 2.0: gpio convert <file> --geoparquet-version 2.0",
            ]

    elif operation == "bounds_calculation":
        # bbox column is still faster for bounds/centroid calculation (pre-computed values)
        if not has_bbox:
            result["needs_warning"] = True
            result["message"] = "No bbox column - computing from geometry (slower)"
            result["suggestions"] = [
                "Add a bbox column for 3-4x faster bounds/centroid: gpio add bbox <file>"
            ]

    elif operation == "check":
        if has_native_geo:
            # Native geometry - bbox optional but can help with bounds queries
            if not has_bbox and verbose:
                debug("Native geometry type detected - bbox column optional for spatial queries")
        elif not has_bbox:
            # 1.x without bbox
            result["needs_warning"] = True
            result["message"] = "No bbox column found"
            result["suggestions"] = [
                "Add a bbox column: gpio add bbox <file>",
                "Or upgrade to GeoParquet 2.0: gpio convert <file> --geoparquet-version 2.0",
            ]

    return result
