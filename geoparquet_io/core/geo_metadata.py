"""
GeoParquet metadata handling functions.

This module provides functions for parsing, creating, and applying GeoParquet
metadata to Arrow tables and Parquet files. It handles both GeoParquet 1.x and
2.0 formats, including bbox covering metadata and native geometry types.

Usage in core modules:
    from geoparquet_io.core.geo_metadata import (
        parse_geo_metadata,
        create_geo_metadata,
    )

Note: This module uses lazy imports for functions from other core modules
to avoid circular dependencies.
"""

from __future__ import annotations

import copy
import json
from typing import TYPE_CHECKING

import duckdb

from geoparquet_io.core.duckdb_utils import (
    _geoarrow_coord_exprs,
    _get_query_column_type,
    quote_identifier,
)
from geoparquet_io.core.logging_config import debug, warn

if TYPE_CHECKING:
    import pyarrow as pa

# =============================================================================
# GeoParquet Version Configuration
# =============================================================================

GEOPARQUET_VERSIONS = {
    "1.0": {"duckdb_param": "V1", "metadata_version": "1.0.0", "rewrite_metadata": True},
    "1.1": {"duckdb_param": "V1", "metadata_version": "1.1.0", "rewrite_metadata": True},
    "1.1-geoarrow": {"duckdb_param": "V1", "metadata_version": "1.1.0", "rewrite_metadata": True},
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


#: Per-column geo metadata keys that are derived from the data itself and are
#: therefore invalidated by anything that changes which rows/coordinates are
#: written (row filters, reprojection, per-partition splits, multi-file merges).
DERIVED_STAT_KEYS = ("bbox", "geometry_types")

#: Sentinel returned by a rewrite callback to mean "drop the geo key entirely".
_DROP_GEO = object()


def _decode_geo_value(raw):
    """Decode a KV ``geo`` value (bytes/str/dict) to a dict, or ``None``."""
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            raw = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def _encode_geo_value(geo_dict: dict, like):
    """Re-encode ``geo_dict`` in the same form (bytes/str/dict) as ``like``."""
    if isinstance(like, bytes):
        return json.dumps(geo_dict).encode("utf-8")
    if isinstance(like, str):
        return json.dumps(geo_dict)
    return geo_dict


def _rewrite_geo_metadata(metadata: dict | None, rewrite) -> dict | None:
    """Return a deep copy of KV ``metadata`` with ``geo`` passed through ``rewrite``.

    Handles both the ``"geo"`` and ``b"geo"`` keys, hands ``rewrite`` a mutable
    decoded dict, and re-encodes the result in whichever form the value arrived
    in. A ``rewrite`` returning :data:`_DROP_GEO` removes the key; an
    unparsable value is left untouched. The input is never mutated.
    """
    if not metadata:
        return metadata

    result = copy.deepcopy(metadata)
    for geo_key in ("geo", b"geo"):
        if geo_key not in result:
            continue
        raw = result[geo_key]
        geo_dict = _decode_geo_value(raw)
        if geo_dict is None:
            continue
        rewritten = rewrite(geo_dict)
        if rewritten is _DROP_GEO:
            del result[geo_key]
        else:
            result[geo_key] = _encode_geo_value(rewritten, raw)
    return result


def _drop_derived_stats(geo_dict: dict) -> dict:
    """Remove :data:`DERIVED_STAT_KEYS` from every column entry, in place."""
    for col_meta in (geo_dict.get("columns") or {}).values():
        if isinstance(col_meta, dict):
            for key in DERIVED_STAT_KEYS:
                col_meta.pop(key, None)
    return geo_dict


def strip_derived_stats(metadata: dict | None) -> dict | None:
    """Return a copy of Parquet KV ``metadata`` without derived geo stats.

    Drops the per-column ``bbox`` and ``geometry_types`` (see
    :data:`DERIVED_STAT_KEYS`) from the ``geo`` metadata so the write machinery
    recomputes them from the data actually written — or omits them when there is
    nothing to describe (both are optional per spec for an empty result).

    Callers are anything that changes which rows or coordinates land in the
    output: row filters (``extract``), coordinate transforms (``reproject``),
    per-partition splits, and multi-file merges whose carried metadata came from
    only the first input file.

    Both ``"geo"`` and ``b"geo"`` keys are handled, and the value is returned in
    the same form (``bytes``/``str``/``dict``) it arrived in. The input is never
    mutated; unparsable ``geo`` values are passed through untouched.
    """
    return _rewrite_geo_metadata(metadata, _drop_derived_stats)


def _covering_column(covering_entry) -> str | None:
    """Return the data column a single ``covering`` entry points at, if any."""
    if not isinstance(covering_entry, dict):
        return None
    # Spatial-index coverings (h3/s2/a5/quadkey): {"column": name, ...}
    column = covering_entry.get("column")
    if isinstance(column, str):
        return column
    # bbox covering: {"xmin": [column, "xmin"], ...}
    for ref in covering_entry.values():
        if isinstance(ref, (list, tuple)) and ref and isinstance(ref[0], str):
            return ref[0]
    return None


def _prune_coverings(col_meta, columns: set[str]) -> None:
    """Drop ``covering`` entries pointing at columns not in ``columns``."""
    covering = col_meta.get("covering") if isinstance(col_meta, dict) else None
    if not isinstance(covering, dict):
        return
    for key in [k for k, v in covering.items() if _covering_column(v) not in columns]:
        del covering[key]
    if not covering:
        del col_meta["covering"]


def _prune_geo_dict_to_columns(geo_dict: dict, columns: set[str]):
    """Drop column entries and coverings that reference absent columns.

    Returns :data:`_DROP_GEO` when the primary geometry column itself is gone,
    meaning the file must not advertise ``geo`` metadata at all.
    """
    col_entries = geo_dict.get("columns")
    if not isinstance(col_entries, dict):
        return geo_dict

    for name in [n for n in col_entries if n not in columns]:
        del col_entries[name]

    primary = geo_dict.get("primary_column")
    if primary is not None and primary not in col_entries:
        return _DROP_GEO

    for col_meta in col_entries.values():
        _prune_coverings(col_meta, columns)
    return geo_dict


def prune_geo_metadata_to_columns(metadata: dict | None, columns: list[str]) -> dict | None:
    """Return a copy of KV ``metadata`` with references to absent columns removed.

    A column projection (``gpio extract --exclude-cols``) can remove the bbox
    column a ``covering`` points at, or a secondary geometry column, leaving geo
    metadata that references a schema root that no longer exists — which readers
    and ``gpio check spec`` both reject. Entries for columns missing from
    ``columns`` are dropped; if the primary geometry column is among them the
    whole ``geo`` key is dropped, since the output is no longer GeoParquet.
    """
    present = set(columns)
    return _rewrite_geo_metadata(metadata, lambda geo: _prune_geo_dict_to_columns(geo, present))


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

    quoted_geom = quote_identifier(geometry_column)

    # GeoArrow native types (STRUCT(x DOUBLE, y DOUBLE)[N]) cannot be passed to
    # ST_XMin directly. Detect at runtime and use UNNEST to extract coordinates.
    col_type = _get_query_column_type(con, query, geometry_column) or ""
    if "STRUCT" in col_type:
        # bracket_depth = col_type.count("[]"): 0=point, 1=linestring/multipoint,
        # 2=polygon/multilinestring, 3=multipolygon. Maps directly to _GEOARROW_FLATTEN_DEPTH
        # (flatten_count = bracket_depth - 1 for non-point).
        _depth_to_encoding = {0: "point", 1: "linestring", 2: "polygon", 3: "multipolygon"}
        enc = _depth_to_encoding.get(col_type.count("[]"), "linestring")
        xmin_e, ymin_e, xmax_e, ymax_e, _, _ = _geoarrow_coord_exprs(quoted_geom, enc)
        bbox_query = f"""
            SELECT
                MIN({xmin_e}) as xmin,
                MIN({ymin_e}) as ymin,
                MAX({xmax_e}) as xmax,
                MAX({ymax_e}) as ymax
            FROM ({query})
            WHERE NOT isnan({xmax_e}) AND NOT isnan({ymax_e})
        """
    else:
        bbox_query = f"""
            SELECT
                MIN(ST_XMin({quoted_geom})) as xmin,
                MIN(ST_YMin({quoted_geom})) as ymin,
                MAX(ST_XMax({quoted_geom})) as xmax,
                MAX(ST_YMax({quoted_geom})) as ymax
            FROM ({query})
        """
    result = con.execute(bbox_query).fetchone()

    if result and all(v is not None for v in result):
        return list(result)
    return None


def _fold_geo_stat_rows(rows) -> tuple[list[float] | None, list[str]]:
    """Fold ``(geom_type, xmin, ymin, xmax, ymax)`` rows into ``(bbox, types)``."""
    from geoparquet_io.core.common import _DUCKDB_TO_SPEC_TYPE, split_zm_suffix

    types: set[str] = set()
    extents: list[tuple[float, float, float, float]] = []
    for geom_type, xmin, ymin, xmax, ymax in rows:
        if geom_type:
            base, suffix = split_zm_suffix(geom_type)
            types.add(_DUCKDB_TO_SPEC_TYPE.get(base.upper(), base) + suffix)
        if None not in (xmin, ymin, xmax, ymax):
            extents.append((xmin, ymin, xmax, ymax))

    if not extents:
        return None, sorted(types)
    bbox = [
        min(e[0] for e in extents),
        min(e[1] for e in extents),
        max(e[2] for e in extents),
        max(e[3] for e in extents),
    ]
    return bbox, sorted(types)


def _geo_stats_unsupported(con, query: str, geometry_column: str) -> bool:
    """True when the combined per-type aggregation cannot run on this column.

    GeoArrow native types (``STRUCT(x DOUBLE, y DOUBLE)[N]``) support neither
    ``ST_GeometryType`` nor the shared aggregation, and a column that is not in
    the query result obviously cannot be aggregated. Both cases fall back to the
    single-stat helpers, which already handle them.
    """
    try:
        col_type = _get_query_column_type(con, query, geometry_column) or ""
        if "STRUCT" in col_type:
            return True
        return geometry_column not in _get_query_columns(con, query)
    except (duckdb.Error, RuntimeError, ValueError, AttributeError):
        return True


def compute_geo_stats_via_sql(
    con,
    query: str,
    geometry_column: str,
    need_bbox: bool = True,
    need_geometry_types: bool = True,
) -> tuple[list[float] | None, list[str]]:
    """Compute ``bbox`` and ``geometry_types`` in a SINGLE scan of ``query``.

    Both stats are aggregates over the same rows, so grouping by geometry type
    yields one small row per type carrying that type's extent — the union of
    which is the collection bbox. That replaces the two independent full scans
    the write strategies used to run, which matters because invalidating a
    carried bbox forces the (possibly expensive, e.g. ``ST_Transform``) query to
    be re-executed for it.

    Args:
        con: DuckDB connection with spatial extension loaded
        query: SQL query containing the geometry column
        geometry_column: Name of the geometry column
        need_bbox: Compute the bbox (``False`` returns ``None`` for it)
        need_geometry_types: Compute geometry types (``False`` returns ``[]``)

    Returns:
        ``(bbox_or_None, geometry_types)``
    """
    from geoparquet_io.core.common import compute_geometry_types_via_sql, zm_suffix_sql

    def _separately() -> tuple[list[float] | None, list[str]]:
        return (
            compute_bbox_via_sql(con, query, geometry_column) if need_bbox else None,
            compute_geometry_types_via_sql(con, query, geometry_column)
            if need_geometry_types
            else [],
        )

    if not (need_bbox and need_geometry_types):
        return _separately()
    if _geo_stats_unsupported(con, query, geometry_column):
        return _separately()

    quoted = '"{}"'.format(geometry_column.replace('"', '""'))
    stats_query = f"""
        SELECT
            ST_GeometryType({quoted}) || {zm_suffix_sql(quoted)} AS geom_type,
            MIN(ST_XMin({quoted})) AS xmin,
            MIN(ST_YMin({quoted})) AS ymin,
            MAX(ST_XMax({quoted})) AS xmax,
            MAX(ST_YMax({quoted})) AS ymax
        FROM ({query})
        WHERE {quoted} IS NOT NULL
        GROUP BY 1
    """
    return _fold_geo_stat_rows(con.execute(stats_query).fetchall())


def compute_geometry_types_via_sql(
    con,
    query: str,
    geometry_column: str,
) -> list[str]:
    """
    Compute distinct geometry types from query using DuckDB.

    Delegates to the canonical dimension-aware implementation in
    ``geoparquet_io.core.common`` (lazy import: common imports this module
    at top level, so a module-level import here would be circular).

    Returns:
        List of spec geometry type names with dimension suffixes
        (e.g., ["Point", "LineString ZM"]) or empty list if column not in query
    """
    from geoparquet_io.core.common import compute_geometry_types_via_sql as _impl

    return _impl(con, query, geometry_column)


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
