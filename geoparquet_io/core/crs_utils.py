"""
CRS (Coordinate Reference System) utilities for GeoParquet files.

This module provides functions for extracting, parsing, and validating
CRS information from GeoParquet files and other spatial formats.
"""

import json
import os
from functools import lru_cache

from geoparquet_io.core.duckdb_utils import _escape_sql_string
from geoparquet_io.core.logging_config import debug, warn


def _extract_crs_identifier(crs_info):
    """
    Extract normalized CRS identifier (authority, code) from various formats.

    Handles PROJJSON dicts, "EPSG:CODE" strings, and URN formats.
    Returns tuple of (authority, code) like ("EPSG", 31287) or ("OGC", "CRS84"), or None.
    Code is int for numeric codes, str for non-numeric (e.g., CRS84).
    """
    if isinstance(crs_info, dict):
        if "id" in crs_info:
            crs_id = crs_info["id"]
            if isinstance(crs_id, dict):
                authority = crs_id.get("authority", "").upper()
                code = crs_id.get("code")
                if authority and code:
                    try:
                        return (authority, int(code))
                    except (ValueError, TypeError):
                        return (authority, str(code).upper())
        return None

    if isinstance(crs_info, str):
        crs_str = crs_info.strip().upper()
        if ":" in crs_str and not crs_str.startswith("URN:"):
            parts = crs_str.split(":")
            if len(parts) == 2:
                try:
                    return (parts[0], int(parts[1]))
                except ValueError:
                    return (parts[0], parts[1])
        if crs_str.startswith("URN:OGC:DEF:CRS:"):
            parts = crs_str.split(":")
            if len(parts) >= 7:
                try:
                    return (parts[4], int(parts[-1]))
                except ValueError:
                    return (parts[4], parts[-1])

    return None


def is_default_crs(crs):
    """
    Check if CRS is the default (OGC:CRS84 or EPSG:4326).

    Returns True if CRS is None, empty, or represents WGS84.
    Used to skip CRS rewriting when output would be default anyway.
    """
    if not crs:
        return True

    identifier = _extract_crs_identifier(crs)
    if identifier:
        authority, code = identifier
        if authority == "EPSG" and code == 4326:
            return True
        if authority == "OGC" and str(code).upper() == "CRS84":
            return True

    return False


def apply_output_crs(col_meta: dict, input_crs) -> None:
    """Set or clear a geometry column's ``crs`` for the requested output CRS.

    Single source of truth for the GeoParquet null-vs-default CRS rule across all
    write paths. The default CRS (OGC:CRS84 / EPSG:4326) is signalled by
    *omitting* the ``crs`` key — never an explicit ``crs: null`` or ``crs: 4326``.

    - ``input_crs`` non-default -> write it explicitly.
    - ``input_crs`` default -> drop ``crs`` (output is the spec default), so a
      stale value carried from the source (e.g. EPSG:3857 after reprojecting to
      4326, or an explicit ``crs: null``) is not written through.
    - ``input_crs`` is ``None`` (CRS unchanged) -> preserve a real ``crs`` but
      still strip a stray default/null one the source or DuckDB may have attached.

    Mutates ``col_meta`` in place.
    """
    if input_crs and not is_default_crs(input_crs):
        col_meta["crs"] = input_crs
    elif input_crs:
        col_meta.pop("crs", None)
    elif is_default_crs(col_meta.get("crs")):
        col_meta.pop("crs", None)


#: Shared guidance appended to null-CRS warnings and the validate message.
NULL_CRS_HINT = (
    "An explicit null CRS means the CRS is *unknown* (not the OGC:CRS84 default). "
    "If the coordinates are really lon/lat WGS84, run "
    "`gpio convert reproject <input> <output> --assume-crs84` to set the default."
)

#: Raised by the file/streaming reproject paths when DuckDB hits a ``crs: null``
#: input without ``--assume-crs84`` (DuckDB's GeoParquet reader rejects it).
NULL_CRS_NO_FLAG_ERROR = (
    "Input has an explicit null CRS (unknown). DuckDB cannot read it. "
    "If the coordinates are lon/lat WGS84, re-run with --assume-crs84 to "
    "treat them as OGC:CRS84 and write the default."
)


def crs_is_explicitly_null(col_meta: dict) -> bool:
    """Return True only when a geometry column's metadata sets ``crs`` to null.

    An explicit ``"crs": null`` means the CRS is unknown, which is different
    from omitting the key entirely (the omitted case defaults to OGC:CRS84).
    """
    return isinstance(col_meta, dict) and "crs" in col_meta and col_meta["crs"] is None


def geoparquet_crs_is_null(parquet_file) -> bool:
    """Return True if the primary geometry column has an explicit ``crs: null``.

    ``parquet_file`` must be a *raw* path/URL — this helper SQL-escapes it
    internally, so passing an already-escaped URL double-escapes it.
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata
    from geoparquet_io.core.file_utils import safe_file_url

    safe_url = safe_file_url(str(parquet_file), verbose=False)
    geo_meta = get_geo_metadata(safe_url)
    if not geo_meta:
        return False
    primary_col = geo_meta.get("primary_column", "geometry")
    col_meta = geo_meta.get("columns", {}).get(primary_col, {})
    return crs_is_explicitly_null(col_meta)


@lru_cache(maxsize=256)
def _emit_null_crs_warning(key: str) -> None:
    """Emit the null-CRS warning exactly once per ``key`` (LRU-bounded)."""
    warn(f"Input has an explicit null CRS (unknown). {NULL_CRS_HINT}")


def warn_null_crs_once(key: str) -> None:
    """Emit the null-CRS warning at most once per ``key`` for this process.

    Dedup is bounded by an LRU cache so long-running processes (the Python API)
    don't accumulate keys without limit.
    """
    if key:
        _emit_null_crs_warning(key)


def reset_null_crs_warnings() -> None:
    """Clear the warn-once dedup cache. Intended for tests."""
    _emit_null_crs_warning.cache_clear()


def apply_target_crs_to_geo_meta(geo_meta: dict, geom_col: str, target_crs: str, con) -> None:
    """Set or clear a geometry column's CRS in ``geo_meta`` in place.

    The GeoParquet default CRS (EPSG:4326 / OGC:CRS84) is signalled by *omitting*
    the ``crs`` key entirely — never by writing an explicit CRS84 object or null.
    """
    columns = geo_meta.get("columns", {})
    if geom_col not in columns:
        return
    if is_default_crs(target_crs):
        columns[geom_col].pop("crs", None)
    else:
        columns[geom_col]["crs"] = parse_crs_string_to_projjson(target_crs, con)


def _validate_projjson(crs: dict) -> bool:
    """Validate that a CRS dict has the expected PROJJSON structure."""
    if not isinstance(crs, dict):
        return False
    if "$schema" not in crs and "type" not in crs and "id" not in crs:
        return False
    return True


def _wrap_query_with_crs(
    query: str,
    geometry_column: str | None,
    input_crs: dict | None,
) -> str:
    """Wrap query with ST_SetCRS() so DuckDB writes CRS into the Parquet schema natively."""
    if not input_crs or is_default_crs(input_crs):
        return query

    if not geometry_column:
        raise ValueError(
            "geometry_column is required when input_crs is specified — "
            "cannot apply CRS without a geometry column"
        )

    if not _validate_projjson(input_crs):
        warn("input_crs does not look like valid PROJJSON — skipping CRS application")
        return query

    escaped_geom = geometry_column.replace('"', '""')
    crs_json = _escape_sql_string(json.dumps(input_crs))
    return f"""
        SELECT * REPLACE (ST_SetCRS("{escaped_geom}", '{crs_json}') AS "{escaped_geom}")
        FROM ({query})
    """


#: Default target CRS for lon/lat operations (grid keying, admin joins). The
#: GeoParquet default; lon/lat axis order is guaranteed by the session-level
#: ``geometry_always_xy = true`` that ``get_duckdb_connection`` sets.
WGS84_TRANSFORM_TARGET = "OGC:CRS84"


def resolve_crs_to_string(crs_info) -> str | None:
    """Resolve CRS info (PROJJSON dict or string) to a CRS string for ST_Transform.

    Tries the authority identifier first, then pyproj resolution, then the raw
    PROJJSON. Returns None if ``crs_info`` is falsy or unresolvable.
    """
    if not crs_info:
        return None

    identifier = _extract_crs_identifier(crs_info)
    if identifier:
        authority, code = identifier
        return f"{authority}:{code}"

    if isinstance(crs_info, dict):
        try:
            from pyproj import CRS

            authority = CRS.from_json_dict(crs_info).to_authority()
            if authority:
                return f"{authority[0]}:{authority[1]}"
        except Exception:
            pass
        return json.dumps(crs_info)

    return None


def crs_transform_sql_expr(
    geom_sql: str,
    source_crs,
    target_crs: str = WGS84_TRANSFORM_TARGET,
) -> str:
    """Return a SQL expression yielding ``geom_sql`` in ``target_crs``.

    Wraps ``geom_sql`` in ``ST_Transform(..., '<src>', '<target>')`` only when
    the source CRS is known and differs from the target; otherwise returns
    ``geom_sql`` unchanged. This is the single source of truth for making the
    CRS-blind spatial operations (grid keying, admin joins) CRS-aware.

    The rules mirror the GeoParquet/DuckDB contract:

    - A missing/``None`` or default (OGC:CRS84 / EPSG:4326) source is treated as
      already being the target — no transform. A CRS-less geometry (e.g. from an
      in-memory Arrow table via ``ST_GeomFromWKB``) is therefore accepted as-is,
      and ``ST_Intersects`` accepts a CRS-less geometry against a CRS-bearing one.
    - An unresolvable source CRS is left untransformed rather than guessed.

    Relies on the session-level ``geometry_always_xy = true`` so the transformed
    coordinates come out as lon/lat (x/y) for the ``*_lonlat_to_cell`` keying.

    Args:
        geom_sql: A SQL geometry expression (e.g. a quoted column name or
            ``ST_Centroid("geometry")``).
        source_crs: The source CRS as a PROJJSON dict, an ``AUTH:CODE`` string,
            or ``None``.
        target_crs: The target CRS string (default OGC:CRS84).
    """
    if not source_crs or is_default_crs(source_crs):
        return geom_sql

    src = resolve_crs_to_string(source_crs)
    if not src:
        return geom_sql

    src_literal = _escape_sql_string(src)
    target_literal = _escape_sql_string(target_crs)
    return f"ST_Transform({geom_sql}, '{src_literal}', '{target_literal}')"


def extract_crs_from_table(table, geometry_column: str | None = None):
    """Return the CRS of ``geometry_column`` from a pyarrow table's geo metadata.

    Returns the CRS value (PROJJSON dict or string) when present and non-default,
    else ``None``. ``geometry_column`` defaults to the geo metadata's declared
    primary column. Used by the table-centric (Python API) operations to detect
    a projected input before grid keying.
    """
    metadata = table.schema.metadata
    if not metadata or b"geo" not in metadata:
        return None
    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    columns = geo_meta.get("columns", {})
    if not isinstance(columns, dict):
        return None
    col = geometry_column or geo_meta.get("primary_column", "geometry")
    crs = columns.get(col, {}).get("crs")
    if crs and not is_default_crs(crs):
        return crs
    return None


def extract_crs_from_parquet(parquet_file, verbose=False):
    """
    Extract CRS (as PROJJSON dict) from a Parquet file.

    Checks in order:
    1. GeoParquet metadata (columns.<geom_col>.crs)
    2. Parquet native geo type (from schema logical_type)
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_geo_metadata,
        get_schema_info,
        parse_geometry_logical_type,
        resolve_crs_reference,
    )
    from geoparquet_io.core.file_utils import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)

    geo_meta = get_geo_metadata(safe_url)
    if geo_meta:
        primary_col = geo_meta.get("primary_column", "geometry")
        columns = geo_meta.get("columns", {})
        if primary_col in columns:
            if crs_is_explicitly_null(columns[primary_col]):
                warn_null_crs_once(safe_url)
            crs = columns[primary_col].get("crs")
            if crs and not is_default_crs(crs):
                if verbose:
                    debug(f"Found CRS in GeoParquet metadata: {_format_crs_display(crs)}")
                return crs

    schema_info = get_schema_info(safe_url)
    for col in schema_info:
        logical_type = col.get("logical_type") or ""
        if logical_type and (
            logical_type.startswith("GeometryType(") or logical_type.startswith("GeographyType(")
        ):
            parsed = parse_geometry_logical_type(logical_type)
            if parsed and "crs" in parsed:
                raw_crs = parsed["crs"]
                crs = resolve_crs_reference(parquet_file, raw_crs)
                if crs and not is_default_crs(crs):
                    if verbose:
                        debug(f"Found CRS in Parquet geo type: {_format_crs_display(crs)}")
                    return crs

    return None


def _detect_crs_from_filegdb(gdb_path, con, verbose=False):
    """Detect CRS from a FileGDB directory by iterating internal .gdbtable files."""
    gdb_path = gdb_path.rstrip("/\\")

    if not os.path.isdir(gdb_path):
        return None

    try:
        gdbtable_files = sorted(
            [f for f in os.listdir(gdb_path) if f.endswith(".gdbtable")],
            reverse=True,
        )
    except OSError:
        return None

    for gdbtable_file in gdbtable_files:
        gdbtable_path = os.path.join(gdb_path, gdbtable_file)
        escaped_path = _escape_sql_string(gdbtable_path)
        try:
            result = con.execute(f"""
                SELECT * FROM ST_Read_Meta('{escaped_path}')
            """).fetchone()

            if not result or not result[3]:
                continue

            for layer in result[3]:
                layer_name = layer.get("name", "")
                if layer_name.startswith("GDB_"):
                    continue

                geometry_fields = layer.get("geometry_fields", [])
                if not geometry_fields:
                    continue

                crs_info = geometry_fields[0].get("crs", {})

                projjson_str = crs_info.get("projjson")
                if projjson_str:
                    crs = json.loads(projjson_str)
                    if verbose:
                        debug(
                            f"Found CRS in FileGDB layer '{layer_name}': {_format_crs_display(crs)}"
                        )
                    return crs

                auth_name = crs_info.get("auth_name")
                auth_code = crs_info.get("auth_code")
                if auth_name and auth_code:
                    crs = {"id": {"authority": auth_name, "code": int(auth_code)}}
                    if verbose:
                        debug(f"Found CRS in FileGDB layer '{layer_name}': {auth_name}:{auth_code}")
                    return crs

        except Exception:
            continue

    return None


def detect_crs_from_spatial_file(input_file, con, verbose=False):
    """Detect CRS from a spatial file (GeoJSON, GPKG, Shapefile, FileGDB)."""
    escaped_input_file = _escape_sql_string(input_file)
    try:
        result = con.execute(f"""
            SELECT * FROM ST_Read_Meta('{escaped_input_file}')
        """).fetchone()

        if result:
            layers = result[3]
            if layers and len(layers) > 0:
                layer = layers[0]
                geometry_fields = layer.get("geometry_fields", [])
                if geometry_fields:
                    crs_info = geometry_fields[0].get("crs", {})
                    projjson_str = crs_info.get("projjson")
                    if projjson_str:
                        crs = json.loads(projjson_str)
                        if verbose:
                            debug(f"Found CRS in spatial file: {_format_crs_display(crs)}")
                        return crs
                    auth_name = crs_info.get("auth_name")
                    auth_code = crs_info.get("auth_code")
                    if auth_name and auth_code:
                        crs = {"id": {"authority": auth_name, "code": int(auth_code)}}
                        if verbose:
                            debug(f"Found CRS: {auth_name}:{auth_code}")
                        return crs
    except Exception as e:
        if verbose:
            warn(f"Could not detect CRS from spatial file: {e}")

    if input_file.rstrip("/\\").lower().endswith(".gdb"):
        if verbose:
            debug("ST_Read_Meta returned empty for FileGDB, trying workaround...")
        return _detect_crs_from_filegdb(input_file, con, verbose)

    return None


def _format_crs_display(crs):
    """Format CRS for display (extract EPSG code if possible)."""
    if not crs:
        return "None"
    identifier = _extract_crs_identifier(crs)
    if identifier:
        return f"{identifier[0]}:{identifier[1]}"
    return str(crs)[:50] + "..." if len(str(crs)) > 50 else str(crs)


def get_crs_display_name(crs_info: dict | str | None) -> str:
    """Get human-readable CRS name with authority code."""
    if crs_info is None:
        return "None (OGC:CRS84)"

    if isinstance(crs_info, str):
        return crs_info

    if isinstance(crs_info, dict):
        name = crs_info.get("name", "")
        crs_id = crs_info.get("id", {})
        if isinstance(crs_id, dict):
            authority = crs_id.get("authority", "EPSG")
            code = crs_id.get("code")
            if code:
                return f"{name} ({authority}:{code})" if name else f"{authority}:{code}"
        if name:
            return name
        return "PROJJSON object"

    return "unknown"


def is_geographic_crs(crs: dict | str | None) -> bool:
    """Check if CRS is geographic (lat/lon) vs projected."""
    if crs is None:
        return True

    if isinstance(crs, dict):
        crs_type = crs.get("type", "").lower()
        if crs_type == "geographiccrs":
            return True
        if crs_type == "projectedcrs":
            return False

        crs_id = crs.get("id", {})
        if isinstance(crs_id, dict):
            authority = crs_id.get("authority", "").upper()
            code = crs_id.get("code")
            if authority == "EPSG" and code == 4326:
                return True
            if authority == "OGC" and str(code).upper() == "CRS84":
                return True

        name = crs.get("name", "").upper()
        projected_indicators = ["UTM", "ZONE", "MERCATOR", "ALBERS", "LAMBERT", "STATE PLANE"]
        if any(indicator in name for indicator in projected_indicators):
            return False
        if any(x in name for x in ["WGS 84", "WGS84", "CRS84", "4326"]):
            return True

    if isinstance(crs, str):
        crs_upper = crs.upper()
        projected_indicators = ["UTM", "ZONE", "MERCATOR", "ALBERS", "LAMBERT"]
        if any(indicator in crs_upper for indicator in projected_indicators):
            return False
        return any(x in crs_upper for x in ["4326", "CRS84", "WGS84"])

    return False


def parse_crs_string_to_projjson(crs_string, con=None):
    """Convert a CRS string (like "EPSG:5070") to full PROJJSON dict."""
    identifier = _extract_crs_identifier(crs_string)
    if not identifier:
        return None

    authority, code = identifier

    try:
        from pyproj import CRS

        crs = CRS.from_authority(authority, code)
        return crs.to_json_dict()
    except Exception:
        return {"id": {"authority": authority, "code": code}}
