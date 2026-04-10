"""
CRS (Coordinate Reference System) utilities for GeoParquet files.

This module provides functions for extracting, parsing, and validating
CRS information from GeoParquet files and other spatial formats.
"""

import json
import os

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
            crs = columns[primary_col].get("crs")
            if crs and not is_default_crs(crs):
                if verbose:
                    debug(f"Found CRS in GeoParquet metadata: {_format_crs_display(crs)}")
                return crs

    schema_info = get_schema_info(safe_url)
    for col in schema_info:
        logical_type = col.get("logical_type", "")
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
