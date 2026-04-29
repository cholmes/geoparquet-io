"""
WFS (Web Feature Service) to GeoParquet conversion.

This module provides functionality to download features from OGC WFS services
and convert them to GeoParquet format. Supports WFS 1.0.0, 1.1.0, and 2.0.0.

Key features:
- DuckDB-native HTTP streaming for fast extraction (10x+ faster than Python HTTP)
- Server-side bbox filtering
- CRS negotiation with EPSG variant handling
- Hilbert curve sorting and bbox column generation
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import pyarrow as pa

# Public API
__all__ = [
    "WFSError",
    "WFSLayerInfo",
    "WFS_VERSIONS",
    "convert_wfs_to_geoparquet",
    "get_layer_info",
    "get_wfs_capabilities",
    "list_available_layers",
    "negotiate_wfs_version",
    "wfs_to_table",
]

from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.crs_utils import parse_crs_string_to_projjson
from geoparquet_io.core.duckdb_utils import _escape_sql_string, get_duckdb_connection
from geoparquet_io.core.http_retry import (
    get_shared_http_client as _get_shared_http_client_base,
)
from geoparquet_io.core.http_retry import (
    reset_http_client as _reset_http_client_base,
)
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.reproject import reproject_table


class WFSError(Exception):
    """Exception raised for WFS-related errors."""

    pass


# GeoJSON output format identifiers (in preference order)
GEOJSON_FORMATS = [
    "application/json",
    "json",
    "geojson",
    "application/geo+json",
    "application/vnd.geo+json",
]

# GML output format identifiers (fallback, in preference order)
GML_FORMATS = [
    "gml3",
    "text/xml; subtype=gml/3.1.1",
    "application/gml+xml; version=3.1",
    "gml32",
    "text/xml; subtype=gml/3.2",
    "gml2",
    "text/xml; subtype=gml/2.1.2",
]


@dataclass
class WFSLayerInfo:
    """WFS layer/feature type metadata."""

    typename: str
    title: str | None
    crs_list: list[str]
    default_crs: str | None
    bbox: tuple[float, float, float, float] | None
    geometry_column: str
    available_formats: list[str]


# Default timeout for HTTP requests (seconds)
DEFAULT_TIMEOUT = 60.0


def _get_shared_http_client(timeout: float = DEFAULT_TIMEOUT):
    """Get shared HTTP client from http_retry module."""
    return _get_shared_http_client_base(timeout=timeout)


def _reset_http_client():
    """Reset shared HTTP client from http_retry module."""
    _reset_http_client_base()


def _make_request(
    url: str,
    params: dict | None = None,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    accept: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> bytes:
    """
    Make HTTP GET request with retry logic.

    Returns raw bytes to handle both JSON and XML responses.

    Args:
        url: Request URL
        params: Query parameters
        max_retries: Number of retry attempts
        retry_delay: Base delay between retries (exponential backoff)
        accept: Accept header value (e.g., "application/json")
        timeout: Request timeout in seconds

    Returns:
        Response content as bytes
    """
    import httpx

    last_exception: Exception | None = None

    headers = {
        "Accept-Encoding": "gzip, deflate",
    }
    if accept:
        headers["Accept"] = accept

    # Build full URL for logging
    request_desc = url
    if params:
        param_summary = ", ".join(f"{k}={v}" for k, v in list(params.items())[:5])
        if len(params) > 5:
            param_summary += f", ... ({len(params)} params)"
        request_desc = f"{url}?{param_summary}"

    for attempt in range(max_retries):
        try:
            debug(f"HTTP GET: {request_desc[:100]}{'...' if len(request_desc) > 100 else ''}")
            start_time = time.time()
            client = _get_shared_http_client(timeout=timeout)
            response = client.get(url, params=params, headers=headers)
            elapsed = time.time() - start_time
            response.raise_for_status()
            content = bytes(response.content)
            debug(f"HTTP OK: {len(content):,} bytes in {elapsed:.1f}s")
            return content
        except httpx.RemoteProtocolError as e:
            last_exception = e
            warn(f"HTTP protocol error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                _reset_http_client()
                time.sleep(retry_delay * (attempt + 1))
        except httpx.TimeoutException as e:
            last_exception = e
            warn(f"HTTP timeout after {timeout}s (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
        except httpx.NetworkError as e:
            last_exception = e
            warn(f"HTTP network error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 429 or (500 <= status < 600):
                last_exception = e
                warn(f"HTTP {status} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    retry_after = e.response.headers.get("Retry-After")
                    delay = (
                        float(retry_after)
                        if retry_after and retry_after.isdigit()
                        else retry_delay * (attempt + 1)
                    )
                    time.sleep(delay)
                    continue
            elif status == 401:
                raise WFSError("Authentication required. WFS server requires credentials.") from e
            elif status == 403:
                raise WFSError("Access denied. Check your permissions for this WFS service.") from e
            elif status == 404:
                raise WFSError(f"WFS service not found (404). Check the URL: {url}") from e
            raise WFSError(f"HTTP error {status}: {e}") from e

    raise WFSError(f"Request failed after {max_retries} attempts: {last_exception}")


def _clean_service_url(url: str) -> str:
    """
    Clean WFS service URL by removing GetCapabilities parameters.

    Some URLs come with ?service=WFS&request=GetCapabilities which
    interferes with subsequent requests.

    Args:
        url: Input URL (may include query parameters)

    Returns:
        Clean base URL for the WFS service
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)

    # Remove WFS-specific params that shouldn't persist
    for key in ["service", "request", "version", "typename", "typenames"]:
        params.pop(key, None)
        params.pop(key.upper(), None)

    # Rebuild URL
    new_query = urlencode(params, doseq=True) if params else ""
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            "",
        )
    )


def get_wfs_capabilities(service_url: str, version: str = "1.1.0"):
    """
    Get WFS capabilities using OWSLib.

    Args:
        service_url: WFS service URL
        version: WFS version (1.0.0 or 1.1.0)

    Returns:
        OWSLib WebFeatureService object
    """
    try:
        from owslib.wfs import WebFeatureService
    except ImportError as e:
        raise WFSError(
            "owslib is required for WFS extraction. Install with: pip install owslib"
        ) from e

    clean_url = _clean_service_url(service_url)

    try:
        wfs = WebFeatureService(url=clean_url, version=version)
        return wfs
    except Exception as e:
        error_msg = str(e).lower()
        if "connection" in error_msg or "timeout" in error_msg:
            raise WFSError(f"Could not connect to WFS service: {clean_url}\nError: {e}") from e
        elif "xml" in error_msg or "parse" in error_msg:
            raise WFSError(
                f"Invalid WFS response from: {clean_url}\n"
                f"The server may not be a valid WFS service. Error: {e}"
            ) from e
        else:
            raise WFSError(f"Failed to get WFS capabilities: {e}") from e


# WFS versions in preference order (newest first)
WFS_VERSIONS = ["2.0.0", "1.1.0", "1.0.0"]


def negotiate_wfs_version(service_url: str, preferred_version: str = "auto"):
    """
    Negotiate the best WFS version to use with the server.

    Args:
        service_url: WFS service URL
        preferred_version: "auto" to try all versions, or specific version

    Returns:
        Tuple of (negotiated_version, wfs_capabilities_object)

    Raises:
        WFSError: If no supported version works
    """
    if preferred_version != "auto":
        # User specified version, use it directly
        wfs = get_wfs_capabilities(service_url, preferred_version)
        return preferred_version, wfs

    # Auto-negotiate: try versions in preference order
    errors = []
    for version in WFS_VERSIONS:
        try:
            debug(f"Trying WFS version {version}...")
            wfs = get_wfs_capabilities(service_url, version)
            info(f"Using WFS version {version}")
            return version, wfs
        except WFSError as e:
            errors.append(f"  {version}: {e}")
            continue

    # All versions failed
    error_details = "\n".join(errors)
    raise WFSError(
        f"Could not connect to WFS service with any supported version.\n"
        f"Tried: {', '.join(WFS_VERSIONS)}\n"
        f"Errors:\n{error_details}"
    )


def _normalize_crs(crs: str) -> str:
    """
    Normalize CRS string to consistent EPSG format.

    Handles variants like:
    - EPSG:4326
    - urn:ogc:def:crs:EPSG::4326
    - http://www.opengis.net/def/crs/EPSG/0/4326

    Returns:
        Normalized EPSG string (e.g., "EPSG:4326")
    """
    import re

    # Already in simple format
    if re.match(r"^EPSG:\d+$", crs, re.IGNORECASE):
        return crs.upper()

    # URN format: urn:ogc:def:crs:EPSG::4326
    urn_match = re.search(r"EPSG::?(\d+)", crs, re.IGNORECASE)
    if urn_match:
        return f"EPSG:{urn_match.group(1)}"

    # HTTP format: http://www.opengis.net/def/crs/EPSG/0/4326
    http_match = re.search(r"EPSG/\d+/(\d+)", crs, re.IGNORECASE)
    if http_match:
        return f"EPSG:{http_match.group(1)}"

    # CRS84 is equivalent to EPSG:4326 (axis order differs but we handle that)
    if "CRS84" in crs.upper() or "CRS:84" in crs.upper():
        return "EPSG:4326"

    # Return as-is if no pattern matches
    return crs


def _crs_matches(crs1: str, crs2: str) -> bool:
    """Check if two CRS strings represent the same coordinate system."""
    return _normalize_crs(crs1) == _normalize_crs(crs2)


def _estimate_crs_from_bbox(
    bbox: tuple[float, float, float, float],
) -> str | None:
    """
    Estimate CRS from coordinate ranges (rough heuristic).

    This is a best-effort detection for common mismatches.
    Returns None if unable to determine.

    Args:
        bbox: (xmin, ymin, xmax, ymax)

    Returns:
        Estimated EPSG code or None
    """
    xmin, ymin, xmax, ymax = bbox

    # WGS84 / EPSG:4326: lon in [-180, 180], lat in [-90, 90]
    if -180 <= xmin <= 180 and -180 <= xmax <= 180 and -90 <= ymin <= 90 and -90 <= ymax <= 90:
        return "EPSG:4326"

    # ETRS89-LAEA / EPSG:3035: European extent roughly 2M-7M x 1M-5.5M
    # Check this BEFORE Web Mercator since it's more specific
    if 2_000_000 < xmin < 7_500_000 and 2_000_000 < xmax < 7_500_000:
        if 1_000_000 < ymin < 5_500_000 and 1_000_000 < ymax < 5_500_000:
            return "EPSG:3035"

    # Web Mercator / EPSG:3857: typically ±20M meters, centered at 0,0
    # Uses wider ranges than EPSG:3035
    if abs(xmin) > 1_000_000 or abs(xmax) > 1_000_000:
        if abs(xmin) < 21_000_000 and abs(xmax) < 21_000_000:
            if abs(ymin) < 21_000_000 and abs(ymax) < 21_000_000:
                return "EPSG:3857"

    return None


def _validate_crs_coordinates(
    table: pa.Table,
    requested_crs: str,
    strict: bool = False,
) -> tuple[bool, str | None]:
    """
    Validate that table coordinates match the requested CRS.

    Computes bbox from geometries and checks if coordinates are in expected range.

    Args:
        table: PyArrow table with geometry column
        requested_crs: CRS that was requested from WFS
        strict: If True, raise error on mismatch; if False, warn and return detected CRS

    Returns:
        Tuple of (is_valid, detected_crs_if_mismatch)
    """
    if table.num_rows == 0:
        return True, None

    try:
        # Extract bbox from geometry column using DuckDB
        # Geometry is stored as WKB, so we need ST_GeomFromWKB to convert
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            con.register("data", table)
            result = con.execute("""
                SELECT
                    MIN(ST_XMin(ST_GeomFromWKB(geometry))) as xmin,
                    MIN(ST_YMin(ST_GeomFromWKB(geometry))) as ymin,
                    MAX(ST_XMax(ST_GeomFromWKB(geometry))) as xmax,
                    MAX(ST_YMax(ST_GeomFromWKB(geometry))) as ymax
                FROM data
                WHERE geometry IS NOT NULL
            """).fetchone()

            if result is None or result[0] is None:
                return True, None

            bbox = (result[0], result[1], result[2], result[3])
        finally:
            con.close()

        # Validate coordinates match requested CRS
        normalized_crs = _normalize_crs(requested_crs)
        xmin, ymin, xmax, ymax = bbox
        detected = _estimate_crs_from_bbox(bbox)

        # Check for mismatch between requested and detected CRS
        mismatch = False
        if normalized_crs == "EPSG:4326":
            # WGS84 coordinates should be in valid range
            if abs(xmin) > 180 or abs(xmax) > 180 or abs(ymin) > 90 or abs(ymax) > 90:
                mismatch = True
        elif normalized_crs == "EPSG:3857":
            # Web Mercator should have large meter values, not small degree values
            if abs(xmin) < 1000 and abs(xmax) < 1000 and abs(ymin) < 1000 and abs(ymax) < 1000:
                mismatch = True
        elif normalized_crs == "EPSG:3035":
            # LAEA Europe should be in specific range
            if not (2_000_000 < xmin < 7_500_000 and 1_000_000 < ymin < 5_500_000):
                mismatch = True
        elif detected and detected != normalized_crs:
            # Generic check: if we detected a CRS and it differs from requested
            mismatch = True

        if mismatch:
            msg = (
                f"Coordinate mismatch: requested {requested_crs} but got bbox "
                f"[{xmin:.2f}, {ymin:.2f}, {xmax:.2f}, {ymax:.2f}]. "
            )
            if detected:
                msg += f"Coordinates look like {detected}. Server may have ignored srsName."
            else:
                msg += "Server may have returned data in a different CRS."

            if strict:
                raise WFSError(msg)
            else:
                warn(msg)
                return False, detected

        return True, None

    except WFSError:
        raise
    except Exception as e:
        debug(f"CRS validation skipped due to error: {e}")
        return True, None


def _detect_geometry_column(wfs, typename: str) -> str:
    """
    Detect geometry column name from DescribeFeatureType.

    Args:
        wfs: OWSLib WebFeatureService object
        typename: Layer typename

    Returns:
        Geometry column name (default: "geometry")
    """
    try:
        schema = wfs.get_schema(typename)
        if schema and "geometry" in schema:
            return str(schema.get("geometry_column", "geometry"))
        # Check for common geometry column names in properties
        if schema and "properties" in schema:
            for prop_name, prop_type in schema["properties"].items():
                if any(
                    geom in str(prop_type).lower()
                    for geom in ["geometry", "point", "line", "polygon", "multi"]
                ):
                    return str(prop_name)
    except Exception:
        pass  # Fall back to default

    return "geometry"


def get_layer_info(service_url: str, typename: str, version: str = "1.1.0") -> WFSLayerInfo:
    """
    Get metadata for a specific WFS layer.

    Args:
        service_url: WFS service URL
        typename: Layer typename (with or without namespace prefix)
        version: WFS version

    Returns:
        WFSLayerInfo dataclass with layer metadata
    """
    wfs = get_wfs_capabilities(service_url, version)

    # Find the layer (handle namespace variations)
    layer = None
    matched_typename = typename

    if typename in wfs.contents:
        layer = wfs.contents[typename]
    else:
        # Try without namespace prefix
        short_name = typename.split(":")[-1] if ":" in typename else typename
        for key in wfs.contents:
            if key.endswith(f":{short_name}") or key == short_name:
                layer = wfs.contents[key]
                matched_typename = key
                break

    if layer is None:
        available = list(wfs.contents.keys())[:10]
        hint = f"\nAvailable layers (first 10): {', '.join(available)}" if available else ""
        raise WFSError(f"Layer '{typename}' not found in WFS service.{hint}")

    # Extract CRS list
    crs_list = []
    default_crs = None

    if hasattr(layer, "crsOptions") and layer.crsOptions:
        crs_list = [str(crs) for crs in layer.crsOptions]
        default_crs = crs_list[0] if crs_list else None

    # Extract bounding box
    bbox = None
    if hasattr(layer, "boundingBoxWGS84") and layer.boundingBoxWGS84:
        bbox = tuple(layer.boundingBoxWGS84)

    # Detect geometry column
    geometry_column = _detect_geometry_column(wfs, matched_typename)

    # Get available output formats
    available_formats = []

    # Method 1: WFS 1.1.0+ - Check operations metadata parameters
    if hasattr(wfs, "operations"):
        for op in wfs.operations:
            if op.name == "GetFeature" and hasattr(op, "parameters"):
                params = op.parameters
                if "outputFormat" in params and "values" in params["outputFormat"]:
                    available_formats = list(params["outputFormat"]["values"])
                    break

    # Method 2: Legacy attribute (some OWSLib versions)
    if not available_formats and hasattr(wfs, "getfeature_output_formats"):
        available_formats = list(wfs.getfeature_output_formats)

    # Method 3: Fall back to capabilities XML parsing (WFS 1.0.0 style)
    if not available_formats and hasattr(wfs, "capabilities") and wfs.capabilities:
        try:
            from owslib.util import nspath_eval

            ns = wfs.namespaces
            getfeature = wfs.capabilities.find(
                nspath_eval("wfs:Capability/wfs:Request/wfs:GetFeature", ns)
            )
            if getfeature is not None:
                for fmt in getfeature.findall(nspath_eval("wfs:ResultFormat/*", ns)):
                    available_formats.append(fmt.tag.split("}")[-1])
        except Exception:
            pass

    return WFSLayerInfo(
        typename=matched_typename,
        title=getattr(layer, "title", None),
        crs_list=crs_list,
        default_crs=default_crs,
        bbox=bbox,
        geometry_column=geometry_column,
        available_formats=available_formats,
    )


def list_available_layers(service_url: str, version: str = "1.1.0") -> list[dict]:
    """
    List available layers in a WFS service.

    Args:
        service_url: WFS service URL
        version: WFS version

    Returns:
        List of dicts with layer info (name, typename, title, abstract, bbox)
    """
    wfs = get_wfs_capabilities(service_url, version)

    layers = []
    for typename, layer in wfs.contents.items():
        layers.append(
            {
                "name": typename,  # Alias for consistency with CLI
                "typename": typename,
                "title": getattr(layer, "title", None),
                "abstract": getattr(layer, "abstract", None),
                "bbox": tuple(layer.boundingBoxWGS84)
                if hasattr(layer, "boundingBoxWGS84") and layer.boundingBoxWGS84
                else None,
            }
        )

    return layers


def _detect_best_output_format(available_formats: list[str]) -> str:
    """
    Detect the best output format from available formats.

    Prefers GeoJSON for faster parsing, falls back to GML.

    Args:
        available_formats: List of format strings from capabilities

    Returns:
        Best format string to request
    """
    available_lower = [f.lower() for f in available_formats]

    # Check for GeoJSON formats (preferred - faster to parse)
    for fmt in GEOJSON_FORMATS:
        if fmt.lower() in available_lower:
            idx = available_lower.index(fmt.lower())
            return available_formats[idx]

    # Check for GML formats
    for fmt in GML_FORMATS:
        if fmt.lower() in available_lower:
            idx = available_lower.index(fmt.lower())
            return available_formats[idx]

    # Fallback to first available or default
    return available_formats[0] if available_formats else "GML3"


def _negotiate_crs(layer_info: WFSLayerInfo, output_crs: str | None = None) -> str:
    """
    Negotiate the best CRS to request from the WFS server.

    Strategy:
    1. If output_crs specified and supported -> use it
    2. Try EPSG:4326 variants (most universal)
    3. Fall back to server default

    Args:
        layer_info: Layer metadata with CRS list
        output_crs: User-requested output CRS

    Returns:
        CRS string to use in requests
    """
    crs_list = layer_info.crs_list

    # If user specified CRS, check if supported
    if output_crs:
        for crs in crs_list:
            if _crs_matches(crs, output_crs):
                debug(f"Using requested CRS: {crs}")
                return crs
        warn(f"Requested CRS '{output_crs}' not in layer's CRS list. Using server default.")

    # Try EPSG:4326 variants
    for crs in crs_list:
        if _crs_matches(crs, "EPSG:4326"):
            debug(f"Using WGS84: {crs}")
            return crs

    # Fall back to default
    if layer_info.default_crs:
        debug(f"Using server default CRS: {layer_info.default_crs}")
        return layer_info.default_crs

    # Last resort
    if crs_list:
        debug(f"Using first available CRS: {crs_list[0]}")
        return crs_list[0]

    return "EPSG:4326"


def _determine_bbox_strategy(
    bbox_mode: str,
    layer_info: WFSLayerInfo,
) -> bool:
    """
    Determine whether to use server-side bbox filtering.

    Unlike BigQuery, WFS doesn't easily expose row counts, so auto mode
    defaults to server-side filtering (conservative for remote services).

    Args:
        bbox_mode: "auto", "server", or "local"
        layer_info: Layer metadata (reserved for future use)

    Returns:
        True if server-side filtering should be used
    """
    # layer_info reserved for future use (e.g., checking server capabilities)
    _ = layer_info

    if bbox_mode == "server":
        debug("Using server-side bbox filter (forced by --bbox-mode server)")
        return True
    if bbox_mode == "local":
        debug("Using local bbox filter (forced by --bbox-mode local)")
        return False

    # Auto mode: default to server-side for WFS
    # WFS servers typically handle spatial filtering efficiently
    debug("Using server-side bbox filter (auto mode for WFS)")
    return True


def _is_urn_crs(crs: str) -> bool:
    """Check if CRS string is in URN format (urn:ogc:def:crs:EPSG::4326)."""
    return crs.lower().startswith("urn:")


def _is_geographic_crs(crs: str) -> bool:
    """Check if CRS is a geographic coordinate system (lat/lon, not projected)."""
    normalized = _normalize_crs(crs)
    # Common geographic CRS codes
    geographic_codes = {
        "EPSG:4326",  # WGS84
        "EPSG:4269",  # NAD83
        "EPSG:4267",  # NAD27
        "EPSG:4258",  # ETRS89
    }
    return normalized in geographic_codes or "CRS84" in crs.upper()


def _needs_axis_swap(crs: str, version: str, axis_order: str = "auto") -> bool:
    """
    Determine if bbox coordinates need axis order swap (lat,lon instead of lon,lat).

    Per OGC spec:
    - WFS 1.0.0: Always lon,lat (XY)
    - WFS 1.1.0+: Depends on CRS definition
      - EPSG:4326 (simple): lon,lat (common practice, though spec says lat,lon)
      - urn:ogc:def:crs:EPSG::4326: lat,lon per spec (YX)
      - CRS:84: Always lon,lat (XY) by definition

    Args:
        crs: Coordinate reference system string
        version: WFS version
        axis_order: "auto" (detect), "xy" (force lon,lat), "latlon" (force lat,lon)

    Returns:
        True if bbox needs to be swapped to lat,lon order
    """
    if axis_order == "xy":
        return False
    if axis_order == "latlon":
        return True

    # WFS 1.0.0 always uses XY order
    if version == "1.0.0":
        return False

    # CRS:84 is explicitly XY ordered
    if "CRS84" in crs.upper() or "CRS:84" in crs.upper():
        return False

    # For WFS 1.1.0+: URN format with geographic CRS uses lat,lon
    if _is_urn_crs(crs) and _is_geographic_crs(crs):
        return True

    return False


def _build_bbox_param(
    bbox: tuple[float, float, float, float],
    crs: str,
    version: str = "1.1.0",
    axis_order: str = "auto",
) -> str:
    """
    Build WFS bbox parameter string with correct axis order.

    WFS 1.0.0: xmin,ymin,xmax,ymax (always XY)
    WFS 1.1.0+: Axis order depends on CRS format
      - EPSG:4326 (simple): xmin,ymin,xmax,ymax,crs
      - urn:ogc:def:crs:EPSG::4326: ymin,xmin,ymax,xmax,crs (lat,lon per spec)

    Args:
        bbox: Bounding box tuple (xmin, ymin, xmax, ymax) in lon,lat order
        crs: Coordinate reference system
        version: WFS version
        axis_order: "auto" (detect from CRS), "xy" (force lon,lat), "latlon" (force lat,lon)

    Returns:
        Bbox parameter string with correct axis order for the CRS
    """
    xmin, ymin, xmax, ymax = bbox

    if version == "1.0.0":
        return f"{xmin},{ymin},{xmax},{ymax}"

    # Check if we need to swap axis order for this CRS
    if _needs_axis_swap(crs, version, axis_order):
        # Swap to lat,lon order (ymin,xmin,ymax,xmax)
        debug(f"Using lat,lon axis order for URN CRS: {crs}")
        return f"{ymin},{xmin},{ymax},{xmax},{crs}"
    else:
        # Standard lon,lat order (xmin,ymin,xmax,ymax)
        return f"{xmin},{ymin},{xmax},{ymax},{crs}"


def _validate_identifier(name: str) -> str:
    """
    Validate and sanitize a SQL identifier (column name).

    Args:
        name: Column name to validate

    Returns:
        Validated column name

    Raises:
        WFSError: If the name contains invalid characters
    """
    # Only allow alphanumeric, underscore, and standard identifier characters
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        # Check for dangerous patterns
        if '"' in name or "'" in name or ";" in name or "--" in name:
            raise WFSError(f"Invalid geometry column name '{name}': contains unsafe characters")
        # Allow dots for qualified names but escape them
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_\.]*$", name):
            raise WFSError(f"Invalid geometry column name '{name}': must be a valid identifier")
    return name


def _build_local_bbox_filter(
    bbox: tuple[float, float, float, float],
    geometry_column: str,
) -> str:
    """
    Build DuckDB SQL filter for local bbox filtering.

    Args:
        bbox: Bounding box tuple (xmin, ymin, xmax, ymax)
        geometry_column: Name of geometry column

    Returns:
        DuckDB ST_Intersects SQL condition

    Raises:
        WFSError: If geometry column name is invalid
    """
    # Validate column name to prevent SQL injection
    safe_column = _validate_identifier(geometry_column)

    xmin, ymin, xmax, ymax = bbox
    wkt = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"
    return f"ST_Intersects(\"{safe_column}\", ST_GeomFromText('{wkt}'))"


def _get_feature_count(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
) -> int | None:
    """
    Try to get feature count using resultType=hits.

    This is a WFS 1.1.0+ feature that returns count without fetching data.

    Args:
        service_url: WFS service URL
        typename: Layer typename
        version: WFS version

    Returns:
        Feature count or None if not supported
    """
    if version == "1.0.0":
        return None  # resultType=hits not supported in WFS 1.0.0

    clean_url = _clean_service_url(service_url)
    params = {
        "service": "WFS",
        "version": version,
        "request": "GetFeature",
        "typeNames" if version == "2.0.0" else "typeName": typename,
        "resultType": "hits",
    }

    try:
        content = _make_request(clean_url, params=params)

        # Parse XML response to find numberOfFeatures
        import re

        match = re.search(rb'numberOfFeatures="(\d+)"', content)
        if match:
            return int(match.group(1))

        # Try alternative attribute name
        match = re.search(rb'numberMatched="(\d+)"', content)
        if match:
            return int(match.group(1))

    except Exception:
        pass

    return None


def _infer_column_types(table: pa.Table) -> pa.Table:
    """
    Infer and cast string columns to appropriate types.

    WFS servers often serialize all values as quoted strings in JSON,
    causing DuckDB to infer them as VARCHAR. This function detects
    columns that can be safely cast to int64, float64, or bool.

    Args:
        table: PyArrow table with potentially mis-typed string columns

    Returns:
        Table with string columns cast to inferred types
    """
    if table.num_rows == 0:
        return table

    con = get_duckdb_connection()
    try:
        con.register("_wfs_data", table)

        new_columns = []
        for field in table.schema:
            col_name = field.name

            # Only process string/large_string columns
            if field.type not in (pa.string(), pa.large_string()):
                new_columns.append((col_name, table[col_name]))
                continue

            # Check type compatibility using TRY_CAST
            # Quote column name to handle reserved words/special chars
            quoted_col = f'"{col_name}"'

            try:
                stats = con.execute(f"""
                    SELECT
                        COUNT(*) AS total,
                        COUNT({quoted_col}) AS non_null,
                        SUM(CASE WHEN TRY_CAST({quoted_col} AS BIGINT)::VARCHAR = {quoted_col}
                            THEN 1 ELSE 0 END) AS is_int,
                        SUM(CASE WHEN TRY_CAST({quoted_col} AS DOUBLE) IS NOT NULL
                            THEN 1 ELSE 0 END) AS is_float,
                        SUM(CASE WHEN LOWER({quoted_col}) IN ('true', 'false', '1', '0')
                            THEN 1 ELSE 0 END) AS is_bool
                    FROM _wfs_data
                """).fetchone()

                total, non_null, is_int, is_float, is_bool = stats

                # Skip columns with all nulls
                if non_null == 0:
                    new_columns.append((col_name, table[col_name]))
                    continue

                # Determine target type (order matters: int > float > bool > string)
                if is_int == non_null:
                    # All non-null values are valid integers
                    casted = (
                        con.execute(f"""
                        SELECT CAST({quoted_col} AS BIGINT) FROM _wfs_data
                    """)
                        .arrow()
                        .read_all()
                        .column(0)
                    )
                    new_columns.append((col_name, casted))
                elif is_float == non_null and is_int < non_null:
                    # All non-null values are valid floats (but not all integers)
                    casted = (
                        con.execute(f"""
                        SELECT CAST({quoted_col} AS DOUBLE) FROM _wfs_data
                    """)
                        .arrow()
                        .read_all()
                        .column(0)
                    )
                    new_columns.append((col_name, casted))
                elif is_bool == non_null:
                    # All non-null values are valid booleans
                    casted = (
                        con.execute(f"""
                        SELECT CASE
                            WHEN LOWER({quoted_col}) IN ('true', '1') THEN TRUE
                            WHEN LOWER({quoted_col}) IN ('false', '0') THEN FALSE
                            ELSE NULL
                        END
                        FROM _wfs_data
                    """)
                        .arrow()
                        .read_all()
                        .column(0)
                    )
                    new_columns.append((col_name, casted))
                else:
                    # Keep as string
                    new_columns.append((col_name, table[col_name]))

            except Exception as e:
                # On any error, keep original column
                debug(f"Type inference failed for column '{col_name}': {e}")
                new_columns.append((col_name, table[col_name]))

        con.unregister("_wfs_data")
        return pa.table(dict(new_columns))
    finally:
        con.close()


def _fetch_wfs_page_duckdb(url: str) -> pa.Table:
    """
    Fetch a WFS GeoJSON page directly using DuckDB's httpfs.

    This is MUCH faster than Python HTTP because:
    - DuckDB streams the HTTP response (no Python memory buffering)
    - JSON parsing happens in C++ (faster than Python json)
    - Geometry conversion happens in-database (no temp files)

    Args:
        url: Full WFS GetFeature URL with all parameters

    Returns:
        PyArrow Table with geometry column (WKB) and all properties
    """
    con = get_duckdb_connection(load_spatial=True, load_httpfs=True)

    # Configure longer HTTP timeout for slow WFS servers (10 minutes)
    # Large datasets like 309k features can take 2-3 minutes to stream
    con.execute("SET http_timeout=600000")

    # Escape single quotes in URL to prevent SQL injection
    safe_url = _escape_sql_string(url)

    # Use DuckDB to fetch and parse the WFS GeoJSON in one query
    # This streams the HTTP response and parses JSON directly
    # Step 1: Check for empty features array (DuckDB can't UNNEST empty JSON arrays)
    # Step 2: Unnest features and extract geometry + properties struct
    # Step 3: Expand properties struct into individual columns
    #
    # Note: When features is [] (empty), read_json_auto infers it as JSON type
    # rather than a list, causing UNNEST to fail. We handle this by first
    # checking the feature count.
    count_query = f"""
        SELECT len(features) AS cnt
        FROM read_json_auto('{safe_url}', maximum_object_size=536870912)
    """

    try:
        debug(f"DuckDB fetch: {url[:80]}...")
        start_time = time.time()

        # Check if response has any features
        count_result = con.execute(count_query).fetchone()
        feature_count = count_result[0] if count_result else 0

        if feature_count == 0:
            # Return empty table with just geometry column
            debug("Empty response, returning empty table")
            return pa.table({"geometry": pa.array([], type=pa.binary())})

        # Full query to extract features
        query = f"""
            WITH features AS (
                SELECT unnest(features) AS feature
                FROM read_json_auto('{safe_url}', maximum_object_size=536870912)
            ),
            extracted AS (
                SELECT
                    ST_AsWKB(ST_GeomFromGeoJSON(feature.geometry)) AS geometry,
                    feature.properties AS props
                FROM features
            )
            SELECT
                geometry,
                unnest(props)
            FROM extracted
        """

        result = con.execute(query)
        table = result.arrow().read_all()

        elapsed = time.time() - start_time
        debug(f"DuckDB OK: {table.num_rows:,} rows in {elapsed:.1f}s")
        return table
    except Exception as e:
        error_msg = str(e)
        if "HTTP" in error_msg or "Could not" in error_msg:
            raise WFSError(f"Failed to fetch WFS data: {e}") from e
        raise WFSError(f"Failed to parse WFS response: {e}") from e


def _build_wfs_url(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
    max_features: int | None = None,
    start_index: int | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    crs: str | None = None,
    axis_order: str = "auto",
) -> str:
    """Build a WFS GetFeature URL with pagination support."""
    from urllib.parse import urlencode

    clean_url = _clean_service_url(service_url)

    params = {
        "service": "WFS",
        "version": version,
        "request": "GetFeature",
        "typeNames" if version == "2.0.0" else "typeName": typename,
        "outputFormat": "application/json",
    }

    if max_features is not None:
        # WFS 2.0 uses count, WFS 1.x uses maxFeatures
        if version == "2.0.0":
            params["count"] = str(max_features)
        else:
            params["maxFeatures"] = str(max_features)

    if start_index is not None and start_index > 0 and version != "1.0.0":
        params["startIndex"] = str(start_index)

    # Always include srsName when CRS is specified (Issue #405)
    # This tells the server what CRS to return data in, independent of bbox
    # Note: WFS 1.0.0 uses SRS in bbox only, srsName is 1.1.0+ parameter
    if crs and version != "1.0.0":
        params["srsName"] = crs

    if bbox and crs:
        params["bbox"] = _build_bbox_param(bbox, crs, version, axis_order)

    return f"{clean_url}?{urlencode(params)}"


def fetch_all_features_duckdb(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
    max_features: int | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    crs: str | None = None,
    max_workers: int = 1,
    page_size: int = 10000,
    axis_order: str = "auto",
) -> pa.Table:
    """
    Fetch WFS features using DuckDB's native HTTP streaming.

    Supports two modes:
    - Single request (max_workers=1): Streams all features in one request. Fast for most cases.
    - Parallel pagination (max_workers>1): Splits into paginated requests for very large
      datasets (1M+ features) where a single request might timeout.

    Args:
        service_url: WFS service URL
        typename: Layer typename
        version: WFS version
        max_features: Maximum features to fetch (None = all)
        bbox: Optional bounding box filter
        crs: CRS for bbox parameter
        max_workers: Number of parallel requests (1 = single streaming request)
        page_size: Features per page when using parallel mode (default: 10000)
        axis_order: Bbox axis order ("auto", "xy", "latlon")

    Returns:
        PyArrow Table with geometry (WKB) and all properties
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Get expected count for progress and pagination
    total_count = _get_feature_count(service_url, typename, version)
    if max_features and total_count:
        total_count = min(total_count, max_features)

    # Single request mode (default) - fastest for most cases
    if max_workers == 1 or version == "1.0.0":
        if total_count:
            progress(f"Streaming {total_count:,} features via DuckDB...")
        else:
            progress("Streaming features via DuckDB...")

        url = _build_wfs_url(
            service_url,
            typename,
            version,
            max_features=max_features,
            bbox=bbox,
            crs=crs,
            axis_order=axis_order,
        )
        table = _fetch_wfs_page_duckdb(url)
        return _infer_column_types(table)

    # Parallel pagination mode for large datasets
    if total_count is None:
        warn("Cannot determine feature count; falling back to single request mode.")
        url = _build_wfs_url(
            service_url,
            typename,
            version,
            max_features=max_features,
            bbox=bbox,
            crs=crs,
            axis_order=axis_order,
        )
        table = _fetch_wfs_page_duckdb(url)
        return _infer_column_types(table)

    # Calculate page ranges
    effective_total = max_features if max_features else total_count
    num_pages = (effective_total + page_size - 1) // page_size
    actual_workers = min(max_workers, num_pages)

    progress(
        f"Fetching {effective_total:,} features in {num_pages} pages using {actual_workers} workers..."
    )

    # Build page URLs
    pages = []
    for i in range(num_pages):
        start = i * page_size
        remaining = effective_total - start
        count = min(page_size, remaining)
        if count <= 0:
            break
        url = _build_wfs_url(
            service_url,
            typename,
            version,
            max_features=count,
            start_index=start,
            bbox=bbox,
            crs=crs,
            axis_order=axis_order,
        )
        pages.append((i, start, url))

    # Fetch pages in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        future_to_page = {
            executor.submit(_fetch_wfs_page_duckdb, url): (page_num, start)
            for page_num, start, url in pages
        }

        for future in as_completed(future_to_page):
            page_num, start = future_to_page[future]
            try:
                table = future.result()
                results[page_num] = table
                debug(f"Page {page_num + 1}/{num_pages}: {table.num_rows:,} features")
            except Exception as e:
                raise WFSError(f"Failed to fetch page {page_num + 1} (offset {start}): {e}") from e

    # Combine tables in order
    if not results:
        raise WFSError("No features returned from WFS service.")

    tables = [results[i] for i in sorted(results.keys())]
    combined = pa.concat_tables(tables)
    debug(f"Combined {len(tables)} pages: {combined.num_rows:,} total features")

    # Infer types AFTER concat to ensure consistent schema (issue #400)
    return _infer_column_types(combined)


def wfs_to_table(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
    bbox: tuple[float, float, float, float] | None = None,
    bbox_mode: str = "auto",
    output_crs: str | None = None,
    limit: int | None = None,
    max_workers: int = 1,
    page_size: int = 10000,
    axis_order: str = "auto",
    strict_crs: bool = False,
    verbose: bool = False,
) -> pa.Table:
    """
    Fetch WFS layer as PyArrow Table.

    Uses DuckDB's native HTTP streaming for 10x+ faster extraction:
    - HTTP response is streamed directly in C++ (no Python buffering)
    - JSON parsing happens in DuckDB (faster than Python json)
    - Geometry conversion happens in-database (no temp files)

    For very large datasets (1M+ features), use max_workers > 1 to enable
    parallel pagination, which splits the request into smaller chunks.

    Args:
        service_url: WFS service URL
        typename: Layer typename
        version: WFS version (1.0.0, 1.1.0, or 2.0.0)
        bbox: Bounding box filter (xmin, ymin, xmax, ymax)
        bbox_mode: Bbox strategy ("auto", "server", "local")
        output_crs: Guarantee output in this CRS. If server returns different CRS, data is
            reprojected automatically. (e.g., "EPSG:4326")
        limit: Maximum features to fetch
        max_workers: Parallel requests for large datasets (default: 1 = single request)
        page_size: Features per page when using parallel mode (default: 10000)
        axis_order: Bbox axis order ("auto", "xy", "latlon")
        strict_crs: If True, fail on CRS mismatch (before reprojection). If False and
            output_crs is set, reproject automatically; otherwise warn and use detected CRS
        verbose: Enable debug output

    Returns:
        PyArrow Table with GeoParquet-compatible geometry
    """
    configure_verbose(verbose)

    # Handle auto version negotiation
    if version == "auto":
        version, _ = negotiate_wfs_version(service_url)

    # Get layer info
    info("Connecting to WFS service...")
    layer_info = get_layer_info(service_url, typename, version)

    debug(f"Layer: {layer_info.typename}")
    debug(f"Title: {layer_info.title}")
    debug(f"Available CRS: {len(layer_info.crs_list)} options")
    debug(f"Available formats: {layer_info.available_formats}")

    # Negotiate CRS
    crs = _negotiate_crs(layer_info, output_crs)

    # Detect best output format
    output_format = _detect_best_output_format(layer_info.available_formats)
    debug(f"Using output format: {output_format}")

    # Determine bbox strategy
    use_server_bbox = True
    if bbox:
        use_server_bbox = _determine_bbox_strategy(bbox_mode, layer_info)

    # Use DuckDB-native streaming for fast extraction
    # Single request mode is 10x+ faster than Python HTTP
    # Parallel mode is useful for very large datasets (1M+ features)
    table = fetch_all_features_duckdb(
        service_url=service_url,
        typename=layer_info.typename,
        version=version,
        max_features=limit,
        bbox=bbox if use_server_bbox else None,
        crs=crs,
        max_workers=max_workers,
        page_size=page_size,
        axis_order=axis_order,
    )

    if table.num_rows == 0:
        if bbox:
            # Empty results with bbox filter is valid - just no features in that area
            warn(f"No features found in bbox for layer '{typename}'. Writing empty file.")
        else:
            # Empty results without bbox likely indicates a problem
            raise WFSError(
                f"No features returned from WFS service for layer '{typename}'.\n"
                "Check that the layer exists and is not empty."
            )

    # Apply local bbox filter if needed
    if bbox and not use_server_bbox:
        debug("Applying local bbox filter...")
        filter_sql = _build_local_bbox_filter(bbox, "geometry")
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            con.register("features", table)
            filtered = con.execute(f"SELECT * FROM features WHERE {filter_sql}").arrow()
            table = filtered.read_all()
            debug(f"After local filter: {table.num_rows:,} features")
        finally:
            con.close()

    # Validate CRS - check if coordinates match requested CRS
    crs_valid, detected_crs = _validate_crs_coordinates(table, crs, strict=strict_crs)
    if not crs_valid and detected_crs:
        if output_crs:
            # User explicitly requested CRS but server returned different — reproject
            info(f"Server returned {detected_crs}, reprojecting to requested {output_crs}")
            try:
                table = reproject_table(table, target_crs=output_crs, source_crs=detected_crs)
            except Exception as e:
                raise WFSError(
                    f"Failed to reproject from {detected_crs} to {output_crs}: {e}"
                ) from e
            crs = output_crs
        else:
            # No explicit request — use detected CRS for metadata
            info(f"Using detected CRS {detected_crs} for output metadata")
            crs = detected_crs

    # Add CRS metadata to schema
    projjson = parse_crs_string_to_projjson(_normalize_crs(crs))
    if projjson:
        geo_meta = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "crs": projjson,
                }
            },
        }
        existing_meta = table.schema.metadata or {}
        existing_meta[b"geo"] = json.dumps(geo_meta).encode("utf-8")
        table = table.replace_schema_metadata(existing_meta)

    success(f"Fetched {table.num_rows:,} features from WFS")
    return table


def convert_wfs_to_geoparquet(
    service_url: str,
    typename: str,
    output_file: str,
    version: str = "1.1.0",
    bbox: tuple[float, float, float, float] | None = None,
    bbox_mode: str = "auto",
    output_crs: str | None = None,
    limit: int | None = None,
    max_workers: int = 1,
    page_size: int = 10000,
    axis_order: str = "auto",
    strict_crs: bool = False,
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
    """
    Extract WFS layer and save as optimized GeoParquet.

    Args:
        service_url: WFS service URL
        typename: Layer typename
        output_file: Output GeoParquet file path
        version: WFS version
        bbox: Bounding box filter
        bbox_mode: Bbox strategy
        output_crs: Guarantee output in this CRS (reprojects if server returns different)
        limit: Maximum features
        max_workers: Parallel requests for large datasets (default: 1)
        page_size: Features per page when using parallel mode (default: 10000)
        axis_order: Bbox axis order ("auto", "xy", "latlon")
        strict_crs: If True, fail on CRS mismatch (before reprojection). If False and
            output_crs is set, reproject automatically; otherwise warn and use detected CRS
        skip_hilbert: Skip Hilbert curve sorting
        skip_bbox: Skip adding bbox column
        compression: Compression algorithm
        compression_level: Compression level
        row_group_size_mb: Row group size in MB
        row_group_rows: Row group size in rows
        geoparquet_version: GeoParquet version
        overwrite: Overwrite existing file
        verbose: Enable debug output
    """
    configure_verbose(verbose)

    # Check output file
    output_path = Path(output_file)
    if output_path.exists() and not overwrite:
        raise WFSError(f"Output file exists: {output_file}\nUse --overwrite to replace it.")

    # Fetch data
    table = wfs_to_table(
        service_url,
        typename,
        version=version,
        bbox=bbox,
        bbox_mode=bbox_mode,
        output_crs=output_crs,
        limit=limit,
        max_workers=max_workers,
        page_size=page_size,
        axis_order=axis_order,
        strict_crs=strict_crs,
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
