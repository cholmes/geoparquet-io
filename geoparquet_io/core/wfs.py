"""
WFS (Web Feature Service) to GeoParquet conversion.

This module provides functionality to download features from OGC WFS services
and convert them to GeoParquet format. Supports WFS 1.0.0, 1.1.0, and 2.0.0.

Key features:
- Fast extraction via Python httpx download + DuckDB local parsing (~10x faster)
- Server-side bbox filtering
- CRS negotiation with EPSG variant handling
- Hilbert curve sorting and bbox column generation
- Auto-pagination and parallel workers for large datasets
- Auto-tile mode to bypass server startIndex limits
"""

from __future__ import annotations

import json
import re
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import duckdb
import pyarrow as pa

# Public API
__all__ = [
    "EmptyLayerError",
    "LayerNotFoundError",
    "WFSAuthenticationError",
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

from geoparquet_io.core.common import (
    _cast_table_to_schema,
    _compute_unified_schema,
    write_geoparquet_table,
)
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

# Maximum JSON object size for DuckDB parsing (1GB)
_MAX_JSON_OBJECT_SIZE = 1073741824


class WFSError(Exception):
    """Exception raised for WFS-related errors."""

    pass


class EmptyLayerError(WFSError):
    """Raised when a WFS layer has 0 features."""

    def __init__(self, typename: str) -> None:
        self.typename = typename
        super().__init__(
            f"No features returned from WFS service for layer '{typename}'.\n"
            "Check that the layer exists and is not empty."
        )


class LayerNotFoundError(WFSError):
    """Raised when a WFS layer does not exist in the service."""

    def __init__(self, typename: str, available: list[str] | None = None) -> None:
        self.typename = typename
        self.available = available or []
        hint = (
            f"\nAvailable layers (first 10): {', '.join(self.available[:10])}"
            if self.available
            else ""
        )
        super().__init__(f"Layer '{typename}' not found in WFS service.{hint}")


class WFSAuthenticationError(WFSError):
    """Raised when WFS service requires authentication or access is denied."""

    def __init__(self, url: str, status_code: int, message: str) -> None:
        self.url = url
        self.status_code = status_code
        super().__init__(message)


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

# Schema-metadata key under which the CRS declared by the server in its GeoJSON
# response (the FeatureCollection ``crs`` member) is carried up from the fetch
# layer to wfs_to_table. This is the authoritative statement of which CRS the
# server actually honored — far more reliable than guessing from a bbox. See
# https://github.com/geoparquet/geoparquet-io/issues/499
_SERVER_CRS_METADATA_KEY = b"_wfs_server_crs"


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
    sortable_attribute: str | None = None


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
                raise WFSAuthenticationError(
                    url, 401, "Authentication required. WFS server requires credentials."
                ) from e
            elif status == 403:
                raise WFSAuthenticationError(
                    url, 403, "Access denied. Check your permissions for this WFS service."
                ) from e
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


def _with_server_crs(table: pa.Table, server_crs: str | None) -> pa.Table:
    """Attach the server-declared CRS to a table's schema metadata.

    Carries the CRS the server reported in its GeoJSON response up through the
    fetch/concat/type-inference pipeline so wfs_to_table can trust it instead of
    guessing from coordinates. No-op when ``server_crs`` is None.
    """
    if not server_crs:
        return table
    metadata = dict(table.schema.metadata or {})
    metadata[_SERVER_CRS_METADATA_KEY] = server_crs.encode()
    return table.replace_schema_metadata(metadata)


def _read_server_crs(table: pa.Table) -> str | None:
    """Read the server-declared CRS previously attached via _with_server_crs."""
    metadata = table.schema.metadata or {}
    value = metadata.get(_SERVER_CRS_METADATA_KEY)
    return value.decode() if value else None


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
            # The bbox heuristic can reliably distinguish coordinate *categories*
            # (geographic degrees vs. projected meters) but NOT one projected CRS
            # from another — e.g. EPSG:22174 (POSGAR 98 / Argentina 4) and
            # EPSG:3857 both use large metric coordinates. Only treat this as a
            # mismatch when the categories disagree; otherwise trust the
            # server-honored CRS rather than relabel it on a guess, which silently
            # corrupts downstream reprojection (issue #499).
            requested_geographic = _is_geographic_crs(normalized_crs)
            detected_geographic = detected == "EPSG:4326"
            if requested_geographic != detected_geographic:
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


def _reconcile_source_crs(
    table: pa.Table,
    *,
    source_crs: str,
    requested_crs: str,
    output_crs: str | None,
    strict_crs: bool,
    origin: str,
) -> tuple[pa.Table, str]:
    """Reconcile a known source CRS against the requested CRS.

    ``source_crs`` is the CRS the data is actually in (from the server's GeoJSON
    ``crs`` member, or — as a last resort — guessed from coordinates). When it
    matches what we requested, the data is trusted as-is. When it genuinely
    differs, the server ignored ``srsName``; we reproject to ``output_crs`` if
    one was given, raise under ``strict_crs``, else label the output with the
    real ``source_crs`` rather than silently mislabeling it (issue #499).
    """
    if _crs_matches(source_crs, requested_crs):
        return table, requested_crs

    lead = "Server declared" if origin == "server" else "Coordinates look like"
    base_msg = (
        f"{lead} {source_crs} but {requested_crs} was requested; server may have ignored srsName."
    )

    if strict_crs:
        raise WFSError(base_msg)

    if output_crs:
        info(f"{base_msg} Reprojecting from {source_crs} to {output_crs}.")
        try:
            table = reproject_table(table, target_crs=output_crs, source_crs=source_crs)
        except Exception as e:
            raise WFSError(f"Failed to reproject from {source_crs} to {output_crs}: {e}") from e
        return table, output_crs

    warn(f"{base_msg} Labeling output with {source_crs}.")
    return table, source_crs


def _resolve_crs_for_output(
    table: pa.Table,
    requested_crs: str,
    output_crs: str | None,
    strict_crs: bool,
) -> tuple[pa.Table, str]:
    """Decide the CRS to label (and optionally reproject) the fetched data to.

    Trust order:
      1. The CRS the server declared in its GeoJSON response — authoritative.
         When present we never second-guess it with coordinate heuristics.
      2. A bbox-coordinate guess — last resort, only when the server declared
         nothing. The guess cannot tell two projected CRSs apart, so it is used
         conservatively (see _validate_crs_coordinates).

    Returns ``(table, crs)`` where ``table`` may have been reprojected.
    """
    server_crs = _read_server_crs(table)
    if server_crs:
        debug(f"Server declared CRS {server_crs} in GeoJSON response")
        return _reconcile_source_crs(
            table,
            source_crs=server_crs,
            requested_crs=requested_crs,
            output_crs=output_crs,
            strict_crs=strict_crs,
            origin="server",
        )

    crs_valid, detected_crs = _validate_crs_coordinates(table, requested_crs, strict=strict_crs)
    if crs_valid or not detected_crs:
        return table, requested_crs
    return _reconcile_source_crs(
        table,
        source_crs=detected_crs,
        requested_crs=requested_crs,
        output_crs=output_crs,
        strict_crs=strict_crs,
        origin="detected",
    )


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


def _detect_sortable_attribute(wfs, typename: str) -> str | None:
    """
    Detect a sortable (non-geometry) attribute from DescribeFeatureType.

    GeoServer requires a sortBy parameter for stable pagination on layers
    without a primary key. This function finds the first non-geometry
    attribute that can be used for sorting.

    Args:
        wfs: OWSLib WebFeatureService object
        typename: Layer typename

    Returns:
        First sortable attribute name, or None if none found
    """
    try:
        schema = wfs.get_schema(typename)
        if schema and "properties" in schema:
            geometry_col = schema.get("geometry_column", "geometry")
            for prop_name, prop_type in schema["properties"].items():
                # Skip geometry columns
                if prop_name == geometry_col:
                    continue
                prop_type_str = str(prop_type).lower()
                if any(
                    geom in prop_type_str
                    for geom in ["geometry", "point", "line", "polygon", "multi", "curve"]
                ):
                    continue
                # Found a non-geometry attribute
                return str(prop_name)
    except Exception:
        pass
    return None


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
        raise LayerNotFoundError(typename, available)

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

    # Detect sortable attribute for stable pagination (Issue #488)
    sortable_attribute = _detect_sortable_attribute(wfs, matched_typename)

    return WFSLayerInfo(
        typename=matched_typename,
        title=getattr(layer, "title", None),
        crs_list=crs_list,
        default_crs=default_crs,
        bbox=bbox,
        geometry_column=geometry_column,
        available_formats=available_formats,
        sortable_attribute=sortable_attribute,
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
    """Check if CRS is a geographic coordinate system (lat/lon, not projected).

    Uses pyproj for authoritative detection across all geographic CRSs rather
    than a hardcoded list — an allowlist misclassifies valid geographic systems
    outside it (e.g. EPSG:4171 RGF93) as projected, which would flag a false
    coordinate mismatch and relabel correct data to EPSG:4326. Falls back to a
    small known set only when pyproj cannot resolve the CRS.
    """
    normalized = _normalize_crs(crs)
    try:
        from pyproj import CRS as _PyprojCRS

        return bool(_PyprojCRS.from_user_input(normalized).is_geographic)
    except Exception:
        # pyproj unavailable or CRS unrecognized — fall back to a known set.
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
    bbox: tuple[float, float, float, float] | None = None,
    crs: str | None = None,
    axis_order: str = "auto",
) -> int | None:
    """
    Try to get feature count using resultType=hits.

    This is a WFS 1.1.0+ feature that returns count without fetching data.

    Args:
        service_url: WFS service URL
        typename: Layer typename
        version: WFS version
        bbox: Optional bounding box filter (xmin, ymin, xmax, ymax)
        crs: CRS for bbox parameter
        axis_order: Bbox axis order ("auto", "xy", "latlon")

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

    if bbox and crs:
        params["bbox"] = _build_bbox_param(bbox, crs, version, axis_order)

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


def _probe_properties_type(
    con: duckdb.DuckDBPyConnection, safe_path: str, max_object_size: int
) -> tuple[str, bool]:
    """
    Probe the type of feature.properties to determine if unnest() will work.

    Empty properties ({}) become MAP(VARCHAR, JSON), null properties become JSON,
    missing properties key raises an error - none can be unnested.
    Only STRUCT types support unnest().

    Args:
        con: DuckDB connection with spatial extension loaded
        safe_path: Path to the GeoJSON file (already escaped for SQL)
        max_object_size: Maximum JSON object size for read_json_auto

    Returns:
        Tuple of (props_type string, can_unnest_props bool)

    See: https://github.com/geoparquet/geoparquet-io/issues/441
    """
    try:
        props_type_result = con.execute(f"""
            WITH features AS (
                SELECT unnest(features) AS feature
                FROM read_json_auto('{safe_path}', maximum_object_size={max_object_size})
            )
            SELECT typeof(feature.properties) AS props_type
            FROM features
            LIMIT 1
        """).fetchone()
        props_type = props_type_result[0] if props_type_result else "NULL"
    except duckdb.BinderException as e:
        # Missing 'properties' key in feature struct (malformed GeoJSON per RFC 7946)
        if "properties" in str(e):
            props_type = "MISSING"
        else:
            raise

    can_unnest_props = props_type.startswith("STRUCT")
    return props_type, can_unnest_props


def _build_wfs_feature_query(
    safe_path: str, extract_fid: bool, can_unnest_props: bool, max_object_size: int
) -> str:
    """
    Build SQL query to extract WFS features from GeoJSON.

    Args:
        safe_path: Path to the GeoJSON file (already escaped for SQL)
        extract_fid: If True, include feature.id as _wfs_fid column
        can_unnest_props: If True, unnest properties into columns; otherwise geometry-only
        max_object_size: Maximum JSON object size for read_json_auto

    Returns:
        SQL query string
    """
    fid_col = "feature.id AS _wfs_fid," if extract_fid else ""
    fid_select = "_wfs_fid," if extract_fid else ""

    if can_unnest_props:
        # Normal path: unnest properties into separate columns
        return f"""
            WITH features AS (
                SELECT unnest(features) AS feature
                FROM read_json_auto('{safe_path}', maximum_object_size={max_object_size})
            ),
            extracted AS (
                SELECT
                    {fid_col}
                    ST_AsWKB(ST_GeomFromGeoJSON(feature.geometry)) AS geometry,
                    feature.properties AS props
                FROM features
            )
            SELECT {fid_select} geometry, unnest(props) FROM extracted
        """
    else:
        # Fallback: properties are empty/null/missing (MAP, JSON, or MISSING type),
        # return geometry-only table
        return f"""
            WITH features AS (
                SELECT unnest(features) AS feature
                FROM read_json_auto('{safe_path}', maximum_object_size={max_object_size})
            )
            SELECT
                {fid_col}
                ST_AsWKB(ST_GeomFromGeoJSON(feature.geometry)) AS geometry
            FROM features
        """


def _extract_server_crs_from_geojson(
    con: duckdb.DuckDBPyConnection, safe_path: str, max_object_size: int
) -> str | None:
    """Read the CRS the server declared in a GeoJSON FeatureCollection.

    WFS servers (e.g. GeoServer) echo the CRS actually used in a top-level
    ``crs`` member, e.g. ``{"crs": {"properties": {"name":
    "urn:ogc:def:crs:EPSG::22174"}}}``. This is authoritative — when present it
    tells us exactly what the server honored, so we never have to guess from
    coordinate ranges. Returns a normalized ``EPSG:<code>`` string, or None when
    the response omits the member (RFC 7946 GeoJSON has no ``crs``).

    See https://github.com/geoparquet/geoparquet-io/issues/499
    """
    try:
        row = con.execute(f"""
            SELECT crs.properties.name AS crs_name
            FROM read_json_auto('{safe_path}', maximum_object_size={max_object_size})
            LIMIT 1
        """).fetchone()
    except (duckdb.BinderException, duckdb.Error):
        # No 'crs' member, or a shape we don't recognize — fall back to guessing.
        return None

    if not row or not row[0]:
        return None
    return _normalize_crs(str(row[0]))


def _fetch_wfs_page(
    url: str, extract_fid: bool = False, max_retries: int = 3, retry_delay: float = 2.0
) -> pa.Table:
    """
    Fetch a WFS GeoJSON page via httpx and parse with DuckDB locally.

    Downloads GeoJSON using Python httpx (thread-safe, ~10x faster than DuckDB
    httpfs for JSON), streams to a temp file, then parses with DuckDB's spatial
    extension. Includes automatic retry for transient network errors.

    Args:
        url: Full WFS GetFeature URL with all parameters
        extract_fid: If True, extract feature.id as _wfs_fid column (for dedup)
        max_retries: Number of retry attempts for transient errors
        retry_delay: Base delay between retries (exponential backoff)

    Returns:
        PyArrow Table with geometry column (WKB) and all properties
    """
    import httpx

    last_exception: Exception | None = None

    for attempt in range(max_retries):
        start_time = time.time()
        if attempt > 0:
            debug(f"Retry {attempt}/{max_retries - 1}: {url[:80]}...")
        else:
            debug(f"Fetching: {url[:100]}...")

        # Stream response to temp file to avoid memory exhaustion on large responses
        tmp_path = None
        total_bytes = 0
        try:
            with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False) as tmp_file:
                tmp_path = tmp_file.name
                client = _get_shared_http_client(timeout=600)

                with client.stream(
                    "GET",
                    url,
                    headers={"Accept": "application/json", "Accept-Encoding": "gzip, deflate"},
                ) as response:
                    # Check HTTP status
                    if response.status_code == 400:
                        body = response.read().decode("utf-8", errors="replace")
                        if "startindex" in body.lower():
                            raise WFSError(
                                f"Server rejected paginated request (startIndex limit): {body.strip()}"
                            )
                        raise WFSError(f"WFS request failed with HTTP 400: {body[:500]}")
                    response.raise_for_status()

                    # Validate content-type (servers may return HTML errors with 200 OK)
                    content_type = response.headers.get("content-type", "").lower()
                    if (
                        content_type
                        and "json" not in content_type
                        and "javascript" not in content_type
                    ):
                        # Reject HTML, XML, or text/plain (often error pages)
                        if any(t in content_type for t in ("html", "xml", "text/plain")):
                            preview = response.read().decode("utf-8", errors="replace")[:500]
                            raise WFSError(
                                f"Expected JSON response but got {content_type}. "
                                f"Server may have returned an error page:\n{preview}"
                            )

                    # Stream content to file
                    for chunk in response.iter_bytes(chunk_size=65536):
                        tmp_file.write(chunk)
                        total_bytes += len(chunk)

            # Success - break out of retry loop
            download_time = time.time() - start_time
            size_mb = total_bytes / (1024 * 1024)
            debug(f"Downloaded {size_mb:.1f} MB in {download_time:.1f}s")
            break

        except httpx.HTTPStatusError as e:
            if tmp_path:
                Path(tmp_path).unlink(missing_ok=True)
            raise WFSError(f"WFS request failed with HTTP {e.response.status_code}") from e
        except (httpx.RequestError, httpx.RemoteProtocolError) as e:
            # Transient network error - retry
            last_exception = e
            if tmp_path:
                Path(tmp_path).unlink(missing_ok=True)
            if attempt < max_retries - 1:
                delay = retry_delay * (attempt + 1)
                warn(f"Network error (attempt {attempt + 1}/{max_retries}): {e}")
                warn(f"Retrying in {delay:.1f}s...")
                _reset_http_client()
                time.sleep(delay)
                continue
            raise WFSError(f"Failed to fetch WFS data after {max_retries} attempts: {e}") from e
        except WFSError:
            if tmp_path:
                Path(tmp_path).unlink(missing_ok=True)
            raise
    else:
        # Exhausted retries without success
        raise WFSError(f"Failed to fetch WFS data after {max_retries} attempts: {last_exception}")

    con = None
    try:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        safe_path = _escape_sql_string(tmp_path)

        # Check feature count first (DuckDB can't UNNEST empty JSON arrays)
        count_result = con.execute(f"""
            SELECT len(features) AS cnt
            FROM read_json_auto('{safe_path}', maximum_object_size={_MAX_JSON_OBJECT_SIZE})
        """).fetchone()
        feature_count = count_result[0] if count_result else 0

        # Authoritative CRS the server reported in its GeoJSON response, if any.
        server_crs = _extract_server_crs_from_geojson(con, safe_path, _MAX_JSON_OBJECT_SIZE)

        if feature_count == 0:
            debug("Empty response, returning empty table")
            empty = pa.table({"geometry": pa.array([], type=pa.binary())})
            return _with_server_crs(empty, server_crs)

        # Detect property type and build extraction query
        props_type, can_unnest_props = _probe_properties_type(con, safe_path, _MAX_JSON_OBJECT_SIZE)
        if not can_unnest_props:
            debug(f"Properties type is {props_type}, cannot unnest - returning geometry-only table")

        parse_start = time.time()
        query = _build_wfs_feature_query(
            safe_path, extract_fid, can_unnest_props, _MAX_JSON_OBJECT_SIZE
        )

        result = con.execute(query)
        table = result.arrow().read_all()

        parse_time = time.time() - parse_start
        total_time = time.time() - start_time
        debug(f"Parsed {table.num_rows:,} rows in {parse_time:.1f}s (total: {total_time:.1f}s)")
        return _with_server_crs(table, server_crs)
    except WFSError:
        raise
    except Exception as e:
        raise WFSError(f"Failed to parse WFS response: {e}") from e
    finally:
        if con:
            con.close()
        Path(tmp_path).unlink(missing_ok=True)


def _generate_tile_grid(
    bbox: tuple[float, float, float, float],
    num_tiles: int,
) -> list[tuple[float, float, float, float]]:
    """
    Generate a grid of tile bboxes covering the given bbox.

    Uses aspect-ratio-aware layout so tiles are roughly square in
    geographic coordinates.
    """
    import math

    if num_tiles <= 1:
        return [bbox]

    xmin, ymin, xmax, ymax = bbox
    width = xmax - xmin
    height = ymax - ymin

    if width <= 0 or height <= 0:
        return [bbox]

    aspect = width / height
    cols = max(1, round(math.sqrt(num_tiles * aspect)))
    rows = max(1, math.ceil(num_tiles / cols))

    dx = width / cols
    dy = height / rows

    tiles = []
    for r in range(rows):
        for c in range(cols):
            tiles.append(
                (
                    xmin + c * dx,
                    ymin + r * dy,
                    xmin + (c + 1) * dx,
                    ymin + (r + 1) * dy,
                )
            )

    return tiles


def _refine_tiles_adaptive(
    tiles: list[tuple[float, float, float, float]],
    service_url: str,
    typename: str,
    version: str,
    crs: str,
    axis_order: str,
    max_per_tile: int,
    max_depth: int = 8,
) -> list[tuple[float, float, float, float]]:
    """
    Recursively subdivide tiles that exceed the feature count limit.

    For each tile, probes the server with resultType=hits + bbox to check
    the feature count. Tiles over max_per_tile are split into 4 quadrants.
    """

    def _subdivide(
        bbox: tuple[float, float, float, float], depth: int
    ) -> list[tuple[float, float, float, float]]:
        if depth >= max_depth:
            return [bbox]

        count = _get_feature_count(
            service_url,
            typename,
            version,
            bbox=bbox,
            crs=crs,
            axis_order=axis_order,
        )

        if count is not None and count == 0:
            return []

        if count is None or count <= max_per_tile:
            return [bbox]

        xmin, ymin, xmax, ymax = bbox
        mx = (xmin + xmax) / 2
        my = (ymin + ymax) / 2

        quadrants = [
            (xmin, ymin, mx, my),
            (mx, ymin, xmax, my),
            (xmin, my, mx, ymax),
            (mx, my, xmax, ymax),
        ]

        result = []
        for q in quadrants:
            result.extend(_subdivide(q, depth + 1))
        return result

    refined = []
    for tile in tiles:
        refined.extend(_subdivide(tile, 0))
    return refined


def _deduplicate_tiles(table: pa.Table) -> pa.Table:
    """
    Deduplicate features that appear in multiple tiles.

    Uses _wfs_fid column if present (from GeoJSON feature.id).
    For features with NULL fid, falls back to geometry bytes deduplication.
    Always drops the _wfs_fid column from output.
    """
    if table.num_rows == 0:
        if "_wfs_fid" in table.column_names:
            return table.drop(["_wfs_fid"])
        return table

    con = get_duckdb_connection(load_spatial=False, load_httpfs=False)

    try:
        con.register("tile_data", table)

        if "_wfs_fid" in table.column_names:
            all_cols = [c for c in table.column_names if c != "_wfs_fid"]
            col_list = ", ".join(f'"{c}"' for c in all_cols)
            # Deduplicate by fid when present, fall back to geometry for NULL fids
            query = f"""
                SELECT {col_list}
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY "_wfs_fid") AS _rn
                    FROM tile_data
                    WHERE "_wfs_fid" IS NOT NULL
                ) WHERE _rn = 1
                UNION ALL
                SELECT {col_list}
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY geometry) AS _rn
                    FROM tile_data
                    WHERE "_wfs_fid" IS NULL
                ) WHERE _rn = 1
            """
        else:
            all_cols = table.column_names
            col_list = ", ".join(f'"{c}"' for c in all_cols)
            query = f"""
                SELECT {col_list}
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY geometry) AS _rn
                    FROM tile_data
                ) WHERE _rn = 1
            """

        result = con.execute(query)
        return result.arrow().read_all()
    finally:
        con.close()


def _fetch_with_spatial_tiles(
    service_url: str,
    typename: str,
    version: str,
    total_count: int,
    startindex_limit: int,
    layer_bbox: tuple[float, float, float, float],
    crs: str,
    max_workers: int = 1,
    page_size: int = 10000,
    axis_order: str = "auto",
    max_features: int | None = None,
    sort_by: str | None = None,
) -> pa.Table:
    """
    Fetch a large WFS dataset by subdividing into spatial tiles.

    Used when the server caps startIndex, making full pagination impossible.
    Subdivides the layer bbox into tiles, fetches each tile separately,
    deduplicates features on tile boundaries, and combines results.
    """
    import math

    max_per_tile = startindex_limit
    num_tiles = max(2, math.ceil(total_count / (max_per_tile * 0.8)))

    progress(f"Auto-tiling: splitting {total_count:,} features into ~{num_tiles} spatial tiles...")

    tiles = _generate_tile_grid(layer_bbox, num_tiles)

    progress(f"Refining {len(tiles)} tiles (checking feature counts per tile)...")
    tiles = _refine_tiles_adaptive(
        tiles,
        service_url,
        typename,
        version,
        crs=crs,
        axis_order=axis_order,
        max_per_tile=max_per_tile,
    )

    progress(f"Fetching {len(tiles)} tiles...")

    all_tables = []
    for i, tile_bbox in enumerate(tiles):
        debug(f"Tile {i + 1}/{len(tiles)}: bbox={tile_bbox}")
        tile_table = fetch_all_features_duckdb(
            service_url,
            typename,
            version,
            max_features=max_features,
            bbox=tile_bbox,
            crs=crs,
            max_workers=max_workers,
            page_size=page_size,
            axis_order=axis_order,
            extract_fid=True,
            sort_by=sort_by,
        )
        if tile_table.num_rows > 0:
            all_tables.append(tile_table)
        progress(f"Tile {i + 1}/{len(tiles)}: {tile_table.num_rows:,} features")

    if not all_tables:
        raise EmptyLayerError(typename)

    # All tiles share one server CRS; capture before schema unification drops it.
    server_crs = _read_server_crs(all_tables[0])

    # Unify schemas to handle type mismatches across tiles (e.g., int64 vs decimal128)
    if len(all_tables) > 1:
        schemas = [t.schema for t in all_tables]
        unified_schema = _compute_unified_schema(schemas)
        all_tables = [
            _cast_table_to_schema(t, unified_schema, page_info=f"tile {i + 1}")
            for i, t in enumerate(all_tables)
        ]

    combined = pa.concat_tables(all_tables)
    before_dedup = combined.num_rows

    combined = _deduplicate_tiles(combined)
    after_dedup = combined.num_rows

    if before_dedup != after_dedup:
        debug(
            f"Deduplicated: {before_dedup:,} → {after_dedup:,} ({before_dedup - after_dedup:,} duplicates)"
        )

    return _with_server_crs(_infer_column_types(combined), server_crs)


def _probe_startindex_limit(
    service_url: str,
    typename: str,
    version: str,
    crs: str | None = None,
    axis_order: str = "auto",
) -> int | None:
    """
    Probe a WFS server to discover any startIndex limit.

    Some servers (e.g., PDOK) reject requests with startIndex above a
    threshold (e.g., 50,000). This sends a lightweight probe request
    to detect that limit before attempting full pagination.

    Returns the limit value if detected, or None if no limit found.
    """
    import httpx

    probe_offset = 50001
    url = _build_wfs_url(
        service_url,
        typename,
        version,
        max_features=1,
        start_index=probe_offset,
        crs=crs,
        axis_order=axis_order,
    )

    try:
        client = _get_shared_http_client(timeout=30)
        response = client.get(url, headers={"Accept-Encoding": "gzip, deflate"})
        if response.status_code == 400:
            body = response.text.lower()
            if "startindex" in body:
                import re as _re

                match = _re.search(r"startindex.*?(\d[\d.,]+)", body)
                if match:
                    limit_str = match.group(1).replace(",", "").replace(".", "")
                    try:
                        return int(limit_str)
                    except ValueError:
                        pass
                return 50000
        return None
    except httpx.HTTPError:
        return None


def _build_wfs_url(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
    max_features: int | None = None,
    start_index: int | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    crs: str | None = None,
    axis_order: str = "auto",
    sort_by: str | None = None,
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

    # sortBy is required for stable pagination on PK-less layers (Issue #488)
    if sort_by and version != "1.0.0":
        params["sortBy"] = sort_by

    return f"{clean_url}?{urlencode(params)}"


def _single_fetch_mode(
    service_url: str,
    typename: str,
    version: str,
    max_features: int | None,
    bbox: tuple[float, float, float, float] | None,
    crs: str | None,
    axis_order: str,
    extract_fid: bool,
) -> pa.Table:
    """
    Fetch all features in a single request.

    Used when the dataset is small enough to fit in one request.
    """
    url = _build_wfs_url(
        service_url,
        typename,
        version,
        max_features=max_features,
        bbox=bbox,
        crs=crs,
        axis_order=axis_order,
    )
    table = _fetch_wfs_page(url, extract_fid=extract_fid)
    if extract_fid:
        return table
    # _infer_column_types rebuilds the schema and drops metadata; re-attach the
    # server-declared CRS captured by _fetch_wfs_page.
    return _with_server_crs(_infer_column_types(table), _read_server_crs(table))


def _sequential_pagination_mode(
    service_url: str,
    typename: str,
    version: str,
    effective_total: int,
    page_size: int,
    bbox: tuple[float, float, float, float] | None,
    crs: str | None,
    axis_order: str,
    extract_fid: bool,
    sort_by: str | None = None,
) -> dict[int, pa.Table]:
    """
    Fetch features using sequential adaptive pagination.

    Adapts page size dynamically if server caps maxFeatures.
    """
    results: dict[int, pa.Table] = {}
    offset = 0
    page_num = 0
    server_page_size = page_size

    while offset < effective_total:
        remaining = effective_total - offset
        count = min(server_page_size, remaining)
        url = _build_wfs_url(
            service_url,
            typename,
            version,
            max_features=count,
            start_index=offset,
            bbox=bbox,
            crs=crs,
            axis_order=axis_order,
            sort_by=sort_by,
        )
        try:
            table = _fetch_wfs_page(url, extract_fid=extract_fid)
        except Exception as e:
            raise WFSError(f"Failed to fetch page {page_num + 1} (offset {offset}): {e}") from e

        if table.num_rows == 0:
            break

        results[page_num] = table

        # Detect server-side maxFeatures cap
        if table.num_rows < count and server_page_size > table.num_rows:
            server_page_size = table.num_rows
            debug(f"Server caps response to {server_page_size} features per request")

        offset += table.num_rows
        page_num += 1
        debug(
            f"Page {page_num}: {table.num_rows:,} features (offset {offset:,}/{effective_total:,})"
        )

    return results


def _parallel_pagination_mode(
    service_url: str,
    typename: str,
    version: str,
    effective_total: int,
    page_size: int,
    num_pages: int,
    max_workers: int,
    bbox: tuple[float, float, float, float] | None,
    crs: str | None,
    axis_order: str,
    extract_fid: bool,
    sort_by: str | None = None,
) -> dict[int, pa.Table]:
    """
    Fetch features using parallel pagination.

    Pre-builds page URLs and fetches concurrently using ThreadPoolExecutor.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Build page URLs
    pages: list[tuple[int, int, str]] = []
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
            sort_by=sort_by,
        )
        pages.append((i, start, url))

    # Fetch pages in parallel
    results: dict[int, pa.Table] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(_fetch_wfs_page, url, extract_fid): (page_num, start)
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

    return results


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
    extract_fid: bool = False,
    sort_by: str | None = None,
) -> pa.Table:
    """
    Fetch WFS features via Python httpx download and DuckDB local parsing.

    Downloads GeoJSON with Python httpx (~10x faster than DuckDB httpfs), then
    parses locally with DuckDB's spatial extension.

    Supports multiple modes:
    - Single request (max_workers=1, small dataset): One request for all features
    - Sequential pagination (max_workers=1, large dataset): Paginated requests
    - Parallel pagination (max_workers>1): Concurrent paginated requests,
      recommended for large datasets (100k+ features) — typically 5-6x faster

    Args:
        service_url: WFS service URL
        typename: Layer typename
        version: WFS version
        max_features: Maximum features to fetch (None = all)
        bbox: Optional bounding box filter
        crs: CRS for bbox parameter
        max_workers: Number of parallel requests (1-10). Use 4-8 for large datasets.
        page_size: Features per page when paginating (default: 10000)
        axis_order: Bbox axis order ("auto", "xy", "latlon")
        extract_fid: If True, extract WFS feature IDs for deduplication
        sort_by: Attribute to sort by for stable pagination (auto-detected if None)

    Returns:
        PyArrow Table with geometry (WKB) and all properties
    """
    # Get expected count for progress and pagination
    total_count = _get_feature_count(
        service_url, typename, version, bbox=bbox, crs=crs, axis_order=axis_order
    )
    if max_features and total_count:
        total_count = min(total_count, max_features)

    # Single request for small datasets or when count is unknown
    needs_pagination = (
        total_count is not None and version != "1.0.0" and (max_features or total_count) > page_size
    )

    if not needs_pagination:
        if total_count:
            progress(f"Fetching {total_count:,} features...")
        else:
            progress("Fetching features...")

        table = _single_fetch_mode(
            service_url, typename, version, max_features, bbox, crs, axis_order, extract_fid
        )
        expected = max_features or total_count
        if expected and table.num_rows < expected and version != "1.0.0":
            # Server returned fewer than expected — likely has a maxFeatures cap.
            # Fall through to adaptive pagination to fetch the rest.
            needs_pagination = True
            page_size = table.num_rows or page_size
        else:
            return table

    # Paginated mode — sequential (max_workers=1) or parallel
    effective_total = max_features if max_features else total_count

    # Guard: pagination requires a known count
    if effective_total is None:
        warn("Cannot determine feature count; falling back to single request mode.")
        return _single_fetch_mode(
            service_url, typename, version, max_features, bbox, crs, axis_order, extract_fid
        )

    # Probe for server-side startIndex limit (skip for tile fetches)
    if not extract_fid:
        startindex_limit = _probe_startindex_limit(
            service_url, typename, version, crs=crs, axis_order=axis_order
        )
        if startindex_limit is not None:
            max_reachable = startindex_limit + page_size
            if effective_total > max_reachable:
                raise WFSError(
                    f"Server limits startIndex to {startindex_limit:,}, so only "
                    f"{max_reachable:,} of {effective_total:,} features are reachable via pagination.\n\n"
                    f"Options:\n"
                    f"  1. Use --auto-tile to automatically subdivide into spatial tiles\n"
                    f"  2. Use --limit {max_reachable} to fetch only what's reachable\n"
                    f"  3. Use --bbox to spatially filter to a smaller region\n"
                    f"  4. Download the dataset directly from the provider's bulk download service"
                )

    num_pages = (effective_total + page_size - 1) // page_size
    actual_workers = min(max_workers, num_pages)

    progress(
        f"Fetching {effective_total:,} features in {num_pages} pages "
        f"using {actual_workers} {'worker' if actual_workers == 1 else 'workers'}..."
    )

    # Fetch pages using appropriate mode
    if actual_workers == 1:
        results = _sequential_pagination_mode(
            service_url,
            typename,
            version,
            effective_total,
            page_size,
            bbox,
            crs,
            axis_order,
            extract_fid,
            sort_by=sort_by,
        )
    else:
        results = _parallel_pagination_mode(
            service_url,
            typename,
            version,
            effective_total,
            page_size,
            num_pages,
            actual_workers,
            bbox,
            crs,
            axis_order,
            extract_fid,
            sort_by=sort_by,
        )

    # Combine tables in order
    if not results:
        raise EmptyLayerError(typename)

    tables = [results[i] for i in sorted(results.keys())]

    # All pages come from the same layer/request, so they share one server CRS.
    # Capture it before schema unification, which drops metadata.
    server_crs = _read_server_crs(tables[0])

    # Unify schemas to handle type mismatches across pages (e.g., int64 vs decimal128)
    if len(tables) > 1:
        schemas = [t.schema for t in tables]
        unified_schema = _compute_unified_schema(schemas)
        tables = [
            _cast_table_to_schema(t, unified_schema, page_info=f"page {i + 1}")
            for i, t in enumerate(tables)
        ]

    combined = pa.concat_tables(tables)
    debug(f"Combined {len(tables)} pages: {combined.num_rows:,} total features")

    # Skip type inference for tile fetches — the tiling orchestrator handles it
    if extract_fid:
        return _with_server_crs(combined, server_crs)
    return _with_server_crs(_infer_column_types(combined), server_crs)


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
    auto_tile: bool = False,
    sort_by: str | None = None,
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
        output_crs: Guarantee output in this CRS. If the server returns a
            different CRS than requested, data is reprojected automatically from
            the server's actual CRS. (e.g., "EPSG:4326")
        limit: Maximum features to fetch
        max_workers: Parallel requests for large datasets (default: 1 = single request)
        page_size: Features per page when using parallel mode (default: 10000)
        axis_order: Bbox axis order ("auto", "xy", "latlon")
        strict_crs: If True, fail when the server returns a different CRS than
            requested. If False and output_crs is set, reproject from the
            server's actual CRS; otherwise warn and label the output with the
            server's actual CRS. The CRS the server declares in its GeoJSON
            response is authoritative — gpio never guesses from coordinates when
            the server states which CRS it used (issue #499).
        verbose: Enable debug output
        sort_by: Attribute to sort by for stable pagination. If None, auto-detected from
            DescribeFeatureType. Required for layers without a primary key.

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

    # Auto-detect sortBy attribute for stable pagination (Issue #488)
    # GeoServer requires sortBy for pagination on layers without a primary key
    effective_sort_by = sort_by
    if effective_sort_by is None and version != "1.0.0":
        effective_sort_by = layer_info.sortable_attribute
        if effective_sort_by:
            debug(f"Auto-detected sort attribute: {effective_sort_by}")

    # Determine bbox strategy
    use_server_bbox = True
    if bbox:
        use_server_bbox = _determine_bbox_strategy(bbox_mode, layer_info)

    # Check if auto-tiling is needed (server has startIndex limit + dataset exceeds it)
    use_tiling = False
    tiling_bbox: tuple[float, float, float, float] | None = None
    tiling_total = 0
    tiling_limit = 0
    if auto_tile and version != "1.0.0":
        total_count = _get_feature_count(service_url, layer_info.typename, version)
        startindex_limit = _probe_startindex_limit(
            service_url, layer_info.typename, version, crs=crs, axis_order=axis_order
        )
        if startindex_limit and total_count:
            effective = min(limit, total_count) if limit else total_count
            if effective > startindex_limit + page_size:
                # Determine bbox for tiling: use caller bbox if provided, else layer bbox
                if bbox and use_server_bbox:
                    tiling_bbox = bbox
                elif layer_info.bbox:
                    tiling_bbox = layer_info.bbox
                else:
                    raise WFSError(
                        "Auto-tiling requires a bounding box. Either:\n"
                        "  1. Use --bbox to specify a region\n"
                        "  2. Ensure the layer has a bbox in capabilities"
                    )
                use_tiling = True
                tiling_total = effective
                tiling_limit = startindex_limit

    if use_tiling and tiling_bbox is not None:
        table = _fetch_with_spatial_tiles(
            service_url=service_url,
            typename=layer_info.typename,
            version=version,
            total_count=tiling_total,
            startindex_limit=tiling_limit,
            layer_bbox=tiling_bbox,
            crs=crs,
            max_workers=max_workers,
            page_size=page_size,
            axis_order=axis_order,
            max_features=limit,
            sort_by=effective_sort_by,
        )
    else:
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
            sort_by=effective_sort_by,
        )

    if table.num_rows == 0:
        if bbox:
            # Empty results with bbox filter is valid - just no features in that area
            warn(f"No features found in bbox for layer '{typename}'. Writing empty file.")
        else:
            # Empty results without bbox likely indicates a problem
            raise EmptyLayerError(typename)

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

    # Resolve the CRS to label/reproject to. Trust the server's declared CRS
    # when it gave one; otherwise fall back to a coordinate-range guess.
    table, crs = _resolve_crs_for_output(table, crs, output_crs, strict_crs)

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
        existing_meta = dict(table.schema.metadata or {})
        # Drop the internal server-CRS marker so it doesn't leak into the file.
        existing_meta.pop(_SERVER_CRS_METADATA_KEY, None)
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
    auto_tile: bool = False,
    sort_by: str | None = None,
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
        strict_crs: If True, fail when the server returns a different CRS than
            requested. If False and output_crs is set, reproject from the
            server's actual CRS; otherwise warn and label with the server's
            actual CRS. The server's declared CRS is trusted over any coordinate
            guess (issue #499).
        skip_hilbert: Skip Hilbert curve sorting
        skip_bbox: Skip adding bbox column
        compression: Compression algorithm
        compression_level: Compression level
        row_group_size_mb: Row group size in MB
        row_group_rows: Row group size in rows
        geoparquet_version: GeoParquet version
        overwrite: Overwrite existing file
        verbose: Enable debug output
        auto_tile: Automatically subdivide into spatial tiles for servers with startIndex limits
        sort_by: Attribute to sort by for stable pagination. If None, auto-detected.
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
        auto_tile=auto_tile,
        sort_by=sort_by,
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
