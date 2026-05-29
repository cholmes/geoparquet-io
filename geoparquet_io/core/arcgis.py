"""
ArcGIS Feature Service to GeoParquet conversion.

This module provides functionality to download features from ArcGIS REST API
endpoints (FeatureServer/MapServer) and convert them to GeoParquet format.
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.crs_utils import parse_crs_string_to_projjson
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.exceptions import (
    BatchTooLargeError,
    GeoParquetError,
    InvalidParameterError,
    RemoteAccessError,
)
from geoparquet_io.core.http_retry import (
    make_request_with_retry,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success, warn
from geoparquet_io.core.remote import setup_aws_profile_if_needed

# ArcGIS Online token endpoint
ARCGIS_ONLINE_TOKEN_URL = "https://www.arcgis.com/sharing/rest/generateToken"

# Default page size for feature downloads (ArcGIS typical max is 2000)
DEFAULT_PAGE_SIZE = 2000

# Map ArcGIS WKID codes to EPSG codes for special cases
WKID_TO_EPSG = {
    102100: 3857,  # Web Mercator
    102113: 3785,  # Legacy Web Mercator
}

# Map ArcGIS geometry types to GeoJSON types
ARCGIS_GEOM_TYPES = {
    "esriGeometryPoint": "Point",
    "esriGeometryMultipoint": "MultiPoint",
    "esriGeometryPolyline": "MultiLineString",
    "esriGeometryPolygon": "MultiPolygon",
    "esriGeometryEnvelope": "Polygon",
}


@dataclass
class ArcGISAuth:
    """Authentication configuration for ArcGIS services."""

    token: str | None = None
    token_file: str | None = None
    username: str | None = None
    password: str | None = None
    portal_url: str | None = None


@dataclass
class ArcGISLayerInfo:
    """Metadata about an ArcGIS layer."""

    name: str
    geometry_type: str
    spatial_reference: dict
    fields: list[dict]
    max_record_count: int
    total_count: int


# Adaptive batch size fallback sequence
BATCH_SIZE_FALLBACKS = [1000, 500, 100, 50, 10, 1]


def _get_reduced_batch_size(current_batch: int) -> int | None:
    """
    Get the next smaller batch size from the fallback sequence.

    Args:
        current_batch: Current batch size that failed

    Returns:
        Next smaller batch size, or None if already at minimum (1)
    """
    if current_batch <= 1:
        return None

    # Find the first fallback smaller than current batch
    for fallback in BATCH_SIZE_FALLBACKS:
        if fallback < current_batch:
            return fallback

    # current_batch is very small but > 1, try 1
    return 1


def _make_request(
    method: str,
    url: str,
    params: dict | None = None,
    data: dict | None = None,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    batch_size: int | None = None,
) -> dict:
    """
    Make HTTP request with retry logic.

    Delegates to shared http_retry module for connection pooling and error handling.

    Args:
        method: HTTP method ("GET" or "POST")
        url: Request URL
        params: Query parameters
        data: POST data
        max_retries: Number of retry attempts
        retry_delay: Base delay between retries
        batch_size: Current batch size (for BatchTooLargeError context)

    Returns:
        Parsed JSON response

    Raises:
        RemoteAccessError: For HTTP errors
        BatchTooLargeError: When server returns non-JSON (batch too large)
    """
    return make_request_with_retry(
        method=method,
        url=url,
        params=params,
        data=data,
        max_retries=max_retries,
        retry_delay=retry_delay,
        parse_json=True,
        batch_size=batch_size,
    )


def _handle_arcgis_response(data: dict, context: str) -> dict:
    """Handle ArcGIS REST API response and check for errors."""
    if "error" in data:
        error = data["error"]
        code = error.get("code", "Unknown")
        message = error.get("message", "Unknown error")
        details = error.get("details", [])

        if code in (498, 499):
            raise GeoParquetError(f"{context}: Invalid or expired token. Please re-authenticate.")
        else:
            detail_str = "; ".join(details) if details else ""
            raise GeoParquetError(f"{context}: Error {code} - {message}. {detail_str}")

    return data


def generate_token(
    username: str,
    password: str,
    portal_url: str | None = None,
    verbose: bool = False,
) -> str:
    """
    Generate authentication token via ArcGIS REST API.

    Args:
        username: ArcGIS username
        password: ArcGIS password
        portal_url: Enterprise portal URL (default: ArcGIS Online)
        verbose: Whether to print debug output

    Returns:
        Authentication token string

    Raises:
        GeoParquetError: If token generation fails
    """
    token_url = portal_url or ARCGIS_ONLINE_TOKEN_URL

    if verbose:
        debug(f"Generating token from {token_url}")

    data = {
        "username": username,
        "password": password,
        "referer": "geoparquet-io",
        "f": "json",
        "expiration": 60,  # 60 minutes
    }

    result = _make_request("POST", token_url, data=data)
    result = _handle_arcgis_response(result, "Token generation")

    if "token" not in result:
        raise GeoParquetError("Token generation failed: no token in response")

    if verbose:
        debug("Token generated successfully")

    return result["token"]


def resolve_token(
    auth: ArcGISAuth,
    service_url: str,
    verbose: bool = False,
) -> str | None:
    """
    Resolve authentication token from various sources.

    Priority:
    1. Direct token parameter
    2. Token file (read from file path)
    3. Username/password (generate token via ArcGIS REST API)

    Args:
        auth: ArcGISAuth configuration
        service_url: Service URL (used to detect enterprise portal)
        verbose: Whether to print debug output

    Returns:
        Token string, or None if no auth provided
    """
    # Priority 1: Direct token
    if auth.token:
        if verbose:
            debug("Using direct token")
        return auth.token

    # Priority 2: Token file
    if auth.token_file:
        if verbose:
            debug(f"Reading token from file: {auth.token_file}")
        try:
            import fsspec

            with fsspec.open(auth.token_file, mode="rt") as f:
                return f.read().strip()
        except OSError as e:
            raise GeoParquetError(f"Failed to read token file: {e}") from e

    # Priority 3: Username/password
    if auth.username and auth.password:
        # Try to detect enterprise portal from service URL
        portal_url = auth.portal_url
        if not portal_url and "/arcgis/" in service_url.lower():
            # Enterprise server pattern: https://server.example.com/arcgis/rest/services/...
            # Token URL: https://server.example.com/arcgis/tokens/generateToken
            import re

            match = re.match(r"(https?://[^/]+/arcgis)", service_url, re.IGNORECASE)
            if match:
                portal_url = f"{match.group(1)}/tokens/generateToken"
                if verbose:
                    debug(f"Detected enterprise portal: {portal_url}")

        return generate_token(auth.username, auth.password, portal_url, verbose)

    return None


def _add_token_to_params(params: dict, token: str | None) -> dict:
    """Add authentication token to request parameters."""
    if token:
        return {**params, "token": token}
    return params


def validate_arcgis_url(url: str) -> tuple[str, int | None]:
    """
    Validate and parse ArcGIS Feature Service URL.

    Expected formats:
    - https://services.arcgis.com/.../FeatureServer/0
    - https://server.example.com/arcgis/rest/services/.../MapServer/0

    Args:
        url: ArcGIS service URL

    Returns:
        Tuple of (base_url, layer_id) where layer_id may be None

    Raises:
        InvalidParameterError: If URL is invalid
    """
    import re

    url = url.rstrip("/")

    # Check for ImageServer (raster - not supported)
    if "/ImageServer" in url:
        raise InvalidParameterError(
            "url",
            "ImageServer (raster) services are not supported. "
            "This command only supports vector services (FeatureServer or MapServer). "
            "ImageServer provides raster/imagery data which cannot be converted to GeoParquet.",
        )

    # Check for FeatureServer or MapServer
    if "/FeatureServer" not in url and "/MapServer" not in url:
        raise InvalidParameterError(
            "url",
            "Invalid ArcGIS URL. Expected format: https://services.arcgis.com/.../FeatureServer/0. "
            "The URL must point to a vector layer in a FeatureServer or MapServer. "
            "Make sure the URL includes /FeatureServer/ or /MapServer/ and a layer ID (e.g., /0).",
        )

    # Extract layer ID
    match = re.search(r"/(FeatureServer|MapServer)/(\d+)$", url)
    if match:
        return url, int(match.group(2))

    # URL ends with FeatureServer or MapServer without layer ID
    raise InvalidParameterError(
        "url",
        f"Missing layer ID. You must specify which layer to download by adding the layer ID "
        f"(e.g., {url}/0). To see available layers, open {url}?f=json in a browser.",
    )


def get_layer_info(
    service_url: str,
    token: str | None = None,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    verbose: bool = False,
) -> ArcGISLayerInfo:
    """
    Fetch layer metadata from ArcGIS REST service.

    Args:
        service_url: Full layer URL (e.g., .../FeatureServer/0)
        token: Optional authentication token
        where: SQL WHERE clause for counting features (default: "1=1" = all)
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        verbose: Whether to print debug output

    Returns:
        ArcGISLayerInfo with layer metadata
    """
    if verbose:
        debug(f"Fetching layer info from {service_url}")

    params = _add_token_to_params({"f": "json"}, token)
    data = _make_request("GET", service_url, params=params)
    data = _handle_arcgis_response(data, "Layer info")

    # Get feature count (using the WHERE and bbox filters)
    count = get_feature_count(service_url, where=where, bbox=bbox, token=token, verbose=verbose)

    return ArcGISLayerInfo(
        name=data.get("name", "Unknown"),
        geometry_type=data.get("geometryType", "esriGeometryPoint"),
        spatial_reference=data.get("spatialReference", {"wkid": 4326}),
        fields=data.get("fields", []),
        max_record_count=data.get("maxRecordCount", 1000),
        total_count=count,
    )


def get_feature_count(
    service_url: str,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    token: str | None = None,
    verbose: bool = False,
) -> int:
    """
    Get total feature count from ArcGIS service.

    Args:
        service_url: Full layer URL
        where: WHERE clause filter
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        token: Optional authentication token
        verbose: Whether to print debug output

    Returns:
        Feature count
    """
    query_url = f"{service_url}/query"
    params = {
        "where": where,
        "returnCountOnly": "true",
        "f": "json",
    }

    # Add bbox filter if provided
    if bbox:
        xmin, ymin, xmax, ymax = bbox
        params["geometry"] = f"{xmin},{ymin},{xmax},{ymax}"
        params["geometryType"] = "esriGeometryEnvelope"
        params["spatialRel"] = "esriSpatialRelIntersects"
        params["inSR"] = "4326"

    params = _add_token_to_params(params, token)

    data = _make_request("GET", query_url, params=params)
    data = _handle_arcgis_response(data, "Feature count")

    count = data.get("count", 0)
    if verbose:
        debug(f"Total feature count: {count}")

    return count


def fetch_features_page(
    service_url: str,
    offset: int,
    limit: int,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    out_fields: str = "*",
    token: str | None = None,
    verbose: bool = False,
) -> dict:
    """
    Fetch a single page of features as GeoJSON.

    Args:
        service_url: Full layer URL
        offset: Starting position for results (0-based)
        limit: Number of records to return
        where: WHERE clause filter
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        out_fields: Comma-separated field names or "*" for all
        token: Optional authentication token
        verbose: Whether to print debug output

    Returns:
        GeoJSON FeatureCollection dict
    """
    query_url = f"{service_url}/query"
    params = {
        "where": where,
        "outFields": out_fields,
        "returnGeometry": "true",
        "f": "geojson",
        "resultOffset": str(offset),
        "resultRecordCount": str(limit),
    }

    # Add bbox filter if provided (spatial query)
    if bbox:
        xmin, ymin, xmax, ymax = bbox
        params["geometry"] = f"{xmin},{ymin},{xmax},{ymax}"
        params["geometryType"] = "esriGeometryEnvelope"
        params["spatialRel"] = "esriSpatialRelIntersects"
        params["inSR"] = "4326"  # WGS84

    params = _add_token_to_params(params, token)

    data = _make_request("GET", query_url, params=params, batch_size=limit)

    # GeoJSON responses don't have the standard error format
    # Check if we got features or an error
    if "error" in data:
        _handle_arcgis_response(data, "Feature query")

    return data


def fetch_all_features(
    service_url: str,
    layer_info: ArcGISLayerInfo,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    out_fields: str = "*",
    max_features: int | None = None,
    token: str | None = None,
    batch_size: int | None = None,
    max_workers: int = 1,
    verbose: bool = False,
) -> Generator[dict, None, None]:
    """
    Generator that yields pages of GeoJSON features.

    Handles pagination using resultOffset/resultRecordCount.

    Args:
        service_url: Full layer URL
        layer_info: Layer metadata
        where: WHERE clause filter
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        out_fields: Comma-separated field names or "*" for all
        max_features: Maximum total features to return (limit)
        token: Optional authentication token
        batch_size: Custom batch size (default: server's maxRecordCount)
        max_workers: Number of concurrent requests (1 = sequential, 2-3 recommended)
        verbose: Whether to print debug output

    Yields:
        GeoJSON FeatureCollection dicts for each page
    """
    # Validate max_workers
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if max_workers > 10:
        warn(
            f"max_workers={max_workers} may trigger rate limits. "
            f"Recommended range: 1-10 (2-3 for best balance)"
        )

    # Sanitize and validate batch_size
    sanitized_batch = batch_size if batch_size and batch_size > 0 else DEFAULT_PAGE_SIZE

    # Determine batch size (respect server limit)
    max_batch = min(
        sanitized_batch,
        layer_info.max_record_count or DEFAULT_PAGE_SIZE,
    )

    # Apply user limit to total
    total = layer_info.total_count
    if max_features is not None:
        total = min(total, max_features)

    if max_workers == 1:
        # Sequential fetching with adaptive batch size
        offset = 0
        fetched = 0
        effective_batch = max_batch  # May be reduced on BatchTooLargeError

        while offset < total:
            # Adjust batch size for last page if limit applies
            remaining = total - offset
            current_batch = min(effective_batch, remaining)

            end = min(offset + current_batch, total)
            progress(f"Fetching features {offset + 1}-{end} of {total}...")

            try:
                page = fetch_features_page(
                    service_url,
                    offset,
                    current_batch,
                    where,
                    bbox=bbox,
                    out_fields=out_fields,
                    token=token,
                    verbose=verbose,
                )
            except BatchTooLargeError as e:
                # Server couldn't handle batch size - reduce and retry
                new_batch = _get_reduced_batch_size(current_batch)
                if new_batch is None:
                    # Already at minimum batch size, can't reduce further
                    raise GeoParquetError(
                        f"Server cannot handle even batch_size=1. "
                        f"This layer may have geometry too complex to download. "
                        f"Original error: {e.reason}"
                    ) from e

                warn(
                    f"Server returned error for batch_size={current_batch}. "
                    f"Reducing to {new_batch} and retrying..."
                )
                effective_batch = new_batch
                # Don't increment offset - retry same position with smaller batch
                continue

            features = page.get("features", [])
            if not features:
                break

            yield page

            fetched += len(features)
            offset += current_batch

            # Safety check: if server returned fewer than expected, adjust
            if len(features) < current_batch and offset < total:
                offset = fetched

            # Stop if we've hit the user limit
            if max_features is not None and fetched >= max_features:
                break

        if verbose:
            debug(f"Fetched {fetched} features total")

    else:
        # Parallel fetching with ThreadPoolExecutor
        from concurrent.futures import ThreadPoolExecutor

        fetched = 0
        batch_start = 0
        effective_batch = max_batch  # May be reduced on BatchTooLargeError

        # Reuse a single thread pool for all batches (more efficient)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while batch_start < total:
                # Submit max_workers requests in parallel
                futures = []

                for i in range(max_workers):
                    offset = batch_start + (i * effective_batch)
                    if offset >= total:
                        break

                    remaining = total - offset
                    current_batch = min(effective_batch, remaining)
                    end = min(offset + current_batch, total)

                    # Print progress message synchronously (avoid race condition)
                    progress(f"Fetching features {offset + 1}-{end} of {total}...")

                    future = executor.submit(
                        fetch_features_page,
                        service_url,
                        offset,
                        current_batch,
                        where,
                        bbox=bbox,
                        out_fields=out_fields,
                        token=token,
                        verbose=False,  # Disable per-request verbose to avoid race conditions
                    )
                    futures.append((offset, current_batch, future))

                # Collect results in order
                results = []
                batch_too_large = False
                failed_batch_size = None
                failed_at_index = None

                for idx, (offset, req_batch_size, future) in enumerate(futures):
                    try:
                        page = future.result()
                        results.append((offset, page))
                    except BatchTooLargeError:
                        # Mark for retry with smaller batch
                        batch_too_large = True
                        failed_batch_size = req_batch_size
                        failed_at_index = idx
                        break
                    except Exception as e:
                        # If one request fails, propagate the error (fail-fast)
                        raise RemoteAccessError(
                            service_url, f"Failed to fetch features at offset {offset}: {e}"
                        ) from e

                if batch_too_large:
                    # Cancel/drain remaining futures to avoid duplicate requests
                    from concurrent.futures import CancelledError

                    for remaining_idx in range(failed_at_index + 1, len(futures)):
                        _, _, remaining_future = futures[remaining_idx]
                        remaining_future.cancel()
                        # Drain any that couldn't be cancelled
                        try:
                            remaining_future.result(timeout=0.1)
                        except (CancelledError, BatchTooLargeError, Exception):
                            pass  # Expected - just draining

                    # Clear for retry
                    futures.clear()
                    results.clear()

                    # Reduce batch size and restart from batch_start
                    new_batch = _get_reduced_batch_size(failed_batch_size or effective_batch)
                    if new_batch is None:
                        raise GeoParquetError(
                            "Server cannot handle even batch_size=1. "
                            "This layer may have geometry too complex to download."
                        )
                    warn(
                        f"Server returned error for batch_size={failed_batch_size}. "
                        f"Reducing to {new_batch} and retrying..."
                    )
                    effective_batch = new_batch
                    # Don't increment batch_start - retry from same position
                    continue

                # Sort by offset to maintain order
                results.sort(key=lambda x: x[0])

                # Yield pages in sequential order
                for _offset, page in results:
                    features = page.get("features", [])
                    if not features:
                        continue

                    # Check limit before yielding
                    if max_features is not None and fetched >= max_features:
                        break

                    yield page
                    fetched += len(features)

                # Move to next batch
                batch_start += max_workers * effective_batch

                # Stop if we've hit the user limit
                if max_features is not None and fetched >= max_features:
                    break

        if verbose:
            debug(f"Fetched {fetched} features total using {max_workers} workers")


def _extract_crs_from_spatial_reference(spatial_ref: dict) -> dict | None:
    """Extract CRS as PROJJSON from ArcGIS spatial reference."""
    # ArcGIS uses WKID (Well-Known ID) which maps to EPSG codes
    wkid = spatial_ref.get("wkid") or spatial_ref.get("latestWkid")

    if wkid:
        # Handle special WKIDs
        epsg_code = WKID_TO_EPSG.get(wkid, wkid)
        return parse_crs_string_to_projjson(f"EPSG:{epsg_code}")

    # Fall back to WKT if provided
    wkt = spatial_ref.get("wkt")
    if wkt:
        return parse_crs_string_to_projjson(wkt)

    # Default to WGS84
    return parse_crs_string_to_projjson("EPSG:4326")


def _align_table_to_schema(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """
    Align a table's columns to match a target schema.

    This handles three types of mismatches between DuckDB output and ArcGIS metadata:
    1. Column order differences - reorders columns to match target schema
    2. Extra columns - drops columns not in target schema
    3. Missing columns - adds null columns of the correct type

    This is critical for handling schema variance in paginated ArcGIS responses
    (issue #334), where different batches may have different column ordering or
    missing/extra fields.

    Args:
        table: Source table from DuckDB with potentially different column order
        target_schema: Target schema from ArcGIS layer metadata

    Returns:
        Table with columns aligned to target schema
    """
    source_columns = set(table.column_names)
    target_columns = [field.name for field in target_schema]

    aligned_arrays = []
    for field in target_schema:
        if field.name in source_columns:
            # Column exists - select it (handles reordering)
            col = table.column(field.name)
            aligned_arrays.append(col)
        else:
            # Column missing - create null array of correct type
            null_array = pa.nulls(table.num_rows, type=field.type)
            aligned_arrays.append(null_array)

    # Create new table with aligned columns (automatically drops extra columns)
    return pa.table(dict(zip(target_columns, aligned_arrays, strict=True)))


def _build_schema_from_layer_info(layer_info: ArcGISLayerInfo) -> pa.Schema:
    """
    Build a fixed PyArrow schema from ArcGIS layer metadata.

    This prevents schema mismatches when different batches infer different
    types for the same field (e.g., nulls in batch 1 vs actual values in batch 2).

    Args:
        layer_info: ArcGIS layer metadata containing field definitions

    Returns:
        PyArrow schema with geometry column + attribute fields
    """
    # Map esriFieldType to PyArrow types
    # Reference: https://developers.arcgis.com/rest/services-reference/enterprise/fields/
    TYPE_MAPPING = {
        "esriFieldTypeSmallInteger": pa.int16(),
        "esriFieldTypeInteger": pa.int32(),
        "esriFieldTypeSingle": pa.float32(),
        "esriFieldTypeDouble": pa.float64(),
        "esriFieldTypeString": pa.string(),
        "esriFieldTypeDate": pa.timestamp("ms"),
        "esriFieldTypeOID": pa.int64(),
        "esriFieldTypeGeometry": pa.binary(),  # Shouldn't appear in fields, but handle defensively
        "esriFieldTypeBlob": pa.binary(),
        "esriFieldTypeGUID": pa.string(),
        "esriFieldTypeGlobalID": pa.string(),
        "esriFieldTypeXML": pa.string(),
    }

    fields = []

    # Geometry column always comes first (WKB binary)
    # Some features may have null geometries (attributes without spatial data)
    fields.append(pa.field("geometry", pa.binary(), nullable=True))

    # Add attribute fields based on layer metadata
    for field_info in layer_info.fields:
        field_name = field_info["name"]
        field_type = field_info["type"]
        nullable = field_info.get("nullable", True)

        # Map esriFieldType to PyArrow type
        if field_type in TYPE_MAPPING:
            pa_type = TYPE_MAPPING[field_type]
        else:
            # Unknown type - fallback to string with warning
            warn(
                f"Unknown ArcGIS field type '{field_type}' for field '{field_name}'. "
                f"Falling back to string type."
            )
            pa_type = pa.string()

        fields.append(pa.field(field_name, pa_type, nullable=nullable))

    return pa.schema(fields)


def _geojson_page_to_table(
    features: list[dict],
) -> pa.Table | None:
    """
    Convert a page of GeoJSON features to PyArrow Table with WKB geometry.

    Uses DuckDB's spatial extension for geometry conversion.
    This function is designed to handle a single page (~2000 features)
    to keep memory usage low.

    Args:
        features: List of GeoJSON feature dicts (typically one page)

    Returns:
        PyArrow Table with WKB geometry column, or None if no features
    """
    if not features:
        return None

    # Create a temporary GeoJSON string for DuckDB to parse
    geojson_collection = json.dumps(
        {
            "type": "FeatureCollection",
            "features": features,
        }
    )

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    temp_file = tempfile.gettempdir() + f"/arcgis_page_{uuid.uuid4()}.geojson"

    try:
        with open(temp_file, "w") as f:
            f.write(geojson_collection)

        # Read GeoJSON and convert geometry to WKB
        # Note: DuckDB ST_Read adds OGC_FID column, which we exclude
        query = f"""
            SELECT
                ST_AsWKB(geom) as geometry,
                * EXCLUDE (geom, OGC_FID)
            FROM ST_Read('{temp_file}')
        """

        table = con.execute(query).arrow().read_all()
        return table

    finally:
        con.close()
        if os.path.exists(temp_file):
            os.unlink(temp_file)


def _stream_features_to_parquet(
    service_url: str,
    layer_info: ArcGISLayerInfo,
    output_path: str,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    out_fields: str = "*",
    max_features: int | None = None,
    token: str | None = None,
    batch_size: int | None = None,
    max_workers: int = 1,
    verbose: bool = False,
) -> int:
    """
    Stream features from ArcGIS to a Parquet file page by page.

    This is memory-efficient as it only keeps one page (~2000 features)
    in memory at a time. The output is a raw parquet file without
    Hilbert ordering or bbox column (those are applied in a second pass).

    Args:
        service_url: ArcGIS Feature Service URL
        layer_info: Layer metadata
        output_path: Path to write the parquet file
        where: SQL WHERE clause filter
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        out_fields: Comma-separated field names or "*" for all
        max_features: Maximum total features to return (limit)
        token: Optional authentication token
        batch_size: Custom batch size for pagination
        max_workers: Number of concurrent requests (1 = sequential, 2-3 recommended)
        verbose: Whether to print debug output

    Returns:
        Number of features written
    """
    # Build fixed schema from layer metadata upfront to prevent type mismatches
    # between batches (issue #290)
    target_schema = _build_schema_from_layer_info(layer_info)

    # Filter schema to match requested fields (if out_fields specified)
    if out_fields != "*":
        requested_fields = {f.strip().lower() for f in out_fields.split(",")}
        # Always include geometry
        filtered_fields = [target_schema.field("geometry")]
        # Add only requested attribute fields (case-insensitive match)
        for field in target_schema:
            if field.name != "geometry" and field.name.lower() in requested_fields:
                filtered_fields.append(field)
        target_schema = pa.schema(filtered_fields)

    debug(f"Built schema from layer metadata: {len(target_schema)} fields")

    writer = None
    total_rows = 0
    page_count = 0

    try:
        for page in fetch_all_features(
            service_url,
            layer_info,
            where,
            bbox=bbox,
            out_fields=out_fields,
            max_features=max_features,
            token=token,
            batch_size=batch_size,
            max_workers=max_workers,
            verbose=verbose,
        ):
            features = page.get("features", [])
            if not features:
                continue

            # Convert this page to Arrow table
            page_table = _geojson_page_to_table(features)
            if page_table is None:
                continue

            page_count += 1

            # Align columns to target schema (handles order/missing/extra columns)
            # This is required because DuckDB may return columns in different order
            # than ArcGIS metadata, or some batches may have different fields
            page_table = _align_table_to_schema(page_table, target_schema)

            # Cast to fixed schema (handles type mismatches between batches)
            try:
                page_table = page_table.cast(target_schema, safe=True)
            except pa.ArrowInvalid as e:
                # If safe casting fails, try to provide helpful error message
                raise GeoParquetError(
                    f"Failed to cast batch {page_count} to target schema. "
                    f"This may indicate data corruption or unexpected types from the service. "
                    f"Error: {e}"
                ) from e

            # Initialize writer with fixed schema on first page
            if writer is None:
                writer = pq.ParquetWriter(output_path, target_schema)

            # Write this page
            writer.write_table(page_table)
            total_rows += page_table.num_rows

            # Free memory from this page
            del page_table

        debug(f"Streamed {total_rows} features in {page_count} pages to temp file")
        return total_rows

    finally:
        if writer is not None:
            writer.close()


def arcgis_to_table(
    service_url: str,
    auth: ArcGISAuth | None = None,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    limit: int | None = None,
    batch_size: int | None = None,
    max_workers: int = 1,
    verbose: bool = False,
) -> pa.Table:
    """
    Convert ArcGIS Feature Service to PyArrow Table.

    Uses a memory-efficient two-pass approach:
    1. Stream features page-by-page to a temp parquet file
    2. Read the parquet file back as an Arrow table

    This keeps memory usage low during download (only one page at a time),
    while still producing a complete Arrow table for further processing.

    Server-side filtering is applied to minimize data transfer:
    - where: SQL WHERE clause pushed to server
    - bbox: Spatial filter pushed to server
    - include_cols: Field selection pushed to server (outFields)
    - limit: Row limit applied during pagination

    Args:
        service_url: ArcGIS Feature Service URL (with layer ID)
        auth: Optional authentication configuration
        where: SQL WHERE clause filter
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        include_cols: Comma-separated column names to include (server-side)
        exclude_cols: Comma-separated column names to exclude (client-side after download)
        limit: Maximum number of features to return
        batch_size: Custom batch size for pagination
        max_workers: Number of concurrent requests (1 = sequential, 2-3 recommended)
        verbose: Whether to print debug output

    Returns:
        PyArrow Table with WKB geometry column
    """
    configure_verbose(verbose)

    # Validate URL
    service_url, layer_id = validate_arcgis_url(service_url)

    # Resolve authentication
    token = resolve_token(auth, service_url, verbose) if auth else None

    # Get layer info (with WHERE and bbox filters applied to count)
    layer_info = get_layer_info(service_url, token=token, where=where, bbox=bbox, verbose=verbose)
    debug(f"Layer: {layer_info.name}")
    debug(f"Geometry type: {layer_info.geometry_type}")
    debug(f"Total features matching filter: {layer_info.total_count}")

    if layer_info.total_count == 0:
        filters_applied = where != "1=1" or bbox is not None
        if filters_applied:
            filter_desc = []
            if where != "1=1":
                filter_desc.append(f"where='{where}'")
            if bbox:
                filter_desc.append(f"bbox={bbox}")
            warn(f"No features match filter: {', '.join(filter_desc)}")
        else:
            warn("Layer has no features")
        # Return empty table with geometry column
        return pa.table({"geometry": pa.array([], type=pa.binary())})

    # Determine outFields for server-side column selection
    out_fields = "*"
    if include_cols:
        # Always include geometry-related fields
        fields = [f.strip() for f in include_cols.split(",")]
        out_fields = ",".join(fields)
        debug(f"Requesting fields: {out_fields}")

    # Pass 1: Stream features to temp parquet file (memory-efficient)
    temp_parquet = tempfile.gettempdir() + f"/arcgis_stream_{uuid.uuid4()}.parquet"

    try:
        progress("Streaming features to temp file...")
        total_rows = _stream_features_to_parquet(
            service_url=service_url,
            layer_info=layer_info,
            output_path=temp_parquet,
            where=where,
            bbox=bbox,
            out_fields=out_fields,
            max_features=limit,
            token=token,
            batch_size=batch_size,
            max_workers=max_workers,
            verbose=verbose,
        )

        if total_rows == 0:
            raise GeoParquetError("No features returned from service")

        # Pass 2: Read temp parquet file back as Arrow table
        progress("Reading temp file...")
        table = pq.read_table(temp_parquet)

        # Apply client-side column exclusion if specified
        if exclude_cols:
            cols_to_exclude = {c.strip() for c in exclude_cols.split(",")}
            # Keep geometry column unless explicitly excluded
            cols_to_keep = [name for name in table.column_names if name not in cols_to_exclude]
            if cols_to_keep:
                table = table.select(cols_to_keep)
                debug(f"Excluded columns: {cols_to_exclude}")

        # Add CRS to metadata
        # Always use CRS84 (WGS84 lon/lat) because we request f=geojson,
        # which per RFC 7946 is always WGS84 regardless of the layer's native SR
        crs = parse_crs_string_to_projjson("OGC:CRS84")
        if crs:
            geo_metadata = {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "crs": crs,
                        "geometry_types": [
                            ARCGIS_GEOM_TYPES.get(layer_info.geometry_type, "Geometry")
                        ],
                    }
                },
            }

            # Update table schema with geo metadata
            existing_metadata = table.schema.metadata or {}
            new_metadata = {**existing_metadata, b"geo": json.dumps(geo_metadata).encode("utf-8")}
            table = table.replace_schema_metadata(new_metadata)

        success(f"Converted {table.num_rows} features")
        return table

    finally:
        # Clean up temp file
        if os.path.exists(temp_parquet):
            os.unlink(temp_parquet)


def convert_arcgis_to_geoparquet(
    service_url: str,
    output_file: str,
    token: str | None = None,
    token_file: str | None = None,
    username: str | None = None,
    password: str | None = None,
    portal_url: str | None = None,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    limit: int | None = None,
    skip_hilbert: bool = False,
    skip_bbox: bool = False,
    max_workers: int = 1,
    batch_size: int | None = None,
    compression: str = "ZSTD",
    compression_level: int = 15,
    verbose: bool = False,
    geoparquet_version: str | None = None,
    profile: str | None = None,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    overwrite: bool = False,
) -> None:
    """
    Convert ArcGIS Feature Service to GeoParquet file.

    Main CLI entry point for ArcGIS to GeoParquet conversion.

    Server-side filtering options (pushed to ArcGIS for efficiency):
    - where: SQL WHERE clause
    - bbox: Spatial bounding box filter
    - include_cols: Select specific fields to download
    - limit: Maximum number of features to download

    Args:
        service_url: ArcGIS Feature Service URL
        output_file: Output file path (local or remote)
        token: Direct authentication token
        token_file: Path to file containing token
        username: ArcGIS username (requires password)
        password: ArcGIS password (requires username)
        portal_url: Enterprise portal URL for token generation
        where: SQL WHERE clause filter (pushed to server)
        bbox: Bounding box filter (xmin,ymin,xmax,ymax in WGS84, pushed to server)
        include_cols: Comma-separated columns to include (pushed to server)
        exclude_cols: Comma-separated columns to exclude (applied client-side)
        limit: Maximum number of features to return
        skip_hilbert: Skip Hilbert spatial ordering
        skip_bbox: Skip adding bbox column for spatial query optimization
        max_workers: Number of concurrent requests (1 = sequential, 2-3 recommended)
        batch_size: Features per request (default: server's maxRecordCount, auto-reduces on error)
        compression: Compression codec (ZSTD, GZIP, etc.)
        compression_level: Compression level
        verbose: Whether to print verbose output
        geoparquet_version: GeoParquet version to write
        profile: AWS profile for S3 output
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
    """
    configure_verbose(verbose)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, output_file)

    # Check if output file exists and overwrite is False
    if not overwrite and Path(output_file).exists():
        raise GeoParquetError(
            f"Output file already exists: {output_file}. Use --overwrite to replace it."
        )

    # Build auth config
    auth = None
    if any([token, token_file, username, password]):
        auth = ArcGISAuth(
            token=token,
            token_file=token_file,
            username=username,
            password=password,
            portal_url=portal_url,
        )

    # Convert to Arrow table with server-side filtering
    table = arcgis_to_table(
        service_url=service_url,
        auth=auth,
        where=where,
        bbox=bbox,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        limit=limit,
        batch_size=batch_size,
        max_workers=max_workers,
        verbose=verbose,
    )

    # Apply Hilbert ordering if not skipped
    if not skip_hilbert and table.num_rows > 0:
        progress("Applying Hilbert spatial ordering...")
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        table = hilbert_order_table(table)

    # Add bbox column for spatial query optimization
    if not skip_bbox and table.num_rows > 0:
        progress("Adding bbox column for spatial query optimization...")
        from geoparquet_io.core.add.bbox import add_bbox_table

        table = add_bbox_table(table, bbox_column_name="bbox", geometry_column="geometry")

    # Write to GeoParquet
    progress(f"Writing to {output_file}...")
    write_geoparquet_table(
        table,
        output_file,
        geometry_column="geometry",
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
        profile=profile,
    )

    success(f"Converted {table.num_rows} features to {output_file}")
