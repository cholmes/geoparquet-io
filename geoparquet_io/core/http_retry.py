"""
Shared HTTP request utilities with retry logic.

This module provides reusable HTTP request functions with:
- Exponential backoff retry on transient errors
- Connection pooling via shared httpx client
- Gzip compression support
- Proper error classification (retryable vs. fatal)

Used by: arcgis.py, wfs.py
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any, cast

from geoparquet_io.core.exceptions import BatchTooLargeError, RemoteAccessError
from geoparquet_io.core.logging_config import warn

if TYPE_CHECKING:
    import httpx

# Module-level HTTP client for connection pooling
_shared_http_client: httpx.Client | None = None

# Default timeout and retry settings
DEFAULT_TIMEOUT = 60.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0


def get_shared_http_client(
    timeout: float = DEFAULT_TIMEOUT,
    http2: bool = False,
    max_connections: int = 20,
) -> httpx.Client:
    """
    Get or create a shared HTTP client for connection pooling.

    Reuses TCP connections across requests, saving ~100-200ms per request
    on TLS handshakes.

    Args:
        timeout: Request timeout in seconds
        http2: Enable HTTP/2 (disabled by default for ArcGIS compatibility)
        max_connections: Maximum number of connections in pool

    Returns:
        Shared httpx.Client instance
    """
    global _shared_http_client
    import httpx

    if _shared_http_client is None:
        _shared_http_client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            http2=http2,
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections,
            ),
        )

    return _shared_http_client


def reset_http_client() -> None:
    """
    Reset the shared HTTP client (for connection errors or cleanup).

    Closes the existing client and allows a new one to be created.
    """
    global _shared_http_client

    if _shared_http_client is not None:
        _shared_http_client.close()
        _shared_http_client = None


def make_request_with_retry(
    method: str,
    url: str,
    params: dict | None = None,
    data: dict | None = None,
    headers: dict | None = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
    timeout: float = DEFAULT_TIMEOUT,
    parse_json: bool = True,
    batch_size: int | None = None,
) -> dict[str, Any] | bytes:
    """
    Make HTTP request with retry logic and proper error handling.

    Args:
        method: HTTP method ("GET" or "POST")
        url: Request URL
        params: Query parameters (for GET)
        data: Form data (for POST)
        headers: Additional headers (Accept-Encoding: gzip added automatically)
        max_retries: Number of retry attempts
        retry_delay: Base delay between retries (exponential backoff)
        timeout: Request timeout in seconds
        parse_json: If True, parse response as JSON and raise BatchTooLargeError
            on parse failure. If False, return raw bytes.
        batch_size: Current batch size (used for BatchTooLargeError context)

    Returns:
        Parsed JSON dict if parse_json=True, otherwise raw bytes

    Raises:
        RemoteAccessError: For fatal HTTP errors (401, 403, 404) or exhausted retries
        BatchTooLargeError: When JSON parsing fails (server returned HTML error)
    """
    import httpx

    last_exception: Exception | None = None

    # Build headers with compression support
    request_headers = {"Accept-Encoding": "gzip, deflate"}
    if headers:
        request_headers.update(headers)

    for attempt in range(max_retries):
        try:
            client = get_shared_http_client(timeout=timeout)

            if method == "GET":
                response = client.get(url, params=params, headers=request_headers)
            else:
                response = client.post(url, data=data, headers=request_headers)

            response.raise_for_status()

            if parse_json:
                try:
                    return cast(dict[str, Any], response.json())
                except json.JSONDecodeError as e:
                    # Server returned non-JSON (likely HTML error page)
                    # This is NOT retryable with same params - batch is too large
                    content_preview = response.text[:200] if response.text else "(empty)"
                    raise BatchTooLargeError(
                        url=url,
                        batch_size=batch_size or 0,
                        reason=f"Server returned non-JSON response: {content_preview}...",
                    ) from e
            else:
                return bytes(response.content)

        except httpx.RemoteProtocolError as e:
            # Server disconnected - reset connection pool and retry
            last_exception = e
            warn(f"HTTP protocol error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                reset_http_client()
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

            # Retry on rate limit or server errors
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

            # Fatal errors - don't retry
            if status == 401:
                raise RemoteAccessError(
                    url, "Authentication required. Use --token or --username/--password."
                ) from None
            if status == 403:
                raise RemoteAccessError(
                    url, "Access denied. Check your credentials and service permissions."
                ) from None
            if status == 404:
                raise RemoteAccessError(url, "Service not found (404). Check the URL.") from None

            raise RemoteAccessError(url, f"HTTP error {status}: {e}") from e

        except BatchTooLargeError:
            # Don't retry BatchTooLargeError - caller needs to reduce batch size
            raise

    raise RemoteAccessError(url, f"Request failed after {max_retries} attempts: {last_exception}")
