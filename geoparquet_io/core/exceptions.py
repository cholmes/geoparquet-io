"""
Framework-agnostic exceptions for geoparquet-io core modules.

Core modules must NOT import click or raise click exceptions.
These exceptions are converted to click exceptions at the CLI boundary
by the cli/exception_handler.py module.

Exception Hierarchy:
    GeoParquetError (base)
    ├── FileNotFoundGeoParquetError - file/path not found
    ├── InvalidParameterError - invalid function argument
    ├── RemoteAccessError - S3/GCS/Azure access issues
    ├── GeometryError - geometry column issues
    ├── PartitionError - partitioning failures
    └── ValidationError - spec validation failures
"""

from __future__ import annotations


def sanitize_url_for_logging(url: str) -> str:
    """Remove credentials and query params from URL for safe logging.

    Presigned URLs contain credentials in query parameters that should
    not be logged. This function strips the query string and truncates
    long paths for readability while preserving the filename.

    Args:
        url: URL that may contain sensitive query parameters

    Returns:
        Sanitized URL safe for logging, with long paths truncated but
        filename preserved (e.g., "s3://bucket/prefix/.../file.parquet")
    """
    if not url:
        return url
    # Remove query string (may contain presigned credentials)
    if "?" in url:
        url = url.split("?")[0]
    # Truncate very long paths but keep filename for debugging
    parts = url.split("/")
    if len(parts) > 5:
        return "/".join(parts[:4]) + "/..." + "/" + parts[-1]
    return url


class GeoParquetError(Exception):
    """Base exception for all geoparquet-io errors."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class FileNotFoundGeoParquetError(GeoParquetError):
    """Raised when a required file or path is not found."""

    def __init__(self, path: str, detail: str | None = None) -> None:
        self.path = path  # Original path for programmatic access
        sanitized = self._sanitize_path(path)
        msg = f"File not found: {sanitized}"
        if detail:
            msg = f"{msg} - {detail}"
        super().__init__(msg)

    @staticmethod
    def _sanitize_path(path: str) -> str:
        """Remove credentials and query params from path for safe logging.

        Presigned URLs contain sensitive credentials in query parameters
        (e.g., AWSAccessKeyId, Signature, X-Amz-Security-Token). These must
        be stripped before including the path in error messages.
        """
        # Remove query string (may contain presigned credentials)
        if "?" in path:
            path = path.split("?")[0]
        return path


class InvalidParameterError(GeoParquetError):
    """Raised when a function parameter has an invalid value."""

    def __init__(self, param_name: str, reason: str) -> None:
        self.param_name = param_name
        self.reason = reason
        super().__init__(f"Invalid parameter '{param_name}': {reason}")


class RemoteAccessError(GeoParquetError):
    """Raised when remote file access (S3/GCS/Azure) fails."""

    def __init__(self, url: str, reason: str) -> None:
        # Sanitize URL to avoid logging credentials
        self.url = sanitize_url_for_logging(url)
        self.reason = reason
        super().__init__(f"Remote access error for {self.url}: {reason}")


class GeometryError(GeoParquetError):
    """Raised when geometry column operations fail."""

    pass


class PartitionError(GeoParquetError):
    """Raised when partitioning operations fail."""

    pass


class ValidationError(GeoParquetError):
    """Raised when GeoParquet validation fails."""

    pass


# Last DuckDB release for which the 'geography' community extension is published.
# Newer releases 404 from community-extensions.duckdb.org: the build fails on a
# C++11/C++17 link error in DuckDB's plan_serializer tool, added in DuckDB 1.5.2
# (duckdb/duckdb#22097). The fix is merged upstream
# (paleolimbot/duckdb-geography#34); republication for newer DuckDB is pending.
GEOGRAPHY_LAST_PUBLISHED_DUCKDB = "1.5.1"

# Extension-specific guidance appended to ExtensionUnavailableError. Only add an
# entry when we know something the generic message cannot say.
_EXTENSION_HINTS = {
    "geography": (
        f"'geography' is currently published only up to DuckDB "
        f"{GEOGRAPHY_LAST_PUBLISHED_DUCKDB}, and gpio requires a newer DuckDB. "
        f"A build fix has been merged upstream (paleolimbot/duckdb-geography#34) "
        f"and republication is pending, so S2 support should return without any "
        f"action on your part. To use S2 before then, install the last DuckDB "
        f"that provides the extension: "
        f"uv pip install 'duckdb=={GEOGRAPHY_LAST_PUBLISHED_DUCKDB}' "
        f"(note that DuckDB {GEOGRAPHY_LAST_PUBLISHED_DUCKDB} can segfault while "
        f"repairing invalid geometry — see geoparquet-io issue #737)."
    ),
}


class ExtensionUnavailableError(GeoParquetError):
    """Raised when a required DuckDB community extension cannot be installed/loaded.

    Community extensions (e.g. ``geography`` for S2 support) are built per
    DuckDB release. When a new DuckDB version ships before an extension has
    been rebuilt for it, ``INSTALL ... FROM community`` fails with an opaque
    HTTP 404. This exception surfaces an actionable message instead.
    """

    def __init__(
        self,
        name: str,
        duckdb_version: str,
        detail: str | None = None,
        feature: str | None = None,
    ) -> None:
        self.name = name
        self.duckdb_version = duckdb_version
        self.feature = feature
        subject = f"'{feature}' requires" if feature else "This operation requires"
        msg = (
            f"{subject} the DuckDB community extension '{name}', which could not "
            f"be loaded for DuckDB {duckdb_version}. Community extensions are built "
            f"per DuckDB release, so '{name}' may not be published for this version "
            f"yet (see "
            f"https://community-extensions.duckdb.org/extensions/{name}.html)."
        )
        hint = _EXTENSION_HINTS.get(name)
        if hint:
            msg = f"{msg} {hint}"
        if detail:
            msg = f"{msg} Original error: {detail}"
        super().__init__(msg)


class BatchTooLargeError(GeoParquetError):
    """Raised when server returns non-JSON response due to batch size limits.

    This typically happens when a server's actual payload limit is lower than
    its advertised maxRecordCount. The caller should retry with a smaller batch.
    """

    def __init__(self, url: str, batch_size: int, reason: str) -> None:
        self.url = sanitize_url_for_logging(url)
        self.batch_size = batch_size
        self.reason = reason
        super().__init__(f"Batch size {batch_size} too large for {self.url}: {reason}")
