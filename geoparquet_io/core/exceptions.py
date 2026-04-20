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
