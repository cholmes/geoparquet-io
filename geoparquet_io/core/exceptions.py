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


class GeoParquetError(Exception):
    """Base exception for all geoparquet-io errors."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class FileNotFoundGeoParquetError(GeoParquetError):
    """Raised when a required file or path is not found."""

    def __init__(self, path: str, detail: str | None = None) -> None:
        self.path = path
        msg = f"File not found: {path}"
        if detail:
            msg = f"{msg} - {detail}"
        super().__init__(msg)


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
        self.url = self._sanitize_url(url)
        self.reason = reason
        super().__init__(f"Remote access error for {self.url}: {reason}")

    @staticmethod
    def _sanitize_url(url: str) -> str:
        """Remove credentials and query params from URL for safe logging."""
        # Remove query string (may contain presigned credentials)
        if "?" in url:
            url = url.split("?")[0]
        # Truncate path for readability
        parts = url.split("/")
        if len(parts) > 4:
            return "/".join(parts[:4]) + "/..."
        return url


class GeometryError(GeoParquetError):
    """Raised when geometry column operations fail."""

    pass


class PartitionError(GeoParquetError):
    """Raised when partitioning operations fail."""

    pass


class ValidationError(GeoParquetError):
    """Raised when GeoParquet validation fails."""

    pass
