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
    """Raised when partitioning operations fail.

    Attributes:
        result: The run this failure came out of, when there is one. A directory
            sub-partition run (`ops.sub_partition_by_*`) processes many files and
            raises only at the end, so the caller needs what succeeded as well as
            what failed (#811). ``None`` for a single-file partition failure,
            which has no partial run to report.
    """

    def __init__(self, message: str, result: dict | None = None) -> None:
        super().__init__(message)
        self.result = result


class ValidationError(GeoParquetError):
    """Raised when GeoParquet validation fails."""

    pass


# DuckDB's own text for "this extension is not built/published for your DuckDB".
# The HTTP status is the whole signal. Verified against DuckDB 1.5.5:
#
#   unpublished  HTTPException: 'HTTP Error: Failed to download extension
#                "geography" at URL "..." (HTTP 404)'
#   offline      IOException:   'IO Error: Failed to download extension
#                "..." at URL "..." (ERROR Could not establish connection)'
#
# Both say "Failed to download extension", so that phrase decides nothing -- it
# was matching an offline user's error and telling them to wait for an upstream
# republication that has nothing to do with their problem (#778). Only the 404
# means the registry answered and does not have this build.
_NOT_PUBLISHED_SIGNATURES = ("http 404",)


def is_unpublished_extension_error(exc: BaseException | str | None) -> bool:
    """True when `exc` carries DuckDB's "not published for this version" signature.

    Accepts an exception (whose ``__cause__``/``__context__`` chain is walked) or
    the raw error text. Deliberately does NOT match gpio's own wording, which is
    present on every :class:`ExtensionUnavailableError` regardless of cause.
    """
    if exc is None:
        return False
    if isinstance(exc, str):
        blob = exc.lower()
    else:
        parts: list[str] = []
        seen: set[int] = set()
        cur: BaseException | None = exc
        while cur is not None and id(cur) not in seen:
            seen.add(id(cur))
            parts.append(str(cur))
            cur = cur.__cause__ or cur.__context__
        blob = " ".join(parts).lower()
    return any(sig in blob for sig in _NOT_PUBLISHED_SIGNATURES)


# Extension-specific guidance, shown only when the extension really is
# unpublished. Only add an entry when we know something the generic message
# cannot say.
_UNPUBLISHED_EXTENSION_HINTS = {
    "geography": (
        "The upstream build fix is merged (paleolimbot/duckdb-geography#34), so "
        "S2 returns on its own once the extension is republished -- no gpio "
        "change needed. For a hierarchical cell index that works today, use "
        "`gpio add a5` or `gpio partition a5`."
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
        self.unpublished = is_unpublished_extension_error(detail)
        subject = f"'{feature}' requires" if feature else "This operation requires"
        listing = f"https://community-extensions.duckdb.org/extensions/{name}.html"
        msg = (
            f"{subject} the DuckDB community extension '{name}', which could not "
            f"be loaded for DuckDB {duckdb_version}."
        )
        if self.unpublished:
            # DuckDB said "not published for this version" outright.
            msg += (
                f" Community extensions are built per DuckDB release, and '{name}' "
                f"is not published for this one (see {listing})."
            )
            hint = _UNPUBLISHED_EXTENSION_HINTS.get(name)
            if hint:
                msg = f"{msg} {hint}"
        elif detail:
            # A failure that is NOT a 404: do not blame the extension registry.
            msg += (
                " This is not the 'not published for this DuckDB version' 404, so "
                "check that https://community-extensions.duckdb.org is reachable "
                "from here (proxy, firewall, offline)."
            )
        else:
            # No underlying error to reason from: state the possibility, claim nothing.
            msg += (
                f" Community extensions are built per DuckDB release, so '{name}' "
                f"may not be published for this version yet (see {listing})."
            )
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
