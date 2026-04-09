"""File path utilities for handling local and remote files.

This module provides utilities for working with file paths, including:
- Glob pattern detection
- Partition path resolution (Hive-style partitioning)
- Safe URL handling for DuckDB
- Remote error message generation
- Parquet extension validation

Example:
    >>> from geoparquet_io.core.file_utils import safe_file_url, has_glob_pattern
    >>> has_glob_pattern("data/*.parquet")
    True
    >>> safe_file_url("https://example.com/data.parquet")
    'https://example.com/data.parquet'
"""

import glob as glob_module
import os
import urllib.parse

import click

from geoparquet_io.core.logging_config import debug
from geoparquet_io.core.remote import is_remote_url


def has_glob_pattern(path: str) -> bool:
    """
    Check if path contains glob wildcards.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path contains glob characters (*, ?, [), False otherwise
    """
    return any(c in path for c in ("*", "?", "["))


def is_partition_path(path: str) -> bool:
    """
    Check if path represents a partitioned dataset.

    Detects:
    - Local directories containing parquet files
    - Paths with glob patterns (*, ?, [)
    - Hive-style paths (key=value in path) for remote URLs

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path appears to be a partitioned dataset
    """
    # Check for glob patterns
    if has_glob_pattern(path):
        return True

    # Check if local path is a directory
    if not is_remote_url(path) and os.path.isdir(path):
        return True

    # Check for hive-style partitioning in remote URLs (key=value in path components)
    if is_remote_url(path):
        # Extract path portion after scheme and host
        # e.g., s3://bucket/prefix/country=US/data.parquet -> prefix/country=US/data.parquet
        path_parts = path.split("/")
        # Check if any path component contains = (hive-style partition)
        for part in path_parts[3:]:  # Skip scheme://host/bucket parts
            if "=" in part and not part.endswith(".parquet"):
                return True

    return False


def resolve_partition_path(path: str, hive_partitioning: bool | None = None) -> tuple[str, dict]:
    """
    Resolve a partition path to a format DuckDB can read.

    For directories, converts to glob pattern. Returns the resolved path
    and read_parquet options dict.

    Args:
        path: File path or URL (may be directory or glob pattern)
        hive_partitioning: Explicitly enable/disable hive partitioning.
                          If None, auto-detect from path structure.

    Returns:
        tuple: (resolved_path, read_options_dict)
            - resolved_path: Path/glob pattern for DuckDB
            - read_options_dict: Options for read_parquet (hive_partitioning, etc.)
    """
    options = {}
    resolved = path

    # Handle local directories
    if not is_remote_url(path) and os.path.isdir(path):
        try:
            items = os.listdir(path)
            subdirs = [d for d in items if os.path.isdir(os.path.join(path, d))]
            has_parquet_files = any(
                f.endswith(".parquet") for f in items if not os.path.isdir(os.path.join(path, f))
            )
            has_hive_subdirs = any("=" in d for d in subdirs)

            if has_hive_subdirs:
                # Hive-style partitioning with key=value directories
                resolved = os.path.join(path, "**", "*.parquet")
                options["hive_partitioning"] = True
            elif subdirs and not has_parquet_files:
                # Directory has subdirectories but no parquet files at top level
                # Use recursive glob to find parquet files in subdirectories
                resolved = os.path.join(path, "**", "*.parquet")
            elif has_parquet_files:
                # Flat directory with parquet files at top level
                resolved = os.path.join(path, "*.parquet")
            else:
                # Fallback - try recursive
                resolved = os.path.join(path, "**", "*.parquet")
        except OSError:
            # If we can't read the directory, use recursive glob
            resolved = os.path.join(path, "**", "*.parquet")

    # If path contains hive-style markers and hive_partitioning not explicitly set
    # Check path components (directories) for hive-style key=value patterns
    # Exclude glob patterns and the final filename from the check
    if hive_partitioning is None:
        path_parts = resolved.replace("\\", "/").split("/")
        # Check directory components (not filename or glob patterns like ** or *.parquet)
        dir_parts = [p for p in path_parts[:-1] if p and p not in ("**", "*")]
        has_hive_dirs = any("=" in part for part in dir_parts)
        if has_hive_dirs:
            options["hive_partitioning"] = True
    elif hive_partitioning is not None:
        options["hive_partitioning"] = hive_partitioning

    return resolved, options


def get_first_parquet_file(partition_path: str) -> str | None:
    """
    Get the first parquet file from a partitioned dataset.

    Used for metadata inspection when only need to check one file.

    Args:
        partition_path: Directory path or glob pattern

    Returns:
        str: Path to first parquet file, or None if none found
    """
    if is_remote_url(partition_path):
        # For remote, can't easily enumerate - return original path
        # Caller should handle this case
        return partition_path

    if os.path.isdir(partition_path):
        # Walk directory to find first parquet file
        for root, _dirs, files in os.walk(partition_path):
            for f in sorted(files):  # Sort for consistent ordering
                if f.endswith(".parquet"):
                    return os.path.join(root, f)
        return None

    if has_glob_pattern(partition_path):
        # Use glob to find first match
        matches = glob_module.glob(partition_path, recursive=True)
        parquet_matches = [m for m in sorted(matches) if m.endswith(".parquet")]
        return parquet_matches[0] if parquet_matches else None

    # Single file
    return partition_path


def get_all_parquet_files(partition_path: str) -> list[str]:
    """
    Get all parquet files from a partitioned dataset.

    Args:
        partition_path: Directory path or glob pattern

    Returns:
        list: List of paths to all parquet files, sorted for consistent ordering
    """
    if is_remote_url(partition_path):
        # For remote, can't easily enumerate - return as single item
        return [partition_path]

    if os.path.isdir(partition_path):
        # Walk directory to find all parquet files
        parquet_files = []
        for root, _dirs, files in os.walk(partition_path):
            for f in files:
                if f.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, f))
        return sorted(parquet_files)

    if has_glob_pattern(partition_path):
        # Use glob to find all matches
        matches = glob_module.glob(partition_path, recursive=True)
        return sorted([m for m in matches if m.endswith(".parquet")])

    # Single file
    return [partition_path] if os.path.exists(partition_path) else []


def safe_file_url(file_path: str, verbose: bool = False) -> str:
    """
    Handle both local and remote files, returning safe URL.

    For remote URLs, performs URL encoding if needed.
    For local files, validates existence (unless it's a glob pattern).

    Args:
        file_path: Local file path or remote URL (may contain glob patterns)
        verbose: Whether to print verbose output

    Returns:
        str: Safe URL or file path

    Raises:
        click.BadParameter: If local file doesn't exist (non-glob paths only)
    """
    if is_remote_url(file_path):
        # Remote URL - URL encode if HTTP/HTTPS
        if file_path.startswith(("http://", "https://")):
            parsed = urllib.parse.urlparse(file_path)
            # Preserve glob wildcards and hive-style partition markers for DuckDB
            # These characters must not be encoded: * ? [ ] = , /
            duckdb_safe_chars = "/*?[]=,"
            encoded_path = urllib.parse.quote(parsed.path, safe=duckdb_safe_chars)
            safe_url = parsed._replace(path=encoded_path).geturl()
        else:
            safe_url = file_path

        if verbose:
            protocol = file_path.split("://")[0].upper() if "://" in file_path else "HTTP"
            debug(f"Reading from {protocol}: {safe_url}")
        return safe_url
    else:
        # Local file - check existence (skip for glob patterns, DuckDB will handle)
        if not has_glob_pattern(file_path) and not os.path.exists(file_path):
            raise click.BadParameter(f"Local file not found: {file_path}")
        return file_path


def get_remote_error_hint(error_msg: str, file_path: str = "") -> str:
    """
    Generate helpful error messages for remote file access failures.

    Args:
        error_msg: Original error message from DuckDB or other library
        file_path: The remote file path/URL that failed

    Returns:
        str: User-friendly error message with troubleshooting hints
    """
    # Simple pattern matching - check error type and return appropriate hint
    error_lower = error_msg.lower()
    path_lower = file_path.lower()

    # Check for 403/auth errors
    auth_error = "403" in error_msg or "forbidden" in error_lower or "access denied" in error_lower
    if auth_error:
        if "s3://" in path_lower:
            return (
                "Authentication required or access denied:\n"
                "  - S3: Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables\n"
                "  - Or configure ~/.aws/credentials file"
            )
        if "az://" in path_lower or "azure" in path_lower or "blob.core" in path_lower:
            return (
                "Authentication required or access denied:\n"
                "  - Azure: Check AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY\n"
                "  - Or set AZURE_STORAGE_SAS_TOKEN for SAS token auth"
            )
        if "gs://" in path_lower or "gcs://" in path_lower:
            return (
                "Authentication required or access denied:\n"
                "  - GCS: Check GOOGLE_APPLICATION_CREDENTIALS points to service account JSON"
            )
        return "Authentication required or access denied:\n  - File may be private or require authentication"

    # Check for 404 errors
    if "404" in error_msg or "not found" in error_lower or "does not exist" in error_lower:
        base = (
            "File not found at remote location:\n"
            "  - Verify the URL is correct\n"
            "  - Check the file exists at the specified path"
        )
        return f"{base}\n  - URL: {file_path}" if file_path else base

    # Check for timeout
    if "timeout" in error_lower or "timed out" in error_lower:
        return (
            "Connection timed out:\n"
            "  - Check network connectivity\n"
            "  - File may be very large - try a smaller file first\n"
            "  - Remote server may be slow or overloaded"
        )

    # Check for connection issues
    if "unable to connect" in error_lower or "connection" in error_lower:
        return (
            "Cannot connect to remote server:\n"
            "  - Check network connectivity\n"
            "  - Verify the hostname/URL is correct\n"
            "  - Server may be down or unreachable"
        )

    # Generic
    return "Remote file access failed:\n  - Check network connectivity\n  - Verify file URL and access permissions"


def validate_parquet_extension(output_file: str, any_extension: bool = False) -> None:
    """
    Validate that output file has .parquet extension.

    By default, gpio commands that write parquet files require the output
    to have a .parquet extension to prevent accidental misuse (e.g., writing
    a parquet file with .geojson extension).

    Args:
        output_file: Output file path (local or remote)
        any_extension: If True, skip validation and allow any extension

    Raises:
        click.ClickException: If extension is not .parquet and any_extension=False
    """
    # Skip for streaming output or no output specified
    if output_file is None or output_file == "-":
        return

    # User explicitly allowed any extension
    if any_extension:
        return

    # Extract the filename from the path (handles both local and remote URLs)
    if "://" in output_file:
        # Remote URL: extract path portion after protocol://bucket/
        path_part = output_file.split("://", 1)[1]
        filename = path_part.split("/")[-1] if "/" in path_part else path_part
    else:
        filename = os.path.basename(output_file)

    # Check extension (case-insensitive)
    _, ext = os.path.splitext(filename)
    if ext.lower() != ".parquet":
        raise click.ClickException(
            f"Output file '{output_file}' does not have .parquet extension. "
            f"Use --any-extension to allow non-standard extensions."
        )


# Public API - what gets exported with `from file_utils import *`
__all__ = [
    "has_glob_pattern",
    "is_partition_path",
    "resolve_partition_path",
    "get_first_parquet_file",
    "get_all_parquet_files",
    "safe_file_url",
    "get_remote_error_hint",
    "validate_parquet_extension",
]
