"""Remote URL handling utilities for S3, GCS, Azure, and HTTP(S) paths.

This module provides functions to detect and work with remote URLs
that DuckDB can read, including cloud storage protocols (S3, GCS, Azure)
and HTTP(S) URLs.

Example:
    >>> from geoparquet_io.core.remote import is_remote_url, needs_httpfs
    >>> is_remote_url("s3://my-bucket/data.parquet")
    True
    >>> needs_httpfs("s3://my-bucket/data.parquet")
    True
    >>> needs_httpfs("https://example.com/data.parquet")
    False  # HTTP works without httpfs
"""

import os

import click

from geoparquet_io.core.logging_config import info

# Cloud storage schemes that DuckDB supports
CLOUD_SCHEMES = (
    "s3://",
    "s3a://",
    "gs://",
    "gcs://",
    "az://",
    "azure://",
    "abfs://",
    "abfss://",
)

# All remote schemes including HTTP
REMOTE_SCHEMES = (
    "http://",
    "https://",
    *CLOUD_SCHEMES,
)


def is_remote_url(path: str | None) -> bool:
    """
    Check if path is a remote URL that DuckDB can read.

    Supports:
    - HTTP/HTTPS: http://, https://
    - AWS S3: s3://, s3a://
    - Azure: az://, azure://, abfs://, abfss://
    - Google Cloud Storage: gs://, gcs://

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is a remote URL, False otherwise
    """
    if path is None:
        return False
    return any(path.startswith(scheme) for scheme in REMOTE_SCHEMES)


def is_s3_url(path: str | None) -> bool:
    """
    Check if path is an S3 URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is S3
    """
    return isinstance(path, str) and path.startswith(("s3://", "s3a://"))


def is_azure_url(path: str | None) -> bool:
    """
    Check if path is an Azure Blob Storage URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is Azure
    """
    return isinstance(path, str) and path.startswith(("az://", "azure://", "abfs://", "abfss://"))


def is_gcs_url(path: str | None) -> bool:
    """
    Check if path is a Google Cloud Storage URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is GCS
    """
    return isinstance(path, str) and path.startswith(("gs://", "gcs://"))


def needs_httpfs(path: str) -> bool:
    """
    Check if path requires httpfs extension (S3, Azure, GCS).

    HTTP/HTTPS work without httpfs, but cloud storage protocols need it.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if httpfs extension is needed
    """
    return any(path.startswith(scheme) for scheme in CLOUD_SCHEMES)


def get_protocol(path: str) -> str | None:
    """
    Extract protocol/scheme from a URL.

    Args:
        path: File path or URL

    Returns:
        Protocol string (e.g., 's3', 'https') or None for local paths
    """
    if "://" in path:
        return path.split("://")[0].lower()
    return None


def setup_aws_profile_if_needed(profile: str | None, *paths: str | None) -> None:
    """
    Set AWS_PROFILE environment variable if profile specified and S3 URLs detected.

    This allows both DuckDB (via credential_chain) and obstore to use the specified
    AWS profile for authentication. The profile is resolved using standard AWS SDK
    mechanisms (reads from ~/.aws/credentials, ~/.aws/config, etc.).

    Note: This is a convenience wrapper. Setting AWS_PROFILE env var directly
    has the same effect.

    Args:
        profile: AWS profile name or None
        *paths: Variable number of file paths to check for S3 URLs

    Example:
        setup_aws_profile_if_needed(profile, input_file, output_file)
        # Equivalent to: os.environ['AWS_PROFILE'] = profile
    """
    if not profile:
        return

    # Check if any path is S3
    has_s3 = any(p and is_s3_url(p) for p in paths)
    if has_s3:
        os.environ["AWS_PROFILE"] = profile


def validate_profile_for_urls(profile: str | None, *urls: str | None) -> None:
    """
    Validate that profile parameter is only used with S3 URLs.

    The --profile flag sets AWS credentials for S3 operations. Using it with
    other cloud providers (GCS, Azure) would be confusing since they use
    different authentication mechanisms.

    Args:
        profile: AWS profile name or None
        *urls: Variable number of file paths to validate

    Raises:
        click.BadParameter: If profile is used with non-S3 remote URLs

    Example:
        validate_profile_for_urls(profile, input_file, output_file)
    """
    if not profile:
        return

    for url in urls:
        if url and is_remote_url(url) and not is_s3_url(url):
            protocol = url.split("://")[0].upper() if "://" in url else "unknown"
            raise click.BadParameter(
                f"--profile flag is only valid for S3 URLs, but got {protocol} URL: {url}\n"
                f"For {protocol} authentication, use environment variables or default credentials."
            )


def show_remote_read_message(file_path: str, verbose: bool = False) -> None:
    """
    Show consistent message when reading from remote files.

    Args:
        file_path: Path to check (local or remote)
        verbose: If True, show detailed message
    """
    if not is_remote_url(file_path):
        return

    protocol = file_path.split("://")[0].upper() if "://" in file_path else "HTTP"
    if verbose:
        info(f"Reading from {protocol}: {file_path}")
    else:
        info(f"Reading from {protocol} (network operations may take time)...")


# Public API - what gets exported with `from remote import *`
__all__ = [
    "CLOUD_SCHEMES",
    "REMOTE_SCHEMES",
    "is_remote_url",
    "is_s3_url",
    "is_azure_url",
    "is_gcs_url",
    "needs_httpfs",
    "get_protocol",
    "setup_aws_profile_if_needed",
    "validate_profile_for_urls",
    "show_remote_read_message",
]
