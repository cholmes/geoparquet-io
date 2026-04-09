"""
Remote URL handling for S3, GCS, Azure, and HTTP/HTTPS.

This module provides utilities for detecting and working with remote URLs
in cloud object storage (S3, GCS, Azure) and HTTP/HTTPS endpoints.
"""

import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path

import click

from geoparquet_io.core.logging_config import debug, info, progress, success, warn

# Per-bucket cache for S3 buckets that require authentication
# Buckets not in this set are accessed without credentials (works for public buckets)
_s3_buckets_needing_auth: set[str] = set()


def _extract_bucket_name(path: str) -> str:
    """Extract bucket name from S3 URL."""
    # s3://bucket-name/path -> bucket-name
    path_without_protocol = path.split("://", 1)[1]
    return path_without_protocol.split("/")[0]


def _clear_s3_cache():
    """Clear S3 access cache (useful for testing)."""
    global _s3_buckets_needing_auth
    _s3_buckets_needing_auth = set()


def _needs_s3_auth(exception: Exception) -> bool:
    """Detect if exception indicates S3 bucket requires authentication."""
    error_str = str(exception).lower()
    # 403 without credentials means we need to authenticate
    auth_indicators = ["403", "forbidden", "access denied", "unauthorized"]
    return any(ind in error_str for ind in auth_indicators)


def is_remote_url(path):
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
    remote_schemes = [
        "http://",
        "https://",
        "s3://",
        "s3a://",
        "gs://",
        "gcs://",
        "az://",
        "azure://",
        "abfs://",
        "abfss://",
    ]
    return any(path.startswith(scheme) for scheme in remote_schemes)


def is_s3_url(path):
    """
    Check if path is an S3 URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is S3
    """
    return isinstance(path, str) and path.startswith(("s3://", "s3a://"))


def is_azure_url(path):
    """
    Check if path is an Azure Blob Storage URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is Azure
    """
    return isinstance(path, str) and path.startswith(("az://", "azure://", "abfs://", "abfss://"))


def is_gcs_url(path):
    """
    Check if path is a Google Cloud Storage URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is GCS
    """
    return isinstance(path, str) and path.startswith(("gs://", "gcs://"))


def needs_httpfs(path):
    """
    Check if path requires httpfs extension (S3, Azure, GCS).

    HTTP/HTTPS work without httpfs, but cloud storage protocols need it.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if httpfs extension is needed
    """
    httpfs_schemes = [
        "s3://",
        "s3a://",
        "gs://",
        "gcs://",
        "az://",
        "azure://",
        "abfs://",
        "abfss://",
    ]
    return any(path.startswith(scheme) for scheme in httpfs_schemes)


def setup_aws_profile_if_needed(profile, *paths):
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


def validate_profile_for_urls(profile, *urls):
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


def show_remote_read_message(file_path, verbose=False):
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


def get_remote_error_hint(error_msg, file_path=""):
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
            return "Authentication required or access denied:\n  - S3: Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables\n  - Or configure ~/.aws/credentials file"
        if "az://" in path_lower or "azure" in path_lower or "blob.core" in path_lower:
            return "Authentication required or access denied:\n  - Azure: Check AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY\n  - Or set AZURE_STORAGE_SAS_TOKEN for SAS token auth"
        if "gs://" in path_lower or "gcs://" in path_lower:
            return "Authentication required or access denied:\n  - GCS: Check GOOGLE_APPLICATION_CREDENTIALS points to service account JSON"
        return "Authentication required or access denied:\n  - File may be private or require authentication"

    # Check for 404 errors
    if "404" in error_msg or "not found" in error_lower or "does not exist" in error_lower:
        base = "File not found at remote location:\n  - Verify the URL is correct\n  - Check the file exists at the specified path"
        return f"{base}\n  - URL: {file_path}" if file_path else base

    # Check for timeout
    if "timeout" in error_lower or "timed out" in error_lower:
        return "Connection timed out:\n  - Check network connectivity\n  - File may be very large - try a smaller file first\n  - Remote server may be slow or overloaded"

    # Check for connection issues
    if "unable to connect" in error_lower or "connection" in error_lower:
        return "Cannot connect to remote server:\n  - Check network connectivity\n  - Verify the hostname/URL is correct\n  - Server may be down or unreachable"

    # Generic
    return "Remote file access failed:\n  - Check network connectivity\n  - Verify file URL and access permissions"


def upload_if_remote(local_path, remote_path, profile=None, is_directory=False, verbose=False):
    """
    Upload local file/dir to remote path if remote_path is a remote URL.

    Args:
        local_path: Local file or directory path to upload
        remote_path: Remote URL or local path
        profile: AWS profile name (S3 only, optional)
        is_directory: Whether local_path is a directory
        verbose: Whether to print verbose output

    Returns:
        bool: True if upload was performed, False if not remote
    """
    if not is_remote_url(remote_path):
        return False

    from geoparquet_io.core.upload import upload

    if verbose:
        # Calculate size for progress indication
        if is_directory:
            total_size = sum(
                os.path.getsize(os.path.join(dirpath, filename))
                for dirpath, _, filenames in os.walk(local_path)
                for filename in filenames
            )
        else:
            total_size = os.path.getsize(local_path)

        size_mb = total_size / (1024 * 1024)
        progress(f"Uploading {size_mb:.1f} MB to {remote_path}...")

    pattern = "*.parquet" if is_directory else None
    upload(
        source=Path(local_path),
        destination=remote_path,
        profile=profile,
        pattern=pattern,
        dry_run=False,
    )

    if verbose:
        success(f"Successfully uploaded to {remote_path}")

    return True


@contextmanager
def remote_write_context(output_path, is_directory=False, verbose=False):
    """
    Context manager for remote writes with automatic temp file/dir cleanup.

    Yields actual write path (temp for remote, original for local).
    Handles cleanup automatically on exit.

    Args:
        output_path: Output path (local or remote URL)
        is_directory: Whether output is a directory (for partitioning)
        verbose: Whether to print verbose output

    Yields:
        tuple: (actual_write_path, is_remote)
            - actual_write_path: Path to write to (temp for remote, original for local)
            - is_remote: Boolean indicating if output is remote

    Example:
        with remote_write_context('s3://bucket/file.parquet', verbose=True) as (path, is_remote):
            # Write to path
            write_file(path)
            # Cleanup and upload handled automatically
    """
    is_remote = is_remote_url(output_path)

    if is_remote:
        if is_directory:
            temp_path = tempfile.mkdtemp(prefix="gpio_")
        else:
            temp_fd, temp_path = tempfile.mkstemp(suffix=".parquet")
            os.close(temp_fd)

        if verbose:
            debug(f"Remote output detected: {output_path}")
            debug(f"Writing to temporary {'directory' if is_directory else 'file'}: {temp_path}")
    else:
        temp_path = output_path

    try:
        yield temp_path, is_remote
    finally:
        if is_remote and os.path.exists(temp_path):
            try:
                if is_directory:
                    shutil.rmtree(temp_path)
                else:
                    os.unlink(temp_path)
                if verbose:
                    debug(
                        f"Cleaned up temporary {'directory' if is_directory else 'file'}: {temp_path}"
                    )
            except Exception as e:
                if verbose:
                    warn(
                        f"Could not clean up temp {'directory' if is_directory else 'file'} {temp_path}: {e}"
                    )
