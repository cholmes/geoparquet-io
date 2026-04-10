"""
File path utilities for GeoParquet files.
"""

import glob as glob_module
import os
import urllib.parse
from pathlib import Path

import click

from geoparquet_io.core.logging_config import debug


def has_glob_pattern(path: str) -> bool:
    return any(c in path for c in ("*", "?", "["))


def is_partition_path(path: str) -> bool:
    from geoparquet_io.core.remote import is_remote_url

    if has_glob_pattern(path):
        return True

    if not is_remote_url(path) and os.path.isdir(path):
        return True

    if is_remote_url(path):
        path_parts = path.split("/")
        for part in path_parts[3:]:
            if "=" in part and not part.endswith(".parquet"):
                return True

    return False


def resolve_partition_path(path: str, hive_partitioning: bool | None = None) -> tuple[str, dict]:
    from geoparquet_io.core.remote import is_remote_url

    options = {}
    resolved = path

    if not is_remote_url(path) and os.path.isdir(path):
        try:
            items = os.listdir(path)
            subdirs = [d for d in items if os.path.isdir(os.path.join(path, d))]
            has_parquet_files = any(
                f.endswith(".parquet") for f in items if not os.path.isdir(os.path.join(path, f))
            )
            has_hive_subdirs = any("=" in d for d in subdirs)

            if has_hive_subdirs:
                resolved = os.path.join(path, "**", "*.parquet")
                options["hive_partitioning"] = True
            elif subdirs and not has_parquet_files:
                resolved = os.path.join(path, "**", "*.parquet")
            elif has_parquet_files:
                resolved = os.path.join(path, "*.parquet")
            else:
                resolved = os.path.join(path, "**", "*.parquet")
        except OSError:
            resolved = os.path.join(path, "**", "*.parquet")

    if hive_partitioning is None:
        path_parts = resolved.replace("\\", "/").split("/")
        dir_parts = [p for p in path_parts[:-1] if p and p not in ("**", "*")]
        has_hive_dirs = any("=" in part for part in dir_parts)
        if has_hive_dirs:
            options["hive_partitioning"] = True
    elif hive_partitioning is not None:
        options["hive_partitioning"] = hive_partitioning

    return resolved, options


def get_first_parquet_file(partition_path: str) -> str | None:
    from geoparquet_io.core.remote import is_remote_url

    if is_remote_url(partition_path):
        return partition_path

    if os.path.isdir(partition_path):
        for root, _dirs, files in os.walk(partition_path):
            for f in sorted(files):
                if f.endswith(".parquet"):
                    return os.path.join(root, f)
        return None

    if has_glob_pattern(partition_path):
        matches = glob_module.glob(partition_path, recursive=True)
        parquet_matches = [m for m in sorted(matches) if m.endswith(".parquet")]
        return parquet_matches[0] if parquet_matches else None

    return partition_path


def get_all_parquet_files(partition_path: str) -> list[str]:
    from geoparquet_io.core.remote import is_remote_url

    if is_remote_url(partition_path):
        return [partition_path]

    if os.path.isdir(partition_path):
        parquet_files = []
        for root, _dirs, files in os.walk(partition_path):
            for f in files:
                if f.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, f))
        return sorted(parquet_files)

    if has_glob_pattern(partition_path):
        matches = glob_module.glob(partition_path, recursive=True)
        return sorted([m for m in matches if m.endswith(".parquet")])

    return [partition_path] if os.path.exists(partition_path) else []


def validate_output_path(output_path, verbose=False):
    from geoparquet_io.core.remote import is_remote_url

    if is_remote_url(output_path):
        return

    output_dir = os.path.dirname(output_path) or "."
    if not os.path.exists(output_dir):
        raise click.ClickException(f"Output directory not found: {output_dir}")
    if not os.access(output_dir, os.W_OK):
        raise click.ClickException(f"No write permission for: {output_dir}")


def validate_parquet_extension(output_file: str, any_extension: bool = False) -> None:
    if output_file is None or output_file == "-":
        return

    if any_extension:
        return

    if "://" in output_file:
        path_part = output_file.split("://", 1)[1]
        filename = path_part.split("/")[-1] if "/" in path_part else path_part
    else:
        filename = os.path.basename(output_file)

    _, ext = os.path.splitext(filename)
    if ext.lower() != ".parquet":
        raise click.ClickException(
            f"Output file '{output_file}' does not have .parquet extension. "
            f"Use --any-extension to allow non-standard extensions."
        )


def handle_output_overwrite(
    output_path: str | None, overwrite: bool, input_path: str | None = None
) -> None:
    if not output_path:
        return

    output_file = Path(output_path)

    if not output_file.exists():
        return

    if input_path:
        input_file = Path(input_path)
        try:
            if output_file.resolve() == input_file.resolve():
                raise click.ClickException(f"Cannot overwrite input file: {output_path}")
        except (OSError, click.ClickException) as e:
            if isinstance(e, click.ClickException):
                raise
            pass

    if not overwrite:
        raise click.ClickException(f"Output file already exists: {output_path}")

    output_file.unlink()


def safe_file_url(file_path, verbose=False):
    """
    Prepare a file path for safe use in SQL queries.

    Handles URL encoding for HTTP(S) URLs and escapes single quotes
    to prevent SQL injection when paths are interpolated into queries.

    Args:
        file_path: Local path or remote URL
        verbose: Whether to log debug info

    Returns:
        str: Safe file path/URL with single quotes escaped for SQL
    """
    from geoparquet_io.core.remote import is_remote_url

    if is_remote_url(file_path):
        if file_path.startswith(("http://", "https://")):
            parsed = urllib.parse.urlparse(file_path)
            duckdb_safe_chars = "/*?[]=,"
            encoded_path = urllib.parse.quote(parsed.path, safe=duckdb_safe_chars)
            safe_url = parsed._replace(path=encoded_path).geturl()
        else:
            safe_url = file_path

        if verbose:
            protocol = file_path.split("://")[0].upper() if "://" in file_path else "HTTP"
            debug(f"Reading from {protocol}: {safe_url}")
        # Escape single quotes to prevent SQL injection
        return safe_url.replace("'", "''")
    else:
        if not has_glob_pattern(file_path) and not os.path.exists(file_path):
            raise click.BadParameter(f"Local file not found: {file_path}")
        # Escape single quotes to prevent SQL injection
        return file_path.replace("'", "''")


def _get_file_cache_key(parquet_file: str) -> tuple[str, float]:
    from geoparquet_io.core.remote import is_remote_url

    if is_remote_url(parquet_file):
        return (parquet_file, 0)

    path = Path(parquet_file)
    if path.exists():
        return (str(path.resolve()), path.stat().st_mtime)
    return (str(path), 0)
