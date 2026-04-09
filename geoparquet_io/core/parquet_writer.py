"""
Parquet writing utilities for GeoParquet files.

This module provides functions and classes for writing Parquet files
with GeoParquet metadata and optimal settings.
"""

import re
from dataclasses import dataclass

from geoparquet_io.core.logging_config import warn


@dataclass
class ParquetWriteSettings:
    """
    Central configuration for Parquet write best practices.
    Single source of truth for compression, row groups, and other settings.
    """

    compression: str = "ZSTD"
    compression_level: int = 15
    row_group_rows: int | None = None
    row_group_size_mb: int | None = None

    # Best practice constants
    DEFAULT_COMPRESSION = "ZSTD"
    DEFAULT_COMPRESSION_LEVEL = 15
    DEFAULT_ROW_GROUP_ROWS = 100_000
    DEFAULT_PARQUET_VERSION = "2.6"

    def get_pyarrow_kwargs(self, calculated_row_group_size: int | None = None) -> dict:
        """Get kwargs dict for PyArrow write_table()."""
        pa_compression = self.compression if self.compression != "UNCOMPRESSED" else None
        pa_compression_level = (
            self.compression_level if self.compression in ["GZIP", "ZSTD", "BROTLI"] else None
        )

        row_group_size = (
            calculated_row_group_size or self.row_group_rows or self.DEFAULT_ROW_GROUP_ROWS
        )

        kwargs = {
            "row_group_size": row_group_size,
            "compression": pa_compression,
            "write_statistics": True,
            "use_dictionary": True,
            "version": self.DEFAULT_PARQUET_VERSION,
        }

        if pa_compression_level is not None:
            kwargs["compression_level"] = pa_compression_level

        return kwargs


def parse_size_string(size_str):
    """
    Parse a human-readable size string into bytes.

    Args:
        size_str: String like '256MB', '1GB', '128' (assumed MB if no unit)

    Returns:
        int: Size in bytes
    """
    if not size_str:
        return None

    try:
        return int(size_str) * 1024 * 1024
    except ValueError:
        pass

    size_str = size_str.strip().upper()
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT]?B?)$", size_str)
    if not match:
        raise ValueError(f"Invalid size format: {size_str}")

    value = float(match.group(1))
    unit = match.group(2)

    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
        "TB": 1024 * 1024 * 1024 * 1024,
        "K": 1024,
        "M": 1024 * 1024,
        "G": 1024 * 1024 * 1024,
        "T": 1024 * 1024 * 1024 * 1024,
    }

    multiplier = multipliers.get(unit, 1024 * 1024)
    return int(value * multiplier)


def calculate_row_group_size(
    total_rows, file_size_bytes, target_row_group_size_mb=None, target_row_group_rows=None
):
    """Calculate optimal row group size for parquet file."""
    if target_row_group_rows:
        return min(target_row_group_rows, total_rows)

    if not target_row_group_size_mb:
        target_row_group_size_mb = 130

    target_bytes = target_row_group_size_mb * 1024 * 1024

    if total_rows > 0 and file_size_bytes > 0:
        bytes_per_row = file_size_bytes / total_rows
        rows_per_group = int(target_bytes / bytes_per_row)
        return max(1, min(rows_per_group, total_rows))
    else:
        return max(1, total_rows)


def validate_compression_settings(compression, compression_level, verbose=False):
    """
    Validate compression settings and return normalized values.

    Args:
        compression: Compression codec name
        compression_level: Compression level (may be None)
        verbose: Whether to print verbose output

    Returns:
        tuple: (normalized_compression, normalized_level)

    Raises:
        ValueError: If invalid compression settings
    """
    valid_codecs = ["ZSTD", "GZIP", "SNAPPY", "LZ4", "BROTLI", "UNCOMPRESSED", "NONE"]

    if compression:
        compression = compression.upper()
        if compression == "NONE":
            compression = "UNCOMPRESSED"
        if compression not in valid_codecs:
            raise ValueError(
                f"Invalid compression codec: {compression}. "
                f"Valid options: {', '.join(valid_codecs)}"
            )

    level_codecs = ["ZSTD", "GZIP", "BROTLI"]
    if compression_level is not None and compression not in level_codecs:
        if verbose:
            warn(f"Compression level ignored for {compression} (only applies to {level_codecs})")
        compression_level = None

    return compression, compression_level


def _estimate_row_size(table) -> int:
    """Estimate average row size in bytes from an Arrow table."""
    if table.num_rows == 0:
        return 0

    total_bytes = 0
    for column in table.columns:
        for chunk in column.chunks:
            total_bytes += chunk.nbytes

    return total_bytes // table.num_rows


def _normalize_arrow_large_types(table):
    """Convert large string/binary types to regular types for compatibility."""
    import pyarrow as pa

    new_schema_fields = []
    needs_conversion = False

    for field in table.schema:
        if pa.types.is_large_string(field.type):
            new_schema_fields.append(pa.field(field.name, pa.string()))
            needs_conversion = True
        elif pa.types.is_large_binary(field.type):
            new_schema_fields.append(pa.field(field.name, pa.binary()))
            needs_conversion = True
        else:
            new_schema_fields.append(field)

    if not needs_conversion:
        return table

    new_schema = pa.schema(new_schema_fields, metadata=table.schema.metadata)
    return table.cast(new_schema)


def format_size(size_bytes):
    """Format byte size as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"
