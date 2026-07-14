"""Web-visualization profile for GeoParquet output.

Single source of truth for what ``--optimize-for web`` means: a GeoParquet 2.0
file with native GeospatialStatistics, a covering bbox column, and byte-targeted,
fetch-sized row groups. Written via the PyArrow streaming strategy (memory-safe,
one row group per batch, emits the page index).
"""

from __future__ import annotations

import math

from geoparquet_io.core.parquet_writer import (
    ParquetWriteSettings,
    calculate_row_group_size,
)

WEB_TARGET_ROW_GROUP_MB = 8
WEB_ROW_GROUP_ROWS_MIN = 10_000
WEB_ROW_GROUP_ROWS_MAX = 200_000
WEB_MAX_ROW_GROUPS = 1_000
WEB_COMPRESSION = "ZSTD"
WEB_COMPRESSION_LEVEL = 9
WEB_GEOPARQUET_VERSION = "2.0"
WEB_WRITE_STRATEGY = "streaming"
WEB_WRITE_PAGE_INDEX = True
WEB_DATA_PAGE_SIZE = None  # pyarrow default; footer-first reader never fetches page index


def resolve_web_row_group_rows(
    total_rows: int,
    input_size_bytes: int,
    target_mb: int | None = None,
    explicit_rows: int | None = None,
) -> int:
    """Fetch-unit equation: size row groups by target compressed bytes.

    rows_per_group = clamp(target_bytes / bytes_per_row, MIN, MAX), with a footer
    guard so total_rows / rows_per_group stays under WEB_MAX_ROW_GROUPS. An explicit
    user row count wins outright. Degenerate inputs (unknown size, zero rows) fall
    back to the MIN clamp.
    """
    if explicit_rows:
        return explicit_rows
    if not total_rows or total_rows <= 0:
        return WEB_ROW_GROUP_ROWS_MIN
    target_mb = target_mb or WEB_TARGET_ROW_GROUP_MB
    if input_size_bytes and input_size_bytes > 0:
        rows = calculate_row_group_size(
            total_rows, input_size_bytes, target_row_group_size_mb=target_mb
        )
    else:
        rows = WEB_ROW_GROUP_ROWS_MIN
    rows = max(WEB_ROW_GROUP_ROWS_MIN, min(rows, WEB_ROW_GROUP_ROWS_MAX))
    rows = min(rows, total_rows)  # never more than the whole table
    # Footer guard: too many row groups bloats the initial footer download.
    if total_rows / rows > WEB_MAX_ROW_GROUPS:
        rows = math.ceil(total_rows / WEB_MAX_ROW_GROUPS)
    return int(rows)  # calculate_row_group_size is untyped (returns Any)


def resolve_web_settings(
    row_group_rows: int | None = None,
    compression: str | None = None,
    compression_level: int | None = None,
) -> ParquetWriteSettings:
    """Resolve the web-viz ParquetWriteSettings, honoring user overrides.

    ``row_group_rows`` should be the output of resolve_web_row_group_rows(); when
    None the writer falls back to ParquetWriteSettings' own default.
    """
    return ParquetWriteSettings(
        compression=(compression or WEB_COMPRESSION).upper(),
        compression_level=(
            compression_level if compression_level is not None else WEB_COMPRESSION_LEVEL
        ),
        row_group_rows=row_group_rows,
        write_page_index=WEB_WRITE_PAGE_INDEX,
        data_page_size=WEB_DATA_PAGE_SIZE,
    )
