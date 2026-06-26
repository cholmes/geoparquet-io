"""Build a file->bbox index over a set of partitioned GeoParquet files.

Reads parquet **footers only** (DuckDB's ``parquet_metadata`` — no data scan) and
aggregates per-file bounds from the bbox covering column's row-group statistics.
The result is a small ``_partitions.parquet`` that a browser tiler (or any
client) can intersect with a tile to pick the 1-2 partition files that overlap,
without reading the data files themselves.

Bounds are taken from a bbox covering column, in either layout:

* a ``bbox`` STRUCT with ``xmin/ymin/xmax/ymax`` fields (gpio ``add bbox``), or
* top-level ``xmin/ymin/xmax/ymax`` DOUBLE columns.

Files without a bbox covering raise a clear error (run ``gpio add bbox`` first).
"""

from __future__ import annotations

import os
import re

from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.logging_config import configure_verbose, debug, success
from geoparquet_io.core.remote import (
    needs_httpfs,
    setup_aws_profile_if_needed,
    validate_profile_for_urls,
)

#: bbox covering fields, in canonical order.
_BBOX_FIELDS = ("xmin", "ymin", "xmax", "ymax")
_VALID_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _sql_str(value: str) -> str:
    """Escape a value for use inside a single-quoted SQL string literal."""
    return value.replace("'", "''")


def _resolve_glob(input_path: str) -> str:
    """Expand a local directory to a ``*.parquet`` glob; pass files/globs/URLs through."""
    if os.path.isdir(input_path):
        return os.path.join(input_path, "*.parquet")
    return input_path


def _struct_bbox_paths(paths: set[str]) -> dict[str, str] | None:
    """If a STRUCT covering's four fields are present (e.g. ``"bbox, xmin"``), map them."""
    for path in paths:
        if path.endswith(", xmin"):
            prefix = path[: -len(", xmin")]
            candidate = {f: f"{prefix}, {f}" for f in _BBOX_FIELDS}
            if set(candidate.values()) <= paths:
                return candidate
    return None


def _detect_bbox_paths(con, glob_sql: str) -> dict[str, str] | None:
    """Map each bbox field to its ``path_in_schema``, or None if no covering is present.

    Handles top-level ``xmin`` columns and ``bbox`` STRUCT subfields (whose path
    DuckDB renders as ``"bbox, xmin"``).
    """
    paths = {
        row[0]
        for row in con.execute(
            f"SELECT DISTINCT path_in_schema FROM parquet_metadata({glob_sql})"
        ).fetchall()
    }
    if set(_BBOX_FIELDS) <= paths:
        return {f: f for f in _BBOX_FIELDS}
    return _struct_bbox_paths(paths)


def _index_query(glob_sql: str, bbox_paths: dict[str, str], partition_key: str | None) -> str:
    """Build the per-file bbox aggregation query from footer row-group stats."""
    wanted = ", ".join(f"'{_sql_str(p)}'" for p in bbox_paths.values())
    select_cols = ["file_name"]
    if partition_key:
        # Normalize Windows backslash separators to '/' before extracting, so the
        # hive value stops at the path separator on every platform.
        normalized = "replace(file_name, '\\', '/')"
        select_cols.append(
            f"regexp_extract({normalized}, '{partition_key}=([^/]+)', 1) AS {partition_key}"
        )
    aggs = {
        "xmin": f"min(lo) FILTER (WHERE p = '{_sql_str(bbox_paths['xmin'])}') AS xmin",
        "ymin": f"min(lo) FILTER (WHERE p = '{_sql_str(bbox_paths['ymin'])}') AS ymin",
        "xmax": f"max(hi) FILTER (WHERE p = '{_sql_str(bbox_paths['xmax'])}') AS xmax",
        "ymax": f"max(hi) FILTER (WHERE p = '{_sql_str(bbox_paths['ymax'])}') AS ymax",
    }
    select_cols.extend(aggs.values())
    return f"""
        WITH m AS (
            SELECT file_name, path_in_schema AS p,
                   stats_min_value::DOUBLE AS lo, stats_max_value::DOUBLE AS hi
            FROM parquet_metadata({glob_sql})
            WHERE path_in_schema IN ({wanted})
        )
        SELECT {", ".join(select_cols)}
        FROM m GROUP BY file_name ORDER BY file_name
    """


def build_partition_index(
    input_glob: str,
    output: str,
    partition_key: str | None = None,
    profile: str | None = None,
    verbose: bool = False,
) -> int:
    """Build a file->bbox index and write it to ``output`` as Parquet.

    Args:
        input_glob: File, glob, directory, or remote URL of partitioned GeoParquet.
        output: Path to write the index parquet to.
        partition_key: If given, extract this hive key's value from each file path
            into a column (e.g. ``provincia`` -> column ``provincia`` = ``"28"``).
        profile: AWS profile for remote (S3) reads.
        verbose: Verbose logging.

    Returns:
        Number of files indexed.

    Raises:
        ValueError: If ``partition_key`` is not a valid identifier, or the inputs
            have no bbox covering column to read bounds from.
    """
    configure_verbose(verbose)
    if partition_key is not None and not _VALID_KEY.match(partition_key):
        raise ValueError(
            f"Invalid partition key {partition_key!r}: must be a valid identifier "
            "(letters, digits, underscore; not starting with a digit)."
        )

    resolved = _resolve_glob(input_glob)
    validate_profile_for_urls(profile, resolved, output)
    setup_aws_profile_if_needed(profile, resolved, output)

    con = get_duckdb_connection(
        load_spatial=False,
        load_httpfs=needs_httpfs(resolved) or needs_httpfs(output),
    )
    try:
        glob_sql = f"'{_sql_str(resolved)}'"
        bbox_paths = _detect_bbox_paths(con, glob_sql)
        if bbox_paths is None:
            raise ValueError(
                "No bbox covering column found (looked for a 'bbox' struct or "
                "top-level xmin/ymin/xmax/ymax). Run 'gpio add bbox' on the files first."
            )
        debug(f"Using bbox columns: {bbox_paths}")
        query = _index_query(glob_sql, bbox_paths, partition_key)
        con.execute(f"COPY ({query}) TO '{_sql_str(output)}' (FORMAT PARQUET)")
        count = int(con.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0])
    finally:
        con.close()

    success(f"Wrote partition index for {count} file(s) to: {output}")
    return count
