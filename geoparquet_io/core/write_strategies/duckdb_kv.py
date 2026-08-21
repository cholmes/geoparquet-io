"""
DuckDB KV_METADATA write strategy.

This strategy uses DuckDB's native COPY TO with the KV_METADATA option to write
geo metadata directly during the streaming write. Single atomic operation with
no post-processing required.

Best for: Very large files, minimal memory usage
Memory: O(1) - nearly constant
Speed: Fast writes, no post-processing needed
Reliability: Atomic write - either succeeds completely or fails
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from geoparquet_io.core.duckdb_utils import _escape_sql_string
from geoparquet_io.core.logging_config import configure_verbose, debug, success
from geoparquet_io.core.write_strategies.base import BaseWriteStrategy, build_geo_metadata

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

# Valid compression values whitelist (prevents injection via compression param)
VALID_COMPRESSIONS = frozenset({"ZSTD", "SNAPPY", "GZIP", "LZ4", "UNCOMPRESSED", "BROTLI"})

# DuckDB's memory_limit is a SET value, which cannot be parameterised, so the
# value has to be interpolated into SQL. Only accept a plain size literal: a
# decimal number with an optional decimal (KB/MB/GB/TB) or binary (KiB/…) unit.
_MEMORY_LIMIT_RE = re.compile(r"^\d+(\.\d+)?\s*(K|M|G|T)?i?B$", re.IGNORECASE)


def validate_memory_limit(value: str) -> str:
    """Validate/normalize a DuckDB memory limit before interpolating it into SQL.

    ``memory_limit`` originates from ``--write-memory`` (or from a library
    caller's config) and ends up inside ``SET memory_limit = '…'``. DuckDB's
    ``execute`` runs multi-statement strings, so an unvalidated value can close
    the string literal and append arbitrary SQL. Reject anything that is not a
    plain size.

    Args:
        value: Candidate memory limit, e.g. "512MB", "2GB", "4.5 GB", "1GiB"

    Returns:
        The normalized value (whitespace removed, unit upper-cased).

    Raises:
        ValueError: If the value is not a plain size literal.
    """
    text = str(value).strip()
    if not _MEMORY_LIMIT_RE.match(text):
        raise ValueError(
            f"Invalid memory_limit {value!r}; expected a size like "
            f"'512MB', '2GB', '4.5GB', or '1GiB'."
        )
    return text.upper().replace(" ", "")


def _get_available_memory() -> int | None:
    """
    Get available memory in bytes, accounting for container limits.

    Checks cgroup v2 and v1 limits first (Docker, Kubernetes, etc.),
    then falls back to psutil for bare-metal systems.

    Returns:
        Available memory in bytes, or None if detection fails
    """
    # Check cgroup v2 memory limit (Docker, Kubernetes)
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            limit = f.read().strip()
            if limit != "max":
                cgroup_limit = int(limit)
                # Try to get current usage to calculate available
                try:
                    with open("/sys/fs/cgroup/memory.current") as f2:
                        current = int(f2.read().strip())
                        return cgroup_limit - current
                except (FileNotFoundError, ValueError):
                    # Return 80% of limit if we can't get current usage
                    return int(cgroup_limit * 0.8)
    except (FileNotFoundError, ValueError):
        pass

    # Check cgroup v1 memory limit
    try:
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
            limit = int(f.read().strip())
            # Values near 2^63 indicate no limit
            if limit < 2**60:
                try:
                    with open("/sys/fs/cgroup/memory/memory.usage_in_bytes") as f2:
                        usage = int(f2.read().strip())
                        return limit - usage
                except (FileNotFoundError, ValueError):
                    return int(limit * 0.8)
    except (FileNotFoundError, ValueError):
        pass

    # Fall back to psutil for non-containerized environments
    try:
        import psutil

        return psutil.virtual_memory().available
    except ImportError:
        return None


def get_default_memory_limit() -> str:
    """
    Get default memory limit for DuckDB streaming (50% of available RAM).

    Container-aware: detects Docker/Kubernetes memory limits via cgroups
    before falling back to psutil for bare-metal systems.

    Returns:
        Memory limit string for DuckDB (e.g., '2GB', '512MB')
    """
    available = _get_available_memory()

    if available is None:
        return "2GB"  # Conservative fallback

    # Use 50% of available memory
    limit_bytes = int(available * 0.5)
    limit_gb = limit_bytes / (1024**3)

    if limit_gb >= 1:
        return f"{limit_gb:.1f}GB"

    limit_mb = limit_bytes / (1024**2)
    return f"{max(128, int(limit_mb))}MB"  # Minimum 128MB


def _wrap_query_with_crs(
    query: str,
    geometry_column: str,
    input_crs: dict | None,
) -> str:
    """Wrap query with ST_SetCRS() — delegates to shared helper in common.py."""
    from geoparquet_io.core.crs_utils import _wrap_query_with_crs as _common_wrap_query_with_crs

    return _common_wrap_query_with_crs(query, geometry_column, input_crs)


def _detect_bbox_column_name(schema_names: list[str]) -> str | None:
    """Detect bbox column name from schema using common naming conventions."""
    for name in schema_names:
        if name in ["bbox", "bounds", "extent"] or name.endswith("_bbox"):
            return name
    return None


# Rows sampled to estimate average row size when converting an MB target to a
# row count. Large enough to be representative, small enough to stay cheap.
_MB_ESTIMATE_SAMPLE_ROWS = 20000

# Clauses that must not follow a trailing ORDER BY for it to be safely strippable
# (they change which/how many rows a LIMIT would return).
_ORDER_BY_TAIL_STOPWORDS = re.compile(r"(?i)\b(limit|offset|union|except|intersect|fetch)\b")


def _strip_trailing_order_by(query: str) -> str:
    """Drop a trailing top-level ``ORDER BY`` clause for cheap row sampling.

    Estimating bytes-per-row does not need ordered rows, and a ``LIMIT`` layered
    over an ``ORDER BY`` forces DuckDB to scan (and sort) the *entire* source —
    re-downloading remote inputs and recomputing expensive ordering keys (e.g.
    ``ST_Hilbert``) just to size row groups. Without the ordering the sample's
    ``LIMIT`` pushes down, so DuckDB stops after a few row groups.

    Returns the query unchanged when no strippable top-level trailing ORDER BY is
    found, so estimation still works — it just skips the speed-up.
    """
    depth = 0
    in_single = in_double = False
    last_order_by = -1
    i, n = 0, len(query)
    while i < n:
        ch = query[i]
        if in_single:
            in_single = ch != "'"
        elif in_double:
            in_double = ch != '"'
        elif ch == "'":
            in_single = True
        elif ch == '"':
            in_double = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and ch in "oO":
            match = re.match(r"order\s+by\b", query[i:], re.IGNORECASE)
            prev = query[i - 1] if i else " "
            if match and not (prev.isalnum() or prev == "_"):
                last_order_by = i
                i += match.end()
                continue
        i += 1

    if last_order_by == -1:
        return query
    # Bail if anything follows the ORDER BY that would alter the sampled rows.
    if _ORDER_BY_TAIL_STOPWORDS.search(query[last_order_by:]):
        return query
    return query[:last_order_by].rstrip()


def _resolve_row_group_rows(
    con: duckdb.DuckDBPyConnection,
    query: str,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    verbose: bool,
) -> int | None:
    """Resolve an MB row-group target to a row count for DuckDB COPY TO.

    DuckDB's ``ROW_GROUP_SIZE`` is expressed in rows, so a ``--row-group-size-mb``
    target has to be converted before it can take effect on this strategy (the
    bytes-based ``ROW_GROUP_SIZE_BYTES`` option is unusable here because it
    requires disabling insertion-order preservation, which would undo any
    spatial ordering already applied). An explicit row count always wins; when
    only an MB target is given we estimate bytes-per-row from a sample and mirror
    the arrow write path (see ``_write_table_with_settings``).
    """
    if row_group_rows:
        return row_group_rows
    if not row_group_size_mb:
        return None

    from geoparquet_io.core.common import _estimate_row_size

    # Sample without the ORDER BY so the LIMIT streams (a LIMIT over an ORDER BY
    # would rescan/sort the whole source — re-downloading remote inputs and
    # recomputing the ordering key — just to size row groups).
    sample_query = _strip_trailing_order_by(query)
    try:
        sample = (
            con.execute(f"SELECT * FROM ({sample_query}) LIMIT {_MB_ESTIMATE_SAMPLE_ROWS}")
            .arrow()
            .read_all()
        )
    except Exception as exc:  # pragma: no cover - defensive, fall back to default
        if verbose:
            debug(f"Could not sample rows for --row-group-size-mb estimate: {exc}")
        return None

    if sample.num_rows == 0:
        return None

    bytes_per_row = _estimate_row_size(sample)
    target_bytes = row_group_size_mb * 1024 * 1024
    rows = max(1, int(target_bytes // bytes_per_row))
    if verbose:
        debug(
            f"Resolved --row-group-size-mb {row_group_size_mb} to {rows:,} rows/group "
            f"(~{bytes_per_row} bytes/row)"
        )
    return rows


def _build_copy_options(
    compression: str,
    row_group_rows: int | None,
    geo_meta_escaped: str | None = None,
    extra_kv_metadata: dict[str, str] | None = None,
) -> list[str]:
    """Build COPY TO options list."""
    options = [
        "FORMAT PARQUET",
        f"COMPRESSION {compression}",
        "GEOPARQUET_VERSION 'NONE'",
    ]
    kv_pairs = {}
    if geo_meta_escaped:
        kv_pairs["geo"] = geo_meta_escaped
    if extra_kv_metadata:
        for key, value in extra_kv_metadata.items():
            kv_pairs[key] = _escape_sql_string(value)
    if kv_pairs:
        kv_str = ", ".join(f"'{_escape_sql_string(k)}': '{v}'" for k, v in kv_pairs.items())
        options.append(f"KV_METADATA {{{kv_str}}}")
    if row_group_rows:
        options.append(f"ROW_GROUP_SIZE {row_group_rows}")
    return options


class DuckDBKVStrategy(BaseWriteStrategy):
    """
    Use DuckDB COPY TO with native KV_METADATA for geo metadata.

    This strategy streams data directly through DuckDB's COPY TO command
    with the KV_METADATA option, which embeds geo metadata directly in
    the Parquet footer during the write. No post-processing is needed.
    """

    name = "duckdb-kv"
    description = "DuckDB streaming write with native metadata support"
    supports_streaming = True
    supports_remote = True

    def write_from_query(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        output_path: str,
        geometry_column: str,
        original_metadata: dict | None,
        geoparquet_version: str,
        compression: str,
        compression_level: int,
        row_group_size_mb: int | None,
        row_group_rows: int | None,
        input_crs: dict | None,
        verbose: bool,
        custom_metadata: dict | None = None,
        memory_limit: str | None = None,
        geometry_info: dict | None = None,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Write query results to GeoParquet using DuckDB COPY TO with KV_METADATA."""
        from geoparquet_io.core.remote import is_remote_url, upload_if_remote

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        compression_upper = compression.upper()
        if compression_upper not in VALID_COMPRESSIONS:
            raise ValueError(
                f"Invalid compression: {compression}. Valid: {', '.join(VALID_COMPRESSIONS)}"
            )

        # DuckDB COPY TO sizes row groups by row count, so translate any MB target
        # into rows before it can take effect on this strategy (fixes #547).
        row_group_rows = _resolve_row_group_rows(
            con, query, row_group_size_mb, row_group_rows, verbose
        )

        # Handle non-geo queries: write plain Parquet without geo metadata
        if geometry_column is None:
            self._write_plain_parquet_from_query(
                con, query, output_path, compression_upper, row_group_rows, verbose
            )
            return

        self._configure_duckdb_memory(con, memory_limit, verbose)

        is_remote = is_remote_url(output_path)
        local_path = self._get_local_path(output_path, is_remote)

        try:
            if geoparquet_version == "parquet-geo-only":
                self._write_parquet_geo_only(
                    con,
                    query,
                    local_path,
                    geometry_column,
                    compression_upper,
                    compression_level,
                    row_group_rows,
                    input_crs,
                    output_path,
                    verbose,
                    extra_kv_metadata=extra_kv_metadata,
                )
            else:
                self._write_with_geo_metadata(
                    con,
                    query,
                    local_path,
                    geometry_column,
                    geoparquet_version,
                    compression_upper,
                    compression_level,
                    row_group_rows,
                    original_metadata,
                    input_crs,
                    custom_metadata,
                    output_path,
                    verbose,
                    geometry_info,
                    extra_kv_metadata=extra_kv_metadata,
                )

            if is_remote:
                upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

        finally:
            if is_remote and Path(local_path).exists():
                Path(local_path).unlink()

    def _configure_duckdb_memory(
        self,
        con: duckdb.DuckDBPyConnection,
        memory_limit: str | None,
        verbose: bool,
    ) -> None:
        """Configure DuckDB memory settings for streaming."""
        con.execute("SET threads = 1")  # Required for memory control (DuckDB #8270)
        # Let COPY TO parquet flush row groups to disk instead of buffering the
        # entire result to preserve order. Without this the writer holds the whole
        # output in RAM and a large COPY runs out of memory even with a memory_limit
        # + temp_directory set (the constant-memory design from #185 relied on this
        # spilling). Safe because threads=1 already makes the single pipeline emit
        # rows in order, so output ordering (e.g. sorted files) is preserved.
        con.execute("SET preserve_insertion_order = false")
        # Validate before interpolation: a SET value cannot be parameterised.
        effective_limit = validate_memory_limit(memory_limit or get_default_memory_limit())
        con.execute(f"SET memory_limit = '{effective_limit}'")
        if verbose:
            debug(f"DuckDB memory limit: {effective_limit}")

    def _get_local_path(self, output_path: str, is_remote: bool) -> str:
        """Get local path for writing (temp file if remote)."""
        if is_remote:
            fd, local_path = tempfile.mkstemp(suffix=".parquet")
            os.close(fd)
            return local_path
        return output_path

    def _write_parquet_geo_only(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        local_path: str,
        geometry_column: str,
        compression: str,
        compression_level: int,
        row_group_rows: int | None,
        input_crs: dict | None,
        output_path: str,
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Write parquet-geo-only format (no geo metadata)."""
        if verbose:
            debug("Writing parquet-geo-only (no geo metadata)...")

        # DuckDB 1.5+: Keep native GEOMETRY type — DuckDB writes native Parquet
        # geometry encoding directly. No WKB conversion needed.
        # Apply CRS via ST_SetCRS so DuckDB writes it into the schema natively.
        final_query = _wrap_query_with_crs(query, geometry_column, input_crs)
        escaped_path = _escape_sql_string(local_path)

        copy_options = _build_copy_options(
            compression, row_group_rows, extra_kv_metadata=extra_kv_metadata
        )
        copy_query = f"COPY ({final_query}) TO '{escaped_path}' ({', '.join(copy_options)})"
        con.execute(copy_query)

        if verbose:
            pf = pq.ParquetFile(local_path)
            success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")

    def _write_with_geo_metadata(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        local_path: str,
        geometry_column: str,
        geoparquet_version: str,
        compression: str,
        compression_level: int,
        row_group_rows: int | None,
        original_metadata: dict | None,
        input_crs: dict | None,
        custom_metadata: dict | None,
        output_path: str,
        verbose: bool,
        geometry_info: dict | None = None,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Write with geo metadata (v1.0, v1.1, v2.0)."""
        from geoparquet_io.core.duckdb_utils import _wrap_query_with_blob_conversion

        geo_meta = build_geo_metadata(
            geometry_column=geometry_column,
            geoparquet_version=geoparquet_version,
            original_metadata=original_metadata,
            input_crs=input_crs,
            custom_metadata=custom_metadata,
            geometry_info=geometry_info,
        )

        col_meta = geo_meta["columns"][geometry_column]
        self._compute_missing_metadata(con, query, geometry_column, col_meta, verbose)
        self._add_bbox_covering_if_present(con, query, col_meta, verbose)

        # For v1.x: Cast to BLOB so DuckDB writes plain binary WKB.
        # For v2.0: Keep native GEOMETRY type with CRS — DuckDB writes native
        # Parquet geometry encoding and CRS directly.
        if geoparquet_version in ("1.0", "1.1"):
            final_query = _wrap_query_with_blob_conversion(query, geometry_column, con)
        else:
            final_query = _wrap_query_with_crs(query, geometry_column, input_crs)

        escaped_path = _escape_sql_string(local_path)
        geo_meta_escaped = _escape_sql_string(json.dumps(geo_meta))

        copy_options = _build_copy_options(
            compression, row_group_rows, geo_meta_escaped, extra_kv_metadata
        )
        copy_query = f"COPY ({final_query}) TO '{escaped_path}' ({', '.join(copy_options)})"

        if verbose:
            debug(f"Writing via DuckDB COPY TO with {compression} compression...")
        con.execute(copy_query)

        if verbose:
            pf = pq.ParquetFile(local_path)
            success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")

    def _compute_missing_metadata(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        geometry_column: str,
        col_meta: dict,
        verbose: bool,
    ) -> None:
        """Compute whichever of bbox/geometry_types the carried metadata lacks.

        Both come out of one scan, so a caller that invalidated both (a row
        filter, a reprojection, a multi-file merge) pays for a single pass.
        """
        from geoparquet_io.core.geo_metadata import compute_geo_stats_via_sql

        need_bbox = "bbox" not in col_meta
        need_types = "geometry_types" not in col_meta
        if not (need_bbox or need_types):
            return

        if verbose:
            debug("Computing bbox/geometry types via SQL...")
        bbox, geometry_types = compute_geo_stats_via_sql(
            con,
            query,
            geometry_column,
            need_bbox=need_bbox,
            need_geometry_types=need_types,
        )
        if need_bbox and bbox:
            col_meta["bbox"] = bbox
        if need_types:
            col_meta["geometry_types"] = geometry_types

    def _add_bbox_covering_if_present(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        col_meta: dict,
        verbose: bool,
    ) -> None:
        """Add bbox covering metadata if bbox column is present."""
        schema_result = con.execute(f"SELECT * FROM ({query}) LIMIT 0").arrow()
        bbox_col_name = _detect_bbox_column_name(schema_result.schema.names)

        if bbox_col_name:
            col_meta["covering"] = {
                "bbox": {
                    "xmin": [bbox_col_name, "xmin"],
                    "ymin": [bbox_col_name, "ymin"],
                    "xmax": [bbox_col_name, "xmax"],
                    "ymax": [bbox_col_name, "ymax"],
                }
            }
            if verbose:
                debug(f"Added bbox covering metadata for column '{bbox_col_name}'")

    def write_from_table(
        self,
        table: pa.Table,
        output_path: str,
        geometry_column: str,
        geoparquet_version: str,
        compression: str,
        compression_level: int,
        row_group_size_mb: int | None,
        row_group_rows: int | None,
        verbose: bool,
        input_crs: dict | None = None,
        custom_metadata: dict | None = None,
    ) -> None:
        """Write Arrow table to GeoParquet using DuckDB COPY TO with KV_METADATA."""
        from geoparquet_io.core.common import _detect_version_from_table
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        # Handle non-geo tables: write plain Parquet without geo metadata
        if geometry_column is None:
            self._write_plain_parquet_from_table(
                table, output_path, compression, row_group_rows, verbose
            )
            return

        # Auto-detect version from table schema metadata if not specified
        effective_version = geoparquet_version
        if effective_version is None:
            effective_version = _detect_version_from_table(table, verbose)

        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            con.register("input_table", table)

            # Convert WKB bytes to GEOMETRY for proper spatial processing.
            # geoarrow.wkb extension columns already register as GEOMETRY in
            # DuckDB, where ST_GeomFromWKB(GEOMETRY) is a binder error.
            geom_type = table.schema.field(geometry_column).type
            if getattr(geom_type, "extension_name", None) == "geoarrow.wkb":
                query = "SELECT * FROM input_table"
            else:
                escaped_geom = geometry_column.replace('"', '""')
                query = f"""
                    SELECT * REPLACE (ST_GeomFromWKB("{escaped_geom}") AS "{escaped_geom}")
                    FROM input_table
                """

            self.write_from_query(
                con=con,
                query=query,
                output_path=output_path,
                geometry_column=geometry_column,
                original_metadata=None,
                geoparquet_version=effective_version,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                input_crs=input_crs,
                verbose=verbose,
                custom_metadata=custom_metadata,
            )
        finally:
            con.close()

    def _write_plain_parquet_from_table(
        self,
        table: pa.Table,
        output_path: str,
        compression: str,
        row_group_rows: int | None,
        verbose: bool,
    ) -> None:
        """Write plain Parquet (no geo metadata) from an Arrow table."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.remote import is_remote_url, upload_if_remote

        compression_upper = compression.upper()
        if compression_upper not in VALID_COMPRESSIONS:
            raise ValueError(
                f"Invalid compression: {compression}. Valid: {', '.join(VALID_COMPRESSIONS)}"
            )

        is_remote = is_remote_url(output_path)
        local_path = self._get_local_path(output_path, is_remote)

        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        try:
            con.register("input_table", table)
            escaped_path = _escape_sql_string(local_path)

            options = [
                "FORMAT PARQUET",
                f"COMPRESSION {compression_upper}",
            ]
            if row_group_rows:
                options.append(f"ROW_GROUP_SIZE {row_group_rows}")

            copy_query = f"COPY input_table TO '{escaped_path}' ({', '.join(options)})"

            if verbose:
                debug(f"Writing plain Parquet with {compression_upper} compression...")
            con.execute(copy_query)

            if is_remote:
                upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

            if verbose:
                pf = pq.ParquetFile(local_path)
                success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")
        finally:
            con.close()
            if is_remote and Path(local_path).exists():
                Path(local_path).unlink()

    def _write_plain_parquet_from_query(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        output_path: str,
        compression: str,
        row_group_rows: int | None,
        verbose: bool,
    ) -> None:
        """Write plain Parquet (no geo metadata) from a query."""
        from geoparquet_io.core.remote import is_remote_url, upload_if_remote

        is_remote = is_remote_url(output_path)
        local_path = self._get_local_path(output_path, is_remote)

        try:
            escaped_path = _escape_sql_string(local_path)

            options = [
                "FORMAT PARQUET",
                f"COMPRESSION {compression}",
            ]
            if row_group_rows:
                options.append(f"ROW_GROUP_SIZE {row_group_rows}")

            copy_query = f"COPY ({query}) TO '{escaped_path}' ({', '.join(options)})"

            if verbose:
                debug(f"Writing plain Parquet with {compression} compression...")
            con.execute(copy_query)

            if is_remote:
                upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

            if verbose:
                pf = pq.ParquetFile(local_path)
                success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")
        finally:
            if is_remote and Path(local_path).exists():
                Path(local_path).unlink()
