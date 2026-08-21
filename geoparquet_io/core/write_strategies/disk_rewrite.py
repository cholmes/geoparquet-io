"""
Disk rewrite write strategy.

This strategy writes with DuckDB first (fast but no geo metadata), then reads
and rewrites the file row-group by row-group with PyArrow to add geo metadata.

Best for: Maximum compatibility, fallback when other strategies fail
Memory: O(row_group_size) - one row group at a time
Speed: Slower (reads file twice, writes twice)
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    get_duckdb_connection,
    quote_identifier,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success
from geoparquet_io.core.write_strategies.base import BaseWriteStrategy, build_geo_metadata

if TYPE_CHECKING:
    import duckdb


class DiskRewriteStrategy(BaseWriteStrategy):
    """
    Write with DuckDB, then read/rewrite entire file with PyArrow for metadata.

    This is the most reliable fallback strategy. It writes to disk first using
    DuckDB's fast COPY TO, then rewrites row-group by row-group to add geo
    metadata. Memory usage is bounded by one row group.
    """

    name = "disk-rewrite"
    description = "Full file rewrite (reliable, memory-efficient via row groups)"
    supports_streaming = False
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
        geometry_info: dict | None = None,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Write query results to GeoParquet using DuckDB COPY then PyArrow rewrite."""
        from geoparquet_io.core.common import (
            compute_geometry_types_via_sql,
            validate_compression_settings,
        )
        from geoparquet_io.core.duckdb_utils import _wrap_query_with_wkb_conversion
        from geoparquet_io.core.geo_metadata import compute_bbox_via_sql
        from geoparquet_io.core.remote import is_remote_url, upload_if_remote

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        # Handle non-geo queries: write plain Parquet without geo metadata
        if geometry_column is None:
            self._write_plain_parquet_from_query(
                con, query, output_path, compression, compression_level, verbose
            )
            return

        compression_map = {
            "zstd": "ZSTD",
            "gzip": "GZIP",
            "snappy": "SNAPPY",
            "lz4": "LZ4",
            "none": "UNCOMPRESSED",
            "uncompressed": "UNCOMPRESSED",
            "brotli": "BROTLI",
        }
        duckdb_compression = compression_map.get(compression.lower(), "ZSTD")

        validated_compression, validated_level, _ = validate_compression_settings(
            compression, compression_level, verbose
        )

        is_remote = is_remote_url(output_path)
        work_dir = tempfile.mkdtemp(prefix="gpio_disk_rewrite_")

        try:
            temp_path = os.path.join(work_dir, "temp_duckdb.parquet")
            final_path = os.path.join(work_dir, "final.parquet") if is_remote else output_path

            if verbose:
                debug("Computing bbox via SQL...")
            bbox = compute_bbox_via_sql(con, query, geometry_column)

            if verbose:
                debug("Computing geometry types via SQL...")
            geometry_types = compute_geometry_types_via_sql(con, query, geometry_column)

            final_query = _wrap_query_with_wkb_conversion(query, geometry_column, con)

            escaped_temp = _escape_sql_string(temp_path)
            copy_query = f"""
                COPY ({final_query})
                TO '{escaped_temp}'
                (FORMAT PARQUET, COMPRESSION {duckdb_compression})
            """

            if verbose:
                debug(f"Writing via DuckDB COPY TO with {duckdb_compression} compression...")

            con.execute(copy_query)

            if verbose:
                pf = pq.ParquetFile(temp_path)
                debug(
                    f"DuckDB wrote {pf.metadata.num_rows:,} rows, {pf.metadata.num_row_groups} row groups"
                )

            geo_meta = build_geo_metadata(
                geometry_column=geometry_column,
                geoparquet_version=geoparquet_version,
                original_metadata=original_metadata,
                input_crs=input_crs,
                custom_metadata=custom_metadata,
                bbox=bbox,
                geometry_types=geometry_types,
                geometry_info=geometry_info,
            )

            self._rewrite_with_metadata(
                input_path=temp_path,
                output_path=final_path,
                geo_meta=geo_meta,
                compression=validated_compression,
                compression_level=validated_level,
                verbose=verbose,
                extra_kv_metadata=extra_kv_metadata,
            )

            os.unlink(temp_path)

            if is_remote:
                upload_if_remote(final_path, output_path, is_directory=False, verbose=verbose)
                os.unlink(final_path)

        finally:
            if os.path.exists(work_dir):
                import shutil

                shutil.rmtree(work_dir, ignore_errors=True)

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
        """Write Arrow table to GeoParquet using temporary file and rewrite."""
        from geoparquet_io.core.common import _detect_version_from_table

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        # Handle non-geo tables: write plain Parquet without geo metadata
        if geometry_column is None:
            self._write_plain_parquet_from_table(
                table, output_path, compression, compression_level, verbose
            )
            return

        # Auto-detect version from table schema metadata if not specified
        effective_version = geoparquet_version
        if effective_version is None:
            effective_version = _detect_version_from_table(table, verbose)

        con = get_duckdb_connection(load_httpfs=False)
        try:
            con.register("input_table", table)

            # Convert WKB bytes to GEOMETRY for proper spatial processing
            query = f"""
                SELECT * REPLACE (ST_GeomFromWKB({quote_identifier(geometry_column)}) AS {quote_identifier(geometry_column)})
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
        compression_level: int | None,
        verbose: bool,
    ) -> None:
        """Write plain Parquet (no geo metadata) from an Arrow table."""
        from geoparquet_io.core.common import validate_compression_settings
        from geoparquet_io.core.remote import is_remote_url, upload_if_remote

        validated_compression, validated_level, _ = validate_compression_settings(
            compression, compression_level, verbose
        )

        pa_compression = validated_compression if validated_compression != "UNCOMPRESSED" else None
        writer_kwargs = {"compression": pa_compression}
        if validated_level is not None and pa_compression:
            writer_kwargs["compression_level"] = validated_level

        is_remote = is_remote_url(output_path)
        work_dir = tempfile.mkdtemp(prefix="gpio_disk_rewrite_") if is_remote else None

        try:
            local_path = os.path.join(work_dir, "output.parquet") if is_remote else output_path

            if verbose:
                debug(f"Writing plain Parquet with {validated_compression} compression...")

            pq.write_table(table, local_path, **writer_kwargs)

            if is_remote:
                upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

            if verbose:
                success(f"Wrote {table.num_rows:,} rows to {output_path}")
        finally:
            if work_dir and os.path.exists(work_dir):
                import shutil

                shutil.rmtree(work_dir, ignore_errors=True)

    def _write_plain_parquet_from_query(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        output_path: str,
        compression: str,
        compression_level: int | None,  # noqa: ARG002 - DuckDB COPY doesn't support compression_level
        verbose: bool,
    ) -> None:
        """Write plain Parquet (no geo metadata) from a query.

        Note: compression_level is accepted for API consistency but not used.
        DuckDB's COPY TO command doesn't support compression_level for most
        compression types. For compression_level support, use write_from_table
        which uses PyArrow directly.
        """
        from geoparquet_io.core.remote import is_remote_url, upload_if_remote

        compression_map = {
            "zstd": "ZSTD",
            "gzip": "GZIP",
            "snappy": "SNAPPY",
            "lz4": "LZ4",
            "none": "UNCOMPRESSED",
            "uncompressed": "UNCOMPRESSED",
            "brotli": "BROTLI",
        }
        duckdb_compression = compression_map.get(compression.lower(), "ZSTD")

        is_remote = is_remote_url(output_path)
        work_dir = tempfile.mkdtemp(prefix="gpio_disk_rewrite_") if is_remote else None

        try:
            from geoparquet_io.core.duckdb_utils import _escape_sql_string

            local_path = os.path.join(work_dir, "output.parquet") if is_remote else output_path
            escaped_path = _escape_sql_string(local_path)

            copy_query = f"""
                COPY ({query})
                TO '{escaped_path}'
                (FORMAT PARQUET, COMPRESSION {duckdb_compression})
            """

            if verbose:
                debug(f"Writing plain Parquet with {duckdb_compression} compression...")
            con.execute(copy_query)

            if is_remote:
                upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

            if verbose:
                pf = pq.ParquetFile(local_path)
                success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")
        finally:
            if work_dir and os.path.exists(work_dir):
                import shutil

                shutil.rmtree(work_dir, ignore_errors=True)

    def _rewrite_with_metadata(
        self,
        input_path: str,
        output_path: str,
        geo_meta: dict,
        compression: str,
        compression_level: int | None,
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Rewrite file with proper geo metadata, row group by row group."""
        pf = pq.ParquetFile(input_path)
        schema = pf.schema_arrow

        new_meta = dict(schema.metadata or {})
        new_meta[b"geo"] = json.dumps(geo_meta).encode("utf-8")
        if extra_kv_metadata:
            for key, value in extra_kv_metadata.items():
                bkey = key.encode("utf-8") if isinstance(key, str) else key
                bval = value.encode("utf-8") if isinstance(value, str) else value
                new_meta[bkey] = bval
        new_schema = schema.with_metadata(new_meta)

        if verbose:
            progress(f"Rewriting with geo metadata ({pf.metadata.num_row_groups} row groups)...")

        pa_compression = compression if compression != "UNCOMPRESSED" else None
        writer_kwargs = {
            "compression": pa_compression,
        }
        if compression_level is not None and pa_compression:
            writer_kwargs["compression_level"] = compression_level

        with pq.ParquetWriter(output_path, new_schema, **writer_kwargs) as writer:
            for i in range(pf.metadata.num_row_groups):
                table = pf.read_row_group(i)
                table = table.replace_schema_metadata(new_meta)
                writer.write_table(table)

                if verbose and (i + 1) % 10 == 0:
                    debug(f"Rewrote {i + 1}/{pf.metadata.num_row_groups} row groups...")

        if verbose:
            result_pf = pq.ParquetFile(output_path)
            success(f"Wrote {result_pf.metadata.num_rows:,} rows to {output_path}")
