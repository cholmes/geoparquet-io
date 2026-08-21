"""
Arrow streaming write strategy.

This strategy streams DuckDB query results as Arrow RecordBatches directly
to a ParquetWriter, maintaining constant memory usage regardless of file size.

Best for: Large files, memory-constrained environments
Memory: O(batch_size) - constant regardless of total size
Speed: Slightly slower due to batch overhead
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success
from geoparquet_io.core.write_strategies.base import BaseWriteStrategy

if TYPE_CHECKING:
    import duckdb

# Default batch size for streaming (100K rows per batch)
DEFAULT_BATCH_SIZE = 100_000


def _has_carried_bbox(original_metadata: dict | None, geometry_column: str) -> bool:
    """True when ``original_metadata`` still carries a bbox for this column.

    Callers that filter rows or move coordinates strip it first, which is the
    signal that this write must compute a fresh one.
    """
    from geoparquet_io.core.write_strategies.base import _parse_existing_geo_metadata

    geo_meta = _parse_existing_geo_metadata(original_metadata)
    if not isinstance(geo_meta, dict):
        return False
    col_meta = (geo_meta.get("columns") or {}).get(geometry_column)
    return isinstance(col_meta, dict) and col_meta.get("bbox") is not None


def _detect_bbox_column(schema: pa.Schema) -> dict:
    """Detect bbox column from schema using common naming conventions."""
    for name in schema.names:
        if name in ["bbox", "bounds", "extent"] or name.endswith("_bbox"):
            field = schema.field(name)
            if hasattr(field.type, "names") and set(field.type.names) >= {
                "xmin",
                "ymin",
                "xmax",
                "ymax",
            }:
                return {"has_bbox_column": True, "bbox_column_name": name}
    return {"has_bbox_column": False, "bbox_column_name": None}


def _check_geometry_type(
    con: duckdb.DuckDBPyConnection,
    query: str,
    geometry_column: str,
    verbose: bool,
) -> tuple[bool, str]:
    """Check if geometry column needs WKB conversion. Returns (needs_conversion, final_sql)."""
    from geoparquet_io.core.duckdb_utils import _wrap_query_with_wkb_conversion

    quoted_geom = geometry_column.replace('"', '""')
    type_result = con.execute(f'SELECT TYPEOF("{quoted_geom}") FROM ({query}) LIMIT 1').fetchone()
    duckdb_type = type_result[0] if type_result else None
    needs_wkb = duckdb_type == "GEOMETRY"

    if verbose:
        msg = "wrapping with WKB conversion" if needs_wkb else "no conversion needed"
        debug(f"Geometry column is {duckdb_type}, {msg}")

    final_sql = _wrap_query_with_wkb_conversion(query, geometry_column, con) if needs_wkb else query
    return needs_wkb, final_sql


class ArrowStreamingStrategy(BaseWriteStrategy):
    """
    Stream DuckDB results as RecordBatches to ParquetWriter.

    This strategy executes a SQL query and streams the results directly to
    a Parquet file, avoiding the memory spike of materializing the full
    result as an Arrow table. Memory usage is bounded by batch size.
    """

    name = "streaming"
    description = "Stream query results in batches for constant memory usage"
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
        geometry_info: dict | None = None,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Write query results to GeoParquet using streaming RecordBatch approach."""
        from geoparquet_io.core.common import validate_compression_settings
        from geoparquet_io.core.geo_metadata import GEOPARQUET_VERSIONS

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        # Setup compression and version config
        validated_compression, validated_level, _ = validate_compression_settings(
            compression or "ZSTD", compression_level, verbose
        )
        if validated_compression == "UNCOMPRESSED":
            validated_compression = None

        batch_size = row_group_rows or DEFAULT_BATCH_SIZE

        # Handle non-geo queries: write plain Parquet without geo metadata
        if geometry_column is None:
            self._write_plain_parquet_from_query(
                con, query, output_path, validated_compression, validated_level, batch_size, verbose
            )
            return

        version_config = GEOPARQUET_VERSIONS.get(geoparquet_version, GEOPARQUET_VERSIONS["1.1"])

        # Check geometry type and prepare SQL
        _, final_sql = _check_geometry_type(con, query, geometry_column, verbose)

        # Determine version flags
        use_native_geometry = geoparquet_version in ("2.0", "parquet-geo-only")
        should_add_geo_metadata = geoparquet_version != "parquet-geo-only"
        geoarrow_native = geoparquet_version == "1.1-geoarrow"

        # Pre-compute metadata. A carried bbox is only reusable when it survived
        # into original_metadata; if a caller invalidated it (filter, reproject,
        # multi-file merge) it must be recomputed here or the v1.x output would
        # ship with no bbox at all.
        precomputed_bbox, precomputed_geom_types = self._precompute_metadata(
            con,
            query,
            geometry_column,
            verbose,
            should_add_geo_metadata,
            need_bbox=use_native_geometry
            or not _has_carried_bbox(original_metadata, geometry_column),
        )

        # Compute the geoarrow target type once from whole-dataset geometry types.
        # All batches will be coerced to this single physical type (schema is fixed
        # from batch 1 and cannot change mid-stream).
        geoarrow_target_type = None
        geoarrow_encoding = None
        if geoarrow_native and should_add_geo_metadata:
            from geoparquet_io.core.common import compute_geometry_dimensions_via_sql
            from geoparquet_io.core.geoarrow_encoding import determine_geoarrow_target_type

            # Detect Z/M dimensionality so 3D/measured coordinates select a
            # dimension-aware native type (or fall back to WKB) rather than being
            # silently coerced down to 2D.
            dimensions = compute_geometry_dimensions_via_sql(con, query, geometry_column)
            geoarrow_target_type, geoarrow_encoding = determine_geoarrow_target_type(
                precomputed_geom_types, input_crs, dimensions=dimensions
            )
            if verbose:
                debug(f"1.1-geoarrow target encoding: {geoarrow_encoding}")

        if verbose:
            debug(f"Executing query with batch size {batch_size:,}...")

        result = con.execute(final_sql)
        reader = result.fetch_record_batch(rows_per_batch=batch_size)
        schema = reader.schema

        if geometry_column not in schema.names:
            raise ValueError(
                f"Geometry column '{geometry_column}' not found in query result. "
                f"Available columns: {schema.names}"
            )

        # Build geo metadata
        geo_meta = self._build_geo_metadata_for_query(
            schema,
            geometry_column,
            original_metadata,
            input_crs,
            verbose,
            version_config["metadata_version"],
            should_add_geo_metadata,
            precomputed_geom_types,
            precomputed_bbox,
            geometry_info,
            geoarrow_encoding=geoarrow_encoding,
        )

        # Prepare writer kwargs
        writer_kwargs = {"compression": validated_compression or "NONE"}
        if validated_level is not None and validated_compression:
            writer_kwargs["compression_level"] = validated_level

        if verbose:
            progress(f"Streaming to {output_path}...")

        # Stream batches to file
        self._stream_batches_to_file(
            reader,
            output_path,
            schema,
            geometry_column,
            geo_meta,
            use_native_geometry,
            input_crs,
            precomputed_geom_types,
            writer_kwargs,
            verbose,
            extra_kv_metadata=extra_kv_metadata,
            geoarrow_target_type=geoarrow_target_type,
        )

    def _precompute_metadata(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        geometry_column: str,
        verbose: bool,
        should_add_geo_metadata: bool,
        need_bbox: bool,
    ) -> tuple[list[float] | None, list[str]]:
        """Pre-compute bbox and geometry types in a single scan of the query."""
        from geoparquet_io.core.geo_metadata import compute_geo_stats_via_sql

        if not should_add_geo_metadata:
            return None, []

        if verbose:
            debug("Pre-computing bbox/geometry types via SQL...")
        return compute_geo_stats_via_sql(
            con, query, geometry_column, need_bbox=need_bbox, need_geometry_types=True
        )

    def _build_geo_metadata_for_query(
        self,
        schema: pa.Schema,
        geometry_column: str,
        original_metadata: dict | None,
        input_crs: dict | None,
        verbose: bool,
        metadata_version: str,
        should_add_geo_metadata: bool,
        precomputed_geom_types: list[str],
        precomputed_bbox: list[float] | None,
        geometry_info: dict | None = None,
        geoarrow_encoding: str | None = None,
    ) -> dict | None:
        """Build geo metadata for query results."""
        from geoparquet_io.core.crs_utils import apply_output_crs
        from geoparquet_io.core.geo_metadata import create_geo_metadata

        if not should_add_geo_metadata:
            return None

        bbox_info = _detect_bbox_column(schema)
        geo_meta = create_geo_metadata(
            original_metadata=original_metadata,
            geom_col=geometry_column,
            bbox_info=bbox_info,
            custom_metadata=None,
            verbose=verbose,
            version=metadata_version,
        )

        col_meta = geo_meta["columns"][geometry_column]
        apply_output_crs(col_meta, input_crs)
        col_meta["encoding"] = "WKB"
        col_meta["geometry_types"] = precomputed_geom_types
        if precomputed_bbox is not None:
            col_meta["bbox"] = precomputed_bbox

        # Override encoding when geoarrow_target_type resolved a native encoding.
        # geoarrow_encoding is "WKB" when mixed/unconvertible — no override needed.
        if geoarrow_encoding is not None and geoarrow_encoding != "WKB":
            col_meta["encoding"] = geoarrow_encoding

        # Handle secondary geometry columns from geometry_info
        if geometry_info:
            secondary_columns = geometry_info.get("secondary", [])
            column_metadata = geometry_info.get("metadata", {})

            for sec_col in secondary_columns:
                if sec_col not in geo_meta["columns"]:
                    geo_meta["columns"][sec_col] = {}

                sec_meta = geo_meta["columns"][sec_col]

                # Copy metadata from input for secondary columns
                if sec_col in column_metadata:
                    input_sec_meta = column_metadata[sec_col]
                    for key, value in input_sec_meta.items():
                        # Preserve input metadata (crs, encoding, geometry_types, etc.)
                        if key not in sec_meta:
                            sec_meta[key] = value

                # Ensure encoding is set (required by spec)
                if "encoding" not in sec_meta:
                    sec_meta["encoding"] = "WKB"

        return geo_meta

    def _stream_batches_to_file(
        self,
        reader,
        output_path: str,
        schema: pa.Schema,
        geometry_column: str,
        geo_meta: dict | None,
        use_native_geometry: bool,
        input_crs: dict | None,
        precomputed_geom_types: list[str],
        writer_kwargs: dict,
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None = None,
        geoarrow_target_type=None,
    ) -> None:
        """Stream batches from reader to Parquet file."""
        first_batch = None
        try:
            first_batch = next(iter(reader))
        except StopIteration:
            self._write_empty_result(
                output_path,
                schema,
                geometry_column,
                geo_meta,
                use_native_geometry,
                input_crs,
                writer_kwargs,
                verbose,
                extra_kv_metadata=extra_kv_metadata,
                geoarrow_target_type=geoarrow_target_type,
            )
            return

        schema_with_meta = self._build_streaming_schema(
            schema=schema,
            geometry_column=geometry_column,
            geo_meta=geo_meta,
            use_native_geometry=use_native_geometry,
            input_crs=input_crs,
            geom_types=precomputed_geom_types,
            verbose=verbose,
            extra_kv_metadata=extra_kv_metadata,
            geoarrow_target_type=geoarrow_target_type,
        )

        geoarrow_type = None
        if use_native_geometry:
            col_index = schema_with_meta.get_field_index(geometry_column)
            geoarrow_type = schema_with_meta.field(col_index).type

        rows_written, batches_written = self._write_all_batches(
            output_path,
            schema_with_meta,
            first_batch,
            reader,
            geometry_column,
            geoarrow_type,
            use_native_geometry,
            writer_kwargs,
            verbose,
            geoarrow_target_type=geoarrow_target_type,
        )

        if verbose:
            success(f"Wrote {rows_written:,} rows in {batches_written} row groups")

    def _write_empty_result(
        self,
        output_path: str,
        schema: pa.Schema,
        geometry_column: str,
        geo_meta: dict | None,
        use_native_geometry: bool,
        input_crs: dict | None,
        writer_kwargs: dict,
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None = None,
        geoarrow_target_type=None,
    ) -> None:
        """Handle writing an empty result set."""
        schema_with_meta = self._build_streaming_schema(
            schema=schema,
            geometry_column=geometry_column,
            geo_meta=geo_meta,
            use_native_geometry=use_native_geometry,
            input_crs=input_crs,
            geom_types=[],
            verbose=verbose,
            extra_kv_metadata=extra_kv_metadata,
            geoarrow_target_type=geoarrow_target_type,
        )
        with pq.ParquetWriter(output_path, schema_with_meta, **writer_kwargs):
            pass
        if verbose:
            success("Wrote 0 rows (empty result)")

    def _write_all_batches(
        self,
        output_path: str,
        schema_with_meta: pa.Schema,
        first_batch: pa.RecordBatch,
        reader,
        geometry_column: str,
        geoarrow_type,
        use_native_geometry: bool,
        writer_kwargs: dict,
        verbose: bool,
        geoarrow_target_type=None,
    ) -> tuple[int, int]:
        """Write all batches to Parquet file. Returns (rows_written, batches_written)."""
        rows_written = 0
        batches_written = 0

        with pq.ParquetWriter(output_path, schema_with_meta, **writer_kwargs) as writer:
            # Write first batch
            if use_native_geometry:
                first_batch = self._convert_batch_to_geoarrow(
                    first_batch, geometry_column, geoarrow_type, schema_with_meta
                )
            elif geoarrow_target_type is not None:
                first_batch = self._convert_batch_to_native_geoarrow(
                    first_batch, geometry_column, geoarrow_target_type, schema_with_meta
                )
            table_batch = pa.Table.from_batches([first_batch], schema=schema_with_meta)
            writer.write_table(table_batch)
            rows_written += first_batch.num_rows
            batches_written += 1

            # Write remaining batches
            for batch in reader:
                if use_native_geometry:
                    batch = self._convert_batch_to_geoarrow(
                        batch, geometry_column, geoarrow_type, schema_with_meta
                    )
                elif geoarrow_target_type is not None:
                    batch = self._convert_batch_to_native_geoarrow(
                        batch, geometry_column, geoarrow_target_type, schema_with_meta
                    )
                table_batch = pa.Table.from_batches([batch], schema=schema_with_meta)
                writer.write_table(table_batch)
                rows_written += batch.num_rows
                batches_written += 1

                if verbose and batches_written % 10 == 0:
                    debug(f"Written {rows_written:,} rows in {batches_written} batches...")

        return rows_written, batches_written

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
        """Write Arrow table to GeoParquet using batch streaming."""
        from geoparquet_io.core.common import (
            _compute_geometry_types,
            _detect_version_from_table,
            validate_compression_settings,
        )
        from geoparquet_io.core.crs_utils import apply_output_crs
        from geoparquet_io.core.geo_metadata import GEOPARQUET_VERSIONS, create_geo_metadata

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        validated_compression, validated_level, _ = validate_compression_settings(
            compression or "ZSTD", compression_level, verbose
        )
        if validated_compression == "UNCOMPRESSED":
            validated_compression = None

        batch_size = row_group_rows or DEFAULT_BATCH_SIZE

        # Handle non-geo tables: write plain Parquet without geo metadata
        if geometry_column is None:
            self._write_plain_parquet_from_table(
                table, output_path, validated_compression, validated_level, batch_size, verbose
            )
            return

        # Auto-detect version from table schema metadata if not specified
        effective_version = geoparquet_version
        if effective_version is None:
            effective_version = _detect_version_from_table(table, verbose)

        version_config = GEOPARQUET_VERSIONS.get(effective_version, GEOPARQUET_VERSIONS["1.1"])
        metadata_version = version_config["metadata_version"]

        use_native_geometry = effective_version in ("2.0", "parquet-geo-only")
        should_add_geo_metadata = effective_version != "parquet-geo-only"
        geoarrow_native = effective_version == "1.1-geoarrow"

        # Compute the geoarrow target type once from the whole-table geometry types,
        # so every batch is coerced to one physical type (schema is fixed up front).
        geoarrow_target_type = None
        geoarrow_encoding = None

        geo_meta = None
        if should_add_geo_metadata:
            bbox_info = {"has_bbox_column": False, "bbox_column_name": None}
            geom_types = _compute_geometry_types(table, geometry_column, verbose)

            if geoarrow_native:
                from geoparquet_io.core.geoarrow_encoding import (
                    detect_wkb_dimensions,
                    determine_geoarrow_target_type,
                )

                # Detect Z/M dimensionality from the actual geometry so 3D/measured
                # coordinates select a dimension-aware native type (or fall back to
                # WKB) instead of being silently coerced down to 2D.
                dimensions = detect_wkb_dimensions(table.column(geometry_column))
                geoarrow_target_type, geoarrow_encoding = determine_geoarrow_target_type(
                    geom_types, input_crs, dimensions=dimensions
                )
                if verbose:
                    debug(f"1.1-geoarrow target encoding: {geoarrow_encoding}")

            geo_meta = create_geo_metadata(
                original_metadata=None,
                geom_col=geometry_column,
                bbox_info=bbox_info,
                custom_metadata=custom_metadata,
                verbose=verbose,
                version=metadata_version,
            )
            geo_meta["columns"][geometry_column]["encoding"] = "WKB"
            geo_meta["columns"][geometry_column]["geometry_types"] = geom_types

            apply_output_crs(geo_meta["columns"][geometry_column], input_crs)

            # Override encoding when geoarrow_target_type resolved a native encoding.
            # geoarrow_encoding is "WKB" for mixed/unconvertible geometry — no override.
            if geoarrow_encoding is not None and geoarrow_encoding != "WKB":
                geo_meta["columns"][geometry_column]["encoding"] = geoarrow_encoding

        schema_with_meta = self._build_streaming_schema(
            schema=table.schema,
            geometry_column=geometry_column,
            geo_meta=geo_meta,
            use_native_geometry=use_native_geometry,
            input_crs=input_crs,
            geom_types=geo_meta["columns"][geometry_column]["geometry_types"] if geo_meta else [],
            verbose=verbose,
            geoarrow_target_type=geoarrow_target_type,
        )

        writer_kwargs = {"compression": validated_compression or "NONE"}
        if validated_level is not None and validated_compression:
            writer_kwargs["compression_level"] = validated_level

        geoarrow_type = None
        if use_native_geometry:
            col_index = schema_with_meta.get_field_index(geometry_column)
            geoarrow_type = schema_with_meta.field(col_index).type

        rows_written = 0
        batches_written = 0

        with pq.ParquetWriter(output_path, schema_with_meta, **writer_kwargs) as writer:
            for batch in table.to_batches(max_chunksize=batch_size):
                if use_native_geometry:
                    batch = self._convert_batch_to_geoarrow(
                        batch, geometry_column, geoarrow_type, schema_with_meta
                    )
                elif geoarrow_target_type is not None:
                    batch = self._convert_batch_to_native_geoarrow(
                        batch, geometry_column, geoarrow_target_type, schema_with_meta
                    )
                table_batch = pa.Table.from_batches([batch], schema=schema_with_meta)
                writer.write_table(table_batch)
                rows_written += batch.num_rows
                batches_written += 1

        if verbose:
            success(f"Wrote {rows_written:,} rows in {batches_written} row groups")

    def _build_streaming_schema(
        self,
        schema: pa.Schema,
        geometry_column: str,
        geo_meta: dict | None,
        use_native_geometry: bool,
        input_crs: dict | None,
        geom_types: list[str],
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None = None,
        geoarrow_target_type=None,
    ) -> pa.Schema:
        """Build the output schema for streaming write."""
        from geoparquet_io.core.crs_utils import is_default_crs

        schema_metadata = dict(schema.metadata or {})

        if use_native_geometry:
            import geoarrow.pyarrow as ga

            geoarrow_type = ga.wkb()

            if input_crs and not is_default_crs(input_crs):
                geoarrow_type = geoarrow_type.with_crs(input_crs)
                if verbose:
                    debug("Built geoarrow type with CRS")

            new_fields = []
            for field in schema:
                if field.name == geometry_column:
                    new_fields.append(pa.field(geometry_column, geoarrow_type))
                else:
                    new_fields.append(field)
            schema = pa.schema(new_fields)
        elif geoarrow_target_type is not None:
            # 1.1-geoarrow path: replace geometry field with the native GeoArrow extension type.
            # Only triggers when geometry is WKB (binary); already-native inputs are not
            # routed here (Task 3 keeps them on duckdb-kv).
            new_fields = [
                pa.field(geometry_column, geoarrow_target_type) if f.name == geometry_column else f
                for f in schema
            ]
            schema = pa.schema(new_fields)

        if geo_meta is not None:
            if "geometry_types" not in geo_meta["columns"].get(geometry_column, {}):
                geo_meta["columns"][geometry_column]["geometry_types"] = geom_types
            schema_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")

        if extra_kv_metadata:
            for key, value in extra_kv_metadata.items():
                bkey = key.encode("utf-8") if isinstance(key, str) else key
                bval = value.encode("utf-8") if isinstance(value, str) else value
                schema_metadata[bkey] = bval

        return schema.with_metadata(schema_metadata)

    def _convert_batch_to_geoarrow(
        self,
        batch: pa.RecordBatch,
        geometry_column: str,
        geoarrow_type,
        schema_with_geoarrow: pa.Schema,
    ) -> pa.RecordBatch:
        """Convert a record batch's geometry column to geoarrow extension type."""
        import geoarrow.pyarrow as ga

        col_index = batch.schema.get_field_index(geometry_column)
        geom_col = batch.column(col_index)
        geoarrow_arr = ga.as_wkb(geom_col)

        if geoarrow_arr.type != geoarrow_type:
            geoarrow_arr = pa.ExtensionArray.from_buffers(
                geoarrow_type,
                len(geoarrow_arr),
                geoarrow_arr.buffers(),
            )

        new_columns = []
        for i in range(batch.num_columns):
            if i == col_index:
                new_columns.append(geoarrow_arr)
            else:
                new_columns.append(batch.column(i))

        return pa.RecordBatch.from_arrays(new_columns, schema=schema_with_geoarrow)

    def _convert_batch_to_native_geoarrow(
        self,
        batch: pa.RecordBatch,
        geometry_column: str,
        geoarrow_target_type,
        schema_with_meta: pa.Schema,
    ) -> pa.RecordBatch:
        """Convert WKB binary geometry column to native GeoArrow nested type (1.1-geoarrow path).

        Only converts when the incoming column is binary/large_binary (WKB) or a
        geoarrow.wkb extension type. If it is already a nested GeoArrow type, it is
        left untouched — though this path should not normally be reached because Task 3
        routing sends already-native inputs to the duckdb-kv strategy instead.
        """
        from geoparquet_io.core.geoarrow_encoding import wkb_array_to_geoarrow

        col_index = batch.schema.get_field_index(geometry_column)
        geom_col = batch.column(col_index)

        col_type = batch.schema.field(geometry_column).type
        is_wkb_binary = pa.types.is_binary(col_type) or pa.types.is_large_binary(col_type)
        is_wkb_extension = (
            isinstance(col_type, pa.ExtensionType)
            and getattr(col_type, "extension_name", "") == "geoarrow.wkb"
        )

        if is_wkb_binary or is_wkb_extension:
            converted = wkb_array_to_geoarrow(geom_col, geoarrow_target_type)
            cols = [
                converted if i == col_index else batch.column(i) for i in range(batch.num_columns)
            ]
            return pa.RecordBatch.from_arrays(cols, schema=schema_with_meta)

        # Already native or unexpected type — leave untouched but cast schema
        return pa.RecordBatch.from_arrays(
            [batch.column(i) for i in range(batch.num_columns)],
            schema=schema_with_meta,
        )

    def _write_plain_parquet_from_table(
        self,
        table: pa.Table,
        output_path: str,
        compression: str | None,
        compression_level: int | None,
        batch_size: int,
        verbose: bool,
    ) -> None:
        """Write plain Parquet (no geo metadata) from an Arrow table using streaming."""
        writer_kwargs = {"compression": compression or "NONE"}
        if compression_level is not None and compression:
            writer_kwargs["compression_level"] = compression_level

        rows_written = 0
        batches_written = 0

        if verbose:
            debug(f"Writing plain Parquet in batches of {batch_size:,}...")

        with pq.ParquetWriter(output_path, table.schema, **writer_kwargs) as writer:
            for batch in table.to_batches(max_chunksize=batch_size):
                table_batch = pa.Table.from_batches([batch], schema=table.schema)
                writer.write_table(table_batch)
                rows_written += batch.num_rows
                batches_written += 1

        if verbose:
            success(f"Wrote {rows_written:,} rows in {batches_written} row groups")

    def _write_plain_parquet_from_query(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        output_path: str,
        compression: str | None,
        compression_level: int | None,
        batch_size: int,
        verbose: bool,
    ) -> None:
        """Write plain Parquet (no geo metadata) from a query using streaming."""
        if verbose:
            debug(f"Executing query with batch size {batch_size:,}...")

        result = con.execute(query)
        reader = result.fetch_record_batch(rows_per_batch=batch_size)
        schema = reader.schema

        writer_kwargs = {"compression": compression or "NONE"}
        if compression_level is not None and compression:
            writer_kwargs["compression_level"] = compression_level

        rows_written = 0
        batches_written = 0

        first_batch = None
        try:
            first_batch = next(iter(reader))
        except StopIteration:
            # Empty result set
            with pq.ParquetWriter(output_path, schema, **writer_kwargs):
                pass
            if verbose:
                success("Wrote 0 rows (empty result)")
            return

        with pq.ParquetWriter(output_path, schema, **writer_kwargs) as writer:
            # Write first batch
            table_batch = pa.Table.from_batches([first_batch], schema=schema)
            writer.write_table(table_batch)
            rows_written += first_batch.num_rows
            batches_written += 1

            # Write remaining batches
            for batch in reader:
                table_batch = pa.Table.from_batches([batch], schema=schema)
                writer.write_table(table_batch)
                rows_written += batch.num_rows
                batches_written += 1

        if verbose:
            success(f"Wrote {rows_written:,} rows in {batches_written} row groups")
