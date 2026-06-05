"""Improve GeoParquet files for fiboa compliance."""

from __future__ import annotations

import os
import tempfile

from geoparquet_io.core.logging_config import progress, success, warn

FIBOA_SCHEMA_URL = "https://fiboa.org/specification/v0.3.0/schema.yaml"
VECOREL_CORE_URL = "https://vecorel.org/specification/v0.1.0/schema.yaml"
VECOREL_ADMIN_URL = "https://vecorel.org/administrative-division-extension/v0.1.0/schema.yaml"
VECOREL_METRICS_URL = "https://vecorel.org/geometry-metrics-extension/v0.1.0/schema.yaml"

VALID_CATEGORIES = {
    "conceptual",
    "operational",
    "economic",
    "administrative",
    "other",
}


def improve_fiboa(
    input_file: str,
    output_file: str,
    add_metrics: bool = False,
    add_admin: bool = False,
    add_schemas: bool = False,
    sort_hilbert: bool = True,
    determination_datetime: str | None = None,
    determination_method: str | None = None,
    category: list[str] | None = None,
    keep_source_columns: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    verbose: bool = False,
) -> None:
    """Improve a GeoParquet file for fiboa compliance."""
    if row_group_rows is None and row_group_size_mb is None:
        row_group_rows = 50_000

    has_work = (
        add_metrics
        or add_admin
        or add_schemas
        or sort_hilbert
        or determination_datetime
        or determination_method
        or category
    )
    if not has_work:
        warn("No improvements requested. Use -sz, -a, -s, or column options.")
        return

    # Auto-downgrade GeoParquet 2.0 to 1.1 (vecorel/fiboa don't support 2.0 yet)
    user_requested_version = geoparquet_version is not None
    geoparquet_version, need_bbox = _handle_geoparquet_version(
        input_file, geoparquet_version, user_requested_version, verbose
    )

    current_input = input_file
    temp_files: list[str] = []

    # Track remaining steps for temp file routing
    remaining_steps = []
    if add_metrics:
        remaining_steps.append("metrics")
    if add_admin:
        remaining_steps.append("admin")
    if determination_datetime:
        remaining_steps.append("dt_datetime")
    if determination_method:
        remaining_steps.append("dt_method")
    if category:
        remaining_steps.append("category")
    if sort_hilbert:
        remaining_steps.append("hilbert")
    if add_schemas:
        remaining_steps.append("schemas")

    try:
        if add_metrics:
            remaining_steps.remove("metrics")
            progress("Adding geometry metrics (area + perimeter)...")
            next_output = _get_next_output(output_file, bool(remaining_steps), temp_files)

            from geoparquet_io.core.add.geometry_metrics import add_geometry_metrics

            add_geometry_metrics(
                current_input,
                next_output,
                vecorel=True,
                verbose=verbose,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                geoparquet_version=geoparquet_version,
                overwrite=overwrite,
            )
            current_input = next_output
            success("Added metrics:area and metrics:perimeter")

        if add_admin:
            remaining_steps.remove("admin")
            progress("Adding admin divisions (country_code + subdivision_code)...")
            next_output = _get_next_output(output_file, bool(remaining_steps), temp_files)

            from geoparquet_io.core.add.admin_divisions import add_admin_divisions_multi

            add_admin_divisions_multi(
                current_input,
                next_output,
                dataset_name="overture",
                levels=["country", "region"],
                vecorel=True,
                verbose=verbose,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                geoparquet_version=geoparquet_version,
                overwrite=True,
            )
            current_input = next_output
            success("Added admin:country_code and admin:subdivision_code")

        if determination_datetime:
            remaining_steps.remove("dt_datetime")
            next_output = _get_next_output(output_file, bool(remaining_steps), temp_files)
            _add_determination_datetime(
                current_input,
                next_output,
                determination_datetime,
                verbose,
                compression,
                compression_level,
                row_group_size_mb,
                row_group_rows,
                geoparquet_version,
                keep_source=keep_source_columns,
            )
            current_input = next_output

        if determination_method:
            remaining_steps.remove("dt_method")
            next_output = _get_next_output(output_file, bool(remaining_steps), temp_files)
            _add_literal_column(
                current_input,
                next_output,
                "determination:method",
                f"'{determination_method}'",
                verbose,
                compression,
                compression_level,
                row_group_size_mb,
                row_group_rows,
                geoparquet_version,
            )
            current_input = next_output
            success(f"Set determination:method = '{determination_method}'")

        if category:
            remaining_steps.remove("category")
            next_output = _get_next_output(output_file, bool(remaining_steps), temp_files)
            values_str = ", ".join(f"'{c}'" for c in category)
            _add_literal_column(
                current_input,
                next_output,
                "category",
                f"[{values_str}]",
                verbose,
                compression,
                compression_level,
                row_group_size_mb,
                row_group_rows,
                geoparquet_version,
            )
            current_input = next_output
            success(f"Set category = {category}")

        if sort_hilbert:
            remaining_steps.remove("hilbert")
            progress("Sorting by Hilbert space-filling curve...")
            next_output = _get_next_output(output_file, bool(remaining_steps), temp_files)

            from geoparquet_io.core.hilbert_order import hilbert_order

            hilbert_order(
                current_input,
                next_output,
                add_bbox_flag=True,
                verbose=verbose,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                geoparquet_version=geoparquet_version,
                overwrite=True,
            )
            current_input = next_output
            success("Sorted by Hilbert curve for spatial query performance")

        if add_schemas:
            remaining_steps.remove("schemas")
            progress("Updating Vecorel schemas metadata...")
            _add_schemas_metadata(
                current_input,
                output_file if current_input != output_file else None,
                add_metrics,
                add_admin,
                compression,
                compression_level,
                row_group_size_mb,
                row_group_rows,
                geoparquet_version,
                overwrite,
                verbose,
            )
            success("Updated Vecorel schemas metadata with fiboa URLs")

        # Add bbox column if needed (e.g., after 2.0→1.1 downgrade)
        # Hilbert sorting already adds bbox via add_bbox_flag=True
        if need_bbox and not sort_hilbert:
            _ensure_bbox(output_file, verbose)

        from geoparquet_io.core.constants import ensure_vecorel_columns

        ensure_vecorel_columns(output_file, verbose)

        success(f"\nfiboa improvements complete: {output_file}")

    finally:
        for tf in temp_files:
            if os.path.exists(tf):
                os.unlink(tf)


def _add_determination_datetime(
    input_file,
    output_file,
    value,
    verbose,
    compression,
    compression_level,
    row_group_size_mb,
    row_group_rows,
    geoparquet_version,
    keep_source: bool = False,
) -> None:
    """Add determination:datetime column from a source column or literal."""
    from geoparquet_io.core.common import add_computed_column
    from geoparquet_io.core.duckdb_metadata import get_column_names
    from geoparquet_io.core.file_utils import safe_file_url

    url = safe_file_url(input_file, verbose=False)
    columns = get_column_names(url)
    replace_col = None

    if value in columns:
        sql_expr = f"CAST(\"{value}\" AS TIMESTAMP) AT TIME ZONE 'UTC'"
        if keep_source:
            progress(f"Mapping column '{value}' → determination:datetime (keeping '{value}')")
        else:
            progress(f"Mapping column '{value}' → determination:datetime (removing '{value}')")
            replace_col = value
    else:
        sql_expr = f"TIMESTAMP '{value}' AT TIME ZONE 'UTC'"
        progress(f"Setting determination:datetime = '{value}'")

    add_computed_column(
        input_file,
        output_file,
        column_name="determination:datetime",
        sql_expression=sql_expr,
        replace_column=replace_col,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version,
    )
    success("Added determination:datetime")


def _add_literal_column(
    input_file,
    output_file,
    column_name,
    sql_value,
    verbose,
    compression,
    compression_level,
    row_group_size_mb,
    row_group_rows,
    geoparquet_version,
) -> None:
    """Add a column with a literal value for all rows."""
    from geoparquet_io.core.common import add_computed_column

    add_computed_column(
        input_file,
        output_file,
        column_name=column_name,
        sql_expression=sql_value,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version,
    )


def _get_next_output(final_output: str, more_steps: bool, temp_files: list[str]) -> str:
    """Get the next output path: temp file if more steps remain, else final output."""
    if more_steps:
        fd, path = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        os.unlink(path)
        temp_files.append(path)
        return path
    return final_output


def _add_schemas_metadata(
    input_file,
    output_file,
    has_metrics,
    has_admin,
    compression,
    compression_level,
    row_group_size_mb,
    row_group_rows,
    geoparquet_version,
    overwrite,
    verbose,
) -> None:
    """Add/update Vecorel schemas metadata with all applicable fiboa URLs."""
    from geoparquet_io.core.common import get_parquet_metadata, write_parquet_with_metadata
    from geoparquet_io.core.constants import build_collection_metadata
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.file_utils import safe_file_url
    from geoparquet_io.core.remote import needs_httpfs

    schemas = [FIBOA_SCHEMA_URL, VECOREL_CORE_URL]
    if has_metrics:
        schemas.append(VECOREL_METRICS_URL)
    if has_admin:
        schemas.append(VECOREL_ADMIN_URL)

    metadata, _ = get_parquet_metadata(input_file, verbose)
    extra_kv = build_collection_metadata(schemas, metadata)

    actual_output = output_file or input_file
    input_url = safe_file_url(input_file, verbose)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_file))
    query = f"SELECT * FROM '{input_url}'"

    write_parquet_with_metadata(
        con,
        query,
        actual_output,
        original_metadata=metadata,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
        extra_kv_metadata=extra_kv,
    )


def _handle_geoparquet_version(
    input_file: str,
    geoparquet_version: str | None,
    user_requested: bool,
    verbose: bool,
) -> tuple[str | None, bool]:
    """Handle GeoParquet version for vecorel/fiboa compatibility.

    Returns (effective_version, need_bbox):
    - If input is 2.0 and user didn't request a version, downgrades to 1.1
    - If user explicitly requests 2.0, warns but respects it
    - Returns need_bbox=True when downgrading from 2.0 (bbox column needed for 1.x)
    """
    from geoparquet_io.core.common import detect_geoparquet_file_type

    file_info = detect_geoparquet_file_type(input_file, verbose=False)
    is_v2 = file_info["file_type"] in ("geoparquet_v2", "parquet_geo_only")

    if not is_v2:
        return geoparquet_version, False

    if user_requested and geoparquet_version == "2.0":
        warn(
            "GeoParquet 2.0 output requested, but Vecorel/fiboa specifications "
            "do not yet support GeoParquet 2.0. Output will not pass vec validate."
        )
        return geoparquet_version, False

    progress(
        "Input is GeoParquet 2.0 — downgrading to 1.1 for Vecorel/fiboa compatibility. "
        "Use --geoparquet-version 2.0 to override."
    )
    return "1.1", True


def _ensure_bbox(parquet_file: str, verbose: bool) -> None:
    """Add bbox column if the file doesn't already have one."""
    from geoparquet_io.core.common import add_bbox, check_bbox_structure

    bbox_info = check_bbox_structure(parquet_file, verbose=False)
    if bbox_info["has_bbox_column"]:
        return

    progress("Adding bbox column (recommended for GeoParquet 1.x)...")
    add_bbox(parquet_file, verbose=verbose)
    success("Added bbox column")
