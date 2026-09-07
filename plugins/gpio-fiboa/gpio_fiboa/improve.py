"""Improve GeoParquet files for fiboa compliance."""

from __future__ import annotations

import re
import tempfile
from pathlib import Path

from geoparquet_io.core.constants import (
    FIBOA_CORE_SCHEMA,
    VECOREL_ADMIN_SCHEMA,
    VECOREL_CORE_SCHEMA,
    VECOREL_METRICS_SCHEMA,
)
from geoparquet_io.core.logging_config import progress, success, warn

VALID_CATEGORIES = {
    "conceptual",
    "operational",
    "economic",
    "administrative",
    "other",
}

# Mirrors the CLI's --determination-method choices. Validated here too so the
# Python API (improve_fiboa) can't pass unchecked values into SQL.
VALID_DETERMINATION_METHODS = {
    "manual",
    "surveyed",
    "driven",
    "auto-operation",
    "auto-imagery",
    "unknown",
}

DATETIME_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$"
)


def _validate_improve_inputs(category: list[str] | None, determination_method: str | None) -> None:
    """Validate values that get interpolated into SQL.

    The CLI already checks these, but ``improve_fiboa`` is a public API entry
    point, so re-check here to keep unvalidated input out of the query (defense
    in depth, independent of the CLI layer).
    """
    if category:
        invalid = [c for c in category if c not in VALID_CATEGORIES]
        if invalid:
            raise ValueError(
                f"Invalid categories: {invalid}. Allowed: {', '.join(sorted(VALID_CATEGORIES))}"
            )
    if determination_method and determination_method not in VALID_DETERMINATION_METHODS:
        raise ValueError(
            f"Invalid determination_method: {determination_method!r}. "
            f"Allowed: {', '.join(sorted(VALID_DETERMINATION_METHODS))}"
        )


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
    _validate_improve_inputs(category, determination_method)

    if row_group_rows is None and row_group_size_mb is None:
        row_group_rows = 50_000

    has_work = (
        add_metrics
        or add_admin
        or add_schemas
        or determination_datetime
        or determination_method
        or category
    )
    if not has_work and not sort_hilbert:
        warn("No improvements requested. Use -sz, -a, -s, or column options.")
        return

    # Enforce overwrite intent once, against the real output. The pipeline runs
    # every intermediate step to a temp file and only the final step writes
    # output_file, so a single up-front check is sufficient. This also rejects
    # output_file == input_file. (Individual steps below force overwrite=True
    # because they only ever write fresh temp files or this cleared output.)
    from geoparquet_io.core.file_utils import handle_output_overwrite

    handle_output_overwrite(output_file, overwrite, input_file)

    user_requested_version = geoparquet_version is not None
    geoparquet_version, need_bbox = _handle_geoparquet_version(
        input_file, geoparquet_version, user_requested_version, verbose
    )

    write_opts = {
        "compression": compression,
        "compression_level": compression_level,
        "row_group_size_mb": row_group_size_mb,
        "row_group_rows": row_group_rows,
        "geoparquet_version": geoparquet_version,
    }

    current_input = input_file
    temp_files: list[str] = []

    # Add bbox up front when an admin spatial join is requested but the input
    # lacks a bbox column. The join's cheap bbox pre-filter needs it (otherwise
    # it falls back to a full-geometry intersection over every candidate), and
    # the column survives the join even though the join does not preserve row
    # order. Hilbert sorting still runs last, so the final file stays spatially
    # ordered. Written at the target version so a native 2.0 input becomes 1.1.
    add_bbox_first = add_admin and not _has_bbox_column(input_file)

    steps_remaining = sum(
        [
            add_bbox_first,
            add_metrics,
            add_admin,
            bool(determination_datetime),
            bool(determination_method),
            bool(category),
            sort_hilbert,
            add_schemas,
        ]
    )

    try:
        if add_bbox_first:
            steps_remaining -= 1
            progress("Adding bbox column (enables the admin spatial-join pre-filter)...")
            next_output = _get_next_output(output_file, steps_remaining > 0, temp_files)

            from geoparquet_io.core.add.bbox import add_bbox_column

            add_bbox_column(
                current_input,
                next_output,
                overwrite=True,
                verbose=verbose,
                **write_opts,
            )
            current_input = next_output
            success("Added bbox column")

        if add_metrics:
            steps_remaining -= 1
            progress("Adding geometry metrics (area + perimeter)...")
            next_output = _get_next_output(output_file, steps_remaining > 0, temp_files)

            from geoparquet_io.core.add.geometry_metrics import add_geometry_metrics

            add_geometry_metrics(
                current_input,
                next_output,
                vecorel=False,
                verbose=verbose,
                overwrite=True,
                **write_opts,
            )
            current_input = next_output
            success("Added metrics:area and metrics:perimeter")

        if add_admin:
            steps_remaining -= 1
            progress("Adding admin divisions (country_code + subdivision_code)...")
            next_output = _get_next_output(output_file, steps_remaining > 0, temp_files)

            from geoparquet_io.core.add.admin_divisions import add_admin_divisions_multi

            add_admin_divisions_multi(
                current_input,
                next_output,
                dataset_name="overture",
                levels=["country", "region"],
                # vecorel=True yields the fiboa-spec admin:country_code /
                # admin:subdivision_code column names (not overture_*).
                vecorel=True,
                verbose=verbose,
                overwrite=True,
                **write_opts,
            )
            current_input = next_output
            success("Added admin:country_code and admin:subdivision_code")

        if determination_datetime:
            steps_remaining -= 1
            next_output = _get_next_output(output_file, steps_remaining > 0, temp_files)
            _add_determination_datetime(
                current_input,
                next_output,
                determination_datetime,
                verbose,
                keep_source=keep_source_columns,
                **write_opts,
            )
            current_input = next_output

        if determination_method:
            steps_remaining -= 1
            next_output = _get_next_output(output_file, steps_remaining > 0, temp_files)

            from geoparquet_io.core.common import add_computed_column

            add_computed_column(
                current_input,
                next_output,
                column_name="determination:method",
                sql_expression=f"'{determination_method}'",
                verbose=verbose,
                **write_opts,
            )
            current_input = next_output
            success(f"Set determination:method = '{determination_method}'")

        if category:
            steps_remaining -= 1
            next_output = _get_next_output(output_file, steps_remaining > 0, temp_files)

            from geoparquet_io.core.common import add_computed_column

            values_str = ", ".join(f"'{c}'" for c in category)
            add_computed_column(
                current_input,
                next_output,
                column_name="category",
                sql_expression=f"[{values_str}]",
                verbose=verbose,
                **write_opts,
            )
            current_input = next_output
            success(f"Set category = {category}")

        if sort_hilbert:
            steps_remaining -= 1
            progress("Sorting by Hilbert space-filling curve...")
            next_output = _get_next_output(output_file, steps_remaining > 0, temp_files)

            from geoparquet_io.core.hilbert_order import hilbert_order

            hilbert_order(
                current_input,
                next_output,
                add_bbox_flag=True,
                verbose=verbose,
                overwrite=True,
                **write_opts,
            )
            current_input = next_output
            success("Sorted by Hilbert curve for spatial query performance")

        if add_schemas:
            steps_remaining -= 1
            progress("Updating Vecorel schemas metadata...")
            _add_schemas_metadata(
                current_input,
                output_file if current_input != output_file else None,
                add_metrics,
                add_admin,
                verbose=verbose,
                **write_opts,
            )
            success("Updated Vecorel schemas metadata with fiboa URLs")

        if need_bbox and not sort_hilbert:
            _ensure_bbox(output_file, verbose)

        from geoparquet_io.core.constants import ensure_vecorel_columns

        ensure_vecorel_columns(output_file, verbose)

        success(f"\nfiboa improvements complete: {output_file}")

    finally:
        for tf in temp_files:
            Path(tf).unlink(missing_ok=True)


def _add_determination_datetime(
    input_file,
    output_file,
    value,
    verbose,
    keep_source: bool = False,
    **write_opts,
) -> None:
    """Add determination:datetime column from a source column or literal."""
    from geoparquet_io.core.common import add_computed_column
    from geoparquet_io.core.duckdb_metadata import get_column_names
    from geoparquet_io.core.duckdb_utils import quote_identifier

    # RAW path: get_column_names escapes its own argument (issue #718).
    columns = get_column_names(input_file)
    replace_col = None

    if value in columns:
        sql_expr = f"CAST({quote_identifier(value)} AS TIMESTAMP) AT TIME ZONE 'UTC'"
        if keep_source:
            progress(f"Mapping column '{value}' -> determination:datetime (keeping '{value}')")
        else:
            progress(f"Mapping column '{value}' -> determination:datetime (removing '{value}')")
            replace_col = value
    else:
        if not DATETIME_PATTERN.match(value):
            raise ValueError(f"Invalid datetime literal: {value}")
        sql_expr = f"TIMESTAMP '{value}' AT TIME ZONE 'UTC'"
        progress(f"Setting determination:datetime = '{value}'")

    add_computed_column(
        input_file,
        output_file,
        column_name="determination:datetime",
        sql_expression=sql_expr,
        replace_column=replace_col,
        verbose=verbose,
        **write_opts,
    )
    success("Added determination:datetime")


def _get_next_output(final_output: str, more_steps: bool, temp_files: list[str]) -> str:
    """Get the next output path: temp file if more steps remain, else final output."""
    if more_steps:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            path = tmp.name
        # The next step writes this path itself; hand it back as a free name.
        Path(path).unlink(missing_ok=True)
        temp_files.append(path)
        return path
    return final_output


def _add_schemas_metadata(
    input_file,
    output_file,
    has_metrics,
    has_admin,
    **kwargs,
) -> None:
    """Add/update Vecorel schemas metadata with all applicable fiboa URLs."""
    from geoparquet_io.core.common import (
        get_duckdb_connection,
        get_parquet_metadata,
        needs_httpfs,
        write_parquet_with_metadata,
    )
    from geoparquet_io.core.constants import build_collection_metadata
    from geoparquet_io.core.duckdb_metadata import get_column_names
    from geoparquet_io.core.duckdb_utils import sql_path
    from geoparquet_io.core.file_utils import resolve_file_url

    verbose = kwargs.pop("verbose", False)

    # RAW path: get_column_names escapes its own argument (issue #718).
    columns = get_column_names(input_file)

    schemas = [FIBOA_CORE_SCHEMA, VECOREL_CORE_SCHEMA]
    if has_metrics or "metrics:area" in columns:
        schemas.append(VECOREL_METRICS_SCHEMA)
    if has_admin or "admin:country_code" in columns:
        schemas.append(VECOREL_ADMIN_SCHEMA)

    metadata, _ = get_parquet_metadata(input_file, verbose)
    extra_kv = build_collection_metadata(schemas, metadata)

    actual_output = output_file or input_file
    input_url = resolve_file_url(input_file, verbose)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_file))
    query = f"SELECT * FROM {sql_path(input_url)}"

    write_parquet_with_metadata(
        con,
        query,
        actual_output,
        original_metadata=metadata,
        verbose=verbose,
        extra_kv_metadata=extra_kv,
        **kwargs,
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


def _has_bbox_column(parquet_file: str) -> bool:
    """Return True if the file already has a bbox struct column."""
    from geoparquet_io.core.common import check_bbox_structure

    return check_bbox_structure(parquet_file, verbose=False)["has_bbox_column"]


def _ensure_bbox(parquet_file: str, verbose: bool) -> None:
    """Add bbox column if the file doesn't already have one."""
    from geoparquet_io.core.common import add_bbox, check_bbox_structure

    bbox_info = check_bbox_structure(parquet_file, verbose=False)
    if bbox_info["has_bbox_column"]:
        return

    progress("Adding bbox column (recommended for GeoParquet 1.x)...")
    add_bbox(parquet_file, verbose=verbose)
    success("Added bbox column")
