"""Reduce geometry coordinate precision by snapping to a fixed grid.

Wraps DuckDB's ``ST_ReducePrecision`` in an ordered pipeline:

1. **Repair** — guarded ``ST_MakeValid`` *before* reducing (default on).
   ``ST_ReducePrecision`` raises ``TopologyException`` on invalid input, so the
   repair must precede it; ``--no-repair-geometry`` instead guards the reduce
   with ``TRY`` so invalid input degrades to NULL rather than aborting the file.
2. **Reduce** — snap coordinates to ``grid`` (CRS units). On valid input GEOS
   preserves validity, so no second repair is needed.
3. **Drop empty** — ``ST_ReducePrecision`` routinely collapses slivers to
   ``POLYGON EMPTY`` / ``LINESTRING EMPTY``; drop ``NULL``/empty geometry by
   default so downstream spatial queries and row-group pruning stay correct.

A stored bbox covering column is regenerated from the reduced geometry so it
never goes stale (a stale bbox would mis-prune). The native CRS is preserved —
precision reduction never reprojects.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier
from geoparquet_io.core.file_utils import handle_output_overwrite
from geoparquet_io.core.geometry_detection import STANDARD_GEOMETRY_NAMES
from geoparquet_io.core.geometry_repair import repair_geometry_sql
from geoparquet_io.core.logging_config import configure_verbose, success, warn
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.remote import setup_aws_profile_if_needed, validate_profile_for_urls
from geoparquet_io.core.stream_io import execute_transform
from geoparquet_io.core.streaming import (
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)

_BBOX_FIELDS = ("xmin", "ymin", "xmax", "ymax")


def _describe(con, source: str) -> list[tuple]:
    """Return ``(name, type, ...)`` rows describing the columns of ``source``."""
    rows: list[tuple] = con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    return rows


def _detect_geometry_column(con, source: str) -> str:
    """Pick the geometry column of ``source`` by standard name, defaulting to 'geometry'."""
    names = [row[0] for row in _describe(con, source)]
    return next((n for n in STANDARD_GEOMETRY_NAMES if n in names), "geometry")


def _geometry_is_blob(con, source: str, geom_col: str) -> bool:
    """True when ``geom_col`` is WKB binary (needs decoding) rather than native GEOMETRY."""
    for name, col_type, *_ in _describe(con, source):
        if name == geom_col:
            upper = col_type.upper()
            return "BLOB" in upper or "BINARY" in upper
    return False


def _is_bbox_struct(col_type: str) -> bool:
    """True when a column type is a struct carrying xmin/ymin/xmax/ymax fields."""
    lowered = col_type.lower()
    return lowered.startswith("struct") and all(f in lowered for f in _BBOX_FIELDS)


def _find_bbox_column(con, source: str, geom_col: str) -> str | None:
    """Return the name of a bbox covering struct column (xmin/ymin/xmax/ymax), if any."""
    return next(
        (n for n, t, *_ in _describe(con, source) if n != geom_col and _is_bbox_struct(t)),
        None,
    )


def _bbox_struct_expr(geom_q: str) -> str:
    """STRUCT_PACK expression recomputing a bbox covering from ``geom_q``."""
    return (
        f"STRUCT_PACK(xmin := ST_XMin({geom_q}), ymin := ST_YMin({geom_q}), "
        f"xmax := ST_XMax({geom_q}), ymax := ST_YMax({geom_q}))"
    )


def _reduce_ctes(source: str, geom_q: str, grid: float, *, is_blob: bool, repair: bool):
    """Build the decode -> (repair) -> reduce CTE chain; return ``(ctes, last_name)``.

    Repair runs BEFORE reduce because ``ST_ReducePrecision`` aborts on invalid
    input; with ``repair`` off the reduce is wrapped in ``TRY`` so invalid input
    degrades to NULL instead of aborting the whole file.
    """
    decoded = f"ST_GeomFromWKB({geom_q})" if is_blob else geom_q
    ctes = [f"__rp_in AS (SELECT * REPLACE ({decoded} AS {geom_q}) FROM {source})"]
    last = "__rp_in"
    if repair:
        ctes.append(
            f"__rp_valid AS (SELECT * REPLACE ({repair_geometry_sql(geom_q)} AS {geom_q}) FROM {last})"
        )
        last = "__rp_valid"
    reduce_expr = f"ST_ReducePrecision({geom_q}, CAST({grid} AS DOUBLE))"
    if not repair:
        reduce_expr = f"TRY({reduce_expr})"
    ctes.append(f"__rp_reduced AS (SELECT * REPLACE ({reduce_expr} AS {geom_q}) FROM {last})")
    return ctes, "__rp_reduced"


def _build_reduce_query(
    source: str,
    geom_col: str,
    grid: float,
    *,
    is_blob: bool,
    repair: bool,
    drop_empty: bool,
    bbox_col: str | None,
    as_wkb: bool,
) -> str:
    """Build the repair -> reduce -> drop-empty SQL over ``source``.

    Geometry stays native ``GEOMETRY`` through the CTEs (so ``ST_IsEmpty`` and the
    bbox recompute work); the final projection emits WKB when ``as_wkb`` is set
    (direct Arrow export) or leaves it native (the writer/stream layer re-encodes).
    """
    geom_q = quote_identifier(geom_col)
    ctes, last = _reduce_ctes(source, geom_q, grid, is_blob=is_blob, repair=repair)

    final_geom = f"ST_AsWKB({geom_q})" if as_wkb else geom_q
    replacements = [f"{final_geom} AS {geom_q}"]
    if bbox_col:
        replacements.append(f"{_bbox_struct_expr(geom_q)} AS {quote_identifier(bbox_col)}")

    where = f" WHERE NOT ({geom_q} IS NULL OR ST_IsEmpty({geom_q}))" if drop_empty else ""
    return f"WITH {', '.join(ctes)} SELECT * REPLACE ({', '.join(replacements)}) FROM {last}{where}"


def _warn_dropped(n: int) -> None:
    """Warn that ``n`` null/empty geometries were dropped (no-op when ``n <= 0``)."""
    if n <= 0:
        return
    noun = "geometry" if n == 1 else "geometries"
    warn(f"Dropped {n} null/empty {noun} after precision reduction (use --keep-empty to retain)")


def reduce_precision_table(
    table: pa.Table,
    grid: float,
    geometry_column: str | None = None,
    repair: bool = True,
    drop_empty: bool = True,
) -> pa.Table:
    """Reduce coordinate precision of an Arrow table (Python API core).

    Args:
        table: Input table with a WKB or native geometry column.
        grid: Grid size in the geometry's CRS units (e.g. ``1e-6`` ≈ 0.11 m on EPSG:4326).
        geometry_column: Geometry column name (auto-detected if None).
        repair: Run guarded ``ST_MakeValid`` before reducing (default True).
        drop_empty: Drop geometries that became NULL/empty (default True).

    Returns:
        New table with reduced, WKB-encoded geometry (and regenerated bbox if present).
    """
    geom_col = geometry_column or find_geometry_column_from_table(table) or "geometry"
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.register("__input_table", table)
        query = _build_reduce_query(
            "__input_table",
            geom_col,
            grid,
            is_blob=_geometry_is_blob(con, "__input_table", geom_col),
            repair=repair,
            drop_empty=drop_empty,
            bbox_col=_find_bbox_column(con, "__input_table", geom_col),
            as_wkb=True,
        )
        result = con.execute(query).arrow().read_all()
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)
        if drop_empty:
            _warn_dropped(table.num_rows - result.num_rows)
        return result
    finally:
        con.close()


def _count_dropped(
    con, source: str, geom_col: str, grid: float, repair: bool, is_blob: bool
) -> int:
    """Count geometries that become NULL/empty after reduce(+repair)."""
    geom_q = quote_identifier(geom_col)
    reduced = _build_reduce_query(
        source,
        geom_col,
        grid,
        is_blob=is_blob,
        repair=repair,
        drop_empty=False,
        bbox_col=None,
        as_wkb=False,
    )
    return int(
        con.execute(
            f"SELECT COUNT(*) FROM ({reduced}) WHERE {geom_q} IS NULL OR ST_IsEmpty({geom_q})"
        ).fetchone()[0]
    )


def _resolve_output(input_parquet: str, output_parquet: str | None) -> str | None:
    """Auto-name a non-streaming output as ``<stem>_reduced.parquet`` when unspecified."""
    if output_parquet is not None or should_stream_output(output_parquet):
        return output_parquet
    src = Path(input_parquet)
    return str(src.parent / f"{src.stem}_reduced.parquet")


def reduce_precision(
    input_parquet: str,
    output_parquet: str | None = None,
    *,
    grid: float,
    repair: bool = True,
    drop_empty: bool = True,
    dry_run: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
) -> None:
    """Reduce coordinate precision of a GeoParquet file.

    Supports Arrow IPC streaming: input ``"-"`` reads stdin; output ``"-"`` or a
    piped stdout streams. See :func:`reduce_precision_table` for the pipeline.
    """
    configure_verbose(verbose)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if not is_streaming:
        require_single_file(input_parquet, "reduce-precision")
        output_parquet = _resolve_output(input_parquet, output_parquet)
        handle_output_overwrite(output_parquet, overwrite, input_parquet)
        validate_profile_for_urls(profile, input_parquet, output_parquet)
        setup_aws_profile_if_needed(profile, input_parquet, output_parquet)

    def make_query(source, con) -> str:
        geom_col = _detect_geometry_column(con, source)
        is_blob = _geometry_is_blob(con, source, geom_col)
        if drop_empty and not dry_run:
            _warn_dropped(_count_dropped(con, source, geom_col, grid, repair, is_blob))
        return _build_reduce_query(
            source,
            geom_col,
            grid,
            is_blob=is_blob,
            repair=repair,
            drop_empty=drop_empty,
            bbox_col=_find_bbox_column(con, source, geom_col),
            as_wkb=False,
        )

    execute_transform(
        input_parquet,
        output_parquet,
        make_query,
        verbose=verbose,
        dry_run=dry_run,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        profile=profile,
        geoparquet_version=geoparquet_version,
    )

    if not dry_run and not should_stream_output(output_parquet):
        success(f"Reduced precision to grid {grid}: {output_parquet}")
