"""Invalid-geometry repair helpers (issue #506).

geoparquet-io repairs invalid geometry by default using DuckDB's ``ST_MakeValid``.
``ST_IsValid``/``ST_MakeValid`` require a *native* ``GEOMETRY`` and reject WKB/BLOB,
so every repair must be applied while the geometry column is still a native
``GEOMETRY`` — i.e. *before* any ``ST_AsWKB(...)`` conversion in a pipeline.

This module provides three injection styles:

* :func:`repair_geometry_sql` — wrap an inline geometry *expression* (a column
  reference or a ``ST_GeomFrom*`` call) with a CASE-guarded repair. Used where a
  query is built around a known native ``GEOMETRY`` expression (e.g. WFS feature
  extraction, ``gpio convert geojson``).
* :func:`repair_query_geometry` — count, warn, and repair the geometry column of
  an arbitrary query, auto-detecting native ``GEOMETRY`` vs WKB encoding. Used by
  the SQL/COPY paths (``gpio extract geoparquet``, the convert pipeline).
* :func:`repair_arrow_table_geometry` — count, warn, and repair a WKB-encoded
  PyArrow table's geometry column. Used by extractors that assemble a table in
  memory (WFS, ArcGIS, BigQuery, Carto).
"""

from geoparquet_io.core.duckdb_utils import quote_identifier
from geoparquet_io.core.logging_config import warn


def _warn_invalid(n: int, *, repaired: bool) -> None:
    """Emit a single warning describing invalid geometries found (no-op when n == 0)."""
    if n <= 0:
        return
    verb = "Repaired" if repaired else "Left unrepaired"
    noun = "geometry" if n == 1 else "geometries"
    suffix = "" if repaired else " (--no-repair-geometry)"
    warn(f"{verb} {n} invalid {noun}{suffix}")


def repair_geometry_sql(geom_expr: str) -> str:
    """Return a CASE-guarded ST_MakeValid wrap for a native GEOMETRY expression.

    Only invalid, non-NULL geometry is passed through ``ST_MakeValid``; valid
    geometry is returned unchanged (byte-identical) and NULL is preserved.

    Args:
        geom_expr: A SQL expression yielding a native ``GEOMETRY`` — a quoted
            column reference (e.g. ``"geom"``) or a constructor such as
            ``ST_GeomFromGeoJSON(feature.geometry)``.

    Returns:
        A SQL expression yielding a repaired native ``GEOMETRY``.
    """
    return (
        f"CASE WHEN {geom_expr} IS NULL OR ST_IsValid({geom_expr}) "
        f"THEN {geom_expr} ELSE ST_MakeValid({geom_expr}) END"
    )


def _layered_invalid_count_sql(source_sql: str, parsed_expr: str) -> str:
    """COUNT of invalid geometries, in a shape DuckDB executes without crashing.

    Filtering ``IS NOT NULL`` and evaluating ``ST_IsValid`` over the raw rows in
    one WHERE clause segfaults DuckDB 1.5.x's spatial extension when the column
    contains NULLs: DuckDB 1.5.1's TRY() applies the selection vector twice
    under conditional execution, reading uninitialized vector memory (fixed in
    DuckDB 1.5.2, see duckdb/duckdb-spatial#858 — we are pinned below it for the
    'geography' extension). Verified on
    issue #642's reproduction: projecting the parsed geometry first, filtering
    NULLs in a middle layer, and running ``ST_IsValid`` outermost is logically
    identical and crash-free.
    """
    return (
        f"SELECT COUNT(*) FROM ("
        f"SELECT __g FROM (SELECT {parsed_expr} AS __g FROM {source_sql}) "
        f"WHERE __g IS NOT NULL"
        f") WHERE NOT ST_IsValid(__g)"
    )


def repair_query_geometry(con, query: str, geometry_column: str, *, repair: bool = True) -> str:
    """Count and optionally repair invalid geometry in an arbitrary query.

    Auto-detects whether the geometry column in ``query`` is a native
    ``GEOMETRY`` or WKB-encoded binary, decoding/re-encoding as needed so the
    output column keeps the same encoding as the input. Emits a single warning
    with the invalid count. Used by paths that emit SQL rather than a PyArrow
    table (``gpio extract geoparquet``, ``gpio convert geojson``).

    The geometry column is left untouched (and the original query returned) when
    its type is neither ``GEOMETRY`` nor binary (e.g. a GeoArrow STRUCT), when no
    invalid geometry is found, or when ``repair=False``.

    Args:
        con: An open DuckDB connection with the spatial extension loaded.
        query: SQL query exposing ``geometry_column``.
        geometry_column: Name of the geometry column.
        repair: Whether to repair (True) or only count and warn (False).

    Returns:
        The original query, or a query that repairs the geometry column in place.
    """
    col = quote_identifier(geometry_column)
    try:
        desc = con.execute(f"DESCRIBE ({query})").fetchall()
    except Exception:
        return query
    col_type = next((d[1].upper() for d in desc if d[0] == geometry_column), None)
    if col_type is None:
        return query

    if "GEOMETRY" in col_type:
        # Native GEOMETRY: no decode needed; it cannot be a malformed-bytes case.
        parsed = col
        repaired_expr = repair_geometry_sql(parsed)
    elif "BLOB" in col_type or "BINARY" in col_type:
        # WKB-encoded: decode defensively. TRY() yields NULL on malformed bytes so
        # we never crash the pipeline; such rows are passed through unchanged.
        parsed = f"TRY(ST_GeomFromWKB({col}))"
        # AND-form (repair in THEN, passthrough in ELSE). The equivalent OR-form
        # with ST_MakeValid in the ELSE branch segfaults DuckDB 1.5.1's spatial
        # extension on some real WKB inputs (see repair_arrow_table_geometry).
        repaired_expr = (
            f"CASE WHEN {parsed} IS NOT NULL AND NOT ST_IsValid({parsed}) "
            f"THEN ST_AsWKB(ST_MakeValid({parsed})) ELSE {col} END"
        )
    else:
        # GeoArrow STRUCT or other encoding we don't repair in place.
        return query

    try:
        n = con.execute(_layered_invalid_count_sql(f"({query})", parsed)).fetchone()[0]
    except Exception:
        # Defensive: never let geometry repair break extraction.
        return query
    _warn_invalid(n, repaired=repair)
    if not repair or n == 0:
        return query
    return f"SELECT * REPLACE ({repaired_expr} AS {col}) FROM ({query})"


def repair_arrow_table_geometry(table, geometry_column: str = "geometry", *, repair: bool = True):
    """Count and (optionally) repair invalid geometry in a WKB-encoded Arrow table.

    Used by extraction paths (WFS, ArcGIS, BigQuery, Carto) that assemble a
    PyArrow table whose geometry column is WKB binary. ``ST_IsValid`` and
    ``ST_MakeValid`` require a native ``GEOMETRY``, so the WKB is decoded via
    ``ST_GeomFromWKB`` for the check/repair and re-encoded with ``ST_AsWKB``.

    A single warning with the exact invalid count is emitted when any invalid
    geometry is found (phrasing depends on ``repair``).

    Rebuilding through DuckDB drops Arrow *schema* metadata, so the original
    table-level schema metadata (e.g. the GeoParquet ``geo`` block or a CRS
    marker) is re-applied to the repaired table — the geometry encoding is
    unchanged (WKB in, WKB out), so the metadata stays valid. The original
    table object is returned unchanged when there is nothing to repair (empty,
    no geometry column, no invalid rows, or ``repair=False``).

    Args:
        table: PyArrow table with a WKB-encoded geometry column.
        geometry_column: Name of the WKB geometry column.
        repair: Whether to repair (True) or only count and warn (False).

    Returns:
        Tuple ``(table, n_invalid)``.
    """
    if table.num_rows == 0 or geometry_column not in table.column_names:
        return table, 0

    from geoparquet_io.core.duckdb_utils import (
        get_duckdb_connection,
        register_arrow_table_for_spatial,
    )

    col = quote_identifier(geometry_column)
    # Decode WKB defensively: TRY() yields NULL on malformed bytes so a bad row
    # never crashes extraction — such rows pass through unchanged.
    parsed = f"TRY(ST_GeomFromWKB({col}))"
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        # Combines chunks first: the ST_IsValid count below segfaults DuckDB
        # spatial on a chunked registered table (#737). The original `table` is
        # what we hand back when there is nothing to repair, so the combined
        # copy is released on return.
        register_arrow_table_for_spatial(con, "_gpio_repair_src", table)
        # Layered count: the single-WHERE form segfaults on tables containing
        # NULL geometry rows (issue #642) — see _layered_invalid_count_sql.
        n = con.execute(_layered_invalid_count_sql("_gpio_repair_src", parsed)).fetchone()[0]
        _warn_invalid(n, repaired=repair)
        if not repair or n == 0:
            return table, n
        # AND-form (repair in THEN, passthrough in ELSE). The equivalent
        # OR-form (`col IS NULL OR parsed IS NULL OR ST_IsValid(parsed)` with
        # ST_MakeValid in the ELSE branch) segfaults DuckDB 1.5.1's spatial
        # extension on some real WKB inputs — a conditional-execution bug where
        # the raw-blob `IS NULL` term mis-aligns the selection vector fed to
        # ST_MakeValid. This form is logically identical and crash-free.
        repair_expr = (
            f"CASE WHEN {parsed} IS NOT NULL AND NOT ST_IsValid({parsed}) "
            f"THEN ST_AsWKB(ST_MakeValid({parsed})) ELSE {col} END"
        )
        repaired = (
            con.execute(f"SELECT * REPLACE ({repair_expr} AS {col}) FROM _gpio_repair_src")
            .arrow()
            .read_all()
        )
        # The DuckDB roundtrip drops Arrow schema metadata; restore it so any
        # GeoParquet/CRS metadata the caller already attached survives.
        if table.schema.metadata:
            repaired = repaired.replace_schema_metadata(dict(table.schema.metadata))
        return repaired, n
    except Exception:
        # Defensive: never let geometry repair break an extraction pipeline.
        return table, 0
    finally:
        try:
            con.unregister("_gpio_repair_src")
        except Exception:
            pass
        con.close()
