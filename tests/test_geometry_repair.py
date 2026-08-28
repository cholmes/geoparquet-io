"""Tests for the shared geometry-repair helper (issue #506)."""

import logging

from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.geometry_repair import (
    repair_arrow_table_geometry,
    repair_geometry_sql,
    repair_query_geometry,
)

# A self-intersecting "bowtie" polygon: invalid per OGC simple-feature rules.
BOWTIE_WKT = "POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))"
# A plain unit square: valid.
SQUARE_WKT = "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))"


def _con():
    return get_duckdb_connection()


def test_repair_geometry_sql_fixes_invalid_polygon():
    con = _con()
    expr = repair_geometry_sql(f"ST_GeomFromText('{BOWTIE_WKT}')")
    valid = con.execute(f"SELECT ST_IsValid({expr})").fetchone()[0]
    assert valid is True


def test_repair_geometry_sql_leaves_valid_geometry_unchanged():
    con = _con()
    # A valid geometry should pass through ST_IsValid -> identity branch byte-for-byte.
    expr = repair_geometry_sql(f"ST_GeomFromText('{SQUARE_WKT}')")
    same = con.execute(
        f"SELECT ST_AsWKB({expr}) = ST_AsWKB(ST_GeomFromText('{SQUARE_WKT}'))"
    ).fetchone()[0]
    assert same is True


def test_repair_geometry_sql_preserves_null():
    con = _con()
    expr = repair_geometry_sql("NULL::GEOMETRY")
    result = con.execute(f"SELECT {expr}").fetchone()[0]
    assert result is None


def _wkb_table(con, *wkts, geom_col="geometry", metadata=None):
    """Build a PyArrow table with an id column and a WKB geometry column."""
    rows = "\n UNION ALL ".join(
        f"SELECT {i} AS id, ST_AsWKB(ST_GeomFromText('{w}')) AS \"{geom_col}\""
        for i, w in enumerate(wkts)
    )
    table = con.execute(rows).arrow().read_all()
    if metadata:
        table = table.replace_schema_metadata(metadata)
    return table


def test_repair_arrow_table_geometry_repairs_and_counts(caplog):
    con = _con()
    table = _wkb_table(con, BOWTIE_WKT, BOWTIE_WKT, SQUARE_WKT)
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        repaired, n = repair_arrow_table_geometry(table, "geometry", repair=True)
    assert n == 2
    assert "Repaired 2 invalid geometries" in caplog.text
    assert repaired.schema.field("geometry").type == table.schema.field("geometry").type
    con.register("r", repaired)
    invalid = con.execute(
        "SELECT COUNT(*) FROM r WHERE NOT ST_IsValid(ST_GeomFromWKB(geometry))"
    ).fetchone()[0]
    assert invalid == 0
    assert repaired.num_rows == 3


def test_repair_arrow_table_geometry_opt_out_preserves(caplog):
    con = _con()
    table = _wkb_table(con, BOWTIE_WKT, SQUARE_WKT)
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        result, n = repair_arrow_table_geometry(table, "geometry", repair=False)
    assert n == 1
    assert "Left unrepaired 1 invalid geometry" in caplog.text
    # Opt-out returns the original object untouched.
    assert result is table


def test_repair_arrow_table_geometry_preserves_schema_metadata():
    con = _con()
    meta = {b"geo": b'{"version":"1.0.0"}', b"_server_crs": b"EPSG:4326"}
    table = _wkb_table(con, BOWTIE_WKT, SQUARE_WKT, metadata=meta)
    repaired, n = repair_arrow_table_geometry(table, "geometry", repair=True)
    assert n == 1
    assert repaired.schema.metadata == meta


def test_repair_arrow_table_geometry_noop_when_all_valid():
    con = _con()
    table = _wkb_table(con, SQUARE_WKT, SQUARE_WKT)
    result, n = repair_arrow_table_geometry(table, "geometry", repair=True)
    assert n == 0
    # Nothing invalid → original object returned unchanged (no roundtrip).
    assert result is table


def test_repair_arrow_table_geometry_empty_table_is_safe():
    con = _con()
    table = (
        con.execute(
            f"SELECT 1 AS id, ST_AsWKB(ST_GeomFromText('{SQUARE_WKT}')) AS geometry WHERE 1=0"
        )
        .arrow()
        .read_all()
    )
    result, n = repair_arrow_table_geometry(table, "geometry", repair=True)
    assert n == 0
    assert result is table


def test_repair_query_geometry_native_geometry_column(caplog):
    con = _con()
    base = (
        f"SELECT 1 AS id, ST_GeomFromText('{BOWTIE_WKT}') AS geom "
        f"UNION ALL SELECT 2, ST_GeomFromText('{SQUARE_WKT}')"
    )
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        wrapped = repair_query_geometry(con, base, "geom", repair=True)
    assert "Repaired 1 invalid geometry" in caplog.text
    invalid = con.execute(
        f"SELECT COUNT(*) FROM ({wrapped}) WHERE NOT ST_IsValid(geom)"
    ).fetchone()[0]
    assert invalid == 0


def test_repair_query_geometry_wkb_column():
    con = _con()
    base = (
        f"SELECT 1 AS id, ST_AsWKB(ST_GeomFromText('{BOWTIE_WKT}')) AS geom "
        f"UNION ALL SELECT 2, ST_AsWKB(ST_GeomFromText('{SQUARE_WKT}'))"
    )
    wrapped = repair_query_geometry(con, base, "geom", repair=True)
    # Output stays WKB-encoded; decode for the validity check.
    invalid = con.execute(
        f"SELECT COUNT(*) FROM ({wrapped}) WHERE NOT ST_IsValid(ST_GeomFromWKB(geom))"
    ).fetchone()[0]
    assert invalid == 0


def test_repair_query_geometry_opt_out_returns_original_query_and_warns(caplog):
    con = _con()
    base = f"SELECT ST_GeomFromText('{BOWTIE_WKT}') AS geom"
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        result = repair_query_geometry(con, base, "geom", repair=False)
    assert result == base
    assert "Left unrepaired 1 invalid geometry" in caplog.text
    assert "--no-repair-geometry" in caplog.text


def test_repair_query_geometry_non_spatial_column_is_passthrough():
    con = _con()
    base = "SELECT 1 AS id, 'x' AS geom"
    assert repair_query_geometry(con, base, "geom", repair=True) == base


def test_repair_arrow_table_geometry_tolerates_malformed_wkb():
    """Malformed/non-WKB bytes must not crash extraction (issue #506 robustness)."""
    import pyarrow as pa

    table = pa.table({"geometry": [b"test1", b"test2", b"test3"], "name": ["a", "b", "c"]})
    result, n = repair_arrow_table_geometry(table, "geometry", repair=True)
    # Unparsable bytes are reported as 0 invalid and passed through unchanged.
    assert n == 0
    assert result.num_rows == 3


def _wkb_table_with_nulls(con, *wkts, geom_col="geometry"):
    """Like _wkb_table, but WKT None entries become NULL geometry rows."""
    rows = "\n UNION ALL ".join(
        (
            f'SELECT {i} AS id, NULL::BLOB AS "{geom_col}"'
            if w is None
            else f"SELECT {i} AS id, ST_AsWKB(ST_GeomFromText('{w}')) AS \"{geom_col}\""
        )
        for i, w in enumerate(wkts)
    )
    return con.execute(rows).arrow().read_all()


def test_repair_arrow_table_geometry_with_null_rows(caplog):
    """NULL geometry rows must not crash the repair pass and must survive it.

    Issue #642: the previous single-WHERE invalid count segfaulted DuckDB's
    spatial extension on real-world tables containing NULL geometry rows
    (selection-vector misalignment under conditional execution). The layered
    count shape is logically identical and crash-free; NULL rows pass through.
    """
    con = _con()
    table = _wkb_table_with_nulls(con, SQUARE_WKT, None, BOWTIE_WKT, None, SQUARE_WKT)

    with caplog.at_level(logging.WARNING):
        repaired, n = repair_arrow_table_geometry(table, "geometry")

    assert n == 1  # only the bowtie; NULLs are neither invalid nor repaired
    assert repaired.num_rows == 5
    null_count = sum(1 for v in repaired.column("geometry").to_pylist() if v is None)
    assert null_count == 2


def test_repair_query_geometry_with_null_rows():
    """The query path's layered count also tolerates NULL geometry rows."""
    con = _con()
    query = (
        f"SELECT 1 AS id, ST_AsWKB(ST_GeomFromText('{BOWTIE_WKT}')) AS geometry"
        " UNION ALL SELECT 2, NULL::BLOB"
        f" UNION ALL SELECT 3, ST_AsWKB(ST_GeomFromText('{SQUARE_WKT}'))"
    )
    repaired_query = repair_query_geometry(con, query, "geometry")

    assert repaired_query != query  # the bowtie triggers a repair rewrite
    rows = con.execute(
        f"SELECT id, geometry IS NULL FROM ({repaired_query}) ORDER BY id"
    ).fetchall()
    assert [r[1] for r in rows] == [False, True, False]
    valid = con.execute(
        f"SELECT bool_and(ST_IsValid(TRY(ST_GeomFromWKB(geometry)))) "
        f"FROM ({repaired_query}) WHERE geometry IS NOT NULL"
    ).fetchone()[0]
    assert valid is True


def test_repair_arrow_table_geometry_null_rows_at_scale():
    """The #642 segfault only reproduces at scale — keep one big case.

    Review probing on #645 crashed the old single-WHERE count from ~8k rows
    with this exact recipe (NULL every 997 rows, invalid every 13), while the
    small fixtures above pass either form. Being memory corruption, the crash
    is environment-dependent (it does not reproduce on every machine), so this
    catches a regression where it manifests rather than everywhere — the best
    a test can do for an upstream crash — and pins the counts at scale
    regardless.
    """
    con = _con()
    n = 10_000
    table = (
        con.execute(
            f"""
            SELECT i AS id, CASE
                WHEN i % 997 = 0 THEN NULL
                WHEN i % 13 = 0 THEN ST_AsWKB(ST_GeomFromText('{BOWTIE_WKT}'))
                ELSE ST_AsWKB(ST_GeomFromText('{SQUARE_WKT}'))
            END AS geometry
            FROM range({n}) t(i)
            """
        )
        .arrow()
        .read_all()
    )
    expected_nulls = sum(1 for i in range(n) if i % 997 == 0)
    expected_invalid = sum(1 for i in range(n) if i % 13 == 0 and i % 997 != 0)

    repaired, count = repair_arrow_table_geometry(table, "geometry")

    assert repaired.num_rows == n
    assert count == expected_invalid
    nulls = sum(1 for v in repaired.column("geometry").to_pylist() if v is None)
    assert nulls == expected_nulls


def _chunked_table(con, *wkts, chunks: int = 8, geom_col="geometry"):
    """Build a table whose columns are split across ``chunks`` Arrow chunks."""
    import pyarrow as pa

    single = _wkb_table(con, *wkts, geom_col=geom_col)
    batches = single.to_batches(max_chunksize=max(1, single.num_rows // chunks))
    table = pa.Table.from_batches(batches, schema=single.schema)
    assert table.column(geom_col).num_chunks > 1, "fixture must be chunked"
    return table


def test_repair_arrow_table_geometry_combines_chunks_before_registering():
    """#737: ST_IsValid over a chunked registered table segfaults DuckDB spatial.

    ``pq.read_table()`` returns many chunks for any sizable file (280 for the
    559k-row layer in the report), and the crash is a SIGSEGV inside the *count*
    query -- before any repair runs, and past the reach of the ``except
    Exception`` guard, so the whole process dies with the downloaded data
    already removed by the ``finally`` block.

    Being memory corruption it is non-deterministic at the margin and needs real
    geometry to reproduce, so a test cannot provoke it reliably -- the same
    limitation as the #642 case below. Assert the invariant that avoids it
    instead: whatever gets registered is a single chunk per column.
    """
    from unittest import mock

    con = _con()
    table = _chunked_table(con, *([BOWTIE_WKT, SQUARE_WKT] * 32))
    registered = []

    class _RecordingConnection:
        """Delegates to a real connection, recording what gets registered."""

        def __init__(self, inner):
            self._inner = inner

        def register(self, name, value):
            registered.append(value)
            return self._inner.register(name, value)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    with mock.patch(
        "geoparquet_io.core.duckdb_utils.get_duckdb_connection",
        side_effect=lambda **kw: _RecordingConnection(get_duckdb_connection(**kw)),
    ):
        repaired, n = repair_arrow_table_geometry(table, "geometry", repair=True)

    assert registered, "the helper must register the table it queries"
    for column in registered[0].columns:
        assert column.num_chunks <= 1, (
            f"registered a {column.num_chunks}-chunk column; "
            "ST_IsValid over chunked Arrow input segfaults DuckDB spatial (#737)"
        )
    assert n == 32
    assert repaired.num_rows == table.num_rows


def test_repair_arrow_table_geometry_repairs_a_chunked_table():
    """Chunked input must come back correct, not merely uncrashed."""
    con = _con()
    table = _chunked_table(con, *([BOWTIE_WKT, SQUARE_WKT] * 32))

    repaired, n = repair_arrow_table_geometry(table, "geometry", repair=True)

    assert n == 32
    assert repaired.num_rows == 64
    con.register("_repaired", repaired.combine_chunks())
    remaining = con.execute(
        "SELECT COUNT(*) FROM _repaired WHERE NOT ST_IsValid(ST_GeomFromWKB(geometry))"
    ).fetchone()[0]
    assert remaining == 0


def test_repair_arrow_table_geometry_chunked_preserves_schema_metadata():
    con = _con()
    table = _chunked_table(con, *([BOWTIE_WKT, SQUARE_WKT] * 32))
    table = table.replace_schema_metadata({b"geo": b'{"version": "1.1.0"}'})

    repaired, _ = repair_arrow_table_geometry(table, "geometry", repair=True)

    assert repaired.schema.metadata == {b"geo": b'{"version": "1.1.0"}'}


def test_extract_table_combines_chunks_before_registering():
    """#737 again: extract's table path runs the same ST_IsValid over its input.

    `extract_table` registers the caller's Arrow table and then hands the
    query to `repair_query_geometry`, so a chunked table reaches the same crash
    through `gpio.read(...).extract(...)` and the streaming path.
    """
    from unittest import mock

    from geoparquet_io.core.extract import extract_table

    con = _con()
    table = _chunked_table(con, *([BOWTIE_WKT, SQUARE_WKT] * 32))
    registered = []

    real_register = type(con).register

    def _spy(self, name, value):
        registered.append((name, value))
        return real_register(self, name, value)

    with mock.patch.object(type(con), "register", _spy):
        result = extract_table(table)

    assert result.num_rows == 64
    inputs = [value for name, value in registered if name == "__input_table"]
    assert inputs, "extract_table must register its input table"
    for column in inputs[0].columns:
        assert column.num_chunks <= 1, (
            f"registered a {column.num_chunks}-chunk column; "
            "ST_IsValid over chunked Arrow input segfaults DuckDB spatial (#737)"
        )
