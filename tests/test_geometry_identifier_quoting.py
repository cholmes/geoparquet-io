"""Regression tests: quote geometry-column identifiers at raw SQL interpolation.

Several SQL builders interpolate a geometry/column identifier raw into a query,
so any file whose geometry column has a space, uppercase, reserved word, or
quote breaks with a ParserException -- and because the name can come from a
file's own `geo.primary_column` metadata, a crafted parquet is an injection
vector (see tests/test_admin_sql_injection.py for the sibling todo-008 guard
that already covers admin-divisions / country-codes).

This file guards the four confirmed sites plus siblings found during audit:

1. ``stream_io._wrap_query_with_wkb_conversion`` -- interpolates the geometry
   column as a SQL IDENTIFIER twice (inside ``ST_AsWKB(...)`` and as the
   ``REPLACE`` target). Fix: ``quote_identifier``.
2. ``common.add_bbox``'s ``STRUCT_PACK`` expression -- same identifier bug.
   Fix: ``quote_identifier``.
   Sibling found during audit: ``core/add/bbox.py``'s
   ``_add_bbox_file_based`` builds an almost-identical unquoted STRUCT_PACK
   expression, and it is the function actually exercised by the real
   ``gpio add bbox`` CLI command -- this is the one a user hits in practice.
3. ``check_spatial_order._calculate_consecutive_avg`` /
   ``_calculate_random_avg`` -- interpolate the geometry column as an
   identifier in the sampling-method queries. Fix: ``quote_identifier``.
4. ``duckdb_metadata``'s ``WHERE path_in_schema = '{geometry_column}'``
   filters -- this is a SQL STRING LITERAL comparison, not an identifier, so
   the correct fix is ``_escape_sql_string`` (doubling embedded ``'``), never
   ``quote_identifier``. A spaced name is harmless here (space is valid
   inside a string literal); only an embedded ``'`` breaks it -- currently
   silently, because the callers wrap the query in a broad ``except
   Exception`` and return ``None``/``{}``/``[]`` instead of raising.
   Siblings found during audit: ``get_bbox_from_row_group_stats`` and
   ``get_per_row_group_bbox_stats`` (bbox_column) and
   ``get_compression_info`` (column_name) build the same kind of
   ``path_in_schema = '...'`` string-literal filter.
"""

from __future__ import annotations

import json

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add, check
from geoparquet_io.core import common as common_module
from geoparquet_io.core.check_spatial_order import check_spatial_order
from geoparquet_io.core.duckdb_metadata import (
    get_aggregated_native_geo_stats,
    get_compression_info,
    get_native_geo_statistics,
    get_per_row_group_bbox_stats,
    get_per_row_group_native_geo_stats,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier
from geoparquet_io.core.stream_io import _wrap_query_with_wkb_conversion

# A geometry column name with a space -- breaks unquoted identifier
# interpolation (`ST_XMin(geom col)` is a parse error).
SPACED_COL = "geom col"

# A geometry column name with an embedded double-quote -- breaks
# `quote_identifier` if it were ever applied naively without doubling, and
# breaks raw identifier interpolation outright.
DQUOTE_COL = 'geo"m'

# A geometry column name with an embedded single-quote -- breaks a raw SQL
# STRING LITERAL comparison like `path_in_schema = '{col}'`.
SQUOTE_COL = "geo'm"


def _wkb_fixture(tmp_path, column_name: str, filename: str, wkts: list[str]) -> str:
    """Build a plain-WKB GeoParquet file whose geo.primary_column = column_name."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    quoted = quote_identifier(column_name)
    values_sql = ", ".join(
        f"({i + 1}, ST_AsWKB(ST_GeomFromText('{w}')))" for i, w in enumerate(wkts)
    )
    table = (
        con.execute(f"SELECT * FROM (VALUES {values_sql}) AS t(id, {quoted})").arrow().read_all()
    )
    con.close()

    geo = {
        "version": "1.0.0",
        "primary_column": column_name,
        "columns": {column_name: {"encoding": "WKB", "geometry_types": ["Polygon"]}},
    }
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    path = str(tmp_path / filename)
    pq.write_table(table, path)
    return path


def _native_geo_fixture(tmp_path, column_name: str, filename: str) -> str:
    """Build a parquet-geo-only file (native GEOMETRY, geo_bbox row-group stats)."""
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    con.execute("SET geometry_always_xy = true;")
    quoted = quote_identifier(column_name)
    query = (
        "SELECT * FROM (VALUES "
        "(1, ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))')), "
        "(2, ST_GeomFromText('POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))'))"
        f") AS t(id, {quoted})"
    )
    path = str(tmp_path / filename)
    common_module.write_parquet_with_metadata(
        con, query, path, original_metadata=None, geoparquet_version="parquet-geo-only"
    )
    con.close()
    return path


# --- Site 1: stream_io._wrap_query_with_wkb_conversion ----------------------


class TestStreamIoWkbConversionQuoting:
    """geoparquet_io/core/stream_io.py:190-194"""

    @pytest.mark.parametrize("column_name", [SPACED_COL, DQUOTE_COL])
    def test_wraps_and_converts_correctly(self, column_name):
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            quoted = quote_identifier(column_name)
            con.execute(
                f"CREATE TABLE t AS SELECT 1 AS id, ST_GeomFromText('POINT (1 2)') AS {quoted}"
            )

            wrapped = _wrap_query_with_wkb_conversion("SELECT * FROM t", column_name)
            row = con.execute(wrapped).fetchone()

            assert row[0] == 1
            wkt = con.execute("SELECT ST_AsText(ST_GeomFromWKB(?))", [row[1]]).fetchone()[0]
            assert wkt == "POINT (1 2)"
        finally:
            con.close()

    def test_noop_when_no_geometry_column(self):
        # Behavior for already-valid input (None) must not change.
        assert _wrap_query_with_wkb_conversion("SELECT 1", None) == "SELECT 1"


# --- Site 2: common.add_bbox's STRUCT_PACK expression ------------------------
# Plus sibling: core/add/bbox.py's _add_bbox_file_based (the real `gpio add
# bbox` CLI code path).


class TestCommonAddBboxQuoting:
    """geoparquet_io/core/common.py:3263-3268"""

    @pytest.mark.parametrize("column_name", [SPACED_COL, DQUOTE_COL])
    def test_add_bbox_succeeds_with_correct_values(self, tmp_path, column_name):
        path = _wkb_fixture(
            tmp_path,
            column_name,
            "add_bbox_fixture.parquet",
            wkts=[
                "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))",
            ],
        )

        common_module.add_bbox(path, bbox_column_name="bbox", verbose=False)

        table = pq.read_table(path)
        rows = table.column("bbox").combine_chunks().to_pylist()
        assert rows[0] == {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
        assert rows[1] == {"xmin": 2.0, "ymin": 2.0, "xmax": 3.0, "ymax": 3.0}


class TestCliAddBboxOnSpacedColumn:
    """End-to-end: `gpio add bbox` must not crash on a real `geom col` file.

    Exercises core/add/bbox.py's _add_bbox_file_based -> common.py's
    add_computed_column -> the duckdb-kv write strategy, i.e. the actual code
    path a user hits (found as a sibling of confirmed site 2 during audit).
    """

    @pytest.mark.parametrize("column_name", [SPACED_COL, DQUOTE_COL])
    def test_cli_add_bbox_succeeds(self, tmp_path, column_name):
        input_path = _wkb_fixture(
            tmp_path,
            column_name,
            "cli_add_bbox_input.parquet",
            wkts=[
                "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))",
            ],
        )
        output_path = str(tmp_path / "cli_add_bbox_output.parquet")

        runner = CliRunner()
        result = runner.invoke(add, ["bbox", input_path, output_path])

        assert result.exit_code == 0, result.output
        table = pq.read_table(output_path)
        rows = table.column("bbox").combine_chunks().to_pylist()
        assert rows[0] == {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
        assert rows[1] == {"xmin": 2.0, "ymin": 2.0, "xmax": 3.0, "ymax": 3.0}


# --- Site 3: check_spatial_order sampling-path queries ----------------------


class TestCheckSpatialOrderSamplingQuoting:
    """geoparquet_io/core/check_spatial_order.py:46,64"""

    def test_sampling_method_succeeds_on_spaced_column(self, tmp_path):
        wkts = [f"POINT ({i} {i})" for i in range(10)]
        path = _wkb_fixture(tmp_path, SPACED_COL, "spatial_order_fixture.parquet", wkts)

        result = check_spatial_order(
            path, random_sample_size=5, limit_rows=1000, verbose=False, return_results=True
        )

        assert result["method"] == "sampling"
        assert result["consecutive_avg"] is not None
        assert result["random_avg"] is not None
        # Points march evenly along the diagonal, so consecutive neighbours are
        # always closer than a random pair -> the ratio is safely under 1.
        assert result["ratio"] < 1.0

    def test_cli_check_spatial_succeeds_on_spaced_column(self, tmp_path):
        wkts = [f"POINT ({i} {i})" for i in range(10)]
        path = _wkb_fixture(tmp_path, SPACED_COL, "spatial_order_cli_fixture.parquet", wkts)

        runner = CliRunner()
        result = runner.invoke(check, ["spatial", path])

        assert result.exit_code == 0, result.output


# --- Site 4: duckdb_metadata `path_in_schema = '{...}'` literal filters ----


class TestDuckdbMetadataNativeStatsEscaping:
    """geoparquet_io/core/duckdb_metadata.py:1133,1200,1216,1266

    These are STRING LITERAL comparisons, not identifiers -- a spaced name is
    fine (space is a legal string-literal character); an embedded single
    quote is what breaks the query, so the fix is `_escape_sql_string`, not
    `quote_identifier`.
    """

    @pytest.mark.parametrize("column_name", [SPACED_COL, SQUOTE_COL])
    def test_native_geo_stat_getters_return_correct_values(self, tmp_path, column_name):
        path = _native_geo_fixture(tmp_path, column_name, "native_geo.parquet")

        single = get_native_geo_statistics(path, column_name)
        assert single is not None, "expected stats, got None (query silently failed)"
        assert single["bbox"][:4] == pytest.approx([0.0, 0.0, 3.0, 3.0])
        assert single["geometry_types"] == ["Polygon"]

        agg = get_aggregated_native_geo_stats(path, column_name)
        assert agg.get("bbox") is not None, "expected aggregated bbox, got empty dict"
        assert agg["bbox"][:4] == pytest.approx([0.0, 0.0, 3.0, 3.0])
        assert agg["geometry_types"] == ["Polygon"]

        per_rg = get_per_row_group_native_geo_stats(path, column_name)
        assert len(per_rg) == 1, "expected one row group of stats, got empty list"
        assert per_rg[0]["xmin"] == pytest.approx(0.0)
        assert per_rg[0]["ymin"] == pytest.approx(0.0)
        assert per_rg[0]["xmax"] == pytest.approx(3.0)
        assert per_rg[0]["ymax"] == pytest.approx(3.0)


class TestDuckdbMetadataSiblingLiteralEscaping:
    """Siblings found during audit: other `path_in_schema = '...'` filters.

    get_bbox_from_row_group_stats / get_per_row_group_bbox_stats key off the
    bbox column name, and get_compression_info keys off an arbitrary column
    name -- both build the identical string-literal comparison pattern as
    the four confirmed sites.
    """

    def test_per_row_group_bbox_stats_escapes_quote_in_bbox_column_name(self, tmp_path):
        bbox_col = "bb'ox"
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        quoted = quote_identifier(bbox_col)
        query = f"""
            SELECT * FROM (VALUES
                (1, {{'xmin': 0.0, 'ymin': 0.0, 'xmax': 1.0, 'ymax': 1.0}}),
                (2, {{'xmin': 2.0, 'ymin': 2.0, 'xmax': 3.0, 'ymax': 3.0}})
            ) AS t(id, {quoted})
        """
        table = con.execute(query).arrow().read_all()
        con.close()
        path = str(tmp_path / "bbox_col_quote.parquet")
        pq.write_table(table, path)

        result = get_per_row_group_bbox_stats(path, bbox_column=bbox_col)

        assert len(result) == 1
        assert result[0]["xmin"] == pytest.approx(0.0)
        assert result[0]["xmax"] == pytest.approx(3.0)

    def test_get_compression_info_escapes_quote_in_column_name(self, tmp_path):
        col = "va'l"
        con = duckdb.connect()
        quoted = quote_identifier(col)
        table = con.execute(f"SELECT 1 AS id, 2 AS {quoted}").arrow().read_all()
        con.close()
        path = str(tmp_path / "compression_col_quote.parquet")
        pq.write_table(table, path)

        result = get_compression_info(path, column_name=col)

        assert result, "expected a non-empty compression map, got {} (query silently failed)"
        assert all(k == col for k in result)


# --- Fix round 1: siblings in core/add/bbox.py's manual `"{col}"` quoting --
#
# _build_bbox_sql and add_bbox_table (backing Table.add_bbox() in
# api/table.py) build `"{col}"`/`f'"{c}"'` by hand instead of calling
# quote_identifier, so they already tolerate a space but not an embedded `"`
# (which needs doubling to `""`). _make_add_bbox_query -- used by
# _add_bbox_streaming, the `gpio add bbox -` stdin/stdout CLI path -- has the
# identical bug; it wasn't the function named in the fix-round request, but
# it is the one that actually builds the vulnerable SQL for the streaming
# path (see the fix-round entry in task-C-report.md for detail).


class TestApiTableAddBboxQuoting:
    """geoparquet_io/core/add/bbox.py: add_bbox_table -- backs Table.add_bbox()."""

    def test_table_add_bbox_succeeds_with_correct_values(self):
        from geoparquet_io.api.table import Table

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        quoted = quote_identifier(DQUOTE_COL)
        arrow_table = (
            con.execute(
                f"""
                SELECT * FROM (VALUES
                    (1, ST_AsWKB(ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'))),
                    (2, ST_AsWKB(ST_GeomFromText('POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))')))
                ) AS t(id, {quoted})
                """
            )
            .arrow()
            .read_all()
        )
        con.close()

        table = Table(arrow_table, geometry_column=DQUOTE_COL)
        result = table.add_bbox(column_name="bbox")

        rows = result.table.column("bbox").combine_chunks().to_pylist()
        assert rows[0] == {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
        assert rows[1] == {"xmin": 2.0, "ymin": 2.0, "xmax": 3.0, "ymax": 3.0}


class TestAddBboxStreamingQuoting:
    """geoparquet_io/core/add/bbox.py: _make_add_bbox_query, used by
    _add_bbox_streaming (the `gpio add bbox -` stdin/stdout CLI path).

    _add_bbox_streaming's own geometry-column auto-detection only checks
    STANDARD_GEOMETRY_NAMES (a separate, pre-existing limitation, out of
    scope for this fix), so the hostile name is added to that lookup via
    monkeypatch to let the *real* _add_bbox_streaming run end-to-end rather
    than testing the SQL-building helper in isolation.
    """

    def test_add_bbox_streaming_succeeds_with_correct_values(self, tmp_path, monkeypatch):
        from geoparquet_io.core.add import bbox as bbox_module

        monkeypatch.setattr(
            bbox_module,
            "STANDARD_GEOMETRY_NAMES",
            [DQUOTE_COL, *bbox_module.STANDARD_GEOMETRY_NAMES],
        )
        input_path = _wkb_fixture(
            tmp_path,
            DQUOTE_COL,
            "streaming_input.parquet",
            wkts=[
                "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))",
            ],
        )
        output_path = str(tmp_path / "streaming_output.parquet")

        bbox_module._add_bbox_streaming(
            input_path=input_path,
            output_path=output_path,
            bbox_column_name="bbox",
            verbose=False,
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            profile=None,
            force=False,
            geoparquet_version="2.0",
        )

        table = pq.read_table(output_path)
        rows = table.column("bbox").combine_chunks().to_pylist()
        assert rows[0] == {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
        assert rows[1] == {"xmin": 2.0, "ymin": 2.0, "xmax": 3.0, "ymax": 3.0}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
