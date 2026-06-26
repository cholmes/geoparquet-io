"""Tests for the reduce-precision command (core, CLI, and Python API).

Precision reduction snaps coordinates to a fixed grid via DuckDB's
``ST_ReducePrecision``. The contract under test is the ordered pipeline:

    repair (make-valid) -> reduce -> drop empty/null

plus bbox-column regeneration so a stored covering bbox stays consistent with
the reduced geometry.
"""

from __future__ import annotations

import json

import duckdb
import pyarrow as pa
from click.testing import CliRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _spatial_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    return con


def _geo_table(wkts: list[str], *, with_bbox: bool = False) -> pa.Table:
    """Build a WKB-encoded GeoParquet-style Arrow table from WKT strings.

    Column ``id`` preserves input order; ``geometry`` is WKB binary; an optional
    ``bbox`` struct is computed from the *original* geometry so tests can prove
    it gets regenerated.
    """
    con = _spatial_con()
    rows = ", ".join(f"({i}, ST_GeomFromText('{w}'))" for i, w in enumerate(wkts))
    con.execute(f"CREATE TABLE t AS SELECT * FROM (VALUES {rows}) AS v(id, geom)")
    select = "id, ST_AsWKB(geom) AS geometry"
    if with_bbox:
        select += (
            ", STRUCT_PACK(xmin := ST_XMin(geom), ymin := ST_YMin(geom), "
            "xmax := ST_XMax(geom), ymax := ST_YMax(geom)) AS bbox"
        )
    table = con.execute(f"SELECT {select} FROM t ORDER BY id").arrow().read_all()
    con.close()
    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": []}},
    }
    return table.replace_schema_metadata({b"geo": json.dumps(geo_meta).encode()})


def _bounds(table: pa.Table, col: str = "geometry") -> list[tuple]:
    con = _spatial_con()
    con.register("r", table)
    out = con.execute(
        f"SELECT ST_XMin(g), ST_YMin(g), ST_XMax(g), ST_YMax(g) "
        f'FROM (SELECT ST_GeomFromWKB("{col}") g, id FROM r ORDER BY id)'
    ).fetchall()
    con.close()
    return out


# Slivers that ST_ReducePrecision collapses to EMPTY at grid=1.0
SLIVER = "POLYGON((0 0, 10 0.2, 10 0, 0 0))"
SQUARE = "POLYGON((0.4 0.4, 9.6 0.4, 9.6 9.6, 0.4 9.6, 0.4 0.4))"


# ---------------------------------------------------------------------------
# Core: reduce_precision_table (Arrow in / Arrow out)
# ---------------------------------------------------------------------------
class TestReducePrecisionTable:
    def test_snaps_coordinates_to_grid(self):
        from geoparquet_io.core.reduce_precision import reduce_precision_table

        out = reduce_precision_table(_geo_table([SQUARE]), grid=1.0)
        xmin, ymin, xmax, ymax = _bounds(out)[0]
        assert (xmin, ymin, xmax, ymax) == (0.0, 0.0, 10.0, 10.0)

    def test_output_is_valid(self):
        from geoparquet_io.core.reduce_precision import reduce_precision_table

        # Bowtie is invalid on input; reduce+repair must yield valid geometry.
        bowtie = "POLYGON((0 0, 4 0, 0.1 3.4, 3.9 3.6, 0 0))"
        out = reduce_precision_table(_geo_table([bowtie]), grid=1.0)
        con = _spatial_con()
        con.register("r", out)
        valid = con.execute(
            'SELECT bool_and(ST_IsValid(ST_GeomFromWKB("geometry"))) FROM r'
        ).fetchone()[0]
        con.close()
        assert valid is True

    def test_drops_collapsed_empty_by_default(self):
        from geoparquet_io.core.reduce_precision import reduce_precision_table

        out = reduce_precision_table(_geo_table([SQUARE, SLIVER]), grid=1.0)
        assert out.num_rows == 1  # sliver collapsed to EMPTY -> dropped

    def test_keep_empty_retains_collapsed(self):
        from geoparquet_io.core.reduce_precision import reduce_precision_table

        out = reduce_precision_table(_geo_table([SQUARE, SLIVER]), grid=1.0, drop_empty=False)
        assert out.num_rows == 2

    def test_drops_preexisting_empty(self):
        from geoparquet_io.core.reduce_precision import reduce_precision_table

        out = reduce_precision_table(_geo_table([SQUARE, "POLYGON EMPTY"]), grid=1.0)
        assert out.num_rows == 1

    def test_preserves_geo_metadata(self):
        from geoparquet_io.core.reduce_precision import reduce_precision_table

        out = reduce_precision_table(_geo_table([SQUARE]), grid=1.0)
        assert out.schema.metadata is not None
        assert b"geo" in out.schema.metadata

    def test_regenerates_bbox_column(self):
        from geoparquet_io.core.reduce_precision import reduce_precision_table

        out = reduce_precision_table(_geo_table([SQUARE], with_bbox=True), grid=1.0)
        bbox = out.column("bbox").to_pylist()[0]
        # Stale bbox would be (0.4, 0.4, 9.6, 9.6); regenerated must match reduced geom.
        assert (bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"]) == (
            0.0,
            0.0,
            10.0,
            10.0,
        )

    def test_repair_false_skips_makevalid(self):
        from geoparquet_io.core.reduce_precision import reduce_precision_table

        # With repair disabled the command must not call ST_MakeValid; the
        # operation should still succeed and snap coordinates.
        out = reduce_precision_table(_geo_table([SQUARE]), grid=1.0, repair=False)
        assert out.num_rows == 1


# ---------------------------------------------------------------------------
# CLI: gpio reduce-precision
# ---------------------------------------------------------------------------
class TestReducePrecisionCLI:
    def test_requires_grid(self, buildings_test_file, temp_output_file):
        from geoparquet_io.cli.main import cli

        result = CliRunner().invoke(
            cli, ["reduce-precision", buildings_test_file, temp_output_file]
        )
        assert result.exit_code != 0  # --grid is required

    def test_basic_invocation(self, buildings_test_file, temp_output_file):
        import pyarrow.parquet as pq

        from geoparquet_io.cli.main import cli

        result = CliRunner().invoke(
            cli, ["reduce-precision", buildings_test_file, temp_output_file, "--grid", "1e-6"]
        )
        assert result.exit_code == 0, result.output
        # Output is a readable GeoParquet with the geometry column intact.
        pf = pq.ParquetFile(temp_output_file)
        assert "geometry" in pf.schema_arrow.names
        assert pf.metadata.num_rows > 0

    def test_output_geometry_valid(self, buildings_test_file, temp_output_file):
        from geoparquet_io.cli.main import cli

        result = CliRunner().invoke(
            cli, ["reduce-precision", buildings_test_file, temp_output_file, "--grid", "1e-6"]
        )
        assert result.exit_code == 0, result.output
        con = _spatial_con()
        valid = con.execute(
            f"SELECT bool_and(ST_IsValid(geometry)) FROM read_parquet('{temp_output_file}')"
        ).fetchone()[0]
        con.close()
        assert valid is True


# ---------------------------------------------------------------------------
# Python API: ops.reduce_precision + Table.reduce_precision
# ---------------------------------------------------------------------------
class TestReducePrecisionPythonAPI:
    def test_ops_function(self):
        from geoparquet_io.api import ops

        out = ops.reduce_precision(_geo_table([SQUARE]), grid=1.0)
        assert out.num_rows == 1
        assert _bounds(out)[0] == (0.0, 0.0, 10.0, 10.0)

    def test_table_method_chainable(self):
        from geoparquet_io.api.table import Table

        result = Table(_geo_table([SQUARE, SLIVER])).reduce_precision(grid=1.0)
        assert isinstance(result, Table)
        assert result.num_rows == 1  # sliver dropped
