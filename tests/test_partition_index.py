"""Tests for the partition file->bbox index builder (core, CLI, Python API).

The index is built from parquet footers only (``parquet_metadata`` — no data
scan): per-file bounds aggregated from the bbox covering column's row-group
stats, written to a small ``_partitions.parquet`` a tiler can use to route tiles
to the 1-2 files that overlap.
"""

from __future__ import annotations

import os

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    return con


def _write_file(path: str, wkts: list[str], layout: str = "struct") -> None:
    """Write a small parquet file with geometry + a bbox covering in ``layout``.

    layout: "struct" (gpio bbox struct), "top" (top-level xmin/.. doubles),
    or "none" (no bbox covering at all).
    """
    con = _con()
    rows = ", ".join(f"({i}, ST_GeomFromText('{w}'))" for i, w in enumerate(wkts))
    con.execute(f"CREATE TABLE t AS SELECT id, geom FROM (VALUES {rows}) v(id, geom)")
    if layout == "struct":
        sel = (
            "id, ST_AsWKB(geom) AS geometry, "
            "STRUCT_PACK(xmin := ST_XMin(geom), ymin := ST_YMin(geom), "
            "xmax := ST_XMax(geom), ymax := ST_YMax(geom)) AS bbox"
        )
    elif layout == "top":
        sel = (
            "id, ST_AsWKB(geom) AS geometry, ST_XMin(geom) AS xmin, ST_YMin(geom) AS ymin, "
            "ST_XMax(geom) AS xmax, ST_YMax(geom) AS ymax"
        )
    else:
        sel = "id, ST_AsWKB(geom) AS geometry"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con.execute(f"COPY (SELECT {sel} FROM t) TO '{path}' (FORMAT PARQUET)")
    con.close()


def _rows(path: str) -> list[dict]:
    return pq.read_table(path).to_pylist()


# Two boxes per file so the aggregated file bbox is non-trivial.
FILE_A = ["POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", "POLYGON((5 5, 20 5, 20 20, 5 20, 5 5))"]
FILE_B = ["POLYGON((100 100, 110 100, 110 110, 100 110, 100 100))"]


class TestBuildPartitionIndexCore:
    def test_aggregates_per_file_bounds(self, tmp_path):
        from geoparquet_io.core.partition_index import build_partition_index

        _write_file(str(tmp_path / "a.parquet"), FILE_A)
        _write_file(str(tmp_path / "b.parquet"), FILE_B)
        out = str(tmp_path / "_partitions.parquet")
        build_partition_index(str(tmp_path / "*.parquet"), out)

        rows = {os.path.basename(r["file_name"]): r for r in _rows(out)}
        assert set(rows) == {"a.parquet", "b.parquet"}
        a = rows["a.parquet"]
        assert (a["xmin"], a["ymin"], a["xmax"], a["ymax"]) == (0.0, 0.0, 20.0, 20.0)
        b = rows["b.parquet"]
        assert (b["xmin"], b["ymin"], b["xmax"], b["ymax"]) == (100.0, 100.0, 110.0, 110.0)

    def test_top_level_bbox_layout(self, tmp_path):
        from geoparquet_io.core.partition_index import build_partition_index

        _write_file(str(tmp_path / "a.parquet"), FILE_A, layout="top")
        out = str(tmp_path / "_partitions.parquet")
        build_partition_index(str(tmp_path / "*.parquet"), out)
        a = _rows(out)[0]
        assert (a["xmin"], a["ymin"], a["xmax"], a["ymax"]) == (0.0, 0.0, 20.0, 20.0)

    def test_partition_key_extracted_from_path(self, tmp_path):
        from geoparquet_io.core.partition_index import build_partition_index

        _write_file(str(tmp_path / "provincia=28" / "data.parquet"), FILE_A)
        _write_file(str(tmp_path / "provincia=08" / "data.parquet"), FILE_B)
        out = str(tmp_path / "_partitions.parquet")
        build_partition_index(str(tmp_path / "**" / "*.parquet"), out, partition_key="provincia")
        provincias = sorted(r["provincia"] for r in _rows(out))
        assert provincias == ["08", "28"]

    def test_errors_without_bbox_columns(self, tmp_path):
        from geoparquet_io.core.partition_index import build_partition_index

        _write_file(str(tmp_path / "a.parquet"), FILE_A, layout="none")
        with pytest.raises(ValueError, match="bbox"):
            build_partition_index(str(tmp_path / "*.parquet"), str(tmp_path / "out.parquet"))

    def test_rejects_bad_partition_key(self, tmp_path):
        from geoparquet_io.core.partition_index import build_partition_index

        _write_file(str(tmp_path / "a.parquet"), FILE_A)
        with pytest.raises(ValueError):
            build_partition_index(
                str(tmp_path / "*.parquet"),
                str(tmp_path / "out.parquet"),
                partition_key="bad key; DROP",
            )


class TestPartitionIndexCLI:
    def test_cli_builds_index(self, tmp_path):
        from geoparquet_io.cli.main import cli

        _write_file(str(tmp_path / "a.parquet"), FILE_A)
        _write_file(str(tmp_path / "b.parquet"), FILE_B)
        out = str(tmp_path / "_partitions.parquet")
        result = CliRunner().invoke(
            cli, ["publish", "partition-index", str(tmp_path / "*.parquet"), out]
        )
        assert result.exit_code == 0, result.output
        assert len(_rows(out)) == 2

    def test_cli_errors_without_bbox(self, tmp_path):
        from geoparquet_io.cli.main import cli

        _write_file(str(tmp_path / "a.parquet"), FILE_A, layout="none")
        out = str(tmp_path / "out.parquet")
        result = CliRunner().invoke(
            cli, ["publish", "partition-index", str(tmp_path / "*.parquet"), out]
        )
        assert result.exit_code != 0


class TestPartitionIndexPythonAPI:
    def test_ops_function(self, tmp_path):
        from geoparquet_io.api import ops

        _write_file(str(tmp_path / "a.parquet"), FILE_A)
        out = str(tmp_path / "_partitions.parquet")
        ops.build_partition_index(str(tmp_path / "*.parquet"), out)
        assert len(_rows(out)) == 1
