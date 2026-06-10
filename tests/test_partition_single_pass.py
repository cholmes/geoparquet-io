"""Tests for single-pass partitioning (issue #478).

Partitioning must read the input ONCE via DuckDB ``COPY ... PARTITION_BY``
instead of re-scanning the whole input per partition value. These tests pin the
behaviour that matters: row totals reconcile, only one partitioned scan is
issued, naming is unchanged, and per-partition geo metadata stays correct.
"""

from __future__ import annotations

import json
import os

import duckdb
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.partition.common import partition_by_column


@pytest.fixture
def multi_value_file(temp_output_dir):
    """A small GeoParquet with a low-cardinality ``cat`` column, geometry, and a
    passthrough KV key (``collection``) to verify metadata preservation.

    Three categories with differing geographic extents so per-partition bbox
    must differ from the global bbox.
    """
    from geoparquet_io.core.common import write_parquet_with_metadata

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    path = os.path.join(temp_output_dir, "multi.parquet")
    # cat A in the lower-left, B in the middle, C in the upper-right.
    con.execute(
        """
        CREATE TABLE t AS
        SELECT cat, ST_Point(x, y) AS geometry FROM (
            SELECT 'AAA' AS cat, i AS x, i AS y FROM range(10) tbl(i)
            UNION ALL
            SELECT 'BBB', 100 + i, 100 + i FROM range(20) tbl(i)
            UNION ALL
            SELECT 'CCC', 200 + i, 200 + i FROM range(5) tbl(i)
        )
        """
    )
    write_parquet_with_metadata(
        con,
        "SELECT * FROM t",
        path,
        extra_kv_metadata={"collection": json.dumps({"id": "test-collection"})},
    )
    con.close()
    return path


def _rglob_parquet(folder):
    out = []
    for root, _dirs, files in os.walk(folder):
        out.extend(os.path.join(root, f) for f in files if f.endswith(".parquet"))
    return out


def _row_count(path):
    return pq.ParquetFile(path).metadata.num_rows


class TestReconciliation:
    """sum(partition rows) == input rows; no duplication or loss."""

    def test_flat_keep_column(self, multi_value_file, temp_output_dir):
        out = os.path.join(temp_output_dir, "flat")
        n = partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            keep_partition_column=True,
            skip_analysis=True,
        )
        files = _rglob_parquet(out)
        assert len(files) == 3
        assert n == 3
        assert sum(_row_count(f) for f in files) == 35
        # Files named by value, flat layout
        assert sorted(os.path.basename(f) for f in files) == [
            "AAA.parquet",
            "BBB.parquet",
            "CCC.parquet",
        ]
        # Partition column kept
        assert "cat" in pq.ParquetFile(files[0]).schema_arrow.names

    def test_flat_drop_column(self, multi_value_file, temp_output_dir):
        out = os.path.join(temp_output_dir, "drop")
        partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            keep_partition_column=False,
            skip_analysis=True,
        )
        files = _rglob_parquet(out)
        assert sum(_row_count(f) for f in files) == 35
        assert "cat" not in pq.ParquetFile(files[0]).schema_arrow.names

    def test_hive_layout(self, multi_value_file, temp_output_dir):
        out = os.path.join(temp_output_dir, "hive")
        partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            hive=True,
            skip_analysis=True,
        )
        # Hive dirs cat=AAA/AAA.parquet
        assert os.path.isdir(os.path.join(out, "cat=AAA"))
        assert os.path.isfile(os.path.join(out, "cat=AAA", "AAA.parquet"))
        files = _rglob_parquet(out)
        assert sum(_row_count(f) for f in files) == 35

    def test_chars_prefix(self, multi_value_file, temp_output_dir):
        # First char only -> A, B, C
        out = os.path.join(temp_output_dir, "chars")
        partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            column_prefix_length=1,
            skip_analysis=True,
        )
        files = _rglob_parquet(out)
        assert sorted(os.path.basename(f) for f in files) == [
            "A.parquet",
            "B.parquet",
            "C.parquet",
        ]
        assert sum(_row_count(f) for f in files) == 35

    def test_filename_prefix(self, multi_value_file, temp_output_dir):
        out = os.path.join(temp_output_dir, "pfx")
        partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            filename_prefix="places",
            skip_analysis=True,
        )
        files = _rglob_parquet(out)
        assert all(os.path.basename(f).startswith("places_") for f in files)
        assert sum(_row_count(f) for f in files) == 35


class TestSingleScan:
    """Exactly one partitioned scan of the input; no per-value WHERE writes."""

    def test_one_partition_by_no_per_value_loop(
        self, multi_value_file, temp_output_dir, monkeypatch
    ):
        executed: list[str] = []

        real_connect = duckdb.connect

        class SpyCon:
            def __init__(self, con):
                self._con = con

            def execute(self, sql, *args, **kwargs):
                executed.append(sql)
                return self._con.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._con, name)

        def fake_get_conn(*_args, **_kwargs):
            con = real_connect()
            con.execute("INSTALL spatial; LOAD spatial;")
            con.execute("INSTALL httpfs; LOAD httpfs;")
            return SpyCon(con)

        # Patch the connection factory used inside partition_by_column.
        monkeypatch.setattr(
            "geoparquet_io.core.partition.common.get_duckdb_connection", fake_get_conn
        )

        out = os.path.join(temp_output_dir, "scan")
        partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            skip_analysis=True,
        )

        partition_by_stmts = [s for s in executed if "PARTITION_BY" in s.upper()]
        assert len(partition_by_stmts) == 1, executed

        # No statement should filter the input by a single partition value
        # (the old O(N) per-value pattern).
        per_value = [s for s in executed if '"cat" =' in s or '"cat"=\'' in s]
        assert per_value == [], per_value


class TestMetadata:
    """Each partition keeps valid geo metadata, tight bbox, and passthrough KV."""

    def test_geo_and_passthrough_and_tight_bbox(self, multi_value_file, temp_output_dir):
        out = os.path.join(temp_output_dir, "meta")
        partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            skip_analysis=True,
        )
        files = {os.path.basename(f): f for f in _rglob_parquet(out)}

        bboxes = {}
        for name, path in files.items():
            md = pq.ParquetFile(path).schema_arrow.metadata or {}
            assert b"geo" in md, f"{name} missing geo metadata"
            geo = json.loads(md[b"geo"])
            col = geo["columns"][geo["primary_column"]]
            assert "geometry_types" in col
            assert "bbox" in col
            bboxes[name] = col["bbox"]
            # passthrough KV preserved
            assert b"collection" in md, f"{name} missing collection KV"

        # Per-partition bbox must be tight, not the global bbox.
        # AAA is in lower-left (~0..9), CCC upper-right (~200..204).
        assert bboxes["AAA.parquet"][0] < 50
        assert bboxes["CCC.parquet"][0] > 150
        assert bboxes["AAA.parquet"] != bboxes["CCC.parquet"]
