"""
Regression tests for stale geo-metadata bbox after transforms/filters.

Covers two empirically reproduced correctness bugs:

1. ``gpio convert reproject`` carried the input's degree-space ``bbox`` into the
   output geo metadata even though coordinates were reprojected to meters.
2. ``gpio extract`` with a row filter (bbox/WHERE) kept the full pre-filter
   ``bbox`` in the output geo metadata instead of the surviving-row extent.

Also verifies:
- Zero-row extracts omit ``bbox`` entirely (a non-empty bbox would be a lie).
- An unfiltered, untransformed copy still PRESERVES its carried bbox.
- A failure while reading input metadata for preservation warns (not silence).
"""

import json
import logging

import duckdb
import pytest

from geoparquet_io.core.extract import extract
from geoparquet_io.core.reproject import reproject_impl
from geoparquet_io.core.write_strategies import strip_stale_bbox
from tests.conftest import get_geo_metadata


class TestStripStaleBbox:
    """Unit tests for the strip_stale_bbox helper (all input forms)."""

    def _geo(self):
        return {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {"encoding": "WKB", "bbox": [1, 1, 3, 3]},
            },
        }

    def test_none_and_empty(self):
        assert strip_stale_bbox(None) is None
        assert strip_stale_bbox({}) == {}

    def test_no_geo_key_unchanged(self):
        meta = {"pandas": "{}"}
        assert strip_stale_bbox(meta) is meta

    def test_bytes_form(self):
        meta = {b"geo": json.dumps(self._geo()).encode("utf-8")}
        out = strip_stale_bbox(meta)
        parsed = json.loads(out[b"geo"].decode("utf-8"))
        assert "bbox" not in parsed["columns"]["geometry"]
        # original untouched
        assert "bbox" in json.loads(meta[b"geo"].decode("utf-8"))["columns"]["geometry"]

    def test_str_form(self):
        meta = {"geo": json.dumps(self._geo())}
        out = strip_stale_bbox(meta)
        parsed = json.loads(out["geo"])
        assert "bbox" not in parsed["columns"]["geometry"]

    def test_dict_form(self):
        meta = {"geo": self._geo()}
        out = strip_stale_bbox(meta)
        assert "bbox" not in out["geo"]["columns"]["geometry"]
        # original dict not mutated
        assert "bbox" in meta["geo"]["columns"]["geometry"]

    def test_no_bbox_returns_input_object(self):
        geo = self._geo()
        del geo["columns"]["geometry"]["bbox"]
        meta = {"geo": geo}
        # Nothing to strip: same object handed back.
        assert strip_stale_bbox(meta) is meta

    def test_malformed_columns_returns_input(self):
        meta = {"geo": {"version": "1.1.0"}}  # no "columns"
        assert strip_stale_bbox(meta) is meta


def _make_crs84_points(path):
    """Write a small CRS84 GeoParquet file: points at (1,1), (2,2), (3,3).

    Its geo metadata bbox is [1, 1, 3, 3] (degrees), CRS84 (crs omitted).
    """
    from geoparquet_io.core.common import write_parquet_with_metadata

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    query = (
        "SELECT * FROM (VALUES "
        "(1, ST_Point(1.0, 1.0)), "
        "(2, ST_Point(2.0, 2.0)), "
        "(3, ST_Point(3.0, 3.0))"
        ") AS t(id, geometry)"
    )
    try:
        write_parquet_with_metadata(con, query, str(path), geoparquet_version="1.1")
    finally:
        con.close()


def _read_x_range(path):
    """Return (min_x, max_x) of the geometry column via DuckDB."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    try:
        row = con.execute(
            f"SELECT MIN(ST_X(geometry)), MAX(ST_X(geometry)) FROM read_parquet('{path}')"
        ).fetchone()
    finally:
        con.close()
    return row[0], row[1]


class TestReprojectBboxRefreshed:
    """Reproject must not advertise the old degree-space bbox."""

    @pytest.mark.parametrize("version", ["1.1", "2.0"])
    def test_bbox_in_meters_and_encloses_data(self, tmp_path, version):
        src = tmp_path / "src.parquet"
        out = tmp_path / f"out_{version}.parquet"
        _make_crs84_points(src)

        reproject_impl(
            str(src),
            str(out),
            target_crs="EPSG:3857",
            source_crs="OGC:CRS84",
            geoparquet_version=version,
        )

        geo = get_geo_metadata(str(out))
        col = geo["columns"][geo["primary_column"]]
        bbox = col.get("bbox")
        assert bbox is not None, "reprojected output should still carry a bbox"

        # Web Mercator meters for lon 1..3 deg are ~111k..334k, never <= 3.
        assert bbox[0] > 1000, f"bbox xmin still in degrees: {bbox}"
        assert bbox[2] > 1000, f"bbox xmax still in degrees: {bbox}"

        # The advertised bbox must enclose the actual reprojected coordinates.
        min_x, max_x = _read_x_range(out)
        assert bbox[0] <= min_x + 1e-6
        assert bbox[2] >= max_x - 1e-6


class TestExtractBboxReflectsFilter:
    """Filtered extract must reflect only surviving rows."""

    def test_filtered_bbox_shrinks(self, tmp_path):
        src = tmp_path / "src.parquet"
        out = tmp_path / "out.parquet"
        _make_crs84_points(src)

        # bbox 0.5,0.5,1.5,1.5 keeps only the point at (1, 1).
        extract(str(src), str(out), bbox="0.5,0.5,1.5,1.5")

        geo = get_geo_metadata(str(out))
        col = geo["columns"][geo["primary_column"]]
        bbox = col.get("bbox")
        assert bbox is not None
        # Must be the single surviving point's extent, not [1, 1, 3, 3].
        assert bbox == [1.0, 1.0, 1.0, 1.0], f"stale pre-filter bbox: {bbox}"

    def test_zero_row_extract_omits_bbox(self, tmp_path):
        src = tmp_path / "src.parquet"
        out = tmp_path / "out.parquet"
        _make_crs84_points(src)

        # A bbox far from the data yields zero rows.
        extract(str(src), str(out), bbox="100,100,101,101")

        # The write must still succeed and produce a readable file.
        con = duckdb.connect()
        try:
            count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]
        finally:
            con.close()
        assert count == 0

        geo = get_geo_metadata(str(out))
        col = geo["columns"][geo["primary_column"]]
        assert "bbox" not in col, f"zero-row output advertises a bbox: {col.get('bbox')}"

    def test_unfiltered_copy_preserves_bbox(self, tmp_path):
        src = tmp_path / "src.parquet"
        out = tmp_path / "out.parquet"
        _make_crs84_points(src)

        # Pure column pass-through: no row filter, no transform.
        extract(str(src), str(out))

        geo = get_geo_metadata(str(out))
        col = geo["columns"][geo["primary_column"]]
        assert col.get("bbox") == [1.0, 1.0, 3.0, 3.0]


class TestStreamingPathsInvalidateBbox:
    """The streaming write paths must also drop the stale carried bbox."""

    def test_streaming_extract_filter(self, tmp_path):
        from geoparquet_io.core.extract import _extract_streaming

        src = tmp_path / "src.parquet"
        out = tmp_path / "out.parquet"
        _make_crs84_points(src)

        # Filter to the single point at (1, 1) via the streaming path.
        _extract_streaming(
            str(src),
            str(out),
            None,  # include_cols
            None,  # exclude_cols
            (0.5, 0.5, 1.5, 1.5),  # bbox_tuple
            None,  # geometry_wkt
            None,  # where
            None,  # limit
            False,  # verbose
            "ZSTD",  # compression
            None,  # compression_level
            None,  # row_group_size_mb
            None,  # row_group_rows
            None,  # profile
            "1.1",  # geoparquet_version
        )

        geo = get_geo_metadata(str(out))
        col = geo["columns"][geo["primary_column"]]
        # Must not carry the full pre-filter extent.
        assert col.get("bbox") != [1.0, 1.0, 3.0, 3.0]

    def test_streaming_reproject(self, tmp_path):
        from geoparquet_io.core.reproject import _reproject_streaming

        src = tmp_path / "src.parquet"
        out = tmp_path / "out.parquet"
        _make_crs84_points(src)

        _reproject_streaming(
            str(src),
            str(out),
            "EPSG:3857",  # target_crs
            "OGC:CRS84",  # source_crs
            "ZSTD",  # compression
            None,  # compression_level
            False,  # verbose
            None,  # profile
            "1.1",  # geoparquet_version
        )

        geo = get_geo_metadata(str(out))
        col = geo["columns"][geo["primary_column"]]
        bbox = col.get("bbox")
        # Either recomputed to meters or omitted — never the degree-space stale one.
        assert bbox != [1.0, 1.0, 3.0, 3.0]
        if bbox is not None:
            assert bbox[0] > 1000


class TestMetadataPreservationWarns:
    """A metadata-read failure must warn, not silently drop all metadata."""

    def test_warns_when_preservation_fails(self, tmp_path, monkeypatch, caplog):
        src = tmp_path / "src.parquet"
        out = tmp_path / "out.parquet"
        _make_crs84_points(src)

        import geoparquet_io.core.extract as extract_mod

        def _boom(*args, **kwargs):
            raise OSError("simulated metadata read failure")

        monkeypatch.setattr(extract_mod, "get_parquet_metadata", _boom)

        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            extract(str(src), str(out))

        # The write still succeeds despite the metadata read failing.
        con = duckdb.connect()
        try:
            count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]
        finally:
            con.close()
        assert count == 3

        assert any(
            "metadata" in rec.message.lower() and rec.levelno == logging.WARNING
            for rec in caplog.records
        ), f"expected a warning about skipped metadata; got {[r.message for r in caplog.records]}"
