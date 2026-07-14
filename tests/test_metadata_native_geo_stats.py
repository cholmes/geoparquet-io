import geoarrow.pyarrow as gap
import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.metadata_utils import has_parquet_native_geo_stats


def _write_native_geom(path):
    # Two points -> WKB bytes; typed as geoarrow.wkb so pyarrow emits GeoStatistics.
    import struct

    def wkb_point(x, y):
        return struct.pack("<BIdd", 1, 1, x, y)

    geom = pa.array([wkb_point(0, 1), wkb_point(99, 100)], type=pa.binary())
    geom = geom.cast(gap.wkb())
    tbl = pa.table({"geometry": geom})
    pq.write_table(tbl, path)


def test_detects_native_geo_stats(tmp_path):
    p = tmp_path / "native.parquet"
    _write_native_geom(str(p))
    result = has_parquet_native_geo_stats(str(p))
    assert result["has_stats"] is True
    assert result["sample_bbox"] == [0.0, 1.0, 99.0, 100.0]  # xmin,ymin,xmax,ymax


def test_no_native_geo_stats_on_plain_binary(tmp_path):
    p = tmp_path / "plain.parquet"
    pq.write_table(pa.table({"geometry": pa.array([b"x", b"y"])}), p)
    assert has_parquet_native_geo_stats(str(p))["has_stats"] is False
